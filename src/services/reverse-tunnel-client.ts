/**
 * Reverse Tunnel Client — the BACKEND-side WS CLIENT that dials INTO
 * each active Daytona MicroVM via its signed `*.daytonaproxy01.eu`
 * preview URL.
 *
 * WHY THIS EXISTS
 * ───────────────
 * The previous architecture (Task 14 + earlier) had the VM's
 * `tunnel_client.py` dialing OUT to `wss://arcforge-backend.onrender.com/api/tunnel`.
 * This worked in theory but FAILED in production because the Daytona
 * EU-region egress filter blocks ALL outbound TLS from the VM (verified
 * live in Task 14 — `*.onrender.com`, `*.google.com`, `*.nvidia.com`,
 * even `1.1.1.1` by IP are reset right after the TLS Client Hello;
 * only `api.github.com` is whitelisted for git clones).
 *
 * The fix: FLIP the tunnel direction. The BACKEND dials INTO the VM
 * via the signed `*.daytonaproxy01.eu` URL — the same path the frontend
 * already uses for its `/ws` connection to the VM's orchestrator. This
 * is INBOUND through the egress filter (the connection originates
 * externally), so it's not blocked.
 *
 * The VM-side orchestrator exposes a new `/reverse-tunnel` WS endpoint
 * (see `daytona-service/app/agent_sidecar/orchestrator.py` — the
 * `reverse_tunnel_endpoint` function + the `rt_mux` multiplexer). The
 * orchestrator's worker thread sends `req` frames over this inbound
 * WS; this service receives them, calls NVIDIA (with the server-side
 * key injected), and streams `res`/`chunk`/`done` frames back down the
 * same WS.
 *
 * PROTOCOL (matches the existing `/api/tunnel` server in
 * `src/routes/tunnel.ts` — keep in sync):
 *   VM→backend: {t:"req", id, method, path, headers, body}
 *   backend→VM: {t:"res", id, status, headers}
 *                {t:"chunk", id, body}
 *                {t:"done", id}
 *                {t:"error", id, message}
 *                {t:"ping"} / {t:"pong"}
 *
 * PATH ROUTING (2026-08-28 + GROUP 2 session 2): req frames whose `path`
 *   starts with /tunnel/github are routed to services/github-proxy.ts
 *   (this connection is per-sandbox, so the proxy resolves
 *   sandboxId→project→user→PAT and calls api.github.com with the PAT
 *   injected server-side — the PAT never enters the VM). /mcp/supabase
 *   frames route to services/supabase-mcp.ts (sandboxId→project→user →
 *   vault OAuth token → the official Supabase MCP server — the token
 *   never enters the VM). Other /tunnel/* paths get a 404 JSON;
 *   everything else goes to the NVIDIA forwarder unchanged. Mirrors
 *   routes/tunnel.ts.
 *
 * AUTH
 * ────
 * The shared secret `process.env.AGENT_PROXY_SECRET` is presented as
 * `X-Agent-Token` on the WS upgrade — the same secret the VM's
 * orchestrator reads from its `TUNNEL_TOKEN` env var (written by the
 * installer in `agent_installer.py`).
 *
 * LIFECYCLE
 * ─────────
 * `ensureReverseTunnel(sandbox_id, signed_url)` is called by the
 * agent-info endpoint (in `src/routes/workspace.ts`) AFTER it fetches
 * the signed URL from the daytona-service. If a connection for that
 * sandbox is already live (or in-flight), the call is a no-op. The
 * connection persists until the VM dies or the backend restarts; a
 * reconnect loop with exponential backoff handles transient drops.
 *
 * There is no explicit disconnect call — connections are retired
 * implicitly when the WS closes (VM died) and the reconnect loop
 * gives up after maxRetries. The next agent-info call will trigger a
 * fresh dial-in.
 */
import WebSocket from "ws";
import { forwardToNvidia, type ForwardParams } from "./nvidia-forwarder";
import { handleGithubTunnel } from "./github-proxy";
import { handleMcpTunnel } from "./supabase-mcp";
import { logger } from "../lib/logger";

// ─── Configuration ──────────────────────────────────────────────────────
/** Path on the VM's orchestrator (port 9000) where this service dials in. */
const REVERSE_TUNNEL_PATH = "/reverse-tunnel";
/**
 * NEVER give up. There is no max-retry cap.
 *
 * The previous cap (MAX_RETRIES=30) retired a live sandbox's bridge after
 * 30 close events, creating a silent dead window until the next
 * agent-info call re-triggered ensureReverseTunnel. With Render services
 * on Uptime Robot (never sleep), every close is a transient (Daytona
 * proxy idle timeout, VM orchestrator restart, network blip) that
 * recovers on the next dial. Keep reconnecting forever with the backoff
 * schedule below; a truly-dead VM fails harmlessly on each dial attempt.
 */
const BACKOFF_SEC = [1, 2, 5, 10, 30, 30, 30, 60];
/**
 * Ping every 15s (was 60s). The Daytona preview proxy closes idle WS
 * connections on its own schedule (shorter than 60s), which caused
 * silent drops before the keepalive ping could fire. 15s keeps the
 * proxy from idle-closing the connection.
 */
const IDLE_PING_MS = 15_000;
/** Pong timeout (ms) — if no pong after this, terminate the connection. */
const PONG_TIMEOUT_MS = 30_000;

// ─── Per-VM connection state ─────────────────────────────────────────────
interface ReverseTunnelConnection {
  sandboxId: string;
  /** The signed `*.daytonaproxy01.eu` base URL (e.g. `https://9000-xxx.daytonaproxy01.eu`). */
  signedBaseUrl: string;
  /** Active WS or null when reconnecting. */
  ws: WebSocket | null;
  /** True while we're actively trying to connect (prevents dup dials). */
  connecting: boolean;
  /** True when the connection is being torn down deliberately. */
  closing: boolean;
  /** Current retry count (resets to 0 on a successful connection). */
  retries: number;
  /** Idle timer — fires a ping if no frame received in IDLE_PING_MS. */
  idleTimer: ReturnType<typeof setTimeout> | null;
  /** Pong timer — fires a terminate if no pong in PONG_TIMEOUT_MS. */
  pongTimer: ReturnType<typeof setTimeout> | null;
  /** True while waiting for a pong. */
  awaitingPong: boolean;
}

// ─── Registry — sandbox_id → connection ─────────────────────────────────
const connections = new Map<string, ReverseTunnelConnection>();

// ─── Auth ───────────────────────────────────────────────────────────────
function proxySecret(): string {
  return process.env.AGENT_PROXY_SECRET || "";
}

// ─── Helpers ─────────────────────────────────────────────────────────────
/** Build the full WS URL from the signed base URL + path. */
function buildWsUrl(signedBaseUrl: string): string {
  // Strip trailing slash on the base, then append our path.
  let base = signedBaseUrl.replace(/\/+$/, "");
  // The signed URL comes back as https://... — convert to wss://...
  if (base.startsWith("https://")) {
    base = `wss://${base.slice("https://".length)}`;
  } else if (base.startsWith("http://")) {
    base = `ws://${base.slice("http://".length)}`;
  }
  return `${base}${REVERSE_TUNNEL_PATH}`;
}

/** Send a text frame if the WS is open. No-op otherwise. */
function send(conn: ReverseTunnelConnection, frame: Record<string, unknown>): void {
  if (conn.ws && conn.ws.readyState === WebSocket.OPEN) {
    conn.ws.send(JSON.stringify(frame));
  }
}

/** Reset the idle timer — called whenever any frame is received. */
function resetIdle(conn: ReverseTunnelConnection): void {
  if (conn.idleTimer) clearTimeout(conn.idleTimer);
  if (conn.pongTimer) {
    clearTimeout(conn.pongTimer);
    conn.pongTimer = null;
  }
  conn.awaitingPong = false;
  if (conn.closing || !conn.ws) return;
  conn.idleTimer = setTimeout(() => {
    if (conn.closing || !conn.ws || conn.ws.readyState !== WebSocket.OPEN) return;
    conn.awaitingPong = true;
    send(conn, { t: "ping" });
    conn.pongTimer = setTimeout(() => {
      if (conn.closing) return;
      logger.warn(
        { sandboxId: conn.sandboxId },
        "reverse-tunnel: no pong within timeout — terminating connection",
      );
      try {
        conn.ws?.close(1011, "pong timeout");
      } catch {
        /* noop */
      }
      // Hard terminate if close didn't take effect.
      setTimeout(() => {
        if (conn.ws && conn.ws.readyState !== WebSocket.CLOSED) {
          conn.ws.terminate();
        }
      }, 1_000);
    }, PONG_TIMEOUT_MS);
  }, IDLE_PING_MS);
}

/** Clear all timers — called on close. */
function clearTimers(conn: ReverseTunnelConnection): void {
  if (conn.idleTimer) {
    clearTimeout(conn.idleTimer);
    conn.idleTimer = null;
  }
  if (conn.pongTimer) {
    clearTimeout(conn.pongTimer);
    conn.pongTimer = null;
  }
}

// ─── Per-req frame handler ──────────────────────────────────────────────
/**
 * Handle a single `req` frame end-to-end, ROUTED BY PATH:
 *   /tunnel/github → GitHub proxy (sandboxId→project→user→PAT; the PAT
 *                   is injected here, server-side — it never enters the VM)
 *   /mcp/supabase  → Supabase MCP executor (sandboxId→project→user →
 *                   vault OAuth token — GROUP 2 session 2)
 *   /tunnel/*      → 404 JSON (unknown tunnel path)
 *   else           → NVIDIA forwarder (unchanged — /v1/*, /vlm/* and
 *                   legacy bare paths)
 *
 * Sends `res` + `chunk`(s) + `done` to the VM, or `error` on failure.
 * Never throws to the caller — all errors are converted to `error`
 * frames.
 *
 * This is the SAME logic as `handleReqFrame` in `src/routes/tunnel.ts`
 * — the only difference is that we hold the WS to the VM (vs. the VM
 * holding the WS to us) AND this connection is per-sandbox, so the
 * GitHub proxy gets its identity from `conn.sandboxId` directly.
 */
async function handleReqFrame(
  conn: ReverseTunnelConnection,
  frame: { id?: string; method?: string; path?: string; headers?: Record<string, string>; body?: string },
): Promise<void> {
  const id = frame.id || "";
  if (!id) {
    send(conn, { t: "error", id: "", message: "req frame missing id" });
    return;
  }

  // PATH ROUTING — strip the query string before prefix matching (the
  // VM-side client appends ?query to the path).
  const path = (frame.path || "").split("?")[0];

  if (path.startsWith("/tunnel/github")) {
    // GitHub REST + workspace sync on the user's PAT — identity comes
    // from this per-sandbox connection.
    try {
      let sentHead = false;
      for await (const event of handleGithubTunnel(conn, frame)) {
        if (event.kind === "head") {
          sentHead = true;
          send(conn, {
            t: "res",
            id,
            status: event.status,
            headers: event.headers,
          });
        } else {
          if (!sentHead) {
            // Defensive — proxy should always yield head first.
            send(conn, { t: "res", id, status: 200, headers: {} });
            sentHead = true;
          }
          send(conn, { t: "chunk", id, body: event.body });
        }
      }
      send(conn, { t: "done", id });
    } catch (err) {
      const message = err instanceof Error ? err.message : "github tunnel forward failed";
      logger.warn(
        { sandboxId: conn.sandboxId, id, err: message },
        "reverse-tunnel: github request failed",
      );
      send(conn, { t: "error", id, message });
    }
    return;
  }

  if (path.startsWith("/tunnel/")) {
    // Unknown tunnel path — honest 404 JSON (res + chunk + done).
    send(conn, {
      t: "res",
      id,
      status: 404,
      headers: { "content-type": "application/json" },
    });
    send(conn, {
      t: "chunk",
      id,
      body: JSON.stringify({
        ok: false,
        error: `unknown tunnel path "${path}" — supported: /tunnel/github, /mcp/supabase`,
      }),
    });
    send(conn, { t: "done", id });
    return;
  }

  // MCP routing (GROUP 2 session 2): /mcp/<connector> → the vault-backed
  // MCP executor. This connection is per-sandbox, so the user identity
  // resolves directly from conn.sandboxId. Any inbound Authorization
  // header is ignored — the OAuth token is injected server-side only.
  if (path.startsWith("/mcp/")) {
    try {
      let sentHead = false;
      for await (const event of handleMcpTunnel(conn, frame)) {
        if (event.kind === "head") {
          sentHead = true;
          send(conn, {
            t: "res",
            id,
            status: event.status,
            headers: event.headers,
          });
        } else {
          if (!sentHead) {
            // Defensive — proxy should always yield head first.
            send(conn, { t: "res", id, status: 200, headers: {} });
            sentHead = true;
          }
          send(conn, { t: "chunk", id, body: event.body });
        }
      }
      send(conn, { t: "done", id });
    } catch (err) {
      const message = err instanceof Error ? err.message : "mcp tunnel forward failed";
      logger.warn(
        { sandboxId: conn.sandboxId, id, err: message },
        "reverse-tunnel: mcp request failed",
      );
      send(conn, { t: "error", id, message });
    }
    return;
  }

  const params: ForwardParams = {
    method: frame.method || "POST",
    path: frame.path || "/v1/chat/completions",
    headers: frame.headers || {},
    bodyString: frame.body || "",
  };
  try {
    let sentHead = false;
    for await (const event of forwardToNvidia(params)) {
      if (event.kind === "head") {
        sentHead = true;
        send(conn, {
          t: "res",
          id,
          status: event.status,
          headers: event.headers,
        });
      } else {
        // chunk
        if (!sentHead) {
          // Defensive — forwarder should always yield head first.
          send(conn, { t: "res", id, status: 200, headers: {} });
          sentHead = true;
        }
        send(conn, { t: "chunk", id, body: event.body });
      }
    }
    send(conn, { t: "done", id });
  } catch (err) {
    const message = err instanceof Error ? err.message : "tunnel forward failed";
    logger.warn({ sandboxId: conn.sandboxId, id, err: message }, "reverse-tunnel: request failed");
    send(conn, { t: "error", id, message });
  }
}

// ─── Connection lifecycle ──────────────────────────────────────────────
/** Open a single WS to the VM and wire up the message handlers. */
function dialOnce(conn: ReverseTunnelConnection): void {
  const url = buildWsUrl(conn.signedBaseUrl);
  const secret = proxySecret();
  if (!secret) {
    logger.error(
      { sandboxId: conn.sandboxId },
      "reverse-tunnel: AGENT_PROXY_SECRET not set — cannot auth to the VM",
    );
    return;
  }
  logger.info(
    { sandboxId: conn.sandboxId, url },
    "reverse-tunnel: dialing into VM",
  );
  // Set a connect timeout — the WS upgrade should complete in <5s.
  const ws = new WebSocket(url, {
    headers: { "X-Agent-Token": secret },
    handshakeTimeout: 10_000,
    perMessageDeflate: false,
  });
  conn.ws = ws;

  ws.on("open", () => {
    conn.retries = 0;  // successful connect — reset retry count
    conn.connecting = false;
    logger.info(
      { sandboxId: conn.sandboxId, url },
      "reverse-tunnel: connected to VM (inbound through Daytona proxy)",
    );
    resetIdle(conn);
  });

  ws.on("message", (data, isBinary) => {
    if (isBinary) {
      // Protocol is text-only.
      send(conn, { t: "error", id: "", message: "binary frames not supported" });
      return;
    }
    // Any message resets the idle timer.
    resetIdle(conn);

    let frame: Record<string, unknown>;
    try {
      frame = JSON.parse(data.toString()) as Record<string, unknown>;
    } catch {
      send(conn, { t: "error", id: "", message: "bad frame (invalid JSON)" });
      return;
    }
    const t = typeof frame.t === "string" ? frame.t : "";
    switch (t) {
      case "req": {
        const id = typeof frame.id === "string" ? frame.id : "";
        if (!id) {
          send(conn, { t: "error", id: "", message: "req frame missing id" });
          return;
        }
        // Spawn an independent handler — do NOT await.
        handleReqFrame(conn, frame as {
          id: string;
          method?: string;
          path?: string;
          headers?: Record<string, string>;
          body?: string;
        }).catch((err) => {
          const message = err instanceof Error ? err.message : "req handler crashed";
          send(conn, { t: "error", id, message });
        });
        return;
      }
      case "ping":
        send(conn, { t: "pong" });
        return;
      case "pong":
        // Already handled by resetIdle — nothing else to do.
        return;
      default:
        // Unknown frame types: ignore gracefully (forward-compat).
        return;
    }
  });

  ws.on("error", (err: Error) => {
    clearTimers(conn);
    if (conn.closing) return;
    logger.warn(
      { sandboxId: conn.sandboxId, err: err.message },
      "reverse-tunnel: socket error",
    );
  });

  ws.on("close", () => {
    clearTimers(conn);
    if (conn.closing) {
      // Deliberate teardown — don't reconnect.
      logger.info(
        { sandboxId: conn.sandboxId },
        "reverse-tunnel: closed (deliberate)",
      );
      return;
    }
    conn.ws = null;
    conn.connecting = false;
    // NEVER give up — keep reconnecting forever (see BACKOFF_SEC comment).
    // Render services are on Uptime Robot (never sleep); every close is a
    // transient that recovers on the next dial. The previous cap retired
    // live bridges after 30 closes — this reconnect loop now runs forever.
    const delaySec = BACKOFF_SEC[Math.min(conn.retries, BACKOFF_SEC.length - 1)];
    conn.retries += 1;
    logger.info(
      { sandboxId: conn.sandboxId, delaySec, attempt: conn.retries },
      "reverse-tunnel: closed — scheduling reconnect (no max-retry cap)",
    );
    setTimeout(() => {
      // Re-check that we haven't been retired in the meantime.
      if (conn.closing) return;
      const existing = connections.get(conn.sandboxId);
      if (!existing || existing !== conn) return;
      dialOnce(conn);
    }, delaySec * 1000);
  });
}

// ─── Public API ─────────────────────────────────────────────────────────
/**
 * Ensure a reverse-tunnel connection to the given sandbox is open (or
 * being opened). Idempotent — calling with the same sandbox_id while
 * a connection is live is a no-op. Called by the agent-info endpoint
 * after the signed URL is fetched from the daytona-service.
 *
 * The `signedUrl` argument is the URL returned by the daytona-service's
 * /api/v1/workspace/{sandbox_id}/agent-info endpoint — the same URL
 * the frontend uses for its /ws connection to the VM's orchestrator.
 */
export function ensureReverseTunnel(sandboxId: string, signedUrl: string | null | undefined): void {
  if (!sandboxId || !signedUrl) {
    return;
  }
  const existing = connections.get(sandboxId);
  if (existing) {
    const isOpen = !!existing.ws && existing.ws.readyState === WebSocket.OPEN;
    if (isOpen) {
      // Live connection — no-op. If the signed URL changed (rare — only if
      // the sandbox was recreated), update it so the next reconnect uses
      // the new URL.
      if (existing.signedBaseUrl !== signedUrl) {
        existing.signedBaseUrl = signedUrl;
        logger.info(
          { sandboxId, newUrl: signedUrl },
          "reverse-tunnel: signed URL changed — next reconnect will use the new URL",
        );
      }
      return;
    }
    // WS is dead/closed but the entry lingers in the Map. The previous
    // code was a no-op here, which meant a dropped bridge stayed dead
    // until the backoff timer fired (or forever if the timer had already
    // fired and the entry was stale). Force a fresh dial NOW so the bridge
    // recovers immediately on the next agent-info call.
    if (existing.connecting) {
      // A dial is already in flight (either the initial dial or a
      // scheduled reconnect). Let it complete — don't pile on.
      if (existing.signedBaseUrl !== signedUrl) {
        existing.signedBaseUrl = signedUrl;
      }
      return;
    }
    logger.info(
      { sandboxId },
      "reverse-tunnel: existing entry has a dead WS — forcing fresh dial now",
    );
    existing.signedBaseUrl = signedUrl;  // refresh URL in case it changed
    existing.retries = 0;                 // reset — this is a new attempt
    existing.connecting = true;
    dialOnce(existing);
    return;
  }
  const conn: ReverseTunnelConnection = {
    sandboxId,
    signedBaseUrl: signedUrl,
    ws: null,
    connecting: true,
    closing: false,
    retries: 0,
    idleTimer: null,
    pongTimer: null,
    awaitingPong: false,
  };
  connections.set(sandboxId, conn);
  dialOnce(conn);
}

/**
 * Synchronously check whether a reverse-tunnel connection is live.
 * Used by /api/workspace/agent-info to report the bridge status to the
 * frontend (so the frontend can distinguish "VM is up but bridge is
 * still dialing in" from "VM is up and bridge is ready").
 */
export function isReverseTunnelConnected(sandboxId: string): boolean {
  const conn = connections.get(sandboxId);
  return !!conn && !!conn.ws && conn.ws.readyState === WebSocket.OPEN;
}

/**
 * Deliberately close + retire a reverse-tunnel connection (e.g. when
 * the project is deleted). The reconnect loop will NOT fire after this.
 */
export function disconnectReverseTunnel(sandboxId: string): void {
  const conn = connections.get(sandboxId);
  if (!conn) return;
  conn.closing = true;
  clearTimers(conn);
  if (conn.ws) {
    try {
      conn.ws.close(1000, "retired");
    } catch {
      /* noop */
    }
  }
  connections.delete(sandboxId);
}
