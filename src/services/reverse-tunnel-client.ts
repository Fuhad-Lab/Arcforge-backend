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
import { logger } from "../lib/logger";

// ─── Configuration ──────────────────────────────────────────────────────
/** Path on the VM's orchestrator (port 9000) where this service dials in. */
const REVERSE_TUNNEL_PATH = "/reverse-tunnel";
/** Max reconnect attempts before giving up. */
const MAX_RETRIES = 30;
/** Backoff schedule (seconds) — caps at the last value. */
const BACKOFF_SEC = [1, 2, 5, 10, 30];
/** Idle ping interval (ms) — if no frame received in this time, ping. */
const IDLE_PING_MS = 60_000;
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
 * Handle a single `req` frame end-to-end: forward to NVIDIA, send `res`
 * + `chunk`(s) + `done` to the VM, or `error` on failure. Never throws
 * to the caller — all errors are converted to `error` frames.
 *
 * This is the SAME logic as `handleReqFrame` in `src/routes/tunnel.ts`
 * — the only difference is that we hold the WS to the VM (vs. the VM
 * holding the WS to us).
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
    if (conn.retries >= MAX_RETRIES) {
      logger.warn(
        { sandboxId: conn.sandboxId, retries: conn.retries },
        "reverse-tunnel: max retries reached — giving up (a future agent-info call will re-trigger)",
      );
      connections.delete(conn.sandboxId);
      return;
    }
    const delaySec = BACKOFF_SEC[Math.min(conn.retries, BACKOFF_SEC.length - 1)];
    conn.retries += 1;
    logger.info(
      { sandboxId: conn.sandboxId, delaySec, attempt: conn.retries },
      "reverse-tunnel: closed — scheduling reconnect",
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
    // Already connected (or trying) — no-op. If the signed URL has
    // changed (rare — only if the sandbox was recreated), update it
    // so the next reconnect uses the new URL.
    if (existing.signedBaseUrl !== signedUrl) {
      existing.signedBaseUrl = signedUrl;
      logger.info(
        { sandboxId, newUrl: signedUrl },
        "reverse-tunnel: signed URL changed — next reconnect will use the new URL",
      );
    }
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
