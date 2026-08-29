/**
 * /api/tunnel — Inbound Reverse Proxy Tunnel WebSocket endpoint.
 *
 * WHY THIS EXISTS
 * ───────────────
 * Daytona's EU-region sandbox egress firewall drops outbound TLS to
 * *.nvidia.com (the AI provider) AND to *.onrender.com (where this
 * backend lives). The In-VM AI orchestrator therefore cannot call
 * NVIDIA directly, NOR can it call the backend's existing public
 * `/api/llm/chat` HTTPS proxy.
 *
 * The fix: the VM opens a long-lived WebSocket to `/api/tunnel` on
 * this backend (WS over TLS from the VM's POV is one connection that
 * the filter happens to allow). The VM then sends JSON text frames
 * describing HTTP requests it wants to make to NVIDIA; this backend
 * forwards them and streams responses back down the WS. Daytona's
 * filter only ever sees the VM talking to the backend over WS — never
 * any NVIDIA traffic. The backend injects the NVIDIA API key
 * server-side; the key never enters the VM.
 *
 * PATH ROUTING (2026-08-28 + GROUP 2 session 2): every `req` frame used
 *   to go to forwardToNvidia. Now the frame `path` selects a handler:
 *   /tunnel/github → services/github-proxy.ts (sandboxId→user→PAT;
 *                   api.github.com REST + workspace sync — the PAT
 *                   never enters the VM)
 *   /mcp/supabase  → services/supabase-mcp.ts (sandboxId→project→user
 *                   → vault token → official Supabase MCP server; the
 *                   OAuth token never enters the VM)
 *   /tunnel/*      → 404 JSON (unknown tunnel path)
 *   anything else  → forwardToNvidia (unchanged — /v1/*, /vlm/* and
 *                   legacy bare paths)
 * The routing mirrors services/reverse-tunnel-client.ts exactly (the
 * two handlers are intentionally duplicated — one for the VM→backend
 * dial, one for the backend→VM reverse dial).
 *
 * PROTOCOL (shared with the VM-side tunnel client — match EXACTLY)
 * ─────────────────────────────────────────────────────────────────
 * Connection: WS upgrade to `/api/tunnel` with header
 *   `X-Agent-Token: <AGENT_PROXY_SECRET>`. Server validates the token
 *   (401 + no upgrade if wrong/missing). `?token=` query fallback is
 *   also accepted for clients that can't set headers on WS upgrade.
 *
 * All frames are JSON TEXT frames (never binary — keeps the VM parser
 * trivial). Frame shapes:
 *
 *   Client→Server:
 *     { "t":"req", "id":"<uuid>", "method":"POST",
 *       "path":"/v1/chat/completions", "headers":{...}, "body":"<str>",
 *       "sandboxId":"<optional — caller identity for /tunnel/github>" }
 *     { "t":"ping" }
 *
 *   Server→Client:
 *     { "t":"res",   "id":"<uuid>", "status":200, "headers":{...} }
 *     { "t":"chunk", "id":"<uuid>", "body":"<str chunk>" }
 *     { "t":"done",  "id":"<uuid>" }
 *     { "t":"error", "id":"<uuid>", "message":"..." }
 *     { "t":"pong" }
 *
 * Unknown `t` values are ignored gracefully (forward-compat).
 *
 * AUTH
 * ────
 * The shared secret `process.env.AGENT_PROXY_SECRET` is the same one
 * the existing `/api/llm/chat` HTTP proxy uses (X-Agent-Token), so a
 * VM provisioned with its token can use either transport.
 *
 * CONCURRENCY
 * ───────────
 * The server is purely reactive — no in-flight bookkeeping is needed.
 * Each `req` frame spawns an independent async handler keyed by the
 * frame's `id`. Multiple requests can be in flight at once on the same
 * WS connection; their response frames are tagged with the matching
 * `id` so the VM can route them.
 */
import type { Server, IncomingMessage } from "http";
import { WebSocketServer, WebSocket } from "ws";
import { logger } from "../lib/logger";
import { forwardToNvidia } from "../services/nvidia-forwarder";
import { handleGithubTunnel } from "../services/github-proxy";
import { handleMcpTunnel } from "../services/supabase-mcp";

// ─── Tunnel path ────────────────────────────────────────────────────────
const TUNNEL_PATH = "/api/tunnel";

// ─── Keepalive timers ───────────────────────────────────────────────────
/** Send a `ping` if no frame received for this long. */
const IDLE_PING_MS = 60_000;
/** Close the socket if no `pong` (or any frame) arrives this long after a ping. */
const PONG_TIMEOUT_MS = 30_000;

// ─── Frame shapes ───────────────────────────────────────────────────────
type AnyFrame = Record<string, unknown>;

interface ReqFrame {
  t: "req";
  id: string;
  method: string;
  path: string;
  headers: Record<string, string>;
  body: string;
  /** Optional caller identity for /tunnel/github (the direct VM→backend
   *  dial carries no per-sandbox binding — the frame may supply one). */
  sandboxId?: string;
}

// ─── Auth ────────────────────────────────────────────────────────────────
/**
 * Validate the X-Agent-Token (or ?token=) on the WS upgrade request.
 * Returns true if the upgrade should proceed.
 */
function isAuthorized(req: IncomingMessage): boolean {
  const expected = process.env.AGENT_PROXY_SECRET || "";
  if (!expected) {
    logger.error(
      "tunnel: AGENT_PROXY_SECRET not set on the server — refusing all connections",
    );
    return false;
  }

  // 1) Header (preferred).
  const headerToken = req.headers["x-agent-token"];
  if (typeof headerToken === "string" && headerToken === expected) return true;

  // 2) ?token= query fallback (for clients that can't set upgrade headers).
  const url = req.url || "";
  const qIdx = url.indexOf("?");
  if (qIdx >= 0) {
    const search = new URLSearchParams(url.slice(qIdx + 1));
    const qToken = search.get("token");
    if (typeof qToken === "string" && qToken === expected) return true;
  }

  return false;
}

// ─── Send helpers ───────────────────────────────────────────────────────
function send(ws: WebSocket, frame: AnyFrame): void {
  // NEVER use binary frames — the VM parser expects text only.
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify(frame));
  }
}

// ─── Per-request handler ───────────────────────────────────────────────
/**
 * Handle a single `req` frame end-to-end, ROUTED BY PATH:
 *   /tunnel/github → GitHub proxy (user PAT injected server-side)
 *   /mcp/supabase  → Supabase MCP executor (vault OAuth token injected
 *                    server-side — GROUP 2 session 2)
 *   /tunnel/*      → 404 JSON
 *   else           → NVIDIA forwarder (unchanged legacy behavior)
 *
 * Sends `res` + `chunk`(s) + `done` to the VM, or `error` on failure.
 * Never throws to the caller — all errors are converted to `error`
 * frames. Mirrors handleReqFrame in services/reverse-tunnel-client.ts
 * (intentional duplication — keep in sync).
 */
async function handleReqFrame(
  ws: WebSocket,
  frame: ReqFrame,
  connSandbox: { sandboxId: string },
): Promise<void> {
  const id = frame.id || "";
  if (!id) {
    send(ws, { t: "error", id: "", message: "req frame missing id" });
    return;
  }

  // PATH ROUTING — strip the query string before prefix matching (the
  // VM-side client appends ?query to the path).
  const path = (frame.path || "").split("?")[0];

  if (path.startsWith("/tunnel/github")) {
    // This connection is the shared direct dial (no per-sandbox
    // binding of its own) — the sandbox identity comes from the frame
    // (or is remembered from a previous frame on this connection).
    const sandboxId =
      (typeof frame.sandboxId === "string" && frame.sandboxId) || connSandbox.sandboxId;
    if (!sandboxId) {
      send(ws, {
        t: "res",
        id,
        status: 200,
        headers: { "content-type": "application/json" },
      });
      send(ws, {
        t: "chunk",
        id,
        body: JSON.stringify({
          ok: false,
          error:
            "sandbox identity missing on this tunnel connection — include a sandboxId field in the req frame",
        }),
      });
      send(ws, { t: "done", id });
      return;
    }
    try {
      let sentHead = false;
      for await (const event of handleGithubTunnel({ sandboxId }, frame)) {
        if (event.kind === "head") {
          sentHead = true;
          send(ws, { t: "res", id, status: event.status, headers: event.headers });
        } else {
          if (!sentHead) {
            // Defensive: proxy should always yield head first.
            send(ws, { t: "res", id, status: 200, headers: {} });
            sentHead = true;
          }
          send(ws, { t: "chunk", id, body: event.body });
        }
      }
      send(ws, { t: "done", id });
    } catch (err) {
      const message = err instanceof Error ? err.message : "github tunnel forward failed";
      logger.warn({ id, sandboxId, err: message }, "tunnel: github request failed");
      send(ws, { t: "error", id, message });
    }
    return;
  }

  if (path.startsWith("/tunnel/")) {
    // Unknown tunnel path — honest 404 JSON (res + chunk + done).
    send(ws, {
      t: "res",
      id,
      status: 404,
      headers: { "content-type": "application/json" },
    });
    send(ws, {
      t: "chunk",
      id,
      body: JSON.stringify({
        ok: false,
        error: `unknown tunnel path "${path}" — supported: /tunnel/github, /mcp/supabase`,
      }),
    });
    send(ws, { t: "done", id });
    return;
  }

  // MCP routing (GROUP 2 session 2): /mcp/<connector> → the vault-backed
  // MCP executor. The inbound Authorization header (already stripped by
  // the VM-side client) is irrelevant here — the user's OAuth token is
  // injected server-side ONLY, inside callSupabaseMcpTool.
  if (path.startsWith("/mcp/")) {
    const sandboxId =
      (typeof frame.sandboxId === "string" && frame.sandboxId) || connSandbox.sandboxId;
    if (!sandboxId) {
      send(ws, {
        t: "res",
        id,
        status: 403,
        headers: { "content-type": "application/json" },
      });
      send(ws, {
        t: "chunk",
        id,
        body: JSON.stringify({
          ok: false,
          error: "no project owner for this sandbox — include a sandboxId field in the req frame",
        }),
      });
      send(ws, { t: "done", id });
      return;
    }
    try {
      let sentHead = false;
      for await (const event of handleMcpTunnel({ sandboxId }, frame)) {
        if (event.kind === "head") {
          sentHead = true;
          send(ws, { t: "res", id, status: event.status, headers: event.headers });
        } else {
          if (!sentHead) {
            send(ws, { t: "res", id, status: 200, headers: {} });
            sentHead = true;
          }
          send(ws, { t: "chunk", id, body: event.body });
        }
      }
      send(ws, { t: "done", id });
    } catch (err) {
      const message = err instanceof Error ? err.message : "mcp tunnel forward failed";
      logger.warn({ id, sandboxId, err: message }, "tunnel: mcp request failed");
      send(ws, { t: "error", id, message });
    }
    return;
  }

  try {
    let sentHead = false;
    for await (const event of forwardToNvidia({
      method: frame.method,
      path: frame.path,
      headers: frame.headers || {},
      bodyString: frame.body || "",
    })) {
      if (event.kind === "head") {
        sentHead = true;
        send(ws, {
          t: "res",
          id,
          status: event.status,
          headers: event.headers,
        });
      } else {
        // chunk
        if (!sentHead) {
          // Defensive: forwarder should always yield head first, but if
          // it didn't (e.g. a generator change), send a synthetic 200
          // head so the VM HTTP parser doesn't choke.
          send(ws, {
            t: "res",
            id,
            status: 200,
            headers: {},
          });
          sentHead = true;
        }
        send(ws, { t: "chunk", id, body: event.body });
      }
    }
    send(ws, { t: "done", id });
  } catch (err) {
    const message = err instanceof Error ? err.message : "tunnel forward failed";
    logger.warn({ id, err: message }, "tunnel: request failed");
    send(ws, { t: "error", id, message });
  }
}

// ─── Connection lifecycle ──────────────────────────────────────────────
interface LivenessState {
  /** Timer that fires when we've been idle too long → send a ping. */
  idleTimer: ReturnType<typeof setTimeout> | null;
  /** Timer that fires when a ping has gone unanswered for too long. */
  pongTimer: ReturnType<typeof setTimeout> | null;
  /** True after we've sent a ping and are awaiting a pong. */
  awaitingPong: boolean;
  /** True while the socket is open. */
  closed: boolean;
}

function resetIdle(state: LivenessState, ws: WebSocket): void {
  if (state.idleTimer) clearTimeout(state.idleTimer);
  // Any frame (including a pong) means the connection is alive —
  // cancel an outstanding pong wait.
  if (state.pongTimer) {
    clearTimeout(state.pongTimer);
    state.pongTimer = null;
  }
  state.awaitingPong = false;

  if (state.closed) return;
  state.idleTimer = setTimeout(() => {
    if (state.closed) return;
    state.awaitingPong = true;
    send(ws, { t: "ping" });
    // If we don't see ANY frame back within PONG_TIMEOUT_MS, the
    // connection is dead — close it.
    state.pongTimer = setTimeout(() => {
      if (state.closed) return;
      logger.info("tunnel: no pong within timeout — terminating connection");
      try {
        ws.close(1011, "pong timeout");
      } catch {
        /* noop */
      }
      // Hard terminate if close didn't take effect.
      setTimeout(() => {
        if (!state.closed) ws.terminate();
      }, 1_000);
    }, PONG_TIMEOUT_MS);
  }, IDLE_PING_MS);
}

function clearLiveness(state: LivenessState): void {
  if (state.idleTimer) {
    clearTimeout(state.idleTimer);
    state.idleTimer = null;
  }
  if (state.pongTimer) {
    clearTimeout(state.pongTimer);
    state.pongTimer = null;
  }
}

// ─── Attach ────────────────────────────────────────────────────────────
/**
 * Attach the tunnel WebSocket server to the existing HTTP server.
 *
 * Wired in `src/app.ts` alongside the existing `/ws` (websocket-sync)
 * server. Does NOT bind a new port — both WS servers share the same
 * http server created by `createServer(app)` in app.ts.
 */
export function attachTunnelServer(httpServer: Server): void {
  const wss = new WebSocketServer({
    noServer: true, // we handle the upgrade ourselves to gate on auth.
  });

  // Handle the HTTP→WS upgrade manually so we can 401 + reject before
  // the socket is upgraded. (noServer:true means ws won't auto-handle
  // 'upgrade' on the http server.)
  httpServer.on("upgrade", (req, socket, head) => {
    // Only intercept OUR path. Other paths (e.g. /ws) are handled by
    // the websocket-sync WSS (which uses { server } mode and will
    // itself reject unknown paths, but we leave it alone).
    const path = (req.url || "").split("?")[0];
    if (path !== TUNNEL_PATH) {
      return; // let other WS servers handle it.
    }

    if (!isAuthorized(req)) {
      logger.warn(
        { path, ip: req.socket.remoteAddress },
        "tunnel: rejected upgrade (bad/missing token)",
      );
      // Respond 401 in plain HTTP (no upgrade). The VM installer uses
      // this status to know the token is wrong.
      socket.write(
        "HTTP/1.1 401 Unauthorized\r\n" +
          "Content-Type: application/json\r\n" +
          "Connection: close\r\n\r\n" +
          '{"error":"unauthorized"}\n',
      );
      try {
        socket.destroy();
      } catch {
        /* noop */
      }
      return;
    }

    // Auth ok — let ws complete the upgrade.
    wss.handleUpgrade(req, socket, head, (ws) => {
      wss.emit("connection", ws, req);
    });
  });

  wss.on("connection", (ws: WebSocket, req: IncomingMessage) => {
    const state: LivenessState = {
      idleTimer: null,
      pongTimer: null,
      awaitingPong: false,
      closed: false,
    };
    // Caller identity for /tunnel/github — this shared dial has no
    // per-sandbox binding of its own, so the sandboxId is learned from
    // req frames that carry one (and remembered for the connection).
    const connSandbox: { sandboxId: string } = { sandboxId: "" };

    logger.info(
      { ip: req.socket.remoteAddress },
      "tunnel: client connected",
    );
    resetIdle(state, ws);

    ws.on("message", (data, isBinary) => {
      // Ignore binary frames — the protocol is text-only.
      if (isBinary) {
        send(ws, { t: "error", id: "", message: "binary frames not supported" });
        return;
      }

      // Any message resets the idle timer.
      resetIdle(state, ws);

      let frame: AnyFrame;
      try {
        frame = JSON.parse(data.toString()) as AnyFrame;
      } catch {
        send(ws, { t: "error", id: "", message: "bad frame (invalid JSON)" });
        return;
      }

      const t = typeof frame.t === "string" ? frame.t : "";
      switch (t) {
        case "req": {
          // Spawn an independent handler — do NOT await (the next
          // frame must not block on NVIDIA's latency).
          const id = typeof frame.id === "string" ? frame.id : "";
          if (!id) {
            send(ws, { t: "error", id: "", message: "req frame missing id" });
            return;
          }
          // Remember an explicitly-supplied sandbox identity for later
          // /tunnel/github frames on this connection.
          const frameSandboxId =
            typeof frame.sandboxId === "string" ? frame.sandboxId : "";
          if (frameSandboxId) connSandbox.sandboxId = frameSandboxId;
          handleReqFrame(ws, frame as unknown as ReqFrame, connSandbox).catch((err) => {
            const message =
              err instanceof Error ? err.message : "req handler crashed";
            send(ws, { t: "error", id, message });
          });
          return;
        }
        case "ping":
          send(ws, { t: "pong" });
          return;
        case "pong":
          // Already handled by resetIdle — nothing else to do.
          return;
        // Unknown frame types: ignore gracefully (forward-compat).
        default:
          return;
      }
    });

    ws.on("close", () => {
      state.closed = true;
      clearLiveness(state);
      logger.info("tunnel: client disconnected");
    });

    ws.on("error", (err) => {
      state.closed = true;
      clearLiveness(state);
      logger.warn({ err: err.message }, "tunnel: socket error");
    });
  });

  logger.info({ path: TUNNEL_PATH }, "tunnel: WebSocket server attached");
}
