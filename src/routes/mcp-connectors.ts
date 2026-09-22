/**
 * MCP server connector routes — the MCP Servers tab of the Connectors
 * page (frontend → connector-ops edge → here).
 *
 *   POST /api/connectors/mcp/add          → discover + register + authorize URL
 *                                           (…?api_key=… connects with a
 *                                           pasted bearer key instead — the
 *                                           Render fallback while their
 *                                           client approval is pending)
 *   GET  /api/connectors/mcp/list         → sanitized servers + status
 *   POST /api/connectors/mcp/:id/disconnect → drop row + vault tokens
 *   GET  /api/connectors/mcp/:id/tools    → live tools/list (updates cache)
 *   POST /api/connectors/mcp/:id/reconnect→ re-run discovery + authorize
 *   POST /api/connectors/mcp/:id/connect-key → connect an existing row with
 *                                           a user-pasted API key
 *
 * The OAuth callback itself lands on the SHARED /api/connectors/callback
 * (state.connector = "mcp:<uuid>") — see completeConnectorOAuth's
 * delegation to completeMcpOAuth in services/mcp-proxy.ts.
 *
 * All routes require the caller's JWT (requireAuth — same middleware as
 * the fixed connectors). The mcp-proxy service owns every upstream call;
 * tokens never leave the vault.
 */
import { Router, type IRouter, type Request, type Response } from "express";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";
import { deleteConnection } from "../services/connector-vault";
import { allowedLandingBase } from "../services/connector-oauth";
import {
  addMcpServer,
  connectMcpWithKey,
  deleteServerRow,
  getServerRow,
  listMcpTools,
  listUserServers,
  McpError,
  sanitizeServer,
} from "../services/mcp-proxy";

const router: IRouter = Router();

// Every mcp route is authenticated (mirrors the fixed-connector family).
router.use("/connectors/mcp", requireAuth);

/** Map McpError codes to honest HTTP statuses + user-facing messages. */
function fail(res: Response, err: unknown): void {
  if (err instanceof McpError) {
    const status =
      err.code === "invalid_url" ? 400 : err.code === "internal" ? 500 : 502;
    logger.warn({ code: err.code, detail: err.detail ?? null }, "mcp-routes: add/connect failed");
    res.status(status).json({ error: err.message, code: err.code });
    return;
  }
  logger.error({ err: err instanceof Error ? err.message : "unknown" }, "mcp-routes: unexpected failure");
  res.status(500).json({ error: "An unexpected error interrupted the MCP server operation." });
}

/** ── ADD (discover + register + authorize; or API-key connect) ────── */
router.post("/connectors/mcp/add", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const body = (req.body || {}) as { url?: string; origin?: string; api_key?: string };
  const url = typeof body.url === "string" ? body.url : "";
  if (!url.trim()) {
    res.status(400).json({ error: "The MCP server URL is required." });
    return;
  }
  const apiKey = typeof body.api_key === "string" ? body.api_key : undefined;
  try {
    const landingBase = allowedLandingBase(body.origin);
    const outcome = await addMcpServer(userId, url, landingBase, apiKey);
    res.json({
      server_id: outcome.serverId,
      label: outcome.label,
      ...(outcome.authorizeUrl ? { authorize_url: outcome.authorizeUrl } : {}),
      ...(outcome.connected ? { connected: true, tool_count: outcome.toolCount ?? 0 } : {}),
    });
  } catch (err) {
    fail(res, err);
  }
});

/** ── LIST ─────────────────────────────────────────────────────────── */
router.get("/connectors/mcp/list", async (req: Request, res: Response) => {
  const userId = req.userId!;
  try {
    const servers = await listUserServers(userId);
    res.json({ servers });
  } catch (err) {
    fail(res, err);
  }
});

/** ── DISCONNECT ───────────────────────────────────────────────────── */
router.post("/connectors/mcp/:id/disconnect", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const row = await getServerRow(userId, id);
  if (!row) {
    res.status(404).json({ error: "Unknown MCP server." });
    return;
  }
  // Best-effort revocation before dropping the row (never blocks).
  await deleteConnection(userId, `mcp:${id}`).catch(() => undefined);
  await deleteServerRow(userId, id).catch(() => undefined);
  logger.info({ userId, serverId: id }, "mcp-routes: disconnected");
  res.json({ ok: true });
});

/** ── TOOLS (live tools/list; refreshes the cache) ─────────────────── */
router.get("/connectors/mcp/:id/tools", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const row = await getServerRow(userId, id);
  if (!row) {
    res.status(404).json({ error: "Unknown MCP server." });
    return;
  }
  try {
    const tools = await listMcpTools(row);
    res.json({ tools });
  } catch (err) {
    fail(res, err);
  }
});

/** ── RECONNECT (re-discovery + fresh authorize URL) ───────────────── */
router.post("/connectors/mcp/:id/reconnect", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const body = (req.body || {}) as { origin?: string };
  const row = await getServerRow(userId, id);
  if (!row) {
    res.status(404).json({ error: "Unknown MCP server." });
    return;
  }
  try {
    const outcome = await addMcpServer(userId, row.server_url, allowedLandingBase(body.origin));
    res.json({
      server_id: outcome.serverId,
      label: outcome.label,
      ...(outcome.authorizeUrl ? { authorize_url: outcome.authorizeUrl } : {}),
      ...(outcome.connected ? { connected: true, tool_count: outcome.toolCount ?? 0 } : {}),
    });
  } catch (err) {
    fail(res, err);
  }
});

/** ── CONNECT-KEY (user fix 2026-09-16 — the Render fallback) ────────
 *  Secondary connection option for an EXISTING server row: the user
 *  pastes a standard API key / bearer token, the backend validates it
 *  with a live MCP initialize, stores it encrypted in the vault, and
 *  injects `Authorization: Bearer <key>` into every outgoing MCP agent
 *  request server-side. For providers whose OAuth client registration is
 *  still pending approval (Render), this is THE connection path. */
router.post("/connectors/mcp/:id/connect-key", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const body = (req.body || {}) as { api_key?: string };
  const apiKey = typeof body.api_key === "string" ? body.api_key.trim() : "";
  if (!apiKey) {
    res.status(400).json({ error: "The API key is required.", code: "key_rejected" });
    return;
  }
  try {
    const outcome = await connectMcpWithKey(userId, id, apiKey);
    res.json({
      server_id: outcome.serverId,
      label: outcome.label,
      connected: true,
      tool_count: outcome.toolCount ?? 0,
    });
  } catch (err) {
    fail(res, err);
  }
});

/** ── STATUS (single server) ───────────────────────────────────────── */
router.get("/connectors/mcp/:id/status", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const row = await getServerRow(userId, id);
  if (!row) {
    res.status(404).json({ error: "Unknown MCP server." });
    return;
  }
  // SELF-HEALING (user fix 2026-09-16): derive through the SAME list
  // engine (refresh-on-read + expiry verdict) so the single-server
  // status can never disagree with the list the page renders.
  const servers = await listUserServers(userId);
  const server = servers.find((s) => s.id === id);
  res.json(server ?? sanitizeServer(row));
});

/** ── CALL (one tool on one of the caller's own servers) ────────────────
 *
 * POST /api/connectors/mcp/:id/call  { tool, args }
 *
 * The HTTP twin of the VM tunnel's /mcp/server/<id> frame — the SAME
 * per-user trust boundary (callUserMcpServer scopes to the caller's own
 * vault rows; the vault token never leaves this process) and the SAME
 * executor. Born 2026-09-22 from the Render env-var work: the platform's
 * own tooling (engine-ops "e2b-to-render") needs to drive the user's
 * connected Render MCP from server-side, and the tunnel path only runs
 * inside a live agent VM. Not advertised in the UI — an ops/debug surface.
 */
router.post("/connectors/mcp/:id/call", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const id = String(req.params.id);
  const tool = typeof req.body?.tool === "string" ? req.body.tool.trim() : "";
  const args =
    req.body?.args && typeof req.body.args === "object" && !Array.isArray(req.body.args)
      ? (req.body.args as Record<string, unknown>)
      : {};
  if (!tool) {
    res.status(400).json({ error: "The mcp tool name is required ({ tool, args })." });
    return;
  }
  try {
    const { callUserMcpServer } = await import("../services/mcp-proxy");
    const outcome = await callUserMcpServer(userId, id, tool, args);
    if (outcome.ok) {
      res.json({ ok: true, result: outcome.result });
      return;
    }
    res.status(outcome.needs_connector ? 403 : 400).json({
      ok: false,
      error: outcome.error,
      ...(outcome.needs_connector ? { needs_connector: true } : {}),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "mcp call failure";
    logger.warn({ userId, id, tool, err: message }, "mcp-routes: call failed");
    res.status(500).json({ error: `MCP call failure: ${message}` });
  }
});

export default router;
