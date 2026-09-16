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
  const row = await getServerRow(userId, String(req.params.id));
  if (!row) {
    res.status(404).json({ error: "Unknown MCP server." });
    return;
  }
  res.json(sanitizeServer(row));
});

export default router;
