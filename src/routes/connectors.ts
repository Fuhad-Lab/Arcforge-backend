/**
 * Generic connector routes (GROUP 2).
 *
 * Architecture: Frontend → Supabase Edge Function (connector-ops) → here.
 * The backend owns authentication (requireAuth), authorization (per-user
 * capability checks), OAuth token exchange, encrypted credential storage,
 * and task resumption when an agent-initiated request completes.
 *
 * Every agent invocation (Chief Agent OR delegated subagent) declares
 * REQUIRED CAPABILITIES; resolveCapability() maps them to connectors and
 * the vault decides authorization. No provider-specific logic exists
 * outside the registry/connector-oauth — future connectors register
 * without touching the Chief Agent pipeline.
 *
 * Routes (all under /api, mounted in routes/index.ts):
 *   GET  /connectors                      → sanitized list + per-user status
 *   POST /connectors/:id/authorize        → {authorize_url} (state carries
 *                                            user + capability + task resume ctx)
 *   GET  /connectors/callback?code&state  → OAuth redirect target (state-signed,
 *                                            public by design; edge relays)
 *   POST /connectors/:id/disconnect       → revoke
 *   POST /connectors/:id/decline          → agent-initiated request cancelled
 *
 * OAuth round-trip mechanics live in services/connector-oauth.ts (shared
 * with the auth-oauth callback route — the github connector's registered
 * callback edge function is auth-oauth, so its codes land there; see the
 * registry's redirectKind).
 *
 * Tokens: AES-256-GCM at rest in public.connector_connections (RLS enabled,
 * zero policies — service-role only). Refreshed on use (Supabase OAuth tokens
 * are short-lived). NEVER returned to the frontend, never logged, never
 * passed to subagents — subagents receive scoped capability grants only.
 */

import { Router, type IRouter, type Request, type Response } from "express";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";
import {
  CONNECTORS,
  connectorCredentials,
  connectorMetadata,
  getConnector,
  resolveCapability,
} from "../services/connector-registry";
import {
  deleteConnection,
  getConnection,
  getTokens,
  markConnectionStatus,
} from "../services/connector-vault";
import {
  allowedLandingBase,
  buildAuthorizeUrl,
  completeConnectorOAuth,
  mintConnectorState,
  notifySidecarConnector,
  preflightAuthorize,
  safeReturnPath,
} from "../services/connector-oauth";

const router: IRouter = Router();

const FRONTEND_URL = process.env.FRONTEND_URL || "https://forgeyn.com.ng";

/** Status derivation: DB row + token freshness. Sanitized — no token
 *  material, no env names. */
function sanitizedStatus(
  row: Awaited<ReturnType<typeof getConnection>>,
): {
  status: "not_connected" | "connected" | "error" | "expired";
  connected_at?: string;
  account_label?: string;
  scopes?: string;
} {
  if (!row) return { status: "not_connected" };
  if (row.status === "error") return { status: "error", account_label: row.account_label ?? undefined };
  if (row.status !== "connected") return { status: "not_connected" };
  if (row.token_expires_at && new Date(row.token_expires_at).getTime() < Date.now()) {
    return { status: "expired", connected_at: row.connected_at ?? undefined, account_label: row.account_label ?? undefined };
  }
  return {
    status: "connected",
    connected_at: row.connected_at ?? undefined,
    account_label: row.account_label ?? undefined,
    scopes: row.scopes ?? undefined,
  };
}

/** ── OAUTH CALLBACK (provider → edge → here) ────────────────────────
 * PUBLIC BY DESIGN and registered BEFORE requireAuth: the browser lands
 * here via a top-level redirect from the OAuth provider with no Supabase
 * JWT. Security = the HMAC-signed state (see connector-oauth.ts). The
 * github connector's callbacks land on the auth-oauth route instead
 * (that edge function is its registered redirect target) — both entry
 * points funnel into the SAME completion engine. */
router.get("/connectors/callback", async (req: Request, res: Response) => {
  const code = typeof req.query.code === "string" ? req.query.code : "";
  const stateRaw = typeof req.query.state === "string" ? req.query.state : "";
  const redirectUrl = await completeConnectorOAuth(code, stateRaw);
  res.redirect(302, redirectUrl);
});

// Authenticated routes below this line — every connector operation is
// authorized for the caller's user. Subagent capability declarations are
// resolved through the SAME vault (no bypass path exists).
// Path-scoped auth: this router is mounted WITHOUT a path prefix in
// routes/index.ts, so a bare router.use(requireAuth) here would gate EVERY
// /api/* request (it broke the auth-exempt /api/db/templates listing).
// Scope it to this router's own route family instead.
router.use("/connectors", requireAuth);

/** ── LIST ─────────────────────────────────────────────────────────── */
router.get("/connectors", async (req: Request, res: Response) => {
  const userId = req.userId!;
  try {
    const connectors = await Promise.all(
      CONNECTORS.map(async (connector) => {
        const row = await getConnection(userId, connector.id);
        const creds = connectorCredentials(connector);
        return {
          ...connectorMetadata(connector),
          ...sanitizedStatus(row),
          configured: Boolean(creds),
        };
      }),
    );
    res.json({ connectors });
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "connectors: list failed");
    res.status(500).json({ error: "Failed to list connectors" });
  }
});

/** ── AUTHORIZE (start OAuth; supports agent-initiated resume ctx) ──── */
router.post("/connectors/:id/authorize", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const connector = getConnector(String(req.params.id));
  if (!connector) {
    res.status(404).json({ error: "Unknown connector" });
    return;
  }
  const creds = connectorCredentials(connector);
  if (!creds) {
    res.status(503).json({ error: `${connector.name} connector is not configured` });
    return;
  }
  const body = (req.body || {}) as {
    capability?: string;
    reason?: string;
    request_id?: string;
    task_id?: string;
    project_id?: string;
    return_path?: string;
    origin?: string;
  };
  // Validate the requested capability belongs to this connector.
  let capability: string | undefined;
  if (body.capability) {
    const resolved = resolveCapability(body.capability);
    if (!resolved || resolved.connector.id !== connector.id) {
      res.status(400).json({ error: `Capability ${body.capability} does not belong to ${connector.name}` });
      return;
    }
    capability = body.capability;
  }

  const state = mintConnectorState({
    connector: connector.id,
    userId,
    userEmail: req.userEmail ?? null,
    capability,
    requestId: body.request_id,
    taskId: body.task_id,
    projectId: body.project_id,
    returnPath: safeReturnPath(body.return_path),
    // ORIGIN-AWARE LANDING (user fix 2026-09-11): validated at mint time
    // and carried in the HMAC state — the callback returns the user to
    // the origin they started from instead of a hardcoded Render URL
    // (landing elsewhere drops the session cookie and the connect reads
    // as broken on forgeyn.com.ng).
    landingBase: allowedLandingBase(body.origin),
  });

  await markConnectionStatus(userId, connector.id, "connecting").catch(() => undefined);

  const authorizeUrl = buildAuthorizeUrl(connector, creds, state);

  // SUPABASE PRE-FLIGHT (user fix 2026-09-12): Supabase rejects
  // unregistered redirect URIs with a raw 422 JSON page — a dead end.
  // One cheap GET before the browser navigates turns that into an
  // actionable error the connectors page renders as a toast.
  const preflightError = await preflightAuthorize(connector, creds, authorizeUrl);
  if (preflightError) {
    await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
    res.status(502).json({ error: preflightError, redirect_uri_hint: true });
    return;
  }

  res.json({ authorize_url: authorizeUrl });
});

/** ── DISCONNECT ───────────────────────────────────────────────────── */
router.post("/connectors/:id/disconnect", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const connector = getConnector(String(req.params.id));
  if (!connector) {
    res.status(404).json({ error: "Unknown connector" });
    return;
  }
  await deleteConnection(userId, connector.id);
  logger.info({ userId, connector: connector.id }, "connectors: disconnected");
  res.json({ ok: true });
});

/** ── DECLINE (agent-initiated request cancelled by the user) ──────── */
router.post("/connectors/:id/decline", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const connector = getConnector(String(req.params.id));
  if (!connector) {
    res.status(404).json({ error: "Unknown connector" });
    return;
  }
  const body = (req.body || {}) as { request_id?: string; task_id?: string; project_id?: string };
  logger.info(
    { userId, connector: connector.id, requestId: body.request_id ?? null, taskId: body.task_id ?? null },
    "connectors: agent-initiated request declined",
  );
  // Wake the blocked sidecar request_connector call with an honest
  // refusal (same brokering + ownership guard as the resume path).
  if (body.request_id || body.task_id || body.project_id) {
    await notifySidecarConnector(userId, body.project_id, {
      request_id: body.request_id,
      declined: true,
    });
  }
  res.json({ ok: true });
});

/** ── STATUS (single connector; used by capability checks) ─────────── */
router.get("/connectors/:id/status", async (req: Request, res: Response) => {
  const userId = req.userId!;
  const connector = getConnector(String(req.params.id));
  if (!connector) {
    res.status(404).json({ error: "Unknown connector" });
    return;
  }
  const row = await getConnection(userId, connector.id);
  const tokens = row ? await getTokens(userId, connector.id) : null;
  res.json({
    ...connectorMetadata(connector),
    ...sanitizedStatus(row),
    token_available: Boolean(tokens),
  });
});

export default router;
