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
 * outside the registry — future connectors register without touching the
 * Chief Agent pipeline.
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
  mintState,
  upsertConnection,
  verifyState,
  expiryDate,
} from "../services/connector-vault";

const router: IRouter = Router();

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const EDGE_BASE = process.env.EDGE_FUNCTION_BASE_URL || "";

function connectorCallbackUrl(): string {
  if (EDGE_BASE) return `${EDGE_BASE.replace(/\/+$/, "")}/connector-ops`;
  if (SUPABASE_URL) return `${SUPABASE_URL.replace(/\/+$/, "")}/functions/v1/connector-ops`;
  return "https://arcforge-edge.invalid/functions/v1/connector-ops";
}

const FRONTEND_URL = process.env.FRONTEND_URL || "https://arcforge-web.onrender.com";

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
 * JWT. Security = the HMAC-signed state, which this backend minted for an
 * authenticated user at authorize time (carries user + resume context). */
router.get("/connectors/callback", async (req: Request, res: Response) => {
  const code = typeof req.query.code === "string" ? req.query.code : "";
  const stateRaw = typeof req.query.state === "string" ? req.query.state : "";
  const state = verifyState(stateRaw);
  if (!code || !state || state.purpose !== "connector" || !state.connector || !state.userId) {
    res.redirect(302, `${FRONTEND_URL}/connectors?connected=unknown&status=error`);
    return;
  }
  const connector = getConnector(state.connector);
  if (!connector) {
    res.redirect(302, `${FRONTEND_URL}/connectors?connected=${state.connector}&status=error`);
    return;
  }
  const creds = connectorCredentials(connector);
  if (!creds) {
    res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=error&message=not_configured`);
    return;
  }
  const userId = state.userId;

  try {
    let accessToken = "";
    let refreshToken: string | null = null;
    let expiresIn: number | null = null;
    let accountLabel: string | undefined;
    let scopes: string | undefined;
    let githubLogin: string | undefined;

    if (connector.authMethod === "oauth_supabase") {
      // Supabase OAuth token exchange — Basic auth, form-urlencoded body.
      const tokenRes = await fetch("https://api.supabase.com/v1/oauth/token", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Accept: "application/json",
          Authorization: `Basic ${Buffer.from(`${creds.clientId}:${creds.clientSecret}`).toString("base64")}`,
        },
        body: new URLSearchParams({
          grant_type: "authorization_code",
          code,
          redirect_uri: connectorCallbackUrl(),
        }),
      });
      if (!tokenRes.ok) {
        const detail = await tokenRes.text();
        logger.warn(
          { connector: connector.id, status: tokenRes.status, detail: detail.slice(0, 200) },
          "connector-oauth: token exchange failed",
        );
        await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
        res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=error&message=exchange_failed`);
        return;
      }
      const tokenJson = (await tokenRes.json()) as {
        access_token?: string;
        refresh_token?: string;
        expires_in?: number;
        scope?: string;
      };
      accessToken = tokenJson.access_token || "";
      refreshToken = tokenJson.refresh_token || null;
      expiresIn = tokenJson.expires_in ?? null;
      scopes = tokenJson.scope ?? undefined;

      // Resolve the account identity for the connection label (never the
      // token): the OAuth user endpoint is the documented identity call.
      if (accessToken) {
        try {
          const meRes = await fetch("https://api.supabase.com/v1/oauth/user", {
            headers: { Authorization: `Bearer ${accessToken}` },
          });
          if (meRes.ok) {
            const me = (await meRes.json()) as { email?: string; name?: string; user_name?: string };
            accountLabel = me.email || me.name || me.user_name || undefined;
          }
        } catch {
          /* label is best-effort */
        }
      }
    } else {
      // GitHub App user-to-server token exchange.
      const tokenRes = await fetch("https://github.com/login/oauth/access_token", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "User-Agent": "arcforge",
        },
        body: JSON.stringify({
          client_id: creds.clientId,
          client_secret: creds.clientSecret,
          code,
          redirect_uri: connectorCallbackUrl(),
        }),
      });
      const tokenJson = (await tokenRes.json()) as {
        access_token?: string;
        refresh_token?: string;
        expires_in?: number;
        error?: string;
      };
      accessToken = tokenJson.access_token || "";
      refreshToken = tokenJson.refresh_token || null;
      expiresIn = tokenJson.expires_in ?? null;
      if (!accessToken) {
        logger.warn({ err: tokenJson.error }, "connector-oauth: github exchange failed");
        await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
        res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=error&message=exchange_failed`);
        return;
      }
      // Label with the resolved GitHub login.
      const meRes = await fetch("https://api.github.com/user", {
        headers: {
          Authorization: `Bearer ${accessToken}`,
          Accept: "application/vnd.github+json",
          "User-Agent": "arcforge",
        },
      });
      if (meRes.ok) {
        const me = (await meRes.json()) as { login?: string };
        accountLabel = me.login || undefined;
        githubLogin = me.login || undefined;
      }
    }

    if (!accessToken) {
      await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
      res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=error&message=no_token`);
      return;
    }

    await upsertConnection(userId, connector.id, {
      accessToken,
      refreshToken,
      expiresAt: expiryDate(expiresIn),
    }, { scopes, accountLabel, githubLogin });

    logger.info(
      { userId, connector: connector.id, capability: state.capability ?? null, hasTask: Boolean(state.taskId) },
      "connector-oauth: connected (token stored encrypted; value never logged)",
    );

    // TODO(GROUP 2 session 2): when state.taskId/sandboxId are present,
    // POST <vm-url>/internal/connectors (X-VM-Token) so the sidecar wakes
    // the blocked request_connector tool call and resumes the paused task.
    // The state context is preserved above (requestId/taskId/projectId).

    res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=ok`);
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "connector-oauth: callback error");
    await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
    res.redirect(302, `${FRONTEND_URL}/connectors?connected=${connector.id}&status=error&message=internal`);
  }
});

// Authenticated routes below this line — every connector operation is
// authorized for the caller's user. Subagent capability declarations are
// resolved through the SAME vault (no bypass path exists).
router.use(requireAuth);

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

  const state = mintState({
    purpose: "connector",
    connector: connector.id,
    userId,
    userEmail: req.userEmail ?? null,
    capability,
    requestId: body.request_id,
    taskId: body.task_id,
    projectId: body.project_id,
  });

  await markConnectionStatus(userId, connector.id, "connecting").catch(() => undefined);

  let authorizeUrl: string;
  if (connector.authMethod === "oauth_supabase") {
    // Supabase OAuth: scopes are configured on the OAuth app itself
    // (the scope query param is deprecated per current docs).
    authorizeUrl =
      `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(creds.clientId)}` +
      `&redirect_uri=${encodeURIComponent(connectorCallbackUrl())}` +
      `&response_type=code&state=${encodeURIComponent(state)}`;
  } else {
    // GitHub App (user-to-server OAuth). Permissions are baked into the
    // app configuration; no scope parameter.
    authorizeUrl =
      `https://github.com/login/oauth/authorize?client_id=${encodeURIComponent(creds.clientId)}` +
      `&redirect_uri=${encodeURIComponent(connectorCallbackUrl())}` +
      `&state=${encodeURIComponent(state)}`;
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
  // TODO(GROUP 2 session 2): notify the sidecar /internal/connectors with
  // {declined: true} so the blocked agent resumes with an honest refusal.
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
