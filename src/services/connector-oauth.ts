/**
 * Shared connector OAuth engine — GROUP 2.
 *
 * ONE implementation of the provider round-trip, used by BOTH callback
 * entry points (the edge function that receives the provider's redirect
 * depends on the connector's registered callback — see redirectKind in
 * connector-registry):
 *   • GET /api/connectors/callback   ← connector-ops edge (supabase flow)
 *   • GET /api/auth/github/callback  ← auth-oauth edge (github flow, when
 *     the HMAC-signed state's purpose is "connector")
 *
 * The authorize URL builder and the pre-flight check (supabase only) live
 * here too, so the route files stay thin and the two entry points can
 * never drift apart.
 *
 * 2026-09-12 (user fixes, both diagnosed live):
 *   • GitHub: the GitHub App client secret in env is INVALID
 *     ("incorrect_client_credentials"), so the connector now uses the
 *     VERIFIED sign-in OAuth App pair with explicit repo scopes — see
 *     connector-registry.
 *   • Supabase: the OAuth app's registered redirect URI list does not
 *     include our callback (authorize answers 422 "redirect_uri not
 *     allowed"), so authorize pre-flights the URL and returns an
 *     actionable error instead of dropping the user on a raw JSON page.
 */

import { logger } from "../lib/logger";
import { getProjectRow } from "../lib/project-lookup";
import { getAgentInfo } from "./daytona-workspace";
import {
  type ConnectorDefinition,
  connectorCredentials,
  getConnector,
} from "./connector-registry";
import {
  type OAuthState,
  expiryDate,
  getTokens,
  markConnectionStatus,
  mintState,
  rotateTokens,
  upsertConnection,
  verifyState,
} from "./connector-vault";
import { completeMcpOAuth } from "./mcp-proxy";

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const EDGE_BASE = process.env.EDGE_FUNCTION_BASE_URL || "";
const FRONTEND_URL = process.env.FRONTEND_URL || "https://studio.forgeyn.com";

/** In-VM sidecar delivery timeout (local HTTP inside the VM via the
 *  preview URL — same budget as workspace.ts's secrets route). */
const SIDECAR_NOTIFY_TIMEOUT_MS = 20_000;

/** Resolve the redirect_uri for a connector: the edge function that is
 *  REGISTERED as the provider's callback URL. authorize + exchange MUST
 *  use the same value (both providers enforce exact redirect_uri
 *  consistency across the round-trip). */
export function connectorRedirectUri(connector: ConnectorDefinition): string {
  const base = (EDGE_BASE || (SUPABASE_URL ? `${SUPABASE_URL.replace(/\/+$/, "")}/functions/v1` : "")).replace(/\/+$/, "");
  if (!base) {
    return connector.redirectKind === "auth-oauth"
      ? "https://arcforge-edge.invalid/functions/v1/auth-oauth"
      : "https://arcforge-edge.invalid/functions/v1/connector-ops";
  }
  return `${base}/${connector.redirectKind}`;
}

/** ORIGIN-AWARE LANDING (user fix 2026-09-11): the OAuth round-trip must
 *  return users to the origin they started from. studio.forgeyn.com is
 *  the canonical public domain (the frontend hardcodes nothing — it sends
 *  window.location.origin on authorize); extra origins can be allow
 *  listed without a redeploy via the FRONTEND_ORIGINS env var
 *  (comma-separated absolute origins, no trailing slash). */
const CANONICAL_FRONTEND_ORIGINS = [
  "https://studio.forgeyn.com",
  // legacy origin — stays allowed during the 2026-09 domain migration
  "https://forgeyn.com.ng",
  "https://www.forgeyn.com.ng",
  ...(process.env.FRONTEND_ORIGINS || "")
    .split(",")
    .map((s) => s.trim())
    .filter(Boolean),
];

/** Validate a caller-supplied origin for post-OAuth landing. Accepted:
 *  the canonical public domain(s), the configured FRONTEND_URL, any
 *  FRONTEND_ORIGINS entry, and the platform/dev suffix hosts. Anything
 *  else falls back to FRONTEND_URL — never an open redirect. */
export function allowedLandingBase(raw: string | undefined | null): string {
  if (!raw) return FRONTEND_URL;
  try {
    const url = new URL(raw);
    const origin = url.origin;
    if (
      (url.protocol === "https:" && CANONICAL_FRONTEND_ORIGINS.includes(origin)) ||
      (url.protocol === "https:" && origin === new URL(FRONTEND_URL).origin)
    ) {
      return origin;
    }
    const host = url.hostname;
    if (
      (url.protocol === "https:" &&
        (host.endsWith(".onrender.com") || host.endsWith(".arcforge.app") || host.endsWith(".vercel.app"))) ||
      (url.protocol === "http:" && (host === "localhost" || host === "127.0.0.1"))
    ) {
      return origin;
    }
  } catch {
    /* malformed — fall through */
  }
  return FRONTEND_URL;
}

/** Safe post-connect landing path (GROUP 3 import flow): a RELATIVE path
 *  on the frontend origin only — starts with "/", not "//" (protocol
 *  relative), no scheme/host, bounded length. Returns undefined otherwise. */
export function safeReturnPath(raw: unknown): string | undefined {
  if (typeof raw !== "string") return undefined;
  const trimmed = raw.trim();
  if (!trimmed || trimmed.length > 128) return undefined;
  if (!trimmed.startsWith("/") || trimmed.startsWith("//") || trimmed.includes(":\\")) {
    return undefined;
  }
  try {
    const url = new URL(trimmed, "https://arcforge.invalid");
    // Only same-origin paths survive (URL() resolves ?query/#hash fine).
    if (url.origin !== "https://arcforge.invalid") return undefined;
    return trimmed;
  } catch {
    return undefined;
  }
}

/** Post-connect landing base: the state's validated landingBase (the
 *  origin the user started the connect from — see allowedLandingBase),
 *  falling back to FRONTEND_URL. The path stays same-origin. */
function landing(state: { landingBase?: string; returnPath?: string }): string {
  const base = state.landingBase || FRONTEND_URL;
  return `${base}${safeReturnPath(state.returnPath) || "/connectors"}`;
}

/** Mint the HMAC state for a connector authorization (route-level helper
 *  so both entry points share the same shape). */
export function mintConnectorState(input: Omit<OAuthState, "nonce" | "exp" | "purpose" | "connector"> & {
  connector: string;
}): string {
  return mintState({ purpose: "connector", ...input });
}

/** Build the provider authorize URL for a connector. */
export function buildAuthorizeUrl(
  connector: ConnectorDefinition,
  creds: { clientId: string; clientSecret: string },
  state: string,
): string {
  const redirectUri = connectorRedirectUri(connector);
  if (connector.authMethod === "oauth_supabase") {
    // Supabase OAuth: scopes are configured on the OAuth app itself
    // (the scope query param is deprecated per current docs).
    return (
      `https://api.supabase.com/v1/oauth/authorize?client_id=${encodeURIComponent(creds.clientId)}` +
      `&redirect_uri=${encodeURIComponent(redirectUri)}` +
      `&response_type=code&state=${encodeURIComponent(state)}`
    );
  }
  // GitHub (classic OAuth App AND GitHub App): same authorize endpoint.
  // Classic OAuth Apps request their scopes per-authorization; GitHub
  // App permissions are baked into the app configuration.
  let url =
    `https://github.com/login/oauth/authorize?client_id=${encodeURIComponent(creds.clientId)}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&state=${encodeURIComponent(state)}`;
  if (connector.authMethod === "oauth_github" && connector.authorizeScopes) {
    url += `&scope=${encodeURIComponent(connector.authorizeScopes)}`;
  }
  return url;
}

/** SUPABASE PRE-FLIGHT (user fix 2026-09-12): Supabase validates the
 *  redirect_uri at the AUTHORIZE step and rejects unregistered values
 *  with a raw 422 JSON page — a dead end the user lands on with no
 *  explanation. A cheap GET of the authorize URL (no code is issued, the
 *  state is never consumed) detects this BEFORE the browser navigates:
 *  2xx/3xx = the URI is registered, 4xx = actionable error. Returns null
 *  when the flight passes, or a user-facing error message when it
 *  doesn't.
 *
 *  2026-09-11 (follow-up): the provider's 422 body has TWO distinct
 *  shapes — "Unrecognized client_id" (the app was deleted / the env
 *  points at a ghost) and "redirect_uri not allowed" (the app exists
 *  but its registered Redirect URLs don't include ours). Both are
 *  dashboard-side registration problems the backend cannot heal, so the
 *  message now says EXACTLY where to click (organization → OAuth Apps —
 *  there is no "Account → Developer settings" page in the current
 *  dashboard) and fingerprints the app by the first 8 chars of its
 *  client ID so the right app is edited (an edit on a different app
 *  silently does nothing). The client ID is a public identifier, never
 *  a secret. */
export async function preflightAuthorize(
  connector: ConnectorDefinition,
  creds: { clientId: string; clientSecret: string },
  authorizeUrl: string,
): Promise<string | null> {
  if (connector.authMethod !== "oauth_supabase") return null;
  try {
    const res = await fetch(authorizeUrl, {
      method: "GET",
      redirect: "manual",
      headers: { "User-Agent": "arcforge-preflight", Accept: "application/json" },
      signal: AbortSignal.timeout(10_000),
    });
    if (res.status >= 400) {
      const body = await res.text().catch(() => "");
      logger.warn(
        { connector: connector.id, status: res.status, detail: body.slice(0, 200) },
        "connector-oauth: authorize pre-flight rejected the redirect_uri",
      );
      const redirectUri = connectorRedirectUri(connector);
      const idHint = creds.clientId.slice(0, 8);
      if (body.includes("Unrecognized client_id")) {
        return (
          `Supabase does not recognize this connector's OAuth app (Client ID starts with ` +
          `"${idHint}") — it may have been deleted. In the Supabase dashboard open your ` +
          `organization → OAuth Apps, recreate the app with this Redirect URL, then put its ` +
          `new client ID and secret in the SUPABASE_OAUTH_CLIENT_ID / ` +
          `SUPABASE_OAUTH_CLIENT_SECRET environment variables: ${redirectUri}`
        );
      }
      return (
        `Supabase rejected this OAuth app's redirect URI (the provider returned status ` +
        `${res.status}). In the Supabase dashboard open your organization → OAuth Apps, ` +
        `open the app whose Client ID starts with "${idHint}" (verify it — editing a ` +
        `different app has no effect), add this exact Redirect URL to its list, then click ` +
        `Update: ${redirectUri}`
      );
    }
  } catch (err) {
    logger.warn(
      { err: err instanceof Error ? err.message : "unknown" },
      "connector-oauth: authorize pre-flight unreachable (continuing — the browser will show the provider's own error)",
    );
    /* Network hiccup — do not block the flow on a failed check. */
  }
  return null;
}

/** ── TOKEN REFRESH ENGINE (user fix 2026-09-16 — the disconnect cure) ──
 *  LIVE-DIAGNOSED ROOT CAUSE: GitHub connector tokens expire after 8h
 *  (the sign-in OAuth App has "Expire user authorization tokens" ON —
 *  verified in the vault: connected 17:37, token_expires_at 01:37) and
 *  Supabase OAuth tokens after 24h, but NOTHING refreshed them on the
 *  read path — the Connectors page derived status purely from the stale
 *  token_expires_at and flipped healthy connections to "Reconnect
 *  required" while a valid refresh token sat unused in the vault.
 *
 *  This engine renews a connection's tokens with the provider BEFORE
 *  any status is derived (list, status, and every consumer), rotating
 *  the fresh pair back into the vault. Outcomes:
 *   • fresh    — token still valid (or has no expiry), nothing done;
 *   • rotated  — provider issued a new pair, vault updated;
 *   • dead     — provider DEFINITIVELY rejected the refresh token
 *               (invalid_grant & friends) → the row is marked "error"
 *               so the card honestly asks for a reconnect;
 *   • skipped  — no refresh token / provider unreachable (transient)
 *               → the caller derives status from what is stored.
 *
 *  GitHub refresh tokens are SINGLE-USE and rotate on every refresh —
 *  the new one is persisted immediately with the new access token. */
export type ConnectionRefreshOutcome =
  | { kind: "fresh" }
  | { kind: "rotated" }
  | { kind: "dead" }
  | { kind: "skipped" };

/** Refresh window: renew when the access token expires within this. */
const REFRESH_WINDOW_MS = 5 * 60 * 1000;

/** In-flight refresh dedup: GitHub refresh tokens are SINGLE-USE — two
 *  concurrent readers (the list route fans out per connector; list +
 *  status can overlap) must share ONE provider round-trip or the loser
 *  burns the rotated token and the connection reads dead. Keyed by
 *  user+connector; the entry clears when the round-trip settles. */
const inflightRefreshes = new Map<string, Promise<ConnectionRefreshOutcome>>();

export function refreshConnectionIfNeeded(
  userId: string,
  connectorId: string,
): Promise<ConnectionRefreshOutcome> {
  const key = `${userId}:${connectorId}`;
  const existing = inflightRefreshes.get(key);
  if (existing) return existing;
  const pending = doRefreshConnection(userId, connectorId).finally(() => {
    inflightRefreshes.delete(key);
  });
  inflightRefreshes.set(key, pending);
  return pending;
}

async function doRefreshConnection(
  userId: string,
  connectorId: string,
): Promise<ConnectionRefreshOutcome> {
  // Only the fixed connectors live here; user-added MCP servers
  // (mcp:<uuid>) are refreshed by their own proxy (mcp-proxy.ts).
  const connector = getConnector(connectorId);
  if (!connector) return { kind: "skipped" };

  const tokens = await getTokens(userId, connectorId);
  if (!tokens) return { kind: "skipped" };

  const expiresAtMs = tokens.expiresAt ? new Date(tokens.expiresAt).getTime() : 0;
  if (!expiresAtMs || expiresAtMs - Date.now() > REFRESH_WINDOW_MS) {
    return { kind: "fresh" };
  }
  if (!tokens.refreshToken) {
    // Expired with no refresh token (classic non-rotating app): nothing
    // to renew — the caller reports the honest reconnect-required state.
    return { kind: "skipped" };
  }
  const creds = connectorCredentials(connector);
  if (!creds) {
    logger.warn(
      { connector: connectorId },
      "connector-refresh: connector OAuth client not configured — cannot renew",
    );
    return { kind: "skipped" };
  }

  try {
    let next: { accessToken: string; refreshToken: string | null; expiresIn: number | null } | null = null;

    if (connector.authMethod === "oauth_supabase") {
      // Supabase: refresh grant over the same Basic-auth token endpoint
      // as the code exchange (verified live against current docs).
      const res = await fetch("https://api.supabase.com/v1/oauth/token", {
        method: "POST",
        headers: {
          "Content-Type": "application/x-www-form-urlencoded",
          Accept: "application/json",
          Authorization: `Basic ${Buffer.from(`${creds.clientId}:${creds.clientSecret}`).toString("base64")}`,
        },
        body: new URLSearchParams({
          grant_type: "refresh_token",
          refresh_token: tokens.refreshToken,
        }),
        signal: AbortSignal.timeout(30_000),
      });
      if (!res.ok) {
        const detail = await res.text().catch(() => "");
        // 4xx = the refresh token itself was rejected (revoked/expired/
        // already rotated) — a reconnect is the only honest path. 5xx =
        // provider trouble, transient: keep the stored pair as-is.
        const dead = res.status >= 400 && res.status < 500;
        logger.warn(
          { connector: connectorId, status: res.status, dead, detail: detail.slice(0, 160) },
          dead
            ? "connector-refresh: provider rejected the refresh token — marking error (reconnect required)"
            : "connector-refresh: provider token endpoint errored (transient)",
        );
        if (dead) {
          await markConnectionStatus(userId, connectorId, "error").catch(() => undefined);
          return { kind: "dead" };
        }
        return { kind: "skipped" };
      }
      const json = (await res.json()) as {
        access_token?: string;
        refresh_token?: string;
        expires_in?: number;
      };
      if (json.access_token) {
        next = {
          accessToken: json.access_token,
          refreshToken: json.refresh_token || null,
          expiresIn: json.expires_in ?? null,
        };
      }
    } else {
      // GitHub (classic OAuth App and GitHub App user-to-server): the
      // refresh grant is the same endpoint as the code exchange; GitHub
      // answers 200 with an {error} body when the grant is rejected.
      const res = await fetch("https://github.com/login/oauth/access_token", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          Accept: "application/json",
          "User-Agent": "arcforge",
        },
        body: JSON.stringify({
          client_id: creds.clientId,
          client_secret: creds.clientSecret,
          grant_type: "refresh_token",
          refresh_token: tokens.refreshToken,
        }),
        signal: AbortSignal.timeout(30_000),
      });
      const json = (await res.json().catch(() => ({}))) as {
        access_token?: string;
        refresh_token?: string;
        expires_in?: number;
        error?: string;
      };
      if (!json.access_token || json.error) {
        const dead = Boolean(json.error) || (res.status >= 400 && res.status < 500);
        logger.warn(
          { connector: connectorId, status: res.status, err: json.error ?? null, dead },
          dead
            ? "connector-refresh: github rejected the refresh token — marking error (reconnect required)"
            : "connector-refresh: github token endpoint errored (transient)",
        );
        if (dead) {
          await markConnectionStatus(userId, connectorId, "error").catch(() => undefined);
          return { kind: "dead" };
        }
        return { kind: "skipped" };
      }
      next = {
        accessToken: json.access_token,
        refreshToken: json.refresh_token || null,
        expiresIn: json.expires_in ?? null,
      };
    }

    if (!next) return { kind: "skipped" };

    await rotateTokens(userId, connectorId, {
      accessToken: next.accessToken,
      // GitHub rotates refresh tokens on every use (single-use); a
      // provider that omits one keeps the stored token.
      refreshToken: next.refreshToken || tokens.refreshToken,
      expiresAt: expiryDate(next.expiresIn),
    });
    logger.info(
      { userId, connector: connectorId },
      "connector-refresh: tokens renewed (values never logged)",
    );
    return { kind: "rotated" };
  } catch (err) {
    logger.warn(
      { connector: connectorId, err: err instanceof Error ? err.message : "unknown" },
      "connector-refresh: renewal threw (transient) — keeping the stored pair",
    );
    return { kind: "skipped" };
  }
}

/** ── VM NOTIFICATION (GROUP 2 session 2) ─────────────────────────────
 *  Resolve the project's sandbox → VM sidecar url+token (same brokering
 *  as workspace.ts postSecretToSidecar) and POST /internal/connectors so
 *  the blocked request_connector tool call wakes and the paused task
 *  resumes. Ownership is enforced: the project row's user_id MUST match
 *  `userId` (the HMAC-signed state identity on the public callback path;
 *  req.userId on the authenticated decline path). Failures are logged
 *  honestly and NEVER fail the caller — the connection itself already
 *  succeeded; only the task resume degrades. */
export async function notifySidecarConnector(
  userId: string,
  projectId: string | undefined,
  payload: { request_id?: string; granted?: boolean; declined?: boolean; capability?: string; detail?: string },
): Promise<void> {
  if (!projectId) {
    logger.warn(
      { userId, requestId: payload.request_id ?? null },
      "connector-resume: no projectId in the request context — the sidecar was not notified",
    );
    return;
  }
  try {
    const row = await getProjectRow(projectId);
    if (!row) {
      logger.warn(
        { userId, projectId, requestId: payload.request_id ?? null },
        "connector-resume: project row not found — the sidecar was not notified",
      );
      return;
    }
    if (row.user_id !== userId) {
      // User isolation: the state's user must own the project row.
      logger.warn(
        { userId, projectId, requestId: payload.request_id ?? null },
        "connector-resume: project belongs to a different user — refusing to notify the sidecar",
      );
      return;
    }
    if (!row.sandbox_id) {
      logger.warn(
        { userId, projectId, requestId: payload.request_id ?? null },
        "connector-resume: project has no sandbox — the sidecar was not notified",
      );
      return;
    }
    const info = await getAgentInfo(row.sandbox_id);
    if (!info?.url || !info.token) {
      logger.warn(
        { userId, projectId, sandboxId: row.sandbox_id, requestId: payload.request_id ?? null },
        "connector-resume: VM sidecar unreachable — the task resume degrades",
      );
      return;
    }
    const res = await fetch(`${info.url.replace(/\/+$/, "")}/internal/connectors`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "X-VM-Token": info.token,
      },
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(SIDECAR_NOTIFY_TIMEOUT_MS),
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "");
      logger.warn(
        { userId, projectId, sandboxId: row.sandbox_id, requestId: payload.request_id ?? null, status: res.status, detail: text.slice(0, 200) },
        "connector-resume: sidecar /internal/connectors responded non-2xx",
      );
      return;
    }
    logger.info(
      { userId, projectId, sandboxId: row.sandbox_id, requestId: payload.request_id ?? null, granted: payload.granted === true, declined: payload.declined === true },
      "connector-resume: sidecar notified (token value never logged)",
    );
  } catch (err) {
    logger.warn(
      { userId, projectId, requestId: payload.request_id ?? null, err: err instanceof Error ? err.message : "unknown" },
      "connector-resume: sidecar notification failed — the task resume degrades",
    );
  }
}

/** ── OAUTH CALLBACK CORE (provider → edge → here) ────────────────────
 * PUBLIC BY DESIGN: the browser lands here via a top-level redirect from
 * the OAuth provider with no Supabase JWT. Security = the HMAC-signed
 * state, which this backend minted for an authenticated user at
 * authorize time (carries user + resume context).
 *
 * Returns the final 302 URL (never tokens) for the route to relay. */
export async function completeConnectorOAuth(code: string, stateRaw: string): Promise<string> {
  const state = verifyState(stateRaw);
  if (!code || !state || state.purpose !== "connector" || !state.connector || !state.userId) {
    return `${FRONTEND_URL}/connectors?connected=unknown&status=error`;
  }
  // User-added MCP servers (connector = "mcp:<uuid>"): the PKCE-bound
  // exchange + vault storage + tool-catalog snapshot live in mcp-proxy —
  // the same completion contract, different token mechanics.
  if (state.connector.startsWith("mcp:")) {
    return completeMcpOAuth(code, state, landing);
  }
  const connector = getConnector(state.connector);
  if (!connector) {
    return `${landing(state)}?connected=${encodeURIComponent(state.connector)}&status=error`;
  }
  const creds = connectorCredentials(connector);
  if (!creds) {
    return `${landing(state)}?connected=${connector.id}&status=error&message=not_configured`;
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
          // The token exchange redirect_uri must EXACTLY match the one
          // used at authorize time (Supabase enforces this).
          redirect_uri: connectorRedirectUri(connector),
        }),
      });
      if (!tokenRes.ok) {
        const detail = await tokenRes.text();
        logger.warn(
          { connector: connector.id, status: tokenRes.status, detail: detail.slice(0, 200) },
          "connector-oauth: token exchange failed",
        );
        await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
        return `${landing(state)}?connected=${connector.id}&status=error&message=exchange_failed`;
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
      // GitHub token exchange — classic OAuth App AND GitHub App
      // user-to-server use the same endpoint and payload shape. The
      // redirect_uri must match the authorize request's exactly.
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
          redirect_uri: connectorRedirectUri(connector),
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
      // GitHub exchange errors (bad_verification_code,
      // incorrect_client_credentials, redirect_uri_mismatch) land here —
      // logged honestly, surfaced as exchange_failed.
      if (!accessToken) {
        logger.warn(
          { connector: connector.id, err: tokenJson.error, method: connector.authMethod },
          "connector-oauth: github exchange failed",
        );
        await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
        return `${landing(state)}?connected=${connector.id}&status=error&message=exchange_failed`;
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
      return `${landing(state)}?connected=${connector.id}&status=error&message=no_token`;
    }

    // Capability grant bookkeeping (GROUP 2 session 2): an authorize call
    // WITH a specific capability grants exactly that one; the Connectors
    // page (no capability) grants ALL connector capabilities — the user
    // explicitly connected the whole connector.
    const grantedCapabilities = state.capability
      ? [state.capability]
      : connector.capabilities.map((c) => c.id);

    await upsertConnection(userId, connector.id, {
      accessToken,
      refreshToken,
      expiresAt: expiryDate(expiresIn),
    }, { scopes, accountLabel, githubLogin, grantedCapabilities });

    logger.info(
      { userId, connector: connector.id, capability: state.capability ?? null, hasTask: Boolean(state.taskId), granted: grantedCapabilities },
      "connector-oauth: connected (token stored encrypted; value never logged)",
    );

    // Task resume: when the OAuth round-trip originated from an
    // agent-initiated request_connector call, wake the blocked sidecar
    // tool so the paused task continues with the grant. Ownership is
    // verified inside (state.userId is the HMAC-signed identity this
    // backend minted at authorize time; the project row must match it).
    if (state.taskId || state.requestId || state.projectId) {
      await notifySidecarConnector(userId, state.projectId, {
        request_id: state.requestId,
        granted: true,
        capability: state.capability,
      });
    }

    return `${landing(state)}?connected=${connector.id}&status=ok`;
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "connector-oauth: callback error");
    await markConnectionStatus(userId, connector.id, "error").catch(() => undefined);
    return `${landing(state)}?connected=${connector.id}&status=error&message=internal`;
  }
}
