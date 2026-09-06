/**
 * /api/auth/session/* — email/password session management (EDGE-RELAYED).
 *
 * Architecture: Frontend → Supabase Edge Function (auth-session) → THIS
 * backend → Supabase Auth (GoTrue REST). The frontend holds NO Supabase
 * URL, NO anon key, NO supabase-js client — it talks to the edge function
 * only, and the edge forwards here with the caller's IP for rate limiting.
 *
 * Routes (all anonymous-safe by design — they ARE the credential check):
 *   POST /api/auth/session/sign-in   {email, password} → {session, user}
 *   POST /api/auth/session/sign-up   {email, password} → {user, session?}
 *   POST /api/auth/session/refresh   {refresh_token}   → {session, user}
 *   POST /api/auth/session/sign-out  {access_token, refresh_token} → {ok}
 *   POST /api/auth/session/google-start {redirectTo}  → {authorize_url}
 *
 * Security:
 *  - The ANON key is used for token grants so GoTrue's own per-email /
 *    per-IP rate limits stay ACTIVE (the service role would bypass them).
 *  - An in-memory rate limiter adds a second layer per email+IP (the edge
 *    relays the browser's IP in X-Client-IP; when absent the edge hop's
 *    address is used).
 *  - Validation: emails are lowercased + shape-checked; passwords must be
 *    8–72 chars (GoTrue enforces its own policy on top).
 *  - No tokens, emails or passwords are ever logged.
 */

import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { isSupabaseConfigured, ensureUserRow } from "../lib/supabase-db";

const router: IRouter = Router();

const SUPABASE_URL = (process.env.SUPABASE_URL || "").replace(/\/+$/, "");
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

// ─── tiny in-memory rate limiter (email + IP buckets) ─────────────────────
// 10 attempts / 15 min per email and per source IP. Service-role GoTrue
// calls bypass platform limits, so this layer keeps credential stuffing
// expensive even when callers rotate emails. Memory-bounded: stale buckets
// are swept on every check.
const RATE_WINDOW_MS = 15 * 60_000;
const RATE_MAX = 10;
const buckets = new Map<string, { hits: number[]; }>();

function rateLimited(key: string): boolean {
  const now = Date.now();
  const bucket = buckets.get(key);
  const hits = (bucket?.hits ?? []).filter((t) => now - t < RATE_WINDOW_MS);
  if (hits.length >= RATE_MAX) {
    buckets.set(key, { hits });
    return true;
  }
  hits.push(now);
  buckets.set(key, { hits });
  // Sweep: drop empty/expired buckets occasionally (every ~100 checks).
  if (buckets.size > 512) {
    for (const [k, v] of buckets) {
      if (v.hits.every((t) => now - t >= RATE_WINDOW_MS)) buckets.delete(k);
    }
  }
  return false;
}

function clientIp(req: Request): string {
  const relayed = req.headers["x-client-ip"];
  if (typeof relayed === "string" && relayed.trim()) return relayed.trim().slice(0, 64);
  const xff = req.headers["x-forwarded-for"];
  if (typeof xff === "string" && xff.trim()) return xff.split(",")[0].trim().slice(0, 64);
  return "unknown";
}

// ─── validation helpers ────────────────────────────────────────────────────
const EMAIL_RE = /^[^\s@]+@[^\s@]+\.[^\s@]{2,}$/;

function normalizeEmail(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  const email = raw.trim().toLowerCase();
  if (!EMAIL_RE.test(email) || email.length > 254) return null;
  return email;
}

function normalizePassword(raw: unknown): string | null {
  if (typeof raw !== "string") return null;
  if (raw.length < 8 || raw.length > 72) return null;
  return raw;
}

/** Standard GoTrue REST call with the anon key (platform rate limits stay
 *  active). Returns the parsed JSON body; throws on network failure. */
async function gotrue(
  path: string,
  init: { method?: string; body?: unknown; bearer?: string },
): Promise<{ status: number; json: Record<string, unknown> }> {
  const res = await fetch(`${SUPABASE_URL}${path}`, {
    method: init.method ?? "POST",
    headers: {
      "Content-Type": "application/json",
      apikey: SUPABASE_ANON_KEY,
      ...(init.bearer ? { Authorization: `Bearer ${init.bearer}` } : {}),
    },
    body: init.body !== undefined ? JSON.stringify(init.body) : undefined,
  });
  let json: Record<string, unknown> = {};
  try {
    json = (await res.json()) as Record<string, unknown>;
  } catch {
    json = {};
  }
  return { status: res.status, json };
}

/** Map a GoTrue error payload to a user-safe message + status. */
function authError(status: number, json: Record<string, unknown>): { status: number; message: string } {
  const code = typeof json.error_code === "string" ? json.error_code : "";
  const msg = typeof json.msg === "string" ? json.msg : typeof json.error === "string" ? json.error : "";
  switch (code) {
    case "invalid_credentials":
    case "email_not_confirmed":
    case "over_request_rate_limit":
    case "over_email_send_rate_limit":
      return { status, message: msg || "Authentication failed" };
    case "user_banned":
      return { status: 403, message: "This account has been suspended." };
    case "validation_failed":
    case "weak_password":
      return { status: 400, message: msg || "Please check your email and password." };
    default:
      // Never leak internal details — the frontend shows a safe retry copy.
      if (status === 400) return { status: 400, message: msg || "Invalid email or password." };
      if (status === 429) return { status: 429, message: "Too many attempts — please wait a few minutes and try again." };
      return { status: 502, message: "The sign-in service is unavailable right now. Please try again in a moment." };
  }
}

function sanitizeUser(user: unknown): { id: string; email: string | null } | null {
  if (!user || typeof user !== "object") return null;
  const u = user as { id?: unknown; email?: unknown };
  if (typeof u.id !== "string" || !u.id) return null;
  return { id: u.id, email: typeof u.email === "string" ? u.email : null };
}

function sanitizeSession(session: unknown): {
  access_token: string;
  refresh_token: string;
  expires_at: number | null;
} | null {
  if (!session || typeof session !== "object") return null;
  const s = session as { access_token?: unknown; refresh_token?: unknown; expires_at?: unknown };
  if (typeof s.access_token !== "string" || !s.access_token) return null;
  if (typeof s.refresh_token !== "string" || !s.refresh_token) return null;
  return {
    access_token: s.access_token,
    refresh_token: s.refresh_token,
    expires_at: typeof s.expires_at === "number" ? s.expires_at : null,
  };
}

// ─── POST /api/auth/session/sign-in ────────────────────────────────────────
router.post("/auth/session/sign-in", async (req: Request, res: Response, _next: NextFunction) => {
  if (!isSupabaseConfigured() || !SUPABASE_ANON_KEY) {
    res.status(503).json({ error: "Authentication is not configured" });
    return;
  }
  const body = (req.body || {}) as { email?: unknown; password?: unknown };
  const email = normalizeEmail(body.email);
  const password = normalizePassword(body.password);
  if (!email || !password) {
    res.status(400).json({ error: "Enter a valid email and a password of at least 8 characters." });
    return;
  }
  const ip = clientIp(req);
  if (rateLimited(`email:${email}`) || rateLimited(`ip:${ip}`)) {
    res.status(429).json({ error: "Too many attempts — please wait a few minutes and try again." });
    return;
  }
  try {
    const { status, json } = await gotrue("/auth/v1/token?grant_type=password", {
      body: { email, password },
    });
    if (status !== 200) {
      const mapped = authError(status, json);
      logger.warn({ status, code: json.error_code ?? "" }, "auth-session sign-in rejected");
      res.status(mapped.status).json({ error: mapped.message });
      return;
    }
    const session = sanitizeSession(json);
    const user = sanitizeUser(json.user);
    if (!session || !user) {
      res.status(502).json({ error: "The sign-in service returned an invalid session." });
      return;
    }
    // Ensure the public.users row exists (FK target for projects).
    void ensureUserRow(user.id, user.email ?? email).catch(() => {});
    res.json({ session, user });
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "auth-session sign-in failed");
    res.status(502).json({ error: "The sign-in service is unreachable. Please try again in a moment." });
  }
});

// ─── POST /api/auth/session/sign-up ────────────────────────────────────────
router.post("/auth/session/sign-up", async (req: Request, res: Response, _next: NextFunction) => {
  if (!isSupabaseConfigured() || !SUPABASE_ANON_KEY) {
    res.status(503).json({ error: "Authentication is not configured" });
    return;
  }
  const body = (req.body || {}) as { email?: unknown; password?: unknown };
  const email = normalizeEmail(body.email);
  const password = normalizePassword(body.password);
  if (!email || !password) {
    res.status(400).json({ error: "Enter a valid email and a password of at least 8 characters." });
    return;
  }
  const ip = clientIp(req);
  if (rateLimited(`email:${email}`) || rateLimited(`ip:${ip}`)) {
    res.status(429).json({ error: "Too many attempts — please wait a few minutes and try again." });
    return;
  }
  try {
    const { status, json } = await gotrue("/auth/v1/signup", {
      body: { email, password },
    });
    if (status !== 200 && status !== 201) {
      const mapped = authError(status, json);
      logger.warn({ status, code: json.error_code ?? "" }, "auth-session sign-up rejected");
      res.status(mapped.status).json({ error: mapped.message });
      return;
    }
    const user = sanitizeUser(json.user);
    if (!user) {
      res.status(502).json({ error: "The sign-up service returned an incomplete response." });
      return;
    }
    // When email confirmation is required there is no session yet — the
    // frontend shows "check your email" and the user signs in after
    // confirming. Otherwise the session is included.
    const session = sanitizeSession(json.session);
    if (session) void ensureUserRow(user.id, user.email ?? email).catch(() => {});
    res.status(201).json({ user, session });
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "auth-session sign-up failed");
    res.status(502).json({ error: "The sign-up service is unreachable. Please try again in a moment." });
  }
});

// ─── POST /api/auth/session/refresh ────────────────────────────────────────
router.post("/auth/session/refresh", async (req: Request, res: Response, _next: NextFunction) => {
  if (!isSupabaseConfigured() || !SUPABASE_ANON_KEY) {
    res.status(503).json({ error: "Authentication is not configured" });
    return;
  }
  const body = (req.body || {}) as { refresh_token?: unknown };
  const refreshToken = typeof body.refresh_token === "string" ? body.refresh_token.trim() : "";
  if (!refreshToken || refreshToken.length > 256) {
    res.status(400).json({ error: "A refresh token is required." });
    return;
  }
  const ip = clientIp(req);
  if (rateLimited(`ip:${ip}`)) {
    res.status(429).json({ error: "Too many attempts — please wait a few minutes and try again." });
    return;
  }
  try {
    const { status, json } = await gotrue("/auth/v1/token?grant_type=refresh_token", {
      body: { refresh_token: refreshToken },
    });
    if (status !== 200) {
      const mapped = authError(status, json);
      res.status(mapped.status === 502 ? 401 : mapped.status).json({ error: mapped.message });
      return;
    }
    const session = sanitizeSession(json);
    const user = sanitizeUser(json.user);
    if (!session || !user) {
      res.status(401).json({ error: "The session could not be refreshed — please sign in again." });
      return;
    }
    res.json({ session, user });
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "auth-session refresh failed");
    res.status(502).json({ error: "The session service is unreachable. Please try again in a moment." });
  }
});

// ─── POST /api/auth/session/sign-out ───────────────────────────────────────
router.post("/auth/session/sign-out", async (req: Request, res: Response, _next: NextFunction) => {
  if (!isSupabaseConfigured() || !SUPABASE_ANON_KEY) {
    res.status(503).json({ error: "Authentication is not configured" });
    return;
  }
  const body = (req.body || {}) as { access_token?: unknown; refresh_token?: unknown };
  const accessToken = typeof body.access_token === "string" ? body.access_token.trim() : "";
  const refreshToken = typeof body.refresh_token === "string" ? body.refresh_token.trim() : "";
  if (!accessToken && !refreshToken) {
    // Nothing to revoke — still a successful sign-out for the client.
    res.json({ ok: true });
    return;
  }
  try {
    // GoTrue logout revokes the bearer's session(s); the refresh token in
    // the body additionally revokes that specific rotation family.
    await gotrue("/auth/v1/logout", {
      bearer: accessToken || undefined,
      body: refreshToken ? { refresh_token: refreshToken } : undefined,
    });
    res.json({ ok: true });
  } catch (err) {
    logger.warn({ err: err instanceof Error ? err.message : "unknown" }, "auth-session sign-out relay failed");
    // Client-side cookie clearing proceeds regardless.
    res.json({ ok: true });
  }
});

// ─── POST /api/auth/session/google-start ───────────────────────────────────
// Returns the GoTrue authorize URL for the Google provider. The browser
// navigates to it directly (top-level redirect); Supabase redirects back
// with the session in the URL fragment (implicit flow) which the frontend
// parses on boot. No Supabase keys appear in the URL — /auth/v1/authorize
// is the OAuth entry point and needs none.
router.post("/auth/session/google-start", async (req: Request, res: Response, _next: NextFunction) => {
  if (!SUPABASE_URL) {
    res.status(503).json({ error: "Authentication is not configured" });
    return;
  }
  const body = (req.body || {}) as { redirectTo?: unknown };
  let redirectTo = typeof body.redirectTo === "string" ? body.redirectTo.trim() : "";
  // Only allow OUR origins (the site's canonical domains + dev) — the
  // GoTrue site URL allowlist also enforces this server-side.
  try {
    const u = new URL(redirectTo || "https://forgeyn.com.ng/");
    const host = u.hostname;
    const ok =
      (u.protocol === "https:" &&
        (host.endsWith(".onrender.com") || host === "forgeyn.com.ng" || host === "www.forgeyn.com.ng" || host.endsWith(".arcforge.app"))) ||
      (u.protocol === "http:" && (host === "localhost" || host === "127.0.0.1"));
    if (!ok) redirectTo = "https://forgeyn.com.ng/";
  } catch {
    redirectTo = "https://forgeyn.com.ng/";
  }
  const authorizeUrl =
    `${SUPABASE_URL}/auth/v1/authorize?provider=google&redirect_to=${encodeURIComponent(redirectTo)}`;
  res.json({ authorize_url: authorizeUrl });
});

export default router;
