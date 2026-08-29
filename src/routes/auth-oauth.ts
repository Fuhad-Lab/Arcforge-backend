/**
 * GitHub Sign-In (GROUP 2) — Frontend → Supabase Edge Function (auth-oauth)
 * → this backend → GitHub OAuth → backend-minted Supabase session →
 * sanitized state → frontend.
 *
 * GitHub Sign-In credentials are SEPARATE from the GitHub App connector
 * credentials (repository importing): env GITHUB_SIGNIN_CLIENT_ID /
 * GITHUB_SIGNIN_CLIENT_SECRET (Render environment variables — the secret is
 * never in source, Git, the frontend, logs, or URLs).
 *
 * Flow:
 *  1. POST /api/auth/github/start {origin} → {authorize_url}
 *     (state = HMAC-signed blob {purpose:"signin", origin, nonce, exp})
 *  2. Browser authorizes on GitHub → GitHub redirects to the edge function
 *     (the registered callback URL) → GET /api/auth/github/callback?code&state
 *     → backend exchanges the code (client secret server-side only), reads
 *     the GitHub identity, provisions/links the Supabase user via the admin
 *     API, mints a real Supabase session SERVER-SIDE (magiclink
 *     generateLink + verify exchange), stashes it behind a single-use
 *     one-time code, and 302s the browser to
 *     <origin>/auth?auth=github&otc=<code>&status=ok
 *  3. POST /api/auth/github/exchange {otc} → {session} — the frontend
 *     calls supabase.auth.setSession() with it. The GitHub OAuth token is
 *     DISCARDED after identity resolution: signing in does NOT grant
 *     GitHub connector capabilities (separately scoped consent).
 *
 * These three routes are intentionally reachable without a Supabase JWT:
 * the edge function is the public surface; security here = the signed state
 * (CSRF) + single-use 120s one-time codes. Everything else behind requireAuth.
 */

import { Router, type IRouter, type Request, type Response } from "express";
import { logger } from "../lib/logger";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";
import { mintState, verifyState, stashSession, takeSession } from "../services/connector-vault";

const router: IRouter = Router();

const SUPABASE_URL = process.env.SUPABASE_URL || "";
const SUPABASE_ANON_KEY = process.env.SUPABASE_ANON_KEY || "";

// The edge function is the registered GitHub callback URL — the backend is
// never exposed to the browser. Derived from the deployed function config,
// with a fallback derived from the Supabase project URL.
const EDGE_BASE = process.env.EDGE_FUNCTION_BASE_URL || "";
const edgeCallbackUrl = (): string => {
  if (EDGE_BASE) return `${EDGE_BASE.replace(/\/+$/, "")}/auth-oauth`;
  if (SUPABASE_URL) return `${SUPABASE_URL.replace(/\/+$/, "")}/functions/v1/auth-oauth`;
  return "https://arcforge-edge.invalid/functions/v1/auth-oauth";
};

const FRONTEND_FALLBACK_ORIGIN = process.env.FRONTEND_URL || "https://arcforge-web.onrender.com";

function allowedOrigin(raw: string | undefined | null): string {
  if (!raw) return FRONTEND_FALLBACK_ORIGIN;
  try {
    const url = new URL(raw);
    const host = url.hostname;
    if (
      (url.protocol === "https:" &&
        (host.endsWith(".onrender.com") || host.endsWith(".arcforge.app") || host.endsWith(".vercel.app"))) ||
      (url.protocol === "http:" && (host === "localhost" || host === "127.0.0.1"))
    ) {
      return url.origin;
    }
  } catch {
    /* malformed — fall through */
  }
  return FRONTEND_FALLBACK_ORIGIN;
}

/** ── 1. START ─────────────────────────────────────────────────────── */
router.post("/auth/github/start", async (req: Request, res: Response) => {
  const clientId = process.env.GITHUB_SIGNIN_CLIENT_ID || "";
  const clientSecret = process.env.GITHUB_SIGNIN_CLIENT_SECRET || "";
  if (!clientId || !clientSecret) {
    res.status(503).json({ error: "GitHub sign-in is not configured" });
    return;
  }
  const body = (req.body || {}) as { origin?: string };
  const origin = allowedOrigin(body.origin);
  const state = mintState({ purpose: "signin", origin });
  const redirectUri = edgeCallbackUrl();
  const authorizeUrl =
    `https://github.com/login/oauth/authorize?client_id=${encodeURIComponent(clientId)}` +
    `&redirect_uri=${encodeURIComponent(redirectUri)}` +
    `&scope=${encodeURIComponent("read:user user:email")}` +
    `&state=${encodeURIComponent(state)}`;
  res.json({ authorize_url: authorizeUrl });
});

/** ── 2. CALLBACK (GitHub → edge → here) ───────────────────────────── */
router.get("/auth/github/callback", async (req: Request, res: Response) => {
  const clientId = process.env.GITHUB_SIGNIN_CLIENT_ID || "";
  const clientSecret = process.env.GITHUB_SIGNIN_CLIENT_SECRET || "";
  if (!clientId || !clientSecret) {
    res.redirect(302, `${FRONTEND_FALLBACK_ORIGIN}/auth?auth=github&status=error&message=not_configured`);
    return;
  }
  const code = typeof req.query.code === "string" ? req.query.code : "";
  const stateRaw = typeof req.query.state === "string" ? req.query.state : "";
  const state = verifyState(stateRaw);
  if (!code || !state || state.purpose !== "signin") {
    res.redirect(302, `${FRONTEND_FALLBACK_ORIGIN}/auth?auth=github&status=error&message=invalid_state`);
    return;
  }
  const origin = allowedOrigin(state.origin);

  try {
    // Exchange the code — the client secret exists ONLY here (backend env).
    const tokenRes = await fetch("https://github.com/login/oauth/access_token", {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Accept: "application/json",
        "User-Agent": "arcforge",
      },
      body: JSON.stringify({
        client_id: clientId,
        client_secret: clientSecret,
        code,
        redirect_uri: edgeCallbackUrl(),
      }),
    });
    const tokenJson = (await tokenRes.json()) as {
      access_token?: string;
      error?: string;
      error_description?: string;
    };
    const githubToken = tokenJson.access_token;
    if (!githubToken) {
      logger.warn(
        { err: tokenJson.error },
        "github-signin: code exchange failed",
      );
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=exchange_failed`);
      return;
    }

    // Resolve the GitHub identity (token discarded right after).
    const headers = {
      Authorization: `Bearer ${githubToken}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "arcforge",
    };
    const userRes = await fetch("https://api.github.com/user", { headers });
    if (!userRes.ok) {
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=github_profile`);
      return;
    }
    const ghUser = (await userRes.json()) as {
      id?: number;
      login?: string;
      name?: string | null;
      email?: string | null;
      avatar_url?: string | null;
    };
    let email = ghUser.email ?? null;
    if (!email) {
      const emailsRes = await fetch("https://api.github.com/user/emails", { headers });
      if (emailsRes.ok) {
        const emails = (await emailsRes.json()) as Array<{
          email: string;
          primary: boolean;
          verified: boolean;
        }>;
        email =
          emails.find((e) => e.primary && e.verified)?.email ??
          emails.find((e) => e.verified)?.email ??
          null;
      }
    }
    if (!email) {
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=no_email`);
      return;
    }
    email = email.toLowerCase();

    if (!isSupabaseConfigured()) {
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=auth_unavailable`);
      return;
    }

    // Provision / link the platform user and mint a REAL Supabase session
    // server-side: admin.generateLink(magiclink) does not send any email;
    // the returned hashed_token is exchanged at /auth/v1/verify for a
    // session. The GitHub token never becomes a Supabase credential.
    const admin = getServiceSupabase();
    // generateLink() handles user creation for magiclink (per the installed
    // SDK's contract) — no create_user flag needed.
    const link = await admin.auth.admin.generateLink({
      type: "magiclink",
      email,
      options: {
        data: {
          full_name: ghUser.name || ghUser.login || null,
          avatar_url: ghUser.avatar_url ?? null,
          provider: "github",
          github_id: ghUser.id != null ? String(ghUser.id) : null,
          github_login: ghUser.login ?? null,
        },
      },
    });
    if (link.error || !link.data?.properties) {
      logger.warn(
        { err: link.error?.message },
        "github-signin: generateLink failed",
      );
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=session_failed`);
      return;
    }

    const props = link.data.properties as unknown as {
      hashed_token?: string;
      email_otp?: string;
      action_link?: string;
      // "signup" for brand-new (unconfirmed) users, "magiclink" for
      // existing ones — the verify endpoint requires the MATCHING type.
      verification_type?: string;
    };
    const tokenHash = props.hashed_token || "";
    if (!tokenHash) {
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=session_failed`);
      return;
    }
    const verifyType =
      props.verification_type === "signup" || props.verification_type === "magiclink"
        ? props.verification_type
        : "magiclink";

    // NOTE: /auth/v1/verify with a token_hash must NOT carry the email field
    // (400 "Only the token_hash and type should be provided" otherwise) and
    // `type` must match the token's own verification_type (a magiclink
    // generateLink for a NEW user mints a *signup* confirmation token).
    const verifyRes = await fetch(`${SUPABASE_URL}/auth/v1/verify`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        apikey: SUPABASE_ANON_KEY,
        Authorization: `Bearer ${SUPABASE_ANON_KEY}`,
      },
      body: JSON.stringify({
        type: verifyType,
        token_hash: tokenHash,
      }),
    });
    if (!verifyRes.ok) {
      const detail = await verifyRes.text();
      logger.warn({ status: verifyRes.status, detail: detail.slice(0, 200) },
        "github-signin: verify exchange failed");
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=session_failed`);
      return;
    }
    const session = (await verifyRes.json()) as {
      access_token: string;
      refresh_token: string;
      expires_in?: number;
      expires_at?: number;
      user?: { id?: string; email?: string };
    };
    if (!session.access_token || !session.refresh_token) {
      res.redirect(302, `${origin}/auth?auth=github&status=error&message=session_failed`);
      return;
    }

    // Ensure the public.users row exists (FK target for projects).
    try {
      const { error: upsertErr } = await getServiceSupabase()
        .from("users")
        .upsert(
          { id: session.user?.id, email },
          { onConflict: "id", ignoreDuplicates: false },
        );
      if (upsertErr) logger.warn({ err: upsertErr.message }, "github-signin: users upsert");
    } catch {
      /* trigger may handle it */
    }

    const otc = stashSession({
      accessToken: session.access_token,
      refreshToken: session.refresh_token,
      expiresAt: session.expires_at ?? Date.now() + 3600_000,
      userId: session.user?.id ?? "",
      email: session.user?.email ?? email,
    });
    logger.info(
      { userId: session.user?.id, githubLogin: ghUser.login },
      "github-signin: session minted (token values never logged)",
    );
    res.redirect(302, `${origin}/auth?auth=github&status=ok&otc=${encodeURIComponent(otc)}`);
  } catch (err) {
    logger.error({ err: err instanceof Error ? err.message : "unknown" }, "github-signin: callback error");
    res.redirect(302, `${origin}/auth?auth=github&status=error&message=internal`);
  }
});

/** ── 3. EXCHANGE (frontend redeems the one-time code) ─────────────── */
router.post("/auth/github/exchange", async (req: Request, res: Response) => {
  const body = (req.body || {}) as { otc?: string };
  const otc = typeof body.otc === "string" ? body.otc : "";
  const session = takeSession(otc);
  if (!session) {
    res.status(401).json({ error: "Invalid or expired sign-in code — please try again." });
    return;
  }
  // Sanitized session payload — the only place tokens legitimately reach
  // the browser is as the user's OWN session (set via supabase.auth.setSession).
  res.json({
    session: {
      access_token: session.accessToken,
      refresh_token: session.refreshToken,
      expires_at: session.expiresAt,
    },
    user: { id: session.userId, email: session.email },
  });
});

export default router;
