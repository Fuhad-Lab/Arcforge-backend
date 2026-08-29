/**
 * Connector vault — GROUP 2.
 *
 * Stores connector OAuth tokens for authenticated platform users:
 *   - AES-256-GCM encrypted at rest (CONNECTOR_ENCRYPTION_KEY, Render env)
 *   - in public.connector_connections (RLS enabled, ZERO policies →
 *     service-role only; the browser can never read this table)
 *   - one row per (user_id, connector_id)
 *
 * Also mints/verifies signed OAuth state blobs (HMAC-SHA256) that carry the
 * user identity + task-resumption context through the provider redirect.
 * The state is what lets the unauthenticated OAuth callback route safely:
 * it can ONLY have been minted by this backend for an authenticated user.
 *
 * Tokens NEVER appear in: frontend code, localStorage/cookies, URLs
 * (only one-time codes and states travel in URLs), logs, agent messages,
 * Git, or subagent arguments.
 */

import crypto from "node:crypto";
import { logger } from "../lib/logger";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";

// ─── ENCRYPTION ────────────────────────────────────────────────────────

function encryptionKey(): Buffer {
  const raw = process.env.CONNECTOR_ENCRYPTION_KEY || "";
  if (/^[0-9a-fA-F]{64}$/.test(raw)) return Buffer.from(raw, "hex");
  if (raw) return Buffer.from(raw, "utf8").subarray(0, 32).length >= 32
    ? Buffer.from(raw, "utf8").subarray(0, 32)
    : crypto.createHash("sha256").update(raw).digest();
  // Deterministic dev fallback so local/offline runs never crash. Production
  // always sets CONNECTOR_ENCRYPTION_KEY on Render (see worklog).
  return crypto
    .createHash("sha256")
    .update(process.env.AGENT_PROXY_SECRET || "arcforge-dev")
    .digest();
}

export function encryptToken(plaintext: string): string {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv("aes-256-gcm", encryptionKey(), iv);
  const enc = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString("base64url")}.${enc.toString("base64url")}.${tag.toString("base64url")}`;
}

export function decryptToken(payload: string): string | null {
  try {
    const [ivB64, dataB64, tagB64] = payload.split(".");
    if (!ivB64 || !dataB64 || !tagB64) return null;
    const decipher = crypto.createDecipheriv(
      "aes-256-gcm",
      encryptionKey(),
      Buffer.from(ivB64, "base64url"),
    );
    decipher.setAuthTag(Buffer.from(tagB64, "base64url"));
    const dec = Buffer.concat([
      decipher.update(Buffer.from(dataB64, "base64url")),
      decipher.final(),
    ]);
    return dec.toString("utf8");
  } catch {
    return null;
  }
}

// ─── SIGNED OAUTH STATE ────────────────────────────────────────────────

export interface OAuthState {
  purpose: "signin" | "connector";
  connector?: string;
  userId?: string;
  userEmail?: string | null;
  capability?: string;
  requestId?: string;
  taskId?: string;
  projectId?: string;
  sandboxId?: string;
  origin?: string;
  /** Post-connect relative landing path on the frontend (GROUP 3): lets
   *  flows like the GitHub import modal resume where they started instead
   * of always landing on /connectors. Server-validated to be a safe
   * same-origin path — never a full URL (no open-redirect). */
  returnPath?: string;
  nonce: string;
  exp: number;
}

function stateSecret(): string {
  return (
    process.env.CONNECTOR_STATE_SECRET ||
    process.env.AGENT_PROXY_SECRET ||
    "arcforge-dev-state"
  );
}

export function mintState(state: Omit<OAuthState, "nonce" | "exp">, ttlMs = 10 * 60 * 1000): string {
  const full: OAuthState = {
    ...state,
    nonce: crypto.randomBytes(12).toString("base64url"),
    exp: Date.now() + ttlMs,
  };
  const body = Buffer.from(JSON.stringify(full), "utf8").toString("base64url");
  const sig = crypto.createHmac("sha256", stateSecret()).update(body).digest("base64url");
  return `${body}.${sig}`;
}

export function verifyState(payload: string | null | undefined): OAuthState | null {
  if (!payload) return null;
  const [body, sig] = payload.split(".");
  if (!body || !sig) return null;
  const expected = crypto.createHmac("sha256", stateSecret()).update(body).digest("base64url");
  const a = Buffer.from(sig);
  const b = Buffer.from(expected);
  if (a.length !== b.length || !crypto.timingSafeEqual(a, b)) return null;
  try {
    const state = JSON.parse(Buffer.from(body, "base64url").toString("utf8")) as OAuthState;
    if (!state.exp || state.exp < Date.now()) return null;
    return state;
  } catch {
    return null;
  }
}

// ─── CONNECTION ROWS ───────────────────────────────────────────────────

export interface ConnectionRow {
  id: string;
  user_id: string;
  connector_id: string;
  status: string;
  scopes: string | null;
  account_label: string | null;
  project_ref: string | null;
  token_expires_at: string | null;
  github_login: string | null;
  connected_at: string | null;
  /** JSON array string of granted capability ids (GROUP 2 session 2).
   *  NULL/absent = full grant (backward compat — the user connected the
   *  whole connector before per-capability grants existed). */
  granted_capabilities: string | null;
}

export interface StoredTokens {
  accessToken: string;
  refreshToken: string | null;
  expiresAt: string | null;
}

function expiryDate(secondsFromNow: number | null | undefined): string | null {
  if (!secondsFromNow || secondsFromNow <= 0) return null;
  return new Date(Date.now() + secondsFromNow * 1000).toISOString();
}

/** Parse the stored granted-capabilities JSON array. NULL/absent/
 *  malformed = full grant (null) — the pre-session-2 contract. */
export function parseGrantedCapabilities(row: { granted_capabilities?: string | null }): string[] | null {
  const raw = row.granted_capabilities;
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as unknown;
    if (Array.isArray(parsed) && parsed.every((c) => typeof c === "string")) {
      return parsed.length > 0 ? (parsed as string[]) : null;
    }
  } catch {
    /* fallthrough — malformed = full grant */
  }
  return null;
}

export async function upsertConnection(
  userId: string,
  connectorId: string,
  tokens: StoredTokens,
  meta: {
    scopes?: string;
    accountLabel?: string;
    projectRef?: string;
    githubLogin?: string;
    /** Capability ids the user granted (GROUP 2 session 2). Omit/null =
     *  full grant (the whole connector). */
    grantedCapabilities?: string[] | null;
  },
): Promise<void> {
  if (!isSupabaseConfigured()) throw new Error("Supabase is not configured");
  const { error } = await getServiceSupabase().from("connector_connections").upsert(
    {
      user_id: userId,
      connector_id: connectorId,
      status: "connected",
      scopes: meta.scopes ?? null,
      account_label: meta.accountLabel ?? null,
      project_ref: meta.projectRef ?? null,
      access_token_enc: encryptToken(tokens.accessToken),
      refresh_token_enc: tokens.refreshToken
        ? encryptToken(tokens.refreshToken)
        : null,
      token_expires_at: tokens.expiresAt,
      github_login: meta.githubLogin ?? null,
      granted_capabilities:
        meta.grantedCapabilities && meta.grantedCapabilities.length > 0
          ? JSON.stringify(meta.grantedCapabilities)
          : null,
      connected_at: new Date().toISOString(),
      updated_at: new Date().toISOString(),
    },
    { onConflict: "user_id,connector_id" },
  );
  if (error) {
    logger.error({ err: error.message, connectorId }, "connector-vault: upsert failed");
    throw new Error("Failed to store the connector connection");
  }
}

export async function markConnectionStatus(
  userId: string,
  connectorId: string,
  status: "connecting" | "error",
): Promise<void> {
  if (!isSupabaseConfigured()) return;
  await getServiceSupabase()
    .from("connector_connections")
    .upsert(
      {
        user_id: userId,
        connector_id: connectorId,
        status,
        updated_at: new Date().toISOString(),
      },
      { onConflict: "user_id,connector_id" },
    );
}

export async function getConnection(
  userId: string,
  connectorId: string,
): Promise<ConnectionRow | null> {
  if (!isSupabaseConfigured()) return null;
  const { data, error } = await getServiceSupabase()
    .from("connector_connections")
    .select(
      "id,user_id,connector_id,status,scopes,account_label,project_ref,token_expires_at,github_login,connected_at,granted_capabilities",
    )
    .eq("user_id", userId)
    .eq("connector_id", connectorId)
    .maybeSingle();
  if (error) {
    logger.error({ err: error.message, connectorId }, "connector-vault: read failed");
    return null;
  }
  return (data as ConnectionRow) ?? null;
}

/** Load + decrypt the stored access/refresh tokens. Returns null when the
 *  user has no connection. Never logs token material. */
export async function getTokens(
  userId: string,
  connectorId: string,
): Promise<StoredTokens | null> {
  if (!isSupabaseConfigured()) return null;
  const { data, error } = await getServiceSupabase()
    .from("connector_connections")
    .select("access_token_enc,refresh_token_enc,token_expires_at")
    .eq("user_id", userId)
    .eq("connector_id", connectorId)
    .maybeSingle();
  if (error || !data) return null;
  const access = decryptToken(data.access_token_enc || "");
  if (!access) return null;
  const refresh = data.refresh_token_enc ? decryptToken(data.refresh_token_enc) : null;
  return { accessToken: access, refreshToken: refresh, expiresAt: data.token_expires_at };
}

export async function rotateTokens(
  userId: string,
  connectorId: string,
  tokens: StoredTokens,
): Promise<void> {
  if (!isSupabaseConfigured()) return;
  await getServiceSupabase()
    .from("connector_connections")
    .update({
      access_token_enc: encryptToken(tokens.accessToken),
      refresh_token_enc: tokens.refreshToken ? encryptToken(tokens.refreshToken) : null,
      token_expires_at: tokens.expiresAt,
      updated_at: new Date().toISOString(),
    })
    .eq("user_id", userId)
    .eq("connector_id", connectorId);
}

export async function deleteConnection(userId: string, connectorId: string): Promise<void> {
  if (!isSupabaseConfigured()) return;
  await getServiceSupabase()
    .from("connector_connections")
    .delete()
    .eq("user_id", userId)
    .eq("connector_id", connectorId);
}

// ─── GITHUB SIGN-IN ONE-TIME CODES ─────────────────────────────────────

export interface PendingSession {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
  userId: string;
  email: string | null;
}

const pendingSessions = new Map<string, PendingSession>();

/** Store a freshly minted Supabase session behind a single-use one-time
 *  code (120s TTL). The code — not the session — travels in the redirect
 *  URL; the frontend exchanges it exactly once. */
export function stashSession(session: PendingSession): string {
  const otc = crypto.randomBytes(32).toString("base64url");
  pendingSessions.set(otc, session);
  setTimeout(() => pendingSessions.delete(otc), 120_000).unref?.();
  return otc;
}

export function takeSession(otc: string): PendingSession | null {
  const session = pendingSessions.get(otc) ?? null;
  if (session) pendingSessions.delete(otc); // single use
  return session;
}

// ─── HELPERS ───────────────────────────────────────────────────────────

export { expiryDate };
