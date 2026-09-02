/**
 * Forgvi Engine — workspace grants.
 *
 * A workspace grant binds a Forgvi 2.0 run to ONE Daytona sandbox:
 *
 *   fg1.<base64url(payload)>.<base64url(hmac-sha256(payload))>
 *
 * The Forge backend mints it ONLY after verifying that the requesting user
 * owns the project (JWT + project-row ownership check), so the engine can
 * trust the embedded {projectId, sandboxId, userId} without ever seeing a
 * user credential. Short-lived (minutes), single-purpose, unforgeable
 * without WORKSPACE_GRANT_SECRET — which lives only on the backend and the
 * engine, never in a browser.
 *
 * This is the isolation boundary: one grant = one project's sandbox. A run
 * can never touch another user's workspace because it can never be handed a
 * grant for it.
 */

import { createHmac, randomUUID, timingSafeEqual } from "node:crypto";

const GRANT_PREFIX = "fg1";
const DEFAULT_TTL_MS = 20 * 60_000; // 20 minutes — covers engine wake + run start

/** b64url without padding. */
const b64url = (buf) => Buffer.from(buf).toString("base64url");

/** Constant-time HMAC comparison. */
function safeEqual(a, b) {
  const ba = Buffer.from(a);
  const bb = Buffer.from(b);
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

/**
 * Mint a grant. Mirrors the backend's minting logic — used by tests and
 * local development; production grants are minted by the Forge backend.
 */
export function mintWorkspaceGrant(
  { projectId, sandboxId, userId },
  { secret, ttlMs = DEFAULT_TTL_MS, now = Date.now() } = {},
) {
  if (!secret) throw new Error("WORKSPACE_GRANT_SECRET is required to mint a grant");
  const payload = {
    v: 1,
    projectId: String(projectId),
    sandboxId: String(sandboxId),
    userId: String(userId ?? "unknown"),
    iat: now,
    exp: now + ttlMs,
    jti: randomUUID(),
  };
  const body = b64url(JSON.stringify(payload));
  const mac = b64url(createHmac("sha256", secret).update(body).digest());
  return `${GRANT_PREFIX}.${body}.${mac}`;
}

/**
 * Verify a grant and return its claims, or null (bad format / bad signature
 * / expired). Never throws — callers treat null as "not bound".
 */
export function verifyWorkspaceGrant(token, { secret, now = Date.now(), clockSkewMs = 60_000 } = {}) {
  try {
    if (!secret || typeof token !== "string") return null;
    const parts = token.split(".");
    if (parts.length !== 3 || parts[0] !== GRANT_PREFIX) return null;
    const [body, mac] = [parts[1], parts[2]];
    const expected = createHmac("sha256", secret).update(body).digest();
    const given = Buffer.from(mac, "base64url");
    if (given.length !== expected.length || !timingSafeEqual(given, expected)) return null;
    const payload = JSON.parse(Buffer.from(body, "base64url").toString("utf-8"));
    if (payload?.v !== 1) return null;
    if (typeof payload.sandboxId !== "string" || !payload.sandboxId) return null;
    if (typeof payload.projectId !== "string" || !payload.projectId) return null;
    if (typeof payload.exp !== "number" || payload.exp + clockSkewMs < now) return null;
    return {
      projectId: payload.projectId,
      sandboxId: payload.sandboxId,
      userId: typeof payload.userId === "string" ? payload.userId : "unknown",
      expiresAt: payload.exp,
    };
  } catch {
    return null;
  }
}
