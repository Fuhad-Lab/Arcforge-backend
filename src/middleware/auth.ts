import type { Request, Response, NextFunction } from "express";
import { logger } from "../lib/logger";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";

// ─── EXPRESS REQUEST AUGMENTATION ─────────────────────────────────────────
// Attach authenticated user context to every Request instance (typed, no `any`).
declare global {
  namespace Express {
    interface Request {
      /** Supabase auth user UUID (set by requireAuth). */
      userId?: string;
      /** Email of the authenticated user, if available. */
      userEmail?: string | null;
      /** Optional project UUID for scoped operations. */
      projectId?: string;
    }
  }
}

/**
 * JWT-based authentication middleware.
 *
 * Reads `Authorization: Bearer <jwt>` (forwarded by the Supabase Edge
 * Functions) and verifies the token against Supabase Auth using the
 * service-role client. On success attaches `req.userId` / `req.userEmail`.
 *
 * If Supabase env vars are missing (local dev), requests are allowed
 * through as "dev-anonymous" so the API remains testable offline.
 *
 * NOTE: the legacy header-based `authMiddleware` / `optionalAuth` (which
 * blindly trusted X-User-Id) have been REMOVED — every route now goes
 * through requireAuth.
 */
export async function requireAuth(
  req: Request,
  res: Response,
  next: NextFunction,
): Promise<void> {
  if (!isSupabaseConfigured()) {
    // Dev mode: no Supabase configured — allow anonymous access.
    req.userId = "dev-anonymous";
    req.userEmail = null;
    next();
    return;
  }

  const header = req.headers.authorization;
  const token =
    typeof header === "string" && header.toLowerCase().startsWith("bearer ")
      ? header.slice(7).trim()
      : "";

  if (!token) {
    res
      .status(401)
      .json({ error: "Missing Authorization: Bearer <token> header" });
    return;
  }

  try {
    const { data, error } = await getServiceSupabase().auth.getUser(token);

    if (error || !data?.user?.id) {
      logger.warn(
        { err: error?.message ?? "no user" },
        "requireAuth: invalid or expired token",
      );
      res.status(401).json({ error: "Invalid or expired token" });
      return;
    }

    req.userId = data.user.id;
    req.userEmail = data.user.email ?? null;
    next();
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "Token verification failed";
    logger.error({ err: message }, "requireAuth: verification error");
    res.status(401).json({ error: "Token verification failed" });
  }
}
