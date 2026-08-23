import type { Request, Response, NextFunction } from "express";
import { logger } from "../lib/logger";

/**
 * Auth middleware: Extracts user context from headers set by Supabase Edge Functions.
 *
 * Expected headers:
 *   X-User-Id     — the Supabase auth user UUID
 *   X-Project-Id  — (optional) the project UUID for scoped operations
 *
 * These are attached by the edge-function middleware layer.
 */
export function authMiddleware(req: Request, res: Response, next: NextFunction): void {
  const userId = req.headers["x-user-id"] as string | undefined;

  if (!userId) {
    // In dev/testing mode, allow anonymous access
    if (process.env.NODE_ENV !== "production") {
      (req as any).userId = req.body?.userId || "dev-anonymous";
      return next();
    }
    res.status(401).json({ error: "Missing X-User-Id header" });
    return;
  }

  (req as any).userId = userId;
  const projectId = req.headers["x-project-id"] as string | undefined;
  if (projectId) (req as any).projectId = projectId;

  next();
}

/**
 * Optional auth: sets userId if present, doesn't block if missing.
 */
export function optionalAuth(req: Request, _res: Response, next: NextFunction): void {
  const userId = req.headers["x-user-id"] as string | undefined;
  if (userId) (req as any).userId = userId;
  else (req as any).userId = req.body?.userId || "anonymous";
  next();
}
