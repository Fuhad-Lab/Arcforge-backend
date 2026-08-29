/**
 * Silent Auto-Continue Watchdog.
 *
 * ─────────────────────────────────────────────────────────────────────────
 * WHY THIS FILE EXISTS — "THE AI MIGHT STOP MIDWAY" FIX
 * ─────────────────────────────────────────────────────────────────────────
 * The user-facing pipeline must NEVER surface "the AI stopped" to the user.
 * When an LLM call (or any pipeline phase) fails mid-way, this wrapper:
 *
 *   1. Silently retries the call up to `maxRetries` times with backoff.
 *   2. Logs each retry to the INTERNAL logger only (never to the SSE stream).
 *   3. If all retries exhaust, returns `{ ok: false }` so the caller can
 *      degrade gracefully — emitting a neutral "completed" event rather
 *      than the raw error text.
 *
 * The user sees a smooth, uninterrupted progress stream. Curious devs can
 * still inspect the internal logger to see what was retried.
 * ─────────────────────────────────────────────────────────────────────────
 */

import { logger } from "./logger";

export interface SilentContinueOptions {
  /** Human-readable label for log lines (e.g. "Spec generation", "God Mode pipeline"). */
  label: string;
  /** Max silent retries (default 3 → 4 total attempts). */
  maxRetries?: number;
  /** Base backoff in ms; multiplied by (attempt+1) with jitter. Default 2000. */
  baseDelayMs?: number;
}

export interface SilentContinueResult<T> {
  ok: boolean;
  result?: T;
  /** Present when ok === false. INTERNAL ONLY — never surface to the user. */
  error?: string;
  /** Number of silent retries that occurred. */
  retries: number;
}

/**
 * Run an async function with silent auto-continue. Never throws — always
 * returns a `SilentContinueResult`. The caller decides how to degrade on
 * `ok: false` (typically: emit a neutral "done" event, never the raw error).
 *
 * The wrapped function receives a `resumeHint` string on retry so it can
 * tell the model "continue from where you stopped" if the caller passes it
 * through to the LLM prompt.
 */
export async function runWithSilentAutoContinue<T>(
  fn: (resumeHint: string, attempt: number) => Promise<T>,
  opts: SilentContinueOptions,
): Promise<SilentContinueResult<T>> {
  const max = opts.maxRetries ?? 3;
  const base = opts.baseDelayMs ?? 2000;

  let lastError = "";
  for (let attempt = 0; attempt <= max; attempt++) {
    try {
      const resumeHint =
        attempt === 0
          ? ""
          : `The previous attempt was interrupted. Continue from where you stopped. Do not repeat completed work.`;
      const result = await fn(resumeHint, attempt);
      if (attempt > 0) {
        logger.info(
          { label: opts.label, attempt, retries: attempt },
          "silent auto-continue: phase succeeded after retry",
        );
      }
      return { ok: true, result, retries: attempt };
    } catch (err: unknown) {
      lastError = err instanceof Error ? err.message : String(err);
      // INTERNAL log only — never reaches the SSE stream / user UI.
      logger.warn(
        { label: opts.label, attempt: attempt + 1, totalAttempts: max + 1, err: lastError },
        "silent auto-continue: retrying after failure",
      );
      if (attempt < max) {
        const delay = base * (attempt + 1) + Math.floor(Math.random() * 500);
        await new Promise((r) => setTimeout(r, delay));
        continue;
      }
    }
  }
  return { ok: false, error: lastError, retries: max };
}
