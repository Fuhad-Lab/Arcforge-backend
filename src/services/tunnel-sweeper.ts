/**
 * Tunnel Sweeper — autonomous reverse-tunnel restoration.
 *
 * WHY THIS EXISTS (incident 2026-08-27): reverse-tunnel dials were ONLY
 * triggered by the frontend's agent-info call. A backend deploy restarts
 * the process → every tunnel WS dies → the connections Map starts EMPTY →
 * nothing re-dials for studios that are already open (their VM WebSocket
 * is fine, so no reconnect logic fires) → every in-VM LLM call fails with
 * "reverse-tunnel WS not connected" and swarm tasks died mid-agent.
 *
 * The sweeper runs every 45s (first sweep 10s after boot): for every
 * recent project with a sandbox, it brokers agent-info from the
 * daytona-service (which also refreshes the SIGNED preview URL) and
 * calls ensureReverseTunnel — exactly what the agent-info route does for
 * a logged-in user, but autonomous and for all sandboxes at once.
 *
 * Dials are idempotent (no-op when a tunnel is live) and best-effort per
 * sandbox (a dead VM fails harmlessly on its dial attempt).
 */
import { logger } from "../lib/logger";
import { isSupabaseConfigured, getServiceSupabase } from "../lib/supabase-db";
import { getAgentInfo } from "./daytona-workspace";
import { ensureReverseTunnel } from "./reverse-tunnel-client";

const SWEEP_INTERVAL_MS = 45_000;
const FIRST_SWEEP_DELAY_MS = 10_000;
const MAX_SANDBOXES_PER_SWEEP = 25;

let sweeping = false;
let started = false;

export async function sweepTunnels(): Promise<void> {
  if (sweeping) return;
  sweeping = true;
  let restored = 0;
  try {
    if (!isSupabaseConfigured()) return;
    const client = getServiceSupabase();
    const { data, error } = await client
      .from("projects")
      .select("id, sandbox_id, updated_at")
      .not("sandbox_id", "is", null)
      .order("updated_at", { ascending: false })
      .limit(MAX_SANDBOXES_PER_SWEEP);
    if (error) {
      logger.warn({ err: error.message }, "tunnel sweeper: projects query failed");
      return;
    }
    for (const row of data ?? []) {
      const sandboxId = row.sandbox_id as string | null;
      if (!sandboxId) continue;
      try {
        const info = await getAgentInfo(sandboxId);
        if (info.url && info.alive) {
          ensureReverseTunnel(sandboxId, info.url);
          restored += 1;
        }
      } catch (err) {
        // Best-effort per sandbox — a dead/provisioning VM just fails its dial.
        logger.debug(
          { sandboxId, err: err instanceof Error ? err.message : err },
          "tunnel sweeper: sandbox skipped",
        );
      }
    }
    if (restored > 0) {
      logger.info({ restored }, "tunnel sweeper: ensured tunnels");
    }
  } catch (err) {
    logger.warn(
      { err: err instanceof Error ? err.message : err },
      "tunnel sweeper failed",
    );
  } finally {
    sweeping = false;
  }
}

export function startTunnelSweeper(): void {
  if (started) return;
  started = true;
  setInterval(() => {
    void sweepTunnels();
  }, SWEEP_INTERVAL_MS);
  setTimeout(() => {
    void sweepTunnels();
  }, FIRST_SWEEP_DELAY_MS);
  logger.info(
    { intervalSec: SWEEP_INTERVAL_MS / 1000 },
    "tunnel sweeper started — reverse tunnels now self-restore after deploys",
  );
}
