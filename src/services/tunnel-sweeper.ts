/**
 * Tunnel Sweeper — autonomous reverse-tunnel restoration WITH quota
 * hygiene.
 *
 * WHY THIS EXISTS (incident 2026-08-27): reverse-tunnel dials were ONLY
 * triggered by the frontend's agent-info call. A backend deploy restarts
 * the process → every tunnel WS dies → the connections Map starts EMPTY →
 * nothing re-dials for studios that are already open → every in-VM LLM
 * call fails with "reverse-tunnel WS not connected" and swarm tasks died
 * mid-agent.
 *
 * THE ORIGINAL FIX BECAME A LEAK. The first version swept EVERY project
 * with a sandbox_id every 45s — forever. Each sweep called getAgentInfo
 * (a Daytona read) per sandbox, and ANY Daytona read refreshes the
 * sandbox's lastActivityAt. Result: nothing ever looked idle → the 30-min
 * auto-stop never fired → the quota reaper never matched → stale 4 GiB
 * sandboxes accumulated until the 10 GiB org quota was exhausted and new
 * builds failed ("the sandbox is full"). Worse, each tunnel it ensured
 * carries a NEVER-GIVE-UP reconnect loop with 15s pings, which keeps an
 * abandoned sandbox's activity fresh all on its own.
 *
 * v2 — ELIGIBILITY-GATED SWEEPING. A sandbox keeps its tunnel ONLY while
 * it is plausibly in use:
 *
 *   1. IN-FLIGHT DELEGATION — the backend is actively awaiting a VM task
 *      (vm-delegation-registry). Precise signal; covers backend-driven
 *      builds regardless of row staleness.
 *   2. RECENT PROJECT — projects.updated_at within ACTIVE_WINDOW (20 min):
 *      a user interacted with this project just now (open studio, fresh
 *      build, editor write).
 *   3. GRACE PROBE — projects.updated_at within GRACE_WINDOW (2h): probe
 *      the VM daemon's /status once per sweep. A non-idle daemon means
 *      the swarm is still working (VM-driven builds and approval waits
 *      never bump projects.updated_at) → keep the tunnel. An idle daemon
 *      → disconnect AND PARK (never probe again while the project stays
 *      stale) so the sandbox's lastActivityAt finally ages and the
 *      daytona-service quota reaper can delete it ~30 min later.
 *   4. STALE — older than GRACE_WINDOW: disconnect. The reaper will
 *      reclaim the quota.
 *
 * Parking is what breaks the poison loop: the sweeper stops touching a
 * sandbox the moment it is provably idle, so Daytona's activity clock
 * starts ticking for real.
 */
import { logger } from "../lib/logger";
import { isSupabaseConfigured, getServiceSupabase } from "../lib/supabase-db";
import { getAgentInfo } from "./daytona-workspace";
import {
  ensureReverseTunnel,
  disconnectReverseTunnel,
  isReverseTunnelConnected,
} from "./reverse-tunnel-client";
import { delegationStartedAt } from "./vm-delegation-registry";

const SWEEP_INTERVAL_MS = 60_000;
const FIRST_SWEEP_DELAY_MS = 10_000;
const MAX_SANDBOXES_PER_SWEEP = 25;

/** Recent project-row activity ⇒ definitely in use. */
const ACTIVE_WINDOW_MS = 20 * 60_000;
/** Beyond this, a sandbox is stale: disconnect and let the reaper delete it. */
const GRACE_WINDOW_MS = 2 * 60 * 60_000;
/** Per-sandbox daemon probe timeout. */
const DAEMON_PROBE_TIMEOUT_MS = 8_000;

/** Sandboxes the sweeper has declared idle (daemon reported "idle") and
 *  must NOT be probed again while their project stays stale. Cleared the
 *  moment the project shows fresh activity or a delegation starts. */
const parked = new Map<string, number>();

let sweeping = false;
let started = false;

interface ProjectRow {
  id: string;
  sandbox_id: string | null;
  updated_at: string;
}

/** Fetch the daemon's orchestrator state ("idle" when nothing runs). */
async function probeDaemonState(
  url: string,
  token: string,
): Promise<string | null> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), DAEMON_PROBE_TIMEOUT_MS);
  try {
    const res = await fetch(`${url.replace(/\/+$/, "")}/status`, {
      headers: { "X-Agent-Token": token },
      signal: controller.signal,
    });
    if (!res.ok) return null;
    const body = (await res.json()) as { state?: string };
    return typeof body.state === "string" ? body.state : null;
  } catch {
    return null;
  } finally {
    clearTimeout(timer);
  }
}

function ageMs(updatedAt: string): number {
  const t = Date.parse(updatedAt);
  if (Number.isNaN(t)) return Number.POSITIVE_INFINITY;
  return Date.now() - t;
}

export async function sweepTunnels(): Promise<void> {
  if (sweeping) return;
  sweeping = true;
  let kept = 0;
  let disconnected = 0;
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

    const seen = new Set<string>();
    for (const row of (data ?? []) as ProjectRow[]) {
      const sandboxId = row.sandbox_id;
      if (!sandboxId) continue;
      seen.add(sandboxId);

      const delegationActive = delegationStartedAt(sandboxId) !== null;

      // 1. In-flight backend delegation — always keep.
      if (delegationActive) {
        parked.delete(sandboxId);
        await ensureTunnelFor(sandboxId);
        kept += 1;
        continue;
      }

      const age = ageMs(row.updated_at);

      // 2. Recent project activity — keep, and un-park.
      if (age <= ACTIVE_WINDOW_MS) {
        parked.delete(sandboxId);
        await ensureTunnelFor(sandboxId);
        kept += 1;
        continue;
      }

      // 4. Stale project — disconnect (reaper will reclaim the quota).
      if (age > GRACE_WINDOW_MS) {
        if (disconnectReverseTunnelIfPresent(sandboxId)) disconnected += 1;
        parked.delete(sandboxId);
        continue;
      }

      // 3. Grace window — probe the daemon, but never re-probe a parked
      //    (already-judged-idle) sandbox: probing refreshes lastActivityAt
      //    and would keep the sandbox alive forever.
      if (parked.has(sandboxId)) {
        if (disconnectReverseTunnelIfPresent(sandboxId)) disconnected += 1;
        continue;
      }

      let info;
      try {
        info = await getAgentInfo(sandboxId);
      } catch (err) {
        logger.debug(
          { sandboxId, err: err instanceof Error ? err.message : err },
          "tunnel sweeper: sandbox skipped",
        );
        continue;
      }
      if (!info.alive || !info.url || !info.token) {
        // Dead or not-yet-installed sidecar: nothing to keep alive. Park it
        // so we stop paying a Daytona read every sweep.
        parked.set(sandboxId, Date.now());
        if (disconnectReverseTunnelIfPresent(sandboxId)) disconnected += 1;
        continue;
      }
      const state = await probeDaemonState(info.url, info.token);
      if (state !== null && state !== "idle") {
        // The swarm is working (VM-driven build, approval wait, debugger) —
        // its LLM calls need the tunnel even though the project row is stale.
        await ensureReverseTunnel(sandboxId, info.url);
        kept += 1;
      } else {
        // Daemon idle (or unreachable): park + disconnect so the sandbox
        // can finally idle out and be reaped.
        parked.set(sandboxId, Date.now());
        if (disconnectReverseTunnelIfPresent(sandboxId)) disconnected += 1;
      }
    }

    // Prune parked entries for sandboxes no longer in the project list.
    for (const id of parked.keys()) {
      if (!seen.has(id)) parked.delete(id);
    }

    if (kept > 0 || disconnected > 0) {
      logger.info(
        { kept, disconnected, parked: parked.size },
        "tunnel sweeper: eligibility sweep complete",
      );
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

async function ensureTunnelFor(sandboxId: string): Promise<void> {
  try {
    const info = await getAgentInfo(sandboxId);
    if (info.url && info.alive) {
      ensureReverseTunnel(sandboxId, info.url);
    }
  } catch (err) {
    logger.debug(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "tunnel sweeper: sandbox skipped",
    );
  }
}

function disconnectReverseTunnelIfPresent(sandboxId: string): boolean {
  // disconnectReverseTunnel is a no-op when no connection exists; we only
  // count (and log) actual retirements of live state by checking first.
  const wasConnected = isReverseTunnelConnected(sandboxId);
  disconnectReverseTunnel(sandboxId);
  if (wasConnected) {
    logger.info({ sandboxId }, "tunnel sweeper: retired reverse tunnel (ineligible)");
  }
  return wasConnected;
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
    {
      intervalSec: SWEEP_INTERVAL_MS / 1000,
      activeWindowMin: ACTIVE_WINDOW_MS / 60_000,
      graceWindowMin: GRACE_WINDOW_MS / 60_000,
    },
    "tunnel sweeper v2 started — eligibility-gated tunnels + quota hygiene",
  );
}
