/**
 * VM Delegation Registry — in-memory tracking of sandboxes whose
 * generation task the backend is ACTIVELY awaiting.
 *
 * WHY THIS EXISTS (incident 2026-08-27 "sandbox is full, again"):
 * The tunnel sweeper used to ensure reverse tunnels for EVERY project
 * that ever had a sandbox_id — forever. Every ensure→getAgentInfo hop
 * is a Daytona API read, and ANY read refreshes the sandbox's
 * lastActivityAt, so sandboxes never idled out: auto-stop (30 min)
 * never fired, the quota reaper never matched, and two stale 4 GiB
 * sandboxes were enough to exhaust the 10 GiB org quota and block all
 * new builds.
 *
 * The registry gives the sweeper a PRECISE "this VM is mid-build"
 * signal (the backend is polling its /status right now) that does not
 * depend on Supabase's projects.updated_at — which VM-driven flows
 * (direct frontend→daemon prompts, approval loops) never bump.
 */

const active = new Map<string, number>();

/** Timestamp (ms) the delegation started, or null when none is in flight. */
export function delegationStartedAt(sandboxId: string): number | null {
  return active.get(sandboxId) ?? null;
}

/** Mark a sandbox as having an in-flight backend-driven generation. */
export function markVmDelegationActive(sandboxId: string): void {
  if (!sandboxId) return;
  active.set(sandboxId, Date.now());
}

/** Mark the delegation finished — the sandbox is no longer protected. */
export function markVmDelegationIdle(sandboxId: string): void {
  active.delete(sandboxId);
}

/** Sandbox ids currently protected by an in-flight delegation. */
export function activeDelegationIds(): string[] {
  return [...active.keys()];
}
