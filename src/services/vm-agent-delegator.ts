/**
 * VM-Agent Delegator — routes first-generation THROUGH the in-VM agent
 * (and therefore THROUGH the reverse-tunnel), instead of the previous
 * host-side `agentPlatform.runPipeline()` path that called NVIDIA directly
 * from the backend.
 *
 * WHY THIS EXISTS
 * ───────────────
 * The user's prescribed architecture is: the in-VM orchestrator makes LLM
 * calls via the reverse-tunnel (backend dials INTO the VM via the signed
 * `*.daytonaproxy01.eu` URL, the VM sends `req` frames, the backend
 * injects the NVIDIA key + forwards to NVIDIA + streams res/chunk/done
 * back). Follow-up prompts already use this path (the frontend's /ws to
 * the VM triggers the orchestrator's task queue).
 *
 * But first-generation DID NOT — `generate.ts` ran `agentPlatform.runPipeline`
 * host-side (backend → NVIDIA directly), bypassing the tunnel entirely.
 * This delegator fixes that: after the sandbox is provisioned, the backend
 * POSTs the prompt to the VM's `/prompt` endpoint, polls `/status` as the
 * orchestrator runs architect → developer → write_files → debugger (each
 * llm_chat call goes through the reverse-tunnel), and returns the AI's
 * real summary from `/history`. Files are written NATIVELY in the VM by
 * the orchestrator's `_write_files` — no host-side `writeWorkspaceFilesBulk`.
 *
 * HONEST CONTRACT
 * ───────────────
 * This function does NOT fake success. If the VM agent fails (tunnel not
 * connected, LLM timeout, developer phase produced no files), the returned
 * `status` is "failed" and `errorMessage` carries the real reason. The
 * caller (generate.ts) is responsible for emitting an honest SSE event —
 * never a "Done." fallback.
 *
 * PROTOCOL (matches the orchestrator's `/prompt`, `/status`, `/history`):
 *   POST {url}/prompt   {message: prompt}  → {task_id, queued: true}
 *   GET  {url}/status                      → {state, detail, task_id, ...}
 *   GET  {url}/history                     → [{role, content, meta:{result, ...}}, ...]
 *
 *   state transitions: architect → developer → debugger → idle (done)
 *   on failure: worker catches, marks task failed, state → idle
 */
import { getAgentInfo } from "./daytona-workspace";
import { ensureReverseTunnel, isReverseTunnelConnected } from "./reverse-tunnel-client";
import { markVmDelegationActive, markVmDelegationIdle } from "./vm-delegation-registry";
import { logger } from "../lib/logger";

// ─── Configuration ──────────────────────────────────────────────────────
/** How long to wait for the in-VM agent to come alive after provisioning. */
const AGENT_ALIVE_TIMEOUT_MS = 90_000;
/** How long to wait for the backend's reverse-tunnel dial-in to connect. */
const TUNNEL_READY_TIMEOUT_MS = 40_000;
/** Poll interval for /status while the orchestrator runs the pipeline. */
const STATUS_POLL_INTERVAL_MS = 2_000;
/** How long to wait for the orchestrator's pipeline to complete. Nemotron-3.5-
 * lightning is a reasoning model; the developer phase (16384 max_tokens, full
 * app as JSON) takes ~6-10 min once reasoning + code are produced. 15 min
 * matches the nvidia-forwarder fetch cap + gives the full pipeline (architect
 * ~20s + developer ~6-10min + debugger ~30s) room to complete. */
const PIPELINE_TIMEOUT_MS = 900_000;  // 15 min
/** HTTP timeout for each individual call to the VM's orchestrator. */
const VM_HTTP_TIMEOUT_MS = 15_000;

// ─── Types ───────────────────────────────────────────────────────────────
export interface DelegationResult {
  /** The AI's user-facing summary (from /history, last assistant message). */
  summary: string;
  /** "done" if the pipeline completed; "failed" if it errored or timed out. */
  status: "done" | "failed";
  /** Duration of the VM-side pipeline in ms (from POST /prompt to idle). */
  durationMs: number;
  /** The real error reason when status === "failed" (never faked). */
  errorMessage?: string;
  /** The task_id the orchestrator assigned (for log correlation). */
  taskId?: string;
}

export interface DelegationOpts {
  sandboxId: string;
  prompt: string;
  /** Emit an SSE event to the frontend. */
  emit: (event: string, payload: Record<string, unknown>) => void;
}

// ─── Helpers ─────────────────────────────────────────────────────────────
/** Sleep helper. */
function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

/**
 * Wait until the in-VM agent reports installed + alive (polls getAgentInfo).
 * Throws on timeout — the caller surfaces an honest failure.
 */
async function waitForAgentAlive(sandboxId: string, timeoutMs: number): Promise<{ url: string; token: string }> {
  const deadline = Date.now() + timeoutMs;
  let lastInfo: { installed: boolean; alive: boolean; url: string | null; token: string | null } | null = null;
  while (Date.now() < deadline) {
    try {
      const info = await getAgentInfo(sandboxId);
      lastInfo = info;
      if (info.installed && info.alive && info.url && info.token) {
        return { url: info.url, token: info.token };
      }
    } catch (err) {
      logger.warn(
        { sandboxId, err: err instanceof Error ? err.message : String(err) },
        "vm-delegator: agent-info probe failed (will retry)",
      );
    }
    await sleep(2_000);
  }
  const s = lastInfo
    ? `installed=${lastInfo.installed} alive=${lastInfo.alive} url=${lastInfo.url ? "set" : "null"}`
    : "no agent-info response";
  throw new Error(`in-VM agent did not come alive within ${timeoutMs / 1000}s (${s})`);
}

/**
 * Wait until the backend's reverse-tunnel connection to the VM is open.
 * The orchestrator's llm_chat will fail with "backend hasn't dialed in"
 * if this isn't connected, so we MUST wait here.
 */
async function waitForTunnelConnected(sandboxId: string, timeoutMs: number): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (isReverseTunnelConnected(sandboxId)) return;
    await sleep(1_000);
  }
  throw new Error(`reverse-tunnel did not connect within ${timeoutMs / 1000}s`);
}

/** POST the prompt to the VM's /prompt endpoint; return the task_id. */
async function enqueueVmTask(url: string, token: string, prompt: string): Promise<string> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), VM_HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(`${url}/prompt`, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        "Authorization": `Bearer ${token}`,
      },
      body: JSON.stringify({ message: prompt }),
      signal: controller.signal,
    });
    if (!res.ok) {
      const text = await res.text().catch(() => "<unreadable>");
      throw new Error(`VM /prompt HTTP ${res.status}: ${text.slice(0, 300)}`);
    }
    const data = (await res.json()) as { task_id?: string; queued?: boolean };
    if (!data.task_id) {
      throw new Error("VM /prompt returned no task_id");
    }
    return data.task_id;
  } finally {
    clearTimeout(t);
  }
}

/** Fetch the VM's /status (the orchestrator's current active state). */
async function fetchVmStatus(url: string, token: string): Promise<{
  state: string;
  detail?: string;
  task_id?: string;
  llm_ready?: boolean;
}> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), VM_HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(`${url}/status`, {
      headers: { "Authorization": `Bearer ${token}` },
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`VM /status HTTP ${res.status}`);
    }
    return (await res.json()) as { state: string; detail?: string; task_id?: string; llm_ready?: boolean };
  } finally {
    clearTimeout(t);
  }
}

/** Fetch the VM's /history (chat rows; the last assistant message is the summary). */
async function fetchVmHistory(url: string, token: string): Promise<Array<{
  role: string;
  content: string;
  meta?: { result?: { summary?: string; files?: string[]; checks?: { ok?: boolean; issues?: string[] } } } | Record<string, unknown>;
}>> {
  const controller = new AbortController();
  const t = setTimeout(() => controller.abort(), VM_HTTP_TIMEOUT_MS);
  try {
    const res = await fetch(`${url}/history`, {
      headers: { "Authorization": `Bearer ${token}` },
      signal: controller.signal,
    });
    if (!res.ok) {
      throw new Error(`VM /history HTTP ${res.status}`);
    }
    return (await res.json()) as Array<{ role: string; content: string; meta?: { result?: { summary?: string; files?: string[] } } | Record<string, unknown> }>;
  } finally {
    clearTimeout(t);
  }
}

/** Map the orchestrator's state to an SSE activity event (deduped by state). */
function emitStateActivity(
  state: string,
  detail: string | undefined,
  emit: DelegationOpts["emit"],
  lastStateRef: { value: string },
): void {
  if (state === lastStateRef.value) return;
  lastStateRef.value = state;
  let label = "Working";
  let kind = "generate";
  switch (state) {
    case "architect":
      label = "Planning the build";
      kind = "think";
      break;
    case "developer":
      label = "Writing code";
      kind = "generate";
      break;
    case "debugger":
      label = "Verifying the build";
      kind = "think";
      break;
    case "idle":
      // Don't emit for idle — the caller emits the final done.
      return;
    default:
      label = state;
  }
  emit("activity", { label, status: "active", kind, detail: detail || "" });
}

// ─── Main delegator ─────────────────────────────────────────────────────
export async function delegateGenerationToVmAgent(opts: DelegationOpts): Promise<DelegationResult> {
  // Register the in-flight delegation so the tunnel sweeper keeps this
  // sandbox's reverse tunnel eligible (and never parks/reaps it mid-build).
  // EVERY exit path — including the early failure returns below — clears
  // the mark via finally, otherwise a failed build would keep the sandbox
  // protected from quota hygiene forever.
  markVmDelegationActive(opts.sandboxId);
  try {
    return await delegateGenerationInner(opts);
  } finally {
    markVmDelegationIdle(opts.sandboxId);
  }
}

async function delegateGenerationInner(opts: DelegationOpts): Promise<DelegationResult> {
  const { sandboxId, prompt, emit } = opts;
  const started = Date.now();

  emit("activity", { label: "Connecting to in-VM agent", status: "active", kind: "think" });

  // 1. Wait for the in-VM agent to be installed + alive.
  let url: string;
  let token: string;
  try {
    ({ url, token } = await waitForAgentAlive(sandboxId, AGENT_ALIVE_TIMEOUT_MS));
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    logger.error({ sandboxId, err: errorMessage }, "vm-delegator: agent never came alive");
    return { summary: "", status: "failed", durationMs: Date.now() - started, errorMessage };
  }

  // 2. Trigger the backend's reverse-tunnel dial-in + wait for it to connect.
  //    This is the critical step — without it, the orchestrator's llm_chat
  //    fails with "backend hasn't dialed in".
  ensureReverseTunnel(sandboxId, url);
  emit("activity", { label: "Establishing reverse-tunnel bridge", status: "active", kind: "think" });
  try {
    await waitForTunnelConnected(sandboxId, TUNNEL_READY_TIMEOUT_MS);
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    logger.error({ sandboxId, err: errorMessage }, "vm-delegator: reverse-tunnel never connected");
    return { summary: "", status: "failed", durationMs: Date.now() - started, errorMessage };
  }
  emit("activity", { label: "Reverse-tunnel bridge live", status: "done", kind: "think" });

  // 3. Enqueue the prompt on the VM's task queue.
  let taskId: string;
  try {
    taskId = await enqueueVmTask(url, token, prompt);
    logger.info({ sandboxId, taskId }, "vm-delegator: enqueued prompt on VM agent");
  } catch (err) {
    const errorMessage = err instanceof Error ? err.message : String(err);
    logger.error({ sandboxId, err: errorMessage }, "vm-delegator: /prompt POST failed");
    return { summary: "", status: "failed", durationMs: Date.now() - started, errorMessage };
  }

  // 4. Poll /status until the orchestrator returns to idle (pipeline done)
  //    or the timeout fires.
  const lastState = { value: "" };
  const pollDeadline = Date.now() + PIPELINE_TIMEOUT_MS;
  let timedOut = false;
  while (Date.now() < pollDeadline) {
    try {
      const status = await fetchVmStatus(url, token);
      emitStateActivity(status.state, status.detail, emit, lastState);
      if (status.state === "idle" && lastState.value !== "") {
        // The orchestrator went architect→developer→debugger→idle.
        // (We only break on idle AFTER a non-empty state was seen, so we
        // don't break on the initial idle before the task starts.)
        break;
      }
    } catch (err) {
      logger.warn(
        { sandboxId, err: err instanceof Error ? err.message : String(err) },
        "vm-delegator: /status poll failed (will retry)",
      );
    }
    await sleep(STATUS_POLL_INTERVAL_MS);
  }
  if (Date.now() >= pollDeadline) {
    timedOut = true;
  }

  // 5. Fetch /history — the last assistant message is the AI's summary.
  let summary = "";
  let resultMeta: { summary?: string; files?: string[]; checks?: { ok?: boolean; issues?: string[] } } | undefined;
  try {
    const history = await fetchVmHistory(url, token);
    const lastAssistant = [...history].reverse().find((m) => m.role === "assistant");
    if (lastAssistant) {
      summary = (lastAssistant.content || "").trim();
      const meta = lastAssistant.meta as { result?: { summary?: string; files?: string[]; checks?: { ok?: boolean; issues?: string[] } } } | undefined;
      resultMeta = meta?.result;
      if (resultMeta?.summary) {
        summary = resultMeta.summary;
      }
    }
  } catch (err) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : String(err) },
      "vm-delegator: /history fetch failed",
    );
  }

  const durationMs = Date.now() - started;

  // 6. Honest status determination.
  if (timedOut) {
    return {
      summary: summary || "",
      status: "failed",
      durationMs,
      errorMessage: `in-VM pipeline timed out after ${PIPELINE_TIMEOUT_MS / 1000}s`,
      taskId,
    };
  }
  // The orchestrator emits a chat message on BOTH success (the summary)
  // and failure (the error text). Distinguish by checking the result meta:
  //   - success: meta.result exists with files + checks
  //   - failure: meta.result absent (the _mark_failed path doesn't set result)
  const filesWritten = resultMeta?.files ?? [];
  if (filesWritten.length === 0) {
    return {
      summary: summary || "",
      status: "failed",
      durationMs,
      errorMessage: summary
        ? `in-VM agent reported: ${summary.slice(0, 300)}`
        : "in-VM agent produced no files (developer phase may have failed)",
      taskId,
    };
  }

  logger.info(
    { sandboxId, taskId, filesWritten: filesWritten.length, durationMs },
    "vm-delegator: generation complete (via reverse-tunnel)",
  );
  return {
    summary: summary || "Build complete.",
    status: "done",
    durationMs,
    taskId,
  };
}
