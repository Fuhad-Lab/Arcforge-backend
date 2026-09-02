/**
 * Forgvi Engine — the goal-completion loop.
 *
 * The loop that separates Forgvi 2.0 from a chat loop:
 *
 *   goal (objective + acceptance criteria — the engine's law)
 *     → chief iteration (prime-agent kernel, real tools, real artifacts)
 *     → independent verification (the judge scores every criterion)
 *     → completion decision (complete | continue | escalate)
 *     → repeat within the iteration budget
 *     → completion report with evidence, score, and remaining issues
 *
 * Claims are not proof: the chief cannot finish the run; only the judge
 * can, by passing every acceptance criterion.
 */

import { randomUUID } from "node:crypto";
import { readdirSync } from "node:fs";
import { resolve } from "node:path";
import { Journal, recoverJournal } from "./journal.js";
import { ENGINE_IN_VM, createChiefSession, lastAssistantText } from "./kernel.js";
import { RunInteractions } from "./connector-tools.js";
import { verifyWorkspaceGrant } from "./grant.js";
import { verifyAcceptance } from "./verify.js";

/** Clamp helper. */
const clamp = (n, min, max) => Math.min(Math.max(n, min), max);

/**
 * DeltaPump — forwards chief streaming to the journal without flooding SSE.
 *
 * Chief text: the frontend REPLACES the assistant bubble content with each
 * `assistant_text` event, so we always send the FULL accumulated text.
 * Chief thinking: the frontend APPENDS; we send batched deltas.
 */
class DeltaPump {
  constructor(journal, iteration) {
    this.journal = journal;
    this.iteration = iteration;
    this.text = "";
    this.thinkingBatch = "";
    this.lastTextFlush = 0;
    this.lastTextFlushLen = 0;
    this.lastThinkingFlush = 0;
    this.textDirty = false;
  }

  onTextDelta(delta) {
    this.text += delta;
    this.textDirty = true;
    const now = Date.now();
    // Flush when 400+ new chars accumulated or 600ms since the last flush,
    // or when the message looks finished (deltas arrive fast; the final
    // flush happens in finish()).
    if (this.text.length - this.lastTextFlushLen >= 400 || now - this.lastTextFlush >= 600) {
      this.flushText();
    }
  }

  flushText() {
    if (!this.textDirty) return;
    this.lastTextFlush = Date.now();
    this.lastTextFlushLen = this.text.length;
    this.textDirty = false;
    this.journal.emit({ type: "assistant_text", text: this.text }, { role: "chief", iteration: this.iteration });
  }

  onThinkingDelta(delta) {
    this.thinkingBatch += delta;
    const now = Date.now();
    if (this.thinkingBatch.length >= 240 || now - this.lastThinkingFlush >= 1200) {
      this.flushThinking();
    }
  }

  flushThinking() {
    if (!this.thinkingBatch) return;
    const batch = this.thinkingBatch;
    this.thinkingBatch = "";
    this.lastThinkingFlush = Date.now();
    this.journal.emit({ type: "assistant_thinking", text: batch }, { role: "chief", iteration: this.iteration });
  }

  /** Final flush when the chief turn ends. */
  finish() {
    this.flushThinking();
    this.flushText();
  }
}

/** Normalize + clamp the client-supplied budgets (the engine sets the ceiling). */
function normalizeBudgets(budgets) {
  const b = budgets ?? {};
  return {
    maxIterations: clamp(Number(b.maxIterations) || 6, 1, 8),
    wallClockMs: clamp(Number(b.wallClockMs) || 30 * 60_000, 60_000, 30 * 60_000),
    tokenBudget: clamp(Number(b.tokenBudget) || 500_000, 10_000, 500_000),
  };
}

/**
 * RunManager — the registry of live runs.
 *
 * PERSISTENCE (in-VM mode): when ENGINE_PERSIST_DIR is set, the manager
 * scans it at construction and rebuilds every run recorded on disk — the
 * same disk the journal appends to, so a PM2 restart (crash, OOM, VM
 * stop/start) never loses history. Finished runs replay exactly as they
 * ended; interrupted runs get ONE honest terminal event appended
 * (run_finished status "incomplete", reason "engine restarted") so every
 * re-attaching frontend settles immediately instead of hanging on a run
 * that no longer exists. The host (Render) engine has no persist dir and
 * keeps its in-memory-only behavior.
 */
export class RunManager {
  constructor({
    maxConcurrent = Number(process.env.ENGINE_MAX_CONCURRENT ?? 3),
    persistDir = process.env.ENGINE_PERSIST_DIR ?? null,
  } = {}) {
    this.maxConcurrent = Math.max(1, Math.floor(maxConcurrent));
    this.persistDir = persistDir ? resolve(persistDir) : null;
    /** @type {Map<string, any>} */
    this.runs = new Map();
    if (this.persistDir) {
      try {
        this.#recoverPersistedRuns();
      } catch (error) {
        console.warn("[forgvi] run recovery failed:", error?.message ?? error);
      }
    }
  }

  /** Boot recovery: rebuild runs from <persistDir>/<runId>.ndjson files. */
  #recoverPersistedRuns() {
    const files = readdirSync(this.persistDir)
      .filter((f) => f.endsWith(".ndjson"))
      .sort();
    let recovered = 0;
    let finalized = 0;
    for (const file of files) {
      const runId = file.slice(0, -".ndjson".length);
      if (this.runs.has(runId)) continue;
      const rec = recoverJournal(runId, `recovered-${runId.slice(0, 8)}`, runId, this.persistDir);
      if (!rec) continue;
      const first = rec.journal.entries.find((e) => e.event?.type === "run_started");
      const run = {
        runId,
        sessionId: `recovered-${runId.slice(0, 8)}`,
        goalId: runId,
        objective: first?.event?.objective ?? "(recovered run)",
        acceptance: first?.event?.acceptance ?? [],
        budgets: first?.event?.budgets ?? normalizeBudgets(null),
        workspace: recoveredWorkspace(runId),
        status: "incomplete",
        iteration: rec.journal.entries.at(-1)?.iteration ?? 0,
        startedAt: rec.journal.entries[0]?.ts ?? Date.now(),
        finishedAt: rec.lastFinish?.ts ?? Date.now(),
        abortRequested: false,
        report: null,
        journal: rec.journal,
        evidence: [],
        lastChiefReport: "",
        promise: null,
        recovered: true,
      };
      if (rec.finished) {
        const status = String(rec.lastFinish?.event?.status ?? "incomplete");
        run.status = status === "complete" ? "complete" : "incomplete";
        run.finishedAt = rec.lastFinish?.ts ?? Date.now();
        run.report = {
          goalId: run.goalId,
          runId,
          status: run.status,
          summary: String(rec.lastFinish?.event?.summary ?? ""),
          verificationScore: Number(rec.lastFinish?.event?.verificationScore ?? 0),
          remainingIssues: (rec.lastFinish?.event?.remainingIssues ?? []),
          iterations: Number(rec.lastFinish?.event?.iterations ?? 0),
          durationMs: Number(rec.lastFinish?.event?.durationMs ?? 0),
          evidence: [],
        };
        recovered += 1;
      } else {
        // Interrupted mid-flight (crash / restart / VM stop): append the
        // ONE honest terminal event the contract promises, then close.
        run.report = {
          goalId: run.goalId,
          runId,
          status: "incomplete",
          summary: "The engine restarted while this run was in flight. Everything built so far is saved in the workspace — check the Files tab.",
          verificationScore: 0,
          remainingIssues: ["run interrupted by engine restart"],
          iterations: run.iteration,
          durationMs: run.finishedAt - run.startedAt,
          evidence: [],
        };
        run.journal.emit(
          {
            type: "run_finished",
            status: "incomplete",
            summary: run.report.summary,
            verificationScore: 0,
            iterations: run.iteration,
            durationMs: run.report.durationMs,
            remainingIssues: run.report.remainingIssues,
            interrupted: true,
          },
          {},
        );
        run.journal.close();
        finalized += 1;
      }
      this.runs.set(runId, run);
    }
    if (files.length > 0) {
      console.log(
        `[forgvi] run recovery: ${recovered} finished + ${finalized} finalized (interrupted) from ${this.persistDir}`,
      );
    }
  }

  /**
   * Create + start a run. Returns the run record immediately; the loop
   * continues in the background. Throws on validation/overload.
   *
   * `workspaceGrant` (optional) binds the run to a Daytona sandbox: a
   * short-lived HMAC token minted by the Forge backend AFTER it verified the
   * requesting user owns the project. A bad/expired/forged token is rejected
   * with an honest 4xx — the run never silently falls back to the engine's
   * local disk when the caller asked for the project workspace (that would
   * split the workspace in two: the user's VM and a hidden engine copy).
   */
  start({ objective, acceptance, budgets, workspaceGrant }) {
    const cleanObjective = String(objective ?? "").trim();
    const cleanAcceptance = (Array.isArray(acceptance) ? acceptance : [])
      .map((a) => String(a ?? "").trim())
      .filter(Boolean);

    // The engine's law: a goal without acceptance criteria is invalid.
    if (!cleanObjective) throw new Error("objective is required");
    if (cleanAcceptance.length === 0) throw new Error("acceptance criteria are required — a goal without acceptance criteria is invalid");

    if (this.countActive() >= this.maxConcurrent) {
      throw new Error("engine at max concurrent runs — try again shortly");
    }

    // Workspace binding, per mode:
    //
    // IN-VM: the engine process itself lives inside the project's Daytona
    // sandbox, so the run is bound to THIS VM by construction (sandbox id
    // + project id come from the installer's env). The grant is neither
    // required nor verified here — reachability is already capability-
    // gated upstream (only the backend's ownership-checked agent-info
    // brokers the engine's signed preview URL to the project's owner, and
    // the engine can only ever touch its own VM).
    //
    // HOST (Render): the grant path is the trust boundary. The token is
    // optional (local dev), but when present it must be valid — a run
    // asked for the project workspace must never silently fall back to
    // the engine's local disk (that would split the workspace in two:
    // the user's VM and a hidden engine copy).
    let workspace = null;
    if (ENGINE_IN_VM) {
      workspace = {
        sandboxId: process.env.ENGINE_SANDBOX_ID || "this-vm",
        projectId: process.env.ENGINE_PROJECT_ID || null,
        userId: null,
        local: true,
      };
    } else if (workspaceGrant != null && String(workspaceGrant).trim() !== "") {
      const secret = process.env.WORKSPACE_GRANT_SECRET;
      if (!secret) {
        throw new Error("workspace grant rejected: WORKSPACE_GRANT_SECRET is not configured on the engine");
      }
      const claims = verifyWorkspaceGrant(String(workspaceGrant), { secret });
      if (!claims) {
        const error = new Error("workspace grant rejected: invalid or expired — reload the studio and send the request again");
        error.validation = true;
        throw error;
      }
      workspace = claims;
    }

    const runId = randomUUID();
    const sessionId = randomUUID();
    const goalId = randomUUID();
    const clamped = normalizeBudgets(budgets);

    const run = {
      runId,
      sessionId,
      goalId,
      objective: cleanObjective,
      acceptance: cleanAcceptance,
      budgets: clamped,
      workspace,
      status: "running",
      iteration: 0,
      startedAt: Date.now(),
      finishedAt: null,
      abortRequested: false,
      report: null,
      journal: new Journal(runId, sessionId, goalId, { persistDir: this.persistDir }),
      /** ask_user registry — the journal carries questions to the studio,
       * POST /runs/:id/answer resolves them (see server.js). */
      interactions: new RunInteractions(journal),
      /** @type {Array<{kind: string, name: string, status: string}>} */
      evidence: [],
      lastChiefReport: "",
      promise: null,
    };

    this.runs.set(runId, run);
    run.promise = this.#execute(run).catch((error) => {
      // The last-resort guard: no run ends without a terminal event.
      if (run.status === "running") {
        run.status = "incomplete";
        run.finishedAt = Date.now();
        run.journal.emit({ type: "run_error", error: String(error?.message ?? error) });
        run.journal.emit({
          type: "run_finished",
          status: "incomplete",
          summary: "The run failed with an internal error.",
          verificationScore: 0,
          iterations: run.iteration,
          durationMs: Date.now() - run.startedAt,
          remainingIssues: [String(error?.message ?? error)],
        });
        run.journal.close();
      }
    });
    return run;
  }

  get(runId) {
    return this.runs.get(runId) ?? null;
  }

  countActive() {
    let n = 0;
    for (const run of this.runs.values()) if (run.status === "running") n++;
    return n;
  }

  abort(runId, reason = "user abort") {
    const run = this.runs.get(runId);
    if (!run || run.status !== "running") return false;
    run.abortRequested = true;
    run.abortReason = reason;
    return true;
  }

  /** Public view of a run (GET /runs/:id). */
  view(run) {
    return {
      runId: run.runId,
      goalId: run.goalId,
      status: run.status,
      objective: run.objective,
      acceptance: run.acceptance,
      iteration: run.iteration,
      budgets: run.budgets,
      workspace: run.workspace
        ? { sandboxId: run.workspace.sandboxId, projectId: run.workspace.projectId, bound: true }
        : { bound: false },
      startedAt: run.startedAt,
      finishedAt: run.finishedAt,
      report: run.report,
      eventCount: run.journal.seq,
      // Unresolved ask_user questions (the studio re-renders their cards
      // on remount without replaying the whole journal).
      pendingQuestions: run.interactions
        ? [...run.interactions.pending.keys()]
        : [],
    };
  }

  async #execute(run) {
    const { journal } = run;
    journal.emit(
      {
        type: "run_started",
        objective: run.objective,
        acceptance: run.acceptance,
        budgets: run.budgets,
      },
      {},
    );

    // Workspace binding is part of the run's public record: the studio
    // shows the Files/Preview tabs from the VM, so it needs to know the run
    // is VM-bound (and which sandbox) to refresh the tree live.
    if (run.workspace) {
      journal.emit(
        {
          type: "workspace_bound",
          sandboxId: run.workspace.sandboxId,
          projectId: run.workspace.projectId,
          workspaceRoot: "/workspace",
        },
        {},
      );
    }

    const { session, vm } = await createChiefSession({
      runId: run.runId,
      sessionId: run.sessionId,
      workspace: run.workspace,
      interactions: run.interactions,
    });
    run.chiefSession = session;

    // Warm the daytona-service before the first chief command so a Render
    // free-tier cold start doesn't burn the chief's first tool call. Best
    // effort — the exec path retries on its own anyway. (In-VM runs have no
    // REST hop to warm — the tools execute natively in this VM.)
    if (vm) {
      vm.client.ping().catch(() => {});
    }

    // Track tool evidence from the kernel's own event stream:
    // tool_execution_start carries the args; tool_execution_end carries the
    // result + status. The judge sees commands AND their outputs.
    //
    // VM-bound runs ALSO journal each tool call (tool_used): the studio
    // refreshes its Files-tab tree from the VM when it sees one, so 2.0
    // artifacts appear live exactly like the 1.0 swarm's.
    const pendingArgs = new Map();
    const unsubTools = session.subscribe((event) => {
      if (event.type === "tool_execution_start") {
        pendingArgs.set(event.toolCallId, event.args);
      } else if (event.type === "tool_execution_end") {
        const args = pendingArgs.get(event.toolCallId) ?? event.args;
        pendingArgs.delete(event.toolCallId);
        const evidence = {
          kind: "tool",
          name: `${event.toolName}: ${summarizeArgs(args)}`,
          status: event.isError ? "fail" : "pass",
          output: summarizeResult(event.result),
        };
        run.evidence.push(evidence);
        if (run.workspace) {
          journal.emit(
            {
              type: "tool_used",
              tool: event.toolName,
              status: evidence.status,
              detail: evidence.name.slice(0, 200),
            },
            { role: "chief", iteration: run.iteration || undefined },
          );
        }
      }
    });

    let previousGaps = [];
    let escalate = false;

    try {
      for (
        let iteration = 1;
        iteration <= run.budgets.maxIterations && run.status === "running";
        iteration++
      ) {
        // Wall-clock budget check.
        if (Date.now() - run.startedAt > run.budgets.wallClockMs) {
          journal.emit({ type: "run_error", error: "wall clock budget exhausted" });
          run.status = "incomplete";
          break;
        }

        run.iteration = iteration;
        journal.emit({ type: "iteration_started", iteration }, { iteration });

        // ── Chief turn ────────────────────────────────────────────────
        const pump = new DeltaPump(journal, iteration);
        const chiefPrompt =
          iteration === 1
            ? buildFirstPrompt(run)
            : buildFollowUpPrompt({ run, gaps: previousGaps, escalate, iteration });

        const unsubStream = session.subscribe((event) => {
          if (event.type !== "message_update" || !event.assistantMessageEvent) return;
          const kind = event.assistantMessageEvent.type;
          if (kind === "text_delta" && event.assistantMessageEvent.delta) {
            pump.onTextDelta(event.assistantMessageEvent.delta);
          } else if (kind === "thinking_delta" && event.assistantMessageEvent.delta) {
            pump.onThinkingDelta(event.assistantMessageEvent.delta);
          }
        });

        try {
          await session.prompt(chiefPrompt);
        } finally {
          unsubStream();
          pump.finish();
        }

        if (run.abortRequested) break;

        run.lastChiefReport = lastAssistantText(session);

        // ── Verification turn (the independent judge) ─────────────────
        journal.emit({ type: "role_spawned", role: "verifier" }, { role: "verifier", iteration });
        const verdict = await verifyAcceptance({
          objective: run.objective,
          acceptance: run.acceptance,
          chiefReport: run.lastChiefReport,
          evidence: run.evidence,
        });
        journal.emit(
          {
            type: "role_finished",
            role: "verifier",
            ok: verdict.ok,
            summary: `${Math.round(verdict.score * 100)}% of criteria pass — ${verdict.summary}`,
          },
          { role: "verifier", iteration },
        );
        journal.emit(
          {
            type: "verification_result",
            score: verdict.score,
            verdicts: verdict.verdicts,
          },
          { iteration },
        );
        run.lastVerdict = verdict;

        // ── Completion decision ──────────────────────────────────────
        if (verdict.ok) {
          journal.emit(
            {
              type: "completion_decision",
              action: "complete",
              reason: "all acceptance criteria verified by the independent judge",
            },
            { iteration },
          );
          run.status = "complete";
          break;
        }

        const sameGaps =
          previousGaps.length > 0 &&
          verdict.gaps.length === previousGaps.length &&
          verdict.gaps.every((g, i) => normalizeGap(g) === normalizeGap(previousGaps[i]));

        if (sameGaps) {
          escalate = true;
          journal.emit(
            {
              type: "completion_decision",
              action: "escalate",
              reason: `the same gaps remain for a second iteration (${verdict.gaps.join("; ")}) — a new approach is mandated`,
            },
            { iteration },
          );
        } else {
          journal.emit(
            {
              type: "completion_decision",
              action: "continue",
              reason: `verification not satisfied — fix the named gaps: ${verdict.gaps.join("; ")}`,
            },
            { iteration },
          );
        }
        previousGaps = verdict.gaps;
      }
    } finally {
      unsubTools();
    }

    // Aborted mid-flight?
    if (run.status === "running") {
      run.status = "incomplete";
      if (run.abortRequested) {
        try {
          await session.abort();
        } catch {
          /* already idle */
        }
      }
    }

    // ── Completion report ────────────────────────────────────────────
    const verdict = run.lastVerdict;
    run.finishedAt = Date.now();
    const durationMs = run.finishedAt - run.startedAt;
    const remainingIssues =
      run.status === "complete"
        ? []
        : [
            ...(run.abortRequested ? ["run aborted by user"] : []),
            ...(verdict?.gaps ?? run.acceptance.map((a) => `${a} (not verified within the iteration budget)`)),
          ];

    run.report = {
      goalId: run.goalId,
      runId: run.runId,
      status: run.status,
      summary:
        run.status === "complete"
          ? run.lastChiefReport.slice(0, 2000) || "All acceptance criteria verified."
          : `Incomplete after ${run.iteration} iteration(s). ${verdict?.summary ?? ""}`.trim(),
      verificationScore: verdict?.score ?? 0,
      remainingIssues,
      iterations: run.iteration,
      durationMs,
      evidence: (run.evidence ?? []).slice(-12).reverse(),
    };

    journal.emit(
      {
        type: "run_finished",
        status: run.status,
        summary: run.report.summary.slice(0, 1500),
        verificationScore: run.report.verificationScore,
        iterations: run.report.iterations,
        durationMs: run.report.durationMs,
        remainingIssues: run.report.remainingIssues,
      },
      {},
    );
    journal.close();
    // Any ask_user still blocked? Unblock it honestly (the run is over —
    // the tool resolves null and the loop's error guard settles the run).
    run.interactions?.close();

    try {
      session.dispose();
    } catch {
      /* best effort */
    }
  }
}

/** Workspace shown for a recovered run (in-VM runs recovered from disk). */
function recoveredWorkspace(runId) {
  if (ENGINE_IN_VM) {
    return {
      sandboxId: process.env.ENGINE_SANDBOX_ID || "this-vm",
      projectId: process.env.ENGINE_PROJECT_ID || null,
      userId: null,
      local: true,
    };
  }
  return null;
}

/** First chief prompt: the contract. */
function buildFirstPrompt(run) {
  if (run.workspace) {
    return [
      "You are the chief agent of a Forgvi 2.0 run. You operate inside the",
      "project's live workspace — a cloud VM mounted at /workspace. Every file",
      "you create lands on that VM's real filesystem, and every command you",
      "run executes on the VM. The workspace is shared: the studio's Files and",
      "Preview tabs read this exact filesystem, and the project's other agents",
      "(Forgvi 1.0) operate on it too — so keep it clean and build in place.",
      "Existing files in /workspace may already hold earlier work; inspect them",
      "first (ls, cat) and build on top rather than clobbering blindly.",
      "",
      "OBJECTIVE:",
      run.objective,
      "",
      "ACCEPTANCE CRITERIA — an independent judge will verify each of these exactly as written:",
      ...run.acceptance.map((a, i) => `${i + 1}. ${a}`),
      "",
      "You can also: ask the user questions (ask_user) when a decision materially changes",
      "what gets built; use their connected GitHub account (github) and their connected",
      "Supabase database (supabase) when the build calls for real repositories or a real",
      "database — request_connector asks them to connect an account that isn't yet. Prefer",
      "asking over guessing, and prefer real services over fake stand-ins.",
      "",
      `You have at most ${run.budgets.maxIterations} iterations. Claims are not proof: the judge`,
      "scores only what your tool evidence demonstrates. Work in /workspace, build the",
      "artifacts, verify your own work with commands before you claim it, then end with a short",
      "report: what you built, where it lives, and how each criterion is demonstrated.",
    ].join("\n");
  }
  return [
    "You are the chief agent of a Forgvi 2.0 run. You operate inside the engine's workspace:",
    "every file you create is a real artifact on disk, and every command you run is real.",
    "",
    "OBJECTIVE:",
    run.objective,
    "",
    "ACCEPTANCE CRITERIA — an independent judge will verify each of these exactly as written:",
    ...run.acceptance.map((a, i) => `${i + 1}. ${a}`),
    "",
    "You can also: ask the user questions (ask_user) when a decision materially changes",
    "what gets built. Prefer asking over guessing.",
    "",
    `You have at most ${run.budgets.maxIterations} iterations. Claims are not proof: the judge`,
    "scores only what your tool evidence demonstrates. Work in this workspace, build the",
    "artifacts, verify your own work with commands before you claim it, then end with a short",
    "report: what you built, where it lives, and how each criterion is demonstrated.",
  ].join("\n");
}

/** Follow-up chief prompt: the gaps, and the escalation mandate. */
function buildFollowUpPrompt({ run, gaps, escalate, iteration }) {
  const lines = [
    `Iteration ${iteration}. The independent judge scored your previous work and found these gaps:`,
    ...gaps.map((g, i) => `${i + 1}. ${g}`),
    "",
  ];
  if (escalate) {
    lines.push(
      "ESCALATION: the exact same gaps remain from the previous iteration. Your previous approach",
      "is not working. Change the approach fundamentally — do not repeat the same commands or",
      "the same reasoning that failed twice. If a criterion is genuinely impossible, say so",
      "explicitly with the exact blocker instead of claiming success.",
      "",
    );
  } else {
    lines.push(
      "Close each named gap. Verify with real commands before claiming anything.",
      "",
    );
  }
  lines.push(
    `Acceptance criteria (unchanged, the judge re-checks every one):`,
    ...run.acceptance.map((a, i) => `${i + 1}. ${a}`),
    "",
    `Iterations remaining including this one: ${run.budgets.maxIterations - iteration + 1}.`,
  );
  return lines.join("\n");
}

function summarizeArgs(args) {
  if (!args) return "";
  try {
    const obj = typeof args === "string" ? JSON.parse(args) : args;
    // Bash tool args: {command}; edit tool args: {path, ...} — show the
    // most useful single field.
    const cmd = obj?.command ?? obj?.path ?? obj?.file_path ?? obj;
    const str = typeof cmd === "string" ? cmd : JSON.stringify(obj);
    return str.replace(/\s+/g, " ").slice(0, 160);
  } catch {
    return String(args).replace(/\s+/g, " ").slice(0, 160);
  }
}

/** Best-effort extraction of a tool result's textual output. */
function summarizeResult(result) {
  if (result == null) return "";
  try {
    if (typeof result === "string") return result.slice(0, 400);
    if (Array.isArray(result)) {
      return result
        .map((block) =>
          typeof block === "string"
            ? block
            : block?.type === "text"
              ? (block.text ?? "")
              : "",
        )
        .join("\n")
        .slice(0, 400);
    }
    if (typeof result === "object") {
      if (typeof result.output === "string") return result.output.slice(0, 400);
      if (Array.isArray(result.content)) {
        return result.content
          .map((c) => (c?.type === "text" ? (c.text ?? "") : ""))
          .join("\n")
          .slice(0, 400);
      }
      return JSON.stringify(result).slice(0, 400);
    }
    return String(result).slice(0, 400);
  } catch {
    return "";
  }
}

function normalizeGap(gap) {
  return String(gap ?? "")
    .toLowerCase()
    .replace(/[^a-z0-9 ]/g, "")
    .trim();
}
