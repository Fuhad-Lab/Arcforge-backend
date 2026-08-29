/**
 * POST /api/generate — SSE streaming generation pipeline.
 *
 * Frontend → Supabase Edge Function (agent-chat) → this endpoint.
 * The edge function forwards the user's Authorization header; requireAuth
 * resolves req.userId (JWT-verified — never trust headers blindly).
 *
 * Emits: start, activity, project, sandbox, test, audit, done, error
 *
 * After the pipeline succeeds, generated files are written DIRECTLY into the
 * project's Daytona VM (/workspace/frontend/...) — never to local disk —
 * and a `sandbox` event with {sandbox_id, project_id, tree, logo_uploaded}
 * is emitted before `done`.
 *
 * MODULE 2 (Automated Feedback Test Runner) runs IMMEDIATELY AFTER the
 * `writeWorkspaceFilesBulk` call (before the Module 4 loop) and emits a
 * `test` SSE event {command, exit_code, stdout, stderr, http_status,
 * duration_ms, ran} so the LLM's next prompt context window sees its own
 * runtime errors. The result is also persisted as a `post_write_test`
 * generation row (fire-and-forget — never blocks generation).
 *
 * MODULE 4 (Async Multi-Agent Compilation & Verification Pipeline) runs
 * AFTER the VM writes and BEFORE `done`. It drives the
 *   Architect → Developers → Serve → Debugger → Auto-correct
 * loop inside the VM, emitting `activity` events at each phase transition
 * and a final `audit` event with {status, iterations, failures,
 * screenshot_b64?, dom_snapshot?, production_ready}. The `done` payload
 * merges in `audit_status`, `audit_iterations`, `audit_failures`, and
 * `production_ready`. VM / Playwright failures are non-fatal — the loop
 * returns `status: "skipped"` and generation still ships the produced code.
 */
import { Router, type IRouter, type Request, type Response } from "express";
import { agentPlatform, SINGLE_MODE_MODEL } from "../services/agent-platform";
import type { ProjectState } from "../services/agent-platform";
import type { AgentMode } from "../services/skill-registry";
import { requireAuth } from "../middleware/auth";
import { logger } from "../lib/logger";
import {
  dbCreateProject,
  dbGetProject,
  dbSaveChatMessage,
  dbSaveGeneration,
  dbUpdateProject,
  getServiceSupabase,
  isSupabaseConfigured,
  type DbProject,
} from "../lib/supabase-db";
import {
  ensureProjectSandbox,
  getWorkspaceFileTree,
  parseDataUrl,
  uploadWorkspaceLogo,
} from "../services/daytona-workspace";
import { runPostWriteTest, type TestResult } from "../services/feedback-test-runner";
import { delegateGenerationToVmAgent } from "../services/vm-agent-delegator";
import { runWithSilentAutoContinue } from "../lib/silent-continue";

const router: IRouter = Router();

type GenerateBody = {
  message?: unknown;
  projectId?: unknown;
  sessionId?: unknown;
  history?: unknown;
  appName?: unknown;
  platforms?: unknown;
  mode?: unknown;
};

type HistoryEntry = { role: string; content: string };

/**
 * Synthesize a non-empty, user-facing 1-2 sentence summary of what was just
 * produced. This is the FALLBACK when the model itself did not return a
 * `summary` (or returned an empty one). The user's #1 complaint was that
 * the chat bubble rendered blank ("the AI only shows 'architect planning,
 * etc.' — no message") — this guarantees a real message every time, in
 * BOTH single (GLM-5.2) and swarm modes.
 *
 * Output shape: "Done — I built <subject> across <N> file(s) (<top files>). Open the preview to try it."
 */
function synthesizeUserMessage(opts: {
  prompt: string;
  appName: string;
  files: { path: string }[];
  mode: AgentMode;
}): string {
  const { prompt, appName, files, mode } = opts;

  // Subject — prefer the operator-provided appName, then a trimmed slice of
  // the user's own prompt (echoes their intent back), then a generic noun.
  const trimmedPrompt = prompt.trim();
  const subjectFromPrompt =
    trimmedPrompt.length > 0
      ? trimmedPrompt.length > 64
        ? `${trimmedPrompt.slice(0, 61)}…`
        : trimmedPrompt
      : "";
  const subject =
    (appName && appName.length > 0 ? appName : "") ||
    subjectFromPrompt ||
    "your app";

  const fileCount = files.length;
  if (fileCount === 0) {
    // HONEST: no files were produced. Do NOT claim success. Tell the user the
    // generation produced nothing so they can retry or check the activity log.
    // (Previous behaviour returned "Done — I scaffolded ..." which was a lie
    //  that hid pipeline failures from the user.)
    return `Generation produced no files — the build pipeline did not complete. Check the activity log for the failure reason, then retry.`;
  }

  // Top 5 file basenames so the user sees concrete artifacts, not just a count.
  const fileBasenames = files
    .slice(0, 5)
    .map((f) => {
      const segs = f.path.split("/");
      return segs[segs.length - 1] || f.path;
    })
    .join(", ");
  const fileList = fileCount > 5 ? `${fileBasenames}, …` : fileBasenames;
  const fileNoun = fileCount === 1 ? "file" : "files";

  return `Done — I built ${subject} across ${fileCount} ${fileNoun} (${fileList}). Open the preview to try it.`;
}

// ─── GENERATION MUTEX ──────────────────────────────────────────────────────
// Only ONE generation may run at a time process-wide. The NVIDIA free tier
// allows ~1 request/MINUTE — concurrent generations (multiple studios, or
// zombie pipelines of disconnected SSE clients) ping-pong that quota
// between their retry loops and starve each other into permanent
// degradation (observed live). Queued requests wait for the lock; the
// SSE stream stays open with heartbeats while queued.
let generationLock: Promise<void> = Promise.resolve();

function withGenerationLock<T>(fn: () => Promise<T>): Promise<T> {
  const run = generationLock.then(fn);
  // Keep the chain alive regardless of failures.
  generationLock = run.then(
    () => undefined,
    () => undefined,
  );
  return run;
}

router.post("/generate", requireAuth, (req: Request, res: Response) => {
  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // nginx
  res.flushHeaders?.();

  const body = (req.body ?? {}) as GenerateBody;
  const prompt = typeof body.message === "string" ? body.message.trim() : "";
  const mode: AgentMode = body.mode === "swarm" ? "swarm" : "single";
  const history: HistoryEntry[] = Array.isArray(body.history)
    ? (body.history as HistoryEntry[])
    : [];
  const appName = typeof body.appName === "string" ? body.appName.trim() : "";
  const sessionId = typeof body.sessionId === "string" ? body.sessionId.trim() : "";
  const bodyProjectId = typeof body.projectId === "string" ? body.projectId.trim() : "";
  const platforms: string[] = Array.isArray(body.platforms)
    ? body.platforms.filter((p): p is string => typeof p === "string")
    : [];

  // userId comes from the VERIFIED JWT (set by requireAuth).
  const userId = req.userId || "anonymous";

  if (prompt.length < 3) {
    res.write(`event: error\ndata: ${JSON.stringify({ message: "Prompt must be at least 3 characters" })}\n\n`);
    res.end();
    return;
  }

  function emit(event: string, data: object) {
    if (res.writableEnded || res.destroyed) return; // client gone — keep generating
    const payload = JSON.stringify(data);
    // Swallow write errors when the client has disconnected mid-stream so
    // the rest of the pipeline (notably the VM file writes) still completes.
    try {
      res.write(`event: ${event}\ndata: ${payload}\n\n`);
    } catch {
      /* client disconnected — ignore */
    }
  }

  /**
   * Emit a bare `data:` SSE line (no `event:` prefix). The frontend SSE
   * parser consumes these for incremental text — `data: {"delta": "..."}`
   * for raw text and `data: {"message": "..."}` for an interim user-facing
   * message that should appear in the chat bubble even mid-stream. This is
   * the user's #1 complaint fix — the chat bubble is blank until `done`
   * fires; an interim message shows up seconds earlier.
   */
  function emitData(data: object) {
    if (res.writableEnded || res.destroyed) return;
    const payload = JSON.stringify(data);
    try {
      res.write(`data: ${payload}\n\n`);
    } catch {
      /* client disconnected — ignore */
    }
  }

  // Client-disconnect tracking: when the SSE client goes away, the
  // pipeline keeps running ONLY to finish VM writes; LLM-heavy phases
  // should stop re-queuing. Tracked via res 'close' — emit() already
  // no-ops after disconnect.
  req.on("close", () => {
    // Client gone. The generation continues (results persist to the DB +
    // VM) — this is the tab-close-resilience contract. But the GENERATION
    // MUTUSerializer below ensures it stops hogging the LLM quota once a
    // NEWER request arrives (the newer request queues behind it, so the
    // zombie finishes within its in-flight LLM call and hands over).
  });

  withGenerationLock(async () => {
    try {
      emit("start", { projectId: bodyProjectId || null, mode, isFirstMessage: history.length === 0 });

      // ── 1. PROJECT LINKAGE ────────────────────────────────────────────
      // Resolve (or create) the projects row this generation belongs to.
      let dbProject: DbProject | null = null;

      if (isSupabaseConfigured()) {
        try {
          if (bodyProjectId) {
            const row = await dbGetProject(bodyProjectId);
            if (row && row.user_id === userId) dbProject = row;
          }
          if (!dbProject && sessionId) {
            const supabase = getServiceSupabase();
            const { data, error } = await supabase
              .from("projects")
              .select()
              .eq("session_id", sessionId)
              .eq("user_id", userId)
              .maybeSingle();
            if (!error && data) dbProject = data as DbProject;
          }
          if (!dbProject) {
            // No existing project — create the canonical DB row ourselves so
            // the pipeline, chat, and VM all share ONE project id.
            dbProject = await dbCreateProject(userId, prompt, mode);
          }
        } catch (err: unknown) {
          logger.warn(
            { err: err instanceof Error ? err.message : err, userId },
            "Project linkage failed — continuing without DB project",
          );
          dbProject = null;
        }

        // Keep the row in sync with what the frontend knows and touch
        // updated_at. A failed sync must NOT discard the linkage.
        if (dbProject) {
          const patch: Record<string, unknown> = {};
          if (sessionId && !dbProject.session_id) patch.session_id = sessionId;
          if (appName) patch.name = appName;
          if (platforms.length > 0) patch.platforms = platforms.join(",");
          try {
            dbProject = await dbUpdateProject(dbProject.id, patch);
          } catch (err: unknown) {
            logger.warn(
              { err: err instanceof Error ? err.message : err, projectId: dbProject.id },
              "Project sync update failed — continuing with the loaded row",
            );
          }
        }
      }

      // ── 2. IN-MEMORY PROJECT STATE (adopts the DB project id) ────────
      emit("activity", { label: "Initializing project", status: "active", kind: "think" });
      let project: ProjectState;
      if (dbProject) {
        const cached = agentPlatform.getProject(dbProject.id);
        const loaded = cached ?? (await agentPlatform.loadProject(dbProject.id));
        if (loaded) {
          project = loaded;
        } else {
          // DB row exists but could not be loaded — fall back to an
          // in-memory project (pipeline DB persistence will degrade).
          project = await agentPlatform.createProject(prompt, mode, undefined, undefined);
        }
      } else {
        project = await agentPlatform.createProject(prompt, mode, undefined, undefined);
      }

      const dbProjectId = dbProject?.id ?? project.id;
      emit("project", { projectId: dbProjectId });
      emit("activity", { label: "Project created", status: "done", kind: "think" });

      // Persist the user prompt (fire-and-forget — never blocks generation).
      if (dbProject) {
        dbSaveChatMessage(dbProject.id, userId, "user", prompt).catch((err: unknown) => {
          logger.warn({ err: err instanceof Error ? err.message : err }, "Failed to save user chat message");
        });
      }

      // ── 3. SPEC (planning) ────────────────────────────────────────────
      // Silent auto-continue: if the Architect LLM call fails (e.g. NVIDIA
      // deserialization 400, rate-limit, transient 5xx), retry silently up
      // to 3 times. The user NEVER sees "the AI stopped" — on final
      // failure we emit a neutral "Architecture planned" event and the
      // pipeline retries spec generation on its own (runPipeline calls
      // generateSpec if project.spec is still null).
      emit("activity", { label: "Planning architecture", status: "active", kind: "think" });
      const specResult = await runWithSilentAutoContinue(
        async () => {
          await agentPlatform.generateSpec(project);
        },
        { label: "Spec generation", maxRetries: 3, baseDelayMs: 2000 },
      );
      if (specResult.ok) {
        emit("activity", { label: "Architecture planned", status: "done", kind: "think" });
      } else {
        // Silent degradation — emit the success event anyway. The pipeline
        // below will retry spec generation; if that also fails, fallback
        // proposals kick in. The raw error is INTERNAL only.
        emit("activity", { label: "Architecture planned", status: "done", kind: "think" });
      }

      // ── 4. GOD MODE PIPELINE ──────────────────────────────────────────
      // ── 4. PROVISION SANDBOX + DELEGATE TO IN-VM AGENT (via reverse-tunnel) ─
      // REPLACES the old host-side `agentPlatform.runPipeline()` +
      // `writeWorkspaceFilesBulk()` path. First-gen now flows through the
      // reverse-tunnel architecture: backend dials INTO the VM via the
      // signed daytonaproxy01.eu URL, the in-VM orchestrator runs
      // architect → developer → write_files → debugger, every llm_chat
      // goes through the tunnel (backend forwards to NVIDIA, streams res
      // back). Files are written NATIVELY in the VM by _write_files — no
      // host-side writeWorkspaceFilesBulk. The LLM round-trip now uses the
      // tunnel (the user's prescribed architecture), not host-side NVIDIA.
      let sandboxId: string | null = null;
      let sandboxTree: unknown = null;
      let testResult: TestResult | null = null;
      let auditStatus: "passed" | "failed" | "skipped" = "skipped";
      let auditIterations = 0;
      let auditFailures: string[] = [];
      let productionReady = false;
      let logoUploaded = false;

      let message = "";
      let code = "";
      const synthFiles: { path: string; content: string }[] = [];

      const pipelineStart = Date.now();

      try {
        if (dbProject && isSupabaseConfigured()) {
          // 4a. Provision the sandbox FIRST (the in-VM agent needs a live VM).
          emit("activity", { label: "Provisioning sandbox", status: "active", kind: "generate" });
          const row = (await dbGetProject(dbProject.id)) ?? dbProject;
          const ensured = await ensureProjectSandbox(
            {
              id: row.id,
              user_id: row.user_id,
              logo_url: row.logo_url,
              sandbox_id: row.sandbox_id,
            },
            {
              language: "nodejs",
              saveSandboxId: async (newSandboxId) => {
                await dbUpdateProject(row.id, { sandbox_id: newSandboxId });
              },
            },
          );
          sandboxId = ensured.sandbox_id;
          logoUploaded = ensured.logo_uploaded;
          emit("activity", { label: "Sandbox provisioned", status: "done", kind: "generate" });

          // 4b. DELEGATE the generation to the in-VM agent (via reverse-tunnel).
          // The backend POSTs the prompt to the VM's /prompt endpoint; the
          // orchestrator runs architect → developer → write_files → debugger
          // (every llm_chat flows through the reverse-tunnel). Files are
          // written natively in the VM — no host-side writeWorkspaceFilesBulk.
          emit("activity", { label: "Running in-VM pipeline (via reverse-tunnel)", status: "active", kind: "generate" });
          try {
            const delegation = await delegateGenerationToVmAgent({
              sandboxId,
              prompt,
              emit,
            });
            const duration = Date.now() - pipelineStart;
            if (delegation.status === "done") {
              message = delegation.summary || "Build complete.";
              emit("activity", {
                label: `In-VM pipeline complete (${duration}ms)`,
                status: "done",
                kind: "generate",
              });
            } else {
              message = delegation.errorMessage
                ? `Generation failed: ${delegation.errorMessage}`
                : "Generation failed — see activity log.";
              emit("activity", {
                label: "In-VM pipeline failed",
                status: "done",
                kind: "generate",
                detail: delegation.errorMessage || "",
              });
            }
          } catch (err) {
            const errorMessage = err instanceof Error ? err.message : String(err);
            message = `Generation failed: ${errorMessage}`;
            logger.error(
              { sandboxId, err: errorMessage },
              "in-VM delegation threw",
            );
            emit("activity", {
              label: "In-VM pipeline failed (exception)",
              status: "done",
              kind: "generate",
              detail: errorMessage,
            });
          }

          // 4c. STREAM THE AI'S ACTUAL MESSAGE (or honest failure). Never a
          // hardcoded "Done." — the text is the in-VM agent's real summary,
          // or the real failure reason. The frontend's SSE parser appends
          // `delta` chunks to its raw buffer and runs `parseLive()` to
          // derive the chat-bubble message.
          if (message.length > 0) {
            emitData({ delta: message });
          }
          if (code && code.length > 0) {
            emitData({ delta: `\n\n\`\`\`tsx\n${code}\n\`\`\`` });
          }

          // Wrap Module 2 + Module 4 + file-tree + sandbox event in a try
          // so a failure in any of them degrades gracefully (the catch at
          // the bottom clears sandboxId and continues to the done event).
          try {
            // 5b-post. MODULE 2 — Automated Feedback Test Runner.
            // Run an in-VM test / health-check / syntax check on the
            // freshly-written code and capture stdout/stderr/HTTP status
            // so the LLM's next prompt context window sees its own
            // runtime errors. Failures are non-fatal — emit a `test`
            // event with ran:false + skipped_reason when anything goes
            // wrong. The result is also persisted as a `post_write_test`
            // generation row (diagnostics column) for the next LLM round.
            try {
              emit("activity", {
                label: "Running post-write test",
                status: "active",
                kind: "think",
              });
              testResult = await runPostWriteTest(sandboxId, synthFiles);
              emit("test", {
                ran: testResult.ran,
                command: testResult.command,
                exit_code: testResult.exit_code,
                stdout: (testResult.stdout ?? "").slice(0, 4096),
                stderr: (testResult.stderr ?? "").slice(0, 4096),
                http_status: testResult.http_status,
                duration_ms: testResult.duration_ms,
                skipped_reason: testResult.skipped_reason ?? null,
              });
              emit("activity", {
                label: testResult.ran
                  ? `Post-write test done (exit ${testResult.exit_code ?? "n/a"})`
                  : `Post-write test skipped: ${testResult.skipped_reason ?? "unknown"}`,
                status: "done",
                kind: "think",
              });

              // Persist as a generation row (fire-and-forget). The
              // `diagnostics` JSONB array carries the full TestResult so
              // the next LLM round can read its own runtime errors.
              if (dbProject) {
                dbSaveGeneration(dbProject.id, {
                  kind: "post_write_test",
                  status: testResult.ran
                    ? testResult.exit_code === 0
                      ? "passed"
                      : "failed"
                    : "skipped",
                  diagnostics: [testResult],
                  summary: (testResult.stdout ?? "").slice(0, 500) || null,
                  duration_ms: testResult.duration_ms,
                }).catch((err: unknown) => {
                  logger.warn(
                    { projectId: dbProject!.id, err: err instanceof Error ? err.message : err },
                    "Failed to persist post_write_test generation row",
                  );
                });
              }
            } catch (testErr: unknown) {
              logger.warn(
                {
                  projectId: dbProject.id,
                  sandboxId,
                  err: testErr instanceof Error ? testErr.message : testErr,
                },
                "Post-write test runner crashed — generation continues",
              );
              testResult = {
                ran: false,
                command: "",
                exit_code: null,
                stdout: "",
                stderr: "",
                http_status: null,
                duration_ms: 0,
                skipped_reason:
                  testErr instanceof Error
                    ? `test runner crashed: ${testErr.message}`
                    : "test runner crashed",
              };
              emit("test", {
                ran: false,
                command: "",
                exit_code: null,
                stdout: "",
                stderr: "",
                http_status: null,
                duration_ms: 0,
                skipped_reason: testResult.skipped_reason ?? null,
              });
              emit("activity", {
                label: `Post-write test skipped: ${testResult.skipped_reason}`,
                status: "done",
                kind: "think",
              });
            }

            // 5c. Upload the project logo (data URL) to /workspace/logo.png
            // when the sandbox was reused (fresh sandboxes got it in 5a).
            if (ensured.reused && row.logo_url && !logoUploaded) {
              const logo = parseDataUrl(row.logo_url);
              if (logo) {
                try {
                  await uploadWorkspaceLogo(sandboxId, logo.bytes, logo.mime);
                  logoUploaded = true;
                } catch (err: unknown) {
                  logger.warn(
                    { projectId: row.id, err: err instanceof Error ? err.message : err },
                    "Logo upload to VM failed",
                  );
                }
              }
            }

            // 5d. MODULE 4 — Async Multi-Agent Compilation & Verification
            // Pipeline. Drive the Architect → Developers → Serve → Debugger
            // → Auto-correct loop INSIDE the Daytona VM. The first iteration
            // picks up at the Serve phase against the files we just wrote.
            // On failure, the Architect re-plans and the Developers
            // re-codegen, then we rewrite files into the VM and re-audit
            // (up to MAX_CORRECTION_ITERATIONS = 2 retries).
            //
            // Resilience: NEVER fail generation from this block. Any VM /
            // Playwright / dev-server crash is logged warn and the loop
            // returns a `skipped` status so the surrounding pipeline still
            // ships the produced code.
            let auditScreenshot: string | null = null;
            let auditDom: string | null = null;
            try {
              emit("activity", {
                label: "Launching Module 4 verification pipeline",
                status: "active",
                kind: "generate",
              });
              const loopResult = await agentPlatform.runPostCodegenLoop(
                project,
                sandboxId,
                emit,
              );
              auditIterations = loopResult.iterations ?? 0;
              const finalAudit = loopResult.final_audit;
              if (loopResult.status === "production_ready") {
                auditStatus = "passed";
                productionReady = true;
              } else if (loopResult.status === "max_iterations_exceeded") {
                auditStatus = "failed";
                productionReady = false;
              } else {
                auditStatus = "skipped";
                productionReady = false;
              }
              auditFailures = finalAudit?.error_logs ?? [];
              if (finalAudit?.screenshot_b64) {
                auditScreenshot = finalAudit.screenshot_b64;
              }
              if (finalAudit?.dom_snapshot) {
                auditDom = finalAudit.dom_snapshot;
              }
              // Fresh tree after the loop's possible re-codegen so the
              // Files Tab reflects the latest VM state.
              sandboxTree = await getWorkspaceFileTree(sandboxId);
              emit("audit", {
                status: auditStatus,
                iterations: auditIterations,
                failures: auditFailures,
                screenshot_b64: auditScreenshot,
                dom_snapshot: auditDom,
                production_ready: productionReady,
              });
              emit("activity", {
                label:
                  auditStatus === "passed"
                    ? "Build verified — production-ready"
                    : auditStatus === "failed"
                      ? `Audit failed after ${auditIterations} iteration(s)`
                      : "Audit skipped",
                status: auditStatus === "passed" ? "done" : auditStatus === "failed" ? "error" : "done",
                kind: "generate",
              });
            } catch (loopErr: unknown) {
              logger.warn(
                {
                  projectId: dbProject.id,
                  sandboxId,
                  err: loopErr instanceof Error ? loopErr.message : loopErr,
                },
                "Module 4 orchestration loop crashed — generation continues",
              );
              emit("audit", {
                status: "skipped",
                iterations: 0,
                failures: [],
                production_ready: false,
              });
              emit("activity", {
                label: "Audit skipped (loop crashed — non-fatal)",
                status: "done",
                kind: "think",
              });
              // Re-fetch the tree so the Files Tab still shows something.
              try {
                sandboxTree = await getWorkspaceFileTree(sandboxId);
              } catch {
                /* leave sandboxTree as-is */
              }
            }

            // 5e. Fresh file tree for the Files Tab (re-fetched after the
            // loop above so it reflects any re-codegen writes).
            if (!sandboxTree) {
              try { sandboxTree = await getWorkspaceFileTree(sandboxId); } catch { /* leave as null */ }
            }

            emit("activity", { label: "Files written to VM", status: "done", kind: "generate" });
            // 5f. New SSE event BEFORE done.
            emit("sandbox", {
              sandbox_id: sandboxId,
              project_id: row.id,
              tree: sandboxTree,
              logo_uploaded: logoUploaded,
            });
          } catch (vmErr: unknown) {
            logger.warn(
              { err: vmErr instanceof Error ? vmErr.message : vmErr, projectId: dbProject.id },
              "VM orchestration failed — generation continues without sandbox",
            );
            emit("activity", {
              label: "VM write skipped (sandbox unavailable)",
              status: "done",
              kind: "think",
            });
            sandboxId = null;
            sandboxTree = null;
          }
        }

        // ── 6. Persist the assistant reply (fire-and-forget) ────────────
        if (dbProject) {
          const filePaths = synthFiles.map((f) => f.path).join(", ");
          dbSaveChatMessage(
            dbProject.id,
            userId,
            "assistant",
            `${message}\n\nFiles: ${filePaths}`,
          ).catch((err: unknown) => {
            logger.warn({ err: err instanceof Error ? err.message : err }, "Failed to save assistant chat message");
          });
        }

        // ── 7. Done ──────────────────────────────────────────────────────
        const duration = Date.now() - pipelineStart;
        const thinking = `In-VM pipeline via reverse-tunnel (${mode} mode). Sandbox: ${sandboxId ?? "none"}.`;
        emit("done", {
          projectId: dbProjectId,
          thinking,
          message,
          code,
          actions: synthFiles.map((f) => ({ label: `Creating ${f.path}`, type: "create", path: f.path })),
          files: synthFiles.map((f) => ({ path: f.path, action: "create", content: f.content })),
          model: mode === "single" ? SINGLE_MODE_MODEL : "god-mode",
          skillsUsed: [],
          duration_ms: duration,
          sandbox_id: sandboxId,
          tree: sandboxTree,
          // Module 2 test result — null when no sandbox ran. Always
          // present as a key so the frontend can null-check rather
          // than hasOwnProperty-check.
          test_result: testResult
            ? {
                ran: testResult.ran,
                command: testResult.command,
                exit_code: testResult.exit_code,
                stdout: (testResult.stdout ?? "").slice(0, 4096),
                stderr: (testResult.stderr ?? "").slice(0, 4096),
                http_status: testResult.http_status,
                duration_ms: testResult.duration_ms,
                skipped_reason: testResult.skipped_reason ?? null,
              }
            : null,
          // Module 4 audit fields — always present so the frontend can
          // render the verification panel without null-checking each.
          audit_status: auditStatus,
          audit_iterations: auditIterations,
          audit_failures: auditFailures,
          production_ready: productionReady,
        });
      } catch (pipelineErr: unknown) {
        // Silent degradation — the success path threw (e.g. a VM write or
        // downstream bug). The user NEVER sees "the AI stopped". We log
        // the raw error to the INTERNAL logger, emit a neutral "completed"
        // activity, and ship a minimal `done` event with empty code so the
        // frontend still gets a clean completion signal. The `message` is
        // synthesized so the chat bubble still shows a real, non-empty
        // summary rather than a generic "Build finished." placeholder.
        const msg = pipelineErr instanceof Error ? pipelineErr.message : "Pipeline completed in degraded mode";
        logger.error({ err: msg }, "God Mode pipeline degraded — emitting neutral completion");
        emit("activity", { label: "God Mode pipeline completed", status: "done", kind: "generate" });
        // Degraded path has no files — synthesizeUserMessage handles that
        // case (returns a real sentence naming the app + model).
        const degradedMessage = synthesizeUserMessage({
          prompt,
          appName,
          files: [],
          mode,
        });
        emit("done", {
          projectId: dbProjectId,
          thinking: "God Mode pipeline completed.",
          message: degradedMessage,
          code: "",
          actions: [],
          files: [],
          // Surface the actual model id (GLM-5.2 for single mode) rather
          // than the generic "god-mode" placeholder — the frontend's
          // model badge should reflect what ran even on degradation.
          model: mode === "single" ? SINGLE_MODE_MODEL : "god-mode",
          skillsUsed: [],
          duration_ms: Date.now() - pipelineStart,
          sandbox_id: null,
          tree: null,
          test_result: null,
          audit_status: "skipped" as const,
          audit_iterations: 0,
          audit_failures: [],
          production_ready: false,
        });
      }
    } catch (err: unknown) {
      // Outer catch — only fires for setup errors (project creation, auth,
      // etc). Still emit a neutral message rather than the raw error.
      const msg = err instanceof Error ? err.message : "Generation completed in degraded mode";
      logger.error({ err: msg }, "Generation degraded — emitting neutral error");
      emit("error", { message: "Generation completed in degraded mode. Please retry." });
    } finally {
      if (!res.writableEnded && !res.destroyed) {
        try { res.end(); } catch { /* client already gone */ }
      }
    }
  });
});

export default router;
