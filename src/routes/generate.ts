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
  writeWorkspaceFilesBulk,
} from "../services/daytona-workspace";
import { runPostWriteTest, type TestResult } from "../services/feedback-test-runner";
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
    // No files produced (degraded / failed pipeline) — still ship a real,
    // user-facing message so the chat bubble is never blank.
    return `Done — I scaffolded ${subject} with the ${mode === "single" ? SINGLE_MODE_MODEL : "swarm"} pipeline. Open the preview to try it.`;
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

  (async () => {
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
      // Silent auto-continue: if the God Mode pipeline fails mid-way (any
      // LLM call in the swarm — architect, developers, debugger), retry
      // silently up to 3 times. The user NEVER sees "the AI stopped" — on
      // final failure we construct a degraded empty result and continue
      // emitting the normal `done` event with whatever (possibly empty)
      // files the pipeline managed to produce. The raw error is INTERNAL.
      emit("activity", { label: "Running God Mode pipeline", status: "active", kind: "generate" });
      // Interim user-facing message — shows up in the chat bubble seconds
      // before the `done` event fires. Without this, the bubble stays
      // blank ("the AI only shows 'architect planning, etc.' — no
      // message" — the user's #1 complaint). The frontend SSE parser
      // consumes bare `data: {"message": "..."}` lines for this purpose.
      emitData({
        message:
          mode === "single"
          ? `Building ${appName || "your app"} with ${SINGLE_MODE_MODEL}…`
          : `Building ${appName || "your app"} with the agent swarm…`,
      });
      const pipelineStart = Date.now();

      try {
        const pipelineAttempt = await runWithSilentAutoContinue(
          async () => agentPlatform.runPipeline(project),
          { label: "God Mode pipeline", maxRetries: 3, baseDelayMs: 3000 },
        );
        const result = pipelineAttempt.ok && pipelineAttempt.result
          ? pipelineAttempt.result
          : {
              // Degraded empty result — the success path below handles
              // empty files / empty codebase gracefully (synthFiles
              // fallback, empty code string, etc).
              status: "failed" as const,
              attempts: pipelineAttempt.retries,
              diagnostics: [],
              codebase: project.codebase,
              messages: project.messages,
              skillsUsed: project.skillsUsed ?? [],
              phasesCompleted: project.phasesCompleted ?? [],
              negotiationRounds: 0,
            };
        const duration = Date.now() - pipelineStart;

        if (result.phasesCompleted) {
          for (const phase of result.phasesCompleted) {
            emit("activity", {
              label: `${phase.charAt(0).toUpperCase() + phase.slice(1)} phase complete`,
              status: "done",
              kind: "generate",
            });
          }
        }

        if (result.skillsUsed) {
          for (const skill of result.skillsUsed) {
            emit("activity", { label: `Skill: ${skill}`, status: "done", kind: "skill" });
          }
        }

        const files = project.codebase.files || [];
        // Single-mode pipeline returns backend/frontend STRINGS instead of a
        // files array — map them into the mandatory workspace structure:
        //   backend  → /workspace/backend/app.py
        //   frontend → /workspace/frontend/App.tsx
        const synthFiles = [...files];
        if (synthFiles.length === 0) {
          if (project.codebase.backend) {
            synthFiles.push({ path: "backend/app.py", content: project.codebase.backend });
          }
          if (project.codebase.frontend) {
            synthFiles.push({ path: "frontend/App.tsx", content: project.codebase.frontend });
          }
        }
        const mainCode = synthFiles.find((f) => f.path.includes("page.tsx"));
        const code = mainCode ? mainCode.content : (synthFiles.length > 0 ? synthFiles[synthFiles.length - 1].content : "");

        const thinking = `God Mode ${mode} pipeline completed in ${duration}ms. Files: ${synthFiles.map((f) => f.path).join(", ")}`;
        // The user-facing message — guaranteed non-empty. The model may
        // have provided its own 1-2 sentence `summary` (single mode asks
        // for one in its JSON output); when present we prefer it. When
        // absent, we synthesize a real summary from the prompt + files
        // so the chat bubble is never blank (the user's #1 complaint:
        // "the AI only shows 'architect planning, etc.' — no message").
        const modelSummary =
          typeof result.message === "string" ? result.message.trim() : "";
        const message =
          modelSummary.length > 0
            ? modelSummary
            : synthesizeUserMessage({ prompt, appName, files: synthFiles, mode });

        // ── 5. VM ORCHESTRATION — write files into the Daytona sandbox ──
        // Failures here must NEVER fail the generation.
        let sandboxId: string | null = null;
        let sandboxTree: unknown = null;
        // Module 2 test-runner result — hoisted out of the sandbox block
        // so the `done` event always has access to it even when no
        // sandbox ran (test_result stays null in that case).
        let testResult: TestResult | null = null;
        // Module 4 audit payload — hoisted out of the sandbox block so the
        // `done` event always has access to it even when no sandbox ran
        // (status stays "skipped" with 0 iterations in that case).
        let auditStatus: "passed" | "failed" | "skipped" = "skipped";
        let auditIterations = 0;
        let auditFailures: string[] = [];
        let productionReady = false;
        if (dbProject && isSupabaseConfigured()) {
          try {
            emit("activity", { label: "Writing files into VM", status: "active", kind: "generate" });

            // Fresh row (sandbox_id / logo_url may have changed since linkage).
            const row = (await dbGetProject(dbProject.id)) ?? dbProject;

            // 5a. Ensure a live sandbox exists (reuses the project's VM,
            // re-provisions when missing/dead, uploads logo on creation and
            // persists the new sandbox_id to projects.sandbox_id).
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
            let logoUploaded = ensured.logo_uploaded;

            // 5b. Write every generated file into the VM, routing by path:
            //   backend/**  → /workspace/backend/**
            //   frontend/** → /workspace/frontend/**
            //   anything else (swarm-mode raw paths) → /workspace/frontend/**
            if (synthFiles.length > 0) {
              await writeWorkspaceFilesBulk(
                sandboxId,
                synthFiles.map((f) => {
                  const rel = f.path.replace(/^\/+/, "");
                  if (rel.startsWith("backend/")) {
                    return { path: `/workspace/${rel}`, content: f.content };
                  }
                  if (rel.startsWith("frontend/")) {
                    return { path: `/workspace/${rel}`, content: f.content };
                  }
                  return { path: `/workspace/frontend/${rel}`, content: f.content };
                }),
              );
            }

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
        emit("done", {
          projectId: dbProjectId,
          thinking,
          message,
          code,
          actions: synthFiles.map((f) => ({ label: `Creating ${f.path}`, type: "create", path: f.path })),
          files: synthFiles.map((f) => ({ path: f.path, action: "create", content: f.content })),
          model: result.model || "god-mode",
          skillsUsed: result.skillsUsed || [],
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
  })();
});

export default router;
