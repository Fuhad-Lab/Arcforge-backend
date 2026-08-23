/**
 * POST /api/generate — SSE streaming generation pipeline.
 *
 * Frontend → Supabase Edge Function (agent-chat) → this endpoint.
 * The edge function forwards the user's Authorization header; requireAuth
 * resolves req.userId (JWT-verified — never trust headers blindly).
 *
 * Emits: start, activity, project, sandbox, done, error
 *
 * After the pipeline succeeds, generated files are written DIRECTLY into the
 * project's Daytona VM (/workspace/frontend/...) — never to local disk —
 * and a `sandbox` event with {sandbox_id, project_id, tree, logo_uploaded}
 * is emitted before `done`.
 */
import { Router, type IRouter, type Request, type Response } from "express";
import { agentPlatform } from "../services/agent-platform";
import type { ProjectState } from "../services/agent-platform";
import type { AgentMode } from "../services/skill-registry";
import { requireAuth } from "../middleware/auth";
import { logger } from "../lib/logger";
import {
  dbCreateProject,
  dbGetProject,
  dbSaveChatMessage,
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
    const payload = JSON.stringify(data);
    res.write(`event: ${event}\ndata: ${payload}\n\n`);
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
      emit("activity", { label: "Planning architecture", status: "active", kind: "think" });
      try {
        await agentPlatform.generateSpec(project);
        emit("activity", { label: "Architecture planned", status: "done", kind: "think" });
      } catch (specErr: unknown) {
        const msg = specErr instanceof Error ? specErr.message : "skipped";
        emit("activity", { label: "Spec generation: " + msg, status: "done", kind: "think" });
      }

      // ── 4. GOD MODE PIPELINE ──────────────────────────────────────────
      emit("activity", { label: "Running God Mode pipeline", status: "active", kind: "generate" });
      const pipelineStart = Date.now();

      try {
        const result = await agentPlatform.runPipeline(project);
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
            synthFiles.push({ path: "backend/app.py", content: project.codebase.backend, action: "create" as const });
          }
          if (project.codebase.frontend) {
            synthFiles.push({ path: "frontend/App.tsx", content: project.codebase.frontend, action: "create" as const });
          }
        }
        const mainCode = synthFiles.find((f) => f.path.includes("page.tsx"));
        const code = mainCode ? mainCode.content : (synthFiles.length > 0 ? synthFiles[synthFiles.length - 1].content : "");

        const thinking = `God Mode ${mode} pipeline completed in ${duration}ms. Files: ${synthFiles.map((f) => f.path).join(", ")}`;
        const message = `Built with ${synthFiles.length} file${synthFiles.length !== 1 ? "s" : ""} using ${mode} mode.`;

        // ── 5. VM ORCHESTRATION — write files into the Daytona sandbox ──
        // Failures here must NEVER fail the generation.
        let sandboxId: string | null = null;
        let sandboxTree: unknown = null;
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

            // 5d. Fresh file tree for the Files Tab.
            sandboxTree = await getWorkspaceFileTree(sandboxId);

            emit("activity", { label: "Files written to VM", status: "done", kind: "generate" });
            // 5e. New SSE event BEFORE done.
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
        });
      } catch (pipelineErr: unknown) {
        const msg = pipelineErr instanceof Error ? pipelineErr.message : "Pipeline failed";
        emit("activity", { label: "Pipeline: " + msg, status: "done", kind: "think" });
        emit("error", { message: msg });
      }
    } catch (err: unknown) {
      const msg = err instanceof Error ? err.message : "Generation failed";
      emit("error", { message: msg });
    } finally {
      res.end();
    }
  })();
});

export default router;
