import { Router, type IRouter, type Request, type Response } from "express";
import { agentPlatform } from "../services/agent-platform";
import type { AgentMode } from "../services/skill-registry";

const router: IRouter = Router();

/**
 * SSE streaming generate endpoint.
 * Frontend → Edge Function → This endpoint.
 * Emits: start, activity, project, delta, done, error
 */
router.post("/generate", (req: Request, res: Response) => {
  // Set SSE headers
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.setHeader("X-Accel-Buffering", "no"); // nginx
  res.flushHeaders?.();

  const prompt = typeof req.body?.message === "string" ? req.body.message.trim() : "";
  const mode: AgentMode = req.body?.mode === "swarm" ? "swarm" : "single";
  const history: Array<{role: string; content: string}> = Array.isArray(req.body?.history) ? req.body.history : [];
  const appName = typeof req.body?.appName === "string" ? req.body.appName : "";

  if (prompt.length < 3) {
    res.write(`event: error\ndata: ${JSON.stringify({ message: "Prompt must be at least 3 characters" })}\n\n`);
    res.end();
    return;
  }

  async function emit(event: string, data: object) {
    const payload = JSON.stringify(data);
    res.write(`event: ${event}\ndata: ${payload}\n\n`);
  }

  (async () => {
    try {
      emit("start", { projectId: null, mode, isFirstMessage: history.length === 0 });

      const userId = (req.headers["x-user-id"] as string) || req.body?.userId || "anonymous";

      // 1. Create project
      emit("activity", { label: "Initializing project", status: "active", kind: "think" });
      const project = await agentPlatform.createProject(prompt, mode, undefined, userId);
      emit("project", { projectId: project.id });
      emit("activity", { label: "Project created", status: "done", kind: "think" });

      // 2. Generate spec (planning)
      emit("activity", { label: "Planning architecture", status: "active", kind: "think" });
      try {
        const spec = await agentPlatform.generateSpec(project);
        emit("activity", { label: "Architecture planned", status: "done", kind: "think" });
      } catch (specErr: any) {
        emit("activity", { label: "Spec generation: " + (specErr.message || "skipped"), status: "done", kind: "think" });
      }

      // 3. Run God Mode pipeline
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
        const mainCode = files.find((f) => f.path.includes("page.tsx"));
        const code = mainCode ? mainCode.content : (files.length > 0 ? files[files.length - 1].content : "");

        const thinking = `God Mode ${mode} pipeline completed in ${duration}ms. Files: ${files.map((f) => f.path).join(", ")}`;
        const message = `Built with ${files.length} file${files.length !== 1 ? "s" : ""} using ${mode} mode.`;

        emit("done", {
          thinking,
          message,
          code,
          actions: files.map((f) => ({ label: `Creating ${f.path}`, type: "create", path: f.path })),
          files: files.map((f) => ({ path: f.path, action: "create", content: f.content })),
          model: result.model || "god-mode",
          skillsUsed: result.skillsUsed || [],
          duration_ms: duration,
        });
      } catch (pipelineErr: any) {
        emit("activity", { label: "Pipeline: " + (pipelineErr.message || "failed"), status: "done", kind: "think" });
        emit("error", { message: pipelineErr.message || "Pipeline failed" });
      }
    } catch (err: any) {
      emit("error", { message: err.message || "Generation failed" });
    } finally {
      res.end();
    }
  })();
});

export default router;
// trigger rebuild
// fix newline
