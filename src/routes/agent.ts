import { Router, type IRouter } from "express";
import { agentPlatform } from "../services/agent-platform";
import type { AgentMode } from "../services/skill-registry";
import { daytonaExecutor } from "../services/daytona-executor";
import { requireAuth } from "../middleware/auth";

const router: IRouter = Router();

// JWT auth at the router level — these routes were previously fully public.
// Path-scoped auth (see connectors.ts note): this router mounts
// unprefixed, so gate only its own route family.
router.use("/projects", requireAuth);

function validUuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

// ─── PROJECT CRUD ─────────────────────────────────────────────────────

router.get("/projects", async (_req, res, next) => {
  try {
    res.json({ projects: await agentPlatform.listProjects() });
  } catch (error) {
    next(error);
  }
});

router.post("/projects", async (req, res, next) => {
  try {
    const prompt = typeof req.body?.prompt === "string" ? req.body.prompt.trim() : "";
    const mode = req.body?.mode === "single" || req.body?.mode === "swarm"
      ? (req.body.mode as AgentMode)
      : "swarm";
    if (prompt.length < 10 || prompt.length > 20_000) {
      res.status(400).json({ error: "prompt must be between 10 and 20,000 characters" });
      return;
    }
    // Authenticated user (JWT-verified by requireAuth at the router level).
    const userId = req.userId || (typeof req.body?.userId === "string" ? req.body.userId : undefined);
    res.status(201).json(await agentPlatform.createProject(prompt, mode, undefined, userId));
  } catch (error) {
    next(error);
  }
});

router.get("/projects/:projectId", (req, res) => {
  const project = validUuid(req.params.projectId)
    ? agentPlatform.getProject(req.params.projectId)
    : undefined;
  if (!project) {
    res.status(404).json({ error: "project not found" });
    return;
  }
  res.json(project);
});

// ─── PIPELINE ─────────────────────────────────────────────────────────

router.post("/projects/:projectId/spec", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    res.json({ spec: await agentPlatform.generateSpec(project) });
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/pipeline", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    res.json(await agentPlatform.runPipeline(project));
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/export", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    res.json(await agentPlatform.exportProject(project, "./generated-workspaces"));
  } catch (error) {
    next(error);
  }
});

// ─── WORKSPACE FILE OPERATIONS ─────────────────────────────────────────

router.get("/projects/:projectId/workspace", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    res.json({ entries: await agentPlatform.listWorkspace(project, String(req.query.path ?? ".")) });
  } catch (error) {
    next(error);
  }
});

router.get("/projects/:projectId/workspace/tree", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    const tree = await agentPlatform.workspaceTree(project);
    res.json({ tree });
  } catch (error) {
    next(error);
  }
});

router.get("/projects/:projectId/workspace/file", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const filePath = String(req.query.path ?? "");
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!filePath) {
      res.status(400).json({ error: "path query parameter is required" });
      return;
    }
    const content = await agentPlatform.readFile(project, filePath);
    res.json({ path: filePath, content });
  } catch (error) {
    const msg = error instanceof Error ? error.message : "File not found";
    res.status(404).json({ error: msg });
  }
});

router.post("/projects/:projectId/workspace/file", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const filePath = typeof req.body?.path === "string" ? req.body.path : "";
    const content = typeof req.body?.content === "string" ? req.body.content : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!filePath || filePath.endsWith("/")) {
      res.status(400).json({ error: "file path is required" });
      return;
    }
    const savedPath = req.body?.mode === "edit"
      ? await agentPlatform.editFile(project, filePath, content)
      : await agentPlatform.createFile(project, filePath, content);
    res.status(201).json({ path: savedPath });
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/workspace/directory", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const directoryPath = typeof req.body?.path === "string" ? req.body.path : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!directoryPath) {
      res.status(400).json({ error: "directory path is required" });
      return;
    }
    res.status(201).json({ path: await agentPlatform.createDirectory(project, directoryPath) });
  } catch (error) {
    next(error);
  }
});

router.delete("/projects/:projectId/workspace", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const targetPath = typeof req.body?.path === "string" ? req.body.path : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!targetPath) {
      res.status(400).json({ error: "path is required" });
      return;
    }
    await agentPlatform.deletePath(project, targetPath);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/workspace/move", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const source = typeof req.body?.source === "string" ? req.body.source : "";
    const destination = typeof req.body?.destination === "string" ? req.body.destination : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!source || !destination) {
      res.status(400).json({ error: "source and destination are required" });
      return;
    }
    const path = await agentPlatform.movePath(project, source, destination);
    res.json({ path });
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/workspace/copy", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const source = typeof req.body?.source === "string" ? req.body.source : "";
    const destination = typeof req.body?.destination === "string" ? req.body.destination : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!source || !destination) {
      res.status(400).json({ error: "source and destination are required" });
      return;
    }
    const path = await agentPlatform.copyPath(project, source, destination);
    res.json({ path });
  } catch (error) {
    next(error);
  }
});

// ─── SERVICES ─────────────────────────────────────────────────────────

router.post("/projects/:projectId/services/:kind/start", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    const port = Number(req.body?.port);
    if (!project || !kind) {
      res.status(404).json({ error: "project or service not found" });
      return;
    }
    res.status(201).json(await agentPlatform.startService(project, kind, port));
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/services/:kind/stop", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    if (!project || !kind) {
      res.status(404).json({ error: "project or service not found" });
      return;
    }
    await agentPlatform.stopService(project, kind);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── DOCKER CONTAINERS ─────────────────────────────────────────────

router.post("/projects/:projectId/containers/:kind/start", async (req, res, next) => {
  try {
    const projectId = req.params.projectId;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    const port = Number(req.body?.port);
    if (!validUuid(projectId) || !kind) {
      res.status(400).json({ error: "valid projectId and kind (frontend|backend) are required" });
      return;
    }
    if (!Number.isInteger(port) || port < 1024 || port > 65_535) {
      res.status(400).json({ error: "port must be between 1024 and 65535" });
      return;
    }
    const project = agentPlatform.getProject(projectId);
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (project.codebase.files.length === 0) {
      res.status(400).json({ error: "no generated files — run the pipeline first" });
      return;
    }
    const result = await agentPlatform.startService(project, kind, port);
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/containers/:kind/stop", async (req, res, next) => {
  try {
    const projectId = req.params.projectId;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    if (!validUuid(projectId) || !kind) {
      res.status(400).json({ error: "valid projectId and kind (frontend|backend) are required" });
      return;
    }
    await daytonaExecutor.stopContainer(projectId, kind);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

router.get("/projects/:projectId/containers/:kind/logs", async (req, res, next) => {
  try {
    const projectId = req.params.projectId;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    if (!validUuid(projectId) || !kind) {
      res.status(400).json({ error: "valid projectId and kind (frontend|backend) are required" });
      return;
    }
    const logs = await daytonaExecutor.getLogs(projectId, kind);
    res.json({ logs });
  } catch (error) {
    next(error);
  }
});

router.post("/projects/:projectId/containers/:kind/exec", async (req, res, next) => {
  try {
    const projectId = req.params.projectId;
    const kind = req.params.kind === "frontend" || req.params.kind === "backend"
      ? req.params.kind
      : undefined;
    const command = typeof req.body?.command === "string" ? req.body.command.trim() : "";
    if (!validUuid(projectId) || !kind) {
      res.status(400).json({ error: "valid projectId and kind (frontend|backend) are required" });
      return;
    }
    if (!command) {
      res.status(400).json({ error: "command string is required in request body" });
      return;
    }
    const output = await daytonaExecutor.execCommand(projectId, kind, command);
    res.json({ output });
  } catch (error) {
    next(error);
  }
});

// ─── LIVE DEBUG ────────────────────────────────────────────────────────

router.post("/projects/:projectId/debug", async (req, res, next) => {
  try {
    const project = validUuid(req.params.projectId)
      ? agentPlatform.getProject(req.params.projectId)
      : undefined;
    const url = typeof req.body?.url === "string" ? req.body.url : "";
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }
    if (!/^https?:\/\//.test(url)) {
      res.status(400).json({ error: "a live http(s) URL is required" });
      return;
    }
    res.json(await agentPlatform.debugProject(project, url));
  } catch (error) {
    next(error);
  }
});

export default router;
