import { Router, type IRouter } from "express";
import { logger } from "../lib/logger";

const router: IRouter = Router();

const DAYTONA_URL = process.env.DAYTONA_SERVICE_URL || "https://arcforge-daytona.onrender.com";

/** Proxy a request to the Daytona workspace service. */
async function proxyToDaytona(
  reqPath: string,
  options: { method?: string; body?: unknown; headers?: Record<string, string> } = {},
) {
  const url = `${DAYTONA_URL}/api/v1/workspace${reqPath}`;
  const res = await fetch(url, {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    body: options.body ? JSON.stringify(options.body) : undefined,
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Daytona workspace ${res.status}: ${text || res.statusText}`);
  }

  if (res.status === 204) return null;
  return res.json();
}

// ─── POST /api/workspace/init ───────────────────────────────────────
// Create project workspace with scaffolded /workspace/{git,frontend,backend,logo.png}

router.post("/init", async (req, res, next) => {
  try {
    const { project_id, language } = req.body;
    if (!project_id) {
      res.status(400).json({ error: "project_id is required" });
      return;
    }
    const result = await proxyToDaytona("/init", {
      method: "POST",
      body: { project_id, language: language || "nodejs" },
    });
    res.status(201).json(result);
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/:sandboxId/file-tree ────────────────────────
// Live file tree from the VM for the Studio Files Tab sidebar

router.get("/:sandboxId/file-tree", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    const maxDepth = parseInt(req.query.max_depth as string, 10) || 4;
    const tree = await proxyToDaytona(`/${sandboxId}/file-tree?max_depth=${maxDepth}`);
    res.json(tree);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/write ───────────────────────────
// AI agent writes a single file directly into the VM

router.post("/:sandboxId/write", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    await proxyToDaytona(`/${sandboxId}/write`, {
      method: "POST",
      body: req.body,
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/write-bulk ──────────────────────
// AI agent writes multiple files into the VM in one batch

router.post("/:sandboxId/write-bulk", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    await proxyToDaytona(`/${sandboxId}/write-bulk`, {
      method: "POST",
      body: req.body,
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/:sandboxId/read ─────────────────────────────
// Read a file from the VM filesystem

router.get("/:sandboxId/read", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    const path = req.query.path as string;
    if (!path) {
      res.status(400).json({ error: "path query param is required" });
      return;
    }
    const result = await proxyToDaytona(`/${sandboxId}/read?path=${encodeURIComponent(path)}`);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/logo ────────────────────────────
// Pre-studio logo upload → writes directly to /workspace/logo.png in the VM

router.post("/:sandboxId/logo", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    const url = `${DAYTONA_URL}/api/v1/workspace/${sandboxId}/logo`;

    // Forward the raw body as binary (image buffer)
    const contentType = req.headers["content-type"] || "application/octet-stream";
    const buffer = Buffer.from(req.body);

    const fetchRes = await fetch(url, {
      method: "POST",
      headers: {
        "Content-Type": contentType,
        "Content-Length": String(buffer.length),
      },
      body: buffer as unknown as BodyInit,
    });

    if (!fetchRes.ok) {
      const text = await fetchRes.text().catch(() => "");
      throw new Error(`Logo upload failed (${fetchRes.status}): ${text}`);
    }

    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/terminal ────────────────────────
// Live bash terminal — returns exit_code + stdout for feedback loop

router.post("/:sandboxId/terminal", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    const result = await proxyToDaytona(`/${sandboxId}/terminal`, {
      method: "POST",
      body: req.body,
    });
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── DELETE /api/workspace/:sandboxId ───────────────────────────────
// Destroy the workspace sandbox

router.delete("/:sandboxId", async (req, res, next) => {
  try {
    const { sandboxId } = req.params;
    await proxyToDaytona(`/${sandboxId}`, { method: "DELETE" });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

export default router;
