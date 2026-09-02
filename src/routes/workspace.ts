/**
 * /api/workspace — Daytona VM orchestration routes.
 *
 * Three families of routes:
 *   1. Project-scoped (preferred): resolve sandbox_id from the projects
 *      table and enforce ownership (project.user_id === req.userId).
 *   2. Preview proxy (Module 2): `/preview/:sandboxId/:port[/<path>]`
 *      resolves a public/loopback URL per (sandbox, port) pair and
 *      proxies HTTP through to the VM via in-VM curl.
 *   3. Legacy sandboxId-based proxies (kept for the AI agent tooling).
 *
 * Auth: requireAuth (JWT) on every route — the sandbox VMs are the user's
 * workspaces; nobody may touch another user's VM.
 */
import { createHmac, randomUUID } from "node:crypto";
import express, { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";
import {
  getProjectRow,
  getProjectRowBySandbox,
  type ProjectRow,
} from "../lib/project-lookup";
import {
  daytonaBaseUrl,
  destroyWorkspace,
  ensureProjectSandbox,
  getAgentInfo,
  getWorkspaceFileTree,
  isSandboxAlive,
  parseDataUrl,
  proxyToDaytona,
  uploadWorkspaceLogo,
} from "../services/daytona-workspace";
import {
  buildPreviewUrl,
  probePort,
  proxyThroughVm,
  type PreviewResolution,
} from "../services/preview-proxy";
// Reverse-tunnel-client: triggers the backend's inbound WS dial to
// the VM when agent-info is fetched. The frontend uses the signed URL
// (returned below) for its own /ws; we ALSO use it to dial in for the
// LLM bridge — bypasses the Daytona EU egress filter (which blocks
// the VM dialing OUT to *.onrender.com).
import { ensureReverseTunnel, isReverseTunnelConnected } from "../services/reverse-tunnel-client";

const router: IRouter = Router();

// JWT auth on every /api/workspace route.
router.use(requireAuth);

// ─── OWNERSHIP RESOLUTION ─────────────────────────────────────────────────
// getProjectRow / getProjectRowBySandbox / ProjectRow live in
// src/lib/project-lookup.ts (shared with services/github-proxy.ts, which
// must not import route modules — the reverse-tunnel client imports it and
// is itself imported by this router).

/**
 * Ownership guard: responds 404 (missing) or 403 (foreign project) and
 * returns false when the request must stop. On true, `row` is narrowed to a
 * ProjectRow owned by the caller.
 */
function isOwnedBy(
  res: Response,
  row: ProjectRow | null,
  userId?: string,
): row is ProjectRow {
  if (!row) {
    res.status(404).json({ error: "Project not found" });
    return false;
  }
  if (!userId || row.user_id !== userId) {
    res.status(403).json({ error: "Forbidden — project belongs to another user" });
    return false;
  }
  return true;
}

async function saveSandboxId(projectId: string, sandboxId: string): Promise<void> {
  const supabase = getServiceSupabase();
  const { error } = await supabase
    .from("projects")
    .update({ sandbox_id: sandboxId, updated_at: new Date().toISOString() })
    .eq("id", projectId);
  if (error) throw new Error(`sandbox_id save: ${error.message}`);
}

/**
 * Ownership guard variant for the preview proxy routes: when the DB
 * is configured, the sandbox MUST be owned by a project whose user_id
 * matches the caller. When Supabase is NOT configured (local dev), the
 * sandbox is allowed through so preview works in offline development.
 *
 * Returns the validated `ProjectRow` (or `null` in dev mode) when the
 * request may proceed, OR sends 404/403 and returns `false` otherwise.
 */
async function authorizeSandboxAccess(
  req: Request,
  res: Response,
  sandboxId: string,
): Promise<ProjectRow | null | false> {
  // Dev fallback: no DB → skip ownership check (matches the legacy
  // sandbox-id proxy behavior elsewhere in this router).
  if (!isSupabaseConfigured()) return null;

  // 1. Sandbox must be real & alive before we touch it.
  let alive = false;
  try {
    alive = await isSandboxAlive(sandboxId);
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "preview: sandbox liveness check threw",
    );
  }
  if (!alive) {
    res.status(404).json({ error: "Sandbox not found or not alive" });
    return false;
  }

  // 2. Caller must own the project that owns this sandbox.
  const row = await getProjectRowBySandbox(sandboxId);
  if (!isOwnedBy(res, row, req.userId)) return false;
  return row;
}

// ─── GET /api/workspace/agent-info/:projectId ────────────────────────────
// Broker connection info for the project's In-VM agent orchestrator (the
// "Shadow Agent" sidecar). The studio frontend calls this (via the vm-ops
// edge function) when the studio mounts: on success it opens a WebSocket
// straight to the daemon inside the VM (the "dumb terminal" model) and the
// multi-agent pipeline runs entirely inside the VM. When the sidecar is
// missing/not-yet-installed, the frontend transparently falls back to the
// host-side SSE pipeline.

router.get("/agent-info/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const projectId = String(req.params.projectId || "");
    if (!projectId) {
      res.status(400).json({ error: "projectId is required" });
      return;
    }

    const row = await getProjectRow(projectId);
    if (!isOwnedBy(res, row, req.userId)) return;

    // No sandbox provisioned yet — nothing to probe.
    if (!row?.sandbox_id) {
      res.json({ installed: false, port: 9000, url: null, token: null, launcher: null, alive: false, app_url: null, app_port: null, engine_url: null, engine_alive: false });
      return;
    }

    // Sandbox dead → sidecar unreachable; report not-installed so the
    // frontend uses SSE (and the normal init flow can re-provision).
    let alive = false;
    try {
      alive = await isSandboxAlive(row.sandbox_id);
    } catch {
      alive = false;
    }
    if (!alive) {
      res.json({ installed: false, port: 9000, url: null, token: null, launcher: null, alive: false, app_url: null, app_port: null, engine_url: null, engine_alive: false });
      return;
    }

    const info = await getAgentInfo(row.sandbox_id);
    // Trigger the BACKEND's inbound WS dial to the VM via the signed
    // daytonaproxy01.eu URL. The VM's orchestrator exposes a
    // /reverse-tunnel WS endpoint that accepts this dial; the
    // orchestrator's worker thread then sends `req` frames over this
    // inbound WS to bridge LLM calls through this backend (with the
    // server-side NVIDIA key injection). Idempotent — no-op if a
    // connection to this sandbox is already live or in-flight.
    if (info.url) {
      ensureReverseTunnel(row.sandbox_id, info.url);
    }
    res.json({
      ...info,
      // Surface the bridge status to the frontend so it can show
      // "in-VM agent ready" only when BOTH the VM is alive AND the
      // reverse-tunnel WS is connected.
      reverse_tunnel_connected: isReverseTunnelConnected(row.sandbox_id),
    });
  } catch (error) {
    // Degradation contract: never 500 here — the frontend falls back to SSE.
    logger.warn(
      { projectId: req.params.projectId, err: error instanceof Error ? error.message : error },
      "agent-info probe failed — frontend will use host-side SSE",
    );
    res.json({ installed: false, port: 9000, url: null, token: null, launcher: null, alive: false, app_url: null, app_port: null, engine_url: null, engine_alive: false });
  }
});

// ─── POST /api/workspace/secrets/:projectId — secure secret delivery ──────
// Contract C2/C6 submission path: browser → edge fn vm-ops {action:
// "secret-provide"} → this route. The VALUE never travels over the /ws
// event bus and never enters agent context — it goes exactly two places:
//   (a) the Daytona org vault via the Python daytona-service
//       (POST /api/sandbox/{sandbox_id}/secrets — creates
//       arcforge-<project8>-<NAME> with a provider host allowlist and
//       mounts it into the sandbox; degrades honestly to
//       vault:"unavailable" when the key lacks manage:secrets), and
//   (b) the VM sidecar's authenticated /internal/secrets route (X-VM-Token
//       — same url+token brokering as getAgentInfo), which merges it into
//       /home/daytona/.system/secrets.env (mode 600) and respawns the dev
//       server so the app process inherits it.
// On decline, the vault is skipped and the sidecar is notified with
// declined:true so the pending request_secret tool call resolves.
//
// LOGGING: the request body carries the secret value — this handler logs
// ONLY {projectId, sandboxId, requestId, name, declined, vault}. The
// pino-http request serializer (app.ts) logs id/method/url only, so the
// value cannot leak through request logs either.

const SECRETS_TIMEOUTS = {
  /** Vault write + mount through the (possibly cold-starting) daytona-service. */
  daytonaVault: 30_000,
  /** In-VM sidecar delivery (local HTTP inside the VM via the preview URL). */
  sidecar: 20_000,
} as const;

/** POST {request_id, name, value|declined, vault} to the sidecar /internal/secrets. */
async function postSecretToSidecar(
  vmUrl: string,
  vmToken: string,
  payload: { request_id: string; name: string; value?: string; declined?: boolean; vault?: string },
): Promise<void> {
  const res = await fetch(`${vmUrl.replace(/\/+$/, "")}/internal/secrets`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-VM-Token": vmToken,
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(SECRETS_TIMEOUTS.sidecar),
  });
  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`sidecar /internal/secrets responded ${res.status}: ${text.slice(0, 300)}`);
  }
}

router.post("/secrets/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const projectId = String(req.params.projectId || "");
    const body = req.body ?? {};
    const requestId: string = typeof body.requestId === "string" ? body.requestId.trim() : "";
    const name: string = typeof body.name === "string" ? body.name.trim() : "";
    const value: string | undefined =
      typeof body.value === "string" && body.value.length > 0 ? body.value : undefined;
    const declined: boolean = body.declined === true;

    if (!requestId || !name) {
      res.status(400).json({ error: "requestId and name are required" });
      return;
    }
    if (!declined && !value) {
      res.status(400).json({ error: "either value or declined:true is required" });
      return;
    }

    // 1. Ownership check (same guard as agent-info).
    const row = await getProjectRow(projectId);
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    // 2. Broker the VM sidecar url+token (same path as agent-info).
    let info: Awaited<ReturnType<typeof getAgentInfo>> | null = null;
    try {
      info = await getAgentInfo(row.sandbox_id);
    } catch (err: unknown) {
      logger.warn(
        { projectId, sandboxId: row.sandbox_id, err: err instanceof Error ? err.message : err },
        "secrets: agent-info probe failed — cannot reach the VM sidecar",
      );
    }
    if (!info?.url || !info.token) {
      res.status(502).json({
        error: "Workspace VM sidecar unreachable — retry when the workspace is running",
      });
      return;
    }

    // 3. Decline path: skip the vault, notify the sidecar, resolve skipped.
    if (declined) {
      try {
        await postSecretToSidecar(info.url, info.token, { request_id: requestId, name, declined: true, vault: "skipped" });
      } catch (err: unknown) {
        const message = err instanceof Error ? err.message : "sidecar unreachable";
        logger.warn(
          { projectId, sandboxId: row.sandbox_id, requestId, name, err: message },
          "secrets: declined-notify to sidecar failed",
        );
        res.status(502).json({ error: "Sidecar unreachable", detail: message });
        return;
      }
      logger.info(
        { projectId, sandboxId: row.sandbox_id, requestId, name, declined: true },
        "secrets: user declined — sidecar notified (vault skipped)",
      );
      res.json({ ok: true, vault: "skipped" });
      return;
    }

    // 4. Value path: (a) Daytona vault via the Python daytona-service,
    //    (b) sidecar delivery. Vault failures degrade to
    //    vault:"unavailable:<reason>" — delivery still proceeds.
    let vault = "stored";
    let vaultDetail: string | undefined;
    try {
      const vaultRes = await fetch(
        `${daytonaBaseUrl()}/api/sandbox/${encodeURIComponent(row.sandbox_id)}/secrets`,
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            name,
            value,
            project_id: projectId,
            mount_env: name,
          }),
          signal: AbortSignal.timeout(SECRETS_TIMEOUTS.daytonaVault),
        },
      );
      if (vaultRes.ok) {
        const payload = (await vaultRes.json().catch(() => null)) as
          | { vault?: unknown; detail?: unknown }
          | null;
        const vaultState = typeof payload?.vault === "string" ? payload.vault : "";
        if (vaultState === "stored" || vaultState === "unavailable") {
          vault = vaultState;
        } else {
          vault = "stored";
        }
        vaultDetail =
          typeof payload?.detail === "string" && payload.detail ? payload.detail : undefined;
      } else {
        const text = await vaultRes.text().catch(() => "");
        vault = `unavailable: daytona-service HTTP ${vaultRes.status}`;
        vaultDetail = text.slice(0, 300) || undefined;
      }
    } catch (err: unknown) {
      // Network error/timeout reaching the daytona-service — degrade, don't fail.
      vault = `unavailable: ${err instanceof Error ? err.message : "daytona-service unreachable"}`;
    }

    // (b) Deliver to the sidecar — THE critical path (the app reads
    //     process.env inside the VM; the vault is defense-in-depth).
    try {
      await postSecretToSidecar(info.url, info.token, { request_id: requestId, name, value, vault });
    } catch (err: unknown) {
      const message = err instanceof Error ? err.message : "sidecar unreachable";
      logger.warn(
        { projectId, sandboxId: row.sandbox_id, requestId, name, vault, err: message },
        "secrets: sidecar delivery failed — value NOT delivered",
      );
      res.status(502).json({ error: "Sidecar unreachable", detail: message, vault });
      return;
    }

    // Name + sandbox only — the value is NEVER logged.
    logger.info(
      { projectId, sandboxId: row.sandbox_id, requestId, name, vault },
      "secrets: delivered to sidecar (vault state recorded)",
    );
    res.json({ ok: true, vault, ...(vaultDetail ? { detail: vaultDetail } : {}) });
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/init — project-scoped workspace bootstrap ─────────
// Create (or reuse) the project's Daytona VM with the scaffold
// /workspace/{git,frontend,backend} + /workspace/logo.png, then upload the
// project logo when present.

router.post("/init", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const body = req.body ?? {};
    const projectId: string =
      (typeof body.project_id === "string" && body.project_id) ||
      (typeof body.projectId === "string" && body.projectId) ||
      "";
    const language = typeof body.language === "string" ? body.language : "nodejs";

    if (!projectId) {
      res.status(400).json({ error: "project_id is required" });
      return;
    }

    // Dev fallback: no DB configured → legacy direct proxy (already behind auth).
    if (!isSupabaseConfigured()) {
      const result = await proxyToDaytona("/init", {
        method: "POST",
        body: { project_id: projectId, user_id: req.userId ?? null, language },
      });
      res.status(201).json(result);
      return;
    }

    // 1. Ownership check.
    const row = await getProjectRow(projectId);
    if (!isOwnedBy(res, row, req.userId)) return;

    // 2-6. Reuse live sandbox or provision + scaffold + logo, then fetch tree.
    const ensured = await ensureProjectSandbox(row, {
      language,
      saveSandboxId: (sandboxId) => saveSandboxId(row.id, sandboxId),
    });

    res.status(ensured.reused ? 200 : 201).json({
      sandbox_id: ensured.sandbox_id,
      tree: ensured.tree,
      logo_uploaded: ensured.logo_uploaded,
      reused: ensured.reused,
    });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/project/:projectId/file-tree ──────────────────────
// Live file tree for the project's VM (Studio Files Tab sidebar).

router.get("/project/:projectId/file-tree", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      // No VM yet — frontend renders the scaffold-empty state.
      res.json({ tree: null, sandbox_id: null });
      return;
    }

    const maxDepth = parseInt(String(req.query.max_depth ?? "4"), 10) || 4;
    const tree = await getWorkspaceFileTree(row.sandbox_id, maxDepth);
    res.json({ tree, sandbox_id: row.sandbox_id });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/project/:projectId/grant ─────────────────────────
// Mint a Forgvi 2.0 workspace grant — the short-lived HMAC token that binds
// one engine run to THIS project's sandbox. The engine verifies the
// signature (shared WORKSPACE_GRANT_SECRET) and then executes the run's
// bash/edit tools directly against the sandbox, making the VM and the
// engine one shared workspace (same filesystem Forgvi 1.0's in-VM swarm
// uses). One grant = one project's sandbox — the engine can never be handed
// another user's workspace because the grant only exists after this route
// verified the caller owns the project.

const WORKSPACE_GRANT_TTL_MS = 20 * 60_000; // 20 minutes — engine wake + run start

function mintGrant(row: ProjectRow, userId: string): string {
  const secret = process.env.WORKSPACE_GRANT_SECRET;
  if (!secret) {
    throw new Error("WORKSPACE_GRANT_SECRET is not configured on the backend");
  }
  const payload = JSON.stringify({
    v: 1,
    projectId: row.id,
    sandboxId: row.sandbox_id,
    userId,
    iat: Date.now(),
    exp: Date.now() + WORKSPACE_GRANT_TTL_MS,
    jti: randomUUID(),
  });
  const body = Buffer.from(payload, "utf-8").toString("base64url");
  const mac = createHmac("sha256", secret).update(body).digest("base64url");
  return `fg1.${body}.${mac}`;
}

router.get("/project/:projectId/grant", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet — the VM is still booting" });
      return;
    }

    const grant = mintGrant(row, req.userId!);
    // The grant is opaque to the browser (it never carries the secret), and
    // expires in minutes. It is handed to the engine with the next run.
    res.json({ grant, sandbox_id: row.sandbox_id, expires_in_ms: WORKSPACE_GRANT_TTL_MS });
  } catch (error) {
    // The missing-secret case is an honest 500, not an ownership error.
    next(error);
  }
});

// ─── POST /api/workspace/project/:projectId/engine-run ────────────────────
// Record the active Forgvi 2.0 run on the project row — what lets a
// re-mounted studio RE-ATTACH after a reload or closed tab (get-session
// hands the record back; the engine's journal replays the run from the
// recorded runId). Written by the browser at run start (via the vm-ops
// engine-run-set action) and settled at finish/abort. The columns are
// engine_run_id / engine_run_origin / engine_run_status /
// engine_run_objective on public.projects.

router.post("/project/:projectId/engine-run", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    const runId = typeof req.body?.runId === "string" ? req.body.runId.trim() : "";
    if (!runId) {
      res.status(400).json({ error: "runId is required" });
      return;
    }
    const engineOrigin = req.body?.engineOrigin === "vm" ? "vm" : "render";
    const status = typeof req.body?.status === "string" && req.body.status ? req.body.status : "running";
    const objective = typeof req.body?.objective === "string" ? req.body.objective : null;

    const supabase = getServiceSupabase();
    const { error } = await supabase
      .from("projects")
      .update({
        engine_run_id: runId,
        engine_run_origin: engineOrigin,
        engine_run_status: status,
        engine_run_objective: objective,
        updated_at: new Date().toISOString(),
      })
      .eq("id", row.id)
      .eq("user_id", req.userId!);
    if (error) throw new Error(`engine-run set: ${error.message}`);

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/project/:projectId/read ───────────────────────────
// Read a file from the project's VM.

router.get("/project/:projectId/read", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    const path = req.query.path as string | undefined;
    if (!path) {
      res.status(400).json({ error: "path query param is required" });
      return;
    }

    const result = await proxyToDaytona(
      `/${encodeURIComponent(row.sandbox_id)}/read?path=${encodeURIComponent(path)}`,
    );
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/project/:projectId/write ─────────────────────────
// Write a single file into the project's VM.

router.post("/project/:projectId/write", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    const path = typeof req.body?.path === "string" ? req.body.path : "";
    if (!path) {
      res.status(400).json({ error: "path is required" });
      return;
    }
    const content = typeof req.body?.content === "string" ? req.body.content : "";

    await proxyToDaytona(`/${encodeURIComponent(row.sandbox_id)}/write`, {
      method: "POST",
      body: { path, content },
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/project/:projectId/write-bulk ────────────────────
// Write multiple files into the project's VM in one batch.

router.post("/project/:projectId/write-bulk", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    const files = Array.isArray(req.body?.files) ? req.body.files : [];
    if (files.length === 0) {
      res.status(400).json({ error: "files array is required" });
      return;
    }

    await proxyToDaytona(`/${encodeURIComponent(row.sandbox_id)}/write-bulk`, {
      method: "POST",
      body: { files },
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/project/:projectId/terminal ──────────────────────
// Execute a bash command in the project's VM (live terminal).

router.post("/project/:projectId/terminal", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    const command = typeof req.body?.command === "string" ? req.body.command.trim() : "";
    if (!command) {
      res.status(400).json({ error: "command is required" });
      return;
    }
    const cwd = typeof req.body?.cwd === "string" && req.body.cwd ? req.body.cwd : "/workspace";

    const result = await proxyToDaytona(`/${encodeURIComponent(row.sandbox_id)}/terminal`, {
      method: "POST",
      body: { command, cwd },
    });
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/project/:projectId/logo ──────────────────────────
// Upload the project logo into the VM at /workspace/logo.png.

// Binary logo bodies (image/*) bypass express.json — parse them as raw buffers.
const logoRawBody = express.raw({ type: ["image/*", "application/octet-stream"], limit: "5mb" });

router.post("/project/:projectId/logo", logoRawBody, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (!row.sandbox_id) {
      res.status(404).json({ error: "No sandbox for this project yet" });
      return;
    }

    // Accept either a raw binary body or { dataUrl: "data:image/png;base64,..." }.
    let bytes: Buffer | null = null;
    let mime = "image/png";
    const contentType = String(req.headers["content-type"] ?? "");
    if (contentType.includes("application/json") && typeof req.body?.dataUrl === "string") {
      const parsed = parseDataUrl(req.body.dataUrl);
      if (parsed) {
        bytes = parsed.bytes;
        mime = parsed.mime;
      }
    } else if (Buffer.isBuffer(req.body)) {
      bytes = req.body;
      if (contentType.startsWith("image/")) mime = contentType;
    }

    if (!bytes || bytes.length === 0) {
      res.status(400).json({ error: "logo binary body or {dataUrl} required" });
      return;
    }

    await uploadWorkspaceLogo(row.sandbox_id, bytes, mime);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── DELETE /api/workspace/project/:projectId ─────────────────────────────
// Destroy the project's VM and clear projects.sandbox_id.

router.delete("/project/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const row = await getProjectRow(String(req.params.projectId));
    if (!isOwnedBy(res, row, req.userId)) return;

    if (row.sandbox_id) {
      try {
        await destroyWorkspace(row.sandbox_id);
      } catch (err: unknown) {
        // Sandbox may already be gone — clearing the pointer is still correct.
        logger.warn(
          { sandboxId: row.sandbox_id, err: err instanceof Error ? err.message : err },
          "Sandbox destroy failed (continuing to clear sandbox_id)",
        );
      }
      const supabase = getServiceSupabase();
      const { error } = await supabase
        .from("projects")
        .update({ sandbox_id: null, updated_at: new Date().toISOString() })
        .eq("id", row.id);
      if (error) throw new Error(`sandbox_id clear: ${error.message}`);
    }

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ═══════════════════════════════════════════════════════════════════════════
// MODULE 2 — REVERSE PROXY (host-side) + PREVIEW URL RESOLVER
// ═══════════════════════════════════════════════════════════════════════════
//
// The Daytona MicroVM only binds dev-server ports inside its network
// namespace. To expose them externally without per-port host ingress,
// we proxy HTTP requests through this backend: each request triggers
// a `curl -sS -i http://localhost:<port><path>` inside the VM via
// runWorkspaceTerminal. The full HTTP response (status line + headers
// + body) is parsed and streamed back to the caller with the original
// Content-Type and status code preserved.
//
//   GET  /api/workspace/preview/:sandboxId/:port        → PreviewResolution JSON
//   ALL  /api/workspace/preview/:sandboxId/:port/<path>  → proxied HTTP stream
//
// Auth: requireAuth (applied at the router level above) + an explicit
// ownership guard that verifies the caller's project owns the sandbox.

/**
 * Parse the `:port` route param to a positive integer 1..65535. Returns
 * NaN on invalid input; callers should treat NaN as a 400 error.
 */
function parsePort(raw: string | undefined): number {
  const n = parseInt(String(raw ?? ""), 10);
  return Number.isInteger(n) && n >= 1 && n <= 65535 ? n : NaN;
}

// ─── GET /api/workspace/preview/:sandboxId/:port ──────────────────────────
// Resolve a (sandbox, port) pair to its PreviewResolution JSON. Probes
// the port liveness inside the VM and returns the public/loopback URL
// the frontend should embed. The trailing-slash form (`.../:port/`)
// falls through to the proxy so the iframe URL produced here, when
// loaded, actually proxies to the VM root rather than returning JSON.

router.get(
  "/preview/:sandboxId/:port",
  async (req: Request, res: Response, next: NextFunction) => {
    try {
      // Strict routing is off by default — `/preview/sbx/5173/` also
      // matches this route. Forward those to the proxy so the
      // preview_url's trailing-slash form actually serves content.
      if (req.path.endsWith("/")) {
        return next();
      }
      const sandboxId = String(req.params.sandboxId);
      const port = parsePort(
        Array.isArray(req.params.port) ? req.params.port[0] : req.params.port,
      );
      if (Number.isNaN(port)) {
        res.status(400).json({ error: "port must be 1..65535" });
        return;
      }

      const authorized = await authorizeSandboxAccess(req, res, sandboxId);
      if (authorized === false) return; // 404/403 already sent

      let alive = false;
      try {
        alive = await probePort(sandboxId, port);
      } catch (err: unknown) {
        logger.warn(
          { sandboxId, port, err: err instanceof Error ? err.message : err },
          "preview resolver: probePort threw",
        );
      }

      const resolution: PreviewResolution = {
        sandbox_id: sandboxId,
        port,
        preview_url: buildPreviewUrl(sandboxId, port),
        internal_url: `http://localhost:${port}`,
        alive,
      };
      res.json(resolution);
    } catch (error) {
      next(error);
    }
  },
);

// ─── ALL /api/workspace/preview/:sandboxId/:port/* — proxy through VM ────
// Mounted as middleware so it matches EVERY method (GET/POST/PUT/etc)
// and every sub-path under `/preview/:sandboxId/:port/...`. The
// in-VM path is derived from `req.url` (the portion of the URL after
// the matched prefix). Query strings are forwarded.

router.use(
  "/preview/:sandboxId/:port",
  async (req: Request, res: Response, next: NextFunction) => {
    try {
      const sandboxId = String(req.params.sandboxId);
      const port = parsePort(
        Array.isArray(req.params.port) ? req.params.port[0] : req.params.port,
      );
      if (Number.isNaN(port)) {
        res.status(400).json({ error: "port must be 1..65535" });
        return;
      }

      const authorized = await authorizeSandboxAccess(req, res, sandboxId);
      if (authorized === false) return;

      // `req.url` at a mounted middleware is the part of the path
      // AFTER the matched mount point. For `/preview/sbx/5173/foo?b=1`
      // the matched prefix is `/preview/sbx/5173` and req.url is
      // `/foo?b=1`. Normalize empty → "/" so curl always hits root.
      const remainder = (req.url || "/").replace(/^\/+/, "/");
      const queryIdx = remainder.indexOf("?");
      const inVmPath = queryIdx >= 0 ? remainder.slice(0, queryIdx) : remainder;
      const queryString = queryIdx >= 0 ? remainder.slice(queryIdx + 1) : undefined;

      const proxied = await proxyThroughVm(
        sandboxId,
        port,
        inVmPath || "/",
        queryString,
      );

      if (proxied.failed || proxied.status === 0) {
        // Curl couldn't get a usable HTTP response — likely the dev
        // server isn't bound yet, or the VM is unreachable.
        res.status(502).json({
          error: "Preview upstream unavailable",
          sandbox_id: sandboxId,
          port,
          path: inVmPath,
          detail: proxied.body.toString("utf-8").slice(0, 1024) || "no response",
        });
        return;
      }

      // Preserve the upstream status code & content-type (default to
      // octet-stream when the upstream omitted it — mirrors curl).
      res.status(proxied.status);
      const contentType =
        proxied.headers["content-type"] ?? "application/octet-stream";
      res.setHeader("Content-Type", contentType);

      // Forward a safe subset of headers. Skip hop-by-hop & security-
      // sensitive headers (Set-Cookie, Transfer-Encoding, etc.) so we
      // don't shadow the backend's own response semantics.
      const FORWARDABLE = [
        "cache-control",
        "etag",
        "last-modified",
        "vary",
        "access-control-allow-origin",
        "access-control-allow-methods",
        "access-control-allow-headers",
      ];
      for (const key of FORWARDABLE) {
        const val = proxied.headers[key];
        if (val) res.setHeader(toHeaderCase(key), val);
      }

      res.send(proxied.body);
    } catch (error) {
      next(error);
    }
  },
);

/**
 * Title-case a header key for the outbound response (e.g.
 * "content-type" → "Content-Type"). Lowercase input is the canonical
 * form we store in `proxied.headers`.
 */
function toHeaderCase(key: string): string {
  return key
    .split("-")
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join("-");
}

// ═══════════════════════════════════════════════════════════════════════════
// LEGACY SANDBOX-ID PROXIES (authenticated) — kept for AI agent tooling.
// SECURITY (GROUP 3 audit): every legacy route now enforces OWNERSHIP via
// authorizeSandboxAccess — the sandbox must belong to a project owned by the
// caller (previously any authenticated user with a sandboxId could
// read/write/exec/destroy another user's VM).
// ═══════════════════════════════════════════════════════════════════════════

// ─── GET /api/workspace/:sandboxId/file-tree ────────────────────────

router.get("/:sandboxId/file-tree", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    const maxDepth = parseInt(String(req.query.max_depth ?? "4"), 10) || 4;
    const tree = await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/file-tree?max_depth=${maxDepth}`);
    res.json(tree);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/write ───────────────────────────

router.post("/:sandboxId/write", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/write`, {
      method: "POST",
      body: req.body,
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/write-bulk ──────────────────────

router.post("/:sandboxId/write-bulk", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/write-bulk`, {
      method: "POST",
      body: req.body,
    });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/workspace/:sandboxId/read ─────────────────────────────

router.get("/:sandboxId/read", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    const path = req.query.path as string;
    if (!path) {
      res.status(400).json({ error: "path query param is required" });
      return;
    }
    const result = await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/read?path=${encodeURIComponent(path)}`);
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/logo ────────────────────────────
// Pre-studio logo upload → writes directly to /workspace/logo.png in the VM.
// The daytona-service expects multipart/form-data with a `file` field
// (see daytona-service/app/routers/workspace.py → upload_logo).

router.post("/:sandboxId/logo", logoRawBody, async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;

    // Accept either a raw binary body or { dataUrl: "data:image/png;base64,..." }.
    let bytes: Buffer | null = null;
    let mime = "image/png";
    const contentType = String(req.headers["content-type"] ?? "");
    if (contentType.includes("application/json") && typeof req.body?.dataUrl === "string") {
      const parsed = parseDataUrl(req.body.dataUrl);
      if (parsed) {
        bytes = parsed.bytes;
        mime = parsed.mime;
      }
    } else if (Buffer.isBuffer(req.body)) {
      bytes = req.body;
      if (contentType.startsWith("image/")) mime = contentType;
    }

    if (!bytes || bytes.length === 0) {
      res.status(400).json({ error: "logo binary body or {dataUrl} required" });
      return;
    }

    await uploadWorkspaceLogo(sandboxId, bytes, mime);
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/workspace/:sandboxId/terminal ────────────────────────

router.post("/:sandboxId/terminal", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    const result = await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/terminal`, {
      method: "POST",
      body: req.body,
    });
    res.json(result);
  } catch (error) {
    next(error);
  }
});

// ─── DELETE /api/workspace/:sandboxId ───────────────────────────────

router.delete("/:sandboxId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const sandboxId = String(req.params.sandboxId);
    if (!(await authorizeSandboxAccess(req, res, sandboxId))) return;
    await proxyToDaytona(`/${encodeURIComponent(sandboxId)}`, { method: "DELETE" });
    res.status(204).send();
  } catch (error) {
    next(error);
  }
});

export default router;
