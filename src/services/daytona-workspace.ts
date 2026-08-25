/**
 * Daytona workspace orchestration service.
 *
 * Shared client for the Python daytona-service (deployed at
 * DAYTONA_SERVICE_URL) used by /api/workspace routes and the /api/generate
 * pipeline. All user-project code lives INSIDE Daytona MicroVMs — never on
 * this backend's local disk.
 */
import { logger } from "../lib/logger";
import { getSingleModeLlmConfig } from "./agent-platform";

// Generous timeouts — the Render-hosted daytona-service may cold-start.
const TIMEOUTS = {
  sandboxCheck: 30_000,
  init: 240_000,
  fileTree: 60_000,
  writeBulk: 180_000,
  logo: 60_000,
  terminal: 60_000,
  destroy: 60_000,
} as const;

/**
 * Base URL of the Python daytona-service. MUST come from the
 * DAYTONA_SERVICE_URL env var — never hardcoded.
 */
export function daytonaBaseUrl(): string {
  const url = process.env.DAYTONA_SERVICE_URL?.trim();
  if (!url) {
    throw new Error(
      "DAYTONA_SERVICE_URL environment variable is not set — VM orchestration is unavailable",
    );
  }
  return url.replace(/\/+$/, "");
}

export type DaytonaProxyOptions = {
  method?: string;
  body?: unknown;
  headers?: Record<string, string>;
  timeoutMs?: number;
};

/**
 * Proxy a JSON request to the Daytona workspace service
 * (`/api/v1/workspace/...`). Returns parsed JSON (or null for 204).
 */
export async function proxyToDaytona(
  reqPath: string,
  options: DaytonaProxyOptions = {},
): Promise<unknown> {
  const url = `${daytonaBaseUrl()}/api/v1/workspace${reqPath}`;
  const res = await fetch(url, {
    method: options.method || "GET",
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
    body: options.body !== undefined ? JSON.stringify(options.body) : undefined,
    signal: AbortSignal.timeout(options.timeoutMs ?? 60_000),
  });

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Daytona workspace ${res.status}: ${text || res.statusText}`);
  }

  if (res.status === 204) return null;
  return res.json();
}

// ─── SANDBOX LIVENESS ──────────────────────────────────────────────────────

/**
 * Fetch sandbox metadata from the daytona-service.
 * Returns null when the sandbox does not exist / is unreachable.
 */
export async function daytonaGetSandbox(
  sandboxId: string,
): Promise<Record<string, unknown> | null> {
  try {
    const res = await fetch(
      `${daytonaBaseUrl()}/api/v1/sandboxes/${encodeURIComponent(sandboxId)}`,
      { signal: AbortSignal.timeout(TIMEOUTS.sandboxCheck) },
    );
    if (!res.ok) return null;
    return (await res.json()) as Record<string, unknown>;
  } catch {
    return null;
  }
}

/**
 * Verify a sandbox is still alive (exists and reachable).
 * Unknown / unreachable sandboxes are treated as dead so callers recreate.
 */
export async function isSandboxAlive(sandboxId: string): Promise<boolean> {
  const sandbox = await daytonaGetSandbox(sandboxId);
  if (!sandbox) return false;
  const state = String(sandbox.state ?? "").toLowerCase();
  // Terminal states — treat as dead so a fresh workspace gets provisioned.
  if (state === "error" || state === "archived") return false;
  return true;
}

/**
 * Ensure a live sandbox is RUNNING (not merely existing). Daytona
 * auto-stops sandboxes after the idle interval — a stopped sandbox keeps
 * its disk but rejects exec/fs calls. This starts it and waits for the
 * run state. No-op for already-running sandboxes.
 */
export async function ensureSandboxRunning(sandboxId: string): Promise<void> {
  const sandbox = await daytonaGetSandbox(sandboxId);
  if (!sandbox) return;
  const state = String(sandbox.state ?? "").toLowerCase();
  if (state === "running" || state === "started" || state === "starting") return;
  if (state === "error" || state === "archived") return; // dead — caller handles
  try {
    await fetch(
      `${daytonaBaseUrl()}/api/v1/sandboxes/${encodeURIComponent(sandboxId)}/start`,
      { method: "POST", signal: AbortSignal.timeout(120_000) },
    );
    // Wait for the running state (up to ~60s).
    const deadline = Date.now() + 60_000;
    while (Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 3_000));
      const check = await daytonaGetSandbox(sandboxId);
      const s = String(check?.state ?? "").toLowerCase();
      if (s === "running" || s === "started") return;
      if (s === "error" || s === "archived") return;
    }
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "ensureSandboxRunning: start attempt failed (continuing)",
    );
  }
}

// ─── WORKSPACE LIFECYCLE ───────────────────────────────────────────────────

/** Daytona accepts only python|typescript|javascript — normalize legacy names. */
function normalizeLanguage(language?: string): "typescript" | "python" | "javascript" {
  const raw = (language ?? "typescript").toLowerCase();
  if (raw === "python" || raw === "py") return "python";
  if (raw === "javascript" || raw === "js") return "javascript";
  // nodejs, typescript, ts, nextjs, and anything else → typescript
  return "typescript";
}

export type DaytonaWorkspaceInitResult = {
  sandbox_id: string;
  project_id: string;
  user_id?: string | null;
  state: string;
  provision_time_ms: number;
  workspace_root?: string;
  structure?: string[];
};

/** Connection info for the in-VM agent orchestrator sidecar, probed live
 * from the daytona-service (which reads the token from inside the VM). */
export type AgentSidecarInfo = {
  installed: boolean;
  port: number;
  url: string | null;
  token: string | null;
  launcher: string | null;
  alive: boolean;
};

/** LLM config handed to the in-VM orchestrator daemon at sandbox creation
 * so the multi-agent pipeline runs entirely inside the VM. */
export type AgentLlmConfig = { url: string; key: string; model: string };

/**
 * Resolve the sidecar LLM config from the platform's single-mode ("Solo")
 * settings. Returns undefined when no key is configured — the sidecar then
 * installs without an LLM endpoint and the platform keeps using host-side
 * SSE for that project.
 */
function buildAgentLlmConfig(): AgentLlmConfig | undefined {
  const cfg = getSingleModeLlmConfig();
  if (!cfg.url || !cfg.key) return undefined;
  return cfg;
}

/**
 * Create (and scaffold) a project workspace VM via the daytona-service.
 * The sandbox is labeled with {user_id, project_id, type:"workspace"} and
 * scaffolded with /workspace/{git,frontend,backend} + /workspace/logo.png.
 */
export async function initWorkspace(params: {
  project_id: string;
  user_id?: string;
  language?: string;
  agent_llm?: AgentLlmConfig;
}): Promise<DaytonaWorkspaceInitResult> {
  const result = await proxyToDaytona("/init", {
    method: "POST",
    body: {
      project_id: params.project_id,
      user_id: params.user_id ?? null,
      language: normalizeLanguage(params.language),
      ...(params.agent_llm ? { agent_llm: params.agent_llm } : {}),
    },
    timeoutMs: TIMEOUTS.init,
  });
  return result as DaytonaWorkspaceInitResult;
}

/** Probe the in-VM agent orchestrator ("Shadow Agent") in a sandbox.
 *
 * The daytona-service reads the per-VM shared-secret token from inside
 * the VM, health-checks the daemon on port 9000, and opens the Daytona
 * preview link the browser's WebSocket connects to. Returns
 * installed=false while the async install is still in flight (or when the
 * sidecar is unavailable and the platform uses host-side SSE instead).
 */
export async function getAgentInfo(sandboxId: string): Promise<AgentSidecarInfo> {
  const result = await proxyToDaytona(
    `/${encodeURIComponent(sandboxId)}/agent-info`,
    { timeoutMs: 30_000 },
  );
  return result as AgentSidecarInfo;
}

/** Destroy a workspace sandbox. */
export async function destroyWorkspace(sandboxId: string): Promise<void> {
  await proxyToDaytona(`/${encodeURIComponent(sandboxId)}`, {
    method: "DELETE",
    timeoutMs: TIMEOUTS.destroy,
  });
}

// ─── FILE OPERATIONS ───────────────────────────────────────────────────────

/** Live file tree from the VM (feeds the Studio Files Tab sidebar). */
export async function getWorkspaceFileTree(
  sandboxId: string,
  maxDepth = 4,
): Promise<unknown> {
  return proxyToDaytona(
    `/${encodeURIComponent(sandboxId)}/file-tree?max_depth=${maxDepth}`,
    { timeoutMs: TIMEOUTS.fileTree },
  );
}

/** Write a single file into the VM. */
export async function writeWorkspaceFile(
  sandboxId: string,
  path: string,
  content: string,
): Promise<void> {
  await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/write`, {
    method: "POST",
    body: { path, content },
    timeoutMs: TIMEOUTS.writeBulk,
  });
}

/** Bulk-write multiple files ({path, content}) into the VM in one batch. */
export async function writeWorkspaceFilesBulk(
  sandboxId: string,
  files: Array<{ path: string; content: string }>,
): Promise<void> {
  if (files.length === 0) return;
  await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/write-bulk`, {
    method: "POST",
    body: { files },
    timeoutMs: TIMEOUTS.writeBulk,
  });
}

// ─── LOGO UPLOAD ───────────────────────────────────────────────────────────

/**
 * Parse a `data:<mime>;base64,<payload>` URL into raw bytes.
 * Returns null when the string is not a base64 data URL.
 */
export function parseDataUrl(
  dataUrl: string,
): { mime: string; bytes: Buffer } | null {
  const match = /^data:([^;,]+);base64,([\s\S]*)$/.exec(dataUrl.trim());
  if (!match) return null;
  try {
    const bytes = Buffer.from(match[2], "base64");
    if (bytes.length === 0) return null;
    return { mime: match[1], bytes };
  } catch {
    return null;
  }
}

/**
 * Upload a project logo to /workspace/logo.png inside the VM.
 * The daytona-service expects a multipart/form-data file field named `file`.
 */
export async function uploadWorkspaceLogo(
  sandboxId: string,
  bytes: Uint8Array,
  mime: string,
): Promise<void> {
  const form = new FormData();
  form.append(
    "file",
    new Blob([bytes as unknown as BlobPart], { type: mime || "image/png" }),
    "logo.png",
  );

  const res = await fetch(
    `${daytonaBaseUrl()}/api/v1/workspace/${encodeURIComponent(sandboxId)}/logo`,
    {
      method: "POST",
      body: form,
      signal: AbortSignal.timeout(TIMEOUTS.logo),
    },
  );

  if (!res.ok) {
    const text = await res.text().catch(() => "");
    throw new Error(`Logo upload failed (${res.status}): ${text || res.statusText}`);
  }
}

// ─── TERMINAL ──────────────────────────────────────────────────────────────

export type TerminalResult = {
  exit_code: number;
  stdout: string;
  stderr: string;
  timed_out?: boolean;
  duration_ms?: number | null;
};

/** Execute a bash command inside the VM (live terminal). */
export async function runWorkspaceTerminal(
  sandboxId: string,
  command: string,
  cwd = "/workspace",
  timeoutMs = 60_000,
): Promise<TerminalResult> {
  const result = await proxyToDaytona(`/${encodeURIComponent(sandboxId)}/terminal`, {
    method: "POST",
    body: { command, cwd },
    timeoutMs,
  });
  return result as TerminalResult;
}

// ─── COMBINED PROJECT SANDBOX HELPERS ─────────────────────────────────────

export type ProjectSandboxRow = {
  id: string;
  user_id: string;
  logo_url: string | null;
  sandbox_id: string | null;
};

export type EnsuredSandbox = {
  sandbox_id: string;
  tree: unknown;
  logo_uploaded: boolean;
  reused: boolean;
};

/**
 * Ensure a project has a live sandbox VM, uploading the project logo
 * (when the sandbox is freshly created) and returning the live file tree.
 *
 * `saveSandboxId` is invoked with the new sandbox id when one is created so
 * callers can persist it (projects.sandbox_id). Failures to save are
 * non-fatal — logged by the caller.
 */
export async function ensureProjectSandbox(
  row: ProjectSandboxRow,
  options: {
    language?: string;
    saveSandboxId?: (sandboxId: string) => Promise<void>;
  } = {},
): Promise<EnsuredSandbox> {
  // 1. Reuse the existing sandbox when it is still alive.
  if (row.sandbox_id && (await isSandboxAlive(row.sandbox_id))) {
    // Daytona auto-stops idle sandboxes — a stopped sandbox keeps its disk
    // but rejects exec/fs. Start it before reuse.
    await ensureSandboxRunning(row.sandbox_id);
    const tree = await getWorkspaceFileTree(row.sandbox_id);
    return {
      sandbox_id: row.sandbox_id,
      tree,
      logo_uploaded: false,
      reused: true,
    };
  }

  // 1b. QUOTA SELF-HEALING: a dead (error/archived) sandbox still holds CPU
  //     quota on Daytona's free tier. Delete it before provisioning the
  //     replacement — otherwise every failed VM permanently leaks quota
  //     until the org hits "Total CPU limit exceeded" (exactly what
  //     happened live: 10 stale Error sandboxes ate the entire 10-CPU
  //     budget and blocked ALL workspace creation).
  if (row.sandbox_id) {
    try {
      await destroyWorkspace(row.sandbox_id);
      logger.info(
        { projectId: row.id, deadSandboxId: row.sandbox_id },
        "Deleted dead sandbox to free CPU quota before re-provisioning",
      );
    } catch (err: unknown) {
      // Best-effort — a 404 (already gone) is fine.
      logger.debug(
        { projectId: row.id, err: err instanceof Error ? err.message : err },
        "Dead-sandbox cleanup failed (continuing)",
      );
    }
  }

  // 2. Provision a fresh, scaffolded sandbox labeled with user + project ids.
  //    The In-VM agent sidecar receives the single-mode LLM config so its
  //    pipeline runs autonomously inside the VM (In-VM Sidecar pattern).
  const init = await initWorkspace({
    project_id: row.id,
    user_id: row.user_id,
    language: options.language,
    agent_llm: buildAgentLlmConfig(),
  });
  const sandboxId = init.sandbox_id;

  if (options.saveSandboxId) {
    try {
      await options.saveSandboxId(sandboxId);
    } catch (err: unknown) {
      logger.warn(
        { projectId: row.id, sandboxId, err: err instanceof Error ? err.message : err },
        "Failed to persist sandbox_id to project",
      );
    }
  }

  // 3. Upload the project logo (data URL) to /workspace/logo.png.
  let logoUploaded = false;
  if (row.logo_url) {
    const logo = parseDataUrl(row.logo_url);
    if (logo) {
      try {
        await uploadWorkspaceLogo(sandboxId, logo.bytes, logo.mime);
        logoUploaded = true;
      } catch (err: unknown) {
        logger.warn(
          { projectId: row.id, sandboxId, err: err instanceof Error ? err.message : err },
          "Logo upload to VM failed",
        );
      }
    }
  }

  // 4. Live file tree after scaffold + logo.
  const tree = await getWorkspaceFileTree(sandboxId);
  return { sandbox_id: sandboxId, tree, logo_uploaded: logoUploaded, reused: false };
}
