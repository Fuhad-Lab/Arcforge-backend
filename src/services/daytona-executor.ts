import { logger } from "../lib/logger";
import type { GeneratedFile } from "./god-mode-protocol";

// ─── TYPES ─────────────────────────────────────────────────────────────

export type ContainerKind = "frontend" | "backend";

export type ContainerInfo = {
  containerId: string;
  containerName: string;
  projectId: string;
  kind: ContainerKind;
  port: number;
  url: string;
  status: "running" | "starting" | "stopped";
};

type TrackedSandbox = ContainerInfo & {
  sandboxId: string;
  startedAt: number;
};

// ─── CONSTANTS ──────────────────────────────────────────────────────────

const DAYTONA_URL = process.env.DAYTONA_SERVICE_URL || "https://arcforge-daytona.onrender.com";
const SANDBOX_CREATE_TIMEOUT_MS = 120_000;
const EXEC_TIMEOUT_MS = 30_000;

// ─── HELPERS ───────────────────────────────────────────────────────────

function sandboxName(projectId: string, kind: string): string {
  const safe = projectId.replace(/[^a-zA-Z0-9._-]/g, "");
  return `arcforge-${safe}-${kind}`;
}

/** Call the Daytona service REST API. */
async function daytonaFetch<T>(
  path: string,
  options: RequestInit = {},
): Promise<T> {
  const url = `${DAYTONA_URL}${path}`;
  const res = await fetch(url, {
    ...options,
    headers: {
      "Content-Type": "application/json",
      ...options.headers,
    },
  });

  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`Daytona API ${res.status}: ${body || res.statusText}`);
  }

  // 204 No Content
  if (res.status === 204) return undefined as T;

  return res.json() as Promise<T>;
}

// ─── DAYTONA EXECUTOR ────────────────────────────────────────────────

export class DaytonaExecutor {
  /** Map keyed by `projectId:kind` → tracked sandbox */
  private readonly sandboxes = new Map<string, TrackedSandbox>();

  /**
   * Create a Daytona sandbox, upload all project files, install deps,
   * and start the server.  Replaces the old Docker-based startContainer.
   */
  async startContainer(
    projectId: string,
    files: GeneratedFile[],
    entryPoint: string,
    port: number,
    kind: ContainerKind,
    userId?: string,
  ): Promise<ContainerInfo> {
    const key = `${projectId}:${kind}`;
    const name = sandboxName(projectId, kind);

    // Validate port
    if (!Number.isInteger(port) || port < 1024 || port > 65_535) {
      throw new Error("Container port must be between 1024 and 65535.");
    }

    // Stop existing sandbox if running
    await this.stopContainer(projectId, kind).catch(() => { /* ignore */ });

    // 1. Create the Daytona sandbox
    logger.info(
      { projectId, kind, name },
      "Creating Daytona sandbox",
    );

    const sandbox = await daytonaFetch<{ id: string; state: string; name: string }>(
      "/api/v1/sandboxes",
      {
        method: "POST",
        body: JSON.stringify({
          name,
          method: "snapshot",
          language: "nodejs",
          resources: { cpu: 2, memory: "4Gi", disk: "10Gi" },
          env_vars: { PORT: String(port), NODE_ENV: "production" },
          labels: { user_id: userId || 'unknown', project_id: projectId, kind },
        }),
      },
    );

    const sandboxId = sandbox.id;
    logger.info(
      { projectId, kind, sandboxId, name },
      "Daytona sandbox created",
    );

    // 2. Upload all project files to the sandbox VFS
    if (files.length > 0) {
      const uploadPayload = files.map((f) => ({
        path: `/home/daytona/${f.path}`,
        content: f.content,
      }));

      await daytonaFetch(
        `/api/v1/sandboxes/${sandboxId}/files/upload-bulk`,
        {
          method: "POST",
          body: JSON.stringify({ files: uploadPayload }),
        },
      );

      logger.info(
        { projectId, kind, sandboxId, fileCount: files.length },
        "Uploaded files to Daytona sandbox",
      );
    }

    // 3. Install dependencies and start the service
    const startCmd = kind === "frontend"
      ? `cd /home/daytona && npm install --silent 2>/dev/null && npx serve -s . -l ${port} &`
      : `cd /home/daytona && npm install --silent 2>/dev/null && node ${entryPoint} &`;

    const execResult = await daytonaFetch<{
      exit_code: number;
      stdout: string;
      stderr: string;
      duration_ms: number;
    }>(`/api/v1/sandboxes/${sandboxId}/exec`, {
      method: "POST",
      body: JSON.stringify({
        command: startCmd,
        cwd: "/home/daytona",
        timeout_ms: EXEC_TIMEOUT_MS,
      }),
    });

    if (execResult.exit_code !== 0) {
      logger.warn(
        { projectId, kind, sandboxId, exitCode: execResult.exit_code, stderr: execResult.stderr },
        "Start command had non-zero exit (may be backgrounded)",
      );
    }

    const info: ContainerInfo = {
      containerId: sandboxId,
      containerName: name,
      projectId,
      kind,
      port,
      url: `https://arcforge-daytona.onrender.com/sandboxes/${sandboxId}/preview`,
      status: "running",
    };

    this.sandboxes.set(key, { ...info, sandboxId, startedAt: Date.now() });

    logger.info(
      { projectId, kind, sandboxId, name, port },
      "Daytona sandbox started and serving",
    );

    return info;
  }

  /**
   * Stop and delete the Daytona sandbox.
   */
  async stopContainer(projectId: string, kind: string): Promise<void> {
    const key = `${projectId}:${kind}`;
    const tracked = this.sandboxes.get(key);

    if (tracked) {
      try {
        await daytonaFetch(`/api/v1/sandboxes/${tracked.sandboxId}`, {
          method: "DELETE",
        });
        logger.info(
          { projectId, kind, sandboxId: tracked.sandboxId },
          "Daytona sandbox deleted",
        );
      } catch (err) {
        const msg = err instanceof Error ? err.message : String(err);
        logger.warn(
          { projectId, kind, sandboxId: tracked.sandboxId, error: msg },
          "Failed to delete Daytona sandbox",
        );
      }
      this.sandboxes.delete(key);
    }
  }

  /**
   * Returns the URL where the sandbox is accessible, or null if not running.
   */
  getContainerUrl(projectId: string, kind: string): string | null {
    const key = `${projectId}:${kind}`;
    const tracked = this.sandboxes.get(key);
    if (!tracked || tracked.status !== "running") return null;
    return tracked.url;
  }

  /**
   * Get recent sandbox logs by running a journalctl/tail command.
   */
  async getLogs(projectId: string, kind: string): Promise<string> {
    const key = `${projectId}:${kind}`;
    const tracked = this.sandboxes.get(key);
    if (!tracked) {
      throw new Error(`No active sandbox for ${projectId}:${kind}`);
    }

    const result = await daytonaFetch<{
      exit_code: number;
      stdout: string;
      stderr: string;
    }>(`/api/v1/sandboxes/${tracked.sandboxId}/exec`, {
      method: "POST",
      body: JSON.stringify({
        command: "bash -c 'ps aux && echo ---PROCESSES--- && ls -la /home/daytona/'",
        timeout_ms: 10000,
      }),
    });

    return result.stdout || result.stderr || "(no output)";
  }

  /**
   * Execute a command in a sandbox and return its stdout.
   * This is the feedback loop: exit_code + result → agent context.
   */
  async execCommand(
    projectId: string,
    kind: string,
    command: string,
  ): Promise<string> {
    const key = `${projectId}:${kind}`;
    const tracked = this.sandboxes.get(key);
    if (!tracked) {
      throw new Error(`No active sandbox for ${projectId}:${kind}`);
    }

    const result = await daytonaFetch<{
      exit_code: number;
      stdout: string;
      stderr: string;
      duration_ms: number;
    }>(`/api/v1/sandboxes/${tracked.sandboxId}/exec`, {
      method: "POST",
      body: JSON.stringify({
        command,
        cwd: "/home/daytona",
        timeout_ms: EXEC_TIMEOUT_MS,
      }),
    });

    // The feedback loop: if non-zero exit, include stderr in the output
    // so the agent can self-debug
    if (result.exit_code !== 0) {
      return `[exit ${result.exit_code}] ${result.stderr || ""}\n${result.stdout}`;
    }
    return result.stdout;
  }

  /**
   * Check if a sandbox is tracked as running for the given project+kind.
   */
  isActive(projectId: string, kind: string): boolean {
    const key = `${projectId}:${kind}`;
    return this.sandboxes.has(key);
  }

  /**
   * Return all tracked sandboxes.
   */
  listActive(): ContainerInfo[] {
    return Array.from(this.sandboxes.values()).map(({
      containerId,
      containerName,
      projectId,
      kind,
      port,
      url,
      status,
    }) => ({ containerId, containerName, projectId, kind, port, url, status }));
  }
}

/** Singleton instance */
export const daytonaExecutor = new DaytonaExecutor();
