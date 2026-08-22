import { spawn, type ChildProcess } from "node:child_process";
import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import os from "node:os";
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

type TrackedContainer = ContainerInfo & {
  startedAt: number;
};

// ─── CONSTANTS ──────────────────────────────────────────────────────────

const BASE_IMAGE = "node:20-alpine";
const START_TIMEOUT_MS = 30_000;
const EXEC_TIMEOUT_MS = 10_000;
const DOCKER_STOP_TIMEOUT_S = 10;

// ─── HELPERS ───────────────────────────────────────────────────────────

function containerName(projectId: string, kind: string): string {
  // Sanitize projectId to be Docker-safe (strip UUID hyphens, prefix with arcforge-)
  const safe = projectId.replace(/[^a-zA-Z0-9._-]/g, "");
  return `arcforge-${safe}-${kind}`;
}

/** Run a docker CLI command and collect stdout. Rejects on non-zero exit. */
function dockerCommand(
  args: string[],
  timeoutMs: number,
): Promise<string> {
  return new Promise((resolve, reject) => {
    let stdout = "";
    let stderr = "";
    const child: ChildProcess = spawn("docker", args, {
      stdio: ["ignore", "pipe", "pipe"],
    });
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      reject(new Error(`Docker command timed out after ${timeoutMs}ms: docker ${args.join(" ")}`));
    }, timeoutMs);

    child.stdout?.on("data", (chunk: Buffer) => { stdout += chunk.toString(); });
    child.stderr?.on("data", (chunk: Buffer) => { stderr += chunk.toString(); });
    child.on("error", (err) => { clearTimeout(timer); reject(err); });
    child.on("close", (code) => {
      clearTimeout(timer);
      if (code === 0) {
        resolve(stdout.trim());
      } else {
        reject(new Error(`Docker command failed (exit ${code}): ${stderr.trim() || stdout.trim()}`));
      }
    });
  });
}

// ─── DOCKER EXECUTOR ───────────────────────────────────────────────────

export class DockerExecutor {
  /** Map keyed by `projectId:kind` */
  private readonly containers = new Map<string, TrackedContainer>();

  /**
   * Create a Docker container from the appropriate base image, copy files
   * in, install deps, and start the server.
   */
  async startContainer(
    projectId: string,
    files: GeneratedFile[],
    entryPoint: string,
    port: number,
    kind: ContainerKind,
  ): Promise<ContainerInfo> {
    const key = `${projectId}:${kind}`;
    const name = containerName(projectId, kind);

    // Validate port
    if (!Number.isInteger(port) || port < 1024 || port > 65_535) {
      throw new Error("Container port must be between 1024 and 65535.");
    }

    // Stop existing container if running
    await this.stopContainer(projectId, kind).catch(() => { /* ignore */ });

    // Create a temporary directory with the project files
    const tmpDir = path.join(os.tmpdir(), `arcforge-docker-${randomUUID()}`);
    await fs.mkdir(tmpDir, { recursive: true });

    try {
      // Write all generated files to the temp directory
      for (const file of files) {
        const filePath = path.resolve(tmpDir, file.path);
        await fs.mkdir(path.dirname(filePath), { recursive: true });
        await fs.writeFile(filePath, file.content, "utf8");
      }

      // Ensure there is a package.json
      const pkgPath = path.join(tmpDir, "package.json");
      let hasPackageJson: boolean;
      try {
        await fs.access(pkgPath);
        hasPackageJson = true;
      } catch {
        hasPackageJson = false;
      }

      if (!hasPackageJson) {
        const pkg = {
          name: `arcforge-${kind}`,
          version: "1.0.0",
          private: true,
          scripts: {
            ...(kind === "frontend"
              ? { start: `npx serve -s . -l ${port}` }
              : { start: `node ${entryPoint}` }),
          },
          dependencies: {},
        };
        await fs.writeFile(pkgPath, JSON.stringify(pkg, null, 2), "utf8");
      }

      // Build the startup command
      let startCmd: string;
      if (kind === "frontend") {
        startCmd = `npx serve -s . -l ${port}`;
      } else {
        startCmd = `node ${entryPoint}`;
      }

      // Pull the base image (best effort — if already present this is a no-op)
      try {
        await dockerCommand(["pull", BASE_IMAGE], START_TIMEOUT_MS);
      } catch (err) {
        logger.warn(
          { error: err instanceof Error ? err.message : String(err) },
          "Failed to pull base image, proceeding anyway",
        );
      }

      // Run the container in detached mode with --network host
      const containerId = await dockerCommand(
        [
          "run",
          "--detach",
          "--name", name,
          "--network", "host",
          "--workdir", "/app",
          "--env", `PORT=${port}`,
          "--env", `NODE_ENV=production`,
          "--init",           // PID 1 signal forwarding
          "--rm",            // auto-cleanup on stop
          BASE_IMAGE,
          "sh", "-c",
          `npm install --silent 2>/dev/null && ${startCmd}`,
        ],
        START_TIMEOUT_MS,
      );

      const info: ContainerInfo = {
        containerId: containerId.slice(0, 12),
        containerName: name,
        projectId,
        kind,
        port,
        url: `http://127.0.0.1:${port}`,
        status: "running",
      };

      this.containers.set(key, { ...info, startedAt: Date.now() });

      logger.info(
        { projectId, kind, containerName: name, containerId: containerId.slice(0, 12), port },
        "Docker container started",
      );

      return info;
    } finally {
      // Clean up the temp directory
      await fs.rm(tmpDir, { recursive: true, force: true });
    }
  }

  /**
   * Stop and remove the container. Since we use --rm, docker stop is sufficient.
   */
  async stopContainer(projectId: string, kind: string): Promise<void> {
    const key = `${projectId}:${kind}`;
    const name = containerName(projectId, kind);

    this.containers.delete(key);

    try {
      await dockerCommand(
        ["stop", "--time", String(DOCKER_STOP_TIMEOUT_S), name],
        (DOCKER_STOP_TIMEOUT_S + 5) * 1000,
      );
      logger.info({ projectId, kind, containerName: name }, "Docker container stopped");
    } catch (err) {
      // Container may not exist — that's fine
      const msg = err instanceof Error ? err.message : String(err);
      if (!msg.includes("No such container")) {
        logger.warn(
          { projectId, kind, containerName: name, error: msg },
          "Failed to stop Docker container",
        );
      }
    }

    // Extra safety: force remove in case --rm didn't kick in
    try {
      await dockerCommand(["rm", "-f", name], 5_000);
    } catch {
      /* ignore */
    }
  }

  /**
   * Returns the URL where the container is accessible, or null if not running.
   */
  getContainerUrl(projectId: string, kind: string): string | null {
    const key = `${projectId}:${kind}`;
    const tracked = this.containers.get(key);
    if (!tracked || tracked.status !== "running") return null;
    return tracked.url;
  }

  /**
   * Get recent container logs (last 200 lines).
   */
  async getLogs(projectId: string, kind: string): Promise<string> {
    const name = containerName(projectId, kind);
    try {
      return await dockerCommand(
        ["logs", "--tail", "200", name],
        10_000,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(`Failed to get logs for ${name}: ${msg}`);
    }
  }

  /**
   * Execute a command in a running container and return its stdout.
   */
  async execCommand(
    projectId: string,
    kind: string,
    command: string,
  ): Promise<string> {
    const name = containerName(projectId, kind);
    try {
      return await dockerCommand(
        ["exec", name, "sh", "-c", command],
        EXEC_TIMEOUT_MS,
      );
    } catch (err) {
      const msg = err instanceof Error ? err.message : String(err);
      throw new Error(`Exec command failed in ${name}: ${msg}`);
    }
  }

  /**
   * Check if a container is tracked as running for the given project+kind.
   */
  isActive(projectId: string, kind: string): boolean {
    const key = `${projectId}:${kind}`;
    return this.containers.has(key);
  }

  /**
   * Return all tracked containers.
   */
  listActive(): ContainerInfo[] {
    return Array.from(this.containers.values()).map(({
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
export const dockerExecutor = new DockerExecutor();
