/**
 * Forgvi Engine — VM workspace operations.
 *
 * The piece that makes the VM and the engine ONE workspace: prime-agent's
 * bash and edit tools are given custom `operations` that execute against the
 * project's Daytona sandbox through the daytona-service REST API — the same
 * server-side API the Forge backend uses. Nothing is built on the engine's
 * local disk anymore when a run is VM-bound:
 *
 *   - chief bash commands run INSIDE the VM (cwd = /workspace)
 *   - chief file edits write INTO the VM (mkdir -p + upload)
 *   - chief file reads come FROM the VM
 *
 * Because Forgvi 1.0's agent swarm ALSO operates on the same /workspace
 * filesystem (inside the VM), the two engines can never drift apart: a user
 * switching 1.0 → 2.0 or 2.0 → 1.0 sees the exact same files either way, and
 * the studio's Files/Preview tabs (which read the VM) reflect 2.0 work live.
 *
 * Isolation: every client is constructed per-run with the run's own
 * sandboxId (from a verified workspace grant) — one project, one sandbox,
 * never shared.
 */

const DEFAULT_DAYTONA_URL =
  process.env.DAYTONA_SERVICE_URL ?? "https://arcforge-daytona.onrender.com";
const WORKSPACE_ROOT = process.env.ENGINE_VM_WORKSPACE_ROOT ?? "/workspace";

// The daytona-service clamps exec timeouts to [1s, 300s]. We stay just under.
const DAYTONA_MAX_TIMEOUT_MS = 290_000;
// Render free-tier cold start: the first request after idle can take ~60s.
// Every request gets the command's own timeout + this slack to wake + reply.
const COLD_START_SLACK_MS = 90_000;
// Transient failures (wake, 502/503/504, network blips) are retried.
const RETRY_STATUS = new Set([502, 503, 504]);
const RETRY_DELAYS_MS = [4_000, 12_000];

/** Sleep helper. */
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * VmWorkspaceClient — thin REST client for one sandbox on the
 * daytona-service. Retries transient failures (service wake) so a cold
 * daytona-service doesn't fail a run's first command.
 */
export class VmWorkspaceClient {
  constructor({ sandboxId, baseUrl = DEFAULT_DAYTONA_URL, fetchImpl = globalThis.fetch }) {
    this.sandboxId = sandboxId;
    this.baseUrl = String(baseUrl).replace(/\/$/, "");
    this.fetch = fetchImpl;
    this.wakeAttempted = false;
  }

  /** Fetch with cold-start retries. `timeoutMs` covers ALL attempts. */
  async #request(path, { method = "GET", body, timeoutMs = 120_000 } = {}) {
    const deadline = Date.now() + timeoutMs;
    let lastError = null;
    for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
      const remaining = deadline - Date.now();
      if (remaining <= 0) throw new Error(`daytona-service timeout: ${path}`);
      const signal = AbortSignal.timeout(remaining);
      try {
        const res = await this.fetch(`${this.baseUrl}${path}`, {
          method,
          headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
          body: body !== undefined ? JSON.stringify(body) : undefined,
          signal,
        });
        if (RETRY_STATUS.has(res.status) && attempt < RETRY_DELAYS_MS.length) {
          lastError = new Error(`daytona-service ${res.status} on ${path}`);
          await sleep(RETRY_DELAYS_MS[attempt]);
          continue;
        }
        if (res.status === 204) return null;
        const text = await res.text();
        if (!res.ok) {
          const err = new Error(`daytona-service ${res.status} on ${path}: ${text.slice(0, 300)}`);
          err.status = res.status;
          throw err;
        }
        return text ? JSON.parse(text) : null;
      } catch (error) {
        if (error?.name === "TimeoutError" || error?.name === "AbortError") throw error;
        lastError = error;
        if (attempt < RETRY_DELAYS_MS.length) {
          await sleep(RETRY_DELAYS_MS[attempt]);
          continue;
        }
      }
    }
    throw lastError ?? new Error(`daytona-service request failed: ${path}`);
  }

  /** Execute a shell command in the VM. Returns CodeRunResult-shaped data. */
  async exec(command, { cwd = WORKSPACE_ROOT, timeoutMs = 30_000 } = {}) {
    const clamped = Math.min(Math.max(Math.round(timeoutMs), 1_000), DAYTONA_MAX_TIMEOUT_MS);
    const data = await this.#request(
      `/api/v1/sandboxes/${encodeURIComponent(this.sandboxId)}/exec`,
      {
        method: "POST",
        body: { command, cwd, timeout_ms: clamped },
        timeoutMs: clamped + COLD_START_SLACK_MS + RETRY_DELAYS_MS.length * 16_000,
      },
    );
    return {
      exitCode: Number(data?.exit_code ?? -1),
      stdout: String(data?.stdout ?? ""),
      stderr: String(data?.stderr ?? ""),
      timedOut: Boolean(data?.timed_out),
      durationMs: Number(data?.duration_ms ?? 0),
    };
  }

  /** Read a file as utf-8 text. Throws with .status=404 when missing. */
  async readFile(path) {
    const data = await this.#request(
      `/api/v1/sandboxes/${encodeURIComponent(this.sandboxId)}/files/read?path=${encodeURIComponent(path)}`,
      { timeoutMs: 60_000 },
    );
    return String(data?.content ?? "");
  }

  /** Stat a file. Throws with .status=404 when missing. */
  async statFile(path) {
    return this.#request(
      `/api/v1/sandboxes/${encodeURIComponent(this.sandboxId)}/files/info?path=${encodeURIComponent(path)}`,
      { timeoutMs: 60_000 },
    );
  }

  /** mkdir -p the parent, then upload the file content. */
  async writeFile(path, content) {
    const slash = path.lastIndexOf("/");
    const parent = slash > 0 ? path.slice(0, slash) : null;
    if (parent) {
      // Idempotent; failures here surface as write errors next.
      try {
        await this.exec(`mkdir -p ${JSON.stringify(parent)}`, { timeoutMs: 15_000 });
      } catch {
        /* upload will report the real error if the dir is truly unreachable */
      }
    }
    await this.#request(
      `/api/v1/sandboxes/${encodeURIComponent(this.sandboxId)}/files/upload-bulk`,
      {
        method: "POST",
        body: { files: [{ path, content: String(content) }] },
        timeoutMs: 60_000,
      },
    );
  }

  /** Best-effort reachability probe (also warms the service). */
  async ping() {
    await this.#request("/health", { timeoutMs: 90_000 });
    return true;
  }
}

/**
 * prime-agent BashOperations over the VM — the chief's shell commands run
 * inside the sandbox, not on the engine host. Interface contract (from
 * prime-agent core/tools/bash.d.ts):
 *
 *   exec(command, cwd, { onData, signal, timeout }) → { exitCode }
 *
 * `timeout` is seconds; onData streams combined output chunks (Buffer).
 */
export function createVmBashOperations(client, { workspaceRoot = WORKSPACE_ROOT } = {}) {
  return {
    exec: async (command, cwd, options = {}) => {
      const timeoutSec = Number(options.timeout ?? 120);
      const timeoutMs = Number.isFinite(timeoutSec) && timeoutSec > 0 ? timeoutSec * 1000 : 120_000;
      const vmCwd = cwd && cwd.startsWith("/") ? cwd : workspaceRoot;
      const run = client.exec(command, { cwd: vmCwd, timeoutMs });
      let result;
      if (options.signal) {
        // Abort support: the remote command cannot be killed (Daytona has
        // no cancel API), but the turn can stop waiting — exitCode null =
        // killed. IMPORTANT: the non-abort winner still goes through the
        // shared output path below (prime-agent always passes a signal, so
        // this is the NORMAL path, not an edge case).
        const aborted = new Promise((resolve) => {
          options.signal.addEventListener(
            "abort",
            () => resolve({ __aborted: true }),
            { once: true },
          );
        });
        const winner = await Promise.race([run, aborted]);
        if (winner?.__aborted) {
          run.catch(() => {}); // detach — the VM-side command finishes on its own
          return { exitCode: null };
        }
        result = winner;
      } else {
        result = await run;
      }
      if (result.timedOut) {
        options.onData?.(Buffer.from(
          `${result.stdout}${result.stderr ? "\n" + result.stderr : ""}\n[forgvi] command timed out after ${Math.round(timeoutMs / 1000)}s\n`,
        ));
        return { exitCode: 124 };
      }
      const out = result.stdout + (result.stderr ? (result.stdout ? "\n" : "") + result.stderr : "");
      if (out) options.onData?.(Buffer.from(out));
      return { exitCode: normalizeExit(result.exitCode) };
    },
  };
}

/** Daytona reports -1 for internal failures; keep it a plain non-zero. */
function normalizeExit(code) {
  const n = Number(code);
  return Number.isFinite(n) ? n : -1;
}

/**
 * prime-agent EditOperations over the VM — reads come from the sandbox,
 * writes go into it. Interface contract (core/tools/edit.d.ts):
 *
 *   readFile(absolutePath) → Buffer
 *   writeFile(absolutePath, content) → void
 *   access(absolutePath) → void (throws when not readable/writable)
 */
export function createVmEditOperations(client) {
  return {
    readFile: async (absolutePath) => {
      const text = await client.readFile(absolutePath);
      return Buffer.from(text, "utf-8");
    },
    writeFile: async (absolutePath, content) => {
      await client.writeFile(absolutePath, String(content));
    },
    access: async (absolutePath) => {
      try {
        await client.statFile(absolutePath);
      } catch (error) {
        if (error?.status === 404) {
          throw new Error(`ENOENT: no such file, access ${absolutePath}`);
        }
        // Unreachable VM is still "accessible" as far as the edit tool is
        // concerned — the write will surface the real error if there is one.
      }
    },
  };
}

/** Resolve a VM-bound client + operations bundle for a run. */
export function createVmWorkspace({ sandboxId, baseUrl }) {
  const client = new VmWorkspaceClient({ sandboxId, baseUrl });
  return {
    sandboxId,
    client,
    workspaceRoot: WORKSPACE_ROOT,
    bashOperations: createVmBashOperations(client),
    editOperations: createVmEditOperations(client),
  };
}
