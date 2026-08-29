/**
 * MODULE 2 (Part B) — Automated Feedback Test Runner.
 *
 * After every code-write block, the platform issues an automated
 * internal test event command to the VM. The guest's stdout/stderr
 * and HTTP network status flow back into the parent platform DB to
 * be fed into the LLM's next prompt context window — this is the
 * closed-loop feedback that lets the AI see its own runtime errors.
 *
 * Strategy (in priority order, NEVER throws — always returns a
 * TestResult):
 *   1. `package.json` with `scripts.test`     → `npm test --silent`
 *      in /workspace/frontend or /workspace/backend
 *   2. A dev server port is implied by file content
 *      (Vite=5173, Next=3000, Express=3000, Flask=5000)
 *      AND the port is bound                    → curl health-check
 *   3. Python entrypoint (`app.py` / `main.py`) → `python3 -m py_compile`
 *      (syntax-check fallback, no deps needed)
 *   4. JS/TS entrypoint                         → `node --check`
 *   5. Otherwise                                → skip with reason
 *
 * Integration point: routes/generate.ts invokes this AFTER
 * `writeWorkspaceFilesBulk` succeeds and BEFORE the `sandbox` SSE
 * event. The result is emitted as a `test` SSE event and persisted
 * as a `post_write_test` generation row (diagnostics column) so the
 * next LLM round can read its own runtime errors.
 */
import { logger } from "../lib/logger";
import { runWorkspaceTerminal, type TerminalResult } from "./daytona-workspace";
import { probePort } from "./preview-proxy";

export type TestResult = {
  /** True iff we actually issued a test command. */
  ran: boolean;
  /** The actual command run (for LLM transparency). Empty when ran=false. */
  command: string;
  /** Null on timeout / crash. 0..N otherwise. */
  exit_code: number | null;
  stdout: string;
  stderr: string;
  /** HTTP status from a curl health-check. Null when not a health-check. */
  http_status: number | null;
  duration_ms: number;
  /** Present only when ran=false. */
  skipped_reason?: string;
};

const TEST_TIMEOUT_MS = 30_000;

// Dev-server port conventions. Flask defaults to 5000; Vite to 5173;
// Express / Next to 3000.
const PORT_VITE = 5173;
const PORT_NEXT = 3000;
const PORT_EXPRESS = 3000;
const PORT_FLASK = 5000;

type WrittenFile = { path: string; content: string };

/**
 * Decide + run the appropriate test for a project that just had files
 * written. See file-level docstring for the strategy.
 */
export async function runPostWriteTest(
  sandboxId: string,
  files: WrittenFile[],
): Promise<TestResult> {
  if (!sandboxId) {
    return skip("no sandboxId supplied");
  }
  if (!files || files.length === 0) {
    return skip("no files written — nothing to test");
  }

  // ── Strategy 1: package.json scripts.test ──────────────────────────
  const npmTest = detectNpmTest(files);
  if (npmTest) {
    return await runVmCommand(
      sandboxId,
      `npm test --silent`,
      npmTest.cwd,
      /* http */ false,
    );
  }

  // ── Strategy 2: dev-server port health-check ────────────────────────
  const portCandidate = detectDevServerPort(files);
  if (portCandidate) {
    const alive = await probePort(sandboxId, portCandidate.port).catch(
      () => false,
    );
    if (alive) {
      // %http_code → "200" on a real response, "000" on connect failure
      // (already gated by probePort, so 000 is unexpected but handled).
      const cmd = `curl -sS -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:${portCandidate.port}/`;
      const result = await runVmCommand(
        sandboxId,
        cmd,
        "/workspace",
        /* http */ true,
      );
      // The stdout field carries the 3-digit code; promote it into
      // http_status so the LLM context window sees the network outcome
      // explicitly rather than buried in stdout.
      if (result.ran) {
        const codeStr = (result.stdout || "").trim();
        const code = parseInt(codeStr, 10);
        if (!Number.isNaN(code) && code > 0) {
          result.http_status = code;
        }
      }
      return result;
    }
    // Port not alive yet — fall through to syntax checks. The dev
    // server probably hasn't been started (Module 4's
    // serveDeveloperApplications runs AFTER us).
  }

  // ── Strategy 3: Python syntax check ────────────────────────────────
  const pyEntry = detectPythonEntry(files);
  if (pyEntry) {
    return await runVmCommand(
      sandboxId,
      `python3 -m py_compile ${pyEntry.relPath}`,
      pyEntry.cwd,
      /* http */ false,
    );
  }

  // ── Strategy 4: JS/TS syntax check ─────────────────────────────────
  const jsEntry = detectJsEntry(files);
  if (jsEntry) {
    return await runVmCommand(
      sandboxId,
      `node --check ${jsEntry.relPath}`,
      jsEntry.cwd,
      /* http */ false,
    );
  }

  // ── Strategy 5: skip ────────────────────────────────────────────────
  return skip("no recognizable test target in the written files");
}

// ─── DETECTION HELPERS ──────────────────────────────────────────────

/** Locate a package.json among the written files & inspect its scripts. */
function detectNpmTest(
  files: WrittenFile[],
): { cwd: string } | null {
  for (const f of files) {
    const name = basename(f.path);
    if (name !== "package.json") continue;
    const cwd = dirOfWorkspacePath(f.path);
    // Look for "test" key in scripts. Permissive: substring match on
    // "scripts" then a "test" key.
    if (f.content && hasTestScript(f.content)) {
      return { cwd };
    }
  }
  return null;
}

/** True when the package.json content declares a non-empty scripts.test. */
function hasTestScript(pkgContent: string): boolean {
  try {
    const parsed = JSON.parse(pkgContent) as {
      scripts?: Record<string, string>;
    };
    const test = parsed?.scripts?.test;
    return typeof test === "string" && test.trim().length > 0;
  } catch {
    // Not valid JSON — fall back to a regex heuristic.
    return /"scripts"\s*:\s*\{[\s\S]*?"test"\s*:\s*"[^"]+"/.test(pkgContent);
  }
}

/**
 * Return the workspace-relative dir for a written path. generate.ts
 * routes `backend/**` → /workspace/backend/** and `frontend/**` →
 * /workspace/frontend/**. Anything else (swarm-mode raw paths) lands
 * under /workspace/frontend, so we mirror that here.
 */
function dirOfWorkspacePath(filePath: string): string {
  const rel = (filePath || "").replace(/^\/+/, "");
  if (rel.startsWith("backend/")) return "/workspace/backend";
  if (rel.startsWith("frontend/")) return "/workspace/frontend";
  return "/workspace/frontend";
}

/** Pick the right dev-server port from file content. */
function detectDevServerPort(
  files: WrittenFile[],
): { port: number; reason: string } | null {
  for (const f of files) {
    const name = basename(f.path);
    const rel = (f.path || "").replace(/^\/+/, "");
    const isFrontend = rel.startsWith("frontend/") || name === "vite.config.ts" || name === "vite.config.js" || name === "App.tsx" || name === "next.config.js" || name === "next.config.ts";
    const isBackend = rel.startsWith("backend/") || name === "app.py" || name === "main.py" || name === "server.js" || name === "index.js" || name === "package.json";

    // Vite presence → 5173
    if (name.startsWith("vite.config") || f.content?.includes("vite")) {
      return { port: PORT_VITE, reason: "vite detected" };
    }
    // Next.js config → 3000
    if (name.startsWith("next.config")) {
      return { port: PORT_NEXT, reason: "next.js detected" };
    }
    // Flask backend in app.py
    if (name === "app.py" && /flask|Flask/.test(f.content || "")) {
      return { port: PORT_FLASK, reason: "flask detected in app.py" };
    }
    // Express backend
    if (
      isBackend &&
      /express\s*\(|require\s*\(\s*['"]express['"]\s*\)|from\s+['"]express['"]/.test(
        f.content || "",
      )
    ) {
      return { port: PORT_EXPRESS, reason: "express detected" };
    }
    // Default frontend marker (App.tsx without vite.config) → 5173
    if (isFrontend && name === "App.tsx") {
      return { port: PORT_VITE, reason: "frontend App.tsx present (Vite default)" };
    }
  }
  return null;
}

/** Locate a Python entrypoint among written files. */
function detectPythonEntry(
  files: WrittenFile[],
): { relPath: string; cwd: string } | null {
  for (const f of files) {
    const name = basename(f.path);
    if (name === "app.py" || name === "main.py") {
      const cwd = dirOfWorkspacePath(f.path);
      // relPath relative to cwd
      const rel = (f.path || "").replace(/^\/+/, "");
      const tail = rel.startsWith("backend/")
        ? rel.slice("backend/".length)
        : rel.startsWith("frontend/")
          ? rel.slice("frontend/".length)
          : rel;
      return { relPath: tail || name, cwd };
    }
  }
  return null;
}

/** Locate a JS/TS entrypoint among written files. */
function detectJsEntry(
  files: WrittenFile[],
): { relPath: string; cwd: string } | null {
  // Prefer the main server file or page entry, fall back to App.tsx.
  const preferred = ["server.ts", "server.js", "index.ts", "index.js", "App.tsx"];
  for (const target of preferred) {
    for (const f of files) {
      if (basename(f.path) === target) {
        const cwd = dirOfWorkspacePath(f.path);
        const rel = (f.path || "").replace(/^\/+/, "");
        const tail = rel.startsWith("backend/")
          ? rel.slice("backend/".length)
          : rel.startsWith("frontend/")
            ? rel.slice("frontend/".length)
            : rel;
        return { relPath: tail || target, cwd };
      }
    }
  }
  return null;
}

// ─── EXECUTION ──────────────────────────────────────────────────────

/**
 * Run a single VM command and map to a TestResult. NEVER throws.
 *
 * Timeout handling: when the terminal endpoint times out (terminal
 * `timed_out` flag set OR the fetch rejects), we still return a
 * TestResult with ran=true, exit_code=null, stderr="timeout".
 */
async function runVmCommand(
  sandboxId: string,
  command: string,
  cwd: string,
  _isHttp: boolean,
): Promise<TestResult> {
  const startedAt = Date.now();
  let terminal: TerminalResult | null = null;
  try {
    terminal = await runWorkspaceTerminal(
      sandboxId,
      command,
      cwd,
      TEST_TIMEOUT_MS,
    );
  } catch (err: unknown) {
    const duration_ms = Date.now() - startedAt;
    const msg = err instanceof Error ? err.message : String(err);
    const isTimeout =
      /timeout|timed\s*out|abort/i.test(msg) || /timeout/i.test(msg);
    if (isTimeout) {
      logger.warn(
        { sandboxId, command: command.slice(0, 200), duration_ms },
        "feedback-test-runner: command timed out",
      );
      return {
        ran: true,
        command,
        exit_code: null,
        stdout: "",
        stderr: "timeout",
        http_status: null,
        duration_ms,
      };
    }
    logger.warn(
      { sandboxId, command: command.slice(0, 200), err: msg },
      "feedback-test-runner: terminal RPC threw",
    );
    return {
      ran: true,
      command,
      exit_code: null,
      stdout: "",
      stderr: msg,
      http_status: null,
      duration_ms,
    };
  }

  const duration_ms =
    terminal?.duration_ms ?? Date.now() - startedAt;

  if (!terminal) {
    return {
      ran: true,
      command,
      exit_code: null,
      stdout: "",
      stderr: "no terminal response",
      http_status: null,
      duration_ms,
    };
  }

  // Terminal-level timeout flag from the daytona-service.
  if (terminal.timed_out) {
    return {
      ran: true,
      command,
      exit_code: null,
      stdout: terminal.stdout ?? "",
      stderr: (terminal.stderr ?? "") + " (timeout)" || "timeout",
      http_status: null,
      duration_ms,
    };
  }

  return {
    ran: true,
    command,
    exit_code: typeof terminal.exit_code === "number" ? terminal.exit_code : null,
    stdout: terminal.stdout ?? "",
    stderr: terminal.stderr ?? "",
    http_status: null,
    duration_ms,
  };
}

// ─── UTILITIES ──────────────────────────────────────────────────────

/** Construct a skipped TestResult (ran=false) with a reason. */
function skip(reason: string): TestResult {
  return {
    ran: false,
    command: "",
    exit_code: null,
    stdout: "",
    stderr: "",
    http_status: null,
    duration_ms: 0,
    skipped_reason: reason,
  };
}

/** Return the basename of a workspace-style path (`backend/app.py` → `app.py`). */
function basename(p: string): string {
  const parts = (p || "").split(/[\\/]+/).filter(Boolean);
  return parts[parts.length - 1] ?? "";
}
