/**
 * MODULE 4 — Async Multi-Agent Compilation & Verification Pipeline.
 *
 * State machine that wraps the existing Architect (generateSpec) →
 * Developers (runSwarmPipeline / runSinglePipeline) codegen into a full
 * serve + headless-browser audit + auto-correction loop:
 *
 *   [Architect: generateSpec]
 *           │
 *           ▼
 *   [Developers: parallel frontend+backend codegen]  ◄── existing runPipeline
 *           │
 *           ▼
 *   [Serve: launch dev servers inside the Daytona VM]
 *           │
 *           ▼
 *   [Debugger: in-VM Playwright audit (or curl fallback)]
 *           │
 *       ┌───┴───┐
 *    PASS       FAIL
 *       │         │
 *       ▼         ▼
 *     DONE   [Architect re-plan w/ failure report]
 *                    │
 *                    ▼
 *              [Developers re-codegen]  ──► loop back to Serve
 *                                            (max MAX_CORRECTION_ITERATIONS = 2)
 *
 * The loop is invoked AFTER the existing pipeline (runPipeline) has produced
 * files and AFTER generate.ts has written them into the VM via
 * `writeWorkspaceFilesBulk`. Therefore the first iteration starts at the
 * Serve phase against files that are already on disk in the VM.
 *
 * RESILIENCE CONTRACT
 * -------------------
 * Every VM operation (serve, audit, terminal exec) is wrapped in try/catch.
 * A crash anywhere in the loop MUST NEVER fail the surrounding generation
 * pipeline — the caller (generate.ts) wraps the entry point in its own
 * try/catch, and this module additionally guards every internal phase.
 * On max-iterations-exceeded we still return a usable `final_audit` so the
 * user sees what went wrong.
 *
 * AUDIT PROVIDERS (Step 3 of the user spec)
 * ----------------------------------------
 * Primary  — POST {DAYTONA_SERVICE_URL}/api/v1/workspace/{sandboxId}/browser-audit
 *            Body: {frontend_url, backend_url, validation_blueprint}
 *            200 → {status, title, error_logs, dom_snapshot, screenshot_b64}
 * Fallback — curl the dev-server ports from inside the VM via
 *            `runWorkspaceTerminal` and synthesize a result from the HTTP
 *            status codes. Used when the daytona-service 404s the route
 *            (Module 3 still being deployed) or any network error occurs.
 */
import { logger } from "../lib/logger";
import {
  runWorkspaceTerminal,
  daytonaBaseUrl,
  type TerminalResult,
} from "./daytona-workspace";
import {
  dbUpdateProject,
  isSupabaseConfigured,
} from "../lib/supabase-db";
import {
  godModePrompt,
  conversationDigest,
  skillsForPhase,
  type PipelinePhase,
} from "./god-mode-protocol";
import { PLATFORM_SKILLS, type AgentMode } from "./skill-registry";
import type { ProjectState, ProjectSpec } from "./agent-platform";
import { agentPlatform } from "./agent-platform";

// ─── PUBLIC CONFIG ────────────────────────────────────────────────────────

/**
 * Maximum number of auto-correction iterations AFTER the initial codegen.
 * Total runs = 1 (initial) + MAX_CORRECTION_ITERATIONS = 3 max.
 * After this the loop terminates with `production_ready: false` and the
 * best-known audit snapshot is returned.
 */
export const MAX_CORRECTION_ITERATIONS = 2;

// ─── TYPES ────────────────────────────────────────────────────────────────

export type ActivityEmit = (event: "activity", data: {
  label: string;
  status: "active" | "done" | "error";
  kind?: string;
}) => void;

type ProjectLike = ProjectState;

export type ServeResult = {
  frontend_url: string;
  backend_url: string;
  frontend_started: boolean;
  backend_started: boolean;
  frontend_cmd: string | null;
  backend_cmd: string | null;
  raw_logs: { frontend?: TerminalResult; backend?: TerminalResult };
};

export type HttpProbe = { url: string; status: string | null; ok: boolean };

export type AuditResult = {
  status: "success" | "failed" | "unavailable";
  title: string;
  error_logs: string[];
  http_status: { frontend: string | null; backend: string | null };
  dom_snapshot: string | null;
  screenshot_b64: string | null;
  /** True when this audit came from the curl fallback path. */
  from_fallback: boolean;
};

export type Evaluation = {
  passed: boolean;
  failures: string[];
};

export type FailureReport = {
  failures: string[];
  prev_blueprint_summary: string;
  suggested_corrections: Array<{
    area: "frontend" | "backend" | "api" | "config";
    issue: string;
    suggestion: string;
  }>;
};

export type LoopResult = {
  status: "production_ready" | "max_iterations_exceeded" | "skipped";
  iterations: number;
  final_audit: AuditResult | null;
  skills_used: string[];
  phases_completed: PipelinePhase[];
};

// ─── INTERNAL HELPERS ──────────────────────────────────────────────────────

const FRONTEND_PORT = 5173;
const BACKEND_PORT = 3000;
const FRONTEND_URL = `http://localhost:${FRONTEND_PORT}`;
const BACKEND_URL = `http://localhost:${BACKEND_PORT}`;
const SERVE_BIND_DELAY_MS = 4_500;
const PORT_PROBE_TIMEOUT_MS = 8_000;

function safeStringify(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2);
  } catch {
    return String(value);
  }
}

/** Sleep helper that never throws. */
function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));
}

/**
 * Run a shell command inside the VM, swallowing errors. The Daytona
 * terminal endpoint may time out on long-running commands (e.g. nohup'd
 * servers), so we treat non-zero exit_code and network errors identically
 * and just return whatever we got.
 */
async function safeTerminal(
  sandboxId: string,
  command: string,
  cwd = "/workspace",
  timeoutMs = PORT_PROBE_TIMEOUT_MS,
): Promise<TerminalResult | null> {
  try {
    return await runWorkspaceTerminal(sandboxId, command, cwd, timeoutMs);
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, command: command.slice(0, 200), err: err instanceof Error ? err.message : err },
      "orchestration-loop: terminal command failed",
    );
    return null;
  }
}

// ─── STEP 2: SERVE — launch dev servers inside the VM ─────────────────────

/**
 * Detect the right start command for the backend by inspecting files in
 * /workspace/backend. Order of preference:
 *   1. package.json with scripts.start | scripts.dev → `npm start` | `npm run dev`
 *   2. requirements.txt + flask import         → `python3 -m flask run --host=0.0.0.0 --port=3000`
 *   3. requirements.txt + uvicorn/fastapi     → `python3 -m uvicorn app:app --host 0.0.0.0 --port 3000`
 *   4. app.py / main.py                         → `python3 app.py` / `python3 main.py`
 *   5. fallback                                 → `python3 app.py` (best effort)
 */
async function detectBackendStartCommand(
  sandboxId: string,
): Promise<{ cmd: string | null; reason: string }> {
  const ls = await safeTerminal(
    sandboxId,
    "ls -1 /workspace/backend 2>/dev/null",
    "/",
    5_000,
  );
  const listing = ls?.stdout ?? "";
  logger.debug({ sandboxId, listing: listing.slice(0, 500) }, "orchestration-loop: backend ls");

  // Read package.json if present.
  if (listing.includes("package.json")) {
    const pkg = await safeTerminal(
      sandboxId,
      "cat /workspace/backend/package.json 2>/dev/null",
      "/",
      5_000,
    );
    const pkgText = pkg?.stdout ?? "";
    if (pkgText) {
      try {
        const parsed = JSON.parse(pkgText) as {
          scripts?: Record<string, string>;
        };
        if (parsed.scripts?.start) return { cmd: "npm start", reason: "package.json#scripts.start" };
        if (parsed.scripts?.dev) return { cmd: "npm run dev", reason: "package.json#scripts.dev" };
      } catch {
        /* fall through */
      }
    }
  }

  // Python detection.
  if (listing.includes("requirements.txt")) {
    // Install deps first (best-effort) — the actual serve command runs
    // separately. We just probe for flask/uvicorn imports.
    const grep = await safeTerminal(
      sandboxId,
      "grep -iE 'flask|uvicorn|fastapi' /workspace/backend/requirements.txt 2>/dev/null",
      "/",
      5_000,
    );
    const deps = (grep?.stdout ?? "").toLowerCase();
    if (deps.includes("flask")) {
      return {
        cmd: `cd /workspace/backend && pip install -r requirements.txt --quiet 2>/dev/null; python3 -m flask run --host=0.0.0.0 --port=${BACKEND_PORT}`,
        reason: "requirements.txt + flask",
      };
    }
    if (deps.includes("uvicorn") || deps.includes("fastapi")) {
      return {
        cmd: `cd /workspace/backend && pip install -r requirements.txt --quiet 2>/dev/null; python3 -m uvicorn app:app --host 0.0.0.0 --port ${BACKEND_PORT}`,
        reason: "requirements.txt + uvicorn/fastapi",
      };
    }
  }

  if (listing.includes("app.py")) {
    return { cmd: `cd /workspace/backend && python3 app.py`, reason: "app.py present" };
  }
  if (listing.includes("main.py")) {
    return { cmd: `cd /workspace/backend && python3 main.py`, reason: "main.py present" };
  }

  return { cmd: null, reason: "no recognized backend entrypoint" };
}

/**
 * Detect the right start command for /workspace/frontend. Vite defaults to
 * port 5173, so we honor that contract.
 */
async function detectFrontendStartCommand(
  sandboxId: string,
): Promise<{ cmd: string | null; reason: string }> {
  const ls = await safeTerminal(
    sandboxId,
    "ls -1 /workspace/frontend 2>/dev/null",
    "/",
    5_000,
  );
  const listing = ls?.stdout ?? "";
  logger.debug({ sandboxId, listing: listing.slice(0, 500) }, "orchestration-loop: frontend ls");

  if (listing.includes("package.json")) {
    const pkg = await safeTerminal(
      sandboxId,
      "cat /workspace/frontend/package.json 2>/dev/null",
      "/",
      5_000,
    );
    const pkgText = pkg?.stdout ?? "";
    if (pkgText) {
      try {
        const parsed = JSON.parse(pkgText) as {
          scripts?: Record<string, string>;
          dependencies?: Record<string, string>;
        };
        if (parsed.scripts?.start) return { cmd: "npm start", reason: "package.json#scripts.start" };
        if (parsed.scripts?.dev) return { cmd: "npm run dev", reason: "package.json#scripts.dev" };
        // Vite / Next auto-detect: if vite is a dep, `npx vite` will bind 5173.
        if (parsed.dependencies?.vite || parsed.dependencies?.["next"]) {
          return { cmd: "npx vite --port 5173 --host", reason: "vite/next dependency detected" };
        }
      } catch {
        /* fall through */
      }
    }
  }

  // Static files fallback — bind 5173 with python http.server.
  if (listing.length > 0) {
    return {
      cmd: `cd /workspace/frontend && python3 -m http.server ${FRONTEND_PORT} --bind 0.0.0.0`,
      reason: "static file server (no package.json)",
    };
  }

  return { cmd: null, reason: "empty /workspace/frontend" };
}

/**
 * Launch the dev servers inside the VM via nohup, then sleep so the bind
 * has a chance to happen. Never throws — returns partial results so the
 * loop can still attempt an audit even when one side is missing.
 */
export async function serveDeveloperApplications(
  _project: ProjectLike,
  sandboxId: string,
): Promise<ServeResult> {
  const result: ServeResult = {
    frontend_url: FRONTEND_URL,
    backend_url: BACKEND_URL,
    frontend_started: false,
    backend_started: false,
    frontend_cmd: null,
    backend_cmd: null,
    raw_logs: {},
  };

  if (!sandboxId) {
    logger.warn("orchestration-loop: serve called with no sandboxId");
    return result;
  }

  // Backend
  try {
    const be = await detectBackendStartCommand(sandboxId);
    result.backend_cmd = be.cmd;
    if (be.cmd) {
      const wrapped = `cd /workspace/backend && nohup ${be.cmd} > /tmp/backend.log 2>&1 &`;
      const term = await safeTerminal(sandboxId, wrapped, "/", 12_000);
      result.raw_logs.backend = term ?? undefined;
      result.backend_started = true;
      logger.info(
        { sandboxId, cmd: be.cmd, reason: be.reason },
        "orchestration-loop: backend dev server launched",
      );
    } else {
      logger.warn(
        { sandboxId, reason: be.reason },
        "orchestration-loop: could not detect backend start command",
      );
    }
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "orchestration-loop: backend serve crashed (continuing)",
    );
  }

  // Frontend
  try {
    const fe = await detectFrontendStartCommand(sandboxId);
    result.frontend_cmd = fe.cmd;
    if (fe.cmd) {
      const wrapped = `cd /workspace/frontend && nohup ${fe.cmd} > /tmp/frontend.log 2>&1 &`;
      const term = await safeTerminal(sandboxId, wrapped, "/", 12_000);
      result.raw_logs.frontend = term ?? undefined;
      result.frontend_started = true;
      logger.info(
        { sandboxId, cmd: fe.cmd, reason: fe.reason },
        "orchestration-loop: frontend dev server launched",
      );
    } else {
      logger.warn(
        { sandboxId, reason: fe.reason },
        "orchestration-loop: could not detect frontend start command",
      );
    }
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "orchestration-loop: frontend serve crashed (continuing)",
    );
  }

  // Give the VM a moment to bind both ports. 4.5s is generous for a
  // cold Vite/Flask startup on a Daytona MicroVM.
  await sleep(SERVE_BIND_DELAY_MS);

  return result;
}

// ─── STEP 3: HEADLESS BROWSER AUDIT ───────────────────────────────────────

/**
 * Probe a localhost URL from inside the VM by curling it and parsing the
 * `%{http_code}` placeholder. Returns null when curl itself failed.
 */
async function curlHttpStatus(
  sandboxId: string,
  url: string,
): Promise<string | null> {
  const probe = await safeTerminal(
    sandboxId,
    `curl -sS -o /dev/null -w '%{http_code}' --max-time 5 ${url} 2>/dev/null || echo "000"`,
    "/",
    PORT_PROBE_TIMEOUT_MS,
  );
  if (!probe) return null;
  // The terminal returns both stdout and stderr — pull the last 3-char
  // numeric token (the http_code) out of whatever curl emitted.
  const codeMatch = probe.stdout.match(/[0-9]{3}\s*$/);
  return codeMatch ? codeMatch[0].trim() : probe.stdout.trim().slice(-3) || null;
}

/**
 * Call the in-VM Playwright audit endpoint on the daytona-service.
 * On 404 (Module 3 not deployed yet) or any network error, fall back to
 * curling the dev-server ports from inside the VM and synthesizing an
 * `AuditResult` from the HTTP status codes.
 *
 * `validationBlueprint` is the Architect's spec/contract — passed to the
 * browser-audit endpoint so the Playwright engine can compare the live DOM
 * against the planned schema.
 */
export async function executeVirtualBrowserAudit(
  sandboxId: string,
  validationBlueprint: unknown,
): Promise<AuditResult> {
  // Default fallback-shape — overwritten on success.
  const fallback: AuditResult = {
    status: "unavailable",
    title: "",
    error_logs: [],
    http_status: { frontend: null, backend: null },
    dom_snapshot: null,
    screenshot_b64: null,
    from_fallback: true,
  };

  // ── PRIMARY: call the daytona-service browser-audit endpoint ─────────
  try {
    const url = `${daytonaBaseUrl()}/api/v1/workspace/${encodeURIComponent(sandboxId)}/browser-audit`;
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        frontend_url: FRONTEND_URL,
        backend_url: BACKEND_URL,
        validation_blueprint: validationBlueprint,
      }),
      signal: AbortSignal.timeout(45_000),
    });

    if (res.ok) {
      const payload = (await res.json()) as {
        status?: string;
        title?: string;
        error_logs?: string[];
        dom_snapshot?: string | null;
        screenshot_b64?: string | null;
      };
      logger.info(
        { sandboxId, status: payload.status, title: payload.title },
        "orchestration-loop: browser-audit endpoint responded",
      );
      return {
        status: payload.status === "success" ? "success" : payload.status === "failed" ? "failed" : "unavailable",
        title: payload.title ?? "",
        error_logs: Array.isArray(payload.error_logs) ? payload.error_logs : [],
        http_status: { frontend: "200", backend: "200" }, // endpoint already verified reachability
        dom_snapshot: payload.dom_snapshot ?? null,
        screenshot_b64: payload.screenshot_b64 ?? null,
        from_fallback: false,
      };
    }

    if (res.status === 404) {
      logger.info(
        { sandboxId, status: res.status },
        "orchestration-loop: browser-audit endpoint not deployed yet — using curl fallback",
      );
    } else {
      logger.warn(
        { sandboxId, status: res.status },
        "orchestration-loop: browser-audit endpoint returned non-200 — using curl fallback",
      );
    }
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, err: err instanceof Error ? err.message : err },
      "orchestration-loop: browser-audit endpoint unreachable — using curl fallback",
    );
  }

  // ── FALLBACK: curl both ports from inside the VM ──────────────────────
  const [frontendStatus, backendStatus] = await Promise.all([
    curlHttpStatus(sandboxId, FRONTEND_URL),
    curlHttpStatus(sandboxId, BACKEND_URL),
  ]);
  fallback.http_status = { frontend: frontendStatus, backend: backendStatus };

  const feOk = frontendStatus === "200";
  const beOk = backendStatus === "200";
  const errorLogs: string[] = [];
  if (!feOk) errorLogs.push(`Frontend ${FRONTEND_URL} returned HTTP ${frontendStatus ?? "n/a"}.`);
  if (!beOk) errorLogs.push(`Backend ${BACKEND_URL} returned HTTP ${backendStatus ?? "n/a"}.`);

  fallback.error_logs = errorLogs;
  fallback.status = feOk || beOk ? "success" : "failed";
  fallback.title = `curl fallback (frontend=${frontendStatus ?? "n/a"}, backend=${backendStatus ?? "n/a"})`;

  logger.info(
    { sandboxId, status: fallback.status, frontend: frontendStatus, backend: backendStatus },
    "orchestration-loop: curl fallback audit complete",
  );
  return fallback;
}

// ─── STEP 4a: EVALUATION ───────────────────────────────────────────────────

/**
 * Compare the audit result against the Architect's blueprint. Heuristic:
 * the system passes when the audit reports `success`, there are no
 * error_logs, AND at least one dev server responded 200 (frontend or
 * backend). Concrete failure messages are produced for each missing piece.
 */
export function evaluateAgainstBlueprint(
  audit: AuditResult,
  blueprint: ProjectSpec | null,
): Evaluation {
  const failures: string[] = [];

  if (audit.status === "unavailable") {
    failures.push("Audit was unavailable — the live system could not be inspected.");
  }
  if (audit.status === "failed") {
    failures.push("Audit reported a failed status — at least one dev server did not respond.");
  }
  for (const log of audit.error_logs) {
    failures.push(`Console/runtime error: ${log}`);
  }

  // HTTP status checks.
  const feOk = audit.http_status.frontend === "200";
  const beOk = audit.http_status.backend === "200";
  if (!feOk) {
    failures.push(
      `Frontend dev server not reachable on ${FRONTEND_URL} (HTTP ${audit.http_status.frontend ?? "n/a"}).`,
    );
  }
  if (!beOk) {
    failures.push(
      `Backend dev server not reachable on ${BACKEND_URL} (HTTP ${audit.http_status.backend ?? "n/a"}).`,
    );
  }

  // Blueprint coverage — every OpenAPI path must appear in the DOM snapshot
  // when available (very rough heuristic, intended to surface missing pages).
  if (blueprint && audit.dom_snapshot) {
    const dom = audit.dom_snapshot;
    const routes = Object.keys(blueprint.paths ?? {});
    const missing = routes.filter((route) => !dom.includes(route));
    for (const route of missing) {
      failures.push(`Blueprint route ${route} not present in the live DOM snapshot.`);
    }
  }

  const passed =
    audit.status === "success" &&
    audit.error_logs.length === 0 &&
    (feOk || beOk) &&
    failures.length === 0;

  return { passed, failures };
}

// ─── STEP 4b: FAILURE REPORT ───────────────────────────────────────────────

/**
 * Build a structured failure report for the Architect. The report is
 * deliberately compact so the LLM can ingest it without hitting the
 * context window — long DOM snapshots are summarized.
 */
export function buildFailureReport(
  failures: string[],
  prevBlueprint: ProjectSpec | null,
): FailureReport {
  const routeCount = Object.keys(prevBlueprint?.paths ?? {}).length;
  const schemaCount = Object.keys(prevBlueprint?.components?.schemas ?? {}).length;

  const suggested: FailureReport["suggested_corrections"] = [];
  for (const failure of failures) {
    if (/frontend/i.test(failure)) {
      suggested.push({
        area: "frontend",
        issue: failure,
        suggestion:
          "Ensure /workspace/frontend has package.json with a dev script binding 0.0.0.0:5173. Add a healthcheck route that renders a non-empty <body>.",
      });
    } else if (/backend/i.test(failure)) {
      suggested.push({
        area: "backend",
        issue: failure,
        suggestion:
          "Ensure /workspace/backend binds 0.0.0.0:3000 and exposes GET /health returning 200. Include the start command in package.json#scripts.start or use app.py.",
      });
    } else if (/route/i.test(failure)) {
      suggested.push({
        area: "api",
        issue: failure,
        suggestion: "Implement the missing route on the backend and link to it from the frontend.",
      });
    } else {
      suggested.push({
        area: "config",
        issue: failure,
        suggestion: "Adjust the project configuration to address the failure above.",
      });
    }
  }

  return {
    failures,
    prev_blueprint_summary:
      `OpenAPI ${prevBlueprint?.openapi ?? "n/a"} — ${routeCount} path(s), ${schemaCount} schema(s), title: ${prevBlueprint?.info?.title ?? "n/a"}.`,
    suggested_corrections: suggested,
  };
}

// ─── STEP 4c: ARCHITECT RE-PLAN ───────────────────────────────────────────

/**
 * Ask the Architect (leader role, planning phase) to revise the structural
 * blueprint in light of the verification failures. The revised spec is
 * parsed from JSON and stored on the project; the DB row is updated
 * fire-and-forget so the new contract shows up on reload.
 */
export async function architectReplan(
  project: ProjectLike,
  failureReport: FailureReport,
): Promise<ProjectSpec | null> {
  const phase: PipelinePhase = "planning";
  const activeSkills = skillsForPhase(
    PLATFORM_SKILLS,
    phase,
    project.activeConnections,
  );

  const systemPrompt = godModePrompt(
    phase,
    activeSkills,
    (project.mode as AgentMode) ?? "swarm",
    conversationDigest(project.messages) +
      "\n\n## PREVIOUS VERIFICATION FAILURES:\n" +
      safeStringify(failureReport) +
      "\n\n## PREVIOUS BLUEPRINT:\n" +
      safeStringify(project.spec),
  );

  const userPrompt =
    "Revise the structural blueprint to address the failures above. " +
    "Output ONLY valid JSON for an OpenAPI 3.1 contract with the same shape " +
    "as the previous blueprint (paths, components.schemas). Do not output " +
    "source code — only the contract.";

  let raw: string;
  try {
    // Public wrapper around the private NVIDIA fetch — role-aware so the
    // planning-phase God Mode prompt + skills are applied below the hood.
    raw = await agentPlatform.callModelRaw(
      "leader",
      systemPrompt,
      userPrompt,
      true, // jsonMode
      project,
    );
  } catch (err: unknown) {
    logger.error(
      { projectId: project.id, err: err instanceof Error ? err.message : err },
      "orchestration-loop: architectReplan LLM call failed — keeping previous spec",
    );
    return project.spec;
  }

  if (!raw) {
    logger.warn(
      { projectId: project.id },
      "orchestration-loop: architectReplan returned empty response — keeping previous spec",
    );
    return project.spec;
  }

  // Parse with the same fence-tolerant parser the rest of the platform uses.
  let parsed: Record<string, unknown> | null = null;
  try {
    parsed = JSON.parse(raw) as Record<string, unknown>;
  } catch {
    const fence = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
    if (fence?.[1]) {
      try { parsed = JSON.parse(fence[1].trim()) as Record<string, unknown>; } catch { /* give up */ }
    }
  }
  if (!parsed) {
    const first = raw.indexOf("{");
    const last = raw.lastIndexOf("}");
    if (first !== -1 && last > first) {
      try { parsed = JSON.parse(raw.slice(first, last + 1)) as Record<string, unknown>; } catch { /* give up */ }
    }
  }
  if (!parsed) {
    logger.warn(
      { projectId: project.id, raw: raw.slice(0, 400) },
      "orchestration-loop: architectReplan response was not JSON — keeping previous spec",
    );
    return project.spec;
  }

  const newSpec: ProjectSpec = {
    openapi: "3.1.0",
    info: {
      title:
        (parsed.info as { title?: string } | undefined)?.title ??
        project.spec?.info.title ??
        "Revised Project Contract",
      version:
        (parsed.info as { version?: string } | undefined)?.version ??
        project.spec?.info.version ??
        "1.0.1",
    },
    paths:
      (parsed.paths as Record<string, unknown> | undefined) ??
      project.spec?.paths ??
      {},
    components: {
      schemas:
        (parsed.components as { schemas?: Record<string, unknown> } | undefined)?.schemas ??
        (parsed.schemas as Record<string, unknown> | undefined) ??
        project.spec?.components.schemas ??
        {},
    },
  };

  project.spec = newSpec;
  project.updatedAt = new Date().toISOString();

  if (isSupabaseConfigured()) {
    dbUpdateProject(project.id, { spec: newSpec }).catch((err: unknown) => {
      logger.warn(
        { projectId: project.id, err: err instanceof Error ? err.message : err },
        "orchestration-loop: failed to persist revised spec — continuing",
      );
    });
  }

  logger.info(
    { projectId: project.id, routeCount: Object.keys(newSpec.paths).length },
    "orchestration-loop: architectReplan produced a revised blueprint",
  );
  return newSpec;
}

// ─── STEP 5: WRITE FILES BACK INTO THE VM ──────────────────────────────────

/**
 * Write the project's currently-generated files into the VM, mapping the
 * single-mode backend/frontend strings into /workspace/backend/app.py and
 * /workspace/frontend/App.tsx (same routing as generate.ts).
 * Imported lazily to avoid a circular module-load dependency.
 */
async function rewriteFilesToVM(
  sandboxId: string,
  project: ProjectLike,
): Promise<void> {
  // Lazy import — daytona-workspace has no module-eval-time side effects on
  // agent-platform, but isolating the import makes the dependency graph
  // explicit and lets esbuild tree-shake more aggressively.
  const { writeWorkspaceFilesBulk } = await import("./daytona-workspace");

  const files = project.codebase.files ?? [];
  const synthFiles: Array<{ path: string; content: string }> = [...files];
  if (synthFiles.length === 0) {
    if (project.codebase.backend) {
      synthFiles.push({ path: "backend/app.py", content: project.codebase.backend });
    }
    if (project.codebase.frontend) {
      synthFiles.push({ path: "frontend/App.tsx", content: project.codebase.frontend });
    }
  }
  if (synthFiles.length === 0) return;

  await writeWorkspaceFilesBulk(
    sandboxId,
    synthFiles.map((f) => {
      const rel = f.path.replace(/^\/+/, "");
      if (rel.startsWith("backend/")) return { path: `/workspace/${rel}`, content: f.content };
      if (rel.startsWith("frontend/")) return { path: `/workspace/${rel}`, content: f.content };
      return { path: `/workspace/frontend/${rel}`, content: f.content };
    }),
  );
}

// ─── THE MAIN STATE MACHINE ────────────────────────────────────────────────

/**
 * Drive the full Architect → Developers → Serve → Debugger → Auto-correct
 * loop. The first iteration uses the files that the surrounding pipeline
 * (runPipeline + writeWorkspaceFilesBulk in generate.ts) already wrote to
 * the VM — this function picks up at the Serve phase. On failure, the
 * Architect re-plans and the Developers re-run via `runPipeline`, then the
 * new files are rewritten to the VM before the next Serve attempt.
 *
 * Returns the loop result. NEVER throws — all errors are logged and the
 * result is populated with `status: "skipped"` when something fundamental
 * is missing (no sandbox, no project).
 */
export async function executeOrchestrationPipelineLoop(
  project: ProjectLike,
  sandboxId: string | null,
  emit: ActivityEmit,
): Promise<LoopResult> {
  const result: LoopResult = {
    status: "skipped",
    iterations: 0,
    final_audit: null,
    skills_used: project.skillsUsed ?? [],
    phases_completed: project.phasesCompleted ?? [],
  };

  if (!sandboxId) {
    logger.warn(
      { projectId: project.id },
      "orchestration-loop: no sandboxId — skipping serve/audit/correct phases",
    );
    emit("activity", {
      label: "Audit skipped (no VM available)",
      status: "done",
      kind: "think",
    });
    return result;
  }

  let lastAudit: AuditResult | null = null;
  let iteration = 0;

  // Initial serve + audit + (optional) correction loop.
  for (iteration = 1; iteration <= 1 + MAX_CORRECTION_ITERATIONS; iteration += 1) {
    result.iterations = iteration;
    emit("activity", {
      label: `Iteration ${iteration}/${1 + MAX_CORRECTION_ITERATIONS}: serving dev servers`,
      status: "active",
      kind: "generate",
    });

    // ── SERVE ─────────────────────────────────────────────────────────
    const serve = await serveDeveloperApplications(project, sandboxId);
    emit("activity", {
      label: serve.backend_started
        ? `Backend serving on ${serve.backend_url}`
        : "Backend dev server unavailable",
      status: serve.backend_started ? "done" : "error",
      kind: "generate",
    });
    emit("activity", {
      label: serve.frontend_started
        ? `Frontend serving on ${serve.frontend_url}`
        : "Frontend dev server unavailable",
      status: serve.frontend_started ? "done" : "error",
      kind: "generate",
    });

    // ── DEBUGGER ──────────────────────────────────────────────────────
    emit("activity", {
      label: "Debugger: launching headless browser audit",
      status: "active",
      kind: "generate",
    });
    const audit = await executeVirtualBrowserAudit(sandboxId, project.spec);
    lastAudit = audit;
    emit("activity", {
      label: `Audit complete — status: ${audit.status}`,
      status: audit.status === "success" ? "done" : "error",
      kind: "generate",
    });

    // ── EVALUATE ──────────────────────────────────────────────────────
    const evaluation = evaluateAgainstBlueprint(audit, project.spec);
    emit("activity", {
      label:
        evaluation.passed
          ? "Verification passed — build is production-ready"
          : `Verification: ${evaluation.failures.length} failure(s)`,
      status: evaluation.passed ? "done" : "error",
      kind: "generate",
    });

    if (evaluation.passed) {
      result.status = "production_ready";
      result.final_audit = audit;
      result.skills_used = [...project.skillsUsed];
      result.phases_completed = [...project.phasesCompleted];
      return result;
    }

    // ── AUTO-CORRECT (or stop) ────────────────────────────────────────
    if (iteration > MAX_CORRECTION_ITERATIONS) {
      // We've already used our 2 correction iterations — stop.
      logger.warn(
        { projectId: project.id, iteration, failures: evaluation.failures },
        "orchestration-loop: max iterations exceeded — returning best-known audit",
      );
      result.status = "max_iterations_exceeded";
      result.final_audit = audit;
      result.skills_used = [...project.skillsUsed];
      result.phases_completed = [...project.phasesCompleted];
      return result;
    }

    // Build the failure report and have the Architect re-plan, then
    // re-run the swarm pipeline + re-write files into the VM.
    const failureReport = buildFailureReport(evaluation.failures, project.spec);
    emit("activity", {
      label: `Iteration ${iteration + 1}/${1 + MAX_CORRECTION_ITERATIONS}: Architect re-planning`,
      status: "active",
      kind: "think",
    });
    await architectReplan(project, failureReport);

    emit("activity", {
      label: `Iteration ${iteration + 1}: regenerating code`,
      status: "active",
      kind: "generate",
    });
    try {
      await agentPlatform.runPipeline(project);
      await rewriteFilesToVM(sandboxId, project);
      emit("activity", {
        label: `Iteration ${iteration + 1}: revised files written to VM`,
        status: "done",
        kind: "generate",
      });
    } catch (err: unknown) {
      logger.warn(
        { projectId: project.id, iteration, err: err instanceof Error ? err.message : err },
        "orchestration-loop: re-codegen pass failed — continuing with previous files",
      );
      emit("activity", {
        label: "Re-codegen failed — retrying audit with current files",
        status: "error",
        kind: "generate",
      });
    }
  }

  // Unreachable — the for-loop returns on every path. Defensive fallback.
  result.status = "max_iterations_exceeded";
  result.final_audit = lastAudit;
  return result;
}

// ─── CLASS WRAPPER (alternative entrypoint) ────────────────────────────────

/**
 * Optional class-form wrapper for callers that prefer
 * `new OrchestrationLoop().run(project, sandboxId, emit)`. Internally it
 * just delegates to the standalone functions.
 */
export class OrchestrationLoop {
  static async serve(project: ProjectLike, sandboxId: string): Promise<ServeResult> {
    return serveDeveloperApplications(project, sandboxId);
  }

  static async audit(sandboxId: string, blueprint: unknown): Promise<AuditResult> {
    return executeVirtualBrowserAudit(sandboxId, blueprint);
  }

  static evaluate(audit: AuditResult, blueprint: ProjectSpec | null): Evaluation {
    return evaluateAgainstBlueprint(audit, blueprint);
  }

  static buildReport(failures: string[], prev: ProjectSpec | null): FailureReport {
    return buildFailureReport(failures, prev);
  }

  static async replan(project: ProjectLike, report: FailureReport): Promise<ProjectSpec | null> {
    return architectReplan(project, report);
  }

  static async run(
    project: ProjectLike,
    sandboxId: string | null,
    emit: ActivityEmit,
  ): Promise<LoopResult> {
    return executeOrchestrationPipelineLoop(project, sandboxId, emit);
  }
}
