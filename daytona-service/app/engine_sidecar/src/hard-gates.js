/**
 * Forgvi Engine — deterministic hard gates (the no-human-in-the-loop fix).
 *
 * WHY THIS EXISTS (user mandate 2026-09-04): a live 2.0 run shipped an app
 * with a syntax error, and a HUMAN had to fix it. The LLM judge scores
 * text; it cannot see the filesystem and it takes the chief's tool
 * evidence on faith. These gates are DETERMINISTIC: they run real
 * commands against the run's real workspace, they cannot be talked out
 * of, and their verdicts are merged into the judge's result as forced
 * failures — the run CANNOT complete while a gate is red.
 *
 * 2026-09-05 (user mandate): the sqlite/localStorage AUTH-persistence rule
 * is SYSTEM PROMPT LAW ONLY (goal-loop.js lawBlock clause 1 + the .prime
 * personas) — it is NOT machine-enforced here, and the database choice is
 * the USER's (the agent asks which database they want). The gates below
 * enforce only the code-quality laws the user asked to be fixed:
 *
 *   G1 build-proof     — when a Next.js app exists in the workspace, the
 *                        tool evidence MUST contain a PASSING production
 *                        build or typecheck (`next build` / `tsc --noEmit` /
 *                        `npm run build`). A dev server is NOT build proof.
 *   G2 dev-port-law    — no `next dev <digits>` (a bare number is parsed by
 *                        Next.js as a DIRECTORY, not a port) in scripts,
 *                        READMEs or the recorded evidence. The port must be
 *                        passed with -p/--port.
 *
 * Execution backends, exactly like the chief's tools:
 *   - host VM-bound runs: commands through the daytona-service REST API
 *   - in-VM runs: native shell in this VM
 *   - local/unbound runs: native shell in the run directory
 *   - no workspace at all: every gate degrades to "skipped" (honest no-op)
 *
 * Every gate is best-effort-safe: an unreachable workspace marks the gate
 * "skipped" (never fabricates a pass or a fail), and a scan that errors is
 * reported as skipped with the reason. Only POSITIVE findings fail a run.
 */

import { execFile } from "node:child_process";
import { ENGINE_IN_VM } from "./kernel.js";

/** Max ms for one gate command (scans are cheap; builds never run here). */
const GATE_TIMEOUT_MS = Number(process.env.HARD_GATE_TIMEOUT_MS ?? 45_000);

/** Where a local (non-VM) workspace run's files live. */
function workspaceRoot(workspace) {
  if (workspace?.vm) return workspace.vm.workspaceRoot;
  if (workspace?.localCwd) return workspace.localCwd;
  return null;
}

/**
 * Run one shell command in the run's workspace. Returns
 * { ok, exitCode, stdout, stderr } or { ok: false, skipped: reason }.
 */
async function runInWorkspace(workspace, command, timeoutMs = GATE_TIMEOUT_MS) {
  if (workspace?.vm) {
    try {
      const result = await workspace.vm.client.exec(command, {
        cwd: workspace.vm.workspaceRoot,
        timeoutMs,
      });
      return { ok: true, ...result };
    } catch (error) {
      return { ok: false, skipped: `workspace unreachable: ${String(error?.message ?? error).slice(0, 200)}` };
    }
  }
  const root = workspaceRoot(workspace);
  if (!root) return { ok: false, skipped: "no workspace bound to this run" };
  return new Promise((resolve) => {
    execFile(
      "bash",
      ["-lc", command],
      { cwd: root, timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024 },
      (error, stdout = "", stderr = "") => {
        if (error && error.code === undefined && error.killed) {
          resolve({ ok: false, skipped: `gate command timed out after ${timeoutMs}ms` });
          return;
        }
        resolve({
          ok: true,
          exitCode: error ? Number(error.code ?? -1) : 0,
          stdout: String(stdout),
          stderr: String(stderr),
        });
      },
    );
  });
}

/** Build-patterns that count as production build/typecheck proof. */
const BUILD_PROOF = [
  /\bnext\s+build\b/,
  /\btsc\s+--noEmit\b/,
  /\bnpm\s+run\s+build\b/,
  /\bnpm\s+run\s+--workspace[^\s]*\s+build\b/,
  /\bnpm\s+--prefix[^\s]*\s+run\s+build\b/,
  /\bbun\s+run\s+build\b/,
  /\bpnpm\s+(?:run\s+)?build\b/,
  /\bnpm\s+run\s+typecheck\b/,
  /\bnpm\s+run\s+--workspace[^\s]*\s+typecheck\b/,
];

/**
 * Does the tool evidence contain a PASSING build/typecheck?
 *
 * MATCHES THE COMMAND NAME ONLY — never the output. (Bug fixed 2026-09-04,
 * live-observed on run c92327d2: a `cat package.json` command's OUTPUT
 * contains the text "next build" from the scripts section, and the gate
 * wrongly credited it as build proof. Only an EXECUTED build counts: the
 * evidence name is the command line the chief actually ran.)
 */
function hasBuildProof(evidence) {
  for (const entry of evidence ?? []) {
    if (entry?.status !== "pass" && entry?.status !== true) continue;
    const name = String(entry?.name ?? "");
    if (BUILD_PROOF.some((re) => re.test(name))) {
      return name;
    }
  }
  return null;
}

/** Does the evidence contain a bare `next dev <digits>` invocation?
 * (Also name-only — same rationale as hasBuildProof.) */
function hasBarePortDevCommand(evidence) {
  for (const entry of evidence ?? []) {
    const name = String(entry?.name ?? "");
    const match = name.match(/next\s+dev\s+(\d{2,5})(?!\S)/);
    if (match) return match[0];
  }
  return null;
}

/**
 * Run every hard gate. `workspace` = { vm, localCwd } (the chief's own
 * backends); `evidence` = the run's tool-evidence ledger.
 *
 * @returns {Promise<{
 *   gaps: string[],               // forced-failure gap descriptions (chief must fix)
 *   checks: Array<{name: string, status: 'pass'|'fail'|'skipped', detail: string}>,
 *   report: string,               // compact text block for the judge prompt
 * }>}
 */
export async function runHardGates({ workspace, evidence }) {
  const checks = [];
  const gaps = [];

  const record = (name, status, detail) => {
    checks.push({ name, status, detail: String(detail).slice(0, 600) });
    return status;
  };

  // ── G1: production build proof (Next.js app present?) ──────────────
  const nextCheck = await runInWorkspace(
    workspace,
    `grep -l '"next"' package.json apps/*/package.json packages/*/package.json 2>/dev/null | head -n 1 || true`,
  );
  const hasNext = nextCheck.ok && String(nextCheck.stdout ?? "").trim() !== "";
  if (!nextCheck.ok) {
    record("build-proof", "skipped", nextCheck.skipped);
  } else if (!hasNext) {
    record("build-proof", "pass", "no Next.js app in the workspace (gate not applicable)");
  } else {
    const proof = hasBuildProof(evidence);
    if (proof) {
      record("build-proof", "pass", `build proof recorded: ${proof}`);
    } else {
      record("build-proof", "fail", "a Next.js app exists but NO passing `next build` / `tsc --noEmit` / `npm run build` appears in the tool evidence — a dev server is not build proof");
      gaps.push(
        "BUILD PROOF MISSING: a Next.js app exists in the workspace but the tool evidence contains no passing production build or typecheck. Run `npm run build` (or `npx tsc --noEmit`) in the app directory until it exits 0, fix every syntax/type/SSR error it surfaces, and re-run it. The run cannot complete without this.",
      );
    }
  }

  // ── G2: dev-server port law ──────────────────────────────────────────
  const bare = hasBarePortDevCommand(evidence);
  if (bare) {
    record("dev-port-law", "fail", `bare-port dev command recorded: \`${bare}\``);
    gaps.push(
      `DEV PORT LAW VIOLATION: \`${bare}\` was used. Next.js parses a bare number as a DIRECTORY, not a port. Restart the dev server as \`npx next dev -p 3000\` (or \`npm run dev -- -p 3000\`) and verify it with curl.`,
    );
  } else {
    record("dev-port-law", "pass", "no bare-port `next dev` command in the evidence");
  }

  const report = checks
    .map((c) => `[${c.status.toUpperCase()}] ${c.name}: ${c.detail.split("\n")[0].slice(0, 200)}`)
    .join("\n");

  return { gaps, checks, report };
}

/** True when the engine itself runs in-VM (exported for probes). */
export const hardGatesInVm = ENGINE_IN_VM;
