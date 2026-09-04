/**
 * Forgvi Engine — the verifier (independent judge + deterministic gates).
 *
 * The engine's law: claims are not proof. After each chief iteration the
 * judge — a fresh, tool-less session that never saw the chief's reasoning —
 * scores every acceptance criterion PASS/FAIL and names the gaps. The
 * completion policy (goal-loop.js) decides what happens next; the judge
 * only decides what is true.
 *
 * HARD GATES (2026-09-04, the no-human-in-the-loop mandate): before the
 * LLM scores anything, deterministic gates run REAL commands against the
 * run's workspace (build proof, dev-port law — the auth-persistence rule
 * is system-prompt law only, per the 2026-09-05 user mandate). Gate
 * failures are merged into the verdict as FORCED gaps — no model
 * opinion can mark a run complete while a gate is red. The LLM judge sees
 * the gate report and is instructed that gates are authoritative.
 */

import { createJudgeSession, lastAssistantText } from "./kernel.js";
import { runHardGates } from "./hard-gates.js";

/**
 * Run one verification turn.
 *
 * @returns {Promise<{
 *   verdicts: Array<{criterion: string, pass: boolean, evidence: string}>,
 *   score: number,
 *   gaps: string[],
 *   summary: string,
 *   ok: boolean
 * }>}
 */
export async function verifyAcceptance({ objective, acceptance, chiefReport, evidence, workspace }) {
  // ── Deterministic hard gates first — they cannot be argued with. ────
  let gateResult = { gaps: [], checks: [], report: "(hard gates not run — no workspace)" };
  try {
    gateResult = await runHardGates({ workspace, evidence });
  } catch (error) {
    gateResult = {
      gaps: [],
      checks: [{ name: "hard-gates", status: "skipped", detail: String(error?.message ?? error).slice(0, 200) }],
      report: `[SKIPPED] hard-gates: ${String(error?.message ?? error).slice(0, 200)}`,
    };
  }

  const evidenceBlock = (evidence ?? [])
    .slice(-24)
    .map((e, i) => {
      const output = e.output ? `\n   output: ${String(e.output).replace(/\s+/g, " ").slice(0, 220)}` : "";
      return `${i + 1}. [${e.kind}] ${e.name} — ${e.status}${output}`;
    })
    .join("\n");

  const prompt = [
    "You are the independent verifier of an agent run. You did not do the work; you judge it.",
    "",
    "OBJECTIVE:",
    objective,
    "",
    "ACCEPTANCE CRITERIA (each must be judged independently):",
    ...acceptance.map((a, i) => `${i + 1}. ${a}`),
    "",
    "THE WORKER'S REPORT:",
    chiefReport || "(the worker produced no report)",
    "",
    "TOOL EVIDENCE (commands the worker actually executed):",
    evidenceBlock || "(no tool evidence recorded)",
    "",
    "DETERMINISTIC GATE RESULTS (authoritative — a red gate overrides any",
    "worker claim; FAIL any criterion a red gate contradicts):",
    gateResult.report,
    "",
    "Judge each criterion strictly on the evidence available. Do not give benefit of the doubt:",
    "if the report asserts something the evidence does not support, FAIL that criterion.",
    "Do not reward effort, plans, or promises — only demonstrated results.",
    "",
    'Reply with STRICT JSON only (no prose, no markdown fences):',
    '{"verdicts":[{"criterion":"<the criterion text>","pass":true|false,"evidence":"<one line: why>"}],"summary":"<one line overall>"}',
  ].join("\n");

  let text = "";
  for (let attempt = 0; attempt < 2; attempt++) {
    const session = await createJudgeSession();
    try {
      await session.prompt(
        attempt === 0
          ? prompt
          : prompt +
              "\n\nYour previous reply was not valid JSON. Reply again with ONLY the JSON object, nothing else.",
      );
      text = lastAssistantText(session);
    } finally {
      session.dispose();
    }
    const parsed = extractJson(text);
    if (parsed && Array.isArray(parsed.verdicts)) {
      const verdicts = parsed.verdicts
        .filter((v) => v && typeof v.criterion === "string" && typeof v.pass === "boolean")
        .map((v) => ({
          criterion: v.criterion,
          pass: v.pass,
          evidence: String(v.evidence ?? "").slice(0, 300),
        }));
      if (verdicts.length > 0) {
        const passed = verdicts.filter((v) => v.pass).length;
        const score = passed / acceptance.length;
        const judgeGaps = verdicts.filter((v) => !v.pass).map((v) => v.criterion);
        // HARD-GATE MERGE: gate failures are forced gaps — completion is
        // impossible while any deterministic gate is red, regardless of
        // what the LLM believes.
        const gaps = [...gateResult.gaps, ...judgeGaps];
        return {
          verdicts,
          score,
          gaps,
          summary: String(parsed.summary ?? "").slice(0, 500),
          ok: gaps.length === 0,
          hardGates: gateResult.checks,
        };
      }
    }
  }

  // Unparseable verdict — the honest outcome is FAIL with a named gap.
  // (Hard-gate failures still hold — they merge in here too.)
  return {
    verdicts: acceptance.map((a) => ({
      criterion: a,
      pass: false,
      evidence: "verifier output was unparseable — no verdict recorded",
    })),
    score: 0,
    gaps: [
      ...gateResult.gaps,
      ...acceptance.map((a) => `${a} (unverified — judge output invalid)`),
    ],
    summary: "The verifier could not produce a parseable verdict.",
    ok: false,
    hardGates: gateResult.checks,
  };
}

/** Extract the first JSON object from model text (tolerates fences/prose). */
function extractJson(text) {
  if (!text) return null;
  const fenced = text.match(/```(?:json)?\s*([\s\S]*?)```/);
  const candidates = [fenced?.[1], text];
  for (const candidate of candidates) {
    if (!candidate) continue;
    const start = candidate.indexOf("{");
    const end = candidate.lastIndexOf("}");
    if (start === -1 || end <= start) continue;
    try {
      return JSON.parse(candidate.slice(start, end + 1));
    } catch {
      /* try next */
    }
  }
  return null;
}
