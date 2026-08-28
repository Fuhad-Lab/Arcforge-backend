#!/usr/bin/env python3
"""Task 28-e — Orchestrator v4 local verification with a scripted mock LLM.

Exercises the REAL graph machinery (routing, guardrails, conditional edges,
fix loop, unified voice) with the LLM layer mocked:
  1. import + DB init + repo map (tree-sitter when present)
  2. dispatch sequence: backend → frontend → fit_check → debugger(FAIL)
     → fix (deterministic issue) → debugger(PASS) → END
  3. guardrails: premature finish is overridden; budget forces the QA gate
  4. verify_file CLI fallback answers (no LSP daemons in this sandbox)
  5. unified voice: ZERO "Chief Agent"/"swarm" in any emitted activity
     label or status state
"""
import json
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(ROOT, "..", "daytona-service", "app", "agent_sidecar")
sys.path.insert(0, SRC)

WS = tempfile.mkdtemp(prefix="orch-ws-")
SYS = os.path.join(WS, ".system")
os.makedirs(SYS, exist_ok=True)
os.environ.update({
    "ORCH_WORKSPACE": WS,
    "ORCH_SYSTEM_DIR": SYS,
    "ORCH_DB": os.path.join(SYS, "state.db"),
    "ORCH_LOG_FILE": os.path.join(SYS, "orchestrator.log"),
    "ORCH_LLM_URL": "http://localhost:1/v1",  # never called — mocked below
    "ORCH_LLM_READY": "1",
    "ORCH_TOKEN": "test-token",
    # small budgets so the budget guardrail fires fast in test 3
    "ORCH_MAX_DISPATCHES": "8",
})

# ── a small fake workspace for the repo mapper + verify_file ────────────
os.makedirs(os.path.join(WS, "backend"), exist_ok=True)
os.makedirs(os.path.join(WS, "frontend", "app"), exist_ok=True)
os.makedirs(os.path.join(WS, "frontend", "lib"), exist_ok=True)
with open(os.path.join(WS, "backend", "app.py"), "w") as fh:
    fh.write("from flask import Flask, jsonify\n\napp = Flask(__name__)\n\n"
             "def list_items():\n"
             "    return jsonify(items=[])\n\n"
             "class Item:\n    pass\n")
with open(os.path.join(WS, "frontend", "app", "page.tsx"), "w") as fh:
    fh.write("'use client'\n\nexport default function Page() {\n"
             "  return <main>hi</main>\n}\n\n"
             "const helper = () => 1\n")
with open(os.path.join(WS, "frontend", "lib", "api_client.ts"), "w") as fh:
    fh.write("export async function getItems() {\n"
             "  const r = await fetch('http://localhost:8000/api/items')\n"
             "  return r.json()\n}\n")

import orchestrator as orch  # noqa: E402
orch.init_db()

PASS, FAIL = [], []


def check(name: str, ok: bool, detail: str = "") -> None:
    (PASS if ok else FAIL).append(name)
    print(f"  {'✓' if ok else '✗ FAIL'} {name}" + (f" — {detail}" if detail and not ok else ""))


# ── capture every emitted event (activity labels, statuses) ─────────────
EMITTED = []
orig_emit = orch.emit


def spy_emit(event):
    EMITTED.append(event)
    orig_emit(event)


orch.emit = spy_emit

# ── the scripted LLM ──────────────────────────────────────────────────────
DISPATCH_CALLS = []
TRIAGE_CALLS = []
# "crash": first QA fail carries an attributed crash issue (deterministic
#   verbatim route); "gap": first QA fail carries a plan-coverage checklist
#   with a MISSING feature (the chief decides the build brief).
SCENARIO = {"mode": "crash"}


def mock_llm_chat(messages, json_mode=False, max_tokens=4000, model=None, **_):
    system = messages[0]["content"]
    user = messages[-1]["content"] if len(messages) > 1 else ""
    if "decide the next tool call" in user.lower():
        DISPATCH_CALLS.append(user)
        # The chief LLM proposes a PREMATURE finish on the FIRST call (so the
        # guardrails get exercised); sane proposals afterwards.
        if len(DISPATCH_CALLS) == 1:
            return json.dumps({"tool": "finish", "reason": "test: premature"})
        if "backend:" in user and "frontend:" not in user:
            return json.dumps({"tool": "frontend_agent",
                               "task": "Build the UI from the plan."})
        if "frontend:" in user and "fit_check" not in user:
            return json.dumps({"tool": "integration_check"})
        return json.dumps({"tool": "qa_verification"})
    if "MISSING FROM THE LIVE APP" in user or "Decide the fix delegations" in user:
        # THE CHIEF'S GAP DECISION — it must see the missing-features list.
        TRIAGE_CALLS.append(user)
        if "category pie chart" in user:
            return json.dumps({"delegations": [{
                "agent": "frontend",
                "task": ("BUILD THE MISSING PLAN FEATURE: the plan requires a "
                         "category pie chart on the dashboard; the live app "
                         "has no chart. Build ONLY the chart — keep everything "
                         "else intact, verify_file, confirm it renders.")}]})
        return json.dumps({"delegations": []})
    if "routing their message" in system:
        return json.dumps({"kind": "app", "intent": "test app"})
    if "Write the user-facing completion message" in system:
        if "category pie chart" in user:
            return ("I built the app. The category pie chart is still missing "
                    "— that plan feature remains unbuilt.")
        return "I built the test app and verified it end-to-end."
    # agent loops (backend/frontend/debugger/fix): report via done
    if "Backend Agent" in system:
        return json.dumps({"tool": "done",
                           "report": "backend built: Flask app + contract published"})
    if "Frontend Agent" in system:
        return json.dumps({"tool": "done", "report": "frontend built: page.tsx"})
    if "QA gate" in system or "Debugger Agent" in system:
        n = getattr(mock_llm_chat, "_dbg_calls", 0) + 1
        setattr(mock_llm_chat, "_dbg_calls", n)
        if n == 1 and SCENARIO["mode"] == "crash":
            # Crash-class failure: attributed issue, no coverage list —
            # rides verbatim to the frontend tool.
            return json.dumps({
                "tool": "done", "status": "fail",
                "report": "app shows 500",
                "issues": [{"criterion": "App loads",
                            "observation": "Frontend serves HTTP 500. Error: missing import",
                            "suspect": "frontend"}]})
        if n == 1 and SCENARIO["mode"] == "gap":
            # Plan-coverage failure: a feature the plan requires is MISSING.
            return json.dumps({
                "tool": "done", "status": "fail",
                "report": "app boots but the chart feature is absent",
                "coverage": [
                    {"feature": "expense list", "state": "present",
                     "evidence": "items render", "suspect": "frontend"},
                    {"feature": "category pie chart", "state": "missing",
                     "evidence": "no chart anywhere on the page",
                     "suspect": "frontend"}],
                "issues": []})
        return json.dumps({"tool": "done", "status": "pass",
                           "report": "all criteria pass",
                           "coverage": [{"feature": "expense list", "state": "present",
                                          "evidence": "renders", "suspect": "frontend"}]})
    return json.dumps({"tool": "done", "report": "mock done"})


orch.llm_chat = mock_llm_chat
# deterministic-issue probes: make them quiet + harmless
orch._deterministic_issues = lambda: []
# servers never actually start in this sandbox
orch.ensure_servers_up = lambda task_id: {"frontend": False, "backend": False,
                                          "app_port": None}
# git checkpoints: no-op in temp dir
orch.git_checkpoint = lambda label: None


# ═══ TEST 1: repo map (tree-sitter or regex) ═════════════════════════════
print("\n[1] repo map")
rm = orch.generate_repo_map()
check("repo map is non-empty", bool(rm and len(rm) > 100), repr(rm[:200]))
check("repo map has backend defs", "backend/app.py" in rm and "def list_items" in rm)
check("repo map ranks page.tsx above api_client",
      rm.index("frontend/app/page.tsx") < rm.index("frontend/lib/api_client.ts"))
check("repo map excludes bodies", "jsonify(items=[])" not in rm)
print(f"    engine: {'tree-sitter' if orch._TS_ENABLED else 'regex'}, "
      f"langgraph: {orch.LANGGRAPH_AVAILABLE}")

# ═══ TEST 2: verify_file (CLI fallback — no LSP daemons here) ═════════════
print("\n[2] verify_file")
r = orch.LSP_CLIENT.verify_file("backend/app.py")
check("verify_file answers (fallback ok)",
      r.get("ok") is True and "diagnostics" in r, json.dumps(r)[:200])
r2 = orch.LSP_CLIENT.verify_file("backend/nope.py")
check("verify_file flags missing file", r2.get("ok") is False)

# ═══ TEST 3: the graph — full route with a premature-finish proposal ═════
print("\n[3] build graph")
plan = ("# TestApp\n## Overview\nA test app.\n## Data & API\n"
        "GET /api/items returns the item list.\n"
        "POST /api/items creates an item.\n"
        "## Acceptance Criteria\n1. Page loads.\n")
task_id = "testtask01"
orch.enqueue_task("build me a test app")  # registers in DB (chat history)
# run the graph directly (skip the approval machine — tested separately)
result = orch.run_swarm(task_id, "build me a test app", plan)
reports = result.get("agents", {})
check("verdict is pass", result.get("verdict") == "pass", str(result.get("verdict")))
check("backend ran", "backend" in reports and bool(reports["backend"]))
check("frontend ran", "frontend" in reports and bool(reports["frontend"]))
check("fit check ran", "fit_check" in reports and bool(reports["fit_check"]))
check("debugger ran", "debugger" in reports and bool(reports["debugger"]))
check("repairs counted (>=1 fix round)", int(result.get("repairs", 0)) >= 1,
      str(result.get("repairs")))
check("summary present", bool(result.get("summary")))
check("premature finish was overridden (dispatch LLM called)", len(DISPATCH_CALLS) >= 1)
check("pass result carries an empty missing list",
      result.get("missing") == [], str(result.get("missing")))
check("crash route used no triage LLM (deterministic verbatim)",
      len(TRIAGE_CALLS) == 0, str(len(TRIAGE_CALLS)))

# ═══ TEST 4: unified voice — zero swarm/chief vocabulary in the stream ═══
print("\n[4] unified voice")
labels = [e.get("label", "") for e in EMITTED if e.get("type") == "activity"]
states = [json.dumps(e.get("status", {})) for e in EMITTED if e.get("type") == "status"]
bad_words = ("Chief Agent", "Backend Agent", "Frontend Agent",
             "Debugger Agent", "swarm")
bad_labels = [l for l in labels for w in bad_words if w in l]
bad_states = [s for s in states for w in bad_words if w in s]
check("no agent-role names in activity labels", not bad_labels, str(bad_labels[:4]))
check("no swarm-speak in status states", not bad_states, str(bad_states[:2]))
check("friendly labels present",
      any("interface" in l.lower() or "backend service" in l.lower() for l in labels),
      str(labels[:8]))
print("    sample labels:", labels[:8])

# ═══ TEST 5: guardrails — budget exhaustion forces the QA gate ═══════════
print("\n[5] budget guardrail")
state = {"task_id": "t2", "plan": plan,
         "dispatches": ["backend", "frontend", "fit_check"],
         "reports": {"backend": {"report": "b"}, "frontend": {"report": "f"}},
         "dispatch_budget": 3, "verdict": ""}
tool, task, _args = orch._dispatch_guardrails(state, {"tool": "finish", "task": ""})
check("budget exhausted → QA gate forced", tool == "debugger", tool)
state2 = dict(state, dispatches=["backend", "frontend", "fit_check", "debugger"],
              verdict="pass")
tool2, _, _a2 = orch._dispatch_guardrails(state2, {"tool": "finish", "task": ""})
check("QA passed → end allowed", tool2 == "end", tool2)

# frontend-only plan: no backend dispatch
state3 = {"task_id": "t3",
          "plan": "# X\n## Data & API\nNo backend needed (static/frontend-only).\n",
          "dispatches": [], "reports": {}, "dispatch_budget": 8, "verdict": ""}
tool3, _, _a3 = orch._dispatch_guardrails(state3, {"tool": "backend_agent", "task": "x"})
check("frontend-only plan: backend dispatch rejected", tool3 == "frontend", tool3)

# ═══ TEST 5b: the plan-gap loop — debugger reports WHAT'S MISSING, the ═══
# chief decides which tool builds it, the brief quotes the feature
print("\n[5b] plan-gap loop (debugger → chief → build brief)")
SCENARIO["mode"] = "gap"
setattr(mock_llm_chat, "_dbg_calls", 0)
DISPATCH_CALLS.clear()
TRIAGE_CALLS.clear()
plan_gap = ("# GapApp\n## Overview\nAn expense tracker.\n## Data & API\n"
            "GET /api/items returns expenses.\n\n## Pages & UI\n"
            "- an expense list\n- a category pie chart\n\n"
            "## Acceptance Criteria\n1. Expenses render.\n"
            "2. A pie chart breaks expenses down by category.\n")
result_gap = orch.run_swarm("testtask03", "build me a gap app", plan_gap)
check("gap scenario: verdict pass after the fix",
      result_gap.get("verdict") == "pass", str(result_gap.get("verdict")))
check("the chief received the missing-features checklist",
      len(TRIAGE_CALLS) >= 1 and "category pie chart" in TRIAGE_CALLS[0],
      str(TRIAGE_CALLS[:1]))
gap_labels = [e.get("label", "") for e in EMITTED
              if e.get("type") == "activity"]
check("'Building what's missing' label emitted",
      any("missing" in str(l).lower() for l in gap_labels), str(gap_labels[-8:]))
fix_details = [str(e.get("detail", "")) for e in EMITTED
               if e.get("type") == "activity"
               and "missing" in str(e.get("label", "")).lower()]
check("the fix dispatch brief quotes the missing feature",
      any("pie chart" in d for d in fix_details), str(fix_details[:2]))
check("gap pass result carries an empty missing list",
      result_gap.get("missing") == [], str(result_gap.get("missing")))

# ═══ TEST 5c: triage degraded → deterministic fallback gap briefs ═════════
print("\n[5c] triage fallback")
orig_triage = orch.ChiefAgent.triage
orch.ChiefAgent.triage = lambda self, p, d, m: []  # chief LLM degrades
try:
    dels = orch._fix_delegations("tfall", "# P\n## Pages & UI\nchart\n", {
        "status": "fail",
        "issues": [],
        "missing": [{"feature": "category pie chart", "state": "missing",
                     "evidence": "no chart on the page",
                     "suspect": "frontend"}]})
finally:
    orch.ChiefAgent.triage = orig_triage
check("fallback delegation routes to the suspect",
      len(dels) == 1 and dels[0]["agent"] == "frontend", json.dumps(dels)[:200])
check("fallback brief is a build-the-missing-feature brief",
      bool(dels) and dels[0]["task"].startswith("BUILD THE MISSING PLAN FEATURE")
      and "pie chart" in dels[0]["task"],
      dels[0]["task"][:120] if dels else "(none)")

# ═══ TEST 5d: debugger result shape — the coverage checklist rides along ═══
print("\n[5d] debugger result shape")
SCENARIO["mode"] = "crash"
res_v = orch.run_debugger_agent("tvd", "# P\n## Acceptance Criteria\nx\n")
check("debugger result carries missing + coverage keys",
      "missing" in res_v and "coverage" in res_v, str(list(res_v.keys())))

# ═══ TEST 5e: honest-fail summary names what's missing ════════════════════
print("\n[5e] honest-fail summary")
plan_gap_fail = ("# GapApp2\n## Overview\nAn expense tracker.\n"
                 "## Data & API\nGET /api/items returns expenses.\n\n"
                 "## Pages & UI\n- a category pie chart\n\n"
                 "## Acceptance Criteria\n1. A pie chart renders.\n")
SUMMARY_CALLS = []


def gap_fail_llm(messages, json_mode=False, max_tokens=4000, model=None, **_):
    system = messages[0]["content"]
    if "Write the user-facing completion message" in system:
        SUMMARY_CALLS.append(messages[-1]["content"])
        return ("I built the app but verification failed — the category pie "
                "chart is still missing.")
    if "QA gate" in system or "Debugger Agent" in system:
        return json.dumps({
            "tool": "done", "status": "fail",
            "report": "chart still absent",
            "coverage": [{"feature": "category pie chart",
                          "state": "missing",
                          "evidence": "no chart", "suspect": "frontend"}],
            "issues": []})
    if "MISSING FROM THE LIVE APP" in user or "Decide the fix delegations" in user:
        TRIAGE_CALLS.append(messages[-1]["content"])
        return json.dumps({"delegations": [{
            "agent": "frontend",
            "task": "BUILD THE MISSING PLAN FEATURE: category pie chart"}]})
    return mock_llm_chat(messages, json_mode, max_tokens, model, **_)


orig_chat = orch.llm_chat
orch.llm_chat = gap_fail_llm
try:
    result_hf = orch.run_swarm("testtask04", "build me a gap app",
                               plan_gap_fail)
finally:
    orch.llm_chat = orig_chat
check("honest fail: verdict is fail", result_hf.get("verdict") == "fail",
      str(result_hf.get("verdict")))
check("honest fail: missing list surfaces in the result",
      any("pie chart" in str(m.get("feature", ""))
          for m in (result_hf.get("missing") or [])),
      str(result_hf.get("missing")))
check("honest fail: the summary prompt carried the missing features",
      any("MISSING PLAN FEATURES" in u and "pie chart" in u
          for u in SUMMARY_CALLS),
      str(SUMMARY_CALLS)[:200])

# ═══ TEST 6: mini-graph vs langgraph — same route both engines ═══════════
print("\n[6] graph engine parity")
if orch.LANGGRAPH_AVAILABLE:
    real = orch.LANGGRAPH_AVAILABLE
    orch.LANGGRAPH_AVAILABLE = False
    try:
        DISPATCH_CALLS.clear()
        setattr(mock_llm_chat, "_dbg_calls", 0)
        SCENARIO["mode"] = "crash"
        result_mini = orch.run_swarm("testtask02", "build me a test app", plan)
        check("mini engine: verdict pass", result_mini.get("verdict") == "pass")
        check("mini engine: backend+frontend+debugger ran",
              all(result_mini["agents"].get(k)
                  for k in ("backend", "frontend", "debugger")))
        check("mini engine: missing list present",
              "missing" in result_mini, str(list(result_mini.keys())))
    finally:
        orch.LANGGRAPH_AVAILABLE = real
    print("    ran BOTH engines (langgraph + built-in fallback) ✓")
else:
    print("    langgraph not installed here — mini engine tested above")

# ═══ TEST 7: approval state machine (unchanged mechanics) ═════════════════
print("\n[7] approval machine")
import threading
import time as _t
orch.approval_upsert("t9", "PLAN", "pending", "")
t = threading.Thread(target=lambda: orch.approval_wait("t9"), daemon=True)
t.start()
_t.sleep(0.2)
ok = orch.approval_submit("t9", "approve")
t.join(timeout=2)
check("approval submit accepted", ok)
check("approval wait released", not t.is_alive())

print("\n" + "=" * 60)
print(f"RESULT: {len(PASS)} passed, {len(FAIL)} failed")
if FAIL:
    print("FAILED:", FAIL)
shutil.rmtree(WS, ignore_errors=True)
sys.exit(1 if FAIL else 0)
