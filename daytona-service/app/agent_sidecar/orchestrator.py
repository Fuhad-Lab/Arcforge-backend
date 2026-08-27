#!/usr/bin/env python3
"""
ArcForge Orchestrator v4 — the LangGraph-orchestrated build daemon.

Runs inside every Daytona MicroVM (PM2 "agent-brain").

THE PRODUCT SHAPE (user-mandated 2026-09-28): the user talks to ONE
agent. Internally that agent is a hierarchical system — a Chief that
NEVER writes code and specialised sub-agents that ARE ITS TOOLS
(the Chief decides which to call, sub-agents own their own tools,
skills are MCP-hosted) — but the STREAM the user sees speaks in a
single unified voice. No "Chief Agent" / "swarm" vocabulary ever
reaches the UI: one agent, planning, building, verifying, fixing.

                User prompt  (WebSocket)
                      │
              ┌───────▼────────────────────┐
              │  THE AGENT (LangGraph hub) │  Classifies → Plans →
              │  chief_node = dispatcher   │  Seeks APPROVAL → calls
              └───────┬────────────────────┘  its tools as needed.
        ┌─────────────┼──────────────────┐
        │  (optional tools — the chief   │
        │   decides which to invoke)     │
 ┌──────▼─────┐ ┌─────▼──────┐   ┌───────▼───────┐
 │ backend    │ │ frontend   │   │ debugger      │
 │ tool       │◄──► tool     │   │ (QA gate)     │
 └──────┬─────┘ └─────┬──────┘   └───────┬───────┘
        └── api_contract.json + agent_mailbox ──┘
                      │
              plan.md (written EXACTLY as the user approved it)

ORCHESTRATION (LangGraph StateGraph — the 2026-09-28 brain transplant):
  AgentState {plan, repo_map, file_system_state, reports, verdict, …}
  Nodes: chief (dispatcher) · backend · frontend · fit_check · debugger
  Edges: START→chief; chief→{backend|frontend|fit_check|debugger|END}
         (conditional — the agent_dispatcher decision, under deterministic
         guardrails); backend/frontend/fit_check→chief;
         debugger→chief on FAIL, debugger→END on PASS (conditional).
  Uses the real `langgraph` package when installed; otherwise a built-in
  _MiniStateGraph with identical semantics runs the same graph.

CONTEXT LAYER (Tree-sitter RepoMapper — "infinite context" cheaply):
  generate_repo_map() extracts the SKELETON of every source file
  (classes, functions, interfaces, exports — never bodies), ranks files
  by importance (app pages + import-graph references) and injects the
  condensed map into the chief's prompts. Regex fallback when the
  tree-sitter packages are absent.

LSP LAYER (verify_file — "the agent that doesn't hallucinate"):
  LSPClient speaks real LSP JSON-RPC over stdio (typescript-language-
  server for TS/JS, pyright-langserver for Python): didOpen →
  publishDiagnostics. Agents MUST verify every file they write; errors
  are fixed BEFORE the task may be declared done. CLI cascade fallback
  (tsc --noEmit / pyflakes / py_compile) when the daemons are absent.

APPROVAL STATE MACHINE (the pause that makes this a product):
  user prompt → plan drafted → task state AWAITING_APPROVAL → WS event
  {type:"approval_request", plan} → the studio shows [Make changes |
  Approve Plan] → feedback injected as "I have read through the plan.
  Make the following change(s): …" → revised plan → … until approved →
  plan.md locked → the graph runs.

THE DEVELOPMENT TWINS share a context loop:
  • The backend tool writes its server, then PUBLISHES /workspace/.system/
    api_contract.json (endpoints + port) and posts to the agent mailbox.
  • The frontend tool is FORBIDDEN from guessing API URLs — it READS the
    contract and writes frontend/lib/api_client.ts from it.
  • THE FIT CHECK: both servers run; the frontend tool drives the live
    app with browser_tool and attributes every error with evidence —
    its own (it fixes) or the backend's (mailbox: expected vs got).

THE DEBUGGER tool audits the live app against plan.md FEATURE BY
FEATURE and reports exactly WHAT IS MISSING (a plan-coverage
checklist, not just errors). The chief receives that checklist and
DECIDES which of its tools builds each gap; crash-class evidence rides
to the owner verbatim. The cycle continues until the app MATCHES the
approved plan — there is NO fixed round cap (v5). It ends early only
when progress PROVABLY stops — the same gap signature across
consecutive QA rounds surviving ONE context escalation — or the
wall-clock guard trips; then the honest-fail verdict names the
remaining gaps.

Browser Vision Engine (vm_browser.py): Playwright bridge with
navigate / console_spy / interact / screenshot (+ VLM review through the
reverse tunnel — the VM never holds a provider key).

Skills (skills_server.py): hosted as MCP-style tools with STRICT
per-tool-role segregation.

Everything the UI shows is REAL: every activity line is an actual agent
action; there are no hardcoded progress strings anywhere.

Durable state: SQLite (WAL). PM2 restarts re-enqueue unfinished tasks;
a task awaiting approval re-emits its approval request on reconnect.

WebSocket protocol (daemon → client): sync, task_queued, status,
activity, log, chat, files, approval_request, plan_locked, task_done,
task_failed, pong.
(client → daemon): hello, ping, prompt, approval_response.
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import os
import pathlib
import queue as _queue
import re
import secrets
import sqlite3
import subprocess
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import (Any, Callable, Dict, List, Optional, Tuple, Union,
                    TypedDict)
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

PORT = int(os.environ.get("ORCH_PORT", "9000"))
TOKEN = os.environ.get("ORCH_TOKEN", "")
WORKSPACE = os.environ.get("ORCH_WORKSPACE", "/workspace")
RT_TOKEN = os.environ.get("TUNNEL_TOKEN", "")
SYSTEM_DIR = os.environ.get("ORCH_SYSTEM_DIR", "/home/daytona/.system")
DB_PATH = os.environ.get("ORCH_DB", os.path.join(SYSTEM_DIR, "state.db"))

LLM_URL = os.environ.get("ORCH_LLM_URL", "http://localhost:7777/v1")
LLM_USE_REVERSE_TUNNEL = LLM_URL.startswith("reverse-tunnel://")
if not LLM_USE_REVERSE_TUNNEL:
    LLM_URL = LLM_URL.rstrip("/")
    if not LLM_URL.endswith("/chat/completions"):
        LLM_URL = f"{LLM_URL}/chat/completions"
LLM_KEY = os.environ.get("ORCH_LLM_KEY", "tunnel-injected")
LLM_MODEL = os.environ.get("ORCH_LLM_MODEL", "nvidia/nemotron-3-ultra-550b-a55b")
# ─────────────────────────────────────────────────────────────────────────
# ROLE-ROUTED MODEL CHAINS (2026-09-26, NVIDIA API, live-measured head-to-
# head on the REAL tool loops — see worklog Task 28):
#   CHIEF      = nemotron-3-ultra-550b-a55b (550B MoE/55B active — the big
#                planning brain; drifts from JSON schemas occasionally and
#                503s under load → falls back to gpt-oss-120b, the proven
#                planner: 8.3s plans with acceptance criteria).
#   FRONTEND   = minimaxai/minimax-m3 (genuinely good JSON tool-caller that
#                writes real React AND is multimodal — described a UI
#                screenshot accurately in 6.7s; BUT the account's rate
#                limit on it is tight: sustained 429s after ~2 rapid calls
#                → sticky demotion falls back to nemotron-3-super).
#   BACKEND    = deepseek-ai/deepseek-v4-pro-0813 (dominated the field:
#                3-5 steps at 0.7-12s/call, real Express code, recovered
#                from an injected missing-dependency error, published the
#                api contract unprompted. The nemotron-3-super baseline
#                flailed to its step limit on the same task).
#   DEBUGGER   = nemotron-3-super-120b-a12b (BOTH user-picks failed live:
#                deepseek-v4-flash timed out at 160s+ on its FIRST call,
#                nemotron-3.5-lightning burned 2184 reasoning tokens on
#                call 1 and hit the step limit without finishing).
#                nemotron-3-super stays the proven browser-tool caller.
# Fallbacks engage on HTTP 429/404/503 or repeated empty content, with
# sticky per-model demotion (ORCH_MODEL_DEMOTE_S, default 10 min) so a
# rate-limited primary doesn't tax every subsequent step.
# ─────────────────────────────────────────────────────────────────────────
AGENT_MODEL = os.environ.get("ORCH_AGENT_MODEL", "nvidia/nemotron-3-super-120b-a12b")
CHIEF_FALLBACK_MODEL = os.environ.get(
    "ORCH_CHIEF_FALLBACK_MODEL", "openai/gpt-oss-120b")
FRONTEND_MODEL = os.environ.get(
    "ORCH_FRONTEND_MODEL", "minimaxai/minimax-m3")
BACKEND_MODEL = os.environ.get(
    "ORCH_BACKEND_MODEL", "deepseek-ai/deepseek-v4-pro-0813")
DEBUGGER_MODEL = os.environ.get(
    "ORCH_DEBUGGER_MODEL", AGENT_MODEL)


def _model_chain(*candidates: Optional[str]) -> List[str]:
    """Ordered, de-duplicated model chain (primaries first, fallbacks last)."""
    seen: set = set()
    chain: List[str] = []
    for m in candidates:
        if m and m not in seen:
            seen.add(m)
            chain.append(m)
    return chain


CHIEF_MODELS = _model_chain(LLM_MODEL, CHIEF_FALLBACK_MODEL)
FRONTEND_MODELS = _model_chain(FRONTEND_MODEL, AGENT_MODEL)
BACKEND_MODELS = _model_chain(BACKEND_MODEL, AGENT_MODEL)
DEBUGGER_MODELS = _model_chain(DEBUGGER_MODEL, AGENT_MODEL)

# Observability: the full routing table (surfaced in /status + task results).
MODEL_ROUTING = {
    "chief": CHIEF_MODELS,
    "frontend": FRONTEND_MODELS,
    "backend": BACKEND_MODELS,
    "debugger": DEBUGGER_MODELS,
}

# Sticky demotion: model-id → epoch-until. While demoted, the model is
# skipped entirely (the chain's fallback serves the call). Re-armed after
# the window so a transient rate limit doesn't exile a model forever.
_MODEL_DEMOTED: Dict[str, float] = {}
_MODEL_DEMOTE_S = float(os.environ.get("ORCH_MODEL_DEMOTE_S", "600"))

LLM_TIMEOUT_S = float(os.environ.get("ORCH_LLM_TIMEOUT_S", "900"))
LLM_READY = os.environ.get("ORCH_LLM_READY", "1") == "1"

# Vision model for the Browser Vision Engine (routed through the reverse
# tunnel at /vlm/chat/completions; the backend holds the NVIDIA key).
VLM_MODEL = os.environ.get("ORCH_VLM_MODEL", "meta/llama-3.2-11b-vision-instruct")
VLM_ENABLED = os.environ.get("ORCH_VLM_ENABLED", "1") == "1"

NEXT_DEV_PORT = int(os.environ.get("ORCH_NEXT_PORT", "3000"))
VITE_DEV_PORT = int(os.environ.get("ORCH_VITE_PORT", "5173"))
LOG_TAIL_FOR_SYNC = int(os.environ.get("ORCH_SYNC_LOG_TAIL", "50"))
LOG_FILE = os.environ.get("ORCH_LOG_FILE", os.path.join(SYSTEM_DIR, "orchestrator.log"))

# Build-graph tuning
APPROVAL_TIMEOUT_S = float(os.environ.get("ORCH_APPROVAL_TIMEOUT_S", str(6 * 3600)))
# Agent-loop step budget: the agent model is thorough but ~60-100s/call —
# this keeps a single agent run inside ~15-20 min worst case.
AGENT_MAX_STEPS = int(os.environ.get("ORCH_AGENT_MAX_STEPS", "14"))
# v5 CONVERGENCE (user-mandated 2026-08-27): the QA loop has NO fixed
# round cap. It runs until the debugger's OBSERVATION of the live app
# MATCHES plan.md. Early exit only when progress PROVABLY stops:
#   * stagnation — the SAME gap signature across consecutive QA rounds,
#     surviving ONE context escalation (which mandates a different
#     approach), or
#   * the wall-clock safety net (default 45 min — several multiples of a
#     healthy run, so only a genuinely stuck loop ever sees it).
CONVERGE_MAX_S = float(os.environ.get("ORCH_CONVERGE_MAX_S", "2700"))
CONVERGE_STAGNATION = int(os.environ.get("ORCH_CONVERGE_STAGNATION", "2"))
# Graph super-step budget: each QA round costs ~4 super-steps (chief →
# fix tool → chief → debugger), so 240 supports ~50 convergence rounds —
# never the binding constraint.
GRAPH_RECURSION_LIMIT = int(os.environ.get("ORCH_GRAPH_RECURSION_LIMIT", "240"))
# The dispatcher budget bounds the BUILD phase (the dispatcher LLM's fresh
# build decisions). Fix dispatches from failed QA rounds do NOT consume
# it — those are bounded by the convergence safety nets above, so the
# loop can run as many QA rounds as real progress justifies.
MAX_DISPATCHES = int(os.environ.get("ORCH_MAX_DISPATCHES", "8"))
# LSP daemon timeouts (first tsserver load is slow — later calls are fast).
LSP_INIT_TIMEOUT_S = float(os.environ.get("ORCH_LSP_INIT_TIMEOUT_S", "45"))
LSP_DIAG_TIMEOUT_S = float(os.environ.get("ORCH_LSP_DIAG_TIMEOUT_S", "30"))

API_CONTRACT_PATH = os.path.join(WORKSPACE, ".system", "api_contract.json")
PLAN_PATH = os.path.join(WORKSPACE, "plan.md")

os.makedirs(SYSTEM_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler(LOG_FILE)],
)
log = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# Scaffold + workspace helpers (proven v2 machinery, kept as-is)
# ---------------------------------------------------------------------------

_SRC_EXTS = (".tsx", ".ts", ".jsx", ".js", ".py", ".mjs", ".cjs")
_SKIP_DIRS = {"node_modules", ".next", ".git", ".system", "__pycache__", ".venv", "dist", "build"}

SCAFFOLD_FILES: Dict[str, str] = {
    "frontend/package.json": (
        '{"name":"arcforge-app","version":"0.1.0","private":true,'
        '"scripts":{"dev":"next dev","build":"next build","start":"next start"},'
        '"dependencies":{"next":"14.2.35","react":"18.3.1","react-dom":"18.3.1"},'
        '"devDependencies":{"typescript":"5.5.4","@types/node":"20.14.9",'
        '"@types/react":"18.3.3","@types/react-dom":"18.3.0"}}\n'
    ),
    "frontend/tsconfig.json": (
        '{"compilerOptions":{"lib":["dom","dom.iterable","esnext"],'
        '"allowJs":true,"skipLibCheck":true,"strict":true,"noEmit":true,'
        '"esModuleInterop":true,"module":"esnext","moduleResolution":"bundler",'
        '"resolveJsonModule":true,"isolatedModules":true,"jsx":"preserve",'
        '"incremental":true,"plugins":[{"name":"next"}],'
        '"paths":{"@/*":["./*"]}},'
        '"include":["next-env.d.ts","**/*.ts","**/*.tsx",".next/types/**/*.ts"],'
        '"exclude":["node_modules"]}\n'
    ),
    "frontend/next.config.mjs": (
        "/** @type {import('next').NextConfig} */\n"
        "const nextConfig = {}\nexport default nextConfig\n"
    ),
    "frontend/app/layout.tsx": (
        "import type { ReactNode } from 'react'\n"
        "export const metadata = { title: 'ArcForge App', description: 'Built with ArcForge' }\n"
        "export default function RootLayout({ children }: { children: ReactNode }) {\n"
        "  return (\n"
        "    <html lang=\"en\">\n"
        "      <head>\n"
        "        <style>{`*{box-sizing:border-box}body{margin:0;font-family:system-ui,-apple-system,sans-serif;background:#0a0a0a;color:#fafafa}`}</style>\n"
        "      </head>\n"
        "      <body>{children}</body>\n"
        "    </html>\n"
        "  )\n"
        "}\n"
    ),
    "frontend/.gitignore": "node_modules/\n.next/\n",
    ".gitignore": "**/node_modules/\n**/.next/\n**/__pycache__/\n*.log\n",
}


def seed_scaffold(task_id: Optional[str] = None) -> bool:
    fe = os.path.join(WORKSPACE, "frontend")
    seeded = False
    for rel, content in SCAFFOLD_FILES.items():
        dest = os.path.join(WORKSPACE, rel)
        if os.path.exists(dest):
            continue
        os.makedirs(os.path.dirname(dest), exist_ok=True)
        with open(dest, "w", encoding="utf-8") as fh:
            fh.write(content)
        seeded = True
        if task_id:
            upsert_file(dest, task_id, "create")
    if seeded and not os.path.exists(os.path.join(fe, "node_modules")):
        try:
            subprocess.Popen(
                ["nohup", "npm", "install", "--no-audit", "--no-fund", "--loglevel=error"],
                cwd=fe, stdout=open("/tmp/scaffold-install.log", "w"),
                stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            log.info("scaffold: background npm install launched")
        except Exception as exc:  # noqa: BLE001
            log.warning("scaffold: background npm install failed to start: %s", exc)
    return seeded


def _iter_source_files() -> List[str]:
    out: List[str] = []
    for root_name in ("frontend", "backend"):
        base = os.path.join(WORKSPACE, root_name)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
            for fn in sorted(filenames):
                if fn.endswith(_SRC_EXTS) or fn in ("package.json", "requirements.txt"):
                    out.append(os.path.relpath(os.path.join(dirpath, fn), WORKSPACE))
    return sorted(out)


def workspace_has_source() -> bool:
    scaffold = set(SCAFFOLD_FILES.keys())
    for rel in _iter_source_files():
        if rel in scaffold or rel.endswith("app/layout.tsx"):
            continue
        if rel.endswith((".tsx", ".ts", ".jsx", ".py", ".js", ".mjs", ".cjs")):
            return True
    return False


def workspace_tree_text() -> str:
    files = _iter_source_files()
    if not files:
        return "(empty — no app code yet)"
    return "\n".join(files[:60])


def read_file_for_edit(rel: str, cap: int = 6000) -> str:
    try:
        with open(os.path.join(WORKSPACE, rel), "r", encoding="utf-8", errors="replace") as fh:
            content = fh.read()
    except OSError:
        return "(file does not exist yet — create it)"
    if len(content) > cap:
        return content[:cap] + "\n/* ...TRUNCATED — reconstruct the tail coherently... */"
    return content


def git_checkpoint(label: str) -> Optional[str]:
    try:
        def g(*args: str) -> subprocess.CompletedProcess:
            return subprocess.run(
                ["git", "-C", WORKSPACE, *args], capture_output=True, text=True, timeout=30)
        if not os.path.isdir(os.path.join(WORKSPACE, ".git")):
            r = g("init", "-q")
            if r.returncode != 0:
                return None
            g("config", "user.email", "agent@arcforge.local")
            g("config", "user.name", "ArcForge Agent")
        g("add", "-A")
        r = g("commit", "-q", "--no-verify", "-m", label)
        if r.returncode == 0:
            return label
        return None
    except Exception as exc:  # noqa: BLE001
        log.warning("git checkpoint failed (non-fatal): %s", exc)
        return None


# ---------------------------------------------------------------------------
# CONTEXT LAYER — Tree-sitter RepoMapper ("infinite context" without the
# cost). Maps the SKELETON of the codebase (classes, functions, interfaces,
# exports — never bodies) and ranks files by importance, so prompts carry
# a condensed map instead of raw file dumps. Falls back to regex extraction
# when the tree-sitter packages aren't installed yet.
# ---------------------------------------------------------------------------

try:  # tree-sitter core (optional — regex fallback below)
    import tree_sitter  # noqa: F401  # type: ignore
    _TS_CORE = True
except Exception:  # noqa: BLE001
    _TS_CORE = False

_TS_LANG_GETTERS: List[Callable[[str], Any]] = []
try:  # legacy pack (py<3.12 wheels)
    import tree_sitter_languages as _tsl  # type: ignore
    _TS_LANG_GETTERS.append(lambda name: _tsl.get_language(name))
except Exception:  # noqa: BLE001
    pass
try:  # maintained pack
    import tree_sitter_language_pack as _tslp  # type: ignore
    _TS_LANG_GETTERS.append(lambda name: _tslp.get_language(name))
except Exception:  # noqa: BLE001
    pass
_TS_ENABLED = _TS_CORE and bool(_TS_LANG_GETTERS)

# tree-sitter node types whose declaration line we keep (per language).
_TS_NODE_KINDS = {
    "python": ("function_definition", "class_definition"),
    "typescript": ("function_declaration", "class_declaration",
                   "interface_declaration", "abstract_class_declaration",
                   "method_definition", "enum_declaration"),
    "tsx": ("function_declaration", "class_declaration",
            "interface_declaration", "abstract_class_declaration",
            "method_definition", "enum_declaration"),
    "javascript": ("function_declaration", "class_declaration",
                   "method_definition"),
}
_TS_EXT_LANG = {".ts": "typescript", ".tsx": "tsx", ".js": "javascript",
                ".jsx": "javascript", ".py": "python"}

# Regex fallback patterns (first match group = the definition line).
_RE_DEF = {
    ".py": re.compile(r"^\s*(?:async\s+def|def|class)\s+[A-Za-z_]\w*", re.M),
    ".ts": re.compile(
        r"^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?"
        r"(?:function|class|interface|type|enum)\s+[A-Za-z_$][\w$]*"
        r"|^\s*export\s+(?:const|let)\s+[A-Za-z_$][\w$]*\s*=?", re.M),
}
_RE_DEF[".tsx"] = _RE_DEF[".ts"]
_RE_DEF[".jsx"] = _RE_DEF[".ts"]
_RE_DEF[".js"] = _RE_DEF[".ts"]


def _ts_language(lang: str):
    for getter in _TS_LANG_GETTERS:
        try:
            return getter(lang)
        except Exception:  # noqa: BLE001 — try the next pack
            continue
    return None


class ContextManager:
    """Generates the repo map — the codebase's skeleton, ranked. Injected
    into the chief's prompts (replaces raw file dumping): ~90% fewer
    tokens and better model focus. Request full files only when editing."""

    MAX_FILES = 60
    MAX_CHARS = 5200

    @staticmethod
    def _signature_from_line(path: str, line_no: int) -> str:
        """The definition's signature: up to 2 source lines, cleaned."""
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                lines = fh.readlines()
        except OSError:
            return ""
        chunk = "".join(lines[line_no:line_no + 2]).strip()
        chunk = chunk.split("\n")[0].rstrip("{;").rstrip()
        if len(chunk) > 118:
            chunk = chunk[:115] + "…"
        return chunk

    @classmethod
    def _defs_via_treesitter(cls, path: str, lang: str) -> List[str]:
        parser_lang = _ts_language(lang)
        if parser_lang is None:
            return []
        try:
            import tree_sitter as _ts
            parser = _ts.Parser(parser_lang)
            with open(path, "rb") as fh:
                tree = parser.parse(fh.read())
            wanted = _TS_NODE_KINDS.get(lang, ())
            out: List[str] = []
            stack = [tree.root_node]
            while stack and len(out) < 40:
                node = stack.pop()
                if node.type in wanted:
                    sig = cls._signature_from_line(path, node.start_point[0])
                    if sig:
                        out.append(sig)
                stack.extend(reversed(node.children))
            return out
        except Exception:  # noqa: BLE001 — any parser hiccup → regex
            return []

    @staticmethod
    def _defs_via_regex(path: str, ext: str) -> List[str]:
        pat = _RE_DEF.get(ext)
        if pat is None:
            return []
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as fh:
                text = fh.read(200_000)
        except OSError:
            return []
        return [m.group(0).strip()[:118] for m in pat.finditer(text)][:40]

    @staticmethod
    def _import_scores(files: List[str]) -> Dict[str, int]:
        """Aider-style simplified ranking: files referenced by more import
        statements matter more."""
        scores: Dict[str, int] = {f: 0 for f in files}
        bodies: Dict[str, str] = {}
        for f in files:
            try:
                with open(os.path.join(WORKSPACE, f), "r",
                          encoding="utf-8", errors="replace") as fh:
                    bodies[f] = fh.read(60_000)
            except OSError:
                bodies[f] = ""
        for f, body in bodies.items():
            if "import" not in body and "from" not in body:
                continue
            for other in files:
                if other == f:
                    continue
                stem = os.path.splitext(os.path.basename(other))[0]
                if stem in ("page", "layout", "app", "index", "main", "route"):
                    continue  # too generic to count
                if re.search(rf"(?:from\s+['\"][^'\"]*/{stem}['\"]"
                             rf"|import\s+[^;]*/{stem})", body):
                    scores[other] += 1
        return scores

    @classmethod
    def generate_repo_map(cls, root_dir: str = WORKSPACE) -> str:
        """The condensed repo skeleton, importance-ranked. Empty string
        when there is no source yet."""
        files = [f for f in _iter_source_files()
                 if os.path.splitext(f)[1].lower() in _TS_EXT_LANG]
        if not files:
            return ""
        import_scores = cls._import_scores(files)

        def importance(f: str) -> float:
            score = float(import_scores.get(f, 0))
            if f.startswith("frontend/app/") and f.endswith("page.tsx"):
                score += 5  # the pages ARE the app
            elif f.startswith("frontend/app/"):
                score += 3
            elif f.startswith("frontend/lib/") or f.startswith("frontend/components/"):
                score += 2
            elif f.startswith("backend/") and os.path.basename(f) in ("app.py", "main.py", "server.py"):
                score += 4
            elif f.startswith("backend/"):
                score += 1
            return score

        ranked = sorted(files, key=importance, reverse=True)[:cls.MAX_FILES]
        blocks: List[str] = []
        used = 0
        for rel in ranked:
            abs_path = os.path.join(WORKSPACE, rel)
            ext = os.path.splitext(rel)[1].lower()
            lang = _TS_EXT_LANG[ext]
            defs = (cls._defs_via_treesitter(abs_path, lang)
                    if _TS_ENABLED else []) or cls._defs_via_regex(abs_path, ext)
            if not defs:
                continue  # config-ish file with no definitions — skip
            block = f"{rel}\n" + "\n".join(f"  · {d}" for d in defs[:12])
            if used + len(block) > cls.MAX_CHARS:
                break
            blocks.append(block)
            used += len(block) + 1
        if not blocks:
            return ""
        engine = "tree-sitter" if _TS_ENABLED else "regex"
        return (f"(repo map — definitions only, {engine}; request full "
                f"files via read_file only when you need to edit them)\n"
                + "\n".join(blocks))


def generate_repo_map(root_dir: str = WORKSPACE) -> str:
    return ContextManager.generate_repo_map(root_dir)


def file_system_state(task_id: str) -> Dict[str, str]:
    """The write journal for a task: {rel_path: action} — the graph state's
    file_system_state key (mirrors the files table)."""
    try:
        with _db_lock, db() as conn:
            rows = conn.execute(
                "SELECT path, action FROM files WHERE task_id=? ORDER BY ts",
                (task_id,)).fetchall()
    except Exception:  # noqa: BLE001 — table may not exist pre-init
        return {}
    out: Dict[str, str] = {}
    for r in rows:
        try:
            out[os.path.relpath(r["path"], WORKSPACE)] = r["action"]
        except ValueError:
            continue
    return out


# ---------------------------------------------------------------------------
# SQLite state store (chat, tasks, logs, status, files, approvals, mailbox)
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS chat_history (
    id        TEXT PRIMARY KEY,
    ts        REAL NOT NULL,
    role      TEXT NOT NULL,
    content   TEXT NOT NULL,
    meta_json TEXT
);
CREATE TABLE IF NOT EXISTS task_queue (
    id          TEXT PRIMARY KEY,
    ts          REAL NOT NULL,
    status      TEXT NOT NULL,
    prompt      TEXT NOT NULL,
    result_json TEXT,
    error       TEXT,
    attempts    INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS process_logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    task_id TEXT,
    source  TEXT NOT NULL,
    level   TEXT NOT NULL,
    message TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_status (
    key        TEXT PRIMARY KEY,
    value_json TEXT NOT NULL,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS files (
    path    TEXT PRIMARY KEY,
    task_id TEXT,
    ts      REAL NOT NULL,
    action  TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS approvals (
    task_id  TEXT PRIMARY KEY,
    plan     TEXT NOT NULL,
    status   TEXT NOT NULL,
    feedback TEXT,
    updated_at REAL NOT NULL
);
CREATE TABLE IF NOT EXISTS agent_mailbox (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    task_id TEXT,
    from_agent TEXT NOT NULL,
    to_agent   TEXT NOT NULL,
    message    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_chat_ts       ON chat_history(ts);
CREATE INDEX IF NOT EXISTS idx_logs_task_ts  ON process_logs(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_files_ts      ON files(ts);
CREATE INDEX IF NOT EXISTS idx_mailbox_ts    ON agent_mailbox(task_id, ts);
"""

_db_lock = threading.RLock()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with _db_lock, db() as conn:
        conn.executescript(_SCHEMA)


def append_chat(role: str, content: str, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    row = {"id": uuid.uuid4().hex, "ts": time.time(), "role": role,
           "content": content, "meta": meta or {}}
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO chat_history (id, ts, role, content, meta_json) VALUES (?,?,?,?,?)",
            (row["id"], row["ts"], role, content, json.dumps(row["meta"])),
        )
    return row


def append_log(task_id: Optional[str], source: str, level: str, message: str) -> None:
    try:
        with _db_lock, db() as conn:
            conn.execute(
                "INSERT INTO process_logs (ts, task_id, source, level, message) VALUES (?,?,?,?,?)",
                (time.time(), task_id, source, level, str(message)[-4000:]),
            )
    except sqlite3.Error:  # pre-init or locked — logging must never crash a build
        log.log(logging.INFO if level == "info" else logging.WARNING,
                "[%s/%s] %s", source, level, str(message)[:300])


def set_status(key: str, value: Dict[str, Any]) -> None:
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO agent_status (key, value_json, updated_at) VALUES (?,?,?) "
            "ON CONFLICT(key) DO UPDATE SET value_json=excluded.value_json, "
            "updated_at=excluded.updated_at",
            (key, json.dumps(value), time.time()),
        )


def get_status(key: str) -> Optional[Dict[str, Any]]:
    with _db_lock, db() as conn:
        row = conn.execute("SELECT value_json FROM agent_status WHERE key=?", (key,)).fetchone()
    return json.loads(row[0]) if row else None


def recent_chat(limit: int = 200) -> List[Dict[str, Any]]:
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT id, ts, role, content, meta_json FROM chat_history "
            "ORDER BY ts DESC LIMIT ?", (limit,),
        ).fetchall()
    return [
        {"id": r["id"], "ts": r["ts"], "role": r["role"], "content": r["content"],
         "meta": json.loads(r["meta_json"] or "{}")}
        for r in rows
    ][::-1]


def recent_logs(limit: int = LOG_TAIL_FOR_SYNC, task_id: Optional[str] = None) -> List[Dict[str, Any]]:
    with _db_lock, db() as conn:
        if task_id:
            rows = conn.execute(
                "SELECT id, ts, task_id, source, level, message FROM process_logs "
                "WHERE task_id=? ORDER BY ts DESC LIMIT ?", (task_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT id, ts, task_id, source, level, message FROM process_logs "
                "ORDER BY ts DESC LIMIT ?", (limit,),
            ).fetchall()
    return [
        {"id": r["id"], "ts": r["ts"], "task_id": r["task_id"], "source": r["source"],
         "level": r["level"], "message": r["message"]}
        for r in rows
    ][::-1]


def all_tasks() -> List[Dict[str, Any]]:
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT id, ts, status, prompt, result_json, error FROM task_queue ORDER BY ts ASC"
        ).fetchall()
    return [
        {"id": r["id"], "ts": r["ts"], "status": r["status"], "prompt": r["prompt"],
         "result": json.loads(r["result_json"]) if r["result_json"] else None,
         "error": r["error"]}
        for r in rows
    ]


def upsert_file(path: str, task_id: Optional[str], action: str) -> None:
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO files (path, task_id, ts, action) VALUES (?,?,?,?) "
            "ON CONFLICT(path) DO UPDATE SET task_id=excluded.task_id, "
            "ts=excluded.ts, action=excluded.action",
            (path, task_id, time.time(), action),
        )


# -- approvals ---------------------------------------------------------------


def approval_upsert(task_id: str, plan: str, status: str, feedback: str = "") -> None:
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO approvals (task_id, plan, status, feedback, updated_at) "
            "VALUES (?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET "
            "plan=excluded.plan, status=excluded.status, feedback=excluded.feedback, "
            "updated_at=excluded.updated_at",
            (task_id, plan, status, feedback, time.time()),
        )


def approval_get(task_id: str) -> Optional[Dict[str, Any]]:
    with _db_lock, db() as conn:
        row = conn.execute(
            "SELECT task_id, plan, status, feedback, updated_at FROM approvals "
            "WHERE task_id=?", (task_id,)).fetchone()
    if not row:
        return None
    return {"task_id": row["task_id"], "plan": row["plan"], "status": row["status"],
            "feedback": row["feedback"], "updated_at": row["updated_at"]}


# -- agent mailbox (the Twins' communication channel) -------------------------


def mailbox_db_send(task_id: str, from_agent: str, to_agent: str, message: str) -> None:
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO agent_mailbox (ts, task_id, from_agent, to_agent, message) "
            "VALUES (?,?,?,?,?)",
            (time.time(), task_id, from_agent, to_agent, str(message)[:2000]),
        )


def mailbox_db_read(task_id: str, agent: str) -> List[Dict[str, Any]]:
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT ts, from_agent, to_agent, message FROM agent_mailbox "
            "WHERE task_id=? AND to_agent=? ORDER BY ts ASC",
            (task_id, agent)).fetchall()
    return [{"ts": r["ts"], "from": r["from_agent"], "message": r["message"]} for r in rows]


# ---------------------------------------------------------------------------
# WebSocket connection manager
# ---------------------------------------------------------------------------


class ConnectionManager:
    def __init__(self) -> None:
        self.active: List[Any] = []
        self._lock = threading.Lock()

    async def connect(self, ws: Any) -> None:
        await ws.accept()
        with self._lock:
            self.active.append(ws)
        log.info("client connected (%d total)", len(self.active))

    def disconnect(self, ws: Any) -> None:
        with self._lock:
            if ws in self.active:
                self.active.remove(ws)
        log.info("client disconnected (%d total) — background tasks continue", len(self.active))

    async def broadcast(self, event: Dict[str, Any]) -> None:
        with self._lock:
            targets = list(self.active)
        if not targets:
            return
        dead: List[Any] = []
        for ws in targets:
            try:
                await ws.send_text(json.dumps(event))
            except Exception:
                dead.append(ws)
        for ws in dead:
            self.disconnect(ws)

    def broadcast_from_worker(self, event: Dict[str, Any]) -> None:
        loop = _LOOP
        if loop is None or loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self.broadcast(event), loop)
        except Exception as exc:  # pragma: no cover
            log.warning("broadcast failed: %s", exc)


manager = ConnectionManager()
_LOOP: Optional[asyncio.AbstractEventLoop] = None


def emit(event: Dict[str, Any]) -> None:
    manager.broadcast_from_worker(event)


# ---------------------------------------------------------------------------
# REVERSE TUNNEL (backend dials in; LLM + VLM requests flow out through it)
# ---------------------------------------------------------------------------


class _InflightRT:
    __slots__ = ("future", "status", "headers", "body_parts")

    def __init__(self) -> None:
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.status: int = 0
        self.headers: Dict[str, str] = {}
        self.body_parts: List[str] = []


class ReverseTunnelMultiplexer:
    def __init__(self) -> None:
        self._inflight: Dict[str, _InflightRT] = {}
        self._ws: Any = None
        self._ws_connected: asyncio.Event = asyncio.Event()
        self._ws_connected.clear()
        self._send_lock = asyncio.Lock()

    @property
    def connected(self) -> bool:
        return self._ws is not None and self._ws_connected.is_set()

    def register(self, req_id: str) -> _InflightRT:
        entry = _InflightRT()
        self._inflight[req_id] = entry
        return entry

    def on_res(self, req_id: str, status: int, headers: Dict[str, str]) -> None:
        e = self._inflight.get(req_id)
        if e is not None:
            e.status = int(status) if status else 502
            e.headers = headers or {}

    def on_chunk(self, req_id: str, body: str) -> None:
        e = self._inflight.get(req_id)
        if e is not None:
            e.body_parts.append(body or "")

    def on_done(self, req_id: str) -> None:
        e = self._inflight.pop(req_id, None)
        if e is not None and not e.future.done():
            e.future.set_result(None)

    def on_error(self, req_id: str, message: str) -> None:
        e = self._inflight.pop(req_id, None)
        if e is not None and not e.future.done():
            e.future.set_exception(RuntimeError(f"reverse-tunnel: {message}"))

    def cancel(self, req_id: str) -> None:
        self._inflight.pop(req_id, None)

    def fail_all(self, reason: str) -> None:
        for e in self._inflight.values():
            if not e.future.done():
                e.future.set_exception(ConnectionError(reason))
        self._inflight.clear()

    async def send_req(self, frame: Dict[str, Any]) -> None:
        async with self._send_lock:
            if self._ws is None or not self._ws_connected.is_set():
                raise ConnectionError("reverse-tunnel WS not connected (backend hasn't dialed in)")
            await self._ws.send_text(json.dumps(frame))


rt_mux = ReverseTunnelMultiplexer()


# ---------------------------------------------------------------------------
# LLM clients — text (Groq/OpenAI-compatible) + VLM (NVIDIA vision, /vlm path)
# ---------------------------------------------------------------------------

_last_llm_done_ts = 0.0
_last_llm_cost_tokens = 0.0
_last_tunnel_down_emit = 0.0
# NVIDIA NIM pacing (2026-08-28 migration): the account's limit is RPM-based
# (~40 RPM/model) with NO per-request token floor like Groq's 8k. A short
# gap after big calls is pure safety margin, not a TPM window.
TPM_GAP_S = 8.0
TPM_MIN_GAP_S = 2.0
TPM_FLOOR_TOKENS = 8000.0


def emit_tunnel_down(reason: str) -> None:
    """Tell connected frontends the LLM bridge is down so THEY can trigger
    agent-info (which makes the backend re-dial the reverse tunnel). The
    frontend is a dumb terminal but it CAN relay the nudge — its vm-ops
    agent-info call is what forces ensureReverseTunnel. Rate-limited."""
    global _last_tunnel_down_emit
    now = time.time()
    if now - _last_tunnel_down_emit < 20:
        return
    _last_tunnel_down_emit = now
    emit({"type": "tunnel_down", "reason": reason[:200]})


def _estimate_cost_tokens(messages: List[Dict[str, Any]], max_tokens: int) -> float:
    prompt_chars = 0
    for m in messages:
        c = m.get("content")
        if isinstance(c, str):
            prompt_chars += len(c)
        elif isinstance(c, list):  # vision content blocks
            for blk in c:
                if isinstance(blk, dict) and isinstance(blk.get("text"), str):
                    prompt_chars += len(blk["text"])
    return prompt_chars / 4.0 + max_tokens


def pace_for_tpm(gap_s: Optional[float] = None) -> None:
    global _last_llm_done_ts, _last_llm_cost_tokens
    if gap_s is None:
        gap_s = max(TPM_MIN_GAP_S, TPM_GAP_S * min(1.0, _last_llm_cost_tokens / TPM_FLOOR_TOKENS))
    wait = _last_llm_done_ts + gap_s - time.time()
    if wait > 0:
        log.info("tpm pacing: sleeping %.1fs (previous call reserved ~%.0f tokens)",
                 wait, _last_llm_cost_tokens)
        time.sleep(wait)


def _tunnel_request(path: str, body: Dict[str, Any]) -> Tuple[int, str]:
    """One request/response over the reverse tunnel. Returns (status, body)."""
    loop = _LOOP
    if loop is None or loop.is_closed():
        raise RuntimeError("reverse-tunnel: asyncio loop not initialized")
    req_id = uuid.uuid4().hex
    frame = {"t": "req", "id": req_id, "method": "POST", "path": path,
             "headers": {"Content-Type": "application/json"},
             "body": json.dumps(body)}
    future = asyncio.run_coroutine_threadsafe(_rt_send_and_await(req_id, frame), loop)
    entry = future.result(timeout=LLM_TIMEOUT_S + 10)
    return entry.status or 502, "".join(entry.body_parts)


async def _rt_send_and_await(req_id: str, frame: Dict[str, Any]) -> _InflightRT:
    entry = rt_mux.register(req_id)
    try:
        await rt_mux.send_req(frame)
    except Exception:
        rt_mux.cancel(req_id)
        raise
    await entry.future
    return entry


def _parse_chat_completion(raw: str) -> str:
    payload = json.loads(raw)
    content = payload["choices"][0]["message"]["content"]
    if not isinstance(content, str) or not content.strip():
        raise RuntimeError("LLM returned an empty message")
    return content


def _llm_call_impl(body: Dict[str, Any], path: str = "/v1/chat/completions",
                   direct_url: Optional[str] = None) -> str:
    """Dispatch on transport: reverse-tunnel or direct urllib. Retries 429/413.

    TUNNEL-DROP TOLERANCE: backend redeploys restart the reverse-tunnel
    client, leaving a ~30-60s window where the WS is down before the
    backend re-dials. ConnectionError retries use PROGRESSIVE backoff
    (5/15/30/60s ≈ 110s total) so in-flight build tasks survive deploys
    instead of dying mid-agent (observed live: a refine_plan call failed
    'backend hasn't dialed in' during a deploy restart)."""
    _CONNECT_BACKOFF_S = (5, 15, 30, 60)
    last_err: Optional[Exception] = None
    for attempt in range(4):
        try:
            if LLM_USE_REVERSE_TUNNEL:
                status, raw = _tunnel_request(path, body)
            else:
                url = direct_url or LLM_URL
                req = urllib_request.Request(
                    url, data=json.dumps(body).encode("utf-8"),
                    headers={"Content-Type": "application/json",
                             "Authorization": f"Bearer {LLM_KEY}"},
                    method="POST")
                with urllib_request.urlopen(req, timeout=LLM_TIMEOUT_S) as resp:
                    raw = resp.read().decode("utf-8")
                status = 200
            if status != 200:
                preview = raw[:400]
                tpm_limited = status == 413 and (
                    "rate_limit_exceeded" in preview or "tokens per minute" in preview)
                # ONE rate-limit sleep-out, then raise so the role-model
                # CHAIN fallback in llm_chat engages quickly (sustained
                # per-model limits — live: minimax-m3 — must not burn
                # 3×65s inside every step before the fallback fires).
                if (status == 429 or tpm_limited) and attempt < 2:
                    last_err = RuntimeError(f"LLM HTTP {status}: {preview[:200]}")
                    log.warning("llm rate-limited (HTTP %s) — sleeping out the window once", status)
                    time.sleep(65)
                    continue
                raise RuntimeError(f"LLM HTTP {status}: {preview}")
            return _parse_chat_completion(raw)
        except HTTPError as exc:
            if exc.code in (429, 413) and attempt < 2:
                last_err = exc
                time.sleep(20 * (attempt + 1))
                continue
            detail = exc.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(f"LLM HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, OSError, ConnectionError) as exc:
            # Transport-level failure — retry with PROGRESSIVE backoff so a
            # reverse-tunnel re-dial (backend deploy restart) is survived.
            wait = _CONNECT_BACKOFF_S[min(attempt, len(_CONNECT_BACKOFF_S) - 1)]
            if "reverse-tunnel" in str(exc):
                emit_tunnel_down(str(exc))
            if attempt < 3:
                last_err = exc
                log.warning("llm transport failure (attempt %d): %s — retrying in %ss",
                            attempt + 1, str(exc)[:120], wait)
                time.sleep(wait)
                continue
            raise RuntimeError(f"LLM unreachable: {exc}") from exc
        except RuntimeError:
            raise
    raise RuntimeError(f"LLM failed after retries: {last_err}")


def _llm_chat_single(messages: List[Dict[str, str]], json_mode: bool,
                     max_tokens: int, model: str) -> str:
    """One llm_chat attempt against ONE model id.

    EMPTY-CONTENT RETRY: reasoning models occasionally conclude entirely
    inside the reasoning channel and return null content (observed live:
    "LLM returned an empty message" killed whole agent runs at step 2).
    Up to two immediate retries; real errors propagate immediately."""
    body: Dict[str, Any] = {"model": model, "messages": messages,
                            "temperature": 0, "max_tokens": max_tokens}
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    global _last_llm_done_ts, _last_llm_cost_tokens
    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            content = _llm_call_impl(body)
            _last_llm_done_ts = time.time()
            _last_llm_cost_tokens = _estimate_cost_tokens(messages, max_tokens)
            return content
        except RuntimeError as exc:
            _last_llm_done_ts = time.time()
            _last_llm_cost_tokens = _estimate_cost_tokens(messages, max_tokens)
            if "empty message" in str(exc) and attempt < 2:
                log.warning("llm returned empty content — retry %d/2", attempt + 1)
                last_err = exc
                time.sleep(2)
                continue
            raise
    raise last_err or RuntimeError("LLM failed")


def llm_chat(messages: List[Dict[str, str]], json_mode: bool = False,
             max_tokens: int = 16384,
             model: Optional[Union[str, List[str]]] = None) -> str:
    """Text LLM (the swarm's brain).

    `model` may be a single id or an ordered CHAIN [primary, fallback…]
    (role routing — CHIEF/FRONTEND/BACKEND/DEBUGGER_MODELS above). On a
    retryable failure of one model (HTTP 429 rate-limit, 404 not-on-account,
    503 overloaded, or persistent empty content) the model is sticky-demoted
    for ORCH_MODEL_DEMOTE_S seconds and the next model in the chain serves
    the call — so a rate-limited primary (live-observed: minimax-m3 429s
    after ~2 rapid calls) never taxes every subsequent step of a build."""
    chain: List[str] = ([m for m in model if m]
                        if isinstance(model, (list, tuple))
                        else [model or LLM_MODEL]) or [LLM_MODEL]
    # Skip sticky-demoted models entirely (unless ALL are demoted — then
    # try the chain as-is: a rate-limited attempt beats no attempt).
    now = time.time()
    active = [m for m in chain if _MODEL_DEMOTED.get(m, 0) < now] or chain
    last_err: Optional[Exception] = None
    i = 0
    window_waited = False
    while i < len(active):
        m = active[i]
        try:
            return _llm_chat_single(messages, json_mode, max_tokens, m)
        except RuntimeError as exc:
            msg = str(exc)
            last_err = exc
            # Match BOTH error shapes: direct ("LLM HTTP 429: …") and
            # reverse-tunnel-relayed ("reverse-tunnel: LLM upstream HTTP
            # 429: …" — live: this shape BYPASSED the old substring check
            # and 429s killed whole agent loops instead of failing over).
            demotable = bool(re.search(r"HTTP (429|404|503)\b", msg)) \
                or "empty message" in msg
            if demotable and i < len(active) - 1:
                _MODEL_DEMOTED[m] = time.time() + _MODEL_DEMOTE_S
                log.warning("model %s unusable (%s…) — demoted for %ss, "
                            "falling back to %s",
                            m, msg[:110], int(_MODEL_DEMOTE_S), active[i + 1])
                i += 1
                continue
            # Lone-model chain (no fallback) hit a 429: sleep out ONE
            # rate-limit window and retry — a lone model must not insta-die
            # on a transient limit (empty-content was already retried twice
            # inside _llm_chat_single, so only 429s earn the window wait).
            if "HTTP 429" in msg and not window_waited:
                window_waited = True
                log.warning("llm_chat: %s rate-limited with no fallback — "
                            "sleeping out the window once", m)
                time.sleep(65)
                continue  # retry the same model
            raise
    raise last_err or RuntimeError("LLM failed")


def llm_vlm(image_path: str, question: str, max_tokens: int = 420) -> str:
    """Vision LLM — screenshot review via the reverse tunnel (/vlm path →
    NVIDIA on the backend side; the VM never holds the key)."""
    if not VLM_ENABLED:
        return "(vision model disabled)"
    with open(image_path, "rb") as fh:
        b64 = base64.b64encode(fh.read()).decode("ascii")
    body = {
        "model": VLM_MODEL,
        "messages": [{
            "role": "user",
            "content": [
                {"type": "image_url",
                 "image_url": {"url": f"data:image/png;base64,{b64}"}},
                {"type": "text", "text": question},
            ],
        }],
        "max_tokens": max_tokens,
        "temperature": 0.2,
    }
    try:
        return _llm_call_impl(body, path="/vlm/chat/completions")
    except Exception as exc:  # noqa: BLE001 — VLM is best-effort
        return f"(vision check unavailable: {str(exc)[:150]})"


def _repair_double_escaped(text: str) -> str:
    if not text:
        return text
    if text.count("\n") == 0 and text.count("\\n") >= 1:
        return (text.replace("\\r\\n", "\n").replace("\\n", "\n")
                .replace("\\t", "\t").replace('\\"', '"'))
    return text


def _extract_json(text: str) -> Dict[str, Any]:
    fenced = re.search(r"```(?:json)?\s*([\s\S]*?)```", text)
    candidate = fenced.group(1) if fenced else text
    start = candidate.find("{")
    end = candidate.rfind("}")
    if start >= 0 and end > start:
        candidate = candidate[start:end + 1]
    try:
        return json.loads(candidate)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"LLM reply was not valid JSON: {text[:300]}") from exc


# ---------------------------------------------------------------------------
# Browser Vision Engine + Skills server (sidecar modules)
# ---------------------------------------------------------------------------

import importlib.util as _ilu

_SIDE_CAR_DIR = os.path.dirname(os.path.abspath(__file__))


def _load_module(name: str):
    path = os.path.join(_SIDE_CAR_DIR, f"{name}.py")
    if not os.path.exists(path):
        log.warning("sidecar module missing: %s", path)
        return None
    spec = _ilu.spec_from_file_location(name, path)
    mod = _ilu.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_vm_browser_mod = _load_module("vm_browser")
_skills_server_mod = _load_module("skills_server")

browser_engine = None
if _vm_browser_mod is not None:
    browser_engine = _vm_browser_mod.get_browser_engine(vlm_fn=llm_vlm)
else:  # pragma: no cover
    log.error("vm_browser.py failed to load — browser_tool will report not-installed")

if _skills_server_mod is not None:
    _skills_server_mod.load_catalog()
else:  # pragma: no cover
    log.error("skills_server.py failed to load — skills MCP unavailable")


def skills_catalog_summary() -> Dict[str, int]:
    if _skills_server_mod is not None:
        return _skills_server_mod.catalog_summary()
    return {r: 0 for r in ("chief", "frontend", "backend", "debugger")}


# ---------------------------------------------------------------------------
# LSP LAYER — "the agent that doesn't hallucinate".
#   LSPClient speaks real LSP JSON-RPC over stdio:
#     · typescript-language-server --stdio  (TS/JS/TSX — tsserver-backed)
#     · pyright-langserver --stdio          (Python)
#   didOpen → publishDiagnostics; agents MUST call verify_file on every
#   file they write and fix the reported errors before declaring done.
#   When a daemon binary is missing (background install still running) the
#   proven CLI cascade answers instead: tsc --noEmit / pyright / pyflakes /
#   py_compile — the agent always gets diagnostics from SOMETHING real.
# ---------------------------------------------------------------------------

_LSP_EXT_LANG = {".ts": "typescript", ".tsx": "typescriptreact",
                 ".js": "javascript", ".jsx": "javascript", ".py": "python"}
_LSP_SEVERITY = {1: "error", 2: "warning", 3: "info", 4: "hint"}


class _LSPProc:
    """One language-server subprocess + a reader thread feeding a queue."""

    def __init__(self, cmd: List[str], cwd: str) -> None:
        self.cmd = cmd
        self.cwd = cwd
        self.proc = subprocess.Popen(
            cmd, cwd=cwd,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
        )
        self.q: "_queue.SimpleQueue[Optional[Dict[str, Any]]]" = _queue.SimpleQueue()
        self.dead = False
        threading.Thread(target=self._reader, daemon=True,
                         name=f"lsp-reader-{'-'.join(cmd)}").start()

    def _reader(self) -> None:
        try:
            while True:
                headers: Dict[str, str] = {}
                while True:
                    line = self.proc.stdout.readline()
                    if not line:
                        raise EOFError("language server closed stdout")
                    if line in (b"\r\n", b"\n"):
                        break
                    k, _, v = line.decode("utf-8", "replace").partition(":")
                    headers[k.strip().lower()] = v.strip()
                length = int(headers.get("content-length", "0"))
                if length <= 0:
                    continue
                body = self.proc.stdout.read(length)
                if len(body) < length:
                    raise EOFError("truncated LSP message")
                self.q.put(json.loads(body.decode("utf-8", "replace")))
        except Exception:
            self.dead = True
            self.q.put(None)  # poison pill

    def send(self, obj: Dict[str, Any]) -> None:
        body = json.dumps(obj).encode("utf-8")
        self.proc.stdin.write(b"Content-Length: %d\r\n\r\n" % len(body) + body)
        self.proc.stdin.flush()

    def recv(self, timeout_s: float) -> Optional[Dict[str, Any]]:
        try:
            return self.q.get(timeout=timeout_s)
        except _queue.Empty:
            return None

    def close(self) -> None:
        try:
            self.proc.kill()
        except Exception:  # noqa: BLE001
            pass


class LSPClient:
    """verify_file(path) — real LSP diagnostics for one file, with the CLI
    cascade as the fallback. Daemon processes are persistent (one per
    language) and restart transparently when they die."""

    _SERVERS = {
        "typescript": (["typescript-language-server", "--stdio"], "frontend"),
        "typescriptreact": (["typescript-language-server", "--stdio"], "frontend"),
        "javascript": (["typescript-language-server", "--stdio"], "frontend"),
        "python": (["pyright-langserver", "--stdio"], "backend"),
    }

    def __init__(self) -> None:
        self._procs: Dict[str, _LSPProc] = {}
        self._locks: Dict[str, threading.Lock] = {}

    # -- daemon lifecycle ---------------------------------------------------
    def _daemon(self, lang: str, root_dir: str) -> Optional[_LSPProc]:
        proc = self._procs.get(lang)
        if proc is not None and not proc.dead and proc.proc.poll() is None:
            return proc
        if proc is not None:
            proc.close()
        cmd = self._SERVERS[lang][0]
        try:
            proc = _LSPProc(cmd, root_dir)
        except Exception as exc:  # noqa: BLE001 — binary missing etc.
            append_log(None, "lsp", "info",
                       f"{cmd[0]} unavailable ({exc}) — CLI fallback in use")
            return None
        # initialize handshake (first tsserver boot is slow — generous cap)
        try:
            proc.send({"jsonrpc": "2.0", "id": 1, "method": "initialize",
                       "params": {
                           "processId": None,
                           "rootUri": pathlib.Path(root_dir).as_uri(),
                           "workspaceFolders": [{
                               "uri": pathlib.Path(root_dir).as_uri(),
                               "name": os.path.basename(root_dir)}],
                           "capabilities": {},
                       }})
            deadline = time.time() + LSP_INIT_TIMEOUT_S
            while time.time() < deadline:
                msg = proc.recv(max(1.0, deadline - time.time()))
                if msg is None:
                    break
                if msg.get("id") == 1:
                    proc.send({"jsonrpc": "2.0",
                               "method": "initialized",
                               "params": {}})
                    self._procs[lang] = proc
                    return proc
            proc.close()
            append_log(None, "lsp", "info",
                       f"{cmd[0]} initialize timed out — CLI fallback in use")
            return None
        except Exception as exc:  # noqa: BLE001
            proc.close()
            append_log(None, "lsp", "warn",
                       f"{cmd[0]} handshake failed: {exc}")
            return None

    # -- the public API ------------------------------------------------------
    def verify_file(self, rel_path: str) -> Dict[str, Any]:
        rel_path = (rel_path or "").strip().lstrip("/")
        abs_path = safe_join(WORKSPACE, rel_path)
        if abs_path is None or not os.path.isfile(abs_path):
            return {"ok": False, "error": f"file not found: {rel_path}"}
        ext = os.path.splitext(rel_path)[1].lower()
        lang = _LSP_EXT_LANG.get(ext)
        if lang is None:
            return {"ok": False,
                    "error": f"unsupported file type '{ext}' (supported: {', '.join(sorted(_LSP_EXT_LANG))})"}
        root_name = self._SERVERS[lang][1]
        root_dir = os.path.join(WORKSPACE, root_name)
        if not os.path.isdir(root_dir):
            root_dir = WORKSPACE
        diags = self._diagnostics(lang, abs_path, root_dir)
        if diags is None:
            # LSP unavailable → CLI cascade (always answers)
            return {"ok": True, "engine": "fallback",
                    "file": rel_path,
                    "diagnostics": _cli_diagnostics_for_file(rel_path, lang)}
        errors = [d for d in diags if d.get("severity") == "error"]
        return {"ok": len(errors) == 0, "engine": "lsp", "file": rel_path,
                "diagnostics": diags, "error_count": len(errors)}

    def _diagnostics(self, lang: str, abs_path: str,
                     root_dir: str) -> Optional[List[Dict[str, Any]]]:
        lock = self._locks.setdefault(lang, threading.Lock())
        with lock:
            proc = self._daemon(lang, root_dir)
            if proc is None:
                return None
            uri = pathlib.Path(abs_path).as_uri()
            try:
                with open(abs_path, "r", encoding="utf-8",
                          errors="replace") as fh:
                    text = fh.read()
            except OSError as exc:
                return [{"severity": "error", "line": 0,
                         "message": f"unreadable: {exc}"}]
            try:
                proc.send({"jsonrpc": "2.0", "method": "textDocument/didOpen",
                           "params": {"textDocument": {
                               "uri": uri, "languageId": lang,
                               "version": int(time.time()),
                               "text": text}}})
                deadline = time.time() + LSP_DIAG_TIMEOUT_S
                while time.time() < deadline:
                    msg = proc.recv(max(1.0, deadline - time.time()))
                    if msg is None:
                        break
                    if msg.get("method") == "textDocument/publishDiagnostics":
                        params = msg.get("params") or {}
                        if params.get("uri") == uri:
                            out = []
                            for d in (params.get("diagnostics") or [])[:40]:
                                out.append({
                                    "line": (d.get("range") or {}).get(
                                        "start", {}).get("line", 0) + 1,
                                    "severity": _LSP_SEVERITY.get(
                                        d.get("severity", 1), "error"),
                                    "message": str(d.get("message", ""))[:220],
                                })
                            return out
                # Timeout with no diagnostics for OUR file — tsserver can
                # legitimately publish nothing for a clean file; treat the
                # timeout as "no diagnostics" only if the daemon is alive.
                if not proc.dead and proc.proc.poll() is None:
                    return []
                return None
            except Exception as exc:  # noqa: BLE001
                append_log(None, "lsp", "warn",
                           f"{lang} LSP call failed: {exc} — falling back")
                proc.close()
                self._procs.pop(lang, None)
                return None

    def health(self) -> Dict[str, Any]:
        return {"engine": "lsp" if self._procs else "cli-fallback",
                "daemons": sorted(self._procs.keys()),
                "langgraph": LANGGRAPH_AVAILABLE,
                "tree_sitter": _TS_ENABLED}


LSP_CLIENT = LSPClient()


def _cli_diagnostics_for_file(rel_path: str, lang: str) -> str:
    """Whole-side CLI check (the proven cascade) narrowed to one file's
    side of the workspace. Returns a trimmed text report."""
    if lang == "python":
        target = os.path.join(WORKSPACE, rel_path)
        r = subprocess.run(["python3", "-m", "py_compile", target],
                           capture_output=True, text=True, timeout=40)
        if r.returncode == 0:
            # pyflakes catches undefined names py_compile misses
            pf = subprocess.run(["python3", "-m", "pyflakes", target],
                                capture_output=True, text=True, timeout=60)
            if pf.returncode == 0 and not (pf.stdout or "").strip():
                return "CLEAN — no syntax or lint issues (py_compile + pyflakes)."
            if (pf.stdout or "").strip():
                return f"PYFLAKES DIAGNOSTICS:\n{pf.stdout[:2500]}"
            return "CLEAN — no syntax errors (py_compile)."
        return f"SYNTAX ERROR:\n{(r.stderr or '')[-1200:]}"
    # TypeScript / JavaScript: one-shot tsc over the project, filtered to
    # the file of interest (tsc has no single-file mode with project config).
    fe = os.path.join(WORKSPACE, "frontend")
    if not os.path.exists(os.path.join(fe, "package.json")):
        return "frontend/package.json missing — nothing to check"
    try:
        proc = subprocess.run("npx tsc --noEmit 2>&1 | head -n 80",
                              shell=True, cwd=fe, capture_output=True,
                              text=True, timeout=240)
        out = (proc.stdout or "").strip()
        if not out:
            return "TSC CLEAN — no type errors."
        relevant = [ln for ln in out.splitlines() if rel_path.split("/")[-1] in ln]
        return ("TSC DIAGNOSTICS (whole project — filter on your file):\n"
                + ("\n".join(relevant) if relevant else out)[:2500])
    except subprocess.TimeoutExpired:
        return "tsc timed out — diagnostics unavailable"
    except Exception as exc:  # noqa: BLE001
        return f"tsc failed to run: {exc}"


def lsp_diagnostics(scope: str) -> str:
    """Run the language server check for one side. Returns trimmed output."""
    if scope == "frontend":
        fe = os.path.join(WORKSPACE, "frontend")
        if not os.path.exists(os.path.join(fe, "package.json")):
            return "frontend/package.json missing — nothing to check"
        try:
            proc = subprocess.run(
                "npx tsc --noEmit 2>&1 | head -n 60",
                shell=True, cwd=fe, capture_output=True, text=True, timeout=240)
            out = (proc.stdout or "") + (proc.stderr or "")
            out = out.strip()
            if not out:
                return "TSC CLEAN — no type errors."
            return f"TSC DIAGNOSTICS:\n{out[:3000]}"
        except subprocess.TimeoutExpired:
            return "tsc timed out (240s) — diagnostics unavailable"
        except Exception as exc:  # noqa: BLE001
            return f"tsc failed to run: {exc}"
    if scope == "backend":
        be = os.path.join(WORKSPACE, "backend")
        if not os.path.isdir(be):
            return "backend/ does not exist"
        py_files = []
        for dirpath, dirnames, filenames in os.walk(be):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
            for fn in filenames:
                if fn.endswith(".py"):
                    py_files.append(os.path.join(dirpath, fn))
        if not py_files:
            return "no python files to check"
        # pyright (real LSP) when available, else pyflakes, else py_compile
        for cmd in (
            "pyright --outputjson 2>/dev/null | head -c 6000",
            "python3 -m pyflakes " + " ".join(f'"{p}"' for p in py_files[:40]),
        ):
            try:
                proc = subprocess.run(cmd, shell=True, cwd=be, capture_output=True,
                                      text=True, timeout=180)
                out = ((proc.stdout or "") + (proc.stderr or "")).strip()
                if proc.returncode == 127 or not out:
                    continue  # tool not installed — try the next
                if cmd.startswith("pyright"):
                    try:
                        data = json.loads(out)
                        diags = data.get("generalDiagnostics", [])
                        if not diags:
                            return "PYRIGHT CLEAN — no diagnostics."
                        lines = []
                        for d in diags[:40]:
                            rng = d.get("range", {}).get("start", {})
                            lines.append(f"{d.get('file','?').replace(be+'/','')}:"
                                         f"{rng.get('line',0)+1} "
                                         f"[{d.get('severity','?')}] {d.get('message','')}")
                        return "PYRIGHT DIAGNOSTICS:\n" + "\n".join(lines)[:3000]
                    except json.JSONDecodeError:
                        pass  # fall through to raw output below
                else:
                    if out == "":  # pyflakes prints nothing when clean
                        return "PYFLAKES CLEAN — no issues."
                    return f"PYFLAKES DIAGNOSTICS:\n{out[:3000]}"
                return f"DIAGNOSTICS:\n{out[:3000]}"
            except subprocess.TimeoutExpired:
                continue
            except Exception:  # noqa: BLE001
                continue
        # Last resort — compile check
        bad = []
        for p in py_files[:40]:
            r = subprocess.run(["python3", "-m", "py_compile", p],
                               capture_output=True, text=True, timeout=30)
            if r.returncode != 0:
                bad.append(f"{os.path.basename(p)}: {(r.stderr or '')[-300:]}")
        return ("PY_COMPILE OK — no syntax errors." if not bad
                else "SYNTAX ERRORS:\n" + "\n".join(bad)[:3000])
    return f"unknown scope {scope}"


# ---------------------------------------------------------------------------
# Shared execution helpers
# ---------------------------------------------------------------------------


def safe_join(base: str, rel: str) -> Optional[str]:
    """Join + path-traversal guard. None when rel escapes base."""
    rel = (rel or "").strip().lstrip("/").replace("\\", "/")
    if not rel or ".." in rel.split("/"):
        return None
    dest = os.path.normpath(os.path.join(base, rel))
    if not (dest == base or dest.startswith(base + os.sep)):
        return None
    return dest


def shell_run(cmd: str, cwd: str, timeout: int = 180) -> Dict[str, Any]:
    try:
        proc = subprocess.run(cmd, shell=True, cwd=cwd, capture_output=True,
                              text=True, timeout=timeout)
        return {"exit_code": proc.returncode,
                "stdout": (proc.stdout or "")[-2500:], "stderr": (proc.stderr or "")[-2000:]}
    except subprocess.TimeoutExpired:
        return {"exit_code": -1, "stdout": "", "stderr": f"timed out after {timeout}s"}
    except Exception as exc:  # noqa: BLE001
        return {"exit_code": -1, "stdout": "", "stderr": f"{type(exc).__name__}: {exc}"}


def write_workspace_file(rel: str, content: str, task_id: Optional[str],
                         allowed_prefixes: Tuple[str, ...] = ("frontend", "backend")) -> Dict[str, Any]:
    """Scoped write with journaling + UI file event."""
    rel = (rel or "").strip().lstrip("/")
    if not rel:
        return {"ok": False, "error": "path required"}
    if not rel.startswith(allowed_prefixes):
        return {"ok": False, "error":
                f"SCOPE VIOLATION: you may only write under {'/'.join(allowed_prefixes)} — "
                f"'{rel}' is outside your scope."}
    dest = safe_join(WORKSPACE, rel)
    if dest is None:
        return {"ok": False, "error": f"invalid path '{rel}'"}
    os.makedirs(os.path.dirname(dest), exist_ok=True)
    existed = os.path.exists(dest)
    with open(dest, "w", encoding="utf-8") as fh:
        fh.write(_repair_double_escaped(content))
    action = "edit" if existed else "create"
    upsert_file(dest, task_id, action)
    if task_id:
        emit({"type": "files", "task_id": task_id,
              "files": [{"path": dest, "action": action}]})
    return {"ok": True, "path": rel, "action": action, "bytes": len(content)}


# ---------------------------------------------------------------------------
# THE AGENT LOOP ENGINE — JSON tool-calling with history compaction
# ---------------------------------------------------------------------------


def _compact_history(messages: List[Dict[str, str]], keep_last: int = 6,
                     budget_chars: int = 9000) -> List[Dict[str, str]]:
    """Keep [system, task] verbatim + the last `keep_last` exchanges verbatim;
    older tool exchanges collapse to one-line summaries. Keeps every request
    under the Groq floor even for long agent runs."""
    if len(messages) <= 2 + keep_last:
        return messages
    head = messages[:2]
    older = messages[2:-keep_last]
    tail = messages[-keep_last:]
    summary_lines: List[str] = []
    for m in older:
        if m["role"] == "assistant":
            try:
                d = _extract_json(m["content"])
                tool = d.get("tool", "?")
                arg = d.get("path") or d.get("command") or d.get("action") or d.get("skill") or ""
                summary_lines.append(f"assistant called {tool} {str(arg)[:60]}")
            except Exception:
                summary_lines.append("assistant replied (non-JSON, dropped)")
        else:
            text = m["content"]
            summary_lines.append(f"tool result: {text[:120]}")
    digest = "EARLIER STEPS (compacted):\n- " + "\n- ".join(summary_lines[-14:])
    # Hard char budget on the digest
    if len(digest) > budget_chars:
        digest = digest[:budget_chars] + "\n…(older steps truncated)"
    return head + [{"role": "user", "content": digest}] + tail


# UNIFIED VOICE — the stream speaks as ONE agent. Sub-agent tool calls get
# human action labels (never "Backend Agent — write_file"); agent_name stays
# internal (log attribution only).
TOOL_LABELS = {
    "write_file": "Writing a file",
    "write_files": "Writing files",
    "edit_file": "Editing a file",
    "read_file": "Reading a file",
    "delete_file": "Deleting a file",
    "terminal": "Running a command",
    "browser_tool": "Checking the app",
    "console_spy": "Checking the console",
    "navigate": "Opening the app",
    "interact": "Using the app",
    "screenshot": "Taking a screenshot",
    "lsp_diagnostics": "Verifying the code",
    "verify_file": "Verifying the code",
    "api_contract_update": "Publishing the API contract",
    "api_contract_read": "Reading the API contract",
    "mailbox_send": "Coordinating the build",
    "mailbox_read": "Checking the build log",
    "mcp_list_skills": "Reviewing available skills",
    "mcp_use_skill": "Applying a skill",
    "done": "Wrapping up",
}


def _tool_label(tool: str) -> str:
    return TOOL_LABELS.get(tool, tool.replace("_", " ").capitalize())


class AgentContext:
    """Everything a sub-agent run needs, bound to one task. agent_name is
    INTERNAL (log attribution); every user-facing line goes through the
    unified agent voice."""

    def __init__(self, task_id: str, agent_name: str) -> None:
        self.task_id = task_id
        self.agent_name = agent_name          # "chief" | "backend" | "frontend" | "debugger"
        self.files_written: List[str] = []
        self.contract_reads = 0
        self.started = time.time()

    def activity(self, label: str, state: str, detail: str = "") -> None:
        emit({"type": "activity", "task_id": self.task_id, "label": label,
              "state": state, "detail": detail})
        append_log(self.task_id, self.agent_name, "info",
                   f"{label} — {detail}" if detail else label)

    def say(self, message: str, kind: str = "note") -> None:
        """The agent's own stream line — appended to chat so the UI shows
        REAL agent narration (never hardcoded)."""
        append_log(self.task_id, self.agent_name, "info", message)
        emit({"type": "log", "task_id": self.task_id, "source": self.agent_name,
              "level": "info", "message": message})


def run_agent_loop(
    ctx: AgentContext,
    system_prompt: str,
    task_prompt: str,
    toolset: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]],
    max_steps: int = AGENT_MAX_STEPS,
    max_tokens: int = 12000,
    model: Optional[Union[str, List[str]]] = None,
) -> Dict[str, Any]:
    """Run one agent: LLM ↔ tools until it replies {"tool":"done", ...} or
    hits the step limit. Every tool call is journaled + broadcast as an
    activity line (the agent's REAL stream). `model` is the ROLE-ROUTED
    chain (FRONTEND/BACKEND/DEBUGGER_MODELS — see the routing note at the
    top); None → AGENT_MODEL chain."""
    tool_names = ", ".join(sorted(toolset.keys()))
    sys_full = (f"{system_prompt}\n\nAVAILABLE TOOLS: {tool_names}.\n"
                'Reply with ONLY a JSON object per step: {"tool":"<name>", <args…>} '
                'or to finish: {"tool":"done","report":"<what you did and the outcome>"}. '
                "Work step by step — NEVER declare done before every part of "
                "the task is complete and verified. Batch related files into "
                "ONE write_files call.")
    messages: List[Dict[str, str]] = [
        {"role": "system", "content": sys_full},
        {"role": "user", "content": task_prompt},
    ]
    last_report = ""
    done_extras: Dict[str, Any] = {}
    for step in range(1, max_steps + 1):
        pace_for_tpm()
        try:
            # Role-routed chain — e.g. FRONTEND_MODELS = [minimax-m3,
            # nemotron-3-super]. json_mode is fine (visible chain-of-thought
            # preambles are handled by the lenient _extract_json).
            reply = llm_chat(_compact_history(messages), json_mode=True,
                             max_tokens=max_tokens,
                             model=model or AGENT_MODEL)
        except Exception as exc:  # noqa: BLE001
            ctx.say(f"Model call failed at step {step} — recovering: "
                    f"{str(exc)[:200]}")
            return {"tool": "done", "report": f"LLM failure at step {step}: "
                    f"{str(exc)[:200]}", "status": "degraded"}
        try:
            data = _extract_json(reply)
        except Exception:  # noqa: BLE001
            data = {"tool": "done", "report": reply[:600]}
        tool = str(data.get("tool") or "").strip().lower()
        if tool in ("", "done", "finish", "complete"):
            last_report = str(data.get("report") or data.get("summary") or
                              data.get("reply") or "done")[:1200]
            # PRESERVE structured verdict extras (status/issues) — the
            # debugger's verdict is data, not prose (live bug found by the
            # v4 graph test: issues were silently dropped here, so QA
            # failures produced zero fix dispatches).
            for _k in ("status", "issues", "verdict", "summary", "tests",
                       "coverage", "missing"):
                if data.get(_k) is not None:
                    done_extras[_k] = data[_k]
            break
        fn = toolset.get(tool)
        if fn is None:
            result = {"ok": False,
                      "error": f"unknown tool '{tool}' — you have: {tool_names}"}
        else:
            args = {k: v for k, v in data.items() if k != "tool"}
            # UNWRAP nested argument wrappers — models intermittently emit
            #   {"tool": name, "arguments": {...}}   (OpenAI style)
            #   {"tool": name, "args": {...}}        (live 2026-09-27:
            #     deepseek-v4-pro AND minimax-m3 both wrap this way — every
            #     tool saw an empty arg dict and both agents burned whole
            #     step budgets on 'command required'/'read scope' loops)
            #   {"tool": name, "parameters": {...}}  (older OpenAI style)
            # Without unwrapping, perfectly valid calls look empty to the
            # tool functions.
            for _wrapper in ("arguments", "args", "parameters"):
                if isinstance(args.get(_wrapper), dict):
                    merged = dict(args[_wrapper])
                    for k, v in args.items():
                        if k != _wrapper and k not in merged:
                            merged[k] = v
                    args = merged
                    break
            # Redact giant echo-backs from the journaled args
            display_args = {k: (v if not (isinstance(v, str) and len(v) > 90)
                                else v[:90] + "…") for k, v in args.items()}
            ctx.activity(_tool_label(tool), "active",
                         " ".join(f"{k}={v}" for k, v in display_args.items())[:160])
            try:
                result = fn(args)
            except Exception as exc:  # noqa: BLE001 — tools must never kill the loop
                result = {"ok": False, "error": f"{type(exc).__name__}: {exc}"}
        result_str = json.dumps(result, ensure_ascii=False)[:3500]
        ctx.activity(_tool_label(tool), "done",
                     ("ok" if result.get("ok", True) else
                      f"error: {str(result.get('error', ''))[:140]}"))
        messages.append({"role": "assistant", "content": reply})
        messages.append({"role": "user", "content": f"TOOL RESULT ({tool}): {result_str}"})
    else:
        last_report = f"step limit ({max_steps}) reached — {last_report[:300]}"
    out = {"tool": "done", "report": last_report, "steps": min(step, max_steps)}
    out.update(done_extras)
    return out


# ---------------------------------------------------------------------------
# SUB-AGENT TOOLSETS (strict capability scopes)
# ---------------------------------------------------------------------------


def _fs_toolset(ctx: AgentContext, scope_dir: str,
                prefixes: Tuple[str, ...]) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    def write_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path, content = a.get("path", ""), a.get("content", "")
        if not isinstance(content, str) or not content.strip():
            return {"ok": False, "error": "content required"}
        r = write_workspace_file(path, content, ctx.task_id, allowed_prefixes=prefixes)
        if r.get("ok"):
            ctx.files_written.append(r["path"])
        return r

    def write_files(a: Dict[str, Any]) -> Dict[str, Any]:
        """Batched write — one call, many files. Accepts BOTH shapes the
        models naturally produce:
          {"files": {"path": "content", ...}}          (object map)
          {"files": [{"path": p, "content": c}, ...]}   (array of objects)
        Cuts agent-loop steps (each LLM call costs 5-120s depending on
        model, so batching matters)."""
        files = a.get("files")
        items: List[Tuple[str, str]] = []
        if isinstance(files, dict):
            items = [(str(p), str(c)) for p, c in files.items()
                     if isinstance(c, str) and c.strip()]
        elif isinstance(files, list):
            for f in files:
                if isinstance(f, dict) and isinstance(f.get("path"), str) \
                        and isinstance(f.get("content"), str) and f["content"].strip():
                    items.append((f["path"], f["content"]))
        if not items:
            return {"ok": False,
                    "error": 'files required: {"files": {"path": "content"}} or '
                             '{"files": [{"path": p, "content": c}]}. At least one '
                             'non-empty file.'}
        results, ok_count = [], 0
        for path, content in items[:12]:
            r = write_workspace_file(path, content, ctx.task_id,
                                     allowed_prefixes=prefixes)
            if r.get("ok"):
                ok_count += 1
                ctx.files_written.append(r["path"])
            results.append(r)
        return {"ok": ok_count > 0, "written": ok_count,
                "files": results[:12]}

    def edit_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path, find, replace = a.get("path", ""), a.get("find", ""), a.get("replace", "")
        dest = safe_join(WORKSPACE, path) if path.startswith(("frontend", "backend")) else None
        if dest is None:
            return {"ok": False, "error": f"path must be under {prefixes}"}
        if not os.path.exists(dest):
            return {"ok": False, "error": f"{path} does not exist"}
        try:
            with open(dest, "r", encoding="utf-8", errors="replace") as fh:
                content = fh.read()
        except OSError as exc:
            return {"ok": False, "error": str(exc)}
        if find and find in content:
            content = content.replace(find, replace, 1)
        elif find:
            return {"ok": False, "error": f"find-text not present in {path}"}
        else:
            content = replace  # no find → whole-file rewrite
        return write_file({"path": path, "content": content})

    def read_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path = a.get("path", "")
        if path.startswith(("frontend", "backend")) or path == "plan.md":
            dest = safe_join(WORKSPACE, path)
            if dest and os.path.isdir(dest):
                # Directories return a LISTING (models ask for these;
                # raising IsADirectoryError just burned their steps).
                try:
                    entries = sorted(os.listdir(dest))[:60]
                    return {"ok": True, "directory": path,
                            "entries": entries}
                except OSError as exc:
                    return {"ok": False, "error": str(exc)}
            if dest and os.path.exists(dest):
                with open(dest, "r", encoding="utf-8", errors="replace") as fh:
                    return {"ok": True, "content": fh.read()[:7000]}
            return {"ok": False, "error": f"{path} not found"}
        return {"ok": False, "error": "read scope: frontend/, backend/, plan.md"}

    def delete_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path = a.get("path", "")
        dest = safe_join(WORKSPACE, path) if path.startswith(prefixes) else None
        if dest is None or not os.path.isfile(dest):
            return {"ok": False, "error": f"cannot delete {path}"}
        os.remove(dest)
        upsert_file(dest, ctx.task_id, "delete")
        emit({"type": "files", "task_id": ctx.task_id,
              "files": [{"path": dest, "action": "delete"}]})
        return {"ok": True, "deleted": path}

    return {"write_file": write_file, "write_files": write_files,
            "edit_file": edit_file, "read_file": read_file,
            "delete_file": delete_file}


def _terminal_tool(ctx: AgentContext, cwd: str) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    def terminal(a: Dict[str, Any]) -> Dict[str, Any]:
        cmd = str(a.get("command") or a.get("cmd") or "").strip()
        if not cmd:
            return {"ok": False, "error": "command required"}
        if any(bad in cmd for bad in ("rm -rf /", "mkfs", "shutdown", "reboot")):
            return {"ok": False, "error": "command rejected (unsafe)"}
        out = shell_run(cmd, cwd, timeout=240)
        ok = out["exit_code"] == 0
        text = f"exit {out['exit_code']}\n{out['stdout'] or out['stderr']}"[:2800]
        return {"ok": ok, "output": text}
    return terminal


def _browser_toolset(ctx: AgentContext) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    def browser_tool(a: Dict[str, Any]) -> Dict[str, Any]:
        if browser_engine is None:
            return {"ok": False, "error": "browser engine unavailable"}
        action = str(a.get("action") or "").strip().lower()
        if action == "navigate":
            return browser_engine.navigate(str(a.get("url") or "http://localhost:3000"))
        if action in ("console_spy", "console"):
            return browser_engine.console_spy()
        if action == "interact":
            return browser_engine.interact(str(a.get("selector") or ""),
                                           str(a.get("do") or a.get("action2") or "click"),
                                           str(a.get("value") or ""))
        if action == "screenshot":
            return browser_engine.screenshot(str(a.get("filename") or ""),
                                             str(a.get("question") or ""))
        if action == "snapshot":
            # a11y tree of the CURRENT page (common model shorthand)
            return browser_engine.navigate("about:blank" if False else _current_url())
        return {"ok": False, "error": "action must be navigate|console_spy|interact|screenshot"}

    def _current_url() -> str:
        try:
            if browser_engine._page is not None:
                return browser_engine._page.url or "http://localhost:3000"
        except Exception:  # noqa: BLE001
            pass
        return "http://localhost:3000"

    # Top-level aliases — models intermittently call these directly instead
    # of through browser_tool (live: {"tool":"console_spy"}).
    def console_spy(_: Dict[str, Any]) -> Dict[str, Any]:
        if browser_engine is None:
            return {"ok": False, "error": "browser engine unavailable"}
        return browser_engine.console_spy()

    def navigate(a: Dict[str, Any]) -> Dict[str, Any]:
        if browser_engine is None:
            return {"ok": False, "error": "browser engine unavailable"}
        return browser_engine.navigate(str(a.get("url") or "http://localhost:3000"))

    def interact(a: Dict[str, Any]) -> Dict[str, Any]:
        if browser_engine is None:
            return {"ok": False, "error": "browser engine unavailable"}
        return browser_engine.interact(str(a.get("selector") or ""),
                                       str(a.get("do") or "click"),
                                       str(a.get("value") or ""))

    def screenshot(a: Dict[str, Any]) -> Dict[str, Any]:
        if browser_engine is None:
            return {"ok": False, "error": "browser engine unavailable"}
        return browser_engine.screenshot(str(a.get("filename") or ""),
                                         str(a.get("question") or ""))

    return {"browser_tool": browser_tool, "console_spy": console_spy,
            "navigate": navigate, "interact": interact, "screenshot": screenshot}


def _skills_toolset(role: str) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    if _skills_server_mod is None:
        return {}
    def mcp_list_skills(_: Dict[str, Any]) -> Dict[str, Any]:
        return _skills_server_mod.mcp_list_tools(role)
    def mcp_use_skill(a: Dict[str, Any]) -> Dict[str, Any]:
        return _skills_server_mod.mcp_call_tool(
            role, str(a.get("skill") or a.get("name") or ""),
            str(a.get("input") or a.get("question") or ""))
    return {"mcp_list_skills": mcp_list_skills, "mcp_use_skill": mcp_use_skill}


def _mailbox_toolset(ctx: AgentContext) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    # NOTE: the DB helpers are mailbox_db_send/mailbox_db_read — the tool
    # fns below intentionally shadow the short names without breaking them.
    def mailbox_send(a: Dict[str, Any]) -> Dict[str, Any]:
        to = str(a.get("to") or "").strip().lower()
        if to not in ("backend", "frontend", "chief"):
            return {"ok": False, "error": "to must be backend|frontend|chief"}
        msg = str(a.get("message") or a.get("text") or "").strip()
        if not msg:
            return {"ok": False, "error": "message required"}
        mailbox_db_send(ctx.task_id, ctx.agent_name, to, msg)
        return {"ok": True, "delivered_to": to}

    def mailbox_read(_: Dict[str, Any]) -> Dict[str, Any]:
        msgs = mailbox_db_read(ctx.task_id, ctx.agent_name)
        return {"ok": True, "messages": msgs[-10:]}
    return {"mailbox_send": mailbox_send, "mailbox_read": mailbox_read}


def _verify_file_tool(ctx: AgentContext) -> Callable[[Dict[str, Any]], Dict[str, Any]]:
    """verify_file(path) — the LSP-backed spellchecker. Agents MUST call it
    on every file they write; errors are fixed before done may be declared."""
    def verify_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path = str(a.get("path") or a.get("file") or a.get("filename") or "").strip()
        if not path:
            return {"ok": False, "error": "path required"}
        if not path.startswith(("frontend/", "backend/")):
            return {"ok": False, "error": "path must be under frontend/ or backend/"}
        return LSP_CLIENT.verify_file(path)
    return verify_file


def build_backend_toolset(ctx: AgentContext) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    tools: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}
    tools.update(_fs_toolset(ctx, "backend", ("backend",)))
    tools["terminal"] = _terminal_tool(ctx, os.path.join(WORKSPACE, "backend"))

    def lsp(_: Dict[str, Any]) -> Dict[str, Any]:
        return {"ok": True, "diagnostics": lsp_diagnostics("backend")}
    tools["lsp_diagnostics"] = lsp
    tools["verify_file"] = _verify_file_tool(ctx)

    def api_contract_update(a: Dict[str, Any]) -> Dict[str, Any]:
        contract = a.get("contract")
        if not isinstance(contract, (dict, list)):
            # allow a JSON string too
            if isinstance(a.get("contract_json"), str):
                try:
                    contract = json.loads(a["contract_json"])
                except json.JSONDecodeError:
                    return {"ok": False, "error": "contract_json is not valid JSON"}
            else:
                return {"ok": False, "error": "contract object required"}
        os.makedirs(os.path.dirname(API_CONTRACT_PATH), exist_ok=True)
        with open(API_CONTRACT_PATH, "w", encoding="utf-8") as fh:
            json.dump(contract, fh, indent=2)
        mailbox_db_send(ctx.task_id, "backend", "frontend",
                     "API CONTRACT PUBLISHED — read it with api_contract_read.")
        ctx.activity("API contract published", "done")
        return {"ok": True, "path": ".system/api_contract.json"}

    tools["api_contract_update"] = api_contract_update
    tools.update(_mailbox_toolset(ctx))
    tools.update(_skills_toolset("backend"))
    return tools


def build_frontend_toolset(ctx: AgentContext) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    tools: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}
    tools.update(_fs_toolset(ctx, "frontend", ("frontend",)))
    tools["terminal"] = _terminal_tool(ctx, os.path.join(WORKSPACE, "frontend"))

    def lsp(_: Dict[str, Any]) -> Dict[str, Any]:
        return {"ok": True, "diagnostics": lsp_diagnostics("frontend")}
    tools["lsp_diagnostics"] = lsp
    tools["verify_file"] = _verify_file_tool(ctx)

    def api_contract_read(_: Dict[str, Any]) -> Dict[str, Any]:
        ctx.contract_reads += 1
        if not os.path.exists(API_CONTRACT_PATH):
            return {"ok": True, "contract": None,
                    "note": "No backend contract exists — this app appears to be "
                            "frontend-only per the plan. Do not invent API URLs."}
        with open(API_CONTRACT_PATH, "r", encoding="utf-8") as fh:
            return {"ok": True, "contract": json.load(fh)}

    tools["api_contract_read"] = api_contract_read
    tools.update(_browser_toolset(ctx))
    tools.update(_mailbox_toolset(ctx))
    tools.update(_skills_toolset("frontend"))

    # The Integration Link guard: writing fetch/API code before reading the
    # contract earns an explicit warning in the tool result.
    _orig_write = tools["write_file"]

    def guarded_write(a: Dict[str, Any]) -> Dict[str, Any]:
        r = _orig_write(a)
        if r.get("ok") and ctx.contract_reads == 0:
            content = str(a.get("content", ""))
            if re.search(r"""(fetch\(|axios|localhost:\d+|/api/)""", content) \
                    and os.path.exists(API_CONTRACT_PATH):
                r["warning"] = ("INTEGRATION LINK: you wrote API-calling code but have NOT "
                                "read the backend contract this task. Call api_contract_read "
                                "and align your URLs — guessing endpoints is forbidden.")
        return r

    tools["write_file"] = guarded_write
    return tools


def build_debugger_toolset(ctx: AgentContext) -> Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]]:
    tools: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}
    tools.update(_browser_toolset(ctx))

    def read_file(a: Dict[str, Any]) -> Dict[str, Any]:
        path = str(a.get("path") or "")
        if path == "plan.md":
            if os.path.exists(PLAN_PATH):
                with open(PLAN_PATH, "r", encoding="utf-8", errors="replace") as fh:
                    return {"ok": True, "content": fh.read()[:8000]}
            return {"ok": False, "error": "plan.md not found"}
        if path.startswith(("frontend/", "backend/")):
            dest = safe_join(WORKSPACE, path)
            if dest and os.path.exists(dest):
                with open(dest, "r", encoding="utf-8", errors="replace") as fh:
                    return {"ok": True, "content": fh.read()[:5000]}
            return {"ok": False, "error": f"{path} not found"}
        return {"ok": False, "error": "read scope: plan.md, frontend/*, backend/*"}

    tools["read_file"] = read_file
    return tools


# ---------------------------------------------------------------------------
# THE SUB-AGENTS
# ---------------------------------------------------------------------------

BACKEND_SYSTEM = """You are the Backend Agent of ArcForge — an autonomous backend developer working inside a Linux VM.
HARD RULES:
- You NEVER touch frontend/ files and never use a browser. Your world is backend/ + the API contract.
- The approved plan.md is the binding contract. Implement exactly what it specifies.
- YOUR TASK IS NOT DONE until you have called api_contract_update with your EXACT endpoints (method, path, description, response) and port — the Frontend Agent is forbidden from guessing and depends on the contract.
- CODE VERIFICATION IS MANDATORY: after writing any code you MUST call verify_file {path:"backend/<file>"} for EVERY file you wrote. If errors are returned, fix them immediately before marking the task complete. Only declare done when verify_file reports no errors.
WORKFLOW: read the plan → write the code (batch files in ONE write_files call) → verify_file on every file you wrote → fix any reported errors → install deps + start the server via terminal (nohup, background) → smoke-test endpoints with curl → api_contract_update {contract:{base_url,port,endpoints:[{method,path,description,response}]}} → mailbox_send to frontend (contract ready) → done.
BACKEND CHOICE: Python Flask (requirements.txt + app.py, port 8000, enable CORS for localhost:3000) unless the plan says otherwise.
FLASK 3 BANS (recurring live failures — the app crashes at boot): NEVER use @app.before_first_request (removed in Flask 3 — initialise the DB at module scope instead), NEVER use @app.route without explicit methods when you expect JSON bodies, and NEVER call json fields that don't exist.
If this is a follow-up, preserve existing behaviour — modify only what the task requires."""

FRONTEND_SYSTEM = """You are the Frontend Agent of ArcForge — an autonomous frontend developer working inside a Linux VM.
HARD RULES:
- You NEVER touch backend/ files. Your world is frontend/ (Next.js 14 App Router + TypeScript — the pinned scaffold with deps already exists).
- FILE LAYOUT (critical): the home page MUST be frontend/app/page.tsx — the Next.js App Router only serves files under frontend/app/ (page.tsx, layout.tsx, route.ts). NEVER create App.tsx, index.tsx, index.html, or src/ — those are NOT served and the app will 404.
- INTEGRATION LINK (law): you are FORBIDDEN from guessing API URLs. If the plan has a backend, call api_contract_read FIRST and write frontend/lib/api_client.ts from the contract. Every fetch goes through that client.
- CODE VERIFICATION IS MANDATORY: after writing any code you MUST call verify_file {path:"frontend/<file>"} for EVERY file you wrote. If errors are returned, fix them immediately before marking the task complete.
- After writing code: verify_file on every file, then lsp_diagnostics (tsc) until clean; start the dev server via terminal: nohup npx next dev -p 3000 -H 0.0.0.0 & ; then VERIFY with browser_tool: navigate http://localhost:3000 → console_spy → fix every error (yours) or report it to the backend agent (mailbox_send: what you called, what you expected, what you got). You cannot report done while console errors exist OR while http://localhost:3000 shows a 404.
UI STANDARDS: inline styles or one <style> tag only (NO .css files); every file using hooks/handlers starts with 'use client'; Next 14 <Link> takes NO nested <a>; no lorem ipsum — realistic copy; responsive; loading + empty states.
If this is a follow-up, preserve existing behaviour — modify only what the task requires."""

DEBUGGER_SYSTEM = """You are the Debugger Agent of ArcForge — the QA gate. You NEVER write or edit code. You audit the LIVE app against plan.md and report exactly WHAT IS MISSING.
TOOLS: browser_tool (navigate / console_spy / interact / screenshot) and read_file (plan.md, frontend/*, backend/*).
THE PRIMARY QUESTION: what does the plan require that the app does NOT have yet?
WORKFLOW:
1. You already have the plan — extract EVERY feature and acceptance criterion it specifies (UI pages, components, interactions, data flows).
2. browser_tool navigate http://localhost:3000 — inspect the accessibility tree.
3. For EACH plan feature: interact (click/type) to exercise it, then console_spy. Classify it:
   - "present": the UI shows the expected outcome AND no console/network errors fire;
   - "partial": part of it exists but a required piece is absent (name the piece);
   - "missing": the app has none of it.
4. Screenshot key screens when a visual check matters.
FINISH with: {"tool":"done","report":"<evidence-based summary>","status":"pass"|"fail","coverage":[{"feature":"<plan feature>","state":"present|partial|missing","evidence":"what you actually saw","suspect":"frontend|backend|unclear"}],"issues":[{"criterion":"...","observation":"what actually happened","suspect":"frontend|backend|unclear"}]}.
Rules: status is "pass" ONLY when every plan feature is "present". An app that boots but lacks a plan feature is a FAIL — name the feature. Report only real, observed evidence — never speculation. Your missing-features checklist is what the build acts on next: precise feature names, exact gaps."""


def run_backend_agent(task_id: str, task: str, plan_text: str) -> Dict[str, Any]:
    ctx = AgentContext(task_id, "backend")
    ctx.activity("Setting up the backend service", "active", task[:120])
    plan_excerpt = plan_text[:2600]
    system = BACKEND_SYSTEM.replace("{plan}", "")
    # Repo map (tree-sitter skeleton) instead of a raw file dump — cheaper
    # tokens, better focus; full files stay one read_file away.
    repo_map = generate_repo_map()
    user = (f"TASK:\n{task}\n\n"
            f"APPROVED PLAN (plan.md):\n{plan_excerpt}\n\n"
            f"CURRENT CODEBASE SKELETON (verify_file any file you touch):\n"
            f"{repo_map or workspace_tree_text() or '(empty — you are building from scratch)'}")
    pace_for_tpm()
    result = run_agent_loop(ctx, system, user, build_backend_toolset(ctx),
                            max_tokens=12000, model=BACKEND_MODELS)

    # STRUCTURAL POST-CHECK — the Integration Link is not optional: if the
    # agent wrote backend code but never published the contract, the
    # Frontend Agent is stranded (it may not guess endpoints). One pointed
    # repair round; if it still fails, the fit check/debugger surface it.
    has_backend_code = any(rel.startswith("backend/") for rel in _iter_source_files())
    if has_backend_code and not os.path.exists(API_CONTRACT_PATH):
        ctx.activity("Publishing the API contract", "active",
                     "the contract is required before integration")
        repair = ("STRUCTURAL CHECK FAILED: you wrote backend code but never "
                  "published the API contract — the Frontend Agent CANNOT "
                  "integrate without it. Call api_contract_update NOW with "
                  "{contract:{base_url,port,endpoints:[{method,path,description,"
                  "response}]}} describing the server you actually wrote "
                  "(read your files with read_file if unsure). Then done.")
        pace_for_tpm()
        result = run_agent_loop(ctx, system, repair, build_backend_toolset(ctx),
                                max_steps=6, max_tokens=8000,
                                model=BACKEND_MODELS)
        if not os.path.exists(API_CONTRACT_PATH):
            ctx.say("The API contract is still missing after enforcement — "
                    "the integration check will report the gap")

    ctx.activity("Backend service ready", "done",
                 str(result.get("report", ""))[:200])
    ctx.say(f"Backend update: {str(result.get('report',''))[:400]}")
    return result


def run_frontend_agent(task_id: str, task: str, plan_text: str,
                       fit_check: bool = False) -> Dict[str, Any]:
    ctx = AgentContext(task_id, "frontend")
    if fit_check:
        ctx.activity("Checking the app in the browser", "active",
                     "proving the frontend and backend fit together")
    else:
        ctx.activity("Building the interface", "active", task[:120])
    system = FRONTEND_SYSTEM
    if fit_check:
        user = (
            "INTEGRATION FIT CHECK — both servers are running. Your job: prove "
            "frontend and backend fit like a puzzle.\n"
            f"APPROVED PLAN (for reference):\n{plan_text[:1800]}\n\n"
            "STEPS: browser_tool navigate http://localhost:3000 → console_spy → "
            "for EVERY error decide with profound evidence whether it is YOURS "
            "(fix via write_file, then re-navigate) or the BACKEND's (mailbox_send "
            "to backend: exactly what you called, what you expected, what you got). "
            "Use interact on 1-2 key flows to confirm real data renders. "
            "Finish with done + a report stating: page status, console errors "
            "(or none), data integration verdict, any mailbox messages sent."
        )
    else:
        repo_map = generate_repo_map()
        user = (f"TASK:\n{task}\n\n"
                f"APPROVED PLAN (plan.md):\n{plan_text[:2600]}\n\n"
                f"CURRENT CODEBASE SKELETON (verify_file any file you touch):\n"
                f"{repo_map or workspace_tree_text() or '(scaffold only — you are building from scratch)'}")
    pace_for_tpm()
    result = run_agent_loop(ctx, system, user, build_frontend_toolset(ctx),
                            max_tokens=12000, model=FRONTEND_MODELS)

    # STRUCTURAL POST-CHECK — Next.js App Router only serves frontend/app/*.
    # If the agent put the UI anywhere else (App.tsx — observed live: the app
    # 404'd), one pointed repair round moves it into place.
    if not fit_check and not os.path.exists(os.path.join(WORKSPACE, "frontend", "app", "page.tsx")):
        stray = [rel for rel in _iter_source_files()
                 if rel.startswith("frontend/") and not rel.startswith("frontend/app/")
                 and rel.endswith((".tsx", ".ts")) and "lib/" not in rel and "components/" not in rel]
        ctx.activity("Fixing the app layout", "active",
                     "frontend/app/page.tsx missing — the app would 404")
        repair = ("STRUCTURAL CHECK FAILED: frontend/app/page.tsx does not "
                  "exist, so Next.js serves a 404 at '/'. The App Router ONLY "
                  "serves files under app/ (page.tsx per route). "
                  + (f"You wrote the UI into {', '.join(stray[:3])} — that is NOT served. " if stray else "")
                  + "Create frontend/app/page.tsx now with the full home page "
                    "(you may import your existing components), then done.")
        pace_for_tpm()
        result = run_agent_loop(ctx, system, repair, build_frontend_toolset(ctx),
                                max_steps=6, max_tokens=9000,
                                model=FRONTEND_MODELS)

    ctx.activity("Interface ready", "done",
                 str(result.get("report", ""))[:200])
    ctx.say(f"Interface update: {str(result.get('report',''))[:400]}")
    return result


def _deterministic_issues() -> List[Dict[str, str]]:
    """Orchestrator-side probes used when the Debugger agent fails to
    produce actionable issues — the chief ALWAYS gets evidence to delegate
    (live bug: the debugger burned its steps, the chief got 'fail' with no
    issues, repairs=0 while the app 500'd and the backend crashed)."""
    issues: List[Dict[str, str]] = []
    # 1) Frontend probe — navigate / and surface the dev-log reason on >=400;
    #    a refused connection (status 0) means the dev server is DOWN (live
    #    2026-08-27: the server-down case flowed through the gap path as
    #    "App must be running" instead of a crash-class start-the-server fix).
    if browser_engine is not None:
        nav = browser_engine.navigate(f"http://localhost:{NEXT_DEV_PORT}")
        status = int(nav.get("http_status") or 0)
        if status >= 400:
            hint = str(nav.get("server_error_hint") or "")
            first_err = next((ln.strip() for ln in hint.splitlines()
                              if "Error" in ln or "error" in ln.lower()
                              and "at " not in ln), "")[:220]
            issues.append({
                "criterion": "App loads at '/'",
                "observation": f"Frontend serves HTTP {status}. "
                               + (first_err or hint[:200]),
                "suspect": "frontend",
            })
        elif status == 0:
            issues.append({
                "criterion": "App loads at '/'",
                "observation": (f"The frontend dev server is NOT running — "
                                f"http://localhost:{NEXT_DEV_PORT} refuses "
                                "connections. Start it (nohup npx next dev "
                                f"-p {NEXT_DEV_PORT} -H 0.0.0.0 & from "
                                "frontend/) and confirm it answers before done."),
                "suspect": "frontend",
            })
    # 2) Backend probe — curl the first contracted GET endpoint.
    if os.path.exists(API_CONTRACT_PATH):
        try:
            with open(API_CONTRACT_PATH, "r", encoding="utf-8") as fh:
                contract = json.load(fh)
            eps = [e for e in (contract.get("endpoints") or [])
                   if isinstance(e, dict) and str(e.get("method", "GET")).upper() == "GET"]
            base = str(contract.get("base_url") or "http://localhost:8000").rstrip("/")
            if eps:
                path = str(eps[0].get("path", "/"))
                probe = shell_run(f"curl -s -o /dev/null -w '%{{http_code}}' "
                                  f"{base}{path} --max-time 8", WORKSPACE, 20)
                code = str(probe.get("stdout", "")).strip()
                if code in ("000", ""):
                    crash = shell_run("tail -n 15 /tmp/backend-dev.log 2>/dev/null",
                                      WORKSPACE, 10)
                    tb = [ln for ln in (crash.get("stdout") or "").splitlines()
                          if ln.strip()][-4:]
                    issues.append({
                        "criterion": f"GET {path} answers",
                        "observation": "Backend unreachable on " + base
                                       + (". Traceback: " + " | ".join(tb)[:260] if tb else ""),
                        "suspect": "backend",
                    })
                elif code.startswith("5"):
                    issues.append({
                        "criterion": f"GET {path} answers",
                        "observation": f"Backend returns HTTP {code} on {path}",
                        "suspect": "backend",
                    })
        except Exception:  # noqa: BLE001 — probes are best-effort
            pass
    return issues


def run_debugger_agent(task_id: str, plan_text: str) -> Dict[str, Any]:
    ctx = AgentContext(task_id, "debugger")
    ctx.activity("Verifying the app in the browser", "active",
                 "checking the live app against every plan feature")
    # The plan rides IN the prompt — the agent no longer burns its step
    # budget on repeated read_file(plan.md) calls (live: one run spent 7
    # consecutive steps re-reading the same file and hit the limit).
    user = ("Audit the live app at http://localhost:3000 against this APPROVED "
            "PLAN (you already have it — do NOT re-read plan.md; go straight "
            "to the browser). Your PRIMARY question: what does the plan "
            "require that the app does NOT have yet? Walk EVERY plan feature, "
            "classify each present/partial/missing with evidence, then give "
            "your verdict.\n\n"
            f"{plan_text[:3200]}\n")
    pace_for_tpm()
    result = run_agent_loop(ctx, DEBUGGER_SYSTEM, user,
                            build_debugger_toolset(ctx), max_steps=18,
                            max_tokens=4000, model=DEBUGGER_MODELS)
    report = str(result.get("report", ""))
    status = "pass" if (result.get("status") == "pass" or
                        re.search(r"\bstatus\D{0,12}pass\b", report, re.I)) else "fail"
    issues = result.get("issues")
    if not isinstance(issues, list):
        issues = []
    # THE PLAN-COVERAGE CHECKLIST — the debugger's primary product: exactly
    # what the plan requires that the app lacks. The chief acts on THIS.
    coverage = [c for c in (result.get("coverage") or [])
                if isinstance(c, dict)
                and str(c.get("feature", "")).strip()]
    missing = [c for c in coverage
               if str(c.get("state", "missing")).strip().lower() != "present"]
    if status == "fail":
        # DETERMINISTIC FALLBACK — probe the servers ourselves so the chief
        # always receives actionable, evidence-based issues.
        det = _deterministic_issues()
        seen = {json.dumps(i, sort_keys=True) for i in issues}
        for i in det:
            if json.dumps(i, sort_keys=True) not in seen:
                issues.append(i)
        if not issues and not missing:
            issues = [{"criterion": "overall", "observation": report[:400],
                       "suspect": "unclear"}]
        # No coverage list produced → the UNATTRIBUTED issues become the gap
        # report (attributed crash evidence already rides verbatim to its
        # owner in _fix_delegations — no double delegation).
        if not missing:
            missing = [{"feature": str(i.get("criterion", "unknown"))[:200],
                        "state": "missing",
                        "evidence": str(i.get("observation", ""))[:400],
                        "suspect": str(i.get("suspect", "unclear"))}
                       for i in issues
                       if isinstance(i, dict)
                       and str(i.get("suspect", "")).lower()
                       not in ("frontend", "backend")]
    if status == "pass" and missing:
        # Inconsistent verdict: features marked missing cannot be a pass.
        status = "fail"
    ctx.activity("Verification complete", "done",
                 (f"FAIL — missing: "
                  + "; ".join(str(m.get("feature", "?"))[:60]
                               for m in missing[:6]))
                 if status == "fail" and missing else status.upper())
    if status == "fail" and missing:
        feats = "; ".join(str(m.get("feature", "?"))[:60] for m in missing[:4])
        ctx.say(f"Verification: FAIL — missing from the plan: {feats}")
    else:
        ctx.say(f"Verification: {status.upper()} — {report[:400]}")
    return {"status": status, "issues": issues, "missing": missing,
            "coverage": coverage, "report": report}


# ---------------------------------------------------------------------------
# Server launch helpers (the deterministic "Fit Check" plumbing)
# ---------------------------------------------------------------------------


def ensure_servers_up(task_id: str) -> Dict[str, Any]:
    """Deterministically bring up the frontend + backend dev servers so the
    Fit Check and the Debugger test a LIVE app. Logs everything honestly."""
    out: Dict[str, Any] = {"frontend": False, "backend": False, "app_port": None}

    def sh(cmd: str, cwd: str, timeout: int = 240) -> str:
        r = shell_run(cmd, cwd, timeout)
        append_log(task_id, "orchestrator", "info",
                   f"$ {cmd}\n{r['stdout'] or r['stderr']}"[:1200])
        return r["stdout"] + r["stderr"]

    fe = os.path.join(WORKSPACE, "frontend")
    if os.path.exists(os.path.join(fe, "package.json")):
        sh("npm install --no-audit --no-fund --loglevel=error", fe, 600)
        sh(f"fuser -k {NEXT_DEV_PORT}/tcp 2>/dev/null; sleep 1", fe, 15)
        sh("rm -rf .next node_modules/.cache", fe, 60)
        sh(f"nohup npx next dev -p {NEXT_DEV_PORT} -H 0.0.0.0 "
           f"> /tmp/frontend-dev.log 2>&1 < /dev/null &", fe, 20)
        for _ in range(6):
            sh("sleep 5", fe, 10)
            code = sh(f"curl -s -o /dev/null -w '%{{http_code}}' "
                      f"http://localhost:{NEXT_DEV_PORT}/ --max-time 20", fe, 30)
            if code.strip().startswith(("2", "3")):
                out["frontend"] = True
                out["app_port"] = NEXT_DEV_PORT
                break
    be = os.path.join(WORKSPACE, "backend")
    if os.path.isdir(be):
        if os.path.exists(os.path.join(be, "requirements.txt")):
            sh("pip install -q -r requirements.txt 2>&1 | tail -n 2", be, 420)
        # Kill ANY stale backend process first — a leftover server from an
        # earlier round keeps serving OLD code on :8000 while the new one
        # fails to bind silently (live bug: 2 repair rounds rewrote app.py
        # but curl kept hitting the stale process).
        sh("fuser -k 8000/tcp 2>/dev/null; sleep 1", be, 15)
        # Flask/Python backend (the platform's preferred choice) takes
        # precedence over any leftover Node backend from older rounds.
        started = False
        for entry in ("app.py", "main.py", "server.py"):
            if os.path.exists(os.path.join(be, entry)):
                sh(f"nohup python3 {entry} > /tmp/backend-dev.log 2>&1 < /dev/null &",
                   be, 20)
                started = True
                break
        if not started:
            be_pkg = os.path.join(be, "package.json")
            if os.path.exists(be_pkg):
                sh("npm install --no-audit --no-fund --loglevel=error", be, 600)
                try:
                    with open(be_pkg, "r", encoding="utf-8") as fh:
                        bpkg = json.load(fh)
                    bscript = ("start" if "start" in (bpkg.get("scripts") or {})
                               else "dev" if "dev" in (bpkg.get("scripts") or {}) else None)
                except Exception:  # noqa: BLE001
                    bscript = None
                if bscript:
                    sh(f"nohup npm run {bscript} > /tmp/backend-dev.log 2>&1 < /dev/null &",
                       be, 20)
                    started = True
        if started:
            time.sleep(3)
            crash = shell_run("tail -n 20 /tmp/backend-dev.log 2>/dev/null", be, 15)
            txt = crash["stdout"]
            if "Traceback (most recent call last)" in txt:
                append_log(task_id, "orchestrator", "warn",
                           f"backend crashed at boot: {txt[-500:]}")
            else:
                out["backend"] = True
    if out["app_port"]:
        set_status("app", {"port": out["app_port"], "up": True, "task_id": task_id})
        emit({"type": "status", "status": get_status("app")})
    return out


# ---------------------------------------------------------------------------
# THE CHIEF AGENT — gateway, planner, orchestrator
# ---------------------------------------------------------------------------

CHIEF_CLASSIFY_SYSTEM = """You are ArcForge — the user's build agent. Your FIRST job is routing their message:
- "answer": a question / chat / explanation that needs NO code changes (e.g. "What is React?", "Why use TypeScript?"). You answer it directly — the build pipeline does NOT run.
- "app": a request to build, modify, extend or fix an application — it goes through the plan + approval + build pipeline.
Reply with ONLY JSON:
{"kind":"answer","reply":"<your direct answer to the user>"}
or
{"kind":"app","intent":"<one line: what they want built/changed>"}"""

CHIEF_PLAN_SYSTEM = """You are ArcForge — the user's build agent. Draft a build plan in Markdown that the user will review and approve. Once approved it becomes plan.md — the binding contract for the whole build.
Required structure (Markdown):
# <App Name>
## Overview
2-3 sentences: what the app is and does.
## Pages & UI
Each page/screen with its key UI elements and the styling direction.
## Data & API
The data model and EVERY API endpoint the backend will expose (method, path, purpose, response shape) — or "No backend needed (static/frontend-only)" if the app needs no server.
## Components
File layout: frontend/ (Next.js 14 App Router + TypeScript — mandated) and backend/ (your chosen language/framework, only if needed).
## Acceptance Criteria
Numbered, concrete, testable statements the Debugger will click through on the live app (e.g. "1. Typing text and pressing Add creates a new item in the list").
Rules: specific enough to build from; no code; keep it under ~90 lines.
RUNTIME CONSTRAINTS (hard): the app runs inside a single Linux VM — a backend, if needed, must be Python Flask or Node Express with in-memory storage or SQLite ONLY (no MongoDB/Postgres/Redis or any external service — none exist in the VM). The frontend is Next.js 14 App Router + TypeScript, always."""

CHIEF_REFINE_SYSTEM = """You are ArcForge — the user's build agent. The user read your proposed plan and REJECTED it with change requests. Apply their changes and produce the FULL revised plan (same structure: Overview / Pages & UI / Data & API / Components / Acceptance Criteria). Keep everything they did NOT ask to change intact. The revised plan goes back to the user for approval."""

CHIEF_DISPATCH_SYSTEM = """You are ArcForge — the user's build agent. You coordinate specialised sub-agents as YOUR TOOLS: you decide which to call, and each call carries a precise brief. Given the approved plan, the repo map, and what has already been done, decide the NEXT tool call.
Your tools:
- backend_agent: builds the API server under backend/ — call it when the plan defines endpoints that aren't implemented yet.
- frontend_agent: builds the UI under frontend/ (Next.js 14 App Router) — call it when the UI isn't built or needs changes.
- integration_check: starts both servers and proves the frontend talks to the backend — call it ONCE after both agents ran (only when a backend exists).
- qa_verification: end-to-end verifies the live app against the plan's Acceptance Criteria — call it when the build looks complete.
- finish: everything the plan requires is done — no more tool calls.
Reply ONLY JSON:
{"tool":"<backend_agent|frontend_agent|integration_check|qa_verification|finish>","task":"<precise brief for that tool: what to build/fix, referencing concrete plan details — endpoints with paths, pages with elements, file names>","reason":"<one line, internal>"} (omit "task" for finish)."""

CHIEF_TRIAGE_SYSTEM = """You are ArcForge — the user's build agent. Verification audited the live app against the approved plan and reported WHAT IS MISSING. You decide which of your tools builds what.
RULES:
- The MISSING FEATURES list is your work queue: every missing or partial plan feature MUST become a delegation that BUILDS it. Quote the plan requirement, state exactly what is absent, and instruct to build ONLY the missing pieces — never rebuild what already works.
- Crash-class evidence (HTTP errors, tracebacks) is routed verbatim to the owning tool elsewhere — do not re-delegate it.
- Batch related missing features for the same tool into ONE dense delegation; keep the total low.
- Do not delegate anything that is not actionable.
Reply ONLY JSON:
{"delegations":[{"agent":"frontend"|"backend","task":"<precise build/fix instruction>"}]}
or {"delegations":[]} when nothing actionable remains."""

CHIEF_SUMMARY_SYSTEM = """You are ArcForge — the user's ONE build agent (you coordinate internal tools, but the user only ever talks to you). The build finished. Write the user-facing completion message in FIRST PERSON ("I built…", "I verified…"): 2-4 sentences, plain language, stating what the app does, whether it passed end-to-end verification against the approved plan, and anything worth trying in the preview. If verification failed, NAME the plan features that are still missing so the user knows exactly what remains. NEVER mention sub-agents, "chief", "swarm", orchestration, or internal mechanics. No markdown headers."""


class ChiefAgent:
    """The Gateway — receives every prompt, classifies, plans, seeks approval,
    dispatches the swarm. NEVER writes application code."""

    def __init__(self, task_id: str) -> None:
        self.ctx = AgentContext(task_id, "chief")

    # -- step 1: classify -------------------------------------------------
    def classify(self, prompt: str) -> Dict[str, Any]:
        history = recent_chat(12)
        convo = "\n".join(f"{m['role']}: {m['content'][:300]}" for m in history[-6:])
        user = f"Recent conversation:\n{convo}\n\nNEW USER MESSAGE:\n{prompt}"
        try:
            pace_for_tpm()
            data = _extract_json(llm_chat(
                [{"role": "system", "content": CHIEF_CLASSIFY_SYSTEM},
                 {"role": "user", "content": user}],
                json_mode=True, max_tokens=1100, model=CHIEF_MODELS))
            if data.get("kind") == "answer" and str(data.get("reply", "")).strip():
                return {"kind": "answer", "reply": str(data["reply"])}
            if data.get("kind") == "app":
                return {"kind": "app", "intent": str(data.get("intent", prompt[:120]))}
        except Exception as exc:  # noqa: BLE001
            append_log(self.ctx.task_id, "chief", "warn",
                       f"classify degraded: {exc} — defaulting to app")
        return {"kind": "app", "intent": prompt[:120]}

    # -- step 2: plan -------------------------------------------------------
    def draft_plan(self, prompt: str) -> str:
        follow_up = workspace_has_source()
        history = recent_chat(12)
        convo = "\n".join(f"{m['role']}: {m['content'][:250]}" for m in history[-6:])
        user = f"Conversation so far:\n{convo}\n\nUSER REQUEST:\n{prompt}\n"
        if follow_up:
            repo_map = generate_repo_map()
            user += (f"\nEXISTING CODEBASE SKELETON (this is a follow-up — the "
                     f"plan must preserve and extend it):\n"
                     f"{repo_map or workspace_tree_text()}\n")
        pace_for_tpm()
        return llm_chat(
            [{"role": "system", "content": CHIEF_PLAN_SYSTEM},
             {"role": "user", "content": user}],
            json_mode=False, max_tokens=3600, model=CHIEF_MODELS).strip()

    def refine_plan(self, plan: str, feedback: str) -> str:
        # The exact injection template the user mandated.
        user = (f'The user said: "I have read through the plan. Make the '
                f'following change(s): {feedback}"\n\nCURRENT PLAN:\n{plan}\n\n'
                "Produce the full revised plan now.")
        pace_for_tpm()
        return llm_chat(
            [{"role": "system", "content": CHIEF_REFINE_SYSTEM},
             {"role": "user", "content": user}],
            json_mode=False, max_tokens=3600, model=CHIEF_MODELS).strip()

    # -- step 3: the dispatch decision lives in agent_dispatcher (the graph's
    #    chief_node) — sub-agents are the chief's TOOLS; one LLM call picks the
    #    tool AND writes its brief.

    # -- step 4: triage QA gaps into build/fix delegations -----------------
    def triage(self, plan_text: str, debug_report: Dict[str, Any],
               mailbox_msgs: List[Dict[str, Any]]) -> List[Dict[str, str]]:
        """THE CHIEF'S DECISION after a failed QA round: which tool builds
        what's missing. Receives the debugger's missing-features checklist
        (the work queue) plus any unattributed failures; crash-class
        evidence is routed verbatim elsewhere and excluded here so nothing
        is delegated twice."""
        missing = [m for m in (debug_report.get("missing") or [])
                   if isinstance(m, dict)]
        issues = [i for i in (debug_report.get("issues") or [])
                  if isinstance(i, dict)
                  and str(i.get("suspect", "")).lower()
                  not in ("frontend", "backend")]
        mail = "\n".join(f"{m['from']}: {m['message'][:200]}"
                         for m in mailbox_msgs[-8:])
        user = (f"APPROVED PLAN (excerpt):\n{plan_text[:1600]}\n\n"
                f"DEBUGGER VERDICT: {debug_report.get('status','?')}\n\n"
                f"MISSING FROM THE LIVE APP (plan features not yet built — "
                f"your work queue):\n"
                + (json.dumps(missing, indent=1)[:2400] if missing
                   else "(none reported)")
                + "\n\nUNATTRIBUTED FAILURES:\n"
                + (json.dumps(issues, indent=1)[:1200] if issues
                   else "(none)")
                + f"\n\nAGENT MAILBOX:\n{mail or '(empty)'}\n"
                  "Decide the fix delegations.")
        try:
            pace_for_tpm()
            data = _extract_json(llm_chat(
                [{"role": "system", "content": CHIEF_TRIAGE_SYSTEM},
                 {"role": "user", "content": user}],
                json_mode=True, max_tokens=1600, model=CHIEF_MODELS))
            dels = [d for d in (data.get("delegations") or [])
                    if isinstance(d, dict) and d.get("agent") in ("frontend", "backend")
                    and str(d.get("task", "")).strip()]
            return dels[:4]
        except Exception as exc:  # noqa: BLE001
            append_log(self.ctx.task_id, "chief", "warn", f"triage degraded: {exc}")
            return []

    # -- step 5: final user-facing summary ----------------------------------
    def summarize(self, prompt: str, plan_title: str, reports: Dict[str, Any],
                  verdict: str) -> str:
        def rep(key: str) -> str:
            r = reports.get(key)
            return str((r or {}).get("report", "(not run)"))[:400]
        # The honest-fail contract: the verdict tells the user EXACTLY what
        # remains — the debugger's missing-features checklist, verbatim.
        missing = [m for m in ((reports.get("debugger") or {}).get("missing") or [])
                   if isinstance(m, dict) and str(m.get("feature", "")).strip()]
        miss_txt = ""
        if verdict != "pass" and missing:
            feats = "; ".join(str(m.get("feature", "?"))[:80] for m in missing[:6])
            miss_txt = (f"\nMISSING PLAN FEATURES (still not built): {feats}")
        user = (f"USER REQUEST: {prompt[:300]}\nPLAN: {plan_title}\n"
                f"BACKEND REPORT: {rep('backend')}\n"
                f"FRONTEND REPORT: {rep('frontend')}\n"
                f"DEBUGGER VERDICT: {verdict}\n"
                f"DEBUGGER REPORT: {rep('debugger')}{miss_txt}\n"
                "Write the completion message.")
        try:
            pace_for_tpm()
            return llm_chat(
                [{"role": "system", "content": CHIEF_SUMMARY_SYSTEM},
                 {"role": "user", "content": user}],
                json_mode=False, max_tokens=900, model=CHIEF_MODELS).strip()
        except Exception:  # noqa: BLE001
            if verdict == "pass":
                return (f"{plan_title} — the build is finished and verified "
                        "end to end against the approved plan. Open the "
                        "preview to try it.")
            return (f"{plan_title} — I'm not fully done yet. Open the preview "
                    "to try what's there, and tell me to continue and I'll "
                    "pick up where I left off.")


# ---------------------------------------------------------------------------
# Approval state machine (worker-side wait + WS-side release)
# ---------------------------------------------------------------------------

_APPROVAL_LOCK = threading.Lock()
_APPROVAL_EVENTS: Dict[str, threading.Event] = {}
_APPROVAL_DECISIONS: Dict[str, Dict[str, Any]] = {}
# The task currently halting the worker for approval (chief-gateway routing).
_AWAITING_TASK: Optional[str] = None


def approval_wait(task_id: str) -> Optional[Dict[str, Any]]:
    """Block the worker until a decision arrives (WS or REST). None on timeout."""
    with _APPROVAL_LOCK:
        ev = _APPROVAL_EVENTS.setdefault(task_id, threading.Event())
    emit({"type": "approval_request", "task_id": task_id,
          "plan": (approval_get(task_id) or {}).get("plan", "")})
    decided = ev.wait(timeout=APPROVAL_TIMEOUT_S)
    with _APPROVAL_LOCK:
        _APPROVAL_EVENTS.pop(task_id, None)
        decision = _APPROVAL_DECISIONS.pop(task_id, None)
    if not decided or decision is None:
        return None
    return decision


def approval_submit(task_id: str, action: str, feedback: str = "") -> bool:
    """Called from the WS/REST handlers with the user's decision."""
    if action not in ("approve", "change"):
        return False
    with _APPROVAL_LOCK:
        if task_id not in _APPROVAL_EVENTS and task_id != _AWAITING_TASK:
            # A decision for a task that isn't waiting (late duplicate?) —
            # still record it in SQLite so a crashed worker can honour it.
            approval_upsert(task_id, (approval_get(task_id) or {}).get("plan", ""),
                            "approved" if action == "approve" else "changed", feedback)
            return False
        _APPROVAL_DECISIONS[task_id] = {"action": action, "feedback": feedback}
        ev = _APPROVAL_EVENTS.get(task_id)
    approval_upsert(task_id, (approval_get(task_id) or {}).get("plan", ""),
                    "approved" if action == "approve" else "changed", feedback)
    if ev is not None:
        ev.set()
    return True


def route_prompt_during_approval(text: str) -> bool:
    """Chief-gateway rule: while a task awaits approval, any free-typed
    prompt is treated as plan-change feedback via the injection template."""
    global _AWAITING_TASK
    with _APPROVAL_LOCK:
        task_id = _AWAITING_TASK
    if task_id is None:
        return False
    decision = {"action": "change", "feedback": text}
    with _APPROVAL_LOCK:
        _APPROVAL_DECISIONS[task_id] = decision
        ev = _APPROVAL_EVENTS.get(task_id)
    if ev is not None:
        ev.set()
    return True


# ---------------------------------------------------------------------------
# ORCHESTRATION LAYER — LangGraph StateGraph (the brain transplant).
# The manual while-loop pipeline is GONE. The build is now a state graph:
#   START → chief → {backend | frontend | fit_check | debugger} → chief → …
#   debugger PASS → END · debugger FAIL → chief (triage → fix dispatches).
# The chief node IS the agent_dispatcher: sub-agents are its TOOLS and it
# decides which to call (LLM decision under deterministic guardrails that
# can never skip a quality gate or loop forever).
# ---------------------------------------------------------------------------

# Real langgraph when installed (VM boot installs it); an identical-semantics
# built-in engine otherwise — the graph definition is the same either way.
try:
    from langgraph.graph import END as _LG_END, START as _LG_START, StateGraph as _LG_StateGraph  # type: ignore
    LANGGRAPH_AVAILABLE = True
except Exception:  # noqa: BLE001 — not installed yet (background pip)
    LANGGRAPH_AVAILABLE = False

START = _LG_START if LANGGRAPH_AVAILABLE else "__start__"
END = _LG_END if LANGGRAPH_AVAILABLE else "__end__"


class AgentState(TypedDict, total=False):
    """The graph's shared state (LangGraph merges node returns into it)."""
    # inputs
    task_id: str
    prompt: str
    plan: str
    repo_map: str                     # tree-sitter skeleton of the codebase
    file_system_state: Dict[str, str]  # {rel_path: "create"|"edit"} journal
    # routing (the chief's dispatch decision)
    next_agent: str                   # backend|frontend|fit_check|debugger|end
    dispatch_task: str                # the brief for the dispatched tool
    # progress
    reports: Dict[str, Any]           # per-tool reports
    errors: List[str]
    dispatches: List[str]             # history of tool invocations
    fix_queue: List[Dict[str, str]]   # pending fix delegations
    debug_pending: bool               # fixes applied — QA re-run is due
    debug_rounds: int
    repairs: int
    # v5 convergence tracking (NO fixed round cap — see CONVERGE_*):
    last_gap_sig: str                  # signature of the last QA gap set
    stagnation: int                    # consecutive identical gap signatures
    escalated: bool                    # escalation context already injected
    converge_started: float            # wall-clock anchor for the safety net
    stop_reason: str                   # matched|stagnation|budget|no_gaps|''
    # outcome
    verdict: str                      # pass | fail
    missing: List[Dict[str, Any]]     # the debugger's plan-gap checklist
    issues: List[Dict[str, str]]
    summary: str
    app_port: Any


class _MiniStateGraph:
    """Built-in LangGraph-compatible engine (the exact API subset the swarm
    graph uses): add_node / add_edge / add_conditional_edges / compile /
    invoke. Nodes return partial state dicts merged last-write-wins —
    identical semantics to a plain (no-reducer) LangGraph TypedDict state."""

    def __init__(self, schema: Any) -> None:
        self._schema = schema
        self._nodes: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = {}
        self._edges: Dict[str, str] = {}
        self._cond: Dict[str, Tuple[Callable[[Dict[str, Any]], Any], Dict[str, str]]] = {}
        self._entry: Optional[str] = None

    def add_node(self, name: str, fn: Callable[[Dict[str, Any]], Dict[str, Any]]) -> None:
        self._nodes[name] = fn

    def add_edge(self, a: str, b: str) -> None:
        if a == START:
            self._entry = b
        else:
            self._edges[a] = b

    def add_conditional_edges(self, src: str, fn: Callable[[Dict[str, Any]], Any],
                              mapping: Optional[Dict[str, str]] = None) -> None:
        self._cond[src] = (fn, mapping or {})

    def set_entry_point(self, name: str) -> None:
        self._entry = name

    def compile(self) -> "_MiniCompiled":
        return _MiniCompiled(self)


class _MiniCompiled:
    def __init__(self, graph: _MiniStateGraph) -> None:
        self._g = graph

    def invoke(self, state: Dict[str, Any], config: Optional[Dict[str, Any]] = None,
               **_: Any) -> Dict[str, Any]:
        st = dict(state)
        cur = self._g._entry
        limit = int((config or {}).get("recursion_limit", 80))
        steps = 0
        while cur is not None and cur != END:
            fn = self._g._nodes.get(cur)
            if fn is None:
                raise RuntimeError(f"graph node '{cur}' not registered")
            update = fn(st) or {}
            st.update(update)
            steps += 1
            if steps > limit:
                raise RuntimeError(f"graph exceeded {limit} super-steps")
            if cur in self._g._cond:
                route_fn, mapping = self._g._cond[cur]
                key = route_fn(st)
                cur = mapping.get(key, key)  # unmapped keys name a node directly
            else:
                cur = self._g._edges.get(cur)
        return st


def _plan_needs_backend(plan_text: str) -> bool:
    """Deterministic read of the plan: does it define API endpoints?"""
    m = re.search(r"##\s*Data & API(.*?)(?=\n## |\Z)", plan_text, re.S)
    section = (m.group(1) if m else plan_text).lower()
    if "no backend needed" in section or "frontend-only" in section \
            or "static/frontend-only" in section:
        return False
    if re.search(r"\b(get|post|put|patch|delete)\b[^\n]{0,40}/[a-z]", section):
        return True
    if re.search(r"/api/[a-z]", section):
        return True
    return "flask" in section or "express" in section or "endpoint" in section


def _default_brief(tool: str, plan_text: str) -> str:
    """Fallback briefs when the dispatcher omits one (its decision still
    routes; the brief just gets blunter)."""
    if tool == "backend":
        return ("Build the backend exactly as the plan's Data & API section "
                "specifies: every endpoint (method, path, response), the data "
                "model, and the port. Publish the API contract when done.")
    if tool == "frontend":
        return ("Build the frontend exactly as the plan's Pages & UI section "
                "specifies (Next.js 14 App Router — the page MUST be "
                "frontend/app/page.tsx). If a backend exists, read the API "
                "contract first and route every call through "
                "frontend/lib/api_client.ts.")
    return ""


def agent_dispatcher(state: Dict[str, Any]) -> Dict[str, str]:
    """The chief's agent_dispatcher — its single REAL decision point.
    Sub-agents are TOOLS: the chief LLM picks which to call and writes the
    brief; deterministic guardrails then validate the choice (a premature
    finish, a skipped quality gate, or an exhausted budget gets overridden).
    On any LLM failure the guardrails alone route — the build never stalls."""
    task_id = str(state.get("task_id", ""))
    dispatches = list(state.get("dispatches") or [])
    reports = dict(state.get("reports") or {})
    plan_text = str(state.get("plan", ""))

    done_lines = []
    for tool in dict.fromkeys(dispatches):  # order-preserving unique
        rep = str((reports.get(tool) or {}).get("report", ""))[:160]
        done_lines.append(f"- {tool}: {rep or 'ran'}")
    user = (
        f"APPROVED PLAN:\n{plan_text[:2400]}\n\n"
        f"REPO MAP (current codebase skeleton):\n"
        f"{str(state.get('repo_map') or '(no code yet)')[:2400]}\n\n"
        f"TOOLS ALREADY CALLED (in order):\n"
        + ("\n".join(done_lines) if done_lines else "(none yet — nothing is built)")
        + "\n\nDecide the next tool call."
    )
    proposal: Dict[str, str] = {}
    try:
        pace_for_tpm()
        data = _extract_json(llm_chat(
            [{"role": "system", "content": CHIEF_DISPATCH_SYSTEM},
             {"role": "user", "content": user}],
            json_mode=True, max_tokens=1200, model=CHIEF_MODELS))
        # UNWRAP the OpenAI-style wrapper the chief model intermittently
        # emits (live 2026-09-28: {"tool": {"name": ..., "arguments":
        # {...}}} parsed as tool=dict → strict read failed → guardrails
        # had to save the route). Same family as the agent-loop unwrap.
        raw_tool = data.get("tool")
        raw_task, raw_reason = data.get("task", ""), data.get("reason", "")
        if isinstance(raw_tool, dict):
            args = raw_tool.get("arguments") or raw_tool.get("args") or {}
            if isinstance(args, dict):
                raw_task = raw_task or args.get("task", "")
                raw_reason = raw_reason or args.get("reason", "")
            raw_tool = raw_tool.get("name") or raw_tool.get("tool")
        proposal = {"tool": str(raw_tool or "").strip().lower(),
                    "task": str(raw_task or "").strip(),
                    "reason": str(raw_reason or "")[:200]}
    except Exception as exc:  # noqa: BLE001 — guardrails route alone
        append_log(task_id, "chief", "warn",
                   f"dispatch decision degraded ({exc}) — guardrails routing")

    tool, task = _dispatch_guardrails(state, proposal)
    if tool not in ("end", "debugger", "fit_check") and not task:
        task = _default_brief(tool, plan_text)
    if proposal.get("reason"):
        append_log(task_id, "chief", "info",
                   f"dispatch → {tool} ({proposal['reason']})")
    return {"tool": tool, "task": task}


_TOOL_ALIASES = {
    "backend_agent": "backend", "backend": "backend",
    "frontend_agent": "frontend", "frontend": "frontend",
    "integration_check": "fit_check", "fit_check": "fit_check",
    "qa_verification": "debugger", "debugger": "debugger",
    "finish": "end", "end": "end", "": "end",
}


def _dispatch_guardrails(state: Dict[str, Any],
                         proposal: Dict[str, str]) -> Tuple[str, str]:
    """Deterministic validation of the chief's dispatch choice. The chief
    has real agency, but the graph GUARANTEES: nothing finishes unbuilt,
    the integration and QA gates always run, budgets are honoured, and
    no phase loops forever."""
    dispatches = list(state.get("dispatches") or [])
    reports = state.get("reports") or {}
    budget = int(state.get("dispatch_budget") or MAX_DISPATCHES)
    plan_text = str(state.get("plan", ""))
    ran = set(dispatches)
    tool = _TOOL_ALIASES.get(str(proposal.get("tool", "")).strip().lower(), "")
    task = str(proposal.get("task", "")).strip()
    needs_backend = _plan_needs_backend(plan_text)

    # Budget exhausted: force the QA gate once, then finish honestly.
    if len(dispatches) >= budget:
        return ("debugger" if "debugger" not in ran else "end"), task

    # Nothing dispatched yet → sane opening move.
    if not dispatches:
        return ("backend" if needs_backend else "frontend"), task

    # Premature finish → force the missing build/gate.
    if tool == "end":
        if needs_backend and "backend" not in ran:
            return "backend", task
        if "frontend" not in ran:
            return "frontend", task
        if needs_backend and "fit_check" not in ran:
            return "fit_check", task
        if "debugger" not in ran:
            return "debugger", task
        return "end", task

    # Unknown tool name → default route.
    if not tool:
        if needs_backend and "backend" not in ran:
            return "backend", task
        if "frontend" not in ran:
            return "frontend", task
        if needs_backend and "fit_check" not in ran:
            return "fit_check", task
        if "debugger" not in ran:
            return "debugger", task
        return "end", task

    # Integration check only makes sense with a backend; without one go
    # straight to QA.
    if tool == "fit_check" and not needs_backend:
        return ("debugger" if "debugger" not in ran else "end"), task

    return tool, task


def _gap_signature(debug: Dict[str, Any]) -> str:
    """Signature of a QA result's gap set. IDENTICAL signatures across
    consecutive QA rounds mean the dispatched fixes changed nothing — that
    is the stagnation the v5 safety nets key on (a CHANGING gap set is
    progress, and progress never stops the loop)."""
    feats = sorted(str(m.get("feature", "")).strip().lower()[:60]
                   for m in (debug.get("missing") or [])
                   if isinstance(m, dict))
    obs = sorted(str(i.get("observation", "")).strip()[:60]
                 for i in (debug.get("issues") or [])
                 if isinstance(i, dict))
    return "|".join(f for f in feats if f) + "#" + "|".join(o for o in obs if o)


def _fix_delegations(task_id: str, plan_text: str,
                     debug_report: Dict[str, Any],
                     escalated: bool = False) -> List[Dict[str, str]]:
    """QA failed → THE CHIEF DECIDES which tool builds what's missing.
    CRASH-CLASS evidence (live-server errors: 500s, tracebacks) rides
    verbatim to the owning tool (live-proven: a lossy rephrase fixed
    nothing). PLAN-GAP items (the debugger's missing-features checklist)
    go through the chief's triage LLM — it picks the tool and writes the
    build brief for each gap. If that call degrades, deterministic
    fallback briefs route by the debugger's suspect field — the loop
    never stalls."""
    issues = [i for i in (debug_report.get("issues") or [])
              if isinstance(i, dict)]
    missing = [m for m in (debug_report.get("missing") or [])
               if isinstance(m, dict) and str(m.get("feature", "")).strip()]
    delegations: List[Dict[str, str]] = []

    # 1) Crash-class first — the app must boot before features can be judged.
    for i in issues:
        side = str(i.get("suspect", "")).strip().lower()
        obs = str(i.get("observation", "")).strip()
        if side not in ("frontend", "backend") or not obs:
            continue
        if side == "frontend":
            task = (
                f"FIX (deterministic evidence from the live server):\n{obs[:500]}\n\n"
                "Repair the frontend so http://localhost:3000/ returns 200. Read the "
                "referenced file(s) first, fix the EXACT cause (missing import / "
                "missing file / syntax error), verify_file what you change, then "
                "VERIFY with browser_tool navigate http://localhost:3000 — you may "
                "not report done while it returns >=400."
            )
        else:
            task = (
                f"FIX (deterministic evidence from the live server):\n{obs[:500]}\n\n"
                "Repair the backend so its contracted endpoints answer 2xx. Read "
                "your files first, fix the EXACT cause, verify_file what you "
                "change, restart the server via terminal (kill the old process "
                "first: fuser -k 8000/tcp), then VERIFY with terminal curl of the "
                "endpoint — you may not report done while it errors."
            )
        delegations.append({"agent": side, "task": task})

    # 2) Plan gaps — the chief's decision (its tools, its briefs).
    if missing:
        chief = ChiefAgent(task_id)
        chief_dels = chief.triage(plan_text, debug_report,
                                  mailbox_db_read(task_id, "chief"))
        if chief_dels:
            delegations += chief_dels
        else:
            # Chief LLM degraded → deterministic fallback: each missing
            # feature routes to the debugger's suspect with a gap brief.
            for m in missing:
                side = str(m.get("suspect", "")).strip().lower()
                if side not in ("frontend", "backend"):
                    side = "frontend"
                feat = str(m.get("feature", ""))[:200]
                ev = str(m.get("evidence", ""))[:300]
                task = (f"BUILD THE MISSING PLAN FEATURE (verification found it "
                        f"absent from the live app):\n"
                        f"- Feature: {feat}\n"
                        f"- Evidence: {ev or 'not observed in the UI'}\n\n"
                        "Build ONLY this missing piece (keep everything that "
                        "already works intact), verify_file what you write, "
                        "and confirm it renders in the browser before done.")
                delegations.append({"agent": side, "task": task})
    elif issues:
        # No coverage list — unattributed issues go through triage as before.
        vague = [i for i in issues
                 if str(i.get("suspect", "")).lower()
                 not in ("frontend", "backend")]
        if vague:
            chief = ChiefAgent(task_id)
            delegations += chief.triage(
                plan_text, {"status": "fail", "issues": vague,
                            "missing": []},
                mailbox_db_read(task_id, "chief"))
    if escalated:
        for d in delegations:
            d["task"] = (str(d["task"])
                         + "\n\nESCALATION: previous fix rounds did NOT "
                           "close this gap — the SAME feature is still "
                           "missing from the live app, so the approach taken "
                           "before did not work. Take a DIFFERENT approach "
                           "(new file, new route, different component "
                           "structure) and confirm it renders in the browser "
                           "before reporting done.")
    # Per-ROUND bound only (each failed QA round re-triages from scratch).
    # This is NOT a run cap: the run converges until the observation
    # matches plan.md (v5).
    return delegations[:6]


# -- The nodes ----------------------------------------------------------------


def _fix_activity_label(task: str) -> str:
    """Unified-voice label for fix dispatches — the user sees WHAT the
    agent is doing, not which internal tool got the brief."""
    t = str(task)
    if t.startswith("BUILD THE MISSING"):
        return "Building what's missing"
    return "Applying a fix"


def chief_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """THE dispatcher hub. Every other node returns here. Order:
    1. pending fix dispatches (from a failed QA round) run first;
    2. a fresh QA failure produces the debugger's missing-features
       checklist → the CHIEF DECIDES which tool builds each gap
       (crash-class evidence rides verbatim to its owner);
    3. QA re-run is due after fixes (debug_pending);
    4. otherwise — the agent_dispatcher LLM decides the next tool call.
    The node returns only state DELTAS (merged by the engine).

    v5 CONVERGENCE: there is NO fixed QA-round or fix cap. The loop runs
    until the debugger's observation of the live app MATCHES plan.md. It
    ends early only when progress provably stops — the same gap
    signature across consecutive QA rounds surviving ONE context
    escalation — or the wall-clock guard trips; then the honest-fail
    verdict names the remaining gaps."""
    task_id = str(state.get("task_id", ""))
    plan_text = str(state.get("plan", ""))
    reports = dict(state.get("reports") or {})
    dispatches = list(state.get("dispatches") or [])
    fix_queue = list(state.get("fix_queue") or [])
    debug_rounds = int(state.get("debug_rounds") or 0)
    repairs = int(state.get("repairs") or 0)
    verdict = str(state.get("verdict") or "")
    update: Dict[str, Any] = {}

    # 0) v5 wall-clock safety net — no NEW dispatches once the convergence
    #    budget is spent (a draining fix queue is allowed to finish only
    #    while time remains; the honest-fail path names what remains).
    started = float(state.get("converge_started") or 0) or time.time()
    if ((fix_queue or verdict == "fail")
            and time.time() - started > CONVERGE_MAX_S):
        append_log(task_id, "chief", "warn",
                   f"convergence budget spent ({CONVERGE_MAX_S:.0f}s) after "
                   f"{debug_rounds} QA rounds / {repairs} fix dispatches — "
                   "reporting honestly")
        return {"next_agent": "end", "verdict": verdict or "fail",
                "debug_pending": False, "stop_reason": "budget"}

    # 1) Pending fixes from a triaged QA failure — next one, now.
    if fix_queue:
        nxt = fix_queue.pop(0)
        dispatches.append(nxt["agent"])
        update.update({
            "fix_queue": fix_queue, "dispatches": dispatches,
            "repairs": repairs + 1,
            "next_agent": nxt["agent"], "dispatch_task": nxt["task"],
            "repo_map": generate_repo_map(),
            "file_system_state": file_system_state(task_id),
        })
        emit({"type": "activity", "task_id": task_id,
              "label": _fix_activity_label(nxt["task"]), "state": "active",
              "detail": str(nxt["task"])[:120]})
        return update

    # 2) Fresh QA failure → the debugger's missing-features checklist goes
    #    to the chief, which decides which tool builds what. v5: NOT a
    #    round-capped loop — stagnation (identical gap signature surviving
    #    one escalation) is the only no-progress exit besides the budget.
    if verdict == "fail":
        stagnation = int(state.get("stagnation") or 0)
        escalated = bool(state.get("escalated"))
        if stagnation >= CONVERGE_STAGNATION:
            if not escalated:
                escalated = True
                stagnation = 0
                append_log(task_id, "chief", "warn",
                           f"the same gaps persisted across "
                           f"{CONVERGE_STAGNATION + 1} QA rounds — escalating "
                           "(mandating a different approach); NOT stopping")
            else:
                append_log(task_id, "chief", "warn",
                           "no progress after escalation — reporting honestly")
                return {"next_agent": "end", "verdict": verdict,
                        "debug_pending": False,
                        "stop_reason": "stagnation"}
        debug = reports.get("debugger") or {}
        delegations = _fix_delegations(task_id, plan_text, debug, escalated)
        if not delegations:
            append_log(task_id, "chief", "info",
                       "QA failed with no actionable gaps — reporting honestly")
            return {"next_agent": "end", "verdict": verdict,
                    "debug_pending": False, "stop_reason": "no_gaps"}
        nxt = delegations.pop(0)
        dispatches.append(nxt["agent"])
        feats = "; ".join(str(m.get("feature", "?"))[:50]
                          for m in (debug.get("missing") or [])[:4])
        gaps_txt = feats or "see issues"
        append_log(task_id, "chief", "info",
                   f"QA round {debug_rounds} failed (gaps: {gaps_txt}) — "
                   "dispatching fixes; no round cap: converging until the "
                   "app matches the plan")
        update.update({
            "verdict": "",  # consumed — fresh verdict comes from the QA re-run
            "debug_pending": True,
            "fix_queue": delegations, "dispatches": dispatches,
            "repairs": repairs + 1,
            "escalated": escalated, "stagnation": stagnation,
            "next_agent": nxt["agent"], "dispatch_task": nxt["task"],
            "repo_map": generate_repo_map(),
            "file_system_state": file_system_state(task_id),
        })
        emit({"type": "activity", "task_id": task_id,
              "label": _fix_activity_label(nxt["task"]), "state": "active",
              "detail": str(nxt["task"])[:120]})
        return update

    # 3) QA passed → finish.
    if verdict == "pass":
        return {"next_agent": "end", "debug_pending": False,
                "stop_reason": "matched"}

    # 4) Fixes are applied (or nothing failed) and QA is due → run it.
    if state.get("debug_pending"):
        return {"next_agent": "debugger", "dispatch_task": "",
                "debug_pending": False}

    # 5) The dispatcher decision (LLM + guardrails).
    decision = agent_dispatcher(state)
    tool, task = decision["tool"], decision["task"]
    if tool == "end":
        return {"next_agent": "end", "dispatch_task": ""}
    dispatches.append(tool)
    return {"next_agent": tool, "dispatch_task": task,
            "dispatches": dispatches,
            "repo_map": generate_repo_map(),
            "file_system_state": file_system_state(task_id)}


def backend_node(state: Dict[str, Any]) -> Dict[str, Any]:
    task_id = str(state.get("task_id", ""))
    reports = dict(state.get("reports") or {})
    reports["backend"] = run_backend_agent(
        task_id, str(state.get("dispatch_task", "")), str(state.get("plan", "")))
    return {"reports": reports, "verdict": str(state.get("verdict") or "")}


def frontend_node(state: Dict[str, Any]) -> Dict[str, Any]:
    task_id = str(state.get("task_id", ""))
    reports = dict(state.get("reports") or {})
    reports["frontend"] = run_frontend_agent(
        task_id, str(state.get("dispatch_task", "")), str(state.get("plan", "")))
    return {"reports": reports, "verdict": str(state.get("verdict") or "")}


def fit_check_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """The Twins converge: deterministic server bring-up, then the frontend
    tool drives the live app and attributes every error with evidence. Up
    to 2 mailbox-driven backend repair rounds run INSIDE this node."""
    task_id = str(state.get("task_id", ""))
    plan_text = str(state.get("plan", ""))
    reports = dict(state.get("reports") or {})

    def set_active(state_name: str, detail: str = "") -> None:
        set_status("active", {"state": state_name, "detail": detail,
                              "task_id": task_id})
        emit({"type": "status", "status": get_status("active")})

    set_active("building", "starting the app servers")
    servers = ensure_servers_up(task_id)
    append_log(task_id, "orchestrator", "info",
               f"servers: {json.dumps(servers)}")
    set_active("building", "testing the app end-to-end")
    emit({"type": "activity", "task_id": task_id,
          "label": "Testing the app end-to-end", "state": "active",
          "detail": "both servers up — checking the live app in the browser"})
    fit = run_frontend_agent(task_id, "", plan_text, fit_check=True)
    reports["fit_check"] = fit
    repairs = int(state.get("repairs") or 0)
    for _ in range(2):
        backend_fix_msgs = mailbox_db_read(task_id, "backend")
        if not backend_fix_msgs:
            break
        repairs += 1
        fix_task = ("INTEGRATION FIX REQUEST (from the frontend fit check):\n"
                    + "\n".join(f"- {m['message']}" for m in backend_fix_msgs[-4:])
                    + "\nFix the reported endpoint(s), restart your server via "
                      "terminal, and re-publish the api contract if routes changed.")
        emit({"type": "activity", "task_id": task_id,
              "label": "Applying a fix", "state": "active",
              "detail": "backend endpoint repair"})
        run_backend_agent(task_id, fix_task, plan_text)
        run_frontend_agent(task_id, "", plan_text, fit_check=True)
    emit({"type": "activity", "task_id": task_id,
          "label": "Testing the app end-to-end", "state": "done"})
    return {"reports": reports, "repairs": repairs,
            "verdict": str(state.get("verdict") or "")}


def debugger_node(state: Dict[str, Any]) -> Dict[str, Any]:
    """The QA gate: E2E verification against the approved plan. PASS → the
    graph ends; FAIL → back to the chief for triage (deterministic issues
    ride verbatim). Also stamps the round's gap signature — identical
    signatures across consecutive rounds are the stagnation the v5 safety
    nets key on."""
    task_id = str(state.get("task_id", ""))
    reports = dict(state.get("reports") or {})
    set_status("active", {"state": "verifying",
                          "detail": "end-to-end verification",
                          "task_id": task_id})
    emit({"type": "status", "status": get_status("active")})
    debug = run_debugger_agent(task_id, str(state.get("plan", "")))
    reports["debugger"] = debug
    sig = _gap_signature(debug)
    prev_sig = str(state.get("last_gap_sig") or "")
    stagnation = (int(state.get("stagnation") or 0) + 1
                  if (sig and sig == prev_sig) else 0)
    return {"reports": reports, "verdict": str(debug.get("status", "fail")),
            "issues": debug.get("issues") or [],
            "missing": debug.get("missing") or [],
            "debug_rounds": int(state.get("debug_rounds") or 0) + 1,
            "last_gap_sig": sig, "stagnation": stagnation}


def _route_from_chief(state: Dict[str, Any]) -> str:
    return str(state.get("next_agent") or "end")


def _route_from_debugger(state: Dict[str, Any]) -> str:
    return "chief" if str(state.get("verdict")) != "pass" else "end"


def build_swarm_graph():
    """Compile the build graph. Uses the real LangGraph when importable;
    the identical built-in engine otherwise."""
    graph_cls = _LG_StateGraph if LANGGRAPH_AVAILABLE else _MiniStateGraph
    g = graph_cls(AgentState)
    g.add_node("chief", chief_node)
    g.add_node("backend", backend_node)
    g.add_node("frontend", frontend_node)
    g.add_node("fit_check", fit_check_node)
    g.add_node("debugger", debugger_node)
    g.add_edge(START, "chief")
    g.add_conditional_edges(
        "chief", _route_from_chief,
        {"backend": "backend", "frontend": "frontend",
         "fit_check": "fit_check", "debugger": "debugger", "end": END})
    g.add_edge("backend", "chief")
    g.add_edge("frontend", "chief")
    g.add_edge("fit_check", "chief")
    # debugger FAIL → chief (triage/fix loop); PASS → END (the user's spec).
    g.add_conditional_edges("debugger", _route_from_debugger,
                            {"chief": "chief", "end": END})
    return g.compile()


def run_swarm(task_id: str, prompt: str, plan_text: str) -> Dict[str, Any]:
    """Run the build as a LangGraph state machine. plan.md is the contract;
    the chief dispatches its tools; the QA gate guards the exit."""
    set_status("active", {"state": "building", "detail": "starting the build",
                          "task_id": task_id})
    emit({"type": "status", "status": get_status("active")})
    append_log(task_id, "orchestrator", "info",
               "build graph: "
               + ("langgraph" if LANGGRAPH_AVAILABLE else "built-in engine"))

    initial: Dict[str, Any] = {
        "task_id": task_id,
        "prompt": prompt,
        "plan": plan_text,
        "repo_map": generate_repo_map(),
        "file_system_state": file_system_state(task_id),
        "reports": {},
        "errors": [],
        "dispatches": [],
        "fix_queue": [],
        "debug_pending": False,
        "debug_rounds": 0,
        "repairs": 0,
        "last_gap_sig": "",
        "stagnation": 0,
        "escalated": False,
        "converge_started": time.time(),
        "stop_reason": "",
        "dispatch_budget": MAX_DISPATCHES,
        "verdict": "",
        "issues": [],
        "summary": "",
    }
    graph = build_swarm_graph()
    try:
        final = graph.invoke(initial,
                             config={"recursion_limit": GRAPH_RECURSION_LIMIT})
    except Exception as exc:  # noqa: BLE001 — the graph never fails the task
        log.exception("build graph error (task %s)", task_id)
        append_log(task_id, "orchestrator", "error", f"graph error: {exc}")
        final = dict(initial)
        final["verdict"] = final.get("verdict") or "fail"
        final["errors"] = [f"graph error: {exc}"]

    reports = final.get("reports") or {}
    verdict = str(final.get("verdict") or "fail")
    if "debugger" not in reports:
        # The graph ended before the QA gate could run (budget) — honest.
        append_log(task_id, "orchestrator", "warn",
                   "the build ended before verification could run")

    plan_title = plan_text.splitlines()[0].lstrip("# ").strip() or "The app"
    summary = str(final.get("summary") or "")
    if not summary:
        summary = ChiefAgent(task_id).summarize(prompt, plan_title, reports, verdict)

    return {
        "summary": summary,
        "verdict": verdict,
        "repairs": int(final.get("repairs") or 0),
        # v5 convergence telemetry — how the run ended. stop_reason is
        # "matched" when the debugger's observation matched plan.md; the
        # safety nets (stagnation / budget / no_gaps) only trip when
        # progress provably stopped.
        "convergence": {
            "qa_rounds": int(final.get("debug_rounds") or 0),
            "stop_reason": str(final.get("stop_reason") or "")
                           or ("matched" if verdict == "pass" else "ended"),
            "escalated": bool(final.get("escalated")),
        },
        "plan": plan_text,
        # The honest-fail contract: name exactly what's still missing.
        "missing": [m for m in ((reports.get("debugger") or {})
                                .get("missing") or [])
                    if isinstance(m, dict)][:12],
        "agents": {
            "backend": str((reports.get("backend") or {}).get("report", ""))[:600],
            "frontend": str((reports.get("frontend") or {}).get("report", ""))[:600],
            "fit_check": str((reports.get("fit_check") or {}).get("report", ""))[:600],
            "debugger": str((reports.get("debugger") or {}).get("report", ""))[:600],
        },
        "engine": "langgraph" if LANGGRAPH_AVAILABLE else "mini-graph",
    }


# ---------------------------------------------------------------------------
# Task queue + worker
# ---------------------------------------------------------------------------

import queue as _queue  # noqa: E402

task_queue: "_queue.SimpleQueue[str]" = _queue.SimpleQueue()


def enqueue_task(prompt: str) -> str:
    task_id = uuid.uuid4().hex[:12]
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO task_queue (id, ts, status, prompt) VALUES (?,?,?,?)",
            (task_id, time.time(), "pending", prompt),
        )
    task_queue.put(task_id)
    append_chat("user", prompt, {"task_id": task_id})
    emit({"type": "task_queued", "task_id": task_id, "prompt": prompt})
    emit({"type": "chat", "message": recent_chat(1)[0]})
    return task_id


class TaskWorker(threading.Thread):
    """Consumes tasks through the Chief Agent (the swarm gateway)."""

    def __init__(self) -> None:
        super().__init__(name="task-worker", daemon=True)

    def run(self) -> None:
        log.info("task worker started")
        while True:
            task_id = task_queue.get()
            try:
                self._run_task(task_id)
            except Exception as exc:  # never let the worker die
                log.exception("task %s crashed the worker guard", task_id)
                self._mark_failed(task_id, f"internal error: {exc}")

    def _mark(self, task_id: str, status: str, error: Optional[str] = None) -> None:
        with _db_lock, db() as conn:
            if error is not None:
                conn.execute("UPDATE task_queue SET status=?, error=? WHERE id=?",
                             (status, error, task_id))
            else:
                conn.execute("UPDATE task_queue SET status=? WHERE id=?",
                             (status, task_id))

    def _mark_failed(self, task_id: str, error: str) -> None:
        global _AWAITING_TASK
        self._mark(task_id, "failed", error)
        append_chat("assistant",
                    f"I hit an error while working on that: {str(error)[:400]}. "
                    "Your prompt is saved — tell me to retry and I'll pick it back up.",
                    {"task_id": task_id, "failed": True})
        emit({"type": "task_failed", "task_id": task_id, "error": str(error)[:500]})
        emit({"type": "chat", "message": recent_chat(1)[0]})
        set_status("active", {"state": "idle"})
        emit({"type": "status", "status": get_status("active")})
        with _APPROVAL_LOCK:
            if _AWAITING_TASK == task_id:
                _AWAITING_TASK = None

    def _run_task(self, task_id: str) -> None:
        global _AWAITING_TASK
        with _db_lock, db() as conn:
            row = conn.execute(
                "SELECT prompt, status FROM task_queue WHERE id=?", (task_id,)
            ).fetchone()
        if not row or row["status"] in ("done", "failed"):
            return
        prompt = row["prompt"]
        started = time.time()

        def activity(label: str, state: str, detail: str = "") -> None:
            emit({"type": "activity", "task_id": task_id, "label": label,
                  "state": state, "detail": detail})
            append_log(task_id, "daemon", "info",
                       f"{label} — {detail}" if detail else label)

        def set_active(state: str, detail: str = "") -> None:
            set_status("active", {"state": state, "detail": detail, "task_id": task_id})
            emit({"type": "status", "status": get_status("active")})

        try:
            self._mark(task_id, "running")
            set_active("thinking", "reading your request")
            chief = ChiefAgent(task_id)

            # Crash-recovery path: a previous daemon run already got this task
            # to the approval stage. Honour a recorded decision if present.
            prior = approval_get(task_id)
            recovered_plan: Optional[str] = None
            if prior and prior["status"] == "approved":
                recovered_plan = prior["plan"]
                activity("Restoring your session", "done",
                         "recovered an approved plan after the restart")

            follow_up = workspace_has_source()
            if follow_up:
                git_checkpoint(f"checkpoint before: {prompt[:100]}")

            if not recovered_plan:
                # ── Step 1: CLASSIFY (the gateway decision) ──────────────
                activity("Reading your request", "active")
                route = chief.classify(prompt)
                if route["kind"] == "answer":
                    activity("Answering your question", "done")
                    reply = route["reply"]
                    append_chat("assistant", reply, {"task_id": task_id})
                    emit({"type": "chat", "message": recent_chat(1)[0]})
                    result = {"summary": reply, "kind": "answer",
                              "files": [], "checks": {"ok": True, "issues": ""},
                              "repairs": {"rounds": 0, "diagnoses": []},
                              "duration_ms": int((time.time() - started) * 1000),
                              "model": LLM_MODEL, "models": MODEL_ROUTING,
                              "app_port": None}
                    with _db_lock, db() as conn:
                        conn.execute(
                            "UPDATE task_queue SET status='done', result_json=? WHERE id=?",
                            (json.dumps(result), task_id))
                    emit({"type": "task_done", "task_id": task_id, "result": result})
                    set_active("idle")
                    emit({"type": "status", "status": get_status("active")})
                    return

                # ── Step 2: DRAFT PLAN ────────────────────────────────────
                set_active("thinking", "drafting the plan")
                activity("Drafting the build plan", "active")
                plan_text = chief.draft_plan(prompt)
                activity("Drafting the build plan", "done")

                # ── Step 3: THE APPROVAL LOOP (state machine) ─────────────
                while True:
                    self._mark(task_id, "AWAITING_APPROVAL")
                    set_active("awaiting_approval", "waiting for your approval")
                    approval_upsert(task_id, plan_text, "pending", "")
                    # Show the plan as the assistant's own message (the AI
                    # presents its proposal — nothing hardcoded).
                    append_chat("assistant", plan_text,
                                {"task_id": task_id, "kind": "plan"})
                    emit({"type": "chat", "message": recent_chat(1)[0]})
                    with _APPROVAL_LOCK:
                        _AWAITING_TASK = task_id
                    decision = approval_wait(task_id)
                    with _APPROVAL_LOCK:
                        if _AWAITING_TASK == task_id:
                            _AWAITING_TASK = None
                    if decision is None:
                        self._mark_failed(task_id,
                                          "approval timed out — the plan expired "
                                          "without a decision")
                        return
                    if decision["action"] == "approve":
                        approval_upsert(task_id, plan_text, "approved", "")
                        break
                    feedback = str(decision.get("feedback", "")).strip()
                    if not feedback:
                        feedback = "(no changes specified — plan returned as-is)"
                    set_active("thinking", "revising the plan")
                    activity("Revising the plan", "active", feedback[:120])
                    plan_text = chief.refine_plan(plan_text, feedback)
                    activity("Revising the plan", "done")
            else:
                plan_text = recovered_plan

            # ── Step 4: LOCK plan.md (verbatim — no additions, no removals)
            with open(PLAN_PATH, "w", encoding="utf-8") as fh:
                fh.write(plan_text)
            upsert_file(PLAN_PATH, task_id, "create")
            emit({"type": "plan_locked", "task_id": task_id, "path": PLAN_PATH,
                  "plan": plan_text})
            emit({"type": "files", "task_id": task_id,
                  "files": [{"path": PLAN_PATH, "action": "create"}]})
            activity("Plan approved — starting the build", "done",
                     "plan.md is now the contract")

            # ── Step 5: THE BUILD GRAPH (LangGraph) ────────────────────────
            self._mark(task_id, "running")
            if not follow_up:
                seed_scaffold(task_id)
            swarm = run_swarm(task_id, prompt, plan_text)

            # ── Complete ──────────────────────────────────────────────────
            app_port = (get_status("app") or {}).get("port")
            missing_feats = swarm.get("missing") or []
            miss_line = "; ".join(
                str(m.get("feature", "")) for m in missing_feats
                if isinstance(m, dict) and m.get("feature"))[:400]
            result = {
                "summary": swarm["summary"],
                "kind": "app",
                "files": [],  # the file journal (files table) has the truth
                "checks": {"ok": swarm["verdict"] == "pass",
                           "issues": "" if swarm["verdict"] == "pass"
                           else (miss_line or "see verification report")},
                "repairs": {"rounds": swarm["repairs"], "diagnoses": []},
                "verdict": swarm["verdict"],
                "missing": missing_feats,
                "agents": swarm["agents"],
                "plan": plan_text[:4000],
                "duration_ms": int((time.time() - started) * 1000),
                "model": LLM_MODEL,
                "models": MODEL_ROUTING,
                "app_port": app_port,
            }
            with _db_lock, db() as conn:
                conn.execute(
                    "UPDATE task_queue SET status='done', result_json=? WHERE id=?",
                    (json.dumps(result), task_id))
            append_chat("assistant", result["summary"],
                        {"task_id": task_id, "result": result})
            emit({"type": "chat", "message": recent_chat(1)[0]})
            emit({"type": "task_done", "task_id": task_id, "result": result})
            set_active("idle")
            emit({"type": "status", "status": get_status("active")})
            git_checkpoint(f"arcforge: {prompt[:100]}")
            log.info("task %s done in %.1fs (verdict=%s, repairs=%d)", task_id,
                     time.time() - started, swarm["verdict"], swarm["repairs"])
        except Exception as exc:
            log.exception("task %s failed", task_id)
            self._mark_failed(task_id, str(exc))


def recover_pending_tasks() -> int:
    recovered = 0
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT id FROM task_queue WHERE status IN "
            "('pending','running','AWAITING_APPROVAL')"
        ).fetchall()
    for r in rows:
        task_queue.put(r["id"])
        recovered += 1
    if recovered:
        append_log(None, "daemon", "info",
                   f"crash recovery: re-enqueued {recovered} unfinished task(s)")
        log.info("crash recovery: re-enqueued %d task(s)", recovered)
    return recovered


# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------

from fastapi import (  # noqa: E402
    FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect,
)
from fastapi.responses import JSONResponse  # noqa: E402

STARTED_AT = time.time()


def _authorized(request: Request) -> bool:
    if not TOKEN:
        return True
    header = request.headers.get("authorization", "")
    return secrets.compare_digest(header, f"Bearer {TOKEN}")


def _ws_authorized(ws: WebSocket) -> bool:
    if not TOKEN:
        return True
    token = ws.query_params.get("token", "")
    header = (ws.headers.get("authorization", "") or "").removeprefix("Bearer ").strip()
    return secrets.compare_digest(token, TOKEN) or secrets.compare_digest(header, TOKEN)


def _pending_approval_payload() -> Optional[Dict[str, Any]]:
    """The freshest pending approval (for sync replay after reconnect)."""
    with _db_lock, db() as conn:
        row = conn.execute(
            "SELECT task_id, plan, updated_at FROM approvals WHERE status='pending' "
            "ORDER BY updated_at DESC LIMIT 1").fetchone()
    if not row:
        return None
    task_row = None
    with _db_lock, db() as conn:
        task_row = conn.execute(
            "SELECT status FROM task_queue WHERE id=?", (row["task_id"],)).fetchone()
    if not task_row or task_row["status"] != "AWAITING_APPROVAL":
        return None
    return {"task_id": row["task_id"], "plan": row["plan"]}


def _sync_payload() -> Dict[str, Any]:
    active = get_status("active") or {"state": "idle"}
    return {
        "type": "sync",
        "chat_history": recent_chat(200),
        "active_status": active,
        "tasks": all_tasks(),
        "logs": recent_logs(LOG_TAIL_FOR_SYNC),
        "pending_approval": _pending_approval_payload(),
        "server": {
            "uptime_s": int(time.time() - STARTED_AT),
            "model": LLM_MODEL,
            "models": MODEL_ROUTING,
            "vlm_model": VLM_MODEL if VLM_ENABLED else None,
            "workspace": WORKSPACE,
            "llm_ready": LLM_READY,
            "architecture": "agent",
            "skills": skills_catalog_summary(),
            "browser": (browser_engine.health() if browser_engine is not None
                        else {"playwright_installed": False}),
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _LOOP
    _LOOP = asyncio.get_running_loop()
    init_db()
    boot_note = (f"orchestrator v4 (langgraph={LANGGRAPH_AVAILABLE}, "
                 f"tree-sitter={_TS_ENABLED}) up on :{PORT} (db={DB_PATH}, "
                 f"models={json.dumps(MODEL_ROUTING)}, "
                 f"vlm={VLM_MODEL if VLM_ENABLED else 'off'})")
    set_status("boot", {"ts": time.time(), "note": boot_note})
    append_log(None, "daemon", "info", boot_note)
    recover_pending_tasks()
    worker = TaskWorker()
    worker.start()
    log.info(boot_note)
    yield
    if browser_engine is not None:
        browser_engine.close()


app = FastAPI(title="ArcForge Orchestrator", version="3.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    return {"ok": True, "uptime_s": int(time.time() - STARTED_AT)}


def _guard(request: Request) -> None:
    if not _authorized(request):
        raise HTTPException(status_code=401, detail="unauthorized")


@app.get("/status")
async def status_route(request: Request):
    _guard(request)
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM task_queue GROUP BY status"
        ).fetchall()
    counts = {r["status"]: r["n"] for r in rows}
    return {
        "active": get_status("active") or {"state": "idle"},
        "tasks": counts,
        "connected_clients": len(manager.active),
        "model": LLM_MODEL,
        "models": MODEL_ROUTING,
        "vlm_model": VLM_MODEL if VLM_ENABLED else None,
        "llm_ready": LLM_READY,
        "architecture": "agent",
        "skills": skills_catalog_summary(),
        "browser": (browser_engine.health() if browser_engine is not None
                    else {"playwright_installed": False}),
        "lsp": LSP_CLIENT.health(),
    }


@app.get("/history")
async def history_route(request: Request, limit: int = 200):
    _guard(request)
    return {"messages": recent_chat(max(1, min(limit, 1000)))}


@app.get("/logs")
async def logs_route(request: Request, limit: int = 100, task_id: Optional[str] = None):
    _guard(request)
    return {"logs": recent_logs(max(1, min(limit, 1000)), task_id)}


@app.post("/prompt")
async def prompt_route(request: Request):
    _guard(request)
    body = await request.json()
    text = str(body.get("message") or body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "message is required"}, status_code=400)
    if route_prompt_during_approval(text):
        return {"routed": "approval_feedback", "queued": False}
    task_id = enqueue_task(text)
    return {"task_id": task_id, "queued": True}


@app.post("/approval")
async def approval_route(request: Request):
    """REST fallback for the approval decision (WS is the primary path)."""
    _guard(request)
    body = await request.json()
    task_id = str(body.get("task_id") or "").strip()
    action = str(body.get("action") or "").strip()
    feedback = str(body.get("feedback") or "").strip()
    if not task_id or action not in ("approve", "change"):
        return JSONResponse({"error": "task_id and action(approve|change) required"},
                            status_code=400)
    ok = approval_submit(task_id, action, feedback)
    return {"accepted": ok}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """The dumb-terminal channel + the approval interaction surface."""
    if not _ws_authorized(ws):
        await ws.close(code=4401, reason="unauthorized")
        return
    await manager.connect(ws)
    try:
        await ws.send_text(json.dumps(_sync_payload()))
        while True:
            raw = await ws.receive_text()
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            mtype = msg.get("type")
            if mtype == "ping":
                await ws.send_text(json.dumps({"type": "pong", "ts": time.time()}))
            elif mtype == "hello":
                await ws.send_text(json.dumps(_sync_payload()))
            elif mtype == "prompt":
                text = str(msg.get("text") or "").strip()
                if not text:
                    continue
                if route_prompt_during_approval(text):
                    await ws.send_text(json.dumps(
                        {"type": "approval_feedback_accepted", "text": text[:200]}))
                    continue
                task_id = enqueue_task(text)
                await ws.send_text(json.dumps(
                    {"type": "task_queued", "task_id": task_id, "prompt": text}))
            elif mtype == "approval_response":
                task_id = str(msg.get("task_id") or "").strip()
                action = str(msg.get("action") or "").strip()
                feedback = str(msg.get("feedback") or "").strip()
                ok = approval_submit(task_id, action, feedback)
                await ws.send_text(json.dumps(
                    {"type": "approval_response_ack", "task_id": task_id,
                     "accepted": ok}))
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


@app.websocket("/reverse-tunnel")
async def reverse_tunnel_endpoint(ws: WebSocket):
    """Inbound reverse-tunnel WS (the backend dials in through the signed
    daytonaproxy01.eu URL). Text + VLM LLM requests flow out through it."""
    if RT_TOKEN:
        header_tok = ws.headers.get("x-agent-token", "") or ""
        query_tok = ws.query_params.get("token", "") or ""
        if not (secrets.compare_digest(header_tok, RT_TOKEN)
                or secrets.compare_digest(query_tok, RT_TOKEN)):
            await ws.close(code=4401, reason="unauthorized")
            log.warning("reverse-tunnel: rejected upgrade (bad/missing token)")
            return
    await ws.accept()
    if rt_mux._ws is not None and rt_mux._ws is not ws:
        log.info("reverse-tunnel: new dial-in superseding previous connection")
        rt_mux.fail_all("reverse-tunnel: new connection superseded")
    rt_mux._ws = ws
    rt_mux._ws_connected.set()
    log.info("reverse-tunnel: backend dialed in — LLM/VLM bridge is live")
    try:
        while True:
            raw = await ws.receive_text()
            try:
                data = json.loads(raw)
            except json.JSONDecodeError:
                log.warning("reverse-tunnel: dropping malformed frame: %.200s", raw)
                continue
            t = data.get("t")
            req_id = data.get("id")
            if t == "res":
                rt_mux.on_res(req_id, data.get("status", 200), data.get("headers", {}))
            elif t == "chunk":
                rt_mux.on_chunk(req_id, data.get("body", ""))
            elif t == "done":
                rt_mux.on_done(req_id)
            elif t == "error":
                rt_mux.on_error(req_id, data.get("message", "unknown"))
            elif t == "ping":
                try:
                    await ws.send_text(json.dumps({"t": "pong"}))
                except Exception:  # noqa: BLE001
                    pass
            elif t == "pong":
                pass
            else:
                log.debug("reverse-tunnel: ignoring unknown frame t=%s", t)
    except WebSocketDisconnect:
        pass
    except Exception as exc:  # noqa: BLE001
        log.warning("reverse-tunnel: WS handler crashed: %s", exc)
    finally:
        if rt_mux._ws is ws:
            rt_mux._ws = None
            rt_mux._ws_connected.clear()
            rt_mux.fail_all("reverse-tunnel WS disconnected")
            log.info("reverse-tunnel: backend disconnected")
        else:
            log.info("reverse-tunnel: stale handler exiting (newer connection active)")


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    if not TOKEN:
        log.warning("ORCH_TOKEN is not set — running UNAUTHENTICATED (dev mode only)")
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=PORT,
        log_level="info",
        ws_max_size=16 * 1024 * 1024,
        timeout_keep_alive=300,
    )
