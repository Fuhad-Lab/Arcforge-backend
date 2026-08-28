#!/usr/bin/env python3
"""ArcForge Skills Server — the in-VM MCP-style host for platform skills.

THE MANDATE (2026-08-27, user directive):
    "Don't inject the skills again, instead host them as mcp servers.
     Each agent should have the skills that it is meant solely for it.
     Another agent shouldn't use the skills meant for another agent. For
     example, the backend Agent shouldn't use the UI UX pro max skill."

So skills are NO LONGER injected into prompts as boilerplate text. They are
HOSTED here as addressable tools, and every agent gets its own SCOPED view
of the catalog — a per-agent "MCP server" whose tool list is filtered to
that agent's role. The agents discover and consult skills through the
standard tool surface:

    mcp_list_skills()            → this agent's available skills
    mcp_use_skill(skill, input)  → consult a skill (returns its concrete
                                    engineering guidance; some skills also
                                    carry actionable knowledge packs)

SEGREGATION is enforced SERVER-SIDE: request a skill outside your scope and
you get a hard denial naming your scope — never the skill content.

The catalog ships from the host (skill-registry.ts on the backend) as
skills.json next to this file, in the shape:

    [
      {"name": "UI/UX Pro Max", "scope": ["frontend"],
       "description": "...", "instruction": "...", "source": "..."},
      ...
    ]

Skills flagged requiresConnection are filtered out by the HOST before
planting (the VM cannot reach Linear/Figma/Sentry/…).
"""

from __future__ import annotations

import json
import logging
import os
import sqlite3
import threading
import time
from typing import Any, Dict, List, Optional

log = logging.getLogger("skills-server")

SYSTEM_DIR = os.environ.get("ORCH_SYSTEM_DIR", "/home/daytona/.system")
SKILLS_PATH = os.path.join(SYSTEM_DIR, "skills.json")

# The four swarm roles a skill can serve.
ROLES = ("chief", "frontend", "backend", "debugger")

_CATALOG: List[Dict[str, Any]] = []


def load_catalog() -> int:
    """(Re)load skills.json. Returns the number of usable skills."""
    global _CATALOG
    _CATALOG = []
    try:
        if os.path.exists(SKILLS_PATH):
            with open(SKILLS_PATH, "r", encoding="utf-8") as fh:
                raw = json.load(fh)
            if isinstance(raw, list):
                _CATALOG = [
                    {
                        "name": str(s.get("name", "")).strip(),
                        "scope": [r for r in (s.get("scope") or [])
                                  if r in ROLES] or ["chief"],
                        "description": str(s.get("description", "")).strip(),
                        "instruction": str(s.get("instruction", "")).strip(),
                        "source": str(s.get("source", "")).strip(),
                    }
                    for s in raw
                    if isinstance(s, dict) and s.get("name")
                ]
    except Exception as exc:  # noqa: BLE001 — skills must never break the daemon
        log.warning("skills.json unreadable: %s", exc)
    if _CATALOG:
        log.info("skills server: hosting %d skills (%s)",
                 len(_CATALOG),
                 ", ".join(f"{r}={sum(1 for s in _CATALOG if r in s['scope'])}"
                           for r in ROLES))
    return len(_CATALOG)


def skills_for(role: str) -> List[Dict[str, Any]]:
    """The scoped catalog for one agent role (segregation happens here)."""
    if role not in ROLES:
        return []
    return [s for s in _CATALOG if role in s.get("scope", [])]


def catalog_summary() -> Dict[str, int]:
    return {r: len(skills_for(r)) for r in ROLES}


# ── The MCP tool surface (bound into each agent's toolset) ───────────────


def mcp_list_tools(role: str) -> Dict[str, Any]:
    """Tool: mcp_list_skills — this agent's scoped skill catalog."""
    mine = skills_for(role)
    return {
        "ok": True,
        "mcp_server": f"arcforge-skills/{role}",
        "tools": [
            {"name": s["name"], "description": s["description"] or s["instruction"][:120]}
            for s in mine
        ],
        "note": ("Call mcp_use_skill(name) to consult one. Skills outside "
                 f"your role ({role}) are not served to you."),
    }


def mcp_call_tool(role: str, skill_name: str, user_input: str = "") -> Dict[str, Any]:
    """Tool: mcp_use_skill — consult one skill (role-checked)."""
    skill = next((s for s in _CATALOG
                  if s["name"].lower() == (skill_name or "").strip().lower()), None)
    if skill is None:
        mine = [s["name"] for s in skills_for(role)]
        return {
            "ok": False,
            "error": f"unknown skill '{skill_name}'",
            "your_skills": mine,
        }
    if role not in skill.get("scope", []):
        return {
            "ok": False,
            "error": (f"SEGREGATION: skill '{skill['name']}' is not available to the "
                      f"{role} agent (scope: {', '.join(skill['scope'])}). Use only "
                      "your own skills — check mcp_list_skills."),
        }
    payload: Dict[str, Any] = {
        "ok": True,
        "skill": skill["name"],
        "guidance": skill["instruction"],
    }
    if user_input:
        payload["your_input"] = user_input[:500]
    return payload


def bind_tools(role: str) -> Dict[str, Any]:
    """Convenience view used by the orchestrator's toolset builder."""
    return {
        "role": role,
        "skills": [s["name"] for s in skills_for(role)],
    }


def prompt_block(role: str) -> str:
    """A TINY pointer (not an injection!) telling the agent its skill server
    exists — one line, so the agent knows to call mcp_list_skills. The actual
    skill content is only served on demand via mcp_use_skill."""
    mine = skills_for(role)
    if not mine:
        return ""
    names = ", ".join(s["name"] for s in mine[:8])
    return (f"SKILL SERVER (mcp): {len(mine)} skill(s) are hosted for you ({names}). "
            "Call tool mcp_list_skills to see them and mcp_use_skill to consult one "
            "BEFORE writing code in its domain.")


# ── Usage telemetry (v6) ───────────────────────────────────────────────────
# Every mcp_use_skill call is journalled into the daemon's SQLite
# (state.db, table skill_logs) — /status surfaces the aggregate, which is
# the empirical answer to "are the 17 skills actually used by the agents
# that are supposed to use them?"

_SKILL_DB_PATH: Optional[str] = None
_skill_db_lock = threading.Lock()

_SKILL_LOG_SCHEMA = """
CREATE TABLE IF NOT EXISTS skill_logs (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    ts      REAL NOT NULL,
    role    TEXT NOT NULL,
    skill   TEXT NOT NULL,
    ok      INTEGER NOT NULL,
    input   TEXT
);
CREATE INDEX IF NOT EXISTS idx_skill_logs_ts ON skill_logs(ts);
"""


def set_db_path(path: str) -> None:
    """Point the telemetry sink at the daemon's state.db (idempotent)."""
    global _SKILL_DB_PATH
    _SKILL_DB_PATH = str(path)


def log_usage(role: str, skill_name: str, ok: bool, user_input: str = "") -> None:
    """Record one mcp_use_skill call. Never raises."""
    if not _SKILL_DB_PATH:
        return
    try:
        with _skill_db_lock, sqlite3.connect(_SKILL_DB_PATH, timeout=15) as conn:
            conn.executescript(_SKILL_LOG_SCHEMA)
            conn.execute(
                "INSERT INTO skill_logs (ts, role, skill, ok, input) "
                "VALUES (?,?,?,?,?)",
                (time.time(), str(role)[:40], str(skill_name)[:120],
                 1 if ok else 0, str(user_input)[:500]),
            )
    except sqlite3.Error as exc:
        log.debug("skill telemetry write failed: %s", exc)


def usage_summary() -> Dict[str, Any]:
    """Aggregate skill usage (per-skill counts + last-used + per-role
    breakdown) for /status."""
    if not _SKILL_DB_PATH or not os.path.exists(_SKILL_DB_PATH):
        return {"calls": 0, "skills": {}}
    try:
        with _skill_db_lock, sqlite3.connect(_SKILL_DB_PATH, timeout=15) as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT skill, role, ok, COUNT(*) AS n, MAX(ts) AS last_ts "
                "FROM skill_logs GROUP BY skill, role, ok").fetchall()
            total = conn.execute("SELECT COUNT(*) AS n FROM skill_logs").fetchone()
    except sqlite3.Error:
        return {"calls": 0, "skills": {}}
    skills: Dict[str, Any] = {}
    for r in rows:
        name = r["skill"]
        entry = skills.setdefault(
            name, {"calls": 0, "ok": 0, "failed": 0, "last_ts": None,
                   "by_role": {}})
        entry["calls"] += r["n"]
        entry["ok" if r["ok"] else "failed"] += r["n"]
        entry["last_ts"] = max(entry["last_ts"] or 0, r["last_ts"] or 0) or None
        entry["by_role"][r["role"]] = (entry["by_role"].get(r["role"], 0)
                                       + r["n"])
    return {"calls": int(total["n"]) if total else 0, "skills": skills}
