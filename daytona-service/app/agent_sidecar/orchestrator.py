#!/usr/bin/env python3
"""
ArcForge Orchestrator — the In-VM "Shadow Agent" daemon.

This is the BRAIN that lives inside every Daytona MicroVM. It runs as a
persistent background service (managed by PM2 as "agent-brain", with a
shell watchdog fallback) and owns the full agent lifecycle:

    ┌────────────────────────────────────────────────────────────┐
    │  Daytona MicroVM                                           │
    │                                                            │
    │  /workspace/          the user's app (tmpfs — instant HMR) │
    │  /workspace/.system/  -> /home/daytona/.system (disk-backed)│
    │      orchestrator.py   this daemon                         │
    │      state.db          SQLite: chat / tasks / logs / status│
    │                                                            │
    │  PM2 keeps "agent-brain" alive — crash => instant restart  │
    │  Listens on 0.0.0.0:9000, Bearer <AGENT_TOKEN> auth        │
    └────────────────────────────────────────────────────────────┘

ARCHITECTURE (the "In-VM Sidecar" pattern):

  1. The frontend is a DUMB TERMINAL. It opens a WebSocket, renders
     whatever the daemon broadcasts, and sends prompts. It holds no
     execution state of its own.

  2. The daemon is AUTONOMOUS. Prompts land in a durable task queue
     (SQLite). A dedicated worker THREAD consumes the queue and runs the
     multi-agent pipeline (Architect -> Developer -> Debugger). The
     worker is fully decoupled from every WebSocket: if the user closes
     the tab mid-build, the pipeline keeps running and writing to
     state.db. When the user returns and reconnects, the daemon replays
     everything from SQLite (the "sync" handshake).

  3. The daemon SURVIVES CRASHES. PM2 restarts the process if it dies;
     on boot the daemon re-enqueues any task that was pending/running
     when it died, so work is never lost.

SQLite schema (state.db, WAL mode for concurrent reader/writer):

    chat_history(id, ts, role, content, meta_json)
    task_queue  (id, ts, status, prompt, result_json, error)
    process_logs(id, ts, task_id, source, level, message)
    agent_status(key, value_json, updated_at)
    files       (path, task_id, ts, action)          -- file-change journal

WebSocket protocol (daemon -> client):
    {"type":"sync",        chat_history, active_status, tasks, logs}
    {"type":"task_queued", task_id, prompt}
    {"type":"status",      status}                    -- agent_status upsert
    {"type":"activity",    task_id, label, state, detail}
    {"type":"log",         task_id, source, level, message, ts}
    {"type":"chat",        message}                   -- row appended to chat_history
    {"type":"files",       task_id, files:[{path, action}]}
    {"type":"task_done",   task_id, result}
    {"type":"task_failed", task_id, error}
    {"type":"pong"}

WebSocket protocol (client -> daemon):
    {"type":"hello"}                              -- request a fresh sync
    {"type":"prompt", text}
    {"type":"ping"}

Dependencies: fastapi + uvicorn (installed by the host orchestrator at
workspace creation). Everything else is stdlib — sqlite3, threading,
urllib (LLM calls), subprocess (terminal commands).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import secrets
import sqlite3
import subprocess
import threading
import time
import uuid
from contextlib import asynccontextmanager
from typing import Any, Dict, List, Optional
from urllib import request as urllib_request
from urllib.error import HTTPError, URLError

# ---------------------------------------------------------------------------
# Configuration (env-overridable; injected by the host at install time)
# ---------------------------------------------------------------------------

PORT = int(os.environ.get("ORCH_PORT", "9000"))
TOKEN = os.environ.get("ORCH_TOKEN", "")
WORKSPACE = os.environ.get("ORCH_WORKSPACE", "/workspace")
# Physical home is DISK-backed (survives VM stop/start). /workspace may be a
# tmpfs RAM disk, so the host symlinks /workspace/.system -> this directory.
SYSTEM_DIR = os.environ.get("ORCH_SYSTEM_DIR", "/home/daytona/.system")
DB_PATH = os.environ.get("ORCH_DB", os.path.join(SYSTEM_DIR, "state.db"))

# LLM (OpenAI-compatible chat-completions endpoint; configured by the host
# from the platform's single-mode settings — the VM never stores these in
# code, only in the daemon process environment).
LLM_URL = os.environ.get("ORCH_LLM_URL", "")
LLM_KEY = os.environ.get("ORCH_LLM_KEY", "")
LLM_MODEL = os.environ.get("ORCH_LLM_MODEL", "glm-5.2")
LLM_TIMEOUT_S = float(os.environ.get("ORCH_LLM_TIMEOUT_S", "300"))
# Region-aware readiness flag (written by the installer after probing the
# LLM routes from inside the VM). 0 = this VM's egress cannot reach any LLM
# endpoint (eu blocks NVIDIA) — clients then route generation host-side.
LLM_READY = os.environ.get("ORCH_LLM_READY", "1") == "1"

# Pipeline tuning
DEV_SERVER_PORT = int(os.environ.get("ORCH_DEV_SERVER_PORT", "5173"))
LOG_TAIL_FOR_SYNC = int(os.environ.get("ORCH_SYNC_LOG_TAIL", "50"))

LOG_FILE = os.environ.get("ORCH_LOG_FILE", os.path.join(SYSTEM_DIR, "orchestrator.log"))

# ---------------------------------------------------------------------------
# Logging — mirror to stderr (PM2 captures it) and a file
# ---------------------------------------------------------------------------

os.makedirs(SYSTEM_DIR, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_FILE),
    ],
)
log = logging.getLogger("orchestrator")

# ---------------------------------------------------------------------------
# SQLite state store
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
CREATE INDEX IF NOT EXISTS idx_chat_ts       ON chat_history(ts);
CREATE INDEX IF NOT EXISTS idx_logs_task_ts  ON process_logs(task_id, ts);
CREATE INDEX IF NOT EXISTS idx_files_ts      ON files(ts);
"""

_db_lock = threading.RLock()


def db() -> sqlite3.Connection:
    """Thread-safe connection factory (one connection per call, WAL mode)."""
    conn = sqlite3.connect(DB_PATH, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


def init_db() -> None:
    with _db_lock, db() as conn:
        conn.executescript(_SCHEMA)


# -- typed row helpers -------------------------------------------------------


def append_chat(role: str, content: str, meta: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    row = {
        "id": uuid.uuid4().hex,
        "ts": time.time(),
        "role": role,
        "content": content,
        "meta": meta or {},
    }
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO chat_history (id, ts, role, content, meta_json) VALUES (?,?,?,?,?)",
            (row["id"], row["ts"], role, content, json.dumps(row["meta"])),
        )
    return row


def append_log(task_id: Optional[str], source: str, level: str, message: str) -> None:
    with _db_lock, db() as conn:
        conn.execute(
            "INSERT INTO process_logs (ts, task_id, source, level, message) VALUES (?,?,?,?,?)",
            (time.time(), task_id, source, level, str(message)[-4000:]),
        )


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
    out = [
        {"id": r["id"], "ts": r["ts"], "role": r["role"], "content": r["content"],
         "meta": json.loads(r["meta_json"] or "{}")}
        for r in rows
    ]
    return list(reversed(out))


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
    out = [
        {"id": r["id"], "ts": r["ts"], "task_id": r["task_id"], "source": r["source"],
         "level": r["level"], "message": r["message"]}
        for r in rows
    ]
    return list(reversed(out))


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


# ---------------------------------------------------------------------------
# WebSocket connection manager (the broadcast hub)
# ---------------------------------------------------------------------------


class ConnectionManager:
    """Tracks connected dumb-terminals.

    broadcast() NEVER blocks the worker: it is always scheduled onto the
    asyncio loop from the worker thread via run_coroutine_threadsafe. When
    zero clients are connected the events simply go nowhere — the durable
    copy in SQLite is the source of truth and sync replays it on reconnect.
    """

    def __init__(self) -> None:
        self.active: List[Any] = []          # starlette WebSocket objects
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
        # NOTE: no cancellation signal is ever sent to the worker. Tab close
        # is a NON-EVENT for the pipeline — that is the entire point of the
        # sidecar architecture.
        log.info("client disconnected (%d total) — background tasks continue",
                 len(self.active))

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
        """Thread-safe bridge: worker thread -> asyncio loop."""
        loop = _LOOP
        if loop is None or loop.is_closed():
            return
        try:
            asyncio.run_coroutine_threadsafe(self.broadcast(event), loop)
        except Exception as exc:  # pragma: no cover — never kill the worker
            log.warning("broadcast failed: %s", exc)


manager = ConnectionManager()
_LOOP: Optional[asyncio.AbstractEventLoop] = None      # captured at startup


def emit(event: Dict[str, Any]) -> None:
    """Worker-facing helper: broadcast an event to all terminals."""
    manager.broadcast_from_worker(event)


# ---------------------------------------------------------------------------
# LLM client (stdlib urllib — OpenAI-compatible chat completions)
# ---------------------------------------------------------------------------


def llm_chat(
    messages: List[Dict[str, str]],
    json_mode: bool = False,
    max_tokens: int = 16384,
) -> str:
    """Call the configured OpenAI-compatible endpoint. Raises RuntimeError
    with a readable message on failure (the worker catches and degrades)."""
    if not LLM_URL or not LLM_KEY:
        raise RuntimeError("LLM endpoint not configured (ORCH_LLM_URL / ORCH_LLM_KEY)")

    body: Dict[str, Any] = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}

    last_err: Optional[Exception] = None
    for attempt in range(3):
        req = urllib_request.Request(
            LLM_URL,
            data=json.dumps(body).encode("utf-8"),
            headers={
                "Content-Type": "application/json",
                # The key is either a provider API key OR "agent-token:<vm
                # secret>" when routed through the platform's LLM proxy
                # (eu VMs are geo-blocked from the providers) — the proxy
                # authenticates with X-Agent-Token.
                "Authorization": f"Bearer {LLM_KEY}",
                **(
                    {"X-Agent-Token": LLM_KEY.split("agent-token:", 1)[1]}
                    if LLM_KEY.startswith("agent-token:")
                    else {}
                ),
            },
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=LLM_TIMEOUT_S) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            content = payload["choices"][0]["message"]["content"]
            if not isinstance(content, str) or not content.strip():
                raise RuntimeError("LLM returned an empty message")
            return content
        except HTTPError as exc:
            # 429 -> retry with backoff (free-tier rate limits); others raise.
            if exc.code == 429 and attempt < 2:
                last_err = exc
                time.sleep(20 * (attempt + 1))
                continue
            detail = exc.read().decode("utf-8", "replace")[:500]
            raise RuntimeError(f"LLM HTTP {exc.code}: {detail}") from exc
        except (URLError, TimeoutError, OSError) as exc:
            if attempt < 2:
                last_err = exc
                time.sleep(5)
                continue
            raise RuntimeError(f"LLM unreachable: {exc}") from exc
    raise RuntimeError(f"LLM failed after retries: {last_err}")


def _extract_json(text: str) -> Dict[str, Any]:
    """Parse the first JSON object out of an LLM reply (handles markdown
    fences and preamble prose)."""
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
# The autonomous task queue + multi-agent pipeline (the worker THREAD)
# ---------------------------------------------------------------------------

import queue as _queue  # noqa: E402

# The durable queue: worker thread blocks on this. Enqueued by both the WS
# handler and the REST fallback; mirrored into SQLite so a daemon crash
# (and PM2 restart) re-enqueues unfinished work on boot.
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
    """Consumes the task queue and runs the full pipeline.

    Runs as a plain OS thread — NOT tied to any WebSocket, request, or
    event loop. If every client disconnects (tab close, laptop lid, train
    tunnel) the loop keeps executing and writing to state.db; returning
    clients get everything via the sync handshake.
    """

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

    # -- task lifecycle ------------------------------------------------------

    def _mark(self, task_id: str, status: str, error: Optional[str] = None) -> None:
        with _db_lock, db() as conn:
            if error is not None:
                conn.execute(
                    "UPDATE task_queue SET status=?, error=? WHERE id=?",
                    (status, error, task_id),
                )
            else:
                conn.execute("UPDATE task_queue SET status=? WHERE id=?", (status, task_id))

    def _mark_failed(self, task_id: str, error: str) -> None:
        self._mark(task_id, "failed", error)
        append_chat("assistant", (
            "I hit an error while working on that: "
            f"{str(error)[:500]}. Your prompt is saved — tell me to retry and "
            "I'll pick it back up."
        ), {"task_id": task_id, "failed": True})
        emit({"type": "task_failed", "task_id": task_id, "error": str(error)[:500]})
        emit({"type": "chat", "message": recent_chat(1)[0]})
        set_status("active", {"state": "idle"})
        emit({"type": "status", "status": get_status("active")})

    def _run_task(self, task_id: str) -> None:
        with _db_lock, db() as conn:
            row = conn.execute(
                "SELECT prompt, status FROM task_queue WHERE id=?", (task_id,)
            ).fetchone()
        if not row or row["status"] in ("done", "failed"):
            return

        prompt = row["prompt"]
        self._mark(task_id, "running")
        started = time.time()
        log.info("task %s started: %.80s", task_id, prompt)

        def activity(label: str, state: str, detail: str = "") -> None:
            emit({"type": "activity", "task_id": task_id, "label": label,
                  "state": state, "detail": detail})
            append_log(task_id, "daemon", "info",
                       f"{label} — {detail}" if detail else label)

        def set_active(state: str, detail: str = "") -> None:
            set_status("active", {"state": state, "detail": detail, "task_id": task_id})
            emit({"type": "status", "status": get_status("active")})

        try:
            # ── Phase 1: ARCHITECT ─────────────────────────────────────────
            set_active("architect", "Planning the build")
            activity("Planning", "active")
            plan = self._architect(prompt)
            activity("Planning", "done", str(plan.get("summary", ""))[:200])

            # ── Phase 2: DEVELOPER ────────────────────────────────────────
            set_active("developer", "Writing code")
            activity("Writing code", "active")
            files = self._developer(prompt, plan)
            written = self._write_files(files, task_id)
            emit({"type": "files", "task_id": task_id, "files": written})
            activity("Writing code", "done", f"{len(written)} files")

            # ── Phase 3: DEBUGGER ─────────────────────────────────────────
            set_active("debugger", "Verifying the build")
            activity("Verifying", "active")
            debug = self._debugger(written, task_id)
            activity("Verifying", "done",
                     "all checks passed" if debug["ok"] else f"issues: {debug['issues'][:200]}")

            # ── Complete ──────────────────────────────────────────────────
            summary = plan.get("summary") or "Build finished."
            result = {
                "summary": summary,
                "files": [f["path"] for f in written],
                "checks": {"ok": debug["ok"], "issues": debug["issues"]},
                "duration_ms": int((time.time() - started) * 1000),
                "model": LLM_MODEL,
            }
            with _db_lock, db() as conn:
                conn.execute(
                    "UPDATE task_queue SET status='done', result_json=? WHERE id=?",
                    (json.dumps(result), task_id),
                )
            append_chat("assistant", summary, {"task_id": task_id, "result": result})
            emit({"type": "chat", "message": recent_chat(1)[0]})
            emit({"type": "task_done", "task_id": task_id, "result": result})
            set_active("idle")
            emit({"type": "status", "status": get_status("active")})
            log.info("task %s done in %.1fs (%d files)", task_id,
                     time.time() - started, len(written))
        except Exception as exc:
            log.exception("task %s failed", task_id)
            self._mark_failed(task_id, str(exc))

    # ── pipeline phases ─────────────────────────────────────────────────────

    def _architect(self, prompt: str) -> Dict[str, Any]:
        """Phase 1 — produce a compact build plan."""
        history = recent_chat(20)
        convo = "\n".join(f"{m['role']}: {m['content'][:400]}" for m in history[-8:])
        system = (
            "You are the Architect of ArcForge, an autonomous in-VM build agent. "
            "Plan the requested app. Reply with ONLY a JSON object: "
            '{"summary": "<one-paragraph user-facing summary of what you will build>", '
            '"components": ["<short list of major components>"], '
            '"stack": {"frontend": "<e.g. React+Vite>", "backend": "<e.g. Flask or none>"}}'
        )
        user = f"Conversation so far:\n{convo}\n\nNew request: {prompt}"
        try:
            reply = llm_chat(
                [{"role": "system", "content": system}, {"role": "user", "content": user}],
                json_mode=True,
            )
            return _extract_json(reply)
        except Exception as exc:
            append_log(None, "architect", "warn", f"planning degraded: {exc}")
            return {"summary": f"Building: {prompt[:200]}", "components": [], "stack": {}}

    def _developer(self, prompt: str, plan: Dict[str, Any]) -> Dict[str, str]:
        """Phase 2 — generate the full file set as {path: content}."""
        system = (
            "You are the Developer of ArcForge. Implement the planned app COMPLETELY. "
            "Reply with ONLY a JSON object: "
            '{"summary": "<user-facing summary of what was built and how to run it>", '
            '"backend": {"<path>": "<full file content>"}, '
            '"frontend": {"<path>": "<full file content>"}}. '
            "Paths are relative (e.g. \"frontend/package.json\", \"backend/app.py\", "
            "\"frontend/src/App.tsx\"). Frontend must be a self-contained Vite+React app "
            "(include package.json, index.html, src/*). Backend, when needed, is Flask "
            "(include requirements.txt). Write REAL, complete, runnable code — no placeholders."
        )
        user = (
            f"Plan: {json.dumps(plan, ensure_ascii=False)}\n\n"
            f"Original request: {prompt}\n\nProduce the complete file set."
        )
        reply = llm_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            json_mode=True, max_tokens=16384,
        )
        data = _extract_json(reply)
        files: Dict[str, str] = {}
        for group in ("backend", "frontend"):
            blob = data.get(group)
            if isinstance(blob, dict):
                for path, content in blob.items():
                    if isinstance(path, str) and isinstance(content, str) and content.strip():
                        files[path.strip().lstrip("/")] = content
        # Some models return a flat {"files": {...}} — accept that too.
        flat = data.get("files")
        if isinstance(flat, dict) and not files:
            for path, content in flat.items():
                if isinstance(path, str) and isinstance(content, str) and content.strip():
                    files[path.strip().lstrip("/")] = content
        if not files:
            raise RuntimeError("the developer phase produced no files")
        if not data.get("summary"):
            data["summary"] = "Build finished."
        plan["summary"] = data["summary"] or plan.get("summary")
        return files

    def _write_files(self, files: Dict[str, str], task_id: str) -> List[Dict[str, str]]:
        """Native writes into /workspace — instant inotify for dev-server HMR."""
        written: List[Dict[str, str]] = []
        for rel, content in files.items():
            # Route into the mandatory blueprint dirs.
            if rel.startswith(("frontend/", "backend/", "git/")):
                dest = os.path.join(WORKSPACE, rel)
            elif rel == "logo.png":
                dest = os.path.join(WORKSPACE, "logo.png")
            else:
                dest = os.path.join(WORKSPACE, "frontend", rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            existed = os.path.exists(dest)
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(content)
            action = "edit" if existed else "create"
            upsert_file(dest, task_id, action)
            written.append({"path": dest, "action": action})
            append_log(task_id, "developer", "info",
                       f"wrote {dest} ({len(content)} bytes)")
        return written

    def _debugger(self, written: List[Dict[str, str]], task_id: str) -> Dict[str, Any]:
        """Phase 3 — syntax checks, dependency install, dev-server launch."""
        ok = True
        issues: List[str] = []
        ran: List[Dict[str, Any]] = []

        def shell(cmd: str, cwd: str, source: str, timeout: int = 120) -> Dict[str, Any]:
            nonlocal ok
            try:
                proc = subprocess.run(
                    cmd, shell=True, cwd=cwd, capture_output=True, text=True,
                    timeout=timeout,
                )
                out = {"command": cmd, "exit_code": proc.returncode,
                       "stdout": (proc.stdout or "")[-2000:],
                       "stderr": (proc.stderr or "")[-2000:]}
            except subprocess.TimeoutExpired:
                out = {"command": cmd, "exit_code": -1, "stdout": "",
                       "stderr": f"timed out after {timeout}s"}
            append_log(task_id, source, "info" if out["exit_code"] == 0 else "warn",
                       f"$ {cmd}\n{out['stdout'] or out['stderr']}")
            ran.append(out)
            if out["exit_code"] != 0:
                ok = False
                issues.append(
                    f"{cmd}: exit {out['exit_code']} — {(out['stderr'] or '')[:150]}")
            return out

        # 1) Syntax checks
        for f in written:
            path = f["path"]
            if path.endswith(".py"):
                shell(f'python3 -m py_compile "{path}"', WORKSPACE, "debugger", 30)
            elif path.endswith((".js", ".mjs", ".cjs")):
                shell(f'node --check "{path}"', WORKSPACE, "debugger", 30)

        # 2) Frontend deps + dev server
        fe = os.path.join(WORKSPACE, "frontend")
        if os.path.exists(os.path.join(fe, "package.json")):
            shell("npm install --no-audit --no-fund --loglevel=error",
                  fe, "debugger", 300)
            probe = shell(
                f"curl -s -o /dev/null -w '%{{http_code}}' "
                f"http://localhost:{DEV_SERVER_PORT}/ --max-time 2",
                fe, "debugger", 10,
            )
            already_up = str(probe.get("stdout", "")).strip().startswith(("2", "3"))
            if not already_up:
                # Launch detached — the dev server outlives the daemon AND
                # every WebSocket client.
                shell(
                    f"nohup npx vite --port {DEV_SERVER_PORT} --host --strictPort "
                    f"> /tmp/frontend-dev.log 2>&1 < /dev/null &",
                    fe, "debugger", 20,
                )
                shell("sleep 4", fe, "debugger", 10)
                shell(
                    f"curl -s -o /dev/null -w '%{{http_code}}' "
                    f"http://localhost:{DEV_SERVER_PORT}/ --max-time 3",
                    fe, "debugger", 10,
                )

        # 3) Backend deps + server
        be = os.path.join(WORKSPACE, "backend")
        if os.path.exists(os.path.join(be, "requirements.txt")):
            shell("pip install -q -r requirements.txt", be, "debugger", 300)
        for entry in ("app.py", "main.py", "server.py"):
            if os.path.exists(os.path.join(be, entry)):
                shell(
                    f"nohup python3 {entry} > /tmp/backend-dev.log 2>&1 < /dev/null &",
                    be, "debugger", 20,
                )
                break

        return {"ok": ok, "issues": "; ".join(issues)[:1000], "commands": ran}


# ---------------------------------------------------------------------------
# Crash recovery — re-enqueue unfinished tasks after a daemon restart
# ---------------------------------------------------------------------------


def recover_pending_tasks() -> int:
    """PM2 restarted us (or the VM rebooted). Any task that was pending or
    running when we died is unfinished work — put it back on the queue."""
    recovered = 0
    with _db_lock, db() as conn:
        rows = conn.execute(
            "SELECT id FROM task_queue WHERE status IN ('pending','running')"
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
# FastAPI application — REST + WebSocket surface
# ---------------------------------------------------------------------------

from fastapi import (  # noqa: E402
    FastAPI,
    HTTPException,
    Request,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.responses import JSONResponse  # noqa: E402

STARTED_AT = time.time()


def _authorized(request: Request) -> bool:
    """Bearer <AGENT_TOKEN> — the shared secret generated at workspace
    creation. Only the platform (and the VM's owner via the platform) can
    present it."""
    if not TOKEN:
        return True  # dev mode — no token configured
    header = request.headers.get("authorization", "")
    return secrets.compare_digest(header, f"Bearer {TOKEN}")


def _ws_authorized(ws: WebSocket) -> bool:
    if not TOKEN:
        return True
    token = ws.query_params.get("token", "")
    header = (ws.headers.get("authorization", "") or "").removeprefix("Bearer ").strip()
    return secrets.compare_digest(token, TOKEN) or secrets.compare_digest(header, TOKEN)


def _sync_payload() -> Dict[str, Any]:
    """The full state snapshot sent to every freshly connected terminal."""
    active = get_status("active") or {"state": "idle"}
    return {
        "type": "sync",
        "chat_history": recent_chat(200),
        "active_status": active,
        "tasks": all_tasks(),
        "logs": recent_logs(LOG_TAIL_FOR_SYNC),
        "server": {
            "uptime_s": int(time.time() - STARTED_AT),
            "model": LLM_MODEL,
            "workspace": WORKSPACE,
            "llm_ready": LLM_READY,
        },
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _LOOP
    _LOOP = asyncio.get_running_loop()
    init_db()
    boot_note = f"orchestrator up on :{PORT} (db={DB_PATH})"
    set_status("boot", {"ts": time.time(), "note": boot_note})
    append_log(None, "daemon", "info", boot_note)
    recover_pending_tasks()
    worker = TaskWorker()
    worker.start()
    log.info(boot_note)
    yield


app = FastAPI(title="ArcForge Orchestrator", version="1.0.0", lifespan=lifespan)


@app.get("/health")
async def health():
    """Unauthenticated liveness probe (no state leaked)."""
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
        "llm_ready": LLM_READY,
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
    """REST fallback for environments where WebSocket upgrades are blocked
    (corporate proxies, some Daytona preview configurations). Same queue,
    same autonomy — the client can poll /status + /history."""
    _guard(request)
    body = await request.json()
    text = str(body.get("message") or body.get("text") or "").strip()
    if not text:
        return JSONResponse({"error": "message is required"}, status_code=400)
    task_id = enqueue_task(text)
    return {"task_id": task_id, "queued": True}


@app.websocket("/ws")
async def ws_endpoint(ws: WebSocket):
    """The dumb-terminal channel.

    Handshake: client connects (token via ?token= or Authorization header);
    on accept the daemon immediately pushes the full `sync` snapshot
    (chat_history + active_status + tasks + recent logs). From then on the
    client is a pure renderer: every state change is broadcast, and any
    client that arrives late sees everything via its own sync.
    """
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
                if text:
                    task_id = enqueue_task(text)
                    await ws.send_text(json.dumps(
                        {"type": "task_queued", "task_id": task_id, "prompt": text}
                    ))
    except WebSocketDisconnect:
        manager.disconnect(ws)
    except Exception:
        manager.disconnect(ws)


# ---------------------------------------------------------------------------
# Entrypoint — uvicorn, bound to 0.0.0.0 so the Daytona proxy can reach us
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
