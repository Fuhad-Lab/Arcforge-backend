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
# Reverse-tunnel auth: the shared AGENT_PROXY_SECRET between the VM and
# the backend. The backend presents this as `X-Agent-Token` (or ?token=)
# when it dials into /reverse-tunnel. Empty in dev — auth is bypassed.
RT_TOKEN = os.environ.get("TUNNEL_TOKEN", "")
# Physical home is DISK-backed (survives VM stop/start). /workspace may be a
# tmpfs RAM disk, so the host symlinks /workspace/.system -> this directory.
SYSTEM_DIR = os.environ.get("ORCH_SYSTEM_DIR", "/home/daytona/.system")
DB_PATH = os.environ.get("ORCH_DB", os.path.join(SYSTEM_DIR, "state.db"))

# LLM (OpenAI-compatible chat-completions endpoint). The VM's AI client
# supports TWO modes, selected by the ORCH_LLM_URL scheme:
#
#   "reverse-tunnel://"  →  NEW (post-Task-15) — the BACKEND dials INTO
#     this orchestrator's /reverse-tunnel WS endpoint (inbound through
#     the signed daytonaproxy01.eu URL — bypasses the Daytona EU egress
#     filter that blocks the VM dialing OUT to *.onrender.com). When the
#     orchestrator needs to call the LLM, it sends a `req` frame over
#     the inbound WS to the backend; the backend injects the real
#     NVIDIA key (server-side only) and streams res/chunk/done frames
#     back down the same WS. The VM NEVER holds the real key.
#
#   "http://..." / "https://..."  →  LEGACY — the orchestrator POSTs
#     directly via urllib. Used when the VM CAN reach the LLM endpoint
#     (e.g. a non-Daytona region, or the local tunnel_client on
#     http://localhost:7777/v1). Maintained for backward compat and
#     for environments where the egress filter isn't blocking.
#
# ORCH_LLM_KEY is a dummy placeholder when reverse-tunnel mode is active
# (the OpenAI-compatible client API requires *an* api_key arg, but its
# value is ignored — the backend injects the real Bearer token
# server-side before forwarding to NVIDIA). Direct mode (VM→NVIDIA) is
# DISABLED: the VM has no NVIDIA key by design.
LLM_URL = os.environ.get("ORCH_LLM_URL", "http://localhost:7777/v1")
# Detect the reverse-tunnel sentinel BEFORE rstrip — the sentinel is
# "reverse-tunnel://" and we MUST NOT strip its trailing slashes (they
# are part of the scheme marker).
LLM_USE_REVERSE_TUNNEL = LLM_URL.startswith("reverse-tunnel://")
if not LLM_USE_REVERSE_TUNNEL:
    # Normal URL — strip trailing slashes, normalize to the full
    # chat-completions endpoint.
    LLM_URL = LLM_URL.rstrip("/")
    if not LLM_URL.endswith("/chat/completions"):
        LLM_URL = f"{LLM_URL}/chat/completions"
LLM_KEY = os.environ.get("ORCH_LLM_KEY", "tunnel-injected")
LLM_MODEL = os.environ.get("ORCH_LLM_MODEL", "openai/gpt-oss-120b")
LLM_TIMEOUT_S = float(os.environ.get("ORCH_LLM_TIMEOUT_S", "900"))
# Region-aware readiness flag (written by the installer after probing the
# LLM routes from inside the VM). 0 = this VM's egress cannot reach any LLM
# endpoint (eu blocks NVIDIA) — clients then route generation host-side.
# NOTE: in reverse-tunnel mode, LLM_READY is set to 1 by the installer
# because the path doesn't depend on the VM's egress — it depends on the
# backend's ability to dial IN (which is always possible via the signed URL).
LLM_READY = os.environ.get("ORCH_LLM_READY", "1") == "1"

# Pipeline tuning
# ── Frontend dev-server ports (2026-08-27 mandate: Next.js-only frontends) ──
# The platform mandate is: the AI produces a Next.js frontend and picks the
# backend language itself. Next dev runs on 3000; 5173 is kept only for
# legacy Vite apps (older generations / manual prompts) — the debugger
# phase detects the framework from frontend/package.json and launches the
# right server, and both ports are probed when brokering the preview URL.
NEXT_DEV_PORT = int(os.environ.get("ORCH_NEXT_PORT", "3000"))
VITE_DEV_PORT = int(os.environ.get("ORCH_VITE_PORT", "5173"))
# Back-compat alias: older env files set ORCH_DEV_SERVER_PORT for the Vite
# server. If present, it overrides VITE_DEV_PORT (not NEXT_DEV_PORT).
DEV_SERVER_PORT = int(os.environ.get("ORCH_DEV_SERVER_PORT", str(VITE_DEV_PORT)))
VITE_DEV_PORT = DEV_SERVER_PORT
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
# Platform skills — planted by the host installer as skills.json next to
# this daemon (the same 17-skill catalog the host-side pipeline injects via
# god-mode-protocol.ts; single source of truth = skill-registry.ts on the
# backend). Loaded once at boot and injected into the Architect/Developer
# prompts so in-VM generations honour the mandatory skills too.
# ---------------------------------------------------------------------------
SKILLS: List[Dict[str, str]] = []
try:
    _skills_path = os.path.join(SYSTEM_DIR, "skills.json")
    if os.path.exists(_skills_path):
        with open(_skills_path, "r", encoding="utf-8") as _fh:
            _raw = json.load(_fh)
        if isinstance(_raw, list):
            SKILLS = [
                {"name": str(s.get("name", "")).strip(),
                 "instruction": str(s.get("instruction", "")).strip()}
                for s in _raw if isinstance(s, dict) and s.get("name")
            ]
except Exception as _exc:  # noqa: BLE001 — skills must never break the daemon
    log.warning("skills.json present but unreadable: %s", _exc)

if SKILLS:
    log.info("loaded %d platform skills from skills.json", len(SKILLS))

def skills_prompt_block() -> str:
    """Render the mandatory-skills section injected into generation prompts."""
    if not SKILLS:
        return ""
    lines = [
        f"## MANDATORY PLATFORM SKILLS ({len(SKILLS)} active — apply while building):"
    ]
    for i, s in enumerate(SKILLS, 1):
        instr = s["instruction"]
        if len(instr) > 400:
            instr = instr[:397] + "..."
        lines.append(f"{i}. {s['name']}: {instr}")
    lines.append(
        "- Honour these skills in every file you write (structure, quality, "
        "security, and completeness they demand). Do not claim a skill was "
        "applied unless its instruction genuinely shaped the output."
    )
    return "\n".join(lines)

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
# REVERSE TUNNEL — backend dials INTO this orchestrator's /reverse-tunnel
# WS endpoint (inbound through the signed daytonaproxy01.eu URL). Bypasses
# the Daytona EU egress filter that blocks the VM dialing OUT to
# *.onrender.com. See module-level docstring on the LLM_URL config above.
#
# PROTOCOL (matches src/routes/tunnel.ts on the backend — keep in sync):
#   VM→backend: {t:"req", id, method, path, headers, body}
#   backend→VM: {t:"res", id, status, headers}
#                {t:"chunk", id, body}
#                {t:"done", id}
#                {t:"error", id, message}
#                {t:"ping"} / {t:"pong"}
#
# The VM is the WS SERVER (the backend dials in). The orchestrator's
# worker thread (which calls llm_chat) sends `req` frames over the WS;
# the backend receives them, calls NVIDIA with the server-side key,
# and streams res/chunk/done back. The multiplexer tracks in-flight
# req IDs and resolves their asyncio.Futures when the matching frames
# arrive on the /reverse-tunnel WS.
# ---------------------------------------------------------------------------


class _InflightRT:
    """One in-flight LLM request pending on the reverse-tunnel WS."""

    __slots__ = ("future", "status", "headers", "body_parts")

    def __init__(self) -> None:
        # asyncio.Future — must be created on the loop (this class is
        # only ever instantiated from inside coroutines scheduled on
        # _LOOP, so get_running_loop() works).
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.status: int = 0
        self.headers: Dict[str, str] = {}
        self.body_parts: List[str] = []


class ReverseTunnelMultiplexer:
    """Tracks in-flight LLM requests pending on the reverse-tunnel WS.

    Each register(req_id) returns an _InflightRT whose .future the
    worker thread's bridged coroutine awaits. The /reverse-tunnel WS
    handler dispatches res/chunk/done/error frames here. On WS
    disconnect, fail_all() errors every pending request so the worker
    thread's llm_chat raises immediately instead of hanging.
    """

    def __init__(self) -> None:
        self._inflight: Dict[str, _InflightRT] = {}
        self._ws: Any = None  # starlette WebSocket (the backend's inbound WS)
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
        """Fail every in-flight request (called on WS disconnect)."""
        for e in self._inflight.values():
            if not e.future.done():
                e.future.set_exception(ConnectionError(reason))
        self._inflight.clear()

    async def send_req(self, frame: Dict[str, Any]) -> None:
        """Send a req frame over the inbound WS (VM→backend). Raises if
        the backend hasn't dialed in yet (or the WS has dropped)."""
        async with self._send_lock:
            if self._ws is None or not self._ws_connected.is_set():
                raise ConnectionError("reverse-tunnel WS not connected (backend hasn't dialed in)")
            await self._ws.send_text(json.dumps(frame))


rt_mux = ReverseTunnelMultiplexer()


# ---------------------------------------------------------------------------
# LLM client (stdlib urllib — OpenAI-compatible chat completions)
# ---------------------------------------------------------------------------


# Track when the last LLM call finished, for TPM pacing (see llm_chat
# docstring). Simple module-global — the worker thread is the only caller.
_last_llm_done_ts = 0.0
TPM_GAP_S = 65.0  # Groq's token budget window is per-minute; 65s is safe.


def pace_for_tpm() -> None:
    """Block until the previous LLM call is ≥ TPM_GAP_S ago (big requests
    only pass the TPM pre-check on a cold window). No-op on a cold clock."""
    global _last_llm_done_ts
    wait = _last_llm_done_ts + TPM_GAP_S - time.time()
    if wait > 0:
        log.info("tpm pacing: sleeping %.1fs before the next LLM call", wait)
        time.sleep(wait)


def llm_chat(
    messages: List[Dict[str, str]],
    json_mode: bool = False,
    max_tokens: int = 16384,
) -> str:
    """Call the configured OpenAI-compatible endpoint. Raises RuntimeError
    with a readable message on failure (the worker catches and degrades).

    GROQ TPM NOTE (live-measured 2026-08-27): this account's tier enforces
    ~8k tokens/min. The pre-check rejects prompt+max_tokens that exceed
    the CURRENT minute's remaining budget — so a big request passes on a
    cold minute but 413s (`rate_limit_exceeded`, code `tokens`) when it
    follows another call too closely. Empirically a cold-minute burst of
    12k max_tokens + ~5k real completion streams fine and fast (~450
    tok/s). llm_chat therefore: (a) records the time of every successful
    call, (b) exposes pace_for_tpm() so the pipeline can space a big call
    at least TPM_GAP_S after the previous one, and (c) retries 429/413
    rate-limit responses after sleeping out the window.

    Dispatches on the ORCH_LLM_URL scheme:
      - "reverse-tunnel://"  →  send a `req` frame over the inbound
        /reverse-tunnel WS to the backend; the backend injects the real
        provider key server-side and streams res/chunk/done back.
      - "http(s)://..."       →  legacy urllib POST (used in non-Daytona
        environments or where the egress filter isn't blocking).
    """
    if LLM_USE_REVERSE_TUNNEL:
        return _llm_chat_via_reverse_tunnel(
            messages, json_mode=json_mode, max_tokens=max_tokens,
        )

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
                # Dummy Authorization — tunnel_client strips this header
                # at the edge and the ArcForge backend injects the real
                # NVIDIA Bearer token before forwarding to NVIDIA. The VM
                # never holds the real key.
                "Authorization": f"Bearer {LLM_KEY}",
            },
            method="POST",
        )
        try:
            with urllib_request.urlopen(req, timeout=LLM_TIMEOUT_S) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            content = payload["choices"][0]["message"]["content"]
            if not isinstance(content, str) or not content.strip():
                raise RuntimeError("LLM returned an empty message")
            global _last_llm_done_ts
            _last_llm_done_ts = time.time()
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


def _llm_chat_via_reverse_tunnel(
    messages: List[Dict[str, str]],
    json_mode: bool = False,
    max_tokens: int = 16384,
) -> str:
    """Reverse-tunnel LLM call. Sends a `req` frame over the inbound
    /reverse-tunnel WS to the backend, awaits res/chunk/done, returns
    the assembled JSON payload (same shape as an OpenAI chat completion
    response). Called from the worker THREAD (llm_chat runs there);
    bridges to the asyncio loop via run_coroutine_threadsafe so the
    frame send + future await both happen on _LOOP (single-threaded
    by definition — no thread-safety issues on the multiplexer).
    """
    global _last_llm_done_ts
    loop = _LOOP
    if loop is None or loop.is_closed():
        raise RuntimeError("reverse-tunnel: asyncio loop not initialized")

    body: Dict[str, Any] = {
        "model": LLM_MODEL,
        "messages": messages,
        "temperature": 0,
        "max_tokens": max_tokens,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}

    body_str = json.dumps(body)
    req_id = uuid.uuid4().hex
    # Strip Authorization + Host + Content-Length — the backend injects
    # the real NVIDIA Bearer token server-side and recomputes hop-by-hop
    # headers. (We're not actually sending them anyway, but keep the
    # protocol identical to the old tunnel_client for code symmetry.)
    frame: Dict[str, Any] = {
        "t": "req",
        "id": req_id,
        "method": "POST",
        "path": "/v1/chat/completions",
        "headers": {"Content-Type": "application/json"},
        "body": body_str,
    }

    last_err: Optional[Exception] = None
    for attempt in range(3):
        try:
            # Schedule the send-and-await on the asyncio loop. Block the
            # worker thread until it resolves (or times out). The future
            # resolves when /reverse-tunnel receives a `done`/`error` frame.
            future = asyncio.run_coroutine_threadsafe(
                _rt_send_and_await(req_id, frame), loop,
            )
            entry = future.result(timeout=LLM_TIMEOUT_S + 10)
        except asyncio.TimeoutError:
            rt_mux.cancel(req_id)
            if attempt < 2:
                last_err = TimeoutError("reverse-tunnel LLM call timed out")
                time.sleep(5)
                continue
            raise RuntimeError("reverse-tunnel: LLM call timed out")
        except ConnectionError as exc:
            # Backend hasn't dialed in yet, or WS dropped mid-call.
            if attempt < 2:
                last_err = exc
                time.sleep(5)
                continue
            raise RuntimeError(f"reverse-tunnel WS not connected: {exc}") from exc
        except RuntimeError as exc:
            # The backend sent an `error` frame.
            if attempt < 2:
                last_err = exc
                time.sleep(5)
                continue
            raise
        except Exception as exc:  # noqa: BLE001 — defensive
            if attempt < 2:
                last_err = exc
                time.sleep(5)
                continue
            raise RuntimeError(f"reverse-tunnel LLM call failed: {exc}") from exc

        status = entry.status or 502
        body_preview = "".join(entry.body_parts)[:600]
        # Groq rate limits: 429 (RPM) or 413 with code `tokens`/
        # `rate_limit_exceeded` (TPM — the request exceeded the current
        # minute's remaining budget). Both are TRANSIENT: sleep out the
        # window and retry. Other statuses are real errors — raise.
        tpm_limited = status == 413 and (
            "rate_limit_exceeded" in body_preview or "tokens per minute" in body_preview
        )
        if (status == 429 or tpm_limited) and attempt < 2:
            last_err = RuntimeError(
                f"LLM HTTP {status}: rate-limited — {body_preview[:200]}")
            log.warning("llm rate-limited (HTTP %s) — sleeping out the window, retry %d/2",
                        status, attempt + 1)
            time.sleep(65)
            continue
        if status != 200:
            raise RuntimeError(f"LLM HTTP {status}: {body_preview[:500]}")

        # Success — assemble the JSON payload the same way the urllib
        # path would: parse the body and extract choices[0].message.content.
        full_body = "".join(entry.body_parts)
        try:
            payload = json.loads(full_body)
        except json.JSONDecodeError as exc:
            raise RuntimeError(
                f"reverse-tunnel: LLM returned non-JSON (first 300 chars): {full_body[:300]}"
            ) from exc
        content = payload["choices"][0]["message"]["content"]
        if not isinstance(content, str) or not content.strip():
            raise RuntimeError("LLM returned an empty message")
        _last_llm_done_ts = time.time()
        return content

    raise RuntimeError(f"reverse-tunnel LLM failed after retries: {last_err}")


async def _rt_send_and_await(req_id: str, frame: Dict[str, Any]) -> _InflightRT:
    """Coroutine that runs on _LOOP: register the req id, send the req
    frame over the reverse-tunnel WS, await the matching future.
    Returns the resolved _InflightRT (with status + headers + body_parts
    populated). Raises if the WS drops or the backend sends an `error`
    frame — these are converted by the caller's exception handling
    into retry/raise decisions.
    """
    entry = rt_mux.register(req_id)
    try:
        await rt_mux.send_req(frame)
    except Exception:
        rt_mux.cancel(req_id)
        raise
    try:
        await entry.future
    except Exception:
        # The future was resolved with an exception (on_error or fail_all).
        # entry has been popped from the mux already; just re-raise.
        raise
    return entry


def _repair_double_escaped(text: str) -> str:
    """Repair LLM file content that arrived with ONLY escaped newlines.

    Observed live (2026-08-26, Nemotron-3.5-lightning "Social media" run):
    the model double-escapes newlines in JSON file content, so app.py and
    package.json land as a SINGLE line full of literal '\\n' two-char
    sequences — py_compile fails on line 1 and npm dies with EJSONPARSE.

    Safety: fires ONLY when the content has zero REAL newlines but at
    least one escaped one. Any legitimately-formatted code file always
    has real newlines, so healthy files are never touched (a literal
    '\\n' inside a Python/JS string on a normal multi-line file stays
    exactly as the model wrote it).
    """
    if not text:
        return text
    real_nl = text.count("\n")
    lit_nl = text.count("\\n")
    if real_nl == 0 and lit_nl >= 1:
        return (
            text.replace("\\r\\n", "\n")
            .replace("\\n", "\n")
            .replace("\\t", "\t")
            .replace('\\"', '"')
        )
    return text


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
                # Port the app dev-server is serving on (null when none) —
                # the studio re-fetches agent-info on task_done and iframes
                # the SIGNED preview URL for this port (the REAL preview).
                "app_port": debug.get("app_port"),
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
            "Plan the requested app. PLATFORM MANDATE: the frontend MUST be "
            "Next.js (App Router, TypeScript); you CHOOSE the backend language "
            "and framework that best serves the app (e.g. Python/Flask, "
            "Node/Express — or none for a purely static site). "
            "Reply with ONLY a JSON object: "
            '{"summary": "<one-paragraph user-facing summary of what you will build>", '
            '"components": ["<short list of major components>"], '
            '"stack": {"frontend": "Next.js 14 (App Router, TypeScript)", '
            '"backend": "<your chosen language+framework, or none>"}}'
        )
        skills = skills_prompt_block()
        if skills:
            system += "\n\n" + skills
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
        """Phase 2 — generate the full file set as {path: content}.

        PLATFORM MANDATE (2026-08-27): the frontend is ALWAYS a Next.js
        (App Router, TypeScript) app under "frontend/"; the model picks the
        BACKEND language/framework itself when the app needs one. The
        developer-phase output budget is 28000 tokens so a complete Next.js
        app fits in one JSON reply (within Groq free-tier TPM on a cold window).
        """
        system = (
            "You are the Developer of ArcForge. Implement the planned app "
            "COMPLETELY. Reply with ONLY a JSON object: "
            '{"summary": "<user-facing summary of what was built and how to run it>", '
            '"backend": {"<path>": "<full file content>"}, '
            '"frontend": {"<path>": "<full file content>"}}. '
            "Paths are relative and MUST live under frontend/ or backend/. "
            "FRONTEND (MANDATORY): a complete Next.js 14 App Router app in "
            "TypeScript under \"frontend/\" — package.json (next@14, react, "
            "react-dom, scripts dev=\"next dev\"), next.config.mjs, tsconfig.json, "
            "app/layout.tsx, app/page.tsx, app/globals.css, plus every route, "
            "component and lib file the app needs. Use Tailwind ONLY if you "
            "also include its config+postcss files; plain CSS modules or "
            "globals.css are safer. NO create-react-app, NO Vite, NO "
            "index.html — Next.js only. "
            "BACKEND (YOUR CHOICE): if the app needs a server/API, pick ONE "
            "language+framework (e.g. Python Flask with requirements.txt, or "
            "Node Express with backend/package.json + server.js) and write "
            "complete runnable code under \"backend/\" with clear instructions "
            "in the summary. If the frontend alone suffices, omit backend. "
            "Write REAL, complete, runnable code — no placeholders, no TODOs."
        )
        skills = skills_prompt_block()
        if skills:
            system += "\n\n" + skills
        user = (
            f"Plan: {json.dumps(plan, ensure_ascii=False)}\n\n"
            f"Original request: {prompt}\n\nProduce the complete file set."
        )
        # TPM pacing: this is the BIG call (prompt+max_tokens ~29k). It only
        # passes Groq's per-minute pre-check on a cold window — the architect
        # call just ran, so sleep out the remainder of the window first.
        pace_for_tpm()
        reply = llm_chat(
            [{"role": "system", "content": system}, {"role": "user", "content": user}],
            json_mode=True, max_tokens=28000,
        )
        data = _extract_json(reply)
        files: Dict[str, str] = {}
        for group in ("backend", "frontend"):
            blob = data.get(group)
            if isinstance(blob, dict):
                for path, content in blob.items():
                    if isinstance(path, str) and isinstance(content, str) and content.strip():
                        files[path.strip().lstrip("/")] = _repair_double_escaped(content)
        # Some models return a flat {"files": {...}} — accept that too.
        flat = data.get("files")
        if isinstance(flat, dict) and not files:
            for path, content in flat.items():
                if isinstance(path, str) and isinstance(content, str) and content.strip():
                    files[path.strip().lstrip("/")] = _repair_double_escaped(content)
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

        # 2) Frontend deps + dev server — framework-aware.
        #    The mandate is Next.js, but legacy Vite apps (older generations)
        #    are still served. Detection: package.json deps contain "next".
        #    Next dev binds 0.0.0.0 so the SIGNED Daytona preview URL for the
        #    app port can reach it (that URL is what the studio Preview tab
        #    iframes — the REAL live preview).
        fe = os.path.join(WORKSPACE, "frontend")
        app_port: Optional[int] = None
        if os.path.exists(os.path.join(fe, "package.json")):
            try:
                with open(os.path.join(fe, "package.json"), "r", encoding="utf-8") as fh:
                    pkg = json.load(fh)
                deps = {**(pkg.get("dependencies") or {}), **(pkg.get("devDependencies") or {})}
                is_next = "next" in deps or any(p.endswith("next.config.mjs") or p.endswith("next.config.js") for p in [f["path"] for f in written])
            except Exception:
                is_next = False
            if is_next:
                app_port = NEXT_DEV_PORT
                shell("npm install --no-audit --no-fund --loglevel=error",
                      fe, "debugger", 600)
                probe = shell(
                    f"curl -s -o /dev/null -w '%{{http_code}}' "
                    f"http://localhost:{NEXT_DEV_PORT}/ --max-time 3",
                    fe, "debugger", 10,
                )
                already_up = str(probe.get("stdout", "")).strip().startswith(("2", "3"))
                if not already_up:
                    # Kill any stale server on the port first (a previous
                    # generation's next dev may still hold it).
                    shell(f"fuser -k {NEXT_DEV_PORT}/tcp 2>/dev/null; sleep 1",
                          fe, "debugger", 15)
                    shell(
                        f"nohup npx next dev -p {NEXT_DEV_PORT} -H 0.0.0.0 "
                        f"> /tmp/frontend-dev.log 2>&1 < /dev/null &",
                        fe, "debugger", 20,
                    )
                    # Next dev cold-boots + compiles the first route on the
                    # first request — give it room, then warm it with a real
                    # request so the first user page load is fast.
                    for _ in range(4):
                        shell("sleep 5", fe, "debugger", 10)
                        warm = shell(
                            f"curl -s -o /dev/null -w '%{{http_code}}' "
                            f"http://localhost:{NEXT_DEV_PORT}/ --max-time 20",
                            fe, "debugger", 30,
                        )
                        if str(warm.get("stdout", "")).strip().startswith(("2", "3")):
                            break
            else:
                # Legacy Vite app — serve on the Vite port as before.
                app_port = VITE_DEV_PORT
                shell("npm install --no-audit --no-fund --loglevel=error",
                      fe, "debugger", 300)
                probe = shell(
                    f"curl -s -o /dev/null -w '%{{http_code}}' "
                    f"http://localhost:{VITE_DEV_PORT}/ --max-time 2",
                    fe, "debugger", 10,
                )
                already_up = str(probe.get("stdout", "")).strip().startswith(("2", "3"))
                if not already_up:
                    # Launch detached — the dev server outlives the daemon AND
                    # every WebSocket client.
                    shell(
                        f"nohup npx vite --port {VITE_DEV_PORT} --host --strictPort "
                        f"> /tmp/frontend-dev.log 2>&1 < /dev/null &",
                        fe, "debugger", 20,
                    )
                    shell("sleep 4", fe, "debugger", 10)
                    shell(
                        f"curl -s -o /dev/null -w '%{{http_code}}' "
                        f"http://localhost:{VITE_DEV_PORT}/ --max-time 3",
                        fe, "debugger", 10,
                    )

        # 3) Backend deps + server (language chosen by the model).
        be = os.path.join(WORKSPACE, "backend")
        if os.path.exists(os.path.join(be, "requirements.txt")):
            shell("pip install -q -r requirements.txt", be, "debugger", 300)
        be_pkg = os.path.join(be, "package.json")
        if os.path.exists(be_pkg) and not os.path.exists(os.path.join(be, "app.py")) \
                and not os.path.exists(os.path.join(be, "main.py")) \
                and not os.path.exists(os.path.join(be, "server.py")):
            # Node backend — install + start via its scripts (npm start, else dev).
            shell("npm install --no-audit --no-fund --loglevel=error",
                  be, "debugger", 600)
            try:
                with open(be_pkg, "r", encoding="utf-8") as fh:
                    bpkg = json.load(fh)
                bscript = "start" if "start" in (bpkg.get("scripts") or {}) else \
                          ("dev" if "dev" in (bpkg.get("scripts") or {}) else None)
            except Exception:
                bscript = None
            if bscript:
                shell(
                    f"nohup npm run {bscript} > /tmp/backend-dev.log 2>&1 < /dev/null &",
                    be, "debugger", 20,
                )
        for entry in ("app.py", "main.py", "server.py"):
            if os.path.exists(os.path.join(be, entry)):
                shell(
                    f"nohup python3 {entry} > /tmp/backend-dev.log 2>&1 < /dev/null &",
                    be, "debugger", 20,
                )
                break

        # 4) Publish the app-server state — the frontend re-fetches
        #    agent-info on task_done and gets a SIGNED preview URL for this
        #    port; broadcasting it also live-updates any connected studio.
        if app_port is not None:
            set_status("app", {"port": app_port, "up": True, "task_id": task_id})
            emit({"type": "status", "status": get_status("app")})

        return {"ok": ok, "issues": "; ".join(issues)[:1000], "commands": ran,
                "app_port": app_port}


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


@app.websocket("/reverse-tunnel")
async def reverse_tunnel_endpoint(ws: WebSocket):
    """Inbound reverse-tunnel WS endpoint.

    The BACKEND dials into this endpoint via the signed
    *.daytonaproxy01.eu URL (the same proxy the frontend uses for /ws).
    Auth is via the shared AGENT_PROXY_SECRET (TUNNEL_TOKEN env var) —
    presented as `X-Agent-Token` header OR `?token=` query. The backend
    uses the same secret for the existing /api/tunnel endpoint, so a VM
    provisioned with its TUNNEL_TOKEN works for either transport.

    Once accepted, the backend holds the WS open and waits for `req`
    frames from this orchestrator (sent by the worker thread via
    rt_mux.send_req when llm_chat is called). For each `req` frame,
    the backend injects the real NVIDIA key, calls NVIDIA, and streams
    res/chunk/done frames back. Those frames are dispatched into the
    multiplexer to resolve the in-flight futures.

    See the "REVERSE TUNNEL" section above for the full protocol.
    """
    # Auth — X-Agent-Token (preferred) OR ?token= query.
    if RT_TOKEN:
        header_tok = ws.headers.get("x-agent-token", "") or ""
        query_tok = ws.query_params.get("token", "") or ""
        if not (secrets.compare_digest(header_tok, RT_TOKEN)
                or secrets.compare_digest(query_tok, RT_TOKEN)):
            await ws.close(code=4401, reason="unauthorized")
            log.warning("reverse-tunnel: rejected upgrade (bad/missing token)")
            return
    await ws.accept()
    # If a previous backend WS is still tracked, fail its in-flight
    # requests and mark it stale — the new connection replaces it.
    # (We don't actively close the old WS object — its recv loop will
    # exit on its own when it receives a close frame or hits a timeout;
    # we just stop routing new req frames to it.)
    if rt_mux._ws is not None and rt_mux._ws is not ws:
        log.info("reverse-tunnel: new dial-in superseding previous connection — failing in-flight reqs")
        rt_mux.fail_all("reverse-tunnel: new connection superseded")
    rt_mux._ws = ws
    rt_mux._ws_connected.set()
    log.info("reverse-tunnel: backend dialed in — LLM bridge is live")
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
        # COMPARE-AND-SWAP: only clear the global WS state if it still
        # points to THIS ws. If a newer connection has already replaced
        # us (rt_mux._ws != ws), leave the global state alone — the
        # newer connection is the source of truth. This prevents a
        # stale disconnect handler from clearing the active backend
        # connection (e.g. when a transient rogue client dials in and
        # out, the real backend's WS must NOT be torn down).
        if rt_mux._ws is ws:
            rt_mux._ws = None
            rt_mux._ws_connected.clear()
            rt_mux.fail_all("reverse-tunnel WS disconnected")
            log.info("reverse-tunnel: backend disconnected")
        else:
            log.info("reverse-tunnel: stale handler exiting (a newer "
                     "connection is active — leaving rt_mux intact)")


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
