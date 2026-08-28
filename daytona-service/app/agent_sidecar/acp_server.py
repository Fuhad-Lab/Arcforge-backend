#!/usr/bin/env python3
"""ArcForge ACP Server — the Agent Client Protocol surface.

THE MANDATE (2026-10-30, user architecture directive):
    "Your custom frontend connects to this daemon via WebSockets using two
     distinct protocols: 1. ACP (Agent Client Protocol): standardizes the
     AI interactions. Your UI sends prompts via ACP, and the agent streams
     back its thoughts and terminal execution logs. 2. Yjs (CRDT Engine)."

This module is protocol #1: a JSON-RPC 2.0 WebSocket endpoint (/acp) that
speaks the Agent Client Protocol subset the studio (and future standard
editor clients — the protocol Zed popularised) can drive:

    CLIENT → SERVER (requests)
      initialize     {protocolVersion, clientCapabilities} → agent caps
      session/new    {cwd}                       → {sessionId}
      session/prompt {sessionId, prompt}         → {} (turn streams back)
      session/cancel {sessionId, reason?}        → {cancelled: n}
    SERVER → CLIENT (notifications)
      session/updated      — task lifecycle (queued, …)
      agent/turn-start     — a build turn began
      agent/thought        — the agent's narration lines
      agent/tool_call      — a tool/activity started
      agent/tool_call/update — that activity finished
      terminal/output      — REAL terminal output (command + ACI-paginated
                             result) from the agents' terminal tool
      agent/turn-end       — {success, summary}

MAPPING onto the daemon's native event bus (emit()):
    task_queued  → session/updated          activity(active) → agent/tool_call
    activity(done) → agent/tool_call/update log             → agent/thought
    terminal     → terminal/output          task_done/_failed → agent/turn-end

Session model: the daemon is task-oriented; ACP is session-oriented. An
ACP-created session maps its prompts to task ids; events for tasks with no
ACP session report sessionId = task_id (documented fallback, keeps every
client honest about what the agent is doing regardless of who started it).

A bounded ring of recent notifications is replayed on connect, so a late
(or reconnecting) client still sees the terminal feed tail.

The module is loaded by orchestrator.py via _load_module and mounted at
/acp; bind() injects the daemon hooks. It never imports the orchestrator.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import threading
import time
import uuid
from collections import deque
from typing import Any, Callable, Dict, List, Optional

log = logging.getLogger("acp-server")

TOKEN = os.environ.get("ORCH_TOKEN", "")

try:
    from fastapi import WebSocket
except Exception:  # pragma: no cover — fastapi is a hard dep of the daemon
    WebSocket = None  # type: ignore[assignment]

PROTOCOL_VERSION = 1

# ── Daemon hooks (injected via bind()) ─────────────────────────────────────
_enqueue_task: Optional[Callable[[str], Any]] = None
_route_approval_feedback: Optional[Callable[[str], bool]] = None
_cancel_queued: Optional[Callable[[str], bool]] = None

# ── State ──────────────────────────────────────────────────────────────────
_lock = threading.RLock()
_clients: List[Any] = []
_loop: Optional[asyncio.AbstractEventLoop] = None
_sessions: Dict[str, Dict[str, Any]] = {}          # sessionId → {created_at}
_task_to_session: Dict[str, str] = {}              # task_id → sessionId
_seen_tasks: Dict[str, bool] = {}                  # task_id → turn-start sent
_tool_call_ids: Dict[str, str] = {}                # (task_id,label) → toolCallId
_ring: deque = deque(maxlen=200)                   # replayed notifications


def bind(enqueue_task: Callable[[str], Any],
         route_approval_feedback: Callable[[str], bool],
         cancel_queued: Callable[[str], bool]) -> None:
    """Inject the daemon hooks (called by orchestrator at mount time)."""
    global _enqueue_task, _route_approval_feedback, _cancel_queued
    _enqueue_task = enqueue_task
    _route_approval_feedback = route_approval_feedback
    _cancel_queued = cancel_queued
    log.info("ACP server bound to the daemon (enqueue/prompt-routing/cancel)")


def health() -> Dict[str, Any]:
    return {"available": True, "clients": len(_clients),
            "sessions": len(_sessions)}


# ── Notification plumbing ──────────────────────────────────────────────────

def _notify(method: str, params: Dict[str, Any]) -> None:
    """Queue one ACP notification: buffered in the ring AND delivered to
    every connected client (thread-safe; called from worker threads)."""
    note = {"jsonrpc": "2.0", "method": method, "params": params}
    with _lock:
        _ring.append(note)
    if not _clients or _loop is None or _loop.is_closed():
        return
    try:
        asyncio.run_coroutine_threadsafe(_send_all(note), _loop)
    except Exception:  # noqa: BLE001
        pass


async def _send_all(note: Dict[str, Any]) -> None:
    raw = json.dumps(note)
    for ws in list(_clients):
        try:
            await ws.send_text(raw)
        except Exception:  # noqa: BLE001
            pass


def _session_for(task_id: Optional[str]) -> str:
    """ACP sessionId for a daemon task (fallback: the task id itself)."""
    if not task_id:
        return ""
    with _lock:
        return _task_to_session.get(task_id, task_id)


def relay(event: Dict[str, Any]) -> None:
    """Translate one daemon event into ACP notifications. Registered on
    the orchestrator's _EVENT_RELAYS — must never raise."""
    try:
        etype = str(event.get("type", ""))
        task_id = event.get("task_id")

        if etype == "task_queued":
            _notify("session/updated", {
                "sessionId": _session_for(task_id),
                "update": {"state": "queued",
                           "prompt": str(event.get("prompt", ""))[:300]}})
            return

        if etype == "activity":
            label = str(event.get("label", ""))[:200]
            state = str(event.get("state", ""))
            detail = str(event.get("detail", ""))[:400]
            session = _session_for(task_id)
            # First-ever event for a task ⇒ the turn began.
            with _lock:
                fresh = task_id is not None and not _seen_tasks.get(task_id)
                if task_id is not None:
                    _seen_tasks[task_id] = True
            if fresh:
                _notify("agent/turn-start", {"sessionId": session})
            key = f"{task_id}|{label}"
            with _lock:
                tc_id = _tool_call_ids.get(key)
                if tc_id is None:
                    tc_id = uuid.uuid4().hex[:12]
                    _tool_call_ids[key] = tc_id
            if state == "done":
                _notify("agent/tool_call/update", {
                    "sessionId": session, "toolCallId": tc_id,
                    "update": f"done — {detail}" if detail else "done"})
            else:
                _notify("agent/tool_call", {
                    "sessionId": session, "toolCallId": tc_id,
                    "rawToolCall": {"tool": label, "input": detail}})
            return

        if etype == "log":
            _notify("agent/thought", {
                "sessionId": _session_for(task_id),
                "content": str(event.get("message", ""))[:1200]})
            return

        if etype == "terminal":
            cmd = str(event.get("command", ""))
            out = str(event.get("output", ""))
            _notify("terminal/output", {
                "sessionId": _session_for(task_id),
                "data": f"$ {cmd}\n{out}"[:6000]})
            return

        if etype == "task_done":
            result = event.get("result") or {}
            _notify("agent/turn-end", {
                "sessionId": _session_for(task_id),
                "success": True,
                "summary": str(result.get("summary", ""))[:1200]})
            return

        if etype == "task_failed":
            _notify("agent/turn-end", {
                "sessionId": _session_for(task_id),
                "success": False,
                "summary": str(event.get("error", ""))[:1200]})
            return
    except Exception as exc:  # noqa: BLE001 — a relay must never raise
        log.debug("acp relay error: %s", exc)


# ── JSON-RPC request handling ──────────────────────────────────────────────

_ERR_PARSE = {"code": -32700, "message": "Parse error"}
_ERR_METHOD = {"code": -32601, "message": "Method not found"}
_ERR_PARAMS = {"code": -32602, "message": "Invalid params"}


def _handle_request(msg: Dict[str, Any]) -> Dict[str, Any]:
    """Pure request dispatcher (sync — the daemon hooks are sync)."""
    method = str(msg.get("method", ""))
    params = msg.get("params") or {}
    mid = msg.get("id")

    if method == "initialize":
        return {"jsonrpc": "2.0", "id": mid, "result": {
            "protocolVersion": PROTOCOL_VERSION,
            "agentCapabilities": {"loadSession": False},
            "authMethods": [],
            "agent": {"name": "ArcForge",
                      "version": "6.0",
                      "description": "The ArcForge in-VM build agent"},
        }}

    if method == "session/new":
        session_id = uuid.uuid4().hex
        with _lock:
            _sessions[session_id] = {
                "created_at": time.time(),
                "cwd": str(params.get("cwd", "/workspace")),
            }
        log.info("ACP session/new → %s", session_id)
        return {"jsonrpc": "2.0", "id": mid, "result": {"sessionId": session_id}}

    if method == "session/prompt":
        session_id = str(params.get("sessionId", ""))
        prompt = str(params.get("prompt", "")).strip()
        if not prompt:
            return {"jsonrpc": "2.0", "id": mid, "error": _ERR_PARAMS}
        if _route_approval_feedback is not None and _route_approval_feedback(prompt):
            return {"jsonrpc": "2.0", "id": mid,
                    "result": {"routed": "approval_feedback"}}
        if _enqueue_task is None:
            return {"jsonrpc": "2.0", "id": mid, "error": _ERR_METHOD}
        task_id = _enqueue_task(prompt)
        if task_id:
            with _lock:
                _task_to_session[str(task_id)] = session_id
        return {"jsonrpc": "2.0", "id": mid, "result": {"taskId": task_id}}

    if method == "session/cancel":
        session_id = str(params.get("sessionId", ""))
        cancelled = 0
        with _lock:
            tasks = [t for t, s in _task_to_session.items()
                     if s == session_id]
        for task_id in tasks:
            if _cancel_queued is not None and _cancel_queued(task_id):
                cancelled += 1
        return {"jsonrpc": "2.0", "id": mid,
                "result": {"cancelled": cancelled,
                           "note": ("queued tasks cancelled; in-flight "
                                    "tasks run to completion")}}

    return {"jsonrpc": "2.0", "id": mid, "error": _ERR_METHOD}


# ── The WebSocket endpoint ─────────────────────────────────────────────────

async def acp_endpoint(ws: WebSocket) -> None:
    """JSON-RPC 2.0 over text frames at /acp (token-authenticated)."""
    global _loop
    if TOKEN:
        tok = ws.query_params.get("token", "") or ws.headers.get("x-agent-token", "")
        if not secrets.compare_digest(tok, TOKEN):
            await ws.close(code=4401, reason="unauthorized")
            return
    _loop = asyncio.get_running_loop()
    await ws.accept()
    _clients.append(ws)
    log.info("ACP client connected (%d total)", len(_clients))
    try:
        # Replay the notification tail so late joiners see recent
        # thoughts/terminal output (the live feed has history).
        with _lock:
            replay = list(_ring)
        for note in replay[-50:]:
            try:
                await ws.send_text(json.dumps(note))
            except Exception:  # noqa: BLE001
                break
        while True:
            raw = await ws.receive()
            if raw.get("type") == "websocket.disconnect":
                break
            text = raw.get("text")
            if not text:
                continue
            try:
                msg = json.loads(text)
            except json.JSONDecodeError:
                await ws.send_text(json.dumps(
                    {"jsonrpc": "2.0", "id": None, "error": _ERR_PARSE}))
                continue
            if msg.get("id") is None:
                continue                        # notification ($/ping, …)
            reply = _handle_request(msg)
            await ws.send_text(json.dumps(reply))
    except Exception as exc:  # noqa: BLE001 — WebSocketDisconnect & friends
        log.debug("ACP client handler ended: %s", exc)
    finally:
        if ws in _clients:
            _clients.remove(ws)
        log.info("ACP client disconnected (%d total)", len(_clients))
