#!/usr/bin/env python3
"""
ArcForge In-VM Tunnel Client — bridges localhost:7777 → backend WS → NVIDIA.

This is the IN-VM half of the Inbound Reverse Proxy Tunnel. It runs inside
every Daytona MicroVM (managed by PM2 as "tunnel-client", with a shell
watchdog fallback) alongside the orchestrator ("agent-brain").

WHY THIS EXISTS
  Daytona Linux sandboxes are forced into Daytona's EU region, whose egress
  firewall drops outbound TLS to *.nvidia.com and *.onrender.com. So the
  In-VM AI orchestrator can reach NEITHER NVIDIA directly NOR the backend's
  HTTPS LLM proxy. The fix: move the AI generation loop INSIDE the VM, with a
  local tunnel daemon that bridges the VM's localhost:7777 over a single
  WebSocket to the ArcForge backend. The backend injects the real NVIDIA
  key (NEVER present in the VM), forwards to NVIDIA (US region, unblocked),
  and streams the response back down the WS. Daytona's egress filter only
  sees VM↔backend WS traffic (allowed) — never NVIDIA.

ARCHITECTURE
  ┌─────────────────────────────────────────────────────────────────┐
  │  Daytona MicroVM (EU region — egress filter blocks NVIDIA TLS) │
  │                                                                 │
  │  orchestrator.py  (agent-brain, port 9000, SQLite state.db)     │
  │       │  urllib POST http://localhost:7777/v1/chat/completions  │
  │       │  Authorization: Bearer tunnel-injected  (DUMMY — ignored)│
  │       ▼                                                         │
  │  tunnel_client.py (tunnel-client, port 7777)  ←── THIS FILE     │
  │       │  aiohttp HTTP server on 127.0.0.1:7777                  │
  │       │  strips Authorization/Host/Content-Length               │
  │       │  websockets WS client ──────────────────┐              │
  │       └──────────────────────────────────────────┼──────────────┘
  │                                                  │              │
  └──────────────────────────────────────────────────┼──────────────┘
                                                     │
                               (WS traffic — Daytona filter ALLOWS)
                                                     │
  ┌──────────────────────────────────────────────────▼──────────────┐
  │  ArcForge Backend (Render, US region — unblocked)               │
  │  /api/tunnel  WS endpoint (Task 10-a)                            │
  │       │  authenticates via X-Agent-Token: <AGENT_PROXY_SECRET>   │
  │       │  INJECTS the real NVIDIA API key (server-side only)     │
  │       │  multiplexes N in-flight VM requests by uuid id         │
  │       ▼                                                          │
  │  NVIDIA NIM (integrate.api.nvidia.com) — US egress, unblocked   │
  │       │  streams response chunks back down the WS                │
  └──────────────────────────────────────────────────────────────────┘

DIRECT MODE IS DISABLED
  The VM has NO NVIDIA key (by design — it must never leave the backend).
  So the only viable path is through the WS tunnel. TUNNEL_DIRECT_NVIDIA_URL
  is read at startup purely as a connectivity hint (the installer may probe
  whether the VM CAN reach NVIDIA, to log region diagnostics), but it never
  enables direct forwarding — direct mode would require the real key in the
  VM, which the architecture forbids.

WEBSOCKET TUNNEL PROTOCOL (text JSON frames — MUST match host side exactly)
  Connect:  WS upgrade to {TUNNEL_BACKEND_WS_URL}/api/tunnel
            header  X-Agent-Token: {TUNNEL_TOKEN}
  req    :  {t:"req",   id, method, path, headers, body}   VM→backend
  res    :  {t:"res",   id, status, headers}               backend→VM
  chunk  :  {t:"chunk", id, body}                            backend→VM (≥1)
  done   :  {t:"done",  id}                                  backend→VM
  error  :  {t:"error", id, message}                         backend→VM
  ping   :  {t:"ping"}  →  {t:"pong"}          (either side; keepalive)

  The VM is the CLIENT (it dials out to the backend). The body field in
  req/chunk frames is a UTF-8 string (JSON text frames — OpenAI payloads
  are JSON text, so this is safe; binary bodies would need a different
  transport but are never needed for chat completions).

TLS FINGERPRINT SPOOFING (Cloudflare bypass)
  The backend's /api/tunnel WS endpoint is fronted by Cloudflare, which
  applies bot detection on the JA3/JA4 TLS fingerprint of incoming TLS
  Client Hello messages. Python's stdlib `ssl` module (used by `websockets`
  and `aiohttp` clients) produces a Python-specific fingerprint that
  Cloudflare flags as a bot, then resets the TCP connection right after
  the Client Hello ("Connection reset by peer" within ~24ms of the
  connect syscall — BEFORE any HTTP data is sent).

  To bypass this, the WS dial is performed via `curl_cffi.requests.AsyncSession`
  configured with `impersonate="chrome"`. curl_cffi uses libcurl's
  `CURLOPT_SSL_ENABLE_ALPN` + a handcrafted TLS Client Hello that exactly
  matches Chrome's JA3/JA4 fingerprint (cipher ordering, extensions,
  supported_groups list). Cloudflare sees a Chrome client and lets the
  WS upgrade through. The HTTP/2 streaming behavior is preserved (text
  frames still flow bidirectionally).

  This was previously the failure mode: the in-VM agent's localhost:7777
  POST to /v1/chat/completions was returning "LLM HTTP 502: tunnel send
  error: tunnel WS not connected" because the WS dial to wss://...onrender.com
  was reset by Cloudflare. After this fix, the WS connects cleanly and
  the in-VM agent's LLM calls bridge through to NVIDIA end-to-end.

DEPENDENCIES
  aiohttp (local HTTP server), curl_cffi (WS client with TLS-fingerprint
  impersonation). Both are installed into the VM by
  app/services/agent_installer.py alongside fastapi+uvicorn.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import uuid
from typing import Any, Dict, List, Optional

import aiohttp
from aiohttp import web

# curl_cffi is the WS transport. Its TLS Client Hello is spoofed to match
# Chrome (impersonate="chrome") so Cloudflare (which fronts Render's TLS)
# does NOT reset the connection as a bot. The stdlib `ssl`-based `websockets`
# library was previously used here and was blocked by Cloudflare's
# JA3/JA4 fingerprint bot detection — see module docstring.
from curl_cffi.requests import AsyncSession
from curl_cffi import CurlError
from curl_cffi.requests.websockets import WebSocketClosed, WebSocketError

# ---------------------------------------------------------------------------
# Configuration (env, read once at startup — injected by the installer)
# ---------------------------------------------------------------------------

TUNNEL_BACKEND_WS_URL = os.environ.get("TUNNEL_BACKEND_WS_URL", "").rstrip("/")
TUNNEL_TOKEN = os.environ.get("TUNNEL_TOKEN", "")
TUNNEL_LISTEN_HOST = os.environ.get("TUNNEL_LISTEN_HOST", "127.0.0.1")
TUNNEL_LISTEN_PORT = int(os.environ.get("TUNNEL_LISTEN_PORT", "7777"))
TUNNEL_REQUEST_TIMEOUT_S = float(os.environ.get("TUNNEL_REQUEST_TIMEOUT_S", "180"))
TUNNEL_CONNECT_TIMEOUT_S = float(os.environ.get("TUNNEL_CONNECT_TIMEOUT_S", "10"))
# Backoff schedule for WS reconnect (seconds). Caps at the last value.
_TUNNEL_BACKOFF = (1, 2, 5, 10, 30)
# The WS path on the backend (appended to TUNNEL_BACKEND_WS_URL).
_TUNNEL_WS_PATH = "/api/tunnel"
# Optional connectivity hint — NEVER enables direct forwarding (see docstring).
TUNNEL_DIRECT_NVIDIA_URL = os.environ.get("TUNNEL_DIRECT_NVIDIA_URL", "")

# Headers stripped from inbound VM requests before tunneling. The backend
# injects the real Authorization (NVIDIA key) server-side; Host and
# Content-Length are hop-by-hop and would mismatch the backend's outbound
# request to NVIDIA.
_HOP_BY_HOP_INBOUND = frozenset(("authorization", "host", "content-length"))
# Headers stripped from the reconstructed HTTP response back to the VM client.
_HOP_BY_HOP_OUTBOUND = frozenset((
    "transfer-encoding", "content-encoding", "content-length",
    "connection", "keep-alive", "upgrade",
))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [tunnel-client] %(levelname)s %(message)s",
    stream=sys.stdout,
)
log = logging.getLogger("tunnel-client")


# ---------------------------------------------------------------------------
# Multiplexer — maps in-flight request ids to asyncio Futures
# ---------------------------------------------------------------------------

class _Inflight:
    """One in-flight HTTP request pending on WS response frames."""
    __slots__ = ("future", "status", "headers", "body_parts")

    def __init__(self) -> None:
        self.future: asyncio.Future = asyncio.get_running_loop().create_future()
        self.status: int = 0
        self.headers: Dict[str, str] = {}
        self.body_parts: List[str] = []


class TunnelMultiplexer:
    """Tracks in-flight HTTP requests pending on WS response frames.

    Each register(req_id) returns an _Inflight whose .future the local HTTP
    handler awaits. WS response frames (res/chunk/done/error) resolve it.
    On WS disconnect, fail_all() errors every pending request so the HTTP
    handler never hangs forever.
    """

    def __init__(self) -> None:
        self._inflight: Dict[str, _Inflight] = {}

    def register(self, req_id: str) -> _Inflight:
        entry = _Inflight()
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
            e.future.set_exception(RuntimeError(f"tunnel: {message}"))

    def cancel(self, req_id: str) -> None:
        self._inflight.pop(req_id, None)

    def fail_all(self, reason: str) -> None:
        """Fail every in-flight request (called on WS disconnect)."""
        for e in self._inflight.values():
            if not e.future.done():
                e.future.set_exception(ConnectionError(reason))
        self._inflight.clear()


# ---------------------------------------------------------------------------
# Tunnel client — persistent WS connection with auto-reconnect + backoff
# ---------------------------------------------------------------------------

class TunnelClient:
    """Maintains ONE persistent WS connection to the backend and dispatches
    response frames to the multiplexer's in-flight futures."""

    def __init__(self) -> None:
        self.mux = TunnelMultiplexer()
        self.ws: Any = None  # websockets connection or None when down
        self.ws_connected: asyncio.Event = asyncio.Event()
        self.ws_connected.clear()
        self._stop: asyncio.Event = asyncio.Event()
        self._send_lock = asyncio.Lock()

    async def send_req(self, frame: Dict[str, Any]) -> None:
        """Send a req frame over the WS; raise if WS is down."""
        async with self._send_lock:
            if self.ws is None:
                raise ConnectionError("tunnel WS not connected")
            # send_str → text frame (matches our JSON text protocol).
            # curl_cffi handles the underlying libcurl WS frame write.
            await self.ws.send_str(json.dumps(frame))

    def stop(self) -> None:
        self._stop.set()

    async def run_ws_loop(self) -> None:
        """Maintain the WS connection forever with backoff reconnect."""
        if not TUNNEL_BACKEND_WS_URL:
            log.error("TUNNEL_BACKEND_WS_URL not set — WS loop will not start")
            return
        if not TUNNEL_TOKEN:
            log.error("TUNNEL_TOKEN not set — backend will reject the WS")
        attempt = 0
        while not self._stop.is_set():
            try:
                await self._ws_session()
            except asyncio.CancelledError:
                break
            except Exception as exc:  # noqa: BLE001 — reconnect on any error
                log.warning("ws session ended: %s", exc)
            finally:
                self.ws = None
                self.ws_connected.clear()
                self.mux.fail_all("ws disconnected")
            if self._stop.is_set():
                break
            delay = _TUNNEL_BACKOFF[min(attempt, len(_TUNNEL_BACKOFF) - 1)]
            attempt += 1
            log.info("reconnecting ws in %ds (attempt %d)", delay, attempt)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
                break  # stop signaled during backoff sleep
            except asyncio.TimeoutError:
                pass

    async def _ws_session(self) -> None:
        """One WS connection attempt. Uses curl_cffi with Chrome TLS
        impersonation so Cloudflare's bot detection does NOT reset the
        TLS Client Hello (see module docstring for the failure mode this
        fixes).
        """
        url = f"{TUNNEL_BACKEND_WS_URL}{_TUNNEL_WS_PATH}"
        headers = {"X-Agent-Token": TUNNEL_TOKEN}
        log.info("connecting ws (curl_cffi, impersonate=chrome) -> %s", url)
        # AsyncSession carries the impersonation profile (Chrome JA3/JA4).
        # ws_connect on the session returns an AsyncWebSocketContext that
        # we use as an async context manager; on __aenter__ it performs the
        # WS upgrade (TLS Client Hello with Chrome's fingerprint) and yields
        # the live AsyncWebSocket handle.
        session = AsyncSession(impersonate="chrome")
        try:
            async with session.ws_connect(
                url,
                headers=headers,
                timeout=int(TUNNEL_CONNECT_TIMEOUT_S),
            ) as ws:
                self.ws = ws
                self.ws_connected.set()
                log.info("ws connected (chrome JA3 bypassed cloudflare)")
                try:
                    while True:
                        try:
                            # recv_str blocks until the next text frame.
                            # On graceful close, raises WebSocketClosed.
                            # On a transport-level failure, raises CurlError
                            # (or WebSocketError subclass). Either way we
                            # break out and let the reconnect loop re-dial.
                            raw = await ws.recv_str()
                        except WebSocketClosed:
                            log.info("ws closed gracefully by peer")
                            break
                        except (WebSocketError, CurlError) as exc:
                            log.warning("ws transport error: %s", exc)
                            break
                        except Exception as exc:  # noqa: BLE001 — defensive
                            log.warning("ws recv failed: %s", exc)
                            break
                        if not raw:
                            continue
                        await self._on_frame(raw)
                finally:
                    self.ws = None
                    self.ws_connected.clear()
                    # Best-effort close — connection may already be gone.
                    try:
                        await ws.close()
                    except Exception:  # noqa: BLE001
                        pass
        finally:
            try:
                await session.close()
            except Exception:  # noqa: BLE001
                pass

    async def _on_frame(self, raw: Any) -> None:
        if isinstance(raw, (bytes, bytearray)):
            try:
                raw = raw.decode("utf-8")
            except UnicodeDecodeError:
                log.warning("dropping non-utf8 binary frame")
                return
        try:
            data = json.loads(raw)
        except json.JSONDecodeError:
            log.warning("dropping malformed frame: %.200s", raw)
            return
        t = data.get("t")
        req_id = data.get("id")
        if t == "res":
            self.mux.on_res(req_id, data.get("status", 200), data.get("headers", {}))
        elif t == "chunk":
            self.mux.on_chunk(req_id, data.get("body", ""))
        elif t == "done":
            self.mux.on_done(req_id)
        elif t == "error":
            self.mux.on_error(req_id, data.get("message", "unknown"))
        elif t == "ping":
            if self.ws is not None:
                try:
                    await self.ws.send(json.dumps({"t": "pong"}))
                except Exception:  # noqa: BLE001
                    pass
        elif t == "pong":
            pass
        else:
            log.debug("ignoring unknown frame t=%s", t)


# ---------------------------------------------------------------------------
# Local HTTP server (aiohttp) — the VM's "AI endpoint" on localhost:7777
# ---------------------------------------------------------------------------

_tunnel: Optional[TunnelClient] = None  # set in _main()


async def _handle(request: web.Request) -> web.StreamResponse:
    """Proxy any inbound HTTP request through the WS tunnel."""
    tun = _tunnel

    # Local-only health check — does NOT proxy through the WS (used by the
    # installer to confirm the tunnel is up before marking install success).
    if request.path == "/__tunnel_health":
        connected = tun is not None and tun.ws_connected.is_set()
        return web.json_response(
            {"ws": "connected" if connected else "disconnected"},
            status=200 if connected else 503,
        )

    if tun is None:
        return web.Response(status=503, text="tunnel not initialized")

    # Full path + query string — the backend reconstructs the URL to NVIDIA
    # from this (e.g. /v1/chat/completions).
    path = request.path
    if request.query_string:
        path = f"{path}?{request.query_string}"

    # Strip hop-by-hop + Authorization headers (backend injects the real key).
    headers = {
        k: v for k, v in request.headers.items()
        if k.lower() not in _HOP_BY_HOP_INBOUND
    }

    body_bytes = await request.read()
    # JSON text frames carry the body as a UTF-8 string. OpenAI chat payloads
    # are JSON text, so this is safe. Binary bodies would need a different
    # transport (base64) — not needed for chat completions.
    body_str = body_bytes.decode("utf-8", "replace")

    req_id = uuid.uuid4().hex
    frame: Dict[str, Any] = {
        "t": "req",
        "id": req_id,
        "method": request.method,
        "path": path,
        "headers": headers,
        "body": body_str,
    }

    entry = tun.mux.register(req_id)
    try:
        await tun.send_req(frame)
    except Exception as exc:  # noqa: BLE001 — WS is down; error the HTTP req
        tun.mux.cancel(req_id)
        return web.Response(status=502, text=f"tunnel send error: {exc}")

    try:
        await asyncio.wait_for(entry.future, timeout=TUNNEL_REQUEST_TIMEOUT_S)
    except asyncio.TimeoutError:
        tun.mux.cancel(req_id)
        return web.Response(status=504, text="tunnel request timed out")
    except Exception as exc:  # noqa: BLE001 — error frame / disconnect
        return web.Response(status=502, text=f"tunnel error: {exc}")

    status = entry.status or 502
    out_headers = {
        k: v for k, v in entry.headers.items()
        if k.lower() not in _HOP_BY_HOP_OUTBOUND
    }
    body = "".join(entry.body_parts).encode("utf-8")

    resp = web.StreamResponse(status=status, headers=out_headers)
    resp.content_length = len(body)
    await resp.prepare(request)
    await resp.write(body)
    await resp.write_eof()
    return resp


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def _main() -> None:
    global _tunnel
    if not TUNNEL_BACKEND_WS_URL:
        log.error("TUNNEL_BACKEND_WS_URL is required — exiting")
        sys.exit(1)

    _tunnel = TunnelClient()

    # Start the WS reconnect loop as a background task (runs until _stop).
    ws_task = asyncio.create_task(_tunnel.run_ws_loop())

    # Local HTTP server — catch-all for any path/method.
    app = web.Application(client_max_size=10 * 1024 * 1024)
    app.router.add_route("*", "/", _handle)
    app.router.add_route("*", "/{tail:.*}", _handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, TUNNEL_LISTEN_HOST, TUNNEL_LISTEN_PORT)
    await site.start()
    log.info("http listening on %s:%d", TUNNEL_LISTEN_HOST, TUNNEL_LISTEN_PORT)

    # Graceful SIGTERM/SIGINT — stop the HTTP server, close the WS, exit 0.
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    def _request_stop() -> None:
        log.info("shutdown signal received")
        _tunnel.stop()
        stop_event.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except (NotImplementedError, RuntimeError):
            # add_signal_handler is not available on all platforms (Windows,
            # some embedded loops) — fall back to the stdlib signal handler.
            try:
                signal.signal(sig, lambda *_: _request_stop())
            except (ValueError, OSError):
                pass

    try:
        await stop_event.wait()
    finally:
        log.info("stopping http server")
        await runner.cleanup()
        log.info("stopping ws loop")
        ws_task.cancel()
        try:
            await ws_task
        except (asyncio.CancelledError, Exception):  # noqa: BLE001
            pass
        log.info("bye")


if __name__ == "__main__":
    try:
        asyncio.run(_main())
    except KeyboardInterrupt:
        pass
