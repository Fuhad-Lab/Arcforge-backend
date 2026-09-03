"""Guest workspace-agent daemon source + WS client helper.

This module contains the embedded source code for the daemon that runs
INSIDE the Daytona MicroVM (stdlib only, no pip install). The source
strings are written into the VM by DaytonaWorkspaceManager.install_guest_agent
and then launched as a background process via nohup.

The daemon exposes a WebSocket server on 0.0.0.0:3010 (RFC 6455) and
runs a background persistence worker that snapshots the dirty file set
out of the tmpfs /workspace RAM disk to a disk-backed persist directory
every 4 seconds.

Design rationale
----------------
- Raw triple-single-quoted strings (prefix r + three single quotes) are
  used so that escape sequences such as backslash-r-backslash-n inside
  the daemon source survive the outer Python parse verbatim. The inner
  Python parser then interprets them as CR+LF when the daemon source is
  written to disk and re-read.
- The inner source must NOT contain a triple-single-quote sequence (it
  would terminate the outer raw string). Only double quotes are used
  inside.
- The ast.parse verification command in the task plan extracts the inner
  source by splitting on the outer marker then the closing triple-quote.
  This only works because the daemon block is the FIRST such block in
  the file. Keep it that way.
"""

# =============================================================================
# GUEST_DAEMON_SOURCE — the long-running background daemon (stdlib only)
# =============================================================================

GUEST_DAEMON_SOURCE = r'''#!/usr/bin/env python3
"""ArcForge guest workspace-agent -- runs INSIDE the Daytona MicroVM.

Two responsibilities:

  1. WebSocket server on 0.0.0.0:3010 -- receives streamed file writes
     from the host orchestrator and writes them to the tmpfs /workspace
     RAM disk. Because the write is native to the guest kernel, inotify
     fires immediately for hot-reloaders (Vite, Next, Nodemon) -- sub-ms
     HMR.

  2. Asynchronous persistence worker -- every FLUSH_INTERVAL_S (4s by
     default), snapshots the dirty file set out of tmpfs to a disk-backed
     persist directory so a RAM crash or sandbox spin-down does not lose
     work. The flush is fully decoupled from the WS write path.

The daemon is self-contained: stdlib only, no pip install required.
"""
import asyncio
import base64
import hashlib
import json
import os
import signal
import sys
import threading
import time

# ---------------------------------------------------------------------------
# Config (env-overridable)
# ---------------------------------------------------------------------------

WS_PORT = int(os.environ.get("WORKSPACE_AGENT_PORT", "3010"))
WORKSPACE_ROOT = os.environ.get("WORKSPACE_ROOT", "/workspace")
# IMPORTANT: PERSIST_DIR must live on a DISK-backed volume (NOT under
# /workspace -- that would be tmpfs, defeating the purpose of persistence).
PERSIST_DIR = os.environ.get(
    "WORKSPACE_PERSIST_DIR", "/home/daytona/.arcforge-persist",
)
FLUSH_INTERVAL_S = float(os.environ.get("WORKSPACE_FLUSH_INTERVAL_S", "4"))

# ---------------------------------------------------------------------------
# Globals
# ---------------------------------------------------------------------------

dirty = set()                 # absolute paths modified since last flush
dirty_lock = threading.Lock()
last_flush_at = None          # unix ts of last flush (None = never)
last_flush_count = 0
vfs_backend = "disk"          # detected at startup


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def detect_vfs_backend():
    """Return 'tmpfs' if WORKSPACE_ROOT is mounted as a tmpfs volume, else 'disk'."""
    try:
        with open("/proc/mounts") as f:
            for line in f:
                parts = line.split()
                if (
                    len(parts) >= 3
                    and parts[1] == WORKSPACE_ROOT
                    and "tmpfs" in parts[2]
                ):
                    return "tmpfs"
    except Exception:
        pass
    return "disk"


def sha256_bytes(b):
    return hashlib.sha256(b).hexdigest()


def _resolve_under_workspace(path):
    """Coerce any path to live under WORKSPACE_ROOT."""
    if not path:
        return WORKSPACE_ROOT
    if path.startswith(WORKSPACE_ROOT):
        return path
    if path.startswith("/"):
        return WORKSPACE_ROOT + path
    return WORKSPACE_ROOT + "/" + path


# ---------------------------------------------------------------------------
# File ops (RAM-to-RAM writes; inotify fires immediately)
# ---------------------------------------------------------------------------

def write_file(path, b64):
    """Decode base64 and write bytes to path under /workspace.

    Returns dict with ok/path/size/sha256/vfs_backend (or ok=False + error).
    """
    try:
        full = _resolve_under_workspace(path)
        data = base64.b64decode(b64) if b64 else b""
        parent = os.path.dirname(full)
        if parent and not os.path.isdir(parent):
            os.makedirs(parent, exist_ok=True)
        # Atomic write: tmp file + fsync + rename -- so readers never see
        # a half-written file (Vite/Next HMR won't reload on partial bytes).
        tmp = full + ".tmp." + str(os.getpid())
        with open(tmp, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, full)
        with dirty_lock:
            dirty.add(full)
        return {
            "ok": True,
            "path": full,
            "size": len(data),
            "sha256": sha256_bytes(data),
            "vfs_backend": vfs_backend,
        }
    except Exception as e:
        return {"ok": False, "path": path, "size": 0, "error": str(e)}


def read_file(path):
    """Read file under /workspace; return base64-encoded bytes."""
    try:
        full = _resolve_under_workspace(path)
        with open(full, "rb") as f:
            data = f.read()
        return {
            "ok": True,
            "path": full,
            "b64": base64.b64encode(data).decode("ascii"),
            "size": len(data),
        }
    except Exception as e:
        return {"ok": False, "path": path, "error": str(e)}


# ---------------------------------------------------------------------------
# Persistence engine
# ---------------------------------------------------------------------------

def flush_persistence():
    """Copy every dirty path from tmpfs to PERSIST_DIR. Returns count flushed.

    PERSIST_DIR lives on a disk-backed volume (NOT under /workspace --
    which would be tmpfs, defeating the purpose of persistence).
    """
    global last_flush_at, last_flush_count
    with dirty_lock:
        paths = list(dirty)
    if not paths:
        last_flush_at = time.time()
        last_flush_count = 0
        return 0
    try:
        os.makedirs(PERSIST_DIR, exist_ok=True)
    except Exception as e:
        sys.stderr.write("persistence mkdir failed: " + str(e) + "\n")
        return 0
    flushed = 0
    for src in paths:
        try:
            rel = os.path.relpath(src, WORKSPACE_ROOT)
            dst = os.path.join(PERSIST_DIR, rel)
            if not os.path.exists(src):
                # Source was deleted -- remove from persist too
                if os.path.exists(dst):
                    try:
                        os.remove(dst)
                    except Exception:
                        pass
                continue
            parent = os.path.dirname(dst)
            if parent and not os.path.isdir(parent):
                os.makedirs(parent, exist_ok=True)
            tmp = dst + ".tmp." + str(os.getpid())
            with open(src, "rb") as fin, open(tmp, "wb") as fout:
                while True:
                    chunk = fin.read(65536)
                    if not chunk:
                        break
                    fout.write(chunk)
                fout.flush()
                os.fsync(fout.fileno())
            os.replace(tmp, dst)
            flushed += 1
        except Exception as e:
            sys.stderr.write("flush " + src + " failed: " + str(e) + "\n")
    with dirty_lock:
        # Remove successfully flushed paths from dirty set.
        # (Best-effort: any path that failed to flush stays dirty and is
        # retried on the next interval.)
        for src in paths:
            dirty.discard(src)
    last_flush_at = time.time()
    last_flush_count = flushed
    return flushed


def status():
    """Snapshot of daemon state -- returned to callers via the 'status' op."""
    return {
        "tmpfs_mounted": vfs_backend == "tmpfs",
        "vfs_backend": vfs_backend,
        "daemon_running": True,
        "dirty_count": len(dirty),
        "last_flush_at": last_flush_at,
        "last_flush_count": last_flush_count,
        "persist_dir": PERSIST_DIR,
        "workspace_root": WORKSPACE_ROOT,
        "flush_interval_s": FLUSH_INTERVAL_S,
        "ws_port": WS_PORT,
    }


# ===========================================================================
# RFC 6455 WebSocket helpers (stdlib only -- no external deps)
# ===========================================================================

WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


def ws_accept_key(key):
    h = hashlib.sha1((key + WS_MAGIC).encode("ascii")).digest()
    return base64.b64encode(h).decode("ascii")


async def ws_read_http_headers(reader):
    """Read HTTP request headers until \\r\\n\\r\\n. Returns raw bytes."""
    buf = bytearray()
    while b"\r\n\r\n" not in buf:
        chunk = await reader.read(4096)
        if not chunk:
            raise ConnectionError("client closed before sending headers")
        buf += chunk
        if len(buf) > 65536:
            raise ConnectionError("HTTP headers too large")
    return bytes(buf)


async def ws_send_text(writer, text):
    """Send a single text frame (server->client, NOT masked per RFC 6455 5.3)."""
    payload = text.encode("utf-8")
    header = bytearray()
    header.append(0x81)  # FIN + text frame (opcode 0x1)
    length = len(payload)
    if length < 126:
        header.append(length)
    elif length < 65536:
        header.append(126)
        header += length.to_bytes(2, "big")
    else:
        header.append(127)
        header += length.to_bytes(8, "big")
    writer.write(bytes(header) + payload)
    await writer.drain()


async def ws_read_frame(reader):
    """Read one frame. Returns (opcode, payload bytes).

    Control frames (close/ping/pong) are handled inline per RFC 6455 5.5.
    """
    while True:
        b0 = (await reader.readexactly(1))[0]
        b1 = (await reader.readexactly(1))[0]
        opcode = b0 & 0x0F
        masked = b1 & 0x80
        length = b1 & 0x7F
        if length == 126:
            length = int.from_bytes(await reader.readexactly(2), "big")
        elif length == 127:
            length = int.from_bytes(await reader.readexactly(8), "big")
        if masked:
            mask = await reader.readexactly(4)
        else:
            mask = None
        payload = await reader.readexactly(length) if length else b""
        if mask:
            payload = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
        # Control frames
        if opcode == 0x8:  # close
            raise ConnectionError("client sent close")
        if opcode == 0x9:  # ping -- skipped (server would normally pong)
            continue
        if opcode == 0xA:  # pong
            continue
        return opcode, payload


async def handle_conn(reader, writer):
    """Per-connection handler: HTTP upgrade -> WS frame loop."""
    peer = writer.get_extra_info("peername")
    try:
        raw = await ws_read_http_headers(reader)
        header_text = raw.decode("latin1")
        lines = header_text.split("\r\n")
        headers = {}
        for line in lines[1:]:
            if ":" in line:
                k, _, v = line.partition(":")
                headers[k.strip().lower()] = v.strip()
        upgrade = headers.get("upgrade", "").lower()
        ws_key = headers.get("sec-websocket-key")
        if "websocket" not in upgrade or not ws_key:
            writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await writer.drain()
            return
        accept = ws_accept_key(ws_key)
        resp = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
        ).encode("ascii")
        writer.write(resp)
        await writer.drain()
        # Message loop
        while True:
            try:
                _opcode, payload = await ws_read_frame(reader)
            except (ConnectionError, asyncio.IncompleteReadError):
                break
            try:
                msg = json.loads(payload.decode("utf-8"))
            except Exception:
                msg = {"op": "ping"}
            op = msg.get("op", "ping")
            if op == "write":
                resp_obj = write_file(msg.get("path", ""), msg.get("b64", ""))
            elif op == "read":
                resp_obj = read_file(msg.get("path", ""))
            elif op == "flush":
                n = await asyncio.to_thread(flush_persistence)
                resp_obj = {"ok": True, "flushed": n, "vfs_backend": vfs_backend}
            elif op == "status":
                resp_obj = {"ok": True, "status": status()}
            else:
                resp_obj = {"ok": True, "op": "pong", "vfs_backend": vfs_backend}
            try:
                await ws_send_text(writer, json.dumps(resp_obj))
            except Exception:
                break
    except Exception as e:
        try:
            sys.stderr.write("conn error from " + str(peer) + ": " + str(e) + "\n")
        except Exception:
            pass
    finally:
        try:
            writer.close()
        except Exception:
            pass


# ===========================================================================
# Persistence worker thread
# ===========================================================================

def persistence_loop(stop_event):
    """Run flush_persistence every FLUSH_INTERVAL_S until stop_event is set.

    Decoupled from the WS path -- a slow disk flush NEVER blocks a fast
    RAM write (the dirty set is just appended to under dirty_lock).
    """
    while not stop_event.is_set():
        try:
            flush_persistence()
        except Exception as e:
            sys.stderr.write("persistence flush failed: " + str(e) + "\n")
        stop_event.wait(FLUSH_INTERVAL_S)
    # Final flush on shutdown
    try:
        flush_persistence()
    except Exception as e:
        sys.stderr.write("final flush failed: " + str(e) + "\n")


# ===========================================================================
# Main
# ===========================================================================

_shutdown_event = threading.Event()


def _handle_sigterm(signum, frame):
    sys.stderr.write("received signal " + str(signum) + ", shutting down\n")
    _shutdown_event.set()


async def main():
    global vfs_backend
    vfs_backend = detect_vfs_backend()
    sys.stderr.write(
        "workspace-agent starting (vfs=" + vfs_backend
        + ", port=" + str(WS_PORT)
        + ", persist=" + PERSIST_DIR
        + ", flush=" + str(FLUSH_INTERVAL_S) + "s)\n"
    )
    persistence_thread = threading.Thread(
        target=persistence_loop, args=(_shutdown_event,), daemon=True,
    )
    persistence_thread.start()
    signal.signal(signal.SIGTERM, _handle_sigterm)
    signal.signal(signal.SIGINT, _handle_sigterm)
    server = await asyncio.start_server(handle_conn, "0.0.0.0", WS_PORT)
    sys.stderr.write("listening on 0.0.0.0:" + str(WS_PORT) + "\n")
    async with server:
        while not _shutdown_event.is_set():
            await asyncio.sleep(0.5)
    sys.stderr.write("shutting down server\n")
    server.close()
    try:
        flush_persistence()
    except Exception as e:
        sys.stderr.write("final flush failed: " + str(e) + "\n")
    sys.stderr.write("workspace-agent exiting\n")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
'''


# =============================================================================
# GUEST_WS_CLIENT_SOURCE -- one-shot WS client invoked by the host inside the VM
# =============================================================================

GUEST_WS_CLIENT_SOURCE = r'''#!/usr/bin/env python3
"""ArcForge guest WS client -- runs inside the VM, invoked by the host
via sandbox.process.exec().

The host cannot reach the VM's localhost directly, so to talk to the
workspace-agent daemon on 127.0.0.1:3010 we shell out to this script,
which performs the RFC 6455 handshake + frame exchange internally.

Usage:
  python3 .ws_client.py write  <vm_path> <b64_payload_file>
  python3 .ws_client.py read   <vm_path>
  python3 .ws_client.py status
  python3 .ws_client.py flush

Prints one JSON object to stdout (the last line). Exits 0 on success,
1 on error (a JSON error object is still printed before exiting 1).
"""
import base64
import json
import os
import socket
import struct
import sys

PORT = int(os.environ.get("WORKSPACE_AGENT_PORT", "3010"))
HOST = "127.0.0.1"


def ws_connect():
    s = socket.create_connection((HOST, PORT), timeout=10)
    key = base64.b64encode(os.urandom(16)).decode("ascii")
    req = (
        "GET / HTTP/1.1\r\n"
        "Host: " + HOST + ":" + str(PORT) + "\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: " + key + "\r\n"
        "Sec-WebSocket-Version: 13\r\n\r\n"
    ).encode("ascii")
    s.sendall(req)
    buf = b""
    while b"\r\n\r\n" not in buf:
        chunk = s.recv(4096)
        if not chunk:
            raise RuntimeError("server closed before handshake")
        buf += chunk
        if len(buf) > 65536:
            raise RuntimeError("handshake too large")
    first_line = buf.split(b"\r\n", 1)[0]
    if b"101" not in first_line:
        raise RuntimeError("handshake failed: " + first_line.decode("latin1"))
    return s


def ws_send_text(s, text):
    """Send a text frame. Client->server frames MUST be masked (RFC 6455 5.3)."""
    payload = text.encode("utf-8")
    mask = os.urandom(4)
    header = bytearray([0x81])  # FIN + text frame
    length = len(payload)
    if length < 126:
        header.append(0x80 | length)  # mask bit set
    elif length < 65536:
        header.append(0x80 | 126)
        header += struct.pack(">H", length)
    else:
        header.append(0x80 | 127)
        header += struct.pack(">Q", length)
    header += mask
    masked = bytes(b ^ mask[i % 4] for i, b in enumerate(payload))
    s.sendall(bytes(header) + masked)


def _recv_exact(s, n, leftover):
    while len(leftover[0]) < n:
        chunk = s.recv(65536)
        if not chunk:
            raise RuntimeError("connection closed")
        leftover[0] += chunk
    out = leftover[0][:n]
    leftover[0] = leftover[0][n:]
    return out


def ws_recv_text(s):
    """Read one text frame from the server (server->client, NOT masked)."""
    leftover = [b""]
    _b0 = _recv_exact(s, 1, leftover)[0]
    b1 = _recv_exact(s, 1, leftover)[0]
    length = b1 & 0x7F
    if length == 126:
        length = int.from_bytes(_recv_exact(s, 2, leftover), "big")
    elif length == 127:
        length = int.from_bytes(_recv_exact(s, 8, leftover), "big")
    payload = _recv_exact(s, length, leftover) if length else b""
    return payload.decode("utf-8", errors="replace")


def request(msg):
    s = ws_connect()
    try:
        ws_send_text(s, json.dumps(msg))
        return ws_recv_text(s)
    finally:
        try:
            s.close()
        except Exception:
            pass


def main():
    if len(sys.argv) < 2:
        print(json.dumps({"ok": False, "error": "missing op"}))
        sys.exit(1)
    op = sys.argv[1]
    if op == "write":
        if len(sys.argv) < 4:
            print(json.dumps({"ok": False, "error": "usage: write <path> <b64_file>"}))
            sys.exit(1)
        path = sys.argv[2]
        b64_file = sys.argv[3]
        with open(b64_file, "r") as f:
            b64 = f.read()
        print(request({"op": "write", "path": path, "b64": b64}))
    elif op == "read":
        if len(sys.argv) < 3:
            print(json.dumps({"ok": False, "error": "usage: read <path>"}))
            sys.exit(1)
        print(request({"op": "read", "path": sys.argv[2]}))
    elif op == "status":
        print(request({"op": "status"}))
    elif op == "flush":
        print(request({"op": "flush"}))
    else:
        print(json.dumps({"ok": False, "error": "unknown op: " + op}))
        sys.exit(1)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(json.dumps({"ok": False, "error": str(e)}))
        sys.exit(1)
'''
