#!/usr/bin/env python3
"""ArcForge Yjs Bridge — CRDT multiplayer file sync (the Replit experience).

THE MANDATE (2026-10-30, user architecture directive):
    "As the agent writes code inside the VM, Yjs syncs those file diffs in
     real-time to the code editor component in the custom frontend. If the
     user decides to type in the same file while the AI is writing, Yjs
     resolves the conflict mathematically without locking the file or
     overwriting either party's work."

DESIGN
    One shared YDoc per VM, hosted on the sidecar daemon (same port 9000 /
    same signed preview URL the studio already brokers via agent-info —
    endpoint: /yjs, optionally /yjs/<room>; room names are cosmetic, the
    daemon serves a single workspace doc).

    Doc layout:  doc.getMap('files')  →  { "<workspace-relative path>":
    YText(content) }  — exactly the shape the studio's YjsSync client
    binds its CodeMirror editors to.

    • AGENT writes: the orchestrator calls note_file(rel, old, new) on
      every write_workspace_file. The doc is updated with the old→new
      edit MAPPED onto the current (possibly user-edited) content, so
      concurrent USER edits outside the changed region survive — that is
      the CRDT contract.
    • USER edits: arrive as y-websocket updates on /yjs, are applied to
      the doc, and a debounced disk-flush materialises the merged doc
      content back to /workspace (scoped, traversal-guarded) + emits the
      standard `files` event so every other surface (tree, preview)
      refreshes.

    THREAD MODEL (critical): y-py's YDoc is thread-AFFINE (pyo3 !Send —
    verified: cross-thread access panics "YDoc is unsendable"). The doc
    is therefore created on and OWNED BY one dedicated worker thread; every
    access (agent writes from the task thread, sync frames from the event
    loop, flush scans) is submitted to that single writer. Broadcasts are
    handed back to the asyncio loop via run_coroutine_threadsafe.

    PROTOCOL: standard y-websocket binary framing (lib0 varuint):
        message types: 0 = SYNC, 1 = AWARENESS (relayed), 3 = QUERY_AWARENESS
        sync subtypes: 0 = step1 (state vector), 1 = step2 (update), 2 = update
    On connect the server sends SyncStep1(its SV); on receiving Step1 it
    replies Step2(diff vs the client's SV); Step2/Update payloads are
    applied and broadcast to the other clients. Non-sync frames are
    relayed verbatim (awareness/cursors).

    DEPENDENCY: `pip install y-py` (loaded guardedly — absent y-py leaves
    the endpoint answering 1011 and the studio falls back to its plain
    editor; the daemon itself never depends on this module).

    y-py API COMPATIBILITY: y-py's method signatures moved across versions
    (transaction-explicit → implicit) and its state codecs are module
    level (0.6) vs methods (elsewhere). Every call goes through shims
    that try each form, so the bridge runs on either generation.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import json
import logging
import os
import queue as _queue
import secrets
import threading
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

log = logging.getLogger("yjs-bridge")

TOKEN = os.environ.get("ORCH_TOKEN", "")

try:
    import y_py as Y
    Y_AVAILABLE = True
except Exception as _exc:  # noqa: BLE001 — optional dependency
    Y = None  # type: ignore[assignment]
    Y_AVAILABLE = False
    log.warning("y-py unavailable (%s) — CRDT sync disabled", _exc)

try:
    from fastapi import WebSocket
except Exception:  # pragma: no cover — fastapi is a hard dep of the daemon
    WebSocket = None  # type: ignore[assignment]

# ── Injected by the orchestrator at load time ─────────────────────────────
_emit_fn: Optional[Callable[[Dict[str, Any]], None]] = None
_workspace = "/workspace"
_allowed_prefixes: Tuple[str, ...] = ("frontend", "backend")

# ── Bridge state ───────────────────────────────────────────────────────────
_doc: Any = None                    # the shared Y.YDoc (owned by _worker)
_worker: Optional["_DocWorker"] = None
_loop: Optional[asyncio.AbstractEventLoop] = None
_clients: List[Any] = []            # connected WebSocket objects
_flush_timer: Optional[threading.Timer] = None
_MAX_FILE_BYTES = 256 * 1024
_MAX_TOTAL_BYTES = 6 * 1024 * 1024
_MAX_FILES = 600
_SKIP_DIRS = {"node_modules", ".next", ".git", "__pycache__", ".venv",
              "dist", "build", ".turbo", "coverage"}
_TEXT_EXTS = (".tsx", ".ts", ".jsx", ".js", ".py", ".mjs", ".cjs", ".css",
              ".scss", ".html", ".json", ".md", ".txt", ".yml", ".yaml",
              ".toml", ".cfg", ".sh", ".env", ".gitignore", ".prisma")


class _DocWorker:
    """The single writer that owns the YDoc (y-py is thread-affine —
    verified: cross-thread access panics). submit() serialises every doc
    operation onto this thread."""

    def __init__(self) -> None:
        self._q: "_queue.Queue[Tuple[Optional[Callable[[], Any]], Optional[concurrent.futures.Future]]]" = _queue.Queue()
        self.thread = threading.Thread(target=self._run, daemon=True,
                                       name="yjs-crdt")
        self.thread.start()

    def _run(self) -> None:
        while True:
            fn, fut = self._q.get()
            if fn is None:
                if fut is not None:
                    fut.set_result(None)
                return
            try:
                res = fn()
                if fut is not None:
                    fut.set_result(res)
            except BaseException as exc:  # noqa: BLE001 — must not kill the writer
                if fut is not None:
                    fut.set_exception(exc)

    def submit(self, fn: Callable[[], Any], wait: bool = True) -> Any:
        fut: Optional[concurrent.futures.Future] = (
            concurrent.futures.Future() if wait else None)
        self._q.put((fn, fut))
        return fut.result() if fut is not None else None

    def stop(self) -> None:
        self._q.put((None, None))


def init(emit_fn: Callable[[Dict[str, Any]], None], workspace: str,
         allowed_prefixes: Tuple[str, ...] = ("frontend", "backend")) -> None:
    """Wire the bridge to the orchestrator (called once at daemon load)."""
    global _emit_fn, _workspace, _allowed_prefixes, _doc, _worker
    if not Y_AVAILABLE:
        return
    _emit_fn = emit_fn
    _workspace = workspace
    _allowed_prefixes = tuple(allowed_prefixes)
    _worker = _DocWorker()

    def _create() -> None:
        global _doc
        _doc = Y.YDoc()

    _worker.submit(_create)
    log.info("yjs bridge initialised (workspace=%s, single-writer thread)",
             _workspace)


# ── y-py compatibility shims (all run ON the worker thread) ───────────────

def _with_txn(fn: Callable[[Any], Any]) -> Any:
    """Run fn(txn) inside a transaction; y-py versions without explicit
    transactions call fn(None)."""
    try:
        cm = _doc.begin_transaction()
    except Exception:  # noqa: BLE001 — auto-txn y-py
        return fn(None)
    try:
        with cm as txn:
            return fn(txn)
    except TypeError:
        return fn(None)


def _try_candidates(candidates) -> None:
    """Run the first y-py API candidate that works (covers txn-explicit
    0.6 and auto-txn 0.7 method signatures)."""
    last: Optional[Exception] = None
    for cand in candidates:
        try:
            cand()
            return
        except (TypeError, AttributeError) as exc:
            last = exc
    if last is not None:
        raise last


def _text_insert(ytext: Any, index: int, text: str) -> None:
    def txn_form(txn: Any) -> None:
        _try_candidates((
            (lambda: ytext.insert(txn, index, text)) if txn is not None
            else (lambda: ytext.insert(index, text)),
            (lambda: ytext.insert(index, text)) if txn is not None
            else (lambda: ytext.insert(txn, index, text)),
        ))
    _with_txn(txn_form)


def _text_delete(ytext: Any, index: int, length: int) -> None:
    """Delete `length` chars at `index` — ACROSS y-py API generations.

    Live incident 2026-09-02 ("the writes that never happened"): the
    shipped candidates only covered delete(txn, i, len) (0.6.0-style) and
    delete(i, len) (0.7-style). The VMs install y-py 0.6.2, whose
    YText.delete(txn, index) removes exactly ONE character and takes NO
    length — both candidates raised TypeError, every note_file containing
    a deletion failed, the doc stayed stale, and the debounced flush then
    materialised the STALE doc over the agent's disk writes (three files
    reverted to the template in one 21:23:37.277 batch write — verified
    by mtime forensics). The single-char fallback below closes that gap:
    when the length forms are unavailable we delete one character at a
    time, BACKWARDS so the indices stay valid."""
    def txn_form(txn: Any) -> None:
        if txn is not None:
            cands = (
                lambda: ytext.delete(txn, index, length),
                lambda: ytext.delete(index, length),
                lambda: _delete_single_chars(ytext, txn, index, length),
            )
        else:
            cands = (
                lambda: ytext.delete(index, length),
                lambda: _delete_single_chars(ytext, None, index, length),
            )
        _try_candidates(cands)
    _with_txn(txn_form)


def _delete_single_chars(ytext: Any, txn: Any, index: int, length: int) -> None:
    """y-py 0.6.2 fallback: delete(txn, i) removes one character."""
    for i in range(index + length - 1, index - 1, -1):
        if txn is not None:
            ytext.delete(txn, i)
        else:
            ytext.delete(i)


def _map_set(ymap: Any, key: str, value: Any) -> None:
    def txn_form(txn: Any) -> None:
        _try_candidates((
            (lambda: ymap.set(txn, key, value)) if txn is not None
            else (lambda: ymap.set(key, value)),
            (lambda: ymap.__setitem__(key, value)),
            (lambda: ymap.set(key, value)) if txn is not None
            else (lambda: ymap.set(txn, key, value)),
        ))
    _with_txn(txn_form)


def _map_delete(ymap: Any, key: str) -> None:
    def txn_form(txn: Any) -> None:
        _try_candidates((
            (lambda: ymap.pop(txn, key, None)) if txn is not None
            else (lambda: ymap.pop(key, None)),
            (lambda: ymap.delete(txn, key)) if txn is not None
            else (lambda: ymap.delete(key)),
            (lambda: ymap.pop(key, None)),
            (lambda: ymap.pop(txn, key, None)) if txn is not None
            else (lambda: ymap.pop(key, None)),
        ))
    _with_txn(txn_form)


def _text_str(ytext: Any) -> str:
    """Raw content of a YText (str() is the un-quoted form; to_json()
    wraps it in JSON quotes — verified against y-py 0.6.2)."""
    try:
        return str(ytext)
    except Exception:  # noqa: BLE001
        return ytext.to_json()


def _doc_state() -> bytes:
    """State vector of the shared doc (y-py 0.6: module-level)."""
    try:
        return Y.encode_state_vector(_doc)
    except (TypeError, AttributeError):
        return _doc.get_state()  # type: ignore[union-attr]


def _doc_diff(sv: Optional[bytes] = None) -> bytes:
    """Update bytes since sv (full state when sv is None/empty)."""
    try:
        return Y.encode_state_as_update(_doc, sv)
    except (TypeError, AttributeError):
        return _doc.diff(sv)  # type: ignore[union-attr]


def _doc_apply(update: bytes) -> None:
    try:
        Y.apply_update(_doc, update)
    except (TypeError, AttributeError):
        _doc.apply_update(update)  # type: ignore[union-attr]


def _files_map() -> Any:
    return _doc.get_map("files")


def _map_keys() -> List[str]:
    """Keys of the files map (YMap.keys() in y-py 0.6; to_json() is a
    JSON-ENCODED STRING, not a dict — verified). The KeyView BORROWS the
    YMap wrapper, so the map must stay referenced while iterating (a
    temporary segfaults — found by faulthandler)."""
    ymap = _files_map()
    try:
        return list(ymap.keys())
    except Exception:  # noqa: BLE001
        try:
            return list(json.loads(ymap.to_json()).keys())
        except Exception:  # noqa: BLE001
            return []


def _get_ytext(key: str) -> Optional[Any]:
    """The YText for a path (None when absent or not a shared type)."""
    if key not in _map_keys():
        return None
    try:
        val = _files_map().get(key)
    except Exception:  # noqa: BLE001
        return None
    if Y is not None and isinstance(val, Y.YText):
        return val
    return None


# ── Patch machinery (merge-preserving agent writes) ─────────────────────────

def _common_affix(a: str, b: str) -> Tuple[int, int]:
    """(common prefix len, common suffix len) of two strings."""
    p = 0
    min_l = min(len(a), len(b))
    while p < min_l and a[p] == b[p]:
        p += 1
    s = 0
    while s < min_l - p and a[len(a) - 1 - s] == b[len(b) - 1 - s]:
        s += 1
    return p, s


def _map_pos(p: int, old: str, cur: str, pref: int, suff: int) -> int:
    """Map an offset in the AGENT'S base (old) onto the CURRENT doc text
    (cur), which may carry concurrent user edits. Edits before the user's
    region keep their offset; after it, they shift by the user's net
    delta; inside it, they clamp to the region start."""
    if p <= pref:
        return p
    if p >= len(old) - suff:
        return p + (len(cur) - len(old))
    return pref


def _apply_patch(ytext: Any, old: str, new: str) -> None:
    """Apply the old→new edit to ytext. When ytext has drifted from old
    (the user typed meanwhile), the edit's offsets are POSITION-MAPPED so
    the agent's change lands in the right place and the user's concurrent
    edit survives — the multiplayer contract."""
    cur = _text_str(ytext)
    if cur == new:
        return
    if cur == old:
        # no concurrent edits — plain minimal-diff replace
        p, s = _common_affix(old, new)
        del_len = len(old) - p - s
        ins = new[p: len(new) - s]
        if del_len > 0:
            _text_delete(ytext, p, del_len)
        if ins:
            _text_insert(ytext, p, ins)
        return
    # concurrent user edits present — map the agent's offsets onto cur
    p, s = _common_affix(old, new)
    del_len = len(old) - p - s
    ins = new[p: len(new) - s]
    pref, suff = _common_affix(old, cur)
    start = _map_pos(p, old, cur, pref, suff)
    end = _map_pos(p + del_len, old, cur, pref, suff)
    if end > start:
        _text_delete(ytext, start, end - start)
    if ins:
        _text_insert(ytext, start, ins)


# ── Broadcast plumbing ─────────────────────────────────────────────────────

def _send_to_all(data: bytes) -> None:
    """Fire-and-forget binary frame to every connected client (thread-safe;
    silently dropped when no loop/clients — late joiners get the full
    diff via the sync handshake)."""
    if not _clients or _loop is None or _loop.is_closed():
        return
    try:
        asyncio.run_coroutine_threadsafe(_broadcast_bytes(data), _loop)
    except Exception:  # noqa: BLE001
        pass


async def _broadcast_bytes(data: bytes) -> None:
    for ws in list(_clients):
        try:
            await ws.send_bytes(data)
        except Exception:  # noqa: BLE001
            pass


def _broadcast_diff_task(sv_before: bytes) -> None:
    """Worker-thread task: compute + broadcast changes since sv_before.
    The update MUST be framed as a y-websocket sync message ([0, sub,
    len, update]) — a bare update is not a valid protocol frame."""
    try:
        update = _doc_diff(sv_before)
    except Exception as exc:  # noqa: BLE001
        log.warning("yjs diff failed: %s", exc)
        return
    if update:
        _send_to_all(_frame_sync(SYNC_UPDATE, update))


# ── Public API (called by the orchestrator from ANY thread) ───────────────

def note_file(rel: str, old_content: Optional[str], new_content: str) -> None:
    """Mirror one agent file-write into the shared doc (merge-preserving)."""
    if not Y_AVAILABLE or _worker is None:
        return
    rel = (rel or "").strip().lstrip("/").replace("\\", "/")
    if not rel or not rel.startswith(_allowed_prefixes):
        return
    if len(new_content or "") > _MAX_FILE_BYTES:
        return                    # oversized files stay out of the CRDT

    def task() -> None:
        sv = _doc_state()
        ytext = _get_ytext(rel)
        if ytext is None:
            fresh = Y.YText()
            if new_content:
                _text_insert(fresh, 0, new_content)
            _map_set(_files_map(), rel, fresh)
        else:
            base = old_content if old_content is not None else _text_str(ytext)
            _apply_patch(ytext, base, new_content)
        _broadcast_diff_task(sv)

    try:
        _worker.submit(task)
    except Exception as exc:  # noqa: BLE001 — never break a build
        log.warning("yjs note_file(%s) failed: %s", rel, exc)


def note_delete(rel: str) -> None:
    if not Y_AVAILABLE or _worker is None:
        return
    rel = (rel or "").strip().lstrip("/").replace("\\", "/")

    def task() -> None:
        if rel in _map_keys():
            sv = _doc_state()
            _map_delete(_files_map(), rel)
            _broadcast_diff_task(sv)

    try:
        _worker.submit(task)
    except Exception as exc:  # noqa: BLE001
        log.warning("yjs note_delete(%s) failed: %s", rel, exc)


def boot_load() -> None:
    """Preload every workspace source file into the doc so the first
    connecting editor receives the full tree through the sync handshake."""
    if not Y_AVAILABLE or _worker is None:
        return
    files = _read_workspace_files()
    loaded = 0

    def task() -> None:
        nonlocal loaded
        for rel, content in files:
            if content is None:
                continue
            try:
                if rel not in _map_keys():
                    fresh = Y.YText()
                    if content:
                        _text_insert(fresh, 0, content)
                    _map_set(_files_map(), rel, fresh)
                    loaded += 1
            except Exception as exc:  # noqa: BLE001
                log.warning("yjs boot_load(%s) failed: %s", rel, exc)

    try:
        _worker.submit(task)
    except Exception as exc:  # noqa: BLE001
        log.warning("yjs boot_load failed: %s", exc)
    if files:
        log.info("yjs bridge: %d/%d workspace files loaded into the doc",
                 loaded, len(files))


def _read_workspace_files() -> List[Tuple[str, Optional[str]]]:
    """(rel, content|None) for every candidate file — None = skipped.
    Runs on the CALLER's thread (pure filesystem reads, no doc access)."""
    out: List[Tuple[str, Optional[str]]] = []
    budget = _MAX_TOTAL_BYTES
    for root_name in _allowed_prefixes:
        base = os.path.join(_workspace, root_name)
        if not os.path.isdir(base):
            continue
        for dirpath, dirnames, filenames in os.walk(base):
            dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
            for fn in sorted(filenames):
                if len(out) >= _MAX_FILES:
                    return out
                if not (fn.endswith(_TEXT_EXTS) or fn in
                        ("package.json", "requirements.txt", "Dockerfile")):
                    continue
                full = os.path.join(dirpath, fn)
                rel = os.path.relpath(full, _workspace).replace(os.sep, "/")
                try:
                    size = os.path.getsize(full)
                    if size > _MAX_FILE_BYTES or size > budget:
                        out.append((rel, None))
                        continue
                    with open(full, "r", encoding="utf-8", errors="replace") as fh:
                        content = fh.read()
                    budget -= len(content)
                    out.append((rel, content))
                except OSError:
                    out.append((rel, None))
    return out


def health() -> Dict[str, Any]:
    if not Y_AVAILABLE or _worker is None:
        return {"available": False, "clients": len(_clients), "files": 0}
    try:
        n = _worker.submit(lambda: len(_map_keys()))
    except Exception:  # noqa: BLE001
        n = 0
    return {"available": True, "clients": len(_clients), "files": int(n or 0)}


# ── Debounced disk flush (user edits → the VM filesystem) ──────────────────

_FLUSH_DELAY_S = 1.5
_flush_lock = threading.Lock()

# Files whose DOC content changed because of a CLIENT update (a user
# edit in a connected editor). ONLY these are eligible for the disk
# flush — agent writes are already on disk (write_workspace_file writes
# directly) and must NEVER be "healed" backwards by a stale doc. Live
# incident 2026-09-02: note_file failed (y-py delete signature), the doc
# kept the template, a client update scheduled the flush, and the flush
# REVERTED three agent-written files to the template in one batch write.
# The doc-as-source-of-truth healing is now scoped to client edits —
# exactly the case the flush exists for.
_client_dirty: set = set()
_dirty_lock = threading.Lock()


def _mark_client_dirty(keys) -> None:
    with _dirty_lock:
        _client_dirty.update(keys)


def _snapshot_hashes() -> Dict[str, int]:
    """Worker-thread task: {rel: hash(content)} for the whole doc."""
    out: Dict[str, int] = {}
    for key in _map_keys():
        ytext = _get_ytext(key)
        if ytext is not None:
            out[key] = hash(_text_str(ytext))
    return out


def _schedule_flush() -> None:
    global _flush_timer
    with _flush_lock:
        if _flush_timer is not None:
            _flush_timer.cancel()
        _flush_timer = threading.Timer(_FLUSH_DELAY_S, _flush_disk)
        _flush_timer.daemon = True
        _flush_timer.start()


def _safe_disk_path(rel: str) -> Optional[str]:
    rel = (rel or "").strip().lstrip("/").replace("\\", "/")
    if not rel or ".." in rel.split("/"):
        return None
    if not rel.startswith(_allowed_prefixes):
        return None
    dest = os.path.normpath(os.path.join(_workspace, rel))
    if not dest.startswith(_workspace + os.sep):
        return None
    return dest


def _flush_disk() -> None:
    """Materialise CLIENT-EDITED doc content onto the disk.

    Scope (incident 2026-09-02): only files whose doc content changed via
    a client update are flushed — that is the flush's one job (user edits
    → disk). Agent file writes go to disk directly and are mirrored into
    the doc by note_file; if that mirror ever fails, the flush must NOT
    "heal" the file back to the stale doc — that path is what reverted
    live agent work. Non-dirty files are left untouched no matter how
    far doc and disk have drifted (the next note_file or user edit
    re-converges them)."""
    if not Y_AVAILABLE or _worker is None:
        return

    def snapshot() -> Dict[str, str]:
        snap: Dict[str, str] = {}
        for key in _map_keys():
            ytext = _get_ytext(key)
            if ytext is not None:
                snap[key] = _text_str(ytext)
        return snap

    try:
        snap = _worker.submit(snapshot)
    except Exception as exc:  # noqa: BLE001
        log.warning("yjs flush snapshot failed: %s", exc)
        return
    with _dirty_lock:
        dirty = set(_client_dirty)
        _client_dirty.clear()
    changed: List[Dict[str, str]] = []
    for rel in dirty:
        doc_content = snap.get(rel)
        if doc_content is None:
            continue  # deleted from the doc — nothing to materialise
        dest = _safe_disk_path(rel)
        if dest is None or len(doc_content) > _MAX_FILE_BYTES:
            continue
        try:
            disk = None
            if os.path.exists(dest):
                with open(dest, "r", encoding="utf-8", errors="replace") as fh:
                    disk = fh.read()
            if disk == doc_content:
                continue
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "w", encoding="utf-8") as fh:
                fh.write(doc_content)
            changed.append({"path": dest,
                            "action": "edit" if disk is not None else "create"})
        except OSError as exc:
            log.warning("yjs flush(%s) failed: %s", rel, exc)
    if changed and _emit_fn is not None:
        try:
            _emit_fn({"type": "files", "task_id": None, "files": changed})
        except Exception:  # noqa: BLE001
            pass
    if changed:
        log.info("yjs flush: %d file(s) materialised to disk", len(changed))


# ── The y-websocket binary protocol (lib0 varuint framing) ─────────────────

def _w_varuint(n: int) -> bytes:
    out = bytearray()
    while n > 127:
        out.append(128 | (n & 127))
        n >>= 7
    out.append(n & 127)
    return bytes(out)


def _r_varuint(data: bytes, pos: int) -> Tuple[int, int]:
    num, shift = 0, 0
    while True:
        if pos >= len(data):
            raise ValueError("truncated varuint")
        b = data[pos]
        pos += 1
        num |= (b & 127) << shift
        if b < 128:
            return num, pos
        shift += 7


MSG_SYNC = 0          # message types (y-websocket)
MSG_AWARENESS = 1
SYNC_STEP1 = 0        # sync subtypes
SYNC_STEP2 = 1
SYNC_UPDATE = 2


def _frame_sync(sub: int, payload: bytes) -> bytes:
    return _w_varuint(MSG_SYNC) + _w_varuint(sub) + _w_varuint(len(payload)) + payload


def _parse_frame(data: bytes) -> Optional[Tuple[int, int, bytes]]:
    """(MSG_SYNC, subtype, payload) for sync frames; None for others."""
    try:
        mtype, pos = _r_varuint(data, 0)
        if mtype != MSG_SYNC:
            return None
        sub, pos = _r_varuint(data, pos)
        ln, pos = _r_varuint(data, pos)
        return mtype, sub, data[pos:pos + ln]
    except (ValueError, IndexError):
        return None


# ── The WebSocket endpoint (runs on the event loop; doc ops submitted to
#    the single-writer thread via asyncio.to_thread) ────────────────────────

async def yjs_endpoint(ws: WebSocket, room: str = "workspace") -> None:
    """Standard y-websocket server endpoint. room is cosmetic (one doc per
    VM) but accepted so stock WebsocketProvider URL building
    (…/yjs/<room>) works."""
    global _loop
    if not Y_AVAILABLE or _worker is None:
        await ws.close(code=1011, reason="yjs bridge unavailable (y-py missing)")
        return
    if TOKEN:
        tok = ws.query_params.get("token", "") or ws.headers.get("x-agent-token", "")
        if not secrets.compare_digest(tok, TOKEN):
            await ws.close(code=4401, reason="unauthorized")
            return
    _loop = asyncio.get_running_loop()
    await ws.accept()
    _clients.append(ws)
    log.info("yjs client connected (room=%s, %d total)", room, len(_clients))
    try:
        # Sync handshake: tell the client what WE have (step1 = our SV);
        # the client replies step2 with what we lack, and sends its own
        # step1 which we answer with step2 (what it lacks).
        sv = await asyncio.to_thread(_worker.submit, lambda: _doc_state())
        await ws.send_bytes(_frame_sync(SYNC_STEP1, sv))
        while True:
            raw = await ws.receive()
            if raw.get("type") == "websocket.disconnect":
                break
            data = raw.get("bytes")
            if not data:
                continue                     # text frames are not ours
            parsed = _parse_frame(data)
            if parsed is None:
                await _relay(ws, data)       # awareness etc. — relay raw
                continue
            _, sub, payload = parsed
            if sub == SYNC_STEP1:
                # client state vector → answer with the update it lacks
                def diff_task() -> bytes:
                    try:
                        return _doc_diff(payload)
                    except Exception:  # noqa: BLE001
                        return b""

                update = await asyncio.to_thread(_worker.submit, diff_task)
                if update:
                    await ws.send_bytes(_frame_sync(SYNC_STEP2, update))
            elif sub in (SYNC_STEP2, SYNC_UPDATE):
                # client update → apply, propagate, and schedule the
                # debounced disk flush (the USER edited something).
                # CLIENT-DIRTY TRACKING (incident 2026-09-02): snapshot
                # the doc before/after the update and mark the files the
                # CLIENT actually changed — the flush materialises ONLY
                # those (see _flush_disk). A client that merely replays
                # its stale local state changes nothing and flushes
                # nothing, so it can no longer clobber agent disk writes.
                try:
                    pre = await asyncio.to_thread(_worker.submit,
                                                  _snapshot_hashes)
                except Exception:  # noqa: BLE001
                    pre = {}

                def apply_task() -> None:
                    try:
                        _doc_apply(payload)
                    except Exception as exc:  # noqa: BLE001
                        log.warning("yjs apply_update failed: %s", exc)

                await asyncio.to_thread(_worker.submit, apply_task)
                try:
                    post = await asyncio.to_thread(_worker.submit,
                                                   _snapshot_hashes)
                except Exception:  # noqa: BLE001
                    post = dict(pre)
                touched = {k for k, v in post.items() if pre.get(k) != v}
                if touched:
                    _mark_client_dirty(touched)
                await _relay(ws, data)
                _schedule_flush()
    except Exception as exc:  # noqa: BLE001 — WebSocketDisconnect & friends
        log.debug("yjs client handler ended: %s", exc)
    finally:
        if ws in _clients:
            _clients.remove(ws)
        log.info("yjs client disconnected (%d total)", len(_clients))


async def _relay(sender: Any, data: bytes) -> None:
    """Forward a raw frame to every OTHER client (awareness/cursor state,
    and applied updates so multiple editors stay in lockstep)."""
    for ws in list(_clients):
        if ws is sender:
            continue
        try:
            await ws.send_bytes(data)
        except Exception:  # noqa: BLE001
            pass
