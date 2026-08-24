"""Business logic for sandbox lifecycle operations.

All Daytona SDK calls are centralised here so that routers stay thin.

SDK API:
  daytona.create(params, timeout=60) → Sandbox
  daytona.get(sandbox_id_or_name) → Sandbox
  daytona.list() → list[Sandbox]
  daytona.delete(sandbox: Sandbox, timeout=60, wait=False) → None
  daytona.start(sandbox: Sandbox, timeout=60) → None
  daytona.stop(sandbox: Sandbox, timeout=60) → None
  sandbox.start(timeout=60) → None
  sandbox.stop(timeout=60, force=False) → None
  sandbox.wait_for_sandbox_start(timeout=60) → None
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

from app.config import settings
from app.daytona_client import (
    build_create_params,
    extract_sandbox_data,
    get_daytona,
)
from app.models import (
    BulkActionResponse,
    CreateSandboxRequest,
    SandboxListResponse,
    SandboxResponse,
    SandboxState,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Create
# ---------------------------------------------------------------------------


async def create_sandbox(req: CreateSandboxRequest) -> SandboxResponse:
    """Provision a new Daytona sandbox and return its description."""
    daytona = get_daytona()

    # Parse memory/disk from string (e.g. "4Gi") to float (4.0)
    cpu = req.resources.cpu if req.resources else settings.default_cpu
    memory = _parse_resource_size(
        req.resources.memory if req.resources else settings.default_memory
    )
    disk = _parse_resource_size(
        req.resources.disk if req.resources else settings.default_disk
    )

    params = build_create_params(
        method=req.method.value,
        language=req.language.value if req.language else None,
        image=req.image,
        name=req.name,
        cpu=cpu,
        memory=memory,
        disk=disk,
        labels=req.labels or None,
        env_vars=req.env_vars or None,
        auto_stop_interval=settings.sandbox_idle_timeout_seconds,
    )

    logger.info("Creating sandbox (method=%s, lang=%s) …", req.method, req.language)
    t0 = time.monotonic()

    try:
        sandbox = await asyncio.to_thread(
            daytona.create, params=params,
            timeout=settings.daytona_default_timeout,
        )
    except Exception as exc:
        logger.exception("Failed to create sandbox")
        raise _wrap("sandbox creation", exc) from exc

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    logger.info("Sandbox %s created in %d ms", sandbox.id, elapsed_ms)

    return _to_response(extract_sandbox_data(sandbox))


# ---------------------------------------------------------------------------
# Read / List
# ---------------------------------------------------------------------------


async def get_sandbox(sandbox_id: str) -> SandboxResponse:
    daytona = get_daytona()
    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
    except Exception as exc:
        raise _wrap(f"get sandbox {sandbox_id}", exc) from exc
    return _to_response(extract_sandbox_data(sandbox))


async def list_sandboxes(states: list[str] | None = None) -> SandboxListResponse:
    """List sandboxes.

    By default the Daytona SDK's list() only returns sandboxes in the
    default states (started etc.) — stopped/archived/error sandboxes are
    INVISIBLE even though they still hold CPU quota against the org limit
    ("Total CPU limit exceeded" on create). Callers that need the full
    picture (cleanup jobs, quota debugging) pass the full state list.
    """
    daytona = get_daytona()
    try:
        if states:
            from daytona import ListSandboxesQuery

            query = ListSandboxesQuery(states=states)  # type: ignore[arg-type]
            sandboxes = await asyncio.to_thread(lambda: list(daytona.list(query)))
        else:
            sandboxes = await asyncio.to_thread(lambda: list(daytona.list()))
    except Exception as exc:
        raise _wrap("list sandboxes", exc) from exc

    items = [_to_response(extract_sandbox_data(s)) for s in sandboxes]
    return SandboxListResponse(items=items, total=len(items))


# State buckets that still consume quota or block cleanup.
ALL_STATES = [
    "creating", "restoring", "started", "starting", "stopping", "stopped",
    "error", "build_failed", "pending_build", "building_snapshot",
    "pulling_snapshot", "archived", "archiving", "snapshotting", "forking",
    "pausing", "paused", "resuming", "resizing", "unknown",
]


async def list_all_sandboxes() -> SandboxListResponse:
    """List sandboxes in EVERY state (used by cleanup + quota debugging)."""
    return await list_sandboxes(ALL_STATES)


# ---------------------------------------------------------------------------
# State transitions
# ---------------------------------------------------------------------------


async def start_sandbox(sandbox_id: str) -> SandboxResponse:
    daytona = get_daytona()
    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        await asyncio.to_thread(sandbox.start, 60)
    except Exception as exc:
        raise _wrap(f"start sandbox {sandbox_id}", exc) from exc
    # Re-fetch to get fresh state
    return await get_sandbox(sandbox_id)


async def stop_sandbox(sandbox_id: str) -> SandboxResponse:
    daytona = get_daytona()
    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        await asyncio.to_thread(sandbox.stop, 60, False)
    except Exception as exc:
        raise _wrap(f"stop sandbox {sandbox_id}", exc) from exc
    return await get_sandbox(sandbox_id)


# ---------------------------------------------------------------------------
# Delete
# ---------------------------------------------------------------------------


async def delete_sandbox(sandbox_id: str) -> None:
    """Delete a sandbox by ID.

    SDK: daytona.delete(sandbox: Sandbox, timeout=60, wait=False)
    Must fetch the Sandbox object first, then pass it to delete.
    """
    daytona = get_daytona()
    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        await asyncio.to_thread(daytona.delete, sandbox, 60, False)
    except Exception as exc:
        raise _wrap(f"delete sandbox {sandbox_id}", exc) from exc
    logger.info("Sandbox %s deleted", sandbox_id)


# ---------------------------------------------------------------------------
# Bulk operations
# ---------------------------------------------------------------------------


async def bulk_stop(sandbox_ids: list[str]) -> BulkActionResponse:
    result = BulkActionResponse()
    for sid in sandbox_ids:
        try:
            await stop_sandbox(sid)
            result.succeeded.append(sid)
        except Exception as exc:
            result.failed[sid] = str(exc)
    return result


async def bulk_delete(sandbox_ids: list[str]) -> BulkActionResponse:
    result = BulkActionResponse()
    for sid in sandbox_ids:
        try:
            await delete_sandbox(sid)
            result.succeeded.append(sid)
        except Exception as exc:
            result.failed[sid] = str(exc)
    return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wrap(operation: str, exc: Exception) -> RuntimeError:
    msg = f"Daytona {operation} failed: {exc}"
    return RuntimeError(msg)


def _parse_resource_size(val: str | float) -> float:
    """Parse resource size strings like '4Gi', '512Mi' to float GB."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    if s.endswith("gi"):
        return float(s[:-2])
    if s.endswith("mi"):
        return float(s[:-2]) / 1024
    if s.endswith("g"):
        return float(s[:-1])
    if s.endswith("m"):
        return float(s[:-1]) / 1024
    return float(s)


def _to_response(data: dict[str, Any]) -> SandboxResponse:
    state_str = str(data.get("state", "Unknown"))
    try:
        state = SandboxState(state_str.upper())
    except ValueError:
        state = SandboxState.ERROR

    from datetime import datetime

    created_at = data.get("created_at")
    if isinstance(created_at, str):
        try:
            created_at = datetime.fromisoformat(created_at)
        except (ValueError, TypeError):
            created_at = None

    from app.models import ResourcesResponse

    resources = ResourcesResponse(
        cpu=float(data.get("cpu") or 0),
        memory=str(data.get("memory") or ""),
        disk=str(data.get("disk") or ""),
        gpu=float(data.get("gpu") or 0),
    )

    return SandboxResponse(
        id=str(data.get("id", "")),
        name=data.get("name"),
        state=state,
        target=data.get("target"),
        resources=resources,
        labels=data.get("labels") or {},
        env=data.get("env") or {},
        created_at=created_at,
        error=data.get("error_reason"),
        recoverable=data.get("recoverable"),
    )
