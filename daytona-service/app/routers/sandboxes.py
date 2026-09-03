"""REST endpoints for sandbox lifecycle management.

All routes are prefixed with ``/api/v1/sandboxes``.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, status

from app.models import (
    BulkActionResponse,
    CreateSandboxRequest,
    SandboxListResponse,
    SandboxResponse,
)
from app.services import sandbox_service

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sandboxes", tags=["sandboxes"])


# ---------------------------------------------------------------------------
# POST /sandboxes
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=SandboxResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a new sandbox",
    responses={
        201: {"description": "Sandbox created successfully"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def create_sandbox(req: CreateSandboxRequest) -> SandboxResponse:
    try:
        return await sandbox_service.create_sandbox(req)
    except ValueError as exc:
        raise HTTPException(status_code=422, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# GET /sandboxes
# ---------------------------------------------------------------------------


@router.get(
    "",
    response_model=SandboxListResponse,
    summary="List sandboxes (all states with ?all=true)",
)
async def list_sandboxes(all: bool = False) -> SandboxListResponse:
    """List sandboxes.

    By default only default-state sandboxes are returned (SDK behaviour).
    Pass ?all=true to include stopped/archived/error sandboxes — the ones
    that still consume CPU quota and block creation on the free tier.
    """
    try:
        if all:
            return await sandbox_service.list_all_sandboxes()
        return await sandbox_service.list_sandboxes()
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# GET /sandboxes/{sandbox_id}
# ---------------------------------------------------------------------------


@router.get(
    "/{sandbox_id}",
    response_model=SandboxResponse,
    summary="Get sandbox details",
    responses={404: {"description": "Sandbox not found"}},
)
async def get_sandbox(sandbox_id: str) -> SandboxResponse:
    try:
        return await sandbox_service.get_sandbox(sandbox_id)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "does not exist" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


# ---------------------------------------------------------------------------
# POST /sandboxes/{sandbox_id}/start
# ---------------------------------------------------------------------------


@router.post(
    "/{sandbox_id}/start",
    response_model=SandboxResponse,
    summary="Start a stopped sandbox",
    responses={404: {"description": "Sandbox not found"}},
)
async def start_sandbox(sandbox_id: str) -> SandboxResponse:
    try:
        return await sandbox_service.start_sandbox(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# GET /sandboxes/{sandbox_id}/state
# ---------------------------------------------------------------------------


@router.get(
    "/{sandbox_id}/state",
    response_model=SandboxResponse,
    summary="Get sandbox state (lightweight)",
)
async def get_sandbox_state(sandbox_id: str) -> SandboxResponse:
    try:
        return await sandbox_service.get_sandbox(sandbox_id)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "does not exist" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


# ---------------------------------------------------------------------------
# POST /sandboxes/{sandbox_id}/stop
# ---------------------------------------------------------------------------


@router.post(
    "/{sandbox_id}/stop",
    response_model=SandboxResponse,
    summary="Stop a running sandbox",
    responses={404: {"description": "Sandbox not found"}},
)
async def stop_sandbox(sandbox_id: str) -> SandboxResponse:
    try:
        return await sandbox_service.stop_sandbox(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# DELETE /sandboxes/{sandbox_id}
# ---------------------------------------------------------------------------


@router.delete(
    "/{sandbox_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a sandbox",
    responses={
        204: {"description": "Sandbox deleted"},
        404: {"description": "Sandbox not found"},
    },
)
async def delete_sandbox(sandbox_id: str) -> None:
    try:
        await sandbox_service.delete_sandbox(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ---------------------------------------------------------------------------
# POST /sandboxes/bulk/stop
# ---------------------------------------------------------------------------


@router.post(
    "/bulk/stop",
    response_model=BulkActionResponse,
    summary="Stop multiple sandboxes at once",
)
async def bulk_stop(
    sandbox_ids: list[str] = Query(..., alias="id"),
) -> BulkActionResponse:
    return await sandbox_service.bulk_stop(sandbox_ids)


# ---------------------------------------------------------------------------
# POST /sandboxes/bulk/delete
# ---------------------------------------------------------------------------


@router.post(
    "/bulk/delete",
    status_code=status.HTTP_200_OK,
    response_model=BulkActionResponse,
    summary="Delete multiple sandboxes at once",
)
async def bulk_delete(
    sandbox_ids: list[str] = Query(..., alias="id"),
) -> BulkActionResponse:
    return await sandbox_service.bulk_delete(sandbox_ids)
