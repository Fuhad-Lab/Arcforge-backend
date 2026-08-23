import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, status

from app.models import (
    AgentBulkWriteRequest,
    AgentCodeWriteRequest,
    CodeRunResult,
    CreateWorkspaceRequest,
    FileContentResponse,
    FileTreeResponse,
    TerminalCommandRequest,
    WorkspaceInitResponse,
)
from app.services.workspace_coordinator import workspace_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/workspace", tags=["workspace"])


# ======================================================================
# Workspace Lifecycle
# ======================================================================


@router.post(
    "/init",
    response_model=WorkspaceInitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a project workspace with scaffolded directory structure",
    responses={
        201: {"description": "Workspace created and scaffolded"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def create_workspace(req: CreateWorkspaceRequest) -> WorkspaceInitResponse:
    """Spawn a Daytona MicroVM and create the mandatory directory blueprint.

    Creates: /workspace/git/, /workspace/frontend/, /workspace/backend/, /workspace/logo.png
    """
    try:
        data = await workspace_manager.create_and_scaffold_workspace(
            project_id=req.project_id,
            language=req.language,
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return WorkspaceInitResponse(
        sandbox_id=data["id"],
        project_id=req.project_id,
        state=str(data.get("state", "Unknown")),
        provision_time_ms=data.get("provision_time_ms", 0),
    )


@router.delete(
    "/{sandbox_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Destroy a workspace sandbox",
)
async def destroy_workspace(sandbox_id: str) -> None:
    """Permanently destroy the workspace MicroVM."""
    try:
        await workspace_manager.destroy_workspace(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ======================================================================
# Live File Tree
# ======================================================================


@router.get(
    "/{sandbox_id}/file-tree",
    response_model=FileTreeResponse,
    summary="Get live file tree from the VM (for Files Tab sidebar)",
    responses={
        200: {"description": "Nested directory tree from the actual VM filesystem"},
        404: {"description": "Sandbox not found"},
    },
)
async def get_file_tree(
    sandbox_id: str,
    max_depth: int = 4,
) -> dict[str, Any]:
    """Return the live directory tree from the Daytona VM.

    This endpoint is the backend proxy that feeds the frontend Studio
    "Files Tab" sidebar.  It dynamically scans the /workspace folder
    inside the active VM and structures it into a clean nested JSON tree.
    """
    try:
        return await workspace_manager.get_live_ui_file_tree(sandbox_id, max_depth)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


# ======================================================================
# Agent Code Write Operations (Direct VM Writes)
# ======================================================================


@router.post(
    "/{sandbox_id}/write",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Write a single file directly into the VM",
)
async def agent_code_write(sandbox_id: str, req: AgentCodeWriteRequest) -> None:
    """Write code directly into the live Daytona VM filesystem.

    The AI agent calls this instead of writing to static files.
    The path is automatically normalized to /workspace/<path>.
    """
    try:
        await workspace_manager.execute_agent_code_write(
            sandbox_id, req.path, req.content,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/{sandbox_id}/write-bulk",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Write multiple files into the VM in one batch",
)
async def agent_bulk_write(sandbox_id: str, req: AgentBulkWriteRequest) -> None:
    """Bulk-write multiple files into the live VM in a single operation."""
    try:
        await workspace_manager.execute_agent_bulk_write(sandbox_id, req.files)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ======================================================================
# File Read (for agent context window)
# ======================================================================


@router.get(
    "/{sandbox_id}/read",
    response_model=FileContentResponse,
    summary="Read a file from the VM",
)
async def read_file(sandbox_id: str, path: str) -> FileContentResponse:
    """Read a file's content from the VM filesystem."""
    try:
        content = await workspace_manager.read_file_from_vm(sandbox_id, path)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    return FileContentResponse(
        path=path,
        content=content,
        size_bytes=len(content.encode("utf-8")),
    )


# ======================================================================
# Pre-Studio Logo Upload
# ======================================================================


@router.post(
    "/{sandbox_id}/logo",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload project logo to /workspace/logo.png in the VM",
)
async def upload_logo(sandbox_id: str, file: UploadFile) -> None:
    """Intercept the pre-studio logo modal payload and write it to /workspace/logo.png.

    The file buffer is captured and written directly into the VM root.
    """
    content = await file.read()
    if len(content) > 5_000_000:  # 5MB limit
        raise HTTPException(
            status_code=413,
            detail="Logo file must be under 5MB",
        )

    try:
        await workspace_manager.handle_pre_studio_logo(sandbox_id, content)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ======================================================================
# Active Bash Terminal Engine
# ======================================================================


@router.post(
    "/{sandbox_id}/terminal",
    response_model=CodeRunResult,
    summary="Execute a bash command in the VM terminal",
    responses={
        200: {"description": "Command output with exit code for feedback loop"},
        408: {"description": "Command timed out"},
    },
)
async def run_terminal(sandbox_id: str, req: TerminalCommandRequest) -> CodeRunResult:
    """Execute a command in the VM bash shell.

    Returns exit_code + stdout for the continuous autonomous feedback loop.
    The agent uses this for:
    - Running builds (npm run build, pip install)
    - Installing packages
    - Self-correcting debugging via stderr analysis
    """
    try:
        result = await workspace_manager.run_live_terminal_command(
            sandbox_id, req.command, req.cwd, req.timeout_ms,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    if result.timed_out:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=result.stderr or "Command timed out",
        )

    return result
