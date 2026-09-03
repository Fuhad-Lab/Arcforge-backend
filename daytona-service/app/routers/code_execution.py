"""REST endpoints for executing code and shell commands inside sandboxes.

All routes are prefixed with ``/api/v1/sandboxes/{sandbox_id}``.

Two execution modes:
  POST /sandboxes/{id}/code     → process.code_run() for interpreted code
  POST /sandboxes/{id}/exec     → process.exec() for shell commands

Both return CodeRunResult with exit_code + result for the feedback loop.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, status

from app.models import CodeRunRequest, CodeRunResult, ShellExecRequest
from app.services import code_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["code-execution"])


@router.post(
    "/sandboxes/{sandbox_id}/code",
    response_model=CodeRunResult,
    summary="Execute interpreted code (process.code_run)",
    responses={
        200: {"description": "Code executed (check exit_code for success)"},
        404: {"description": "Sandbox not found"},
        408: {"description": "Execution timed out"},
    },
)
async def execute_code(sandbox_id: str, req: CodeRunRequest) -> CodeRunResult:
    """Run interpreted code inside the given sandbox via process.code_run().

    Returns exit_code + stdout for the autonomous feedback loop.
    """
    try:
        result = await code_service.execute_code(sandbox_id, req)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    if result.timed_out:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=result.stderr or "Execution timed out",
        )

    return result


@router.post(
    "/sandboxes/{sandbox_id}/exec",
    response_model=CodeRunResult,
    summary="Execute shell command (process.exec)",
    responses={
        200: {"description": "Command executed (check exit_code for success)"},
        404: {"description": "Sandbox not found"},
        408: {"description": "Execution timed out"},
    },
)
async def execute_shell(sandbox_id: str, req: ShellExecRequest) -> CodeRunResult:
    """Execute a shell command inside the given sandbox via process.exec().

    Returns exit_code + stdout for the autonomous feedback loop.
    This is how the agent runs build tasks, npm install, pip install, etc.
    """
    try:
        result = await code_service.execute_shell(sandbox_id, req)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    if result.timed_out:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=result.stderr or "Command timed out",
        )

    return result
