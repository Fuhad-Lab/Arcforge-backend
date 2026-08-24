"""Orchestrator router -- exposes the EnterpriseOrchestrationEngine
(Module 5 Python orchestrator) over HTTP.

Mounts under ``/api/v1/orchestrate`` (alongside the existing routers).

Endpoints:
    POST /api/v1/orchestrate
        Body: {user_prompt: str, user_id: str | None, project_id: str | None}
        Runs the full ``execute_orchestration_pipeline_loop`` in a
        worker thread via ``asyncio.to_thread`` (300s timeout).
        Returns the orchestrator result envelope.

    GET /api/v1/orchestrate/{sandbox_id}/status
        Best-effort introspection of the orchestrator state for a
        sandbox. Returns ``{running: false}`` if the sandbox is not
        tracked by any active engine instance.

No auth (the daytona-service is internal-only; never exposed to the
public internet directly). Per the Task 1 architecture: frontend ->
edge functions -> backend (Node) -> daytona-service (Python).
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.orchestration_engine import (
    EnterpriseOrchestrationEngine,
    default_engine,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/orchestrate", tags=["orchestrator"])


# ---------------------------------------------------------------------------
# Request / response models
# ---------------------------------------------------------------------------


class OrchestrateRequest(BaseModel):
    """Request body for POST /api/v1/orchestrate."""

    user_prompt: str = Field(
        min_length=1,
        description="The user's natural-language app prompt (e.g. 'build a todo app').",
    )
    user_id: str | None = Field(
        default=None,
        description="Owner user UUID -- every sandbox must be attributed to a user.",
    )
    project_id: str | None = Field(
        default=None,
        description="Project UUID -- if None, the orchestrator generates one.",
    )


class OrchestrateResponse(BaseModel):
    """Response shape for POST /api/v1/orchestrate.

    Mirrors the ``execute_orchestration_pipeline_loop`` return dict,
    with a few additional fields for the API gateway.
    """

    sandbox_id: str | None = None
    project_id: str | None = None
    user_id: str | None = None
    vfs_backend: str = "disk"
    agent_installed: bool = False
    iterations: int = 0
    final_audit: dict[str, Any] | None = None
    blueprint: dict[str, Any] | None = None
    status: str = "skipped"
    files_written: list[dict[str, Any]] = Field(default_factory=list)
    stream_failures: list[dict[str, Any]] = Field(default_factory=list)
    duration_ms: int | None = None


class OrchestratorStatusResponse(BaseModel):
    """Best-effort orchestrator status for a sandbox."""

    running: bool = False
    state: str = "unknown"
    sandbox_id: str | None = None
    project_id: str | None = None
    user_id: str | None = None
    vfs_backend: str = "disk"
    agent_installed: bool = False
    stream_failures: int = 0


# ---------------------------------------------------------------------------
# Active engine registry (for GET /status)
# ---------------------------------------------------------------------------
# Tracks the engine instance per sandbox_id so the GET endpoint can
# report live state. Engines register themselves here when provisioned.
# This is a best-effort introspection -- if the service restarts, the
# registry is empty (production callers should treat the orchestrator
# as fire-and-forget; the backend TS server tracks full state).

_ACTIVE_ENGINES: dict[str, EnterpriseOrchestrationEngine] = {}


def _register_engine(engine: EnterpriseOrchestrationEngine) -> None:
    if engine.workspace_id:
        _ACTIVE_ENGINES[engine.workspace_id] = engine


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------


@router.post(
    "",
    response_model=OrchestrateResponse,
    summary="Run the full Module 5 orchestration pipeline for a user prompt",
    description=(
        "Provisions a Daytona sandbox with tmpfs + guest WS daemon, "
        "generates an architect blueprint, codegens placeholder backend + "
        "frontend files, serves the dev servers, runs a Playwright audit, "
        "and (on failure) replans + re-codegens + re-audits up to 2 more "
        "times. All blocking Daytona SDK calls run in a worker thread via "
        "asyncio.to_thread -- the FastAPI event loop never blocks."
    ),
)
async def orchestrate(req: OrchestrateRequest) -> OrchestrateResponse:
    """Run the full Module 5 orchestration pipeline for a user prompt."""
    import time

    # Use a fresh engine instance per request so multiple requests can
    # run in parallel without state bleed. The default_engine singleton
    # is also exported for callers that want a shared instance.
    engine = EnterpriseOrchestrationEngine(
        project_id=req.project_id,
        user_id=req.user_id,
    )

    # If project_id was not supplied, generate one now (the orchestrator
    # itself also has this fallback, but we surface it in the response).
    project_id = req.project_id or f"orch-{uuid.uuid4().hex[:12]}"

    t0 = time.monotonic()
    try:
        # Run the blocking orchestrator in a worker thread with a 300s
        # timeout (per the spec). The orchestrator methods are SYNC
        # (matching the user's spec which uses time.sleep, not await).
        result = await asyncio.wait_for(
            asyncio.to_thread(
                engine.execute_orchestration_pipeline_loop,
                req.user_prompt,
            ),
            timeout=300.0,
        )
    except asyncio.TimeoutError:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.error(
            "orchestrate: pipeline timed out after 300s for project=%s user=%s",
            project_id, req.user_id,
        )
        raise HTTPException(
            status_code=504,
            detail=(
                "Orchestration pipeline timed out after 300s. This is "
                "most likely the Playwright Chromium install on a fresh "
                "sandbox (first audit takes ~60-90s). Retry -- the install "
                "is cached and subsequent audits are fast."
            ),
        ) from None
    except Exception as exc:
        elapsed = int((time.monotonic() - t0) * 1000)
        logger.exception(
            "orchestrate: pipeline crashed for project=%s user=%s: %s",
            project_id, req.user_id, exc,
        )
        raise HTTPException(
            status_code=500,
            detail=f"Orchestration pipeline crashed: {exc}",
        ) from exc

    elapsed = int((time.monotonic() - t0) * 1000)

    # Register the engine for GET /status introspection.
    _register_engine(engine)

    # Make sure project_id is set on the response even if the orchestrator
    # auto-generated it.
    if not result.get("blueprint"):
        # The orchestrator should always return a blueprint, but defend.
        result["blueprint"] = engine._architect_blueprint(req.user_prompt)

    return OrchestrateResponse(
        sandbox_id=result.get("sandbox_id"),
        project_id=project_id,
        user_id=req.user_id,
        vfs_backend=result.get("vfs_backend", "disk"),
        agent_installed=result.get("agent_installed", False),
        iterations=result.get("iterations", 0),
        final_audit=result.get("final_audit"),
        blueprint=result.get("blueprint"),
        status=result.get("status", "skipped"),
        files_written=result.get("files_written", []),
        stream_failures=result.get("stream_failures", []),
        duration_ms=elapsed,
    )


@router.get(
    "/{sandbox_id}/status",
    response_model=OrchestratorStatusResponse,
    summary="Best-effort introspection of the orchestrator state for a sandbox",
)
async def orchestrator_status(sandbox_id: str) -> OrchestratorStatusResponse:
    """Return the current orchestrator state for a sandbox.

    Returns ``{running: false}`` if the sandbox is not tracked by any
    active engine instance (e.g. the service has been restarted since
    the orchestrate call).
    """
    engine = _ACTIVE_ENGINES.get(sandbox_id)
    if engine is None:
        return OrchestratorStatusResponse(
            running=False,
            state="not_tracked",
            sandbox_id=sandbox_id,
        )
    status = engine.status()
    return OrchestratorStatusResponse(
        running=status.get("running", False),
        state=status.get("state", "unknown"),
        sandbox_id=status.get("sandbox_id", sandbox_id),
        project_id=status.get("project_id"),
        user_id=status.get("user_id"),
        vfs_backend=status.get("vfs_backend", "disk"),
        agent_installed=status.get("agent_installed", False),
        stream_failures=status.get("stream_failures", 0),
    )
