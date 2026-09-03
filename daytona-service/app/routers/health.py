"""Health and readiness endpoints."""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.config import settings
from app.daytona_client import get_daytona
from app.models import HealthResponse

logger = logging.getLogger(__name__)
router = APIRouter(tags=["health"])


@router.get(
    "/",
    summary="Root — service heartbeat",
)
async def root():
    """Return a simple JSON heartbeat so the root URL doesn't 404."""
    return {
        "service": "Arcforge Daytona Service",
        "version": settings.service_version,
        "status": "operational",
    }


@router.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
)
async def health_check() -> HealthResponse:
    """Return service health.  Always 200."""
    return HealthResponse(
        status="healthy",
        version=settings.service_version,
        daytona_connected=False,
        active_sandboxes=0,
    )


@router.get(
    "/ready",
    response_model=HealthResponse,
    summary="Readiness probe (checks Daytona connectivity)",
)
async def readiness_check() -> HealthResponse:
    """Return service readiness.  200 only if Daytona API is reachable.

    SIDE-EFFECT-FREE BY DESIGN (incident 2026-08-26): an UNFILTERED
    ``daytona.list()`` marks every listed sandbox as active (a bare list
    call refreshes ``lastActivityAt`` on all sandboxes within ~5 s),
    which permanently defeats ``autoStopInterval`` — idle workspaces
    accumulated until the org memory quota (10 GiB) was exhausted and
    every new sandbox failed with "Total memory limit exceeded",
    surfacing as "the studio page doesn't connect". This probe now uses
    a labels-filtered query that matches nothing real: it still proves
    API reachability + key validity, but touches no real sandbox. The
    true sandbox count / manual cleanup lives at POST /workspace/reap-idle.
    """
    import asyncio

    from daytona_sdk import ListSandboxesQuery

    daytona_connected = False

    try:
        daytona = get_daytona()
        probe_query = ListSandboxesQuery(labels={"probe": "arcforge-readiness"})

        def _probe() -> list:
            # Materialise the iterator — `len()` on a bare generator raises
            # TypeError (the old code set connected=True before a len() that
            # always threw, silently reporting connected=true / count=0).
            return list(daytona.list(probe_query))

        await asyncio.to_thread(_probe)
        daytona_connected = True
    except Exception:
        logger.warning("Daytona readiness check failed", exc_info=True)

    resp = HealthResponse(
        status="ready" if daytona_connected else "degraded",
        version=settings.service_version,
        daytona_connected=daytona_connected,
        # Deliberately not counted: an unfiltered count would refresh
        # activity on every sandbox. Use POST /workspace/reap-idle for
        # the real inventory.
        active_sandboxes=0,
    )

    if not daytona_connected:
        raise HTTPException(status_code=503, detail=resp.model_dump())

    return resp
