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
    """Return service readiness.  200 only if Daytona API is reachable."""
    import asyncio

    daytona_connected = False
    active_sandboxes = 0

    try:
        daytona = get_daytona()
        sandboxes = await asyncio.to_thread(daytona.list)
        daytona_connected = True
        active_sandboxes = len(sandboxes)
    except Exception:
        logger.warning("Daytona readiness check failed", exc_info=True)

    resp = HealthResponse(
        status="ready" if daytona_connected else "degraded",
        version=settings.service_version,
        daytona_connected=daytona_connected,
        active_sandboxes=active_sandboxes,
    )

    if not daytona_connected:
        raise HTTPException(status_code=503, detail=resp.model_dump())

    return resp
