"""Arcforge Daytona Service - FastAPI application entry point.

Wires together routers, middleware, and lifecycle hooks.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from contextlib import asynccontextmanager, suppress
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.middleware.error_handler import register_error_handlers
from app.routers import (
    code_execution,
    files,
    health,
    orchestrator,
    sandboxes,
    secrets,
    workspace,
)
from app.services.quota_reaper import reap_forever
from app.services.keepalive import keepalive_forever

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

log_level = getattr(logging, settings.log_level.upper(), logging.INFO)
logging.basicConfig(
    level=log_level,
    format="%(asctime)s %(levelname)-8s [%(name)s] %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S%z",
    stream=sys.stdout,
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lifespan
# ---------------------------------------------------------------------------


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    logger.info(
        "Starting %s v%s (debug=%s)",
        settings.service_name,
        settings.service_version,
        settings.debug,
    )
    # Quota hygiene (incident 2026-08-27 "sandbox is full, again"): the
    # reaper existed but was NEVER SCHEDULED — dead code. Idle workspace
    # sandboxes accumulated until the 10 GiB org quota was exhausted and
    # every new build failed. The interval (default 35 min) deliberately
    # exceeds sandbox_idle_timeout_seconds (30 min) so the reaper's own
    # list() calls cannot self-poison the idle clock (see quota_reaper).
    reaper_task = asyncio.create_task(reap_forever())
    # Free-tier spin-down guard: self-ping the public URL so Render never
    # idles the service out (incident 2026-09-03, see keepalive.py).
    keepalive_task = asyncio.create_task(keepalive_forever())
    yield
    reaper_task.cancel()
    keepalive_task.cancel()
    for task in (reaper_task, keepalive_task):
        with suppress(asyncio.CancelledError):
            await task
    logger.info("Shutting down %s", settings.service_name)


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------


def create_app() -> FastAPI:
    application = FastAPI(
        title=settings.service_name,
        version=settings.service_version,
        docs_url="/docs" if settings.debug else None,
        redoc_url="/redoc" if settings.debug else None,
        lifespan=lifespan,
        openapi_url="/openapi.json" if settings.debug else None,
    )

    # --- Middleware ---
    application.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    register_error_handlers(application)

    # --- Routers ---
    api_prefix = "/api/v1"
    application.include_router(health.router, tags=["health"])
    application.include_router(sandboxes.router, prefix=api_prefix)
    application.include_router(code_execution.router, prefix=api_prefix)
    application.include_router(files.router, prefix=api_prefix)
    application.include_router(workspace.router, prefix=api_prefix)
    application.include_router(orchestrator.router, prefix=api_prefix)
    # Secrets Manager: mounted at /api (NOT /api/v1) so the Node backend
    # calls POST /api/sandbox/{sandbox_id}/secrets — the exact path pinned
    # by the C6 backend contract.
    application.include_router(secrets.router, prefix="/api")

    return application


app = create_app()


# ---------------------------------------------------------------------------
# uvicorn entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "app.main:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
        log_level=settings.log_level,
    )
