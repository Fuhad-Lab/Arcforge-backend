"""Arcforge Daytona Service - FastAPI application entry point.

Wires together routers, middleware, and lifecycle hooks.
"""

from __future__ import annotations

import logging
import sys
from contextlib import asynccontextmanager
from typing import AsyncIterator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.config import settings
from app.middleware.error_handler import register_error_handlers
from app.routers import code_execution, files, health, sandboxes, workspace

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
    yield
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
