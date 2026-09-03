"""Global exception handler middleware.

Converts unhandled exceptions into consistent JSON error responses
and logs them with full context for observability.
"""

from __future__ import annotations

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.models import ErrorResponse

logger = logging.getLogger(__name__)


def register_error_handlers(app: FastAPI) -> None:
    """Attach exception handlers to the FastAPI app."""

    @app.exception_handler(Exception)
    async def unhandled_exception(request: Request, exc: Exception) -> JSONResponse:
        """Catch-all for unhandled exceptions."""
        tb = traceback.format_exc()
        logger.error(
            "Unhandled exception on %s %s:\n%s",
            request.method,
            request.url.path,
            tb,
        )

        status_code = 500
        detail = str(exc)

        # Surface Daytona connectivity issues as 502
        if "daytona" in detail.lower() and ("connection" in detail.lower() or "timeout" in detail.lower()):
            status_code = 502

        return JSONResponse(
            status_code=status_code,
            content=ErrorResponse(
                error=type(exc).__name__,
                detail=detail,
                status_code=status_code,
            ).model_dump(),
        )

    @app.exception_handler(ValueError)
    async def value_error_handler(request: Request, exc: ValueError) -> JSONResponse:
        return JSONResponse(
            status_code=422,
            content=ErrorResponse(
                error="ValidationError",
                detail=str(exc),
                status_code=422,
            ).model_dump(),
        )
