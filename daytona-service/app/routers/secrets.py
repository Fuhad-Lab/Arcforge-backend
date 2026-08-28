"""REST endpoints for the Daytona Secrets Manager.

Routes are prefixed with ``/sandbox`` and mounted at ``/api`` (NOT the
``/api/v1`` prefix used by the other routers) so the Node backend can call
them at exactly ``POST /api/sandbox/{sandbox_id}/secrets`` — the path the
C6 backend contract pins.

LOGGING: the request body of the POST route carries a secret VALUE —
uvicorn's access log only records the request line (method + path), no
bodies, and no body-logging middleware exists in this service. The
service layer additionally logs the secret NAME + sandbox id only.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query, status

from app.models import (
    SecretClearResponse,
    SecretStoreRequest,
    SecretStoreResponse,
)
from app.services import secrets_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/sandbox", tags=["secrets"])


# ---------------------------------------------------------------------------
# POST /sandbox/{sandbox_id}/secrets
# ---------------------------------------------------------------------------


@router.post(
    "/{sandbox_id}/secrets",
    response_model=SecretStoreResponse,
    summary="Store a user secret in the Daytona org vault and mount it",
    responses={
        200: {"description": "Vault write attempted (vault=unavailable when permissions lack)"},
        404: {"description": "Sandbox not found"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def store_sandbox_secret(
    sandbox_id: str,
    req: SecretStoreRequest,
) -> SecretStoreResponse:
    """Create ``arcforge-<project8>-<NAME>`` in the org vault (with a
    provider host allowlist inferred from the name) and mount it into the
    sandbox as ``mount_env`` (union-merged with previously tracked mounts).

    A 403 from the vault API (missing ``manage:secrets`` permission) is NOT
    an error: the response is ``{"ok": true, "vault": "unavailable",
    "detail": ...}`` so the caller can continue with reduced protection.
    """
    try:
        return await secrets_manager.store_secret(
            sandbox_id=sandbox_id,
            project_id=req.project_id,
            name=req.name,
            value=req.value,
            mount_env=req.mount_env,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


# ---------------------------------------------------------------------------
# DELETE /sandbox/{sandbox_id}/secrets
# ---------------------------------------------------------------------------


@router.delete(
    "/{sandbox_id}/secrets",
    response_model=SecretClearResponse,
    summary="Unmount all sandbox secrets + delete the project's vault secrets",
    responses={
        200: {"description": "Best-effort cleanup result (never throws)"},
    },
)
async def clear_sandbox_secret(
    sandbox_id: str,
    project_id: str | None = Query(
        default=None,
        description="Owning project UUID — its arcforge-* vault secrets are deleted",
    ),
) -> SecretClearResponse:
    """Detach every mounted secret from the sandbox and delete the
    project's ``arcforge-*`` org-vault secrets.

    NEVER throws — permission failures and Daytona errors are collected
    into ``detail`` (the vault delete path needs ``manage:secrets``).
    """
    return await secrets_manager.clear_secrets(sandbox_id, project_id)
