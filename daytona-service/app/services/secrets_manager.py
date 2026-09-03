"""Business logic for the Daytona Secrets Manager (org vault + sandbox mounts).

Implements contract C2/C6 of the ArcForge v7 architecture: user-provided
secrets (requested by the in-VM agent via the ``request_secret`` tool) are
stored in the Daytona org vault and mounted into the project's sandbox, so
the plaintext value only ever travels browser → backend → Daytona API.

Daytona SDK v0.205.1 secrets surface (verified live):
  - ``SecretApi(get_daytona()._api_client)`` — the raw REST client the
    Daytona instance wraps in its ``.secret`` service. ``SecretApi`` /
    ``CreateSecret`` live in the generated ``daytona_api_client`` package
    (daytona_sdk itself only re-exports ``CreateSecretParams``/``Secret``)
    — the Daytona instance's ``_api_client`` IS a
    ``daytona_api_client.ApiClient`` carrying the auth headers.
  - ``SecretApi.create_secret(CreateSecret{name, value, description, hosts})``
    → ``Secret`` — REQUIRES the ``manage:secrets`` permission on the API
    key. Our current key (write:sandboxes + delete:sandboxes) gets a 403.
  - ``SecretApi.list_secrets()`` → ``list[Secret]`` (id, name, ...).
  - ``SecretApi.delete_secret(secret_id)`` → None.
  - ``SecretApi.update_secret(secret_id, UpdateSecret{value, description,
    hosts})`` → ``Secret`` — used for value rotation on 409 name conflicts.
  - ``sandbox.update_secrets({env_var: secret_name})`` — REPLACES the whole
    mounted set; requires only write:sandboxes (verified working with the
    current key).

SECURITY INVARIANTS (enforced here):
  - The plaintext ``value`` is NEVER logged — log records carry the secret
    NAME and sandbox id only.
  - The vault secret name is namespaced per project
    (``arcforge-<project_id[:8]>-<NAME>``) so a compromised vault entry
    cannot be cross-mounted into another project's sandbox unnoticed.
  - The ``hosts`` allowlist is inferred from the secret NAME for known
    providers, so a leaked OPENAI_API_KEY placeholder cannot be used to
    authenticate against, e.g., api.github.com.

DEGRADED MODE (honest, not exceptional): when the API key lacks
``manage:secrets`` the vault write fails with 403 — the service returns
``{"ok": True, "vault": "unavailable", "detail": <actionable message>}``
instead of raising. The caller (Node backend) still delivers the value to
the VM sidecar over the authenticated /internal/secrets route; the user is
informed that only the in-VM delivery path is protected.
"""

from __future__ import annotations

import asyncio
import logging

from daytona_api_client import CreateSecret, SecretApi, UpdateSecret
from daytona_api_client.exceptions import ApiException

from app.daytona_client import get_daytona
from app.models import SecretClearResponse, SecretStoreResponse

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Known-provider host allowlists (inferred from the secret NAME)
# ---------------------------------------------------------------------------

# Ordered: first substring match wins. Unknown providers → no `hosts`
# restriction (Daytona applies the secret on any egress host).
KNOWN_HOSTS: dict[str, list[str]] = {
    "openai": ["api.openai.com"],
    "anthropic": ["api.anthropic.com"],
    "github": ["api.github.com"],
    "stripe": ["api.stripe.com"],
    "gemini": ["generativelanguage.googleapis.com"],
    "google": ["generativelanguage.googleapis.com"],
    "supabase": ["*.supabase.co"],
}

_VAULT_PREFIX = "arcforge"
_PROJECT_TAG_LEN = 8


def infer_hosts(name: str) -> list[str] | None:
    """Map a secret NAME to a provider host allowlist.

    ``OPENAI_API_KEY`` → ``["api.openai.com"]``; ``SOME_INTERNAL_TOKEN`` →
    ``None`` (no restriction — the value is substituted on any host).
    """
    lowered = name.lower()
    for provider, hosts in KNOWN_HOSTS.items():
        if provider in lowered:
            return hosts
    return None


def vault_secret_name(project_id: str, name: str) -> str:
    """Build the org-vault secret name: arcforge-<project8>-<NAME>."""
    tag = project_id[:_PROJECT_TAG_LEN] if project_id else "shared"
    return f"{_VAULT_PREFIX}-{tag}-{name.upper()}"


# ---------------------------------------------------------------------------
# In-memory mount tracking (update_secrets replaces the mounted set)
# ---------------------------------------------------------------------------

# ``sandbox.update_secrets(...)`` REPLACES the full mounted set — there is
# no getter for the currently-mounted secrets in SDK 0.205.1, so the union
# of previously-mounted env vars must be tracked here. On service restart
# the dict is empty and a later store mounts ONLY the new secret, silently
# detaching secrets mounted before the restart. ACCEPTABLE DEGRADATION:
# the mount is a defense-in-depth layer (the sidecar /internal/secrets
# delivery is the primary path), and the Daytona API exposes no way to
# read back the mounted set.
_mounted: dict[str, dict[str, str]] = {}


def _secret_api() -> SecretApi:
    """Build a SecretApi bound to the shared Daytona client's auth headers."""
    return SecretApi(get_daytona()._api_client)


def _permission_message(action: str) -> str:
    return (
        f"the Daytona API key needs the manage:secrets permission to "
        f"{action} — create a key with it in the Daytona dashboard and set "
        f"DAYTONA_API_KEY"
    )


# ---------------------------------------------------------------------------
# Store (vault create + union mount)
# ---------------------------------------------------------------------------


async def store_secret(
    sandbox_id: str,
    project_id: str,
    name: str,
    value: str,
    mount_env: str | None = None,
) -> SecretStoreResponse:
    """Create the org-vault secret and mount it into the sandbox.

    Never raises on permission problems — a 403 (missing manage:secrets)
    returns ``vault="unavailable"`` with an actionable message so the
    caller can continue with reduced protection. Only sandbox-resolution
    errors propagate (mapped to 404/502 by the router).
    """
    env_var = mount_env or name
    vault_name = vault_secret_name(project_id, name)
    hosts = infer_hosts(name)
    description = f"ArcForge user-provided secret for project {project_id}"

    # NEVER log the value — name + sandbox only.
    logger.info(
        "Storing secret %s for sandbox %s (hosts=%s)",
        vault_name, sandbox_id, hosts or "unrestricted",
    )

    try:
        sandbox = await asyncio.to_thread(get_daytona().get, sandbox_id)
    except Exception as exc:
        raise RuntimeError(f"Cannot access sandbox {sandbox_id}: {exc}") from exc

    # ── 1. Vault write (needs manage:secrets) ───────────────────────────
    vault_ok = True
    detail: str | None = None
    try:
        await asyncio.to_thread(
            _secret_api().create_secret,
            CreateSecret(
                name=vault_name,
                value=value,
                description=description,
                hosts=hosts,
            ),
        )
    except ApiException as exc:
        status = int(getattr(exc, "status", 0) or 0)
        if status == 403:
            # Degraded mode — deliver via the sidecar, surface honestly.
            logger.warning(
                "Vault write for %s rejected (403 — missing manage:secrets)",
                vault_name,
            )
            vault_ok = False
            detail = _permission_message("create org vault secrets")
        elif status == 409:
            # Name already exists (value rotation) — update in place.
            rotated = await _rotate_secret(vault_name, value, description, hosts)
            if rotated is not None:
                detail = rotated  # rotation failed — honest note
                if rotated:
                    vault_ok = False
        else:
            raise RuntimeError(
                f"Daytona vault create_secret failed (HTTP {status}): {exc}"
            ) from exc

    # ── 2. Mount (union — update_secrets replaces the set) ──────────────
    mounted = False
    if vault_ok:
        current = _mounted.get(sandbox_id, {})
        merged = {**current, env_var: vault_name}
        try:
            await asyncio.to_thread(sandbox.update_secrets, merged)
            _mounted[sandbox_id] = merged
            mounted = True
        except Exception as exc:  # noqa: BLE001 — mount is best-effort
            logger.warning(
                "Mounting %s as %s on sandbox %s failed: %s",
                vault_name, env_var, sandbox_id, exc,
            )
            detail = f"vault stored but sandbox mount failed: {exc}"

    return SecretStoreResponse(
        ok=True,
        vault="stored" if vault_ok else "unavailable",
        detail=detail,
        secret_name=vault_name if vault_ok else None,
        mounted=mounted,
    )


async def _rotate_secret(
    vault_name: str,
    value: str,
    description: str,
    hosts: list[str] | None,
) -> str | None:
    """Rotate an existing vault secret's value (409 name-conflict path).

    Returns None on success, or an honest detail string on failure.
    """
    try:
        api = _secret_api()
        secrets = await asyncio.to_thread(api.list_secrets)
        existing = next((s for s in secrets if s.name == vault_name), None)
        if existing is None:
            return "vault name conflict (409) but the existing secret was not found"
        await asyncio.to_thread(
            api.update_secret,
            existing.id,
            UpdateSecret(value=value, description=description, hosts=hosts),
        )
        logger.info("Rotated existing vault secret %s", vault_name)
        return None
    except ApiException as exc:
        status = int(getattr(exc, "status", 0) or 0)
        if status == 403:
            return _permission_message("rotate existing org vault secrets")
        return f"vault rotation for {vault_name} failed (HTTP {status})"
    except Exception as exc:  # noqa: BLE001 — rotation is best-effort
        return f"vault rotation for {vault_name} failed: {exc}"


# ---------------------------------------------------------------------------
# Clear (unmount all + delete this project's arcforge-* vault secrets)
# ---------------------------------------------------------------------------


async def clear_secrets(
    sandbox_id: str,
    project_id: str | None = None,
) -> SecretClearResponse:
    """Detach every mounted secret from the sandbox and delete this
    project's ``arcforge-*`` vault secrets.

    NEVER throws — every failure is collected into ``detail`` so callers
    can tear down workspaces without fearing a half-completed cleanup.
    """
    notes: list[str] = []
    unmounted = False

    # ── 1. Unmount all (update_secrets({}) needs write:sandboxes only) ──
    try:
        sandbox = await asyncio.to_thread(get_daytona().get, sandbox_id)
        await asyncio.to_thread(sandbox.update_secrets, {})
        _mounted.pop(sandbox_id, None)
        unmounted = True
    except Exception as exc:  # noqa: BLE001 — cleanup must never throw
        notes.append(f"unmount failed: {exc}")
        logger.warning(
            "Unmounting secrets from sandbox %s failed: %s", sandbox_id, exc,
        )

    # ── 2. Delete the project's arcforge-* vault secrets ────────────────
    deleted = 0
    if project_id:
        tag = project_id[:_PROJECT_TAG_LEN]
        prefix = f"{_VAULT_PREFIX}-{tag}-"
        try:
            api = _secret_api()
            secrets = await asyncio.to_thread(api.list_secrets)
            for secret in secrets:
                if not (secret.name or "").startswith(prefix):
                    continue
                try:
                    await asyncio.to_thread(api.delete_secret, secret.id)
                    deleted += 1
                except ApiException as exc:
                    status = int(getattr(exc, "status", 0) or 0)
                    if status == 403:
                        notes.append(_permission_message("delete org vault secrets"))
                        break
                    notes.append(f"delete {secret.name} failed (HTTP {status})")
                except Exception as exc:  # noqa: BLE001
                    notes.append(f"delete {secret.name} failed: {exc}")
        except ApiException as exc:
            status = int(getattr(exc, "status", 0) or 0)
            if status == 403:
                notes.append(_permission_message("list org vault secrets"))
            else:
                notes.append(f"vault listing failed (HTTP {status})")
        except Exception as exc:  # noqa: BLE001
            notes.append(f"vault listing failed: {exc}")

    if deleted or notes:
        # Name-level log only — vault secrets never carry the value back.
        logger.info(
            "Cleared secrets for sandbox %s (deleted=%d, notes=%d)",
            sandbox_id, deleted, len(notes),
        )

    return SecretClearResponse(
        ok=True,
        unmounted=unmounted,
        deleted=deleted,
        detail="; ".join(notes) if notes else None,
    )
