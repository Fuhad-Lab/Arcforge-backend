"""Singleton Daytona SDK client wrapper.

Wraps the `daytona_sdk.Daytona` client so that every service module shares
one connection pool.  The client is lazily initialised on first use.

Daytona SDK v0.205.1 — verified API:
  - daytona.create(params) → Sandbox
  - daytona.get(sandbox_id) → Sandbox
  - daytona.list() → list[Sandbox]
  - daytona.delete(sandbox: Sandbox) → None
  - sandbox.process.exec(command, cwd, env, timeout) → ExecuteResponse
  - sandbox.process.code_run(code, params, timeout) → ExecuteResponse
  - sandbox.fs.upload_file(file_bytes, remote_path) → None
  - sandbox.fs.download_file(remote_path) → bytes
  - sandbox.fs.list_files(path, depth) → list[FileInfo]
  - sandbox.fs.create_folder(path, mode) → None
  - sandbox.fs.delete_file(path, recursive) → None
  - sandbox.start(timeout) → None
  - sandbox.stop(timeout, force) → None
"""

from __future__ import annotations

import logging
from threading import Lock
from typing import TYPE_CHECKING

from daytona_sdk import (  # type: ignore[import-untyped]
    CreateSandboxFromImageParams,
    CreateSandboxFromSnapshotParams,
    Daytona,
    DaytonaConfig,
    FileUpload as SdkFileUpload,
    Resources as SdkResources,
)

from app.config import settings

if TYPE_CHECKING:
    from daytona_sdk import Sandbox as SdkSandbox

logger = logging.getLogger(__name__)

_client: Daytona | None = None
_lock = Lock()


def get_daytona() -> Daytona:
    """Return the singleton ``Daytona`` client, creating it if required."""
    global _client
    if _client is None:
        with _lock:
            if _client is None:  # double-checked
                logger.info("Initialising Daytona SDK client …")
                config = DaytonaConfig(**settings.daytona_config_dict)
                _client = Daytona(config)
                logger.info(
                    "Daytona SDK client ready (target=%s).",
                    settings.daytona_target or "org-default",
                )
    return _client


def reset_client() -> None:
    """Drop the current client (useful for tests)."""
    global _client
    with _lock:
        _client = None


# ---------------------------------------------------------------------------
# Thin helpers that map our domain types → SDK params
# ---------------------------------------------------------------------------


def build_create_params(
    *,
    method: str,
    language: str | None = None,
    image: str | None = None,
    name: str | None = None,
    cpu: float | int | None = None,
    memory: float | int | None = None,
    disk: float | int | None = None,
    labels: dict[str, str] | None = None,
    env_vars: dict[str, str] | None = None,
    auto_stop_interval: int | None = None,
) -> CreateSandboxFromSnapshotParams | CreateSandboxFromImageParams:
    """Build the correct ``CreateParams`` variant for ``daytona.create()``.

    SDK CreateSandboxBaseParams fields:
      name, language, os_user, env_vars, labels, public,
      auto_stop_interval, auto_pause_interval, auto_archive_interval,
      auto_delete_interval, ttl_minutes, volumes, secrets, etc.

    Resources(cpu=float, memory=float, disk=float, gpu, gpu_type)
    """
    resources_kwargs: dict[str, object] = {}
    if cpu is not None:
        # Daytona's API requires INTEGER cpu — a float (2.0) is rejected
        # with "Input should be a valid integer".
        cpu_val = float(cpu)
        resources_kwargs["cpu"] = int(cpu_val) if cpu_val.is_integer() else cpu_val
    if memory is not None:
        # SDK accepts numeric (GB) or string — pass as float (GB)
        resources_kwargs["memory"] = float(memory)
    if disk is not None:
        resources_kwargs["disk"] = float(disk)

    resources: SdkResources | None = (
        SdkResources(**resources_kwargs) if resources_kwargs else None
    )

    common_kwargs: dict[str, object] = {
        "name": name,
        "env_vars": env_vars,
        "labels": labels,
        "auto_stop_interval": auto_stop_interval,
    }
    # Remove None values so SDK defaults apply
    common_kwargs = {k: v for k, v in common_kwargs.items() if v is not None}

    if method == "image":
        if not image:
            raise ValueError("`image` is required when method='image'")
        return CreateSandboxFromImageParams(
            image=image,
            resources=resources,
            **common_kwargs,
        )

    # Default: snapshot-based creation
    lang = language or settings.default_sandbox_language
    return CreateSandboxFromSnapshotParams(
        language=lang,
        resources=resources,
        **common_kwargs,
    )


def build_file_upload(path: str, content: bytes) -> SdkFileUpload:
    """Wrap raw bytes into an SDK ``FileUpload``.

    SDK: FileUpload(source: bytes | str, destination: str)
    """
    return SdkFileUpload(source=content, destination=path)


def extract_sandbox_data(sandbox: "SdkSandbox") -> dict:
    """Pull serialisable fields from a Daytona ``Sandbox`` object.

    SDK Sandbox.model_fields:
      id, organization_id, name, snapshot, user, env, labels,
      target, cpu, gpu, memory, disk, state, desired_state,
      error_reason, created_at, updated_at, etc.
    """
    state_raw = getattr(sandbox, "state", None)
    # state may be an enum — extract its value
    if state_raw is not None:
        state_str = getattr(state_raw, "value", str(state_raw))
    else:
        state_str = "Unknown"

    created_at = getattr(sandbox, "created_at", None)
    if hasattr(created_at, "isoformat"):
        created_at = created_at.isoformat()

    return {
        "id": getattr(sandbox, "id", ""),
        "name": getattr(sandbox, "name", None),
        "state": state_str,
        "target": getattr(sandbox, "target", None),
        "cpu": getattr(sandbox, "cpu", None),
        "memory": getattr(sandbox, "memory", None),
        "disk": getattr(sandbox, "disk", None),
        "gpu": getattr(sandbox, "gpu", None),
        "labels": getattr(sandbox, "labels", None) or {},
        "env": getattr(sandbox, "env", None) or {},
        "created_at": created_at,
        "error_reason": getattr(sandbox, "error_reason", None),
        "recoverable": getattr(sandbox, "recoverable", None),
    }
