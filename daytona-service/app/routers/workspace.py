import base64
import json
import logging
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile, status

from app.models import (
    AgentBulkWriteRequest,
    AgentCodeWriteRequest,
    AgentSidecarInfo,
    BrowserAuditRequest,
    BrowserAuditResult,
    BrowserInstallResult,
    CodeRunResult,
    CreateWorkspaceRequest,
    FileContentResponse,
    FileTreeResponse,
    StreamWriteRequest,
    StreamWriteResponse,
    TerminalCommandRequest,
    VfsStatusResponse,
    WorkspaceInitResponse,
)
from app.services.browser_engine import browser_engine
from app.services.workspace_coordinator import workspace_manager

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/workspace", tags=["workspace"])


# ======================================================================
# Workspace Lifecycle
# ======================================================================


@router.post(
    "/init",
    response_model=WorkspaceInitResponse,
    status_code=status.HTTP_201_CREATED,
    summary="Create a project workspace with scaffolded directory structure",
    responses={
        201: {"description": "Workspace created and scaffolded"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def create_workspace(req: CreateWorkspaceRequest) -> WorkspaceInitResponse:
    """Spawn a Daytona MicroVM and create the mandatory directory blueprint.

    Creates: /workspace/git/, /workspace/frontend/, /workspace/backend/, /workspace/logo.png

    Every sandbox is labeled with BOTH user_id and project_id so VMs can be
    attributed to their owner (user_id is constant per user; project_id is
    per project).
    """
    try:
        data = await workspace_manager.create_and_scaffold_workspace(
            project_id=req.project_id,
            language=req.language,
            user_id=req.user_id,
            agent_llm=(
                {"url": req.agent_llm.url, "key": req.agent_llm.key,
                 "model": req.agent_llm.model}
                if req.agent_llm else None
            ),
            agent_skills=(
                [{"name": s.name, "instruction": s.instruction}
                 for s in req.skills]
                if req.skills else None
            ),
        )
    except (ValueError, RuntimeError) as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return WorkspaceInitResponse(
        sandbox_id=data["id"],
        project_id=req.project_id,
        user_id=req.user_id,
        state=str(data.get("state", "Unknown")),
        provision_time_ms=data.get("provision_time_ms", 0),
        vfs_backend=data.get("vfs_backend", "disk"),
        agent_installed=data.get("agent_installed", False),
    )


@router.delete(
    "/{sandbox_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Destroy a workspace sandbox",
)
async def destroy_workspace(sandbox_id: str) -> None:
    """Permanently destroy the workspace MicroVM."""
    try:
        await workspace_manager.destroy_workspace(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/reap-idle",
    summary="Delete idle workspace sandboxes to free Daytona org quota",
)
async def reap_idle(min_age_seconds: int = 0) -> dict:
    """Ops/cleanup endpoint (also used by the quota-aware create retry).

    Deletes workspace sandboxes that are Error-state corpses or idle for
    more than ``min_age_seconds`` (default: the configured idle timeout).
    Pass ``min_age_seconds=0`` to delete EVERY workspace sandbox (full
    reset — useful during incidents).

    NOTE: the listing this performs refreshes sandbox activity server
    side, but the response snapshot still carries pre-refresh
    lastActivityAt values, so the idle decisions are made on real data.
    """
    from app.services.quota_reaper import reap_idle_workspaces

    reaped = await reap_idle_workspaces(
        min_age_seconds=min_age_seconds if min_age_seconds > 0 else None
    )
    return {"reaped": reaped, "count": len(reaped)}


# ======================================================================
# In-VM Agent Orchestrator ("Shadow Agent" sidecar)
# ======================================================================


@router.get(
    "/{sandbox_id}/agent-info",
    response_model=AgentSidecarInfo,
    summary="Broker connection info for the in-VM agent orchestrator daemon",
    responses={
        200: {"description": "Sidecar probe result (installed=false while the async install is in flight)"},
        404: {"description": "Sandbox not found"},
    },
)
async def get_agent_info(sandbox_id: str) -> AgentSidecarInfo:
    """Probe the live VM for the agent orchestrator sidecar.

    Reads the per-VM shared-secret token from inside the sandbox, checks
    the daemon's /health endpoint, and opens the Daytona preview link for
    port 9000. This is the ONLY path by which the daemon's token reaches
    the outside world — the Node backend calls it behind JWT + project
    ownership checks and relays it to the studio frontend.
    """
    from app.services.agent_installer import agent_installer

    try:
        sandbox = await workspace_manager._resolve_sandbox(sandbox_id)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=f"Sandbox not found: {exc}") from exc

    info = await agent_installer.get_agent_info(sandbox)
    return AgentSidecarInfo(
        installed=info.get("installed", False),
        port=info.get("port", 9000),
        url=info.get("url"),
        token=info.get("token"),
        launcher=info.get("launcher"),
        alive=info.get("alive", False),
        app_url=info.get("app_url"),
        app_port=info.get("app_port"),
    )


# ======================================================================
# Live File Tree
# ======================================================================


@router.get(
    "/{sandbox_id}/file-tree",
    response_model=FileTreeResponse,
    summary="Get live file tree from the VM (for Files Tab sidebar)",
    responses={
        200: {"description": "Nested directory tree from the actual VM filesystem"},
        404: {"description": "Sandbox not found"},
    },
)
async def get_file_tree(
    sandbox_id: str,
    max_depth: int = 4,
) -> dict[str, Any]:
    """Return the live directory tree from the Daytona VM.

    This endpoint is the backend proxy that feeds the frontend Studio
    "Files Tab" sidebar.  It dynamically scans the /workspace folder
    inside the active VM and structures it into a clean nested JSON tree.
    """
    try:
        return await workspace_manager.get_live_ui_file_tree(sandbox_id, max_depth)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


# ======================================================================
# Agent Code Write Operations (Direct VM Writes)
# ======================================================================


@router.post(
    "/{sandbox_id}/write",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Write a single file directly into the VM",
)
async def agent_code_write(sandbox_id: str, req: AgentCodeWriteRequest) -> None:
    """Write code directly into the live Daytona VM filesystem.

    The AI agent calls this instead of writing to static files.
    The path is automatically normalized to /workspace/<path>.
    """
    try:
        await workspace_manager.execute_agent_code_write(
            sandbox_id, req.path, req.content,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/{sandbox_id}/write-bulk",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Write multiple files into the VM in one batch",
)
async def agent_bulk_write(sandbox_id: str, req: AgentBulkWriteRequest) -> None:
    """Bulk-write multiple files into the live VM in a single operation."""
    try:
        await workspace_manager.execute_agent_bulk_write(sandbox_id, req.files)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ======================================================================
# File Read (for agent context window)
# ======================================================================


@router.get(
    "/{sandbox_id}/read",
    response_model=FileContentResponse,
    summary="Read a file from the VM",
)
async def read_file(sandbox_id: str, path: str) -> FileContentResponse:
    """Read a file's content from the VM filesystem."""
    try:
        content = await workspace_manager.read_file_from_vm(sandbox_id, path)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    return FileContentResponse(
        path=path,
        content=content,
        size_bytes=len(content.encode("utf-8")),
    )


# ======================================================================
# Pre-Studio Logo Upload
# ======================================================================


@router.post(
    "/{sandbox_id}/logo",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload project logo to /workspace/logo.png in the VM",
)
async def upload_logo(sandbox_id: str, file: UploadFile) -> None:
    """Intercept the pre-studio logo modal payload and write it to /workspace/logo.png.

    The file buffer is captured and written directly into the VM root.
    """
    content = await file.read()
    if len(content) > 5_000_000:  # 5MB limit
        raise HTTPException(
            status_code=413,
            detail="Logo file must be under 5MB",
        )

    try:
        await workspace_manager.handle_pre_studio_logo(sandbox_id, content)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


# ======================================================================
# Active Bash Terminal Engine
# ======================================================================


@router.post(
    "/{sandbox_id}/terminal",
    response_model=CodeRunResult,
    summary="Execute a bash command in the VM terminal",
    responses={
        200: {"description": "Command output with exit code for feedback loop"},
        408: {"description": "Command timed out"},
    },
)
async def run_terminal(sandbox_id: str, req: TerminalCommandRequest) -> CodeRunResult:
    """Execute a command in the VM bash shell.

    Returns exit_code + stdout for the continuous autonomous feedback loop.
    The agent uses this for:
    - Running builds (npm run build, pip install)
    - Installing packages
    - Self-correcting debugging via stderr analysis
    """
    try:
        result = await workspace_manager.run_live_terminal_command(
            sandbox_id, req.command, req.cwd, req.timeout_ms,
        )
    except RuntimeError as exc:
        msg = str(exc)
        if "cannot access" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc

    if result.timed_out:
        raise HTTPException(
            status_code=status.HTTP_408_REQUEST_TIMEOUT,
            detail=result.stderr or "Command timed out",
        )

    return result


# ======================================================================
# Module 1 VFS: Stream-Write + VFS Status
# ======================================================================


@router.post(
    "/{sandbox_id}/stream-write",
    response_model=StreamWriteResponse,
    summary="Stream-write a file to the guest daemon (RAM-disk tmpfs write)",
    responses={
        200: {"description": "File written via WS daemon OR fallback upload_file"},
        400: {"description": "Neither content_b64 nor content provided, or invalid base64"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def stream_write_file(
    sandbox_id: str, req: StreamWriteRequest,
) -> StreamWriteResponse:
    """Stream-write a file to the guest workspace-agent daemon.

    Module 1 VFS write path:
      1. The host serializes the file change into a base64 binary buffer.
      2. An in-VM WebSocket client (``/workspace/.ws_client.py``) is
         invoked via ``sandbox.process.exec``.
      3. The client opens a TCP socket to 127.0.0.1:3010, performs the
         RFC 6455 handshake, and streams the buffer to the guest daemon.
      4. The daemon writes the bytes directly to tmpfs /workspace
         (RAM-to-RAM). The Linux kernel immediately fires an inotify
         event for HMR (Vite/Next/Nodemon).

    Falls back to ``sandbox.fs.upload_file`` if the daemon is not
    running (best-effort contract).
    """
    if req.content_b64:
        try:
            content = base64.b64decode(req.content_b64)
        except Exception as exc:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid base64 content: {exc}",
            ) from exc
    elif req.content is not None:
        content = req.content.encode("utf-8")
    else:
        raise HTTPException(
            status_code=400,
            detail="Either content_b64 or content must be provided",
        )

    try:
        result = await workspace_manager.stream_write_file(
            sandbox_id, req.path, content,
        )
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    return StreamWriteResponse(
        ok=bool(result.get("ok", False)),
        path=result.get("path", req.path),
        size=int(result.get("size", 0)),
        vfs_backend=result.get("vfs_backend", "disk"),
    )


@router.get(
    "/{sandbox_id}/vfs-status",
    response_model=VfsStatusResponse,
    summary="Get VFS + daemon + persistence status",
    responses={
        200: {"description": "tmpfs mount + daemon + dirty-file + last-flush info"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def get_vfs_status(sandbox_id: str) -> VfsStatusResponse:
    """Return tmpfs mount status, daemon liveness, and persistence worker state.

    Module 1 VFS status snapshot:
      - ``tmpfs_mounted``: True if /workspace is mounted as a tmpfs RAM disk.
      - ``daemon_running``: True if the guest workspace-agent is alive.
      - ``dirty_count``: Number of modified-but-not-yet-flushed files.
      - ``last_flush_at``: Unix timestamp of the last persistence flush.
      - ``persist_dir``: Disk-backed persist directory path.

    Falls back to a direct /proc/mounts check if the daemon is not
    running (callers still learn the mount state regardless).
    """
    try:
        s = await workspace_manager.get_persistence_status(sandbox_id)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    last_flush = s.get("last_flush_at")
    return VfsStatusResponse(
        tmpfs_mounted=bool(s.get("tmpfs_mounted")),
        daemon_running=bool(s.get("daemon_running")),
        dirty_count=int(s.get("dirty_count", 0)),
        last_flush_at=(str(last_flush) if last_flush is not None else None),
        persist_dir=str(s.get("persist_dir", "/home/daytona/.arcforge-persist")),
    )


# ======================================================================
# Module 3 Browser Engine: install + audit (Playwright in VM)
# ======================================================================


@router.post(
    "/{sandbox_id}/browser-install",
    response_model=BrowserInstallResult,
    summary="Install Playwright + Chromium inside the VM (idempotent)",
    responses={
        200: {"description": "Install attempted; see `installed` flag for result"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def browser_install(sandbox_id: str) -> BrowserInstallResult:
    """Idempotent installer for the in-VM browser engine.

    Probes ``~/.cache/ms-playwright/chromium-*/chrome-linux/chrome`` first.
    If absent, runs ``pip3 install playwright``, then
    ``python3 -m playwright install chromium``, then
    ``sudo python3 -m playwright install-deps chromium`` (best-effort).

    First-time install takes ~60-90s on a warm sandbox (the 150MB chromium
    download dominates). Subsequent installs return instantly via the
    in-process per-sandbox cache.
    """
    try:
        result = await browser_engine.ensure_browser_installed(sandbox_id)
    except Exception as exc:
        # Contract: ensure_browser_installed never raises, but if a bug
        # surfaces one, the operator still gets a structured 502.
        raise HTTPException(
            status_code=502,
            detail=f"browser_install unexpected failure: {exc}",
        ) from exc

    return BrowserInstallResult(
        installed=bool(result.get("installed", False)),
        browser_path=result.get("browser_path"),
        install_log=result.get("install_log", ""),
        duration_ms=int(result.get("duration_ms", 0)),
    )


@router.post(
    "/{sandbox_id}/browser-audit",
    response_model=BrowserAuditResult,
    summary="Run an in-VM Playwright audit against the live preview URL",
    responses={
        200: {"description": "Audit attempted; see `status` for success/failed"},
        502: {"description": "Daytona API unreachable"},
    },
)
async def browser_audit(
    sandbox_id: str, req: BrowserAuditRequest,
) -> BrowserAuditResult:
    """Run a headless Chromium audit inside the sandbox VM.

    This is the AI's "eyes" -- Module 3 of the ArcForge architecture.

    Flow:
      1. Lazy-install Playwright + Chromium if not already present
         (idempotent; ~60-90s on first audit, instant on subsequent).
      2. Write ``/workspace/.browser-audit.py`` into the VM.
      3. ``sandbox.process.exec("python3 /workspace/.browser-audit.py "
         "<frontend_url> <backend_url|->")``.
      4. The audit script launches headless Chromium, navigates to
         ``frontend_url`` (typically ``http://localhost:5173``), captures
         ``console.error`` + ``pageerror`` events, captures the rendered
         DOM (truncated to 50 000 chars), and captures a full-page PNG
         screenshot (base64-encoded).
      5. The script prints one JSON envelope on stdout -- the host parses
         the last JSON line and returns it.

    NEVER raises -- on any failure (install, write, exec, parse, timeout)
    returns a ``BrowserAuditResult`` with ``status="failed"`` and the
    ``error`` field populated. This is the contract Module 4's
    orchestration loop depends on (the auto-correction loop's evaluator
    can decide replan purely from the response shape, regardless of
    failure mode).
    """
    try:
        result = await browser_engine.execute_audit(
            sandbox_id,
            frontend_url=req.frontend_url,
            backend_url=req.backend_url,
            validation_blueprint=req.validation_blueprint,
        )
    except Exception as exc:
        # Contract: execute_audit never raises, but defend in case.
        logger.exception(
            "browser_audit unexpected exception for sandbox %s", sandbox_id,
        )
        result = {
            "status": "failed",
            "error": f"browser_audit unexpected failure: {exc}",
            "error_logs": [],
            "console_errors": [],
        }

    return BrowserAuditResult(
        status=str(result.get("status", "failed")),
        title=result.get("title"),
        url=result.get("url", req.frontend_url),
        backend_url=result.get("backend_url", req.backend_url),
        http_status=result.get("http_status"),
        error_logs=list(result.get("error_logs", []) or []),
        console_errors=list(result.get("console_errors", []) or []),
        dom_snapshot=result.get("dom_snapshot"),
        screenshot_b64=result.get("screenshot_b64"),
        duration_ms=int(result.get("duration_ms", 0))
        if result.get("duration_ms") is not None
        else None,
        error=result.get("error"),
    )
