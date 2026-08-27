"""Daytona Workspace Manager — the core orchestration layer.

This is the single source of truth for all workspace operations.
Every AI agent write, file tree read, and terminal command flows
through this coordinator into the live Daytona MicroVM.

Mandatory workspace structure (created on every project init):
  /workspace/
    git/          📂  Git repositories, history, config
    frontend/     📂  All frontend source code, HTML, CSS, JS, UI assets
    backend/      📂  Server endpoints, API logic, routes, DB controllers
    logo.png      🖼️  Application logo at workspace root

SDK calls used:
  daytona.create()                      → spawn MicroVM
  sandbox.process.exec(command, cwd)    → scaffold dirs, run builds, terminal
  sandbox.fs.upload_file(bytes, path)   → agent code writes, logo uploads
  sandbox.fs.list_files(path, depth)    → live file tree
  sandbox.fs.download_file(path)        → read file contents
  sandbox.fs.create_folder(path, mode)  → mkdir
  sandbox.fs.delete_file(path, rec)     → rm
  daytona.delete(sandbox)               → destroy MicroVM
"""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import shlex
import time
from typing import Any

from app.config import settings
from app.daytona_client import (
    build_create_params,
    extract_sandbox_data,
    get_daytona,
)
from app.models import CodeRunResult
from app.services.quota_reaper import is_quota_error, reap_idle_workspaces, force_free_quota

logger = logging.getLogger(__name__)

# The canonical workspace root inside every Daytona sandbox
WORKSPACE_ROOT = "/workspace"

# Mandatory directories created on every project scaffold
MANDATORY_DIRS = ["git", "frontend", "backend"]


# ===========================================================================
# DaytonaWorkspaceManager
# ===========================================================================


class DaytonaWorkspaceManager:
    """Orchestrates the full lifecycle of a project workspace inside Daytona.

    This class replaces ALL static/local file operations with live VM
    filesystem operations.  The AI agent never touches the host disk.
    """

    def __init__(self) -> None:
        self._daytona = None  # lazy

    # ------------------------------------------------------------------
    # 1. Create + Scaffold
    # ------------------------------------------------------------------

    async def create_and_scaffold_workspace(
        self,
        project_id: str,
        language: str = "python",
        user_id: str | None = None,
        agent_llm: dict[str, str] | None = None,
        agent_skills: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Spawn a Daytona MicroVM and enforce the strict directory blueprint.

        Every sandbox is labeled with BOTH identifiers (snake_case keys):
          {user_id: <owner uuid>, project_id: <project uuid>, type: "workspace"}

        NOTE: language must be "python" for workspace sandboxes — the python
        snapshot ships a working shell (zsh/bash) for process.exec(), while
        the typescript snapshot image is missing /usr/bin/zsh entirely.

        Returns sandbox metadata dict with the new sandbox ID.
        """
        daytona = self._get_client()

        # Labels: user_id is constant per user, project_id per project —
        # both are REQUIRED for VM attribution across the platform.
        labels: dict[str, str] = {
            "user_id": user_id or "unknown",
            "project_id": project_id,
            "type": "workspace",
        }

        # Unique, human-greppable sandbox name. Timestamp suffix guarantees
        # uniqueness even when a previous sandbox for the same user+project
        # lingers in an error state (Daytona enforces unique names).
        name_parts = [p for p in (user_id, project_id) if p]
        if name_parts:
            name = "arcforge-" + "-".join(p[:8] for p in name_parts)
        else:
            name = f"arcforge-ws-{project_id[:12]}"
        name += f"-{int(time.time()) % 1000000:06d}"

        env_vars: dict[str, str] = {"PROJECT_ID": project_id}
        if user_id:
            env_vars["USER_ID"] = user_id

        params = build_create_params(
            method="snapshot",
            # Classic snapshot (NOT language=) — the SDK's code-toolbox
            # language path is broken in the eu region (every sandbox lands
            # in state=Error with no error_reason; verified live 2026-08-25).
            # daytonaio/sandbox:0.8.0 boots reliably and ships python3 +
            # node + bash/zsh + passwordless sudo (all verified live).
            snapshot=settings.default_workspace_snapshot,
            name=name,
            cpu=settings.default_cpu,
            memory=_parse_resource_size(settings.default_memory),
            disk=_parse_resource_size(settings.default_disk),
            labels=labels,
            env_vars=env_vars,
            auto_stop_interval=settings.sandbox_idle_timeout_seconds,
        )

        logger.info(
            "Creating workspace sandbox for project %s (user %s) …",
            project_id, user_id or "unattributed",
        )
        t0 = time.monotonic()

        sandbox = None
        try:
            sandbox = await asyncio.to_thread(
                daytona.create, params=params,
                timeout=settings.daytona_default_timeout,
            )
        except Exception as exc:
            # Org-quota exhausted ("Total memory limit exceeded" etc.)?
            # Free quota by reaping idle workspace sandboxes, then retry
            # creation ONCE. Incident 2026-08-26: two stale 4 Gi test
            # sandboxes silently ate 8/10 GiB of org memory quota and every
            # new workspace failed with HTTP 500 — surfaced to users as
            # "the studio page doesn't connect".
            #
            # Incident 2026-08-27 ("sandbox is full, again"): the idle reap
            # alone was not enough — the backend tunnel sweeper kept every
            # sandbox's lastActivityAt permanently fresh, so the reaper
            # found NOTHING idle and the retry failed again. When that
            # happens, fall back to force_free_quota: delete the OLDEST
            # workspace sandboxes (never this user's own) and retry one
            # final time. A quota-blocked NEW build must not lose to
            # abandoned corpses.
            if is_quota_error(exc):
                logger.warning(
                    "Sandbox creation rejected by an org quota limit (%s) — "
                    "reaping idle workspaces and retrying once", exc,
                )
                reaped = await reap_idle_workspaces()
                if reaped:
                    try:
                        sandbox = await asyncio.to_thread(
                            daytona.create, params=params,
                            timeout=settings.daytona_default_timeout,
                        )
                    except Exception as retry_exc:
                        exc = retry_exc
                        sandbox = None
                if sandbox is None:
                    freed = await force_free_quota(exclude_user_id=user_id)
                    if freed:
                        try:
                            sandbox = await asyncio.to_thread(
                                daytona.create, params=params,
                                timeout=settings.daytona_default_timeout,
                            )
                        except Exception as force_retry_exc:
                            exc = force_retry_exc
                            sandbox = None
            # create() raises "entered error state" when the sandbox fails to
            # boot during the SDK's wait — the sandbox STILL EXISTS server-side
            # as an Error corpse holding CPU quota. Look it up by its unique
            # name and delete it so this outage cannot leak one corpse per
            # generation (observed live: the eu target fails every boot).
            # (Skipped when the retry above succeeded.)
            if sandbox is None:
                corpse = None
                try:
                    corpse = await asyncio.to_thread(daytona.get, name)
                except Exception:
                    corpse = None
                if corpse is not None:
                    try:
                        await asyncio.to_thread(daytona.delete, corpse, 30, False)
                        logger.warning(
                            "Sandbox creation failed (%s) — deleted the Error corpse "
                            "%s to protect CPU quota", exc, getattr(corpse, "id", name),
                        )
                    except Exception:
                        logger.warning(
                            "Sandbox creation failed (%s) and corpse cleanup also "
                            "failed for name=%s — manual cleanup may be needed",
                            exc, name,
                        )
                raise

        # Post-create health gate: a sandbox can land in state=Error with
        # NO error_reason (observed live in the 'eu' target — every sandbox
        # failed to boot). Delete the corpse immediately so it doesn't leak
        # CPU quota, and raise so the caller knows provisioning failed.
        state_str = str(getattr(sandbox, "state", "") or "")
        if "error" in state_str.lower():
            try:
                await asyncio.to_thread(
                    daytona.delete, sandbox, 30, False,
                )
            except Exception:  # pragma: no cover — cleanup is best-effort
                pass
            raise RuntimeError(
                f"Newly created sandbox {sandbox.id} landed in state={state_str} "
                f"(target={getattr(sandbox, 'target', '?')}) — deleted to protect "
                "CPU quota. Check the DAYTONA_TARGET region."
            )

        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info(
            "Sandbox %s created in %d ms for project %s",
            sandbox.id, elapsed, project_id,
        )

        # --- Scaffold the mandatory directory structure (with best-effort tmpfs) ---
        scaffold_info = await self._scaffold_workspace(sandbox.id)

        # --- Install guest workspace-agent daemon (best-effort, never fatal) ---
        # If this fails, stream-write falls back to sandbox.fs.upload_file.
        agent_installed = False
        try:
            await self.install_guest_agent(sandbox.id)
            agent_installed = True
        except Exception as exc:
            logger.warning(
                "Failed to install guest agent in sandbox %s: %s "
                "(continuing without agent — stream-write will fall back "
                "to upload_file)",
                sandbox.id, exc,
            )

        # --- Install the in-VM agent ORCHESTRATOR sidecar (fire-and-forget) ---
        # The "Shadow Agent": FastAPI daemon on :9000 with SQLite state,
        # PM2-supervised, reachable by the browser through a Daytona preview
        # link. When installed, the studio frontend becomes a dumb terminal
        # (WebSocket) and the multi-agent pipeline runs INSIDE this VM.
        #
        # The install involves pip + npm installs inside the VM (~60-120s on
        # a cold box) — it MUST NOT block workspace creation (the frontend
        # races init against 8s and the SSE fallback keeps working). It runs
        # as a detached asyncio task; `GET /{id}/agent-info` probes the live
        # VM and reports installed=true the moment the daemon answers.
        try:
            from app.services.agent_installer import agent_installer

            install_task = asyncio.create_task(
                agent_installer.install(
                    sandbox, llm_config=agent_llm, skills=agent_skills,
                ),
            )

            def _log_sidecar(fut: "asyncio.Future[dict[str, Any]]") -> None:
                try:
                    res = fut.result()
                    if res.get("installed"):
                        logger.info(
                            "Agent sidecar READY in sandbox %s (launcher=%s)",
                            sandbox.id, res.get("launcher"),
                        )
                    else:
                        logger.info(
                            "Agent sidecar not installed in sandbox %s — "
                            "platform will use host-side SSE", sandbox.id,
                        )
                except Exception as exc:  # pragma: no cover — task guard
                    logger.warning(
                        "Agent sidecar install task errored for %s: %s",
                        sandbox.id, exc,
                    )

            install_task.add_done_callback(_log_sidecar)
        except Exception as exc:
            logger.warning(
                "Could not schedule agent sidecar install for sandbox %s: %s",
                sandbox.id, exc,
            )

        data = extract_sandbox_data(sandbox)
        data["project_id"] = project_id
        data["user_id"] = user_id
        data["provision_time_ms"] = elapsed
        data["vfs_backend"] = scaffold_info.get("vfs_backend", "disk")
        data["agent_installed"] = agent_installed
        return data

    async def _scaffold_workspace(self, sandbox_id: str) -> dict[str, Any]:
        """Create /workspace/git, /workspace/frontend, /workspace/backend inside the VM.

        The /workspace root is not present on stock images and / is root-owned,
        so we sudo-create it and chown it to the sandbox user first (passwordless
        sudo is available on the python snapshot), then mkdir -p the mandatory
        dirs and touch a placeholder logo.png so the tree is complete from birth.

        Module 1 VFS: Before scaffolding dirs, we attempt a best-effort
        ``tmpfs`` mount on /workspace for sub-millisecond HMR (Vite/Next/Nodemon).
        This is non-fatal -- if the mount fails (no privileges, /workspace
        already mounted), we proceed with disk-backed /workspace. Returns
        ``{"vfs_backend": "tmpfs" | "disk"}``.
        """
        daytona = self._get_client()
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)

        # --- Best-effort tmpfs mount (Module 1 VFS) ---
        # Falls back silently to disk-backed /workspace if mount fails (no
        # privileges, /workspace already mounted, etc.). Workspace creation
        # MUST still succeed regardless of this outcome.
        tmpfs_cmd = (
            f"sudo mount -t tmpfs -o size=512M,mode=0755 tmpfs {WORKSPACE_ROOT} 2>/dev/null && "
            f"sudo chown daytona:daytona {WORKSPACE_ROOT} 2>/dev/null; "
            f"mount | grep -q 'on {WORKSPACE_ROOT} type tmpfs' && "
            f"echo 'TMPFS_OK' || echo 'TMPFS_FALLBACK_DISK'"
        )
        vfs_backend = "disk"
        try:
            tmpfs_result = await asyncio.to_thread(
                sandbox.process.exec, tmpfs_cmd, "/home/daytona", None, 30,
            )
            tmpfs_out = (getattr(tmpfs_result, "result", "") or "").strip()
            if "TMPFS_OK" in tmpfs_out:
                vfs_backend = "tmpfs"
                logger.info(
                    "tmpfs mounted on %s in sandbox %s (sub-ms HMR ready)",
                    WORKSPACE_ROOT, sandbox_id,
                )
            else:
                logger.warning(
                    "tmpfs mount fell back to disk for %s in sandbox %s (out=%s) — "
                    "workspace still fully functional, just no RAM-disk HMR speedup",
                    WORKSPACE_ROOT, sandbox_id, tmpfs_out or "(empty)",
                )
        except Exception as exc:
            logger.warning(
                "tmpfs mount attempt raised in sandbox %s: %s (continuing with disk)",
                sandbox_id, exc,
            )

        # --- Mandatory directory scaffold (always runs, regardless of tmpfs) ---
        dirs = " ".join(f"{WORKSPACE_ROOT}/{d}" for d in MANDATORY_DIRS)
        scaffold_cmd = (
            f"sudo mkdir -p {WORKSPACE_ROOT} 2>/dev/null && "
            f"sudo chown daytona:daytona {WORKSPACE_ROOT} 2>/dev/null; "
            f"mkdir -p {dirs} && touch {WORKSPACE_ROOT}/logo.png"
        )

        result = await asyncio.to_thread(
            sandbox.process.exec, scaffold_cmd, "/home/daytona", None, 60,
        )

        exit_code = getattr(result, "exit_code", None)
        exit_code = -1 if exit_code is None else int(exit_code)
        if exit_code != 0:
            logger.error(
                "Scaffold failed (exit %d) in sandbox %s",
                exit_code, sandbox_id,
            )
            raise RuntimeError(
                f"Workspace scaffold failed: {getattr(result, 'result', '')}"
            )

        logger.info(
            "Workspace scaffolded in sandbox %s (vfs_backend=%s)",
            sandbox_id, vfs_backend,
        )
        return {"vfs_backend": vfs_backend}

    # ------------------------------------------------------------------
    # 2. Pre-Studio Logo Upload
    # ------------------------------------------------------------------

    async def handle_pre_studio_logo(
        self, sandbox_id: str, file_bytes: bytes,
    ) -> None:
        """Upload the application logo directly to /workspace/logo.png in the VM.

        This intercepts the image payload from the frontend pre-studio
        setup modal and writes it straight into the guest filesystem.
        """
        target_path = f"{WORKSPACE_ROOT}/logo.png"
        sandbox = await self._resolve_sandbox(sandbox_id)

        await asyncio.to_thread(
            sandbox.fs.upload_file, file_bytes, target_path,
        )

        logger.info(
            "Logo uploaded (%d bytes) to %s in sandbox %s",
            len(file_bytes), target_path, sandbox_id,
        )

    # ------------------------------------------------------------------
    # 3. Live File Tree
    # ------------------------------------------------------------------

    async def get_live_ui_file_tree(
        self, sandbox_id: str, max_depth: int = 4,
    ) -> dict[str, Any]:
        """Query the actual VM filesystem and build a nested JSON tree.

        This feeds the frontend Studio "Files Tab" sidebar.
        The tree is built from sandbox.fs.list_files() with recursive depth,
        then structured into a clean nested object.
        """
        sandbox = await self._resolve_sandbox(sandbox_id)

        try:
            file_infos = await asyncio.to_thread(
                sandbox.fs.list_files, WORKSPACE_ROOT, max_depth,
            )
        except Exception as exc:
            # Fallback: use find command via process.exec
            logger.warning(
                "list_files failed, falling back to find: %s", exc,
            )
            return await self._file_tree_fallback(sandbox_id, max_depth)

        # Build nested tree from flat file info list
        root_node: dict[str, Any] = {
            "name": "workspace",
            "path": WORKSPACE_ROOT,
            "type": "directory",
            "children": [],
        }

        # Index: path → node dict
        nodes: dict[str, dict] = {WORKSPACE_ROOT: root_node}

        for info in file_infos:
            fpath: str = getattr(info, "path", "")
            if not fpath:
                continue
            fname = fpath.rsplit("/", 1)[-1]
            is_dir = getattr(info, "is_dir", False)
            size = getattr(info, "size", 0)
            modified = getattr(info, "modified_at", None)

            node: dict[str, Any] = {
                "name": fname,
                "path": fpath,
                "type": "directory" if is_dir else "file",
                "size": size,
                "modified_at": modified,
            }
            if is_dir:
                node["children"] = []

            nodes[fpath] = node

            # Attach to parent
            parent_path = fpath.rsplit("/", 1)[0] if "/" in fpath else WORKSPACE_ROOT
            parent = nodes.get(parent_path)
            if parent and "children" in parent:
                parent["children"].append(node)

        return root_node

    async def _file_tree_fallback(
        self, sandbox_id: str, max_depth: int,
    ) -> dict[str, Any]:
        """Build file tree using find command when list_files is unavailable."""
        daytona = self._get_client()
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)

        cmd = f"find {WORKSPACE_ROOT} -maxdepth {max_depth} -not -path '*/.*' -printf '%y|%p|%s\n'"
        result = await asyncio.to_thread(
            sandbox.process.exec, cmd, None, None, 15,
        )

        raw = getattr(result, "result", "") or ""

        root_node: dict[str, Any] = {
            "name": "workspace",
            "path": WORKSPACE_ROOT,
            "type": "directory",
            "children": [],
        }
        nodes: dict[str, dict] = {WORKSPACE_ROOT: root_node}

        for line in raw.strip().split("\n"):
            if not line or "|" not in line:
                continue
            parts = line.split("|", 2)
            if len(parts) < 3:
                continue
            ftype_char, fpath, fsize_str = parts[0], parts[1], parts[2]
            is_dir = ftype_char == "d"
            fname = fpath.rsplit("/", 1)[-1]

            node: dict[str, Any] = {
                "name": fname,
                "path": fpath,
                "type": "directory" if is_dir else "file",
                "size": int(fsize_str) if fsize_str.isdigit() else 0,
            }
            if is_dir:
                node["children"] = []

            nodes[fpath] = node
            parent_path = fpath.rsplit("/", 1)[0] if "/" in fpath else WORKSPACE_ROOT
            parent = nodes.get(parent_path)
            if parent and "children" in parent:
                parent["children"].append(node)

        return root_node

    # ------------------------------------------------------------------
    # 4. Agent Code Write (direct VM write)
    # ------------------------------------------------------------------

    async def execute_agent_code_write(
        self, sandbox_id: str, filepath: str, code_content: str,
    ) -> None:
        """Write code directly into the live Daytona VM filesystem.

        The AI agent calls this instead of writing to the host disk.
        The filepath MUST target the VM workspace architecture.
        If the file targets a workspace subdir (frontend/, backend/, git/),
        it is automatically prefixed with /workspace/.
        """
        # Normalize path — ensure it targets /workspace/
        vm_path = self._normalize_vm_path(filepath)
        sandbox = await self._resolve_sandbox(sandbox_id)

        # Ensure parent directory exists (mkdir -p semantics; exec is reliable
        # on the python snapshot and idempotent for existing dirs)
        parent = vm_path.rsplit("/", 1)[0]
        await asyncio.to_thread(
            sandbox.process.exec,
            f"mkdir -p {parent}", "/home/daytona", None, 15,
        )

        # Write the file directly into the VM
        await asyncio.to_thread(
            sandbox.fs.upload_file,
            code_content.encode("utf-8"),
            vm_path,
        )

        logger.info("Agent wrote %d bytes to %s in sandbox %s",
                     len(code_content), vm_path, sandbox_id)

    async def execute_agent_bulk_write(
        self, sandbox_id: str, files: list[dict[str, str]],
    ) -> None:
        """Write multiple files into the VM in a single batch operation.

        Args:
            files: list of {"path": str, "content": str}
        """
        from daytona_sdk import FileUpload as SdkFileUpload

        sandbox = await self._resolve_sandbox(sandbox_id)

        uploads = [
            SdkFileUpload(
                source=f["content"].encode("utf-8"),
                destination=self._normalize_vm_path(f["path"]),
            )
            for f in files
        ]

        await asyncio.to_thread(sandbox.fs.upload_files, uploads)
        logger.info(
            "Agent bulk-wrote %d files to sandbox %s",
            len(files), sandbox_id,
        )

    # ------------------------------------------------------------------
    # 5. Live Terminal (Bash) Engine
    # ------------------------------------------------------------------

    async def run_live_terminal_command(
        self,
        sandbox_id: str,
        bash_command: str,
        execution_dir: str | None = None,
        timeout_ms: int = 30000,
    ) -> CodeRunResult:
        """Execute a command in the VM bash shell. Returns full output for the feedback loop.

        The agent uses this to:
        - Run builds (npm run build, pip install)
        - Install packages
        - Get stdout/stderr for autonomous self-debugging

        Returns exit_code + combined output for the continuous feedback loop.
        """
        sandbox = await self._resolve_sandbox(sandbox_id)
        cwd = execution_dir or WORKSPACE_ROOT
        timeout_sec = timeout_ms // 1000

        t0 = time.monotonic()
        try:
            result = await asyncio.to_thread(
                sandbox.process.exec, bash_command, cwd, None, timeout_sec,
            )
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.exception("Terminal command failed in sandbox %s", sandbox_id)
            return CodeRunResult(
                exit_code=-1,
                stderr=str(exc),
                timed_out="timeout" in str(exc).lower(),
                duration_ms=elapsed,
            )

        elapsed = int((time.monotonic() - t0) * 1000)
        raw_exit = getattr(result, "exit_code", None)
        exit_code = -1 if raw_exit is None else int(raw_exit)
        stdout = getattr(result, "result", "") or ""

        return CodeRunResult(
            exit_code=int(exit_code),
            stdout=stdout,
            stderr="",
            duration_ms=elapsed,
        )

    # ------------------------------------------------------------------
    # 6. File Read (for agent context)
    # ------------------------------------------------------------------

    async def read_file_from_vm(self, sandbox_id: str, filepath: str) -> str:
        """Read a file's content from the VM filesystem."""
        vm_path = self._normalize_vm_path(filepath)
        sandbox = await self._resolve_sandbox(sandbox_id)

        content = await asyncio.to_thread(sandbox.fs.download_file, vm_path)
        if isinstance(content, bytes):
            return content.decode("utf-8", errors="replace")
        return str(content)

    # ------------------------------------------------------------------
    # 7. Workspace Destruction
    # ------------------------------------------------------------------

    async def destroy_workspace(self, sandbox_id: str) -> None:
        """Permanently destroy the workspace sandbox."""
        daytona = self._get_client()
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        await asyncio.to_thread(daytona.delete, sandbox, 60, False)
        logger.info("Workspace sandbox %s destroyed", sandbox_id)

    # ------------------------------------------------------------------
    # 8. Guest Workspace-Agent Daemon (Module 1 VFS)
    # ------------------------------------------------------------------

    async def install_guest_agent(self, sandbox_id: str) -> None:
        """Install + launch the guest workspace-agent daemon in the VM.

        Writes the daemon source to /workspace/.workspace-agent.py and the
        WS client helper to /workspace/.ws_client.py (both stdlib-only --
        no pip install needed), then launches the daemon in the background
        via ``nohup python3 ... &`` with env vars configuring port, persist
        dir, and flush interval.

        Verifies the daemon is listening (best-effort -- non-fatal if curl
        is missing). Best-effort overall: logs warnings on failure, never
        raises (the caller in create_and_scaffold_workspace treats failure
        as non-fatal -- stream-write then falls back to sandbox.fs.upload_file).
        """
        from app.services.workspace_agent import (
            GUEST_DAEMON_SOURCE,
            GUEST_WS_CLIENT_SOURCE,
        )

        sandbox = await self._resolve_sandbox(sandbox_id)

        daemon_path = f"{WORKSPACE_ROOT}/.workspace-agent.py"
        client_path = f"{WORKSPACE_ROOT}/.ws_client.py"
        log_path = f"{WORKSPACE_ROOT}/.agent.log"
        # IMPORTANT: persist_dir MUST live on a DISK-backed volume (NOT under
        # /workspace, which would be tmpfs -- defeating the purpose of
        # persistence).
        persist_dir = "/home/daytona/.arcforge-persist"

        # 1) Write the daemon + client source files
        await asyncio.to_thread(
            sandbox.fs.upload_file,
            GUEST_DAEMON_SOURCE.encode("utf-8"), daemon_path,
        )
        await asyncio.to_thread(
            sandbox.fs.upload_file,
            GUEST_WS_CLIENT_SOURCE.encode("utf-8"), client_path,
        )

        # 2) Ensure persist dir exists on disk-backed /home/daytona +
        #    mark scripts executable (for shebang invocation convenience)
        prep_cmd = (
            f"mkdir -p {persist_dir} && "
            f"chown daytona:daytona {persist_dir} 2>/dev/null; "
            f"chmod +x {daemon_path} {client_path} 2>/dev/null; "
            f"echo OK"
        )
        await asyncio.to_thread(
            sandbox.process.exec, prep_cmd, "/home/daytona", None, 15,
        )

        # 3) Kill any prior instance (idempotent install)
        kill_cmd = "pkill -f workspace-agent 2>/dev/null; sleep 0.3; echo OK"
        await asyncio.to_thread(
            sandbox.process.exec, kill_cmd, "/home/daytona", None, 10,
        )

        # 4) Launch daemon in background via nohup (detached -- survives this exec)
        #    stdin redirected from /dev/null so the process doesn't block.
        launch_cmd = (
            f"WORKSPACE_ROOT={WORKSPACE_ROOT} "
            f"WORKSPACE_PERSIST_DIR={persist_dir} "
            f"WORKSPACE_AGENT_PORT=3010 "
            f"WORKSPACE_FLUSH_INTERVAL_S=4 "
            f"nohup python3 {daemon_path} > {log_path} 2>&1 < /dev/null &"
        )
        await asyncio.to_thread(
            sandbox.process.exec, launch_cmd, "/home/daytona", None, 10,
        )

        # 5) Verify (best-effort -- pgrep + optional curl)
        verify_cmd = (
            "sleep 1.5; "
            "pgrep -f workspace-agent > /dev/null && echo 'AGENT_RUNNING' "
            "|| echo 'AGENT_NOT_RUNNING'; "
            "curl -s -o /dev/null -w 'HTTP_%{http_code}' "
            "http://localhost:3010/ 2>/dev/null || echo 'NO_CURL'"
        )
        verify_result = await asyncio.to_thread(
            sandbox.process.exec, verify_cmd, "/home/daytona", None, 15,
        )
        verify_out = (getattr(verify_result, "result", "") or "").strip()

        if "AGENT_RUNNING" in verify_out:
            logger.info(
                "Guest workspace-agent installed and running in sandbox %s "
                "(verify=%s)",
                sandbox_id, verify_out.replace("\n", " | "),
            )
        else:
            # Read the log to surface the failure
            log_result = await asyncio.to_thread(
                sandbox.process.exec,
                f"tail -n 30 {log_path} 2>/dev/null",
                "/home/daytona", None, 10,
            )
            log_tail = (getattr(log_result, "result", "") or "").strip()
            logger.warning(
                "Guest agent failed to start in sandbox %s (verify=%s, "
                "log tail=%s) -- stream-write will fall back to upload_file",
                sandbox_id, verify_out, log_tail[-500:],
            )
            # Do NOT raise -- install is best-effort per spec.

    async def stream_write_file(
        self, sandbox_id: str, path: str, content: bytes,
    ) -> dict[str, Any]:
        """Stream-write file bytes to the guest daemon via the in-VM WS client.

        Module 1 VFS write path:
          1. Host base64-encodes the bytes.
          2. Host uploads the b64 payload to a temp file under /workspace
             (avoids ARG_MAX limits on the command line).
          3. Host runs ``python3 /workspace/.ws_client.py write <path> <b64_file>``
             inside the VM via ``sandbox.process.exec``.
          4. The WS client opens a TCP socket to 127.0.0.1:3010, performs the
             RFC 6455 handshake, sends a masked text frame with the write
             payload, reads the daemon's response, and prints it to stdout.
          5. The daemon decodes the b64, writes the bytes directly to tmpfs
             /workspace (RAM-to-RAM, inotify fires immediately for HMR),
             marks the path dirty for the persistence worker, and returns
             ok/path/size/sha256/vfs_backend.

        Falls back to ``sandbox.fs.upload_file`` if the daemon is not running
        or the WS path fails for any reason (best-effort contract per spec).

        Returns a dict: ``{ok, path, size, vfs_backend, fallback?}``.
        """
        vm_path = self._normalize_vm_path(path)
        b64 = base64.b64encode(content).decode("ascii")

        sandbox = await self._resolve_sandbox(sandbox_id)

        # Write b64 payload to a temp file the WS client reads (avoids ARG_MAX)
        tmp_payload = f"{WORKSPACE_ROOT}/.stream_buf_{int(time.time() * 1000)}.b64"
        try:
            await asyncio.to_thread(
                sandbox.fs.upload_file, b64.encode("ascii"), tmp_payload,
            )

            # Run the in-VM WS client
            client_cmd = (
                f"python3 {WORKSPACE_ROOT}/.ws_client.py write "
                f"{shlex.quote(vm_path)} {shlex.quote(tmp_payload)}"
            )
            result = await asyncio.to_thread(
                sandbox.process.exec, client_cmd, "/home/daytona", None, 30,
            )
            raw = (getattr(result, "result", "") or "").strip()
            raw_exit = getattr(result, "exit_code", None)
            exit_code = -1 if raw_exit is None else int(raw_exit)

            resp: dict[str, Any] | None = None
            if exit_code == 0 and raw:
                # Parse the LAST non-empty line (in case python printed warnings)
                last_line = raw.split("\n")[-1].strip()
                try:
                    resp = json.loads(last_line)
                except Exception:
                    resp = {
                        "ok": False,
                        "error": f"unparseable response: {last_line[:200]}",
                    }
            else:
                resp = {
                    "ok": False,
                    "error": f"client exit={exit_code}, out={raw[:200]}",
                }

            if not resp.get("ok"):
                # Fallback to direct upload_file
                logger.warning(
                    "Stream-write via WS daemon failed in sandbox %s (err=%s); "
                    "falling back to direct upload_file",
                    sandbox_id, resp.get("error"),
                )
                await asyncio.to_thread(
                    sandbox.fs.upload_file, content, vm_path,
                )
                # Best-effort vfs_backend check (mount status is independent
                # of which write path was used -- upload_file writes to the
                # same /workspace, which IS tmpfs if mounted)
                vfs = await self._check_tmpfs_mount(sandbox_id)
                return {
                    "ok": True,
                    "path": vm_path,
                    "size": len(content),
                    "vfs_backend": "tmpfs" if vfs else "disk",
                    "fallback": "upload_file",
                }

            # Daemon handled the write -- extract vfs_backend from response
            resp["vfs_backend"] = resp.get("vfs_backend", "disk")
            return resp
        finally:
            # Cleanup temp payload
            try:
                await asyncio.to_thread(
                    sandbox.process.exec,
                    f"rm -f {shlex.quote(tmp_payload)}",
                    "/home/daytona", None, 5,
                )
            except Exception:
                pass

    async def get_persistence_status(self, sandbox_id: str) -> dict[str, Any]:
        """Query the guest daemon for VFS + persistence status.

        Returns a dict with tmpfs_mounted, daemon_running, dirty_count,
        last_flush_at, persist_dir. Falls back to a direct /proc/mounts
        check if the daemon is not running (so callers still learn the
        mount state regardless of daemon liveness).
        """
        sandbox = await self._resolve_sandbox(sandbox_id)

        # Try the daemon via the in-VM WS client
        client_cmd = f"python3 {WORKSPACE_ROOT}/.ws_client.py status"
        try:
            result = await asyncio.to_thread(
                sandbox.process.exec, client_cmd, "/home/daytona", None, 15,
            )
            raw = (getattr(result, "result", "") or "").strip()
            raw_exit = getattr(result, "exit_code", None)
            exit_code = -1 if raw_exit is None else int(raw_exit)
            if exit_code == 0 and raw:
                last_line = raw.split("\n")[-1].strip()
                try:
                    outer = json.loads(last_line)
                    if outer.get("ok") and "status" in outer:
                        s = outer["status"]
                        return {
                            "tmpfs_mounted": bool(s.get("tmpfs_mounted")),
                            "daemon_running": True,
                            "dirty_count": int(s.get("dirty_count", 0)),
                            "last_flush_at": s.get("last_flush_at"),
                            "persist_dir": s.get(
                                "persist_dir",
                                "/home/daytona/.arcforge-persist",
                            ),
                        }
                except Exception as exc:
                    logger.warning(
                        "Failed to parse daemon status response in sandbox %s: %s",
                        sandbox_id, exc,
                    )
        except Exception as exc:
            logger.warning(
                "Failed to query daemon status in sandbox %s: %s",
                sandbox_id, exc,
            )

        # Fallback: direct mount check (daemon may not be running)
        tmpfs_mounted = await self._check_tmpfs_mount(sandbox_id)
        return {
            "tmpfs_mounted": tmpfs_mounted,
            "daemon_running": False,
            "dirty_count": 0,
            "last_flush_at": None,
            "persist_dir": "/home/daytona/.arcforge-persist",
        }

    async def _check_tmpfs_mount(self, sandbox_id: str) -> bool:
        """Quick check if /workspace is mounted as tmpfs in the VM."""
        try:
            sandbox = await self._resolve_sandbox(sandbox_id)
            result = await asyncio.to_thread(
                sandbox.process.exec,
                f"mount | grep -q 'on {WORKSPACE_ROOT} type tmpfs' && echo TMPFS || echo DISK",
                "/home/daytona", None, 5,
            )
            out = (getattr(result, "result", "") or "").strip()
            return "TMPFS" in out
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _normalize_vm_path(self, filepath: str) -> str:
        """Ensure a path targets /workspace/.

        If the path already starts with /workspace, return as-is.
        Otherwise, prefix it.
        """
        if filepath.startswith(WORKSPACE_ROOT):
            return filepath
        if filepath.startswith("/"):
            return f"{WORKSPACE_ROOT}{filepath}"
        return f"{WORKSPACE_ROOT}/{filepath}"

    async def _resolve_sandbox(self, sandbox_id: str):
        daytona = self._get_client()
        return await asyncio.to_thread(daytona.get, sandbox_id)

    def _get_client(self):
        return get_daytona()


def _parse_resource_size(val: str | float) -> float:
    """Parse resource size strings like '4Gi' to float GB."""
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().lower()
    if s.endswith("gi"):
        return float(s[:-2])
    if s.endswith("mi"):
        return float(s[:-2]) / 1024
    if s.endswith("g"):
        return float(s[:-1])
    if s.endswith("m"):
        return float(s[:-1]) / 1024
    return float(s)


# Singleton
workspace_manager = DaytonaWorkspaceManager()
