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
import json
import logging
import time
from typing import Any

from app.config import settings
from app.daytona_client import (
    build_create_params,
    extract_sandbox_data,
    get_daytona,
)
from app.models import CodeRunResult

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
        language: str = "nodejs",
    ) -> dict[str, Any]:
        """Spawn a Daytona MicroVM and enforce the strict directory blueprint.

        Returns sandbox metadata dict with the new sandbox ID.
        """
        daytona = self._get_client()

        params = build_create_params(
            method="snapshot",
            language=language,
            name=f"arcforge-ws-{project_id[:12]}",
            cpu=settings.default_cpu,
            memory=_parse_resource_size(settings.default_memory),
            disk=_parse_resource_size(settings.default_disk),
            labels={"projectId": project_id, "type": "workspace"},
            env_vars={"PROJECT_ID": project_id},
            auto_stop_interval=settings.sandbox_idle_timeout_seconds,
        )

        logger.info("Creating workspace sandbox for project %s …", project_id)
        t0 = time.monotonic()

        sandbox = await asyncio.to_thread(
            daytona.create, params=params,
            timeout=settings.daytona_default_timeout,
        )

        elapsed = int((time.monotonic() - t0) * 1000)
        logger.info(
            "Sandbox %s created in %d ms for project %s",
            sandbox.id, elapsed, project_id,
        )

        # --- Scaffold the mandatory directory structure ---
        await self._scaffold_workspace(sandbox.id)

        data = extract_sandbox_data(sandbox)
        data["project_id"] = project_id
        data["provision_time_ms"] = elapsed
        return data

    async def _scaffold_workspace(self, sandbox_id: str) -> None:
        """Create /workspace/git, /workspace/frontend, /workspace/backend inside the VM.

        Uses sandbox.process.exec() to mkdir -p all directories in one shot,
        then touches a placeholder logo.png so the tree is complete from birth.
        """
        dirs = " ".join(f"{WORKSPACE_ROOT}/{d}" for d in MANDATORY_DIRS)
        scaffold_cmd = f"mkdir -p {dirs} && touch {WORKSPACE_ROOT}/logo.png"

        daytona = self._get_client()
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)

        result = await asyncio.to_thread(
            sandbox.process.exec, scaffold_cmd, WORKSPACE_ROOT, None, 30,
        )

        exit_code = getattr(result, "exit_code", -1) or -1
        if exit_code != 0:
            logger.error(
                "Scaffold failed (exit %d) in sandbox %s",
                exit_code, sandbox_id,
            )
            raise RuntimeError(
                f"Workspace scaffold failed: {getattr(result, 'result', '')}"
            )

        logger.info("Workspace scaffolded in sandbox %s", sandbox_id)

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

        # Ensure parent directory exists
        parent = vm_path.rsplit("/", 1)[0]
        await asyncio.to_thread(sandbox.fs.create_folder, parent, "0755")

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
        exit_code = getattr(result, "exit_code", -1) or -1
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
