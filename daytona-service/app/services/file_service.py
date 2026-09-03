"""Business logic for file I/O inside sandboxes.

Daytona SDK v0.205.1 filesystem API (sandbox.fs):
  upload_file(file: bytes, remote_path: str, timeout=1800) → None
  upload_file(local_path: str, remote_path: str, timeout=1800) → None  (overload)
  upload_files(files: list[FileUpload], timeout=1800) → None
  download_file(remote_path: str) → bytes
  download_file(remote_path: str, local_path: str) → None  (overload)
  list_files(path: str, depth: int | None) → list[FileInfo]
  find_files(path: str, pattern: str) → list[Match]
  search_files(path: str, pattern: str) → SearchFilesResponse
  create_folder(path: str, mode: str) → None
  delete_file(path: str, recursive: bool) → None
  move_files(source: str, destination: str) → None
  get_file_info(path: str) → FileInfo
  replace_in_files(...) → None
  set_file_permissions(path: str, permissions: str) → None
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from app.daytona_client import get_daytona
from app.models import (
    FileContentResponse,
    FileEntry,
    FileListResponse,
    FileSearchResult,
)

logger = logging.getLogger(__name__)


async def upload_file(
    sandbox_id: str,
    remote_path: str,
    content: bytes,
) -> None:
    """Upload *content* to *remote_path* inside *sandbox_id*.

    SDK: sandbox.fs.upload_file(file: bytes, remote_path: str)
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        await asyncio.to_thread(sandbox.fs.upload_file, content, remote_path)
    except Exception as exc:
        raise RuntimeError(f"upload_file to {remote_path} failed: {exc}") from exc

    logger.info(
        "Uploaded %d bytes to %s in sandbox %s",
        len(content), remote_path, sandbox_id,
    )


async def upload_files_bulk(
    sandbox_id: str,
    files: list[dict[str, Any]],
) -> None:
    """Upload multiple files at once.

    Args:
        files: list of {"path": str, "content": str|bytes}
    """
    from daytona_sdk import FileUpload as SdkFileUpload

    sandbox = await _get_sandbox(sandbox_id)
    uploads = [
        SdkFileUpload(
            source=f["content"].encode("utf-8") if isinstance(f["content"], str) else f["content"],
            destination=f["path"],
        )
        for f in files
    ]
    try:
        await asyncio.to_thread(sandbox.fs.upload_files, uploads)
    except Exception as exc:
        raise RuntimeError(f"upload_files_bulk failed: {exc}") from exc

    logger.info("Bulk uploaded %d files to sandbox %s", len(files), sandbox_id)


async def download_file(sandbox_id: str, remote_path: str) -> FileContentResponse:
    """Download *remote_path* from *sandbox_id* and return its content.

    SDK: sandbox.fs.download_file(remote_path) → bytes
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        content = await asyncio.to_thread(sandbox.fs.download_file, remote_path)
    except Exception as exc:
        raise RuntimeError(f"download_file from {remote_path} failed: {exc}") from exc

    if isinstance(content, bytes):
        text = content.decode("utf-8", errors="replace")
        size_bytes = len(content)
    else:
        text = str(content)
        size_bytes = len(text.encode("utf-8"))

    return FileContentResponse(
        path=remote_path,
        content=text,
        size_bytes=size_bytes,
    )


async def list_directory(
    sandbox_id: str,
    path: str,
    depth: int | None = 1,
) -> FileListResponse:
    """List entries at *path* using the SDK filesystem API.

    SDK: sandbox.fs.list_files(path: str, depth: int | None) → list[FileInfo]
    FileInfo has: name, path, is_dir, size, modified_at, permissions, etc.
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        file_infos = await asyncio.to_thread(sandbox.fs.list_files, path, depth)
    except Exception as exc:
        raise RuntimeError(f"list_files at {path} failed: {exc}") from exc

    entries = []
    for info in file_infos:
        entry = FileEntry(
            name=getattr(info, "name", ""),
            path=getattr(info, "path", ""),
            is_dir=getattr(info, "is_dir", False),
            size=getattr(info, "size", 0),
            modified_at=getattr(info, "modified_at", None),
            permissions=getattr(info, "permissions", None),
        )
        entries.append(entry)

    return FileListResponse(path=path, entries=entries, total=len(entries))


async def create_folder(sandbox_id: str, path: str, mode: str = "0755") -> None:
    """Create a directory inside the sandbox.

    SDK: sandbox.fs.create_folder(path, mode)
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        await asyncio.to_thread(sandbox.fs.create_folder, path, mode)
    except Exception as exc:
        raise RuntimeError(f"create_folder {path} failed: {exc}") from exc


async def delete_file(sandbox_id: str, path: str, recursive: bool = False) -> None:
    """Delete a file or directory inside the sandbox.

    SDK: sandbox.fs.delete_file(path, recursive)
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        await asyncio.to_thread(sandbox.fs.delete_file, path, recursive)
    except Exception as exc:
        raise RuntimeError(f"delete_file {path} failed: {exc}") from exc


async def search_files(sandbox_id: str, path: str, pattern: str) -> FileSearchResult:
    """Search for files matching a pattern.

    SDK: sandbox.fs.search_files(path, pattern) → SearchFilesResponse
    SDK: sandbox.fs.find_files(path, pattern) → list[Match]
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        matches = await asyncio.to_thread(sandbox.fs.find_files, path, pattern)
    except Exception as exc:
        raise RuntimeError(f"find_files at {path} failed: {exc}") from exc

    match_paths = []
    for m in matches:
        match_paths.append({
            "path": getattr(m, "path", ""),
            "line": getattr(m, "line", None),
            "column": getattr(m, "column", None),
            "text": getattr(m, "text", ""),
        })

    return FileSearchResult(
        path=path,
        pattern=pattern,
        matches=match_paths,
        total=len(match_paths),
    )


async def get_file_info(sandbox_id: str, path: str) -> FileEntry:
    """Get metadata for a single file.

    SDK: sandbox.fs.get_file_info(path) → FileInfo
    """
    sandbox = await _get_sandbox(sandbox_id)
    try:
        info = await asyncio.to_thread(sandbox.fs.get_file_info, path)
    except Exception as exc:
        raise RuntimeError(f"get_file_info {path} failed: {exc}") from exc

    return FileEntry(
        name=getattr(info, "name", ""),
        path=getattr(info, "path", ""),
        is_dir=getattr(info, "is_dir", False),
        size=getattr(info, "size", 0),
        modified_at=getattr(info, "modified_at", None),
        permissions=getattr(info, "permissions", None),
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _get_sandbox(sandbox_id: str):
    """Fetch a sandbox reference from the Daytona client."""
    daytona = get_daytona()
    try:
        return await asyncio.to_thread(daytona.get, sandbox_id)
    except Exception as exc:
        raise RuntimeError(f"Cannot access sandbox {sandbox_id}: {exc}") from exc
