"""REST endpoints for file I/O inside sandboxes.

All routes are prefixed with ``/api/v1/sandboxes/{sandbox_id}/files``.

Uses Daytona SDK sandbox.fs.* methods for VFS operations.
"""

from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, UploadFile as FastUploadFile, status

from app.models import (
    BulkFileUploadRequest,
    FileContentResponse,
    FileDeleteRequest,
    FileEntry,
    FileListResponse,
    FileSearchRequest,
    FileSearchResult,
    FolderCreateRequest,
)
from app.services import file_service

logger = logging.getLogger(__name__)
router = APIRouter(tags=["files"])


@router.post(
    "/sandboxes/{sandbox_id}/files/upload",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload a single file",
)
async def upload_file(sandbox_id: str, path: str, file: FastUploadFile) -> None:
    """Upload file content to a path inside the sandbox.

    SDK: sandbox.fs.upload_file(file: bytes, remote_path: str)
    """
    content = await file.read()
    try:
        await file_service.upload_file(sandbox_id, path, content)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sandboxes/{sandbox_id}/files/upload-bulk",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Upload multiple files at once",
)
async def upload_files_bulk(sandbox_id: str, req: BulkFileUploadRequest) -> None:
    """Upload multiple files in a single request.

    SDK: sandbox.fs.upload_files(files: list[FileUpload])
    """
    try:
        await file_service.upload_files_bulk(sandbox_id, req.files)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get(
    "/sandboxes/{sandbox_id}/files/read",
    response_model=FileContentResponse,
    summary="Read a file from the sandbox",
)
async def read_file(sandbox_id: str, path: str) -> FileContentResponse:
    """Download a file from the sandbox.

    SDK: sandbox.fs.download_file(remote_path) → bytes
    """
    try:
        return await file_service.download_file(sandbox_id, path)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower() or "no such" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc


@router.get(
    "/sandboxes/{sandbox_id}/files/list",
    response_model=FileListResponse,
    summary="List files in a directory",
)
async def list_files(
    sandbox_id: str,
    path: str = "/home/daytona",
    depth: int = 1,
) -> FileListResponse:
    """List entries at a path using SDK filesystem API.

    SDK: sandbox.fs.list_files(path, depth) → list[FileInfo]
    """
    try:
        return await file_service.list_directory(sandbox_id, path, depth)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sandboxes/{sandbox_id}/files/mkdir",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Create a directory",
)
async def create_folder(sandbox_id: str, req: FolderCreateRequest) -> None:
    """Create a directory inside the sandbox.

    SDK: sandbox.fs.create_folder(path, mode)
    """
    try:
        await file_service.create_folder(sandbox_id, req.path, req.mode)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sandboxes/{sandbox_id}/files/delete",
    status_code=status.HTTP_204_NO_CONTENT,
    summary="Delete a file or directory",
)
async def delete_file(sandbox_id: str, req: FileDeleteRequest) -> None:
    """Delete a file or directory inside the sandbox.

    SDK: sandbox.fs.delete_file(path, recursive)
    """
    try:
        await file_service.delete_file(sandbox_id, req.path, req.recursive)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.post(
    "/sandboxes/{sandbox_id}/files/search",
    response_model=FileSearchResult,
    summary="Search for files matching a pattern",
)
async def search_files(sandbox_id: str, req: FileSearchRequest) -> FileSearchResult:
    """Find files matching a pattern.

    SDK: sandbox.fs.find_files(path, pattern) → list[Match]
    """
    try:
        return await file_service.search_files(sandbox_id, req.path, req.pattern)
    except RuntimeError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc


@router.get(
    "/sandboxes/{sandbox_id}/files/info",
    response_model=FileEntry,
    summary="Get file metadata",
)
async def get_file_info(sandbox_id: str, path: str) -> FileEntry:
    """Get metadata for a single file.

    SDK: sandbox.fs.get_file_info(path) → FileInfo
    """
    try:
        return await file_service.get_file_info(sandbox_id, path)
    except RuntimeError as exc:
        msg = str(exc)
        if "not found" in msg.lower():
            raise HTTPException(status_code=404, detail=msg) from exc
        raise HTTPException(status_code=502, detail=msg) from exc
