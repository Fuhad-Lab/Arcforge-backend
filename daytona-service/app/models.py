"""Pydantic models for request / response serialization.

All models use strict mode for production safety.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Enums
# ---------------------------------------------------------------------------


class SandboxLanguage(str, Enum):
    PYTHON = "python"
    NODEJS = "nodejs"
    GO = "go"
    RUST = "rust"
    JAVA = "java"
    TYPESCRIPT = "typescript"
    JAVASCRIPT = "javascript"


class SandboxState(str, Enum):
    """Mirrors Daytona SandboxState enum values."""
    CREATING = "Creating"
    RUNNING = "Running"
    STOPPED = "Stopped"
    ERROR = "Error"
    STARTING = "Starting"
    STOPPING = "Stopping"
    PAUSED = "Paused"
    ARCHIVED = "Archived"
    UNKNOWN = "Unknown"


class CreateMethod(str, Enum):
    SNAPSHOT = "snapshot"
    IMAGE = "image"


# ---------------------------------------------------------------------------
# Request models
# ---------------------------------------------------------------------------


class ResourceSpec(BaseModel):
    """Resource requirements for a sandbox."""
    cpu: float = Field(default=2.0, ge=0.25, le=32.0, description="CPU cores")
    memory: str = Field(default="4Gi", description="Memory (e.g. '4Gi', '512Mi', or numeric GB)")
    disk: str = Field(default="10Gi", description="Disk size (e.g. '10Gi', '100Gi')")
    gpu: float = Field(default=0, ge=0, description="GPU count")


class CreateSandboxRequest(BaseModel):
    """Create a new sandbox."""
    method: CreateMethod = Field(default=CreateMethod.SNAPSHOT)
    language: SandboxLanguage | None = Field(
        default=None,
        description="Language (required when method=snapshot)",
    )
    image: str | None = Field(
        default=None,
        description="Container image (required when method=image)",
    )
    name: str | None = Field(default=None, max_length=128)
    resources: ResourceSpec | None = Field(default=None)
    labels: dict[str, str] = Field(default_factory=dict)
    env_vars: dict[str, str] = Field(default_factory=dict)
    auto_start: bool = Field(default=True)


class CodeRunRequest(BaseModel):
    """Execute interpreted code inside a sandbox via process.code_run().

    SDK: sandbox.process.code_run(code, params=None, timeout=None)
    CodeRunParams(argv=None, env=None)
    """
    code: str = Field(min_length=1, description="Source code to execute")
    env: dict[str, str] | None = Field(default=None, description="Environment variables")
    timeout_ms: int = Field(default=30000, ge=1000, le=300000, description="Timeout in milliseconds")


class ShellExecRequest(BaseModel):
    """Execute a shell command inside a sandbox via process.exec().

    SDK: sandbox.process.exec(command, cwd=None, env=None, timeout=None)
    """
    command: str = Field(min_length=1, description="Shell command to execute")
    cwd: str | None = Field(default=None, description="Working directory")
    env: dict[str, str] | None = Field(default=None, description="Environment variables")
    timeout_ms: int = Field(default=30000, ge=1000, le=300000, description="Timeout in milliseconds")


class BulkFileUploadRequest(BaseModel):
    """Upload multiple files at once."""
    files: list[dict[str, str]] = Field(
        min_length=1,
        description="List of {path: str, content: str} objects",
    )


class FolderCreateRequest(BaseModel):
    """Create a directory inside a sandbox."""
    path: str = Field(min_length=1, description="Directory path")
    mode: str = Field(default="0755", description="Unix permissions")


class FileDeleteRequest(BaseModel):
    """Delete a file or directory inside a sandbox."""
    path: str = Field(min_length=1, description="File/directory path")
    recursive: bool = Field(default=False)


class FileSearchRequest(BaseModel):
    """Search for files matching a pattern."""
    path: str = Field(default="/home/daytona", description="Root search path")
    pattern: str = Field(min_length=1, description="Glob or regex pattern")


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ResourcesResponse(BaseModel):
    cpu: float = 0.0
    memory: str = ""
    disk: str = ""
    gpu: float = 0.0


class SandboxResponse(BaseModel):
    """Serialised sandbox returned by the API."""
    id: str
    name: str | None = None
    state: SandboxState = SandboxState.UNKNOWN
    target: str | None = None
    resources: ResourcesResponse | None = None
    labels: dict[str, str] = Field(default_factory=dict)
    env: dict[str, str] = Field(default_factory=dict)
    created_at: datetime | None = None
    error: str | None = None
    recoverable: bool | None = None


class CodeRunResult(BaseModel):
    """Result of a code execution.

    This is the feedback loop data: exit_code + result (stdout) flow
    back into the agent context window for autonomous self-debugging.
    """
    exit_code: int = 0
    stdout: str = ""
    stderr: str = ""
    timed_out: bool = False
    duration_ms: int | None = None
    charts: list[dict[str, Any]] | None = None


class FileEntry(BaseModel):
    """A single file/directory entry from sandbox.fs.list_files()."""
    name: str = ""
    path: str = ""
    is_dir: bool = False
    size: int = 0
    modified_at: str | None = None
    permissions: str | None = None


class FileContentResponse(BaseModel):
    """Content of a file read from a sandbox."""
    path: str
    content: str
    size_bytes: int = 0


class FileListResponse(BaseModel):
    """Listing of files in a sandbox directory."""
    path: str
    entries: list[FileEntry] = Field(default_factory=list)
    total: int = 0


class FileSearchResult(BaseModel):
    """Result of a file search operation."""
    path: str
    pattern: str
    matches: list[dict[str, Any]] = Field(default_factory=list)
    total: int = 0


class HealthResponse(BaseModel):
    """Service health check payload."""
    status: str = "healthy"
    version: str
    daytona_connected: bool = False
    active_sandboxes: int = 0


class ErrorResponse(BaseModel):
    """Standard error envelope."""
    error: str
    detail: str = ""
    status_code: int = 500


class SandboxListResponse(BaseModel):
    """Sandbox listing."""
    items: list[SandboxResponse] = Field(default_factory=list)
    total: int = 0


class BulkActionResponse(BaseModel):
    """Result of a bulk operation on sandboxes."""
    succeeded: list[str] = Field(default_factory=list)
    failed: dict[str, str] = Field(default_factory=dict)


# ---------------------------------------------------------------------------
# Workspace models
# ---------------------------------------------------------------------------


class CreateWorkspaceRequest(BaseModel):
    """Create a new project workspace with the mandatory directory blueprint."""
    project_id: str = Field(min_length=1, max_length=128, description="Project UUID")
    user_id: str | None = Field(
        default=None,
        max_length=128,
        description="Owner user UUID — every sandbox must carry BOTH user_id and project_id labels",
    )
    language: str = Field(default="nodejs", description="Sandbox language runtime")


class AgentCodeWriteRequest(BaseModel):
    """Write a single file directly into the VM."""
    path: str = Field(min_length=1, description="File path inside workspace (e.g. frontend/index.html)")
    content: str = Field(min_length=0, description="File content")


class AgentBulkWriteRequest(BaseModel):
    """Write multiple files into the VM in one batch."""
    files: list[dict[str, str]] = Field(
        min_length=1,
        description="List of {path: str, content: str} objects",
    )


class TerminalCommandRequest(BaseModel):
    """Execute a bash command in the VM terminal."""
    command: str = Field(min_length=1, description="Bash command to execute")
    cwd: str | None = Field(default=None, description="Working directory (defaults to /workspace)")
    timeout_ms: int = Field(default=30000, ge=1000, le=300000)


class FileTreeResponse(BaseModel):
    """Nested file tree structure for the frontend sidebar."""
    name: str
    path: str
    type: str = Field(description="'directory' or 'file'")
    size: int = 0
    modified_at: str | None = None
    children: list["FileTreeResponse"] = Field(default_factory=list)


class WorkspaceInitResponse(BaseModel):
    """Response after workspace creation + scaffold."""
    sandbox_id: str
    project_id: str
    user_id: str | None = None
    state: str
    provision_time_ms: int
    workspace_root: str = "/workspace"
    structure: list[str] = Field(
        default_factory=lambda: ["git/", "frontend/", "backend/", "logo.png"],
        description="Mandatory workspace directories",
    )