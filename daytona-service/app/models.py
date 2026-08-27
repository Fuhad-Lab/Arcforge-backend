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


class AgentLlmConfig(BaseModel):
    """LLM endpoint config handed to the in-VM orchestrator daemon.

    Mirrors the platform's single-mode ("Solo · GLM") settings from the
    Node backend — the daemon calls the OpenAI-compatible endpoint
    directly from inside the VM, so the pipeline no longer round-trips
    through the host for every phase.
    """
    url: str = Field(default="", description="OpenAI-compatible chat-completions URL")
    key: str = Field(default="", description="API key (Bearer)")
    model: str = Field(default="openai/gpt-oss-120b", description="Model id")


class SkillInstruction(BaseModel):
    """One platform skill hosted by the in-VM skills MCP server.

    Mirrors { name, scope, description, instruction, source } from the
    Node backend's skill-registry (vmSkillsCatalog — the single source of
    truth). Written verbatim into skills.json next to the sidecar; the
    in-VM skills_server.py enforces STRICT per-agent segregation from the
    scope tags (chief / frontend / backend / debugger).
    """
    name: str = Field(min_length=1, max_length=120)
    instruction: str = Field(default="", max_length=4000)
    description: str = Field(default="", max_length=2000)
    source: str = Field(default="", max_length=400)
    scope: list[str] = Field(
        default_factory=list,
        description=("Swarm roles allowed to consult this skill "
                     "(chief|frontend|backend|debugger). Empty = not hosted."),
    )


class CreateWorkspaceRequest(BaseModel):
    """Create a new project workspace with the mandatory directory blueprint."""
    project_id: str = Field(min_length=1, max_length=128, description="Project UUID")
    user_id: str | None = Field(
        default=None,
        max_length=128,
        description="Owner user UUID — every sandbox must carry BOTH user_id and project_id labels",
    )
    language: str = Field(default="nodejs", description="Sandbox language runtime")
    agent_llm: AgentLlmConfig | None = Field(
        default=None,
        description=(
            "Optional LLM config for the in-VM agent orchestrator sidecar. "
            "When omitted the sidecar is installed WITHOUT an LLM endpoint "
            "(daemon runs, pipeline degrades to host-side SSE)."
        ),
    )
    skills: list[SkillInstruction] | None = Field(
        default=None,
        description=(
            "Platform skill catalog to plant as skills.json next to the "
            "sidecar — hosted by the in-VM skills MCP server with strict "
            "per-agent scope segregation."
        ),
    )


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
    vfs_backend: str = Field(
        default="disk",
        description=(
            "VFS backend for /workspace — 'tmpfs' (RAM disk, sub-ms HMR) "
            "or 'disk' (fallback when tmpfs mount fails)"
        ),
    )
    agent_installed: bool = Field(
        default=False,
        description=(
            "True if the guest workspace-agent daemon was installed and "
            "verified running inside the VM"
        ),
    )
    structure: list[str] = Field(
        default_factory=lambda: ["git/", "frontend/", "backend/", "logo.png"],
        description="Mandatory workspace directories",
    )


class AgentSidecarInfo(BaseModel):
    """Connection info for the in-VM agent orchestrator ("Shadow Agent").

    The browser connects its WebSocket straight to `url` (a Daytona preview
    link for the daemon's port) authenticating with `token`. The token is
    generated per-VM at creation and lives only inside the VM — this
    response is the sole broker path, always behind JWT + ownership checks.
    Served by GET /api/v1/workspace/{id}/agent-info (probes the live VM).
    """
    installed: bool = False
    port: int = 9000
    url: str | None = None
    token: str | None = None
    launcher: str | None = Field(
        default=None, description="'pm2' or 'watchdog' (fallback supervisor)",
    )
    alive: bool = Field(
        default=False, description="True when the daemon answers /health",
    )
    app_url: str | None = Field(
        default=None,
        description=(
            "SIGNED Daytona preview URL for the generated app's dev-server "
            "port (3000 Next.js / 5173 legacy Vite). Null until a server "
            "actually answers inside the VM — the studio Preview tab "
            "iframes it for the REAL live preview."
        ),
    )
    app_port: int | None = Field(
        default=None,
        description="The port app_url points at (3000 or 5173), else None",
    )


WorkspaceInitResponse.model_rebuild()


# ---------------------------------------------------------------------------
# Module 1 VFS: stream-write + VFS status
# ---------------------------------------------------------------------------


class StreamWriteRequest(BaseModel):
    """Stream-write a file to the guest daemon (Module 1 VFS).

    Either ``content_b64`` (base64-encoded bytes — preferred for binary
    payloads and avoids any UTF-8 decoding issues) or ``content`` (raw
    UTF-8 text) must be supplied. If both are present, ``content_b64`` wins.
    """
    path: str = Field(min_length=1, description="File path inside workspace")
    content_b64: str | None = Field(
        default=None,
        description="Base64-encoded file bytes (preferred for binary)",
    )
    content: str | None = Field(
        default=None,
        description="Raw UTF-8 file content (alternative to content_b64)",
    )


class StreamWriteResponse(BaseModel):
    """Result of a stream-write to the guest daemon."""
    ok: bool
    path: str
    size: int = 0
    vfs_backend: str = "disk"


class VfsStatusResponse(BaseModel):
    """Snapshot of VFS + daemon + persistence state (Module 1)."""
    tmpfs_mounted: bool
    daemon_running: bool
    dirty_count: int
    last_flush_at: str | None = None
    persist_dir: str


# ---------------------------------------------------------------------------
# Module 3 Browser Engine: install + audit
# ---------------------------------------------------------------------------


class BrowserAuditRequest(BaseModel):
    """Request shape for the in-VM Playwright audit endpoint (Module 3).

    ``frontend_url`` is the local URL the headless browser navigates to --
    almost always ``http://localhost:5173`` (Vite default) inside the VM.
    ``backend_url`` is informational; the audit script surfaces it back
    in the response so the host can correlate.
    ``validation_blueprint`` is the OpenAPI / heuristic contract from the
    Architect -- currently passed through to the response shape for
    correlation; actual blueprint-aware assertions live in the host
    backend's heuristic evaluator (Module 4).
    """
    frontend_url: str = Field(min_length=1)
    backend_url: str | None = None
    validation_blueprint: dict[str, Any] | None = None


class BrowserAuditResult(BaseModel):
    """Result of a single in-VM Playwright audit run (Module 3).

    Always returned -- even on failure (``status="failed"`` with the
    ``error`` field populated). The orchestration loop's evaluator
    (Module 4) heuristically decides pass / replan from ``status`` +
    ``error_logs`` + ``http_status``.
    """
    status: str = "failed"
    title: str | None = None
    url: str | None = None
    backend_url: str | None = None
    http_status: int | None = None
    error_logs: list[str] = Field(default_factory=list)
    console_errors: list[str] = Field(default_factory=list)
    dom_snapshot: str | None = None
    screenshot_b64: str | None = None
    duration_ms: int | None = None
    error: str | None = None


class BrowserInstallResult(BaseModel):
    """Result of an idempotent Chromium install in the VM (Module 3)."""
    installed: bool = False
    browser_path: str | None = None
    install_log: str = ""
    duration_ms: int | None = None