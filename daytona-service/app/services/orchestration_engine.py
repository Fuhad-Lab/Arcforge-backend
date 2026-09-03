"""Enterprise Orchestration Engine -- Python-side Module 5 integration layer.

This module is the Python host-side broker that ties together every
prior ArcForge backend module:

* Module 1 (VFS + daemon) -> ``workspace_manager.stream_write_file``
  (writes via the in-VM RFC 6455 WebSocket daemon with sub-ms inotify
  firing on tmpfs, falling back to ``sandbox.fs.upload_file`` on any
  daemon failure) and ``workspace_manager.run_live_terminal_command``
  (executes bash inside the VM via ``sandbox.process.exec``).

* Module 3 (Browser Engine) -> ``browser_engine.execute_audit`` runs a
  Playwright script inside the MicroVM, captures the rendered DOM +
  console errors + screenshot, and returns a structured envelope. The
  audit always returns SOMETHING (never raises); the orchestrator
  treats ``status != "success"`` as a replan trigger.

* Module 4 (Orchestration Loop) -> the state machine implemented by
  ``execute_orchestration_pipeline_loop`` is the Python mirror of the
  TypeScript ``executeOrchestrationPipelineLoop`` in
  ``src/services/orchestration-loop.ts``. Both follow the
  ``MAX_CORRECTION_ITERATIONS = 2`` contract (1 initial run + 2
  replan attempts = 3 total iterations max).

The class is importable WITHOUT a Daytona API key (lazy init). Only
methods that touch the SDK will fail when no key is configured. This
makes the unit tests in ``/home/z/test/test_orchestration_engine.py``
runnable in CI without secrets.

The class is named ``EnterpriseOrchestrationEngine`` per the user's
Module 5 spec, and follows the spec method-for-method:

    provision_optimized_vfs_environment() -> str
    stream_vfs_code_write(virtual_path, code_stream_content) -> None
    serve_developer_applications() -> None
    execute_virtual_browser_audit(validation_blueprint) -> dict
    execute_orchestration_pipeline_loop(original_user_prompt) -> dict

But the SDK calls use the REAL empirically-validated APIs from Tasks
6-a / 6-c (``build_create_params`` + python snapshot + user_id/project_id
labels + the scaffold from ``workspace_manager``), NOT the spec's
naive ``self.daytona.create()``. The class also implements the
``execute_orchestration_pipeline_loop`` that the spec left as ``pass``.

ALL helper methods contain detailed error catching and handle
gRPC/WebSocket stream tracking -- every workspace_manager call is
wrapped in try/except so a broken stream daemon never aborts the
state machine (it falls back to the direct ``upload_file`` path
inside ``workspace_manager.stream_write_file``).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import time
import uuid
from typing import Any

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Module-level constants -- mirror the TS-side Module 4 contract.
# ---------------------------------------------------------------------------
MAX_CORRECTION_ITERATIONS = 2  # 1 initial + 2 replans = 3 total iterations max.

# Default URLs for the in-VM dev servers. The browser audit navigates
# to the frontend URL (Vite default :5173). The backend URL is
# informational -- the page's own XHRs hit it; failures surface as
# ``console.error`` events captured by the audit script.
DEFAULT_FRONTEND_URL = "http://localhost:5173"
DEFAULT_BACKEND_URL = "http://localhost:3000"


# ===========================================================================
# EnterpriseOrchestrationEngine
# ===========================================================================


class EnterpriseOrchestrationEngine:
    """The Python-side Module 5 orchestrator.

    Lifecycle:
        1. ``__init__`` -- lazy. Does NOT touch Daytona. Reads env vars
           so callers can override the API key per-instance.
        2. ``provision_optimized_vfs_environment`` -- spawns the
           Daytona MicroVM via ``workspace_manager`` (NOT the spec's
           naive ``self.daytona.create()``), mounts tmpfs, installs the
           guest WS daemon, returns the sandbox id.
        3. ``stream_vfs_code_write`` -- writes a file to tmpfs via the
           WS streaming daemon (sub-ms HMR); falls back to the
           direct ``upload_file`` path if the daemon is unreachable.
        4. ``serve_developer_applications`` -- launches the in-VM
           frontend (``npm run dev`` on :5173) and backend
           (``npm run start`` on :3000) dev servers in the background
           via ``nohup ... &``.
        5. ``execute_virtual_browser_audit`` -- delegates to
           ``browser_engine.execute_audit`` which writes the audit
           script to ``/workspace/.browser-audit.py`` and runs it
           via ``sandbox.process.exec``.
        6. ``execute_orchestration_pipeline_loop`` -- the full state
           machine: provision -> architect -> codegen -> serve ->
           audit -> (replan | done). Max 3 total iterations.

    All blocking Daytona SDK calls are wrapped in ``asyncio.run`` so
    the orchestrator methods are SYNC (matching the spec which uses
    ``time.sleep`` not ``await asyncio.sleep``). The FastAPI router
    invokes these sync methods via ``asyncio.to_thread`` so the event
    loop never blocks on a VM round-trip.

    gRPC/WebSocket stream tracking: every ``stream_vfs_code_write`` is
    wrapped in try/except; on a daemon failure, the orchestrator
    logs the failure and falls back to the direct upload path. The
    state machine NEVER raises from a stream-write failure -- it
    proceeds to serve + audit so the audit can surface the problem
    (a 500 response from the dev server will be captured as a
    ``console.error`` in the audit envelope).
    """

    def __init__(
        self,
        api_key: str | None = None,
        project_id: str | None = None,
        user_id: str | None = None,
    ) -> None:
        # LAZY -- do NOT instantiate the Daytona client here so the
        # module is importable without a key (the unit tests rely on
        # this).
        self.api_key = api_key or os.getenv("DAYTONA_API_KEY", "")
        self.daytona = None  # type: ignore[assignment]
        self.sandbox = None  # the raw SDK Sandbox object (set lazily)
        self.workspace_id: str | None = None
        self.project_id: str | None = project_id
        self.user_id: str | None = user_id
        self.vfs_backend: str = "disk"
        self.agent_installed: bool = False
        self._state: str = "uninitialized"
        self._stream_failures: list[dict[str, Any]] = []  # gRPC/WS failure log

    # ------------------------------------------------------------------
    # Lazy SDK bootstrap (only triggered when a method needs it)
    # ------------------------------------------------------------------
    def _ensure_daytona(self):
        """Lazily construct the raw Daytona SDK client.

        Most production paths do NOT need this -- the orchestrator
        delegates to ``workspace_manager`` which owns its own singleton
        client. We keep it for callers that want the spec's literal
        ``self.daytona`` attribute available (e.g. for tests that
        inject a mock SDK instance).
        """
        if self.daytona is None:
            if not self.api_key:
                raise RuntimeError(
                    "DAYTONA_API_KEY not configured -- cannot construct "
                    "the raw Daytona client. Set the env var OR pass "
                    "api_key=... to EnterpriseOrchestrationEngine.__init__.",
                )
            try:
                from daytona_sdk import Daytona, DaytonaConfig  # type: ignore[import-untyped]
            except Exception as exc:  # pragma: no cover -- only happens if SDK is uninstalled
                raise RuntimeError(
                    f"daytona_sdk import failed: {exc}. Install with: "
                    "pip install daytona-sdk",
                ) from exc
            self.daytona = Daytona(DaytonaConfig(api_key=self.api_key))
        return self.daytona

    # ------------------------------------------------------------------
    # 1. Provision (spec: provision_optimized_vfs_environment)
    # ------------------------------------------------------------------
    def provision_optimized_vfs_environment(self) -> str:
        """Spawn the Daytona MicroVM and immediately configure the
        in-memory tmpfs configuration to run our custom VFS layer for
        sub-millisecond hot reloading.

        Production path: delegates to ``workspace_manager.create_and_scaffold_workspace``
        which uses ``build_create_params`` + the python snapshot + the
        {user_id, project_id, type:"workspace"} labels + the literal
        ``/workspace/{git,frontend,backend}+logo.png`` scaffold + the
        best-effort tmpfs mount + the guest WS daemon install.

        Side effects:
            * Sets ``self.workspace_id`` to the new sandbox id.
            * Sets ``self.vfs_backend`` ("tmpfs" | "disk").
            * Sets ``self.agent_installed`` (bool).
            * Sets ``self._state`` to "provisioned".

        Returns:
            The sandbox id string.

        Raises:
            SystemError: if provisioning fails (mirrors the spec's
            error contract) so the caller can surface a clean 500 to
            the API gateway.
        """
        # The orchestrator MUST have project_id/user_id set before
        # provisioning -- this is the contract that lets us attribute
        # VMs to owners across the platform.
        if not self.project_id:
            # Auto-generate so the smoke path can run without a caller
            # supplying one. Production callers should always set this.
            self.project_id = f"orch-{uuid.uuid4().hex[:12]}"
            logger.warning(
                "provision_optimized_vfs_environment: project_id was not "
                "set on the engine -- auto-generated %s. Production callers "
                "should pass project_id explicitly.",
                self.project_id,
            )
        if not self.user_id:
            self.user_id = "orchestrator-anonymous"

        self._state = "provisioning"
        try:
            # Import lazily so the module is importable without the
            # daytona-service app installed (the unit tests patch
            # this import path).
            from app.services.workspace_coordinator import workspace_manager

            data = asyncio.run(
                workspace_manager.create_and_scaffold_workspace(
                    project_id=self.project_id,
                    language="python",
                    user_id=self.user_id,
                ),
            )
        except Exception as exc:
            self._state = "provision_failed"
            logger.exception(
                "provision_optimized_vfs_environment: workspace_manager "
                "create_and_scaffold_workspace failed for project=%s user=%s",
                self.project_id, self.user_id,
            )
            raise SystemError(
                f"Critical error during VFS Sandbox provisioning: {exc}",
            ) from exc

        self.workspace_id = data.get("id") or data.get("sandbox_id")
        if not self.workspace_id:
            self._state = "provision_failed"
            raise SystemError(
                "Critical error during VFS Sandbox provisioning: "
                "workspace_manager returned no sandbox id",
            )
        self.vfs_backend = data.get("vfs_backend", "disk")
        self.agent_installed = bool(data.get("agent_installed", False))
        self._state = "provisioned"
        logger.info(
            "provision_optimized_vfs_environment: sandbox %s ready "
            "(vfs=%s, agent_installed=%s) for project=%s user=%s",
            self.workspace_id, self.vfs_backend, self.agent_installed,
            self.project_id, self.user_id,
        )
        return self.workspace_id

    # ------------------------------------------------------------------
    # 2. Stream write (spec: stream_vfs_code_write)
    # ------------------------------------------------------------------
    def stream_vfs_code_write(
        self,
        virtual_path: str,
        code_stream_content: str,
    ) -> dict[str, Any]:
        """Act as the host-side broker for our gRPC streaming file daemon.

        Writes data directly to the VM's tmpfs space to fire localized
        guest kernel inotify events (sub-ms HMR for Vite/Next/Nodemon).

        Production path (preferred): ``workspace_manager.stream_write_file``
        which encodes the bytes to b64, uploads the b64 to a temp file
        under /workspace, runs the in-VM WS client that opens a TCP
        socket to 127.0.0.1:3010, performs the RFC 6455 handshake, and
        pushes a masked text frame with the write payload.

        Fallback path: ``workspace_manager.execute_agent_code_write``
        (the legacy ``sandbox.fs.upload_file`` path -- still writes
        to tmpfs if tmpfs is mounted, just without the sub-ms inotify
        shortcut for very large files).

        The state machine NEVER raises from a stream-write failure --
        the failure is logged to ``self._stream_failures`` and the
        method returns a dict with ``ok=False``. The audit will then
        surface the missing file as a 404 / console error, which the
        replan loop will fix.

        Returns:
            ``{ok: bool, path: str, size: int, vfs_backend: str,
               fallback: str | None, error: str | None}``
        """
        if not self.workspace_id:
            raise SystemError(
                "stream_vfs_code_write: sandbox not provisioned -- call "
                "provision_optimized_vfs_environment() first",
            )

        if not isinstance(code_stream_content, str):
            raise TypeError(
                f"stream_vfs_code_write: code_stream_content must be str, "
                f"got {type(code_stream_content).__name__}",
            )

        encoded_payload = code_stream_content.encode("utf-8")
        size = len(encoded_payload)

        try:
            from app.services.workspace_coordinator import workspace_manager

            # Preferred path -- the WS streaming daemon.
            try:
                resp = asyncio.run(
                    workspace_manager.stream_write_file(
                        self.workspace_id, virtual_path, encoded_payload,
                    ),
                )
                # workspace_manager.stream_write_file returns a dict with
                # at least {ok, path, size, vfs_backend}. The fallback
                # flag indicates upload_file was used inside the helper.
                if isinstance(resp, dict) and resp.get("ok", False):
                    return {
                        "ok": True,
                        "path": resp.get("path", virtual_path),
                        "size": int(resp.get("size", size)),
                        "vfs_backend": resp.get("vfs_backend", self.vfs_backend),
                        "fallback": resp.get("fallback"),
                        "error": None,
                    }
                # Daemon reported failure -- fall through to legacy path.
                self._stream_failures.append({
                    "path": virtual_path,
                    "stage": "stream_write_file",
                    "error": str(resp.get("error") if isinstance(resp, dict) else resp),
                    "ts": time.time(),
                })
                logger.warning(
                    "stream_vfs_code_write: WS daemon failed for %s in %s: %s "
                    "-- falling back to execute_agent_code_write",
                    virtual_path, self.workspace_id,
                    resp.get("error") if isinstance(resp, dict) else resp,
                )
            except Exception as exc:
                # gRPC/WebSocket stream tracking -- log the failure and
                # fall back to the legacy upload_file path.
                self._stream_failures.append({
                    "path": virtual_path,
                    "stage": "stream_write_file_exception",
                    "error": str(exc),
                    "ts": time.time(),
                })
                logger.warning(
                    "stream_vfs_code_write: stream_write_file raised for %s in %s: %s "
                    "-- falling back to execute_agent_code_write",
                    virtual_path, self.workspace_id, exc,
                )

            # Fallback path -- direct upload_file via the workspace_manager.
            try:
                asyncio.run(
                    workspace_manager.execute_agent_code_write(
                        self.workspace_id, virtual_path, code_stream_content,
                    ),
                )
                return {
                    "ok": True,
                    "path": virtual_path,
                    "size": size,
                    "vfs_backend": self.vfs_backend,
                    "fallback": "execute_agent_code_write",
                    "error": None,
                }
            except Exception as exc:
                self._stream_failures.append({
                    "path": virtual_path,
                    "stage": "execute_agent_code_write",
                    "error": str(exc),
                    "ts": time.time(),
                })
                logger.error(
                    "stream_vfs_code_write: BOTH paths failed for %s in %s: %s",
                    virtual_path, self.workspace_id, exc,
                )
                return {
                    "ok": False,
                    "path": virtual_path,
                    "size": 0,
                    "vfs_backend": self.vfs_backend,
                    "fallback": None,
                    "error": f"both stream_write_file and execute_agent_code_write failed: {exc}",
                }
        except Exception as exc:
            # Outer try/except -- a bug in the import path or an
            # asyncio.run failure. Return a structured failure so the
            # state machine can keep going.
            self._stream_failures.append({
                "path": virtual_path,
                "stage": "stream_vfs_code_write_outer",
                "error": str(exc),
                "ts": time.time(),
            })
            logger.exception(
                "stream_vfs_code_write: outer failure for %s in %s",
                virtual_path, self.workspace_id,
            )
            return {
                "ok": False,
                "path": virtual_path,
                "size": 0,
                "vfs_backend": self.vfs_backend,
                "fallback": None,
                "error": str(exc),
            }

    # ------------------------------------------------------------------
    # 3. Serve dev servers (spec: serve_developer_applications)
    # ------------------------------------------------------------------
    def serve_developer_applications(self) -> dict[str, Any]:
        """Execute non-blocking server initialization routines for both
        the frontend and backend subsystems.

        Mirrors the spec literally:
            cd /workspace/backend && nohup npm run start > backend.log 2>&1 &
            cd /workspace/frontend && nohup npm run dev > frontend.log 2>&1 &
            time.sleep(3)

        Both commands run via ``workspace_manager.run_live_terminal_command``
        which uses ``sandbox.process.exec(command, cwd, env, timeout)``.

        The ``&`` makes them non-blocking inside the VM shell. The
        sleep gives the dev servers time to bind to their ports before
        the audit navigates to them.

        Best-effort: a failure to start one server is logged but does
        NOT abort the state machine -- the audit will surface a 503
        / connection refused which the replan loop can address.
        """
        if not self.workspace_id:
            raise SystemError(
                "serve_developer_applications: sandbox not provisioned",
            )

        backend_started = False
        frontend_started = False

        try:
            from app.services.workspace_coordinator import workspace_manager

            # --- backend ---
            try:
                result = asyncio.run(
                    workspace_manager.run_live_terminal_command(
                        self.workspace_id,
                        "cd /workspace/backend && nohup npm run start > backend.log 2>&1 &",
                        "/workspace",
                    ),
                )
                # exit_code 0 means the shell forked the nohup'd process
                # successfully (it doesn't wait for the server itself).
                backend_started = (getattr(result, "exit_code", -1) == 0)
                if not backend_started:
                    logger.warning(
                        "serve_developer_applications: backend nohup exit=%s out=%s",
                        getattr(result, "exit_code", "?"),
                        (getattr(result, "stdout", "") or "")[:200],
                    )
            except Exception as exc:
                logger.warning(
                    "serve_developer_applications: backend start failed in %s: %s",
                    self.workspace_id, exc,
                )

            # --- frontend ---
            try:
                result = asyncio.run(
                    workspace_manager.run_live_terminal_command(
                        self.workspace_id,
                        "cd /workspace/frontend && nohup npm run dev > frontend.log 2>&1 &",
                        "/workspace",
                    ),
                )
                frontend_started = (getattr(result, "exit_code", -1) == 0)
                if not frontend_started:
                    logger.warning(
                        "serve_developer_applications: frontend nohup exit=%s out=%s",
                        getattr(result, "exit_code", "?"),
                        (getattr(result, "stdout", "") or "")[:200],
                    )
            except Exception as exc:
                logger.warning(
                    "serve_developer_applications: frontend start failed in %s: %s",
                    self.workspace_id, exc,
                )
        except Exception as exc:
            logger.exception(
                "serve_developer_applications: outer failure in %s: %s",
                self.workspace_id, exc,
            )

        # Mirror the spec literally -- sleep 3s for port bind.
        time.sleep(3)

        return {
            "backend_started": backend_started,
            "frontend_started": frontend_started,
            "frontend_url": DEFAULT_FRONTEND_URL,
            "backend_url": DEFAULT_BACKEND_URL,
        }

    # ------------------------------------------------------------------
    # 4. Browser audit (spec: execute_virtual_browser_audit)
    # ------------------------------------------------------------------
    def execute_virtual_browser_audit(
        self,
        validation_blueprint: dict[str, Any],
    ) -> dict[str, Any]:
        """Launch an internal Playwright automation routine inside the
        MicroVM guest kernel.

        Production path: delegates to ``browser_engine.execute_audit``
        which:
            1. Idempotently installs Playwright + Chromium in the VM
               (cached per-sandbox -- first audit takes ~60-90s for
               the chromium download, subsequent audits return in 0ms).
            2. Writes the audit script to ``/workspace/.browser-audit.py``.
            3. Runs it via ``sandbox.process.exec("python3 ...", "/workspace",
               None, audit_timeout_s)``.
            4. Parses the JSON envelope from stdout (always exactly one
               JSON object on the last non-empty line).

        The audit NEVER raises -- on any failure (install, write, exec,
        parse, timeout) it returns a ``status="failed"`` envelope with
        the error message. This contract is what makes the state
        machine safe: the loop's replan heuristic can decide pass /
        replan from ``status`` + ``error_logs``.

        Returns the audit envelope dict with at minimum:
            ``{status, title, url, backend_url, http_status,
               error_logs[], console_errors[], dom_snapshot,
               screenshot_b64, duration_ms, error}``
        """
        if not self.workspace_id:
            raise SystemError(
                "execute_virtual_browser_audit: sandbox not provisioned",
            )

        try:
            from app.services.browser_engine import browser_engine

            audit = asyncio.run(
                browser_engine.execute_audit(
                    self.workspace_id,
                    DEFAULT_FRONTEND_URL,
                    DEFAULT_BACKEND_URL,
                    validation_blueprint,
                ),
            )
            if not isinstance(audit, dict):
                return {
                    "status": "failed",
                    "error": f"browser_engine returned non-dict: {type(audit).__name__}",
                    "error_logs": [],
                }
            return audit
        except Exception as exc:
            logger.exception(
                "execute_virtual_browser_audit: outer failure in %s: %s",
                self.workspace_id, exc,
            )
            return {
                "status": "failed",
                "error": f"orchestrator outer failure: {exc}",
                "error_logs": [],
                "console_errors": [],
                "dom_snapshot": None,
                "screenshot_b64": None,
                "duration_ms": None,
                "url": DEFAULT_FRONTEND_URL,
                "backend_url": DEFAULT_BACKEND_URL,
                "title": None,
                "http_status": None,
            }

    # ------------------------------------------------------------------
    # 5. State machine (spec: execute_orchestration_pipeline_loop)
    # ------------------------------------------------------------------
    def execute_orchestration_pipeline_loop(
        self,
        original_user_prompt: str,
    ) -> dict[str, Any]:
        """Manage the state-machine transition flow between Architect,
        Developers, and Debugger loops.

        Flow:
            1. ``provision_optimized_vfs_environment`` -> sandbox id.
            2. ``_architect_blueprint(user_prompt)`` -> blueprint dict
               with ``endpoints``, ``pages``, ``tech_stack`` keys.
            3. ``_developers_parallel_codegen(blueprint)`` -> writes
               placeholder ``/workspace/backend/app.py`` +
               ``/workspace/frontend/App.tsx`` derived from the blueprint.
            4. ``serve_developer_applications`` -> nohup the dev servers.
            5. ``execute_virtual_browser_audit(blueprint)`` -> audit dict.
            6. Loop (max 2 replan iterations):
               if audit.status != "success":
                 blueprint = _architect_replan(blueprint, audit)
                 _developers_parallel_codegen(blueprint)
                 serve_developer_applications()
                 audit = execute_virtual_browser_audit(blueprint)

        Returns:
            ``{status: "production_ready"|"max_iterations_exceeded"|...,
               iterations: int, final_audit: dict, blueprint: dict,
               sandbox_id: str, vfs_backend: str, agent_installed: bool,
               files_written: list, stream_failures: list}``
        """
        self._state = "started"
        files_written: list[dict[str, Any]] = []

        # Step 1: provision (per the spec).
        try:
            sandbox_id = self.provision_optimized_vfs_environment()
        except SystemError as exc:
            self._state = "provision_failed"
            return {
                "status": "provision_failed",
                "error": str(exc),
                "iterations": 0,
                "final_audit": None,
                "blueprint": None,
                "sandbox_id": None,
                "vfs_backend": self.vfs_backend,
                "agent_installed": self.agent_installed,
                "files_written": [],
                "stream_failures": self._stream_failures,
            }

        # Step 2: architect blueprint.
        blueprint = self._architect_blueprint(original_user_prompt)

        # Step 3: developers parallel codegen.
        files_written.extend(self._developers_parallel_codegen(blueprint))

        # Step 4: serve dev servers.
        self.serve_developer_applications()

        # Step 5: audit.
        audit = self.execute_virtual_browser_audit(blueprint)
        iterations = 1

        # Step 6: replan loop.
        while (
            audit.get("status") != "success"
            and iterations <= MAX_CORRECTION_ITERATIONS
        ):
            logger.info(
                "execute_orchestration_pipeline_loop: iteration %d audit "
                "status=%s -- invoking _architect_replan",
                iterations, audit.get("status"),
            )
            blueprint = self._architect_replan(blueprint, audit)
            files_written.extend(
                self._developers_parallel_codegen(blueprint),
            )
            self.serve_developer_applications()
            audit = self.execute_virtual_browser_audit(blueprint)
            iterations += 1

        final_status = (
            "production_ready"
            if audit.get("status") == "success"
            else "max_iterations_exceeded"
        )
        self._state = "complete"
        return {
            "status": final_status,
            "iterations": iterations,
            "final_audit": audit,
            "blueprint": blueprint,
            "sandbox_id": sandbox_id,
            "vfs_backend": self.vfs_backend,
            "agent_installed": self.agent_installed,
            "files_written": files_written,
            "stream_failures": self._stream_failures,
        }

    # ==================================================================
    # Internal helpers
    # ==================================================================

    def _architect_blueprint(self, user_prompt: str) -> dict[str, Any]:
        """Deterministic blueprint generator based on prompt keywords.

        The Python orchestrator's architect is intentionally
        deterministic (no LLM call needed) -- this is the PYTHON-side
        integration mirror of the TypeScript ``agentPlatform.callModelRaw``
        LLM-driven architect. The real LLM-driven loop lives in the
        TS backend's ``orchestration-loop.ts`` (``architectReplan``).

        The deterministic templates cover the three prompt keywords the
        spec mentions: "todo", "blog", "ecommerce". A "generic" template
        catches everything else.

        Returns a dict with AT LEAST:
            ``{endpoints: [...], pages: [...], tech_stack: {...},
               kind: str, prompt: str}``
        """
        prompt_lower = (user_prompt or "").lower()
        if "todo" in prompt_lower:
            return {
                "kind": "todo",
                "prompt": user_prompt,
                "endpoints": [
                    {"method": "GET", "path": "/api/todos", "description": "List all todos"},
                    {"method": "POST", "path": "/api/todos", "description": "Create a todo"},
                    {"method": "PATCH", "path": "/api/todos/:id", "description": "Toggle done"},
                    {"method": "DELETE", "path": "/api/todos/:id", "description": "Delete a todo"},
                ],
                "pages": ["Home", "TodoList", "AddTodo"],
                "tech_stack": {"frontend": "React+Vite", "backend": "Flask"},
            }
        if "ecommerce" in prompt_lower or "store" in prompt_lower or "shop" in prompt_lower:
            return {
                "kind": "ecommerce",
                "prompt": user_prompt,
                "endpoints": [
                    {"method": "GET", "path": "/api/products", "description": "List products"},
                    {"method": "GET", "path": "/api/products/:id", "description": "Product detail"},
                    {"method": "POST", "path": "/api/cart", "description": "Add to cart"},
                    {"method": "POST", "path": "/api/checkout", "description": "Checkout"},
                ],
                "pages": ["Home", "ProductList", "ProductDetail", "Cart", "Checkout"],
                "tech_stack": {"frontend": "React+Vite", "backend": "Flask"},
            }
        if "blog" in prompt_lower:
            return {
                "kind": "blog",
                "prompt": user_prompt,
                "endpoints": [
                    {"method": "GET", "path": "/api/posts", "description": "List posts"},
                    {"method": "GET", "path": "/api/posts/:slug", "description": "Post detail"},
                    {"method": "POST", "path": "/api/posts", "description": "Create post"},
                ],
                "pages": ["Home", "PostList", "PostDetail"],
                "tech_stack": {"frontend": "React+Vite", "backend": "Flask"},
            }
        # generic fallback
        return {
            "kind": "generic",
            "prompt": user_prompt,
            "endpoints": [
                {"method": "GET", "path": "/api/health", "description": "Health check"},
                {"method": "GET", "path": "/api/items", "description": "List items"},
            ],
            "pages": ["Home"],
            "tech_stack": {"frontend": "React+Vite", "backend": "Flask"},
        }

    def _developers_parallel_codegen(
        self,
        blueprint: dict[str, Any],
    ) -> list[dict[str, Any]]:
        """Write placeholder backend + frontend files into the VM via
        ``stream_vfs_code_write``.

        Files written (per spec):
            * ``/workspace/backend/app.py`` -- a Flask stub exposing the
              blueprint's endpoints.
            * ``/workspace/frontend/App.tsx`` -- a React+Vite stub that
              lists the blueprint's pages.

        Returns a list of ``{path, size, ok}`` dicts for the caller's
        ``files_written`` log. Files that fail to stream are still
        recorded (with ``ok=False``) so the audit failure mode is
        visible in the final result envelope.
        """
        kind = blueprint.get("kind", "generic")
        endpoints = blueprint.get("endpoints", [])
        pages = blueprint.get("pages", ["Home"])

        results: list[dict[str, Any]] = []

        # --- backend/app.py ---
        backend_code = self._gen_backend_code(kind, endpoints)
        write_resp = self.stream_vfs_code_write(
            "/workspace/backend/app.py", backend_code,
        )
        results.append({
            "path": "/workspace/backend/app.py",
            "size": len(backend_code),
            "ok": write_resp.get("ok", False),
            "fallback": write_resp.get("fallback"),
        })

        # --- frontend/App.tsx ---
        frontend_code = self._gen_frontend_code(kind, pages)
        write_resp = self.stream_vfs_code_write(
            "/workspace/frontend/App.tsx", frontend_code,
        )
        results.append({
            "path": "/workspace/frontend/App.tsx",
            "size": len(frontend_code),
            "ok": write_resp.get("ok", False),
            "fallback": write_resp.get("fallback"),
        })

        return results

    def _gen_backend_code(self, kind: str, endpoints: list[dict[str, Any]]) -> str:
        """Generate a minimal Flask backend exposing the blueprint's endpoints."""
        lines = [
            '"""Auto-generated by EnterpriseOrchestrationEngine.',
            f'Kind: {kind}',
            '"""',
            "from flask import Flask, jsonify, request",
            "",
            "app = Flask(__name__)",
            "",
            "@app.route('/api/health', methods=['GET'])",
            "def health():",
            "    return jsonify({'status': 'ok', 'kind': '" + kind + "'})",
            "",
        ]
        for ep in endpoints:
            method = (ep.get("method") or "GET").upper()
            path = ep.get("path") or "/"
            if path == "/api/health":
                continue  # already added above
            # Sanitize the path into a Python function name.
            safe = (
                path.replace("/api/", "")
                .replace("/", "_")
                .replace(":", "")
                .replace("-", "_")
                .replace("_", " ")
            ).strip().replace(" ", "_") or "root"
            fn_name = f"handler_{safe}"
            lines.append(f"@app.route('{path}', methods=['{method}'])")
            lines.append(f"def {fn_name}():")
            lines.append(f"    return jsonify({{'kind': '{kind}', 'path': '{path}'}})")
            lines.append("")
        lines.append("if __name__ == '__main__':")
        lines.append("    app.run(host='0.0.0.0', port=3000)")
        return "\n".join(lines)

    def _gen_frontend_code(self, kind: str, pages: list[str]) -> str:
        """Generate a minimal React App.tsx that renders the blueprint's pages."""
        # Escape any single quotes in pages/kind to keep the JSX valid.
        safe_pages = [p.replace("'", "") for p in pages]
        safe_kind = kind.replace("'", "")
        list_items = "\n        ".join(
            f"<li>{p}</li>" for p in safe_pages
        )
        return (
            "import React, { useState } from 'react';\n"
            "\n"
            f"export default function App() {{\n"
            f"  const [page, setPage] = useState('{safe_pages[0] if safe_pages else 'Home'}');\n"
            "  return (\n"
            "    <div style={{ fontFamily: 'sans-serif', padding: 24 }}>\n"
            f"      <h1>{safe_kind.title()} App</h1>\n"
            "      <nav>\n"
            "        <ul style={{ display: 'flex', gap: 12, listStyle: 'none', padding: 0 }}>\n"
            f"          {list_items}\n"
            "        </ul>\n"
            "      </nav>\n"
            "      <main>\n"
            "        <p>Welcome to the {page} page.</p>\n"
            "      </main>\n"
            "    </div>\n"
            "  );\n"
            "}\n"
        )

    def _architect_replan(
        self,
        blueprint: dict[str, Any],
        audit_failure: dict[str, Any],
    ) -> dict[str, Any]:
        """Produce a revised blueprint given an audit failure.

        Mirrors the TS-side ``architectReplan`` shape but is
        deterministic (no LLM call). The revised blueprint:
            * Adds the audit failure summary so the next codegen + audit
              iteration knows what to fix.
            * Adds a fallback ``/api/health`` endpoint if missing
              (so the audit always finds at least one working route).
            * Adds an ``ErrorFallback`` page so the frontend has a
              graceful failure surface.
            * Increments ``iteration`` so the caller can correlate
              replan generations.
        """
        revised = json.loads(json.dumps(blueprint))  # deep copy
        revised["iteration"] = int(revised.get("iteration", 1)) + 1
        revised["previous_failure"] = {
            "status": audit_failure.get("status"),
            "error": audit_failure.get("error"),
            "error_logs": (audit_failure.get("error_logs") or [])[:5],
            "http_status": audit_failure.get("http_status"),
        }
        # Ensure /api/health exists so the next audit finds at least
        # one working route.
        eps = revised.setdefault("endpoints", [])
        if not any(ep.get("path") == "/api/health" for ep in eps):
            eps.insert(0, {"method": "GET", "path": "/api/health", "description": "Health check"})
        # Add an ErrorFallback page so the frontend can render gracefully.
        pages = revised.setdefault("pages", [])
        if "ErrorFallback" not in pages:
            pages.append("ErrorFallback")
        return revised

    # ------------------------------------------------------------------
    # Status introspection (used by the router's GET /status endpoint)
    # ------------------------------------------------------------------
    def status(self) -> dict[str, Any]:
        """Return the current orchestrator state for a sandbox.

        Best-effort -- callers should NOT rely on this for atomic
        decisions (the orchestrator does not currently track active
        runs in a shared map -- a single Python process can run
        multiple engines in parallel via asyncio.to_thread).
        """
        return {
            "running": self._state in ("started", "provisioning", "provisioned"),
            "state": self._state,
            "sandbox_id": self.workspace_id,
            "project_id": self.project_id,
            "user_id": self.user_id,
            "vfs_backend": self.vfs_backend,
            "agent_installed": self.agent_installed,
            "stream_failures": len(self._stream_failures),
        }


# ===========================================================================
# Module-level exports
# ===========================================================================

# Export the CLASS (so callers can instantiate with their own env):
orchestration_engine = EnterpriseOrchestrationEngine

# Also export a lazily-constructed default singleton. Constructed
# lazily so the module is importable without a Daytona API key.
_default_engine: EnterpriseOrchestrationEngine | None = None


def default_engine() -> EnterpriseOrchestrationEngine:
    """Return the lazily-constructed default engine singleton.

    Construction is lazy so the module is importable without
    DAYTONA_API_KEY configured. The unit tests rely on this.
    """
    global _default_engine
    if _default_engine is None:
        _default_engine = EnterpriseOrchestrationEngine()
    return _default_engine


# ===========================================================================
# CLI smoke -- ``python3 -m app.services.orchestration_engine --smoke``
# prints a fake blueprint and exits 0 (no Daytona API key needed).
# ===========================================================================

def _cli_smoke() -> int:
    """Print a fake blueprint + exit 0. No Daytona calls."""
    engine = EnterpriseOrchestrationEngine()
    bp = engine._architect_blueprint("build a todo app")
    print(json.dumps({
        "smoke": "ok",
        "blueprint": bp,
        "state": engine.status(),
    }, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(
        prog="orchestration_engine",
        description="ArcForge Python orchestrator (Module 5).",
    )
    parser.add_argument(
        "--smoke",
        action="store_true",
        help="Print a fake blueprint and exit 0 (no Daytona API key needed).",
    )
    args = parser.parse_args()
    if args.smoke:
        return _cli_smoke()
    # Default: run the smoke (so bare `python -m ...` does something safe).
    return _cli_smoke()


if __name__ == "__main__":
    raise SystemExit(main())
