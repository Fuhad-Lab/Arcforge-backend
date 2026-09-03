"""Business logic for code execution inside sandboxes.

Daytona SDK v0.205.1 code execution API:
  sandbox.process.exec(command, cwd=None, env=None, timeout=None) → ExecuteResponse
    ExecuteResponse has: exit_code, result (stdout), artifacts (stdout + charts)

  sandbox.process.code_run(code, params=None, timeout=None) → ExecuteResponse
    CodeRunParams(argv=None, env=None)
    ExecuteResponse has: exit_code, result (stdout), artifacts (stdout + charts)

The `result` field contains stdout. `exit_code` is the process exit status.
This is the continuous feedback loop: exit_code + result flow back to the agent
for autonomous self-debugging.
"""

from __future__ import annotations

import asyncio
import logging
import time

from app.daytona_client import get_daytona
from app.models import CodeRunRequest, CodeRunResult, ShellExecRequest

logger = logging.getLogger(__name__)


async def execute_code(sandbox_id: str, req: CodeRunRequest) -> CodeRunResult:
    """Run code inside sandbox *sandbox_id* using ``process.code_run()``.

    SDK signature:
      sandbox.process.code_run(code: str, params: CodeRunParams | None, timeout: int | None)
      CodeRunParams(argv: list[str] | None, env: dict[str, str] | None)
      ExecuteResponse(exit_code, result, artifacts)
    """
    daytona = get_daytona()

    # Fetch sandbox reference
    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
    except Exception as exc:
        raise RuntimeError(f"Cannot access sandbox {sandbox_id}: {exc}") from exc

    timeout_sec = req.timeout_ms // 1000 if req.timeout_ms else 30
    env_vars = req.env if hasattr(req, "env") and req.env else None

    t0 = time.monotonic()
    try:
        # Use process.code_run for interpreted code execution
        result = await asyncio.to_thread(
            sandbox.process.code_run,
            req.code,
            None,  # CodeRunParams — None for defaults
            timeout_sec,
        )
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Code execution failed in sandbox %s", sandbox_id)
        return CodeRunResult(
            exit_code=-1,
            stderr=str(exc),
            timed_out="timeout" in str(exc).lower(),
            duration_ms=elapsed_ms,
        )

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    return _parse_execute_response(result, elapsed_ms)


async def execute_shell(sandbox_id: str, req: ShellExecRequest) -> CodeRunResult:
    """Execute a shell command inside sandbox *sandbox_id* using ``process.exec()``.

    SDK signature:
      sandbox.process.exec(command: str, cwd: str | None, env: dict | None, timeout: int | None)
      ExecuteResponse(exit_code, result, artifacts)
    """
    daytona = get_daytona()

    try:
        sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
    except Exception as exc:
        raise RuntimeError(f"Cannot access sandbox {sandbox_id}: {exc}") from exc

    timeout_sec = req.timeout_ms // 1000 if req.timeout_ms else 30

    t0 = time.monotonic()
    try:
        result = await asyncio.to_thread(
            sandbox.process.exec,
            req.command,
            req.cwd,
            req.env,
            timeout_sec,
        )
    except Exception as exc:
        elapsed_ms = int((time.monotonic() - t0) * 1000)
        logger.exception("Shell exec failed in sandbox %s", sandbox_id)
        return CodeRunResult(
            exit_code=-1,
            stderr=str(exc),
            timed_out="timeout" in str(exc).lower(),
            duration_ms=elapsed_ms,
        )

    elapsed_ms = int((time.monotonic() - t0) * 1000)
    return _parse_execute_response(result, elapsed_ms)


def _parse_execute_response(result: object, elapsed_ms: int) -> CodeRunResult:
    """Extract data from SDK ExecuteResponse into our CodeRunResult.

    ExecuteResponse fields (v0.205.1):
      - exit_code: int | None
      - result: str (stdout output)
      - artifacts: ExecutionArtifacts (stdout, charts)
    """
    exit_code = getattr(result, "exit_code", -1)
    if exit_code is None:
        exit_code = -1

    stdout = getattr(result, "result", "") or ""

    # Check artifacts for additional stdout
    artifacts = getattr(result, "artifacts", None)
    artifact_stdout = ""
    if artifacts is not None:
        artifact_stdout = getattr(artifacts, "stdout", "") or ""

    # Use the longer of result vs artifacts.stdout
    final_stdout = stdout if len(stdout) >= len(artifact_stdout) else artifact_stdout

    # Charts metadata if present
    charts = []
    if artifacts is not None:
        raw_charts = getattr(artifacts, "charts", None)
        if raw_charts:
            charts = [
                {"type": str(getattr(c, "type", "unknown")), "title": getattr(c, "title", "")}
                for c in raw_charts
            ]

    return CodeRunResult(
        exit_code=int(exit_code),
        stdout=final_stdout,
        stderr="",
        timed_out=False,
        duration_ms=elapsed_ms,
        charts=charts if charts else None,
    )
