"""Browser Engine Manager -- Playwright-in-VM integration (Module 3).

This module gives the ArcForge orchestration loop (Module 4) the ability to
"see" running code inside the Daytona MicroVM. It is the AI's eyes.

Responsibilities
----------------
1. ``ensure_browser_installed(sandbox_id)`` -- idempotent installer that
   runs ``pip install playwright`` + ``playwright install chromium`` inside
   the VM the FIRST time a sandbox is audited. Cached per-sandbox in a
   process-level set so subsequent audits skip the ~60s download.

2. ``execute_audit(sandbox_id, frontend_url, backend_url, validation_blueprint)``
   -- writes a small Python audit script to ``/workspace/.browser-audit.py``
   inside the VM, then runs it via ``sandbox.process.exec``. The script
   uses the Playwright async API to:
     * Launch headless Chromium with --no-sandbox --disable-gpu
     * Open the local forwarded preview URL (http://localhost:5173 by default)
     * Capture ``console.error`` + ``pageerror`` events
     * Capture the rendered DOM (truncated to 50 000 chars)
     * Capture a full-page PNG screenshot (base64)
     * Print one JSON line to stdout that the host parses

Both methods are BEST-EFFORT and NEVER raise -- on any failure they
return a result dict with ``status="failed"`` and the error message.
This contract is what makes Module 4's auto-correction loop safe:
the audit always returns SOMETHING, even on catastrophe, and the loop's
heuristic evaluator can decide pass / fail / replan from there.

All blocking Daytona SDK calls (``daytona.get``, ``sandbox.process.exec``,
``sandbox.fs.upload_file``) are wrapped in ``asyncio.to_thread`` to match
the existing pattern in ``workspace_coordinator.py`` -- the FastAPI
event loop never blocks on a VM round-trip.

The Playwright package is installed INSIDE the VM (NOT on the host
service). The host service has zero new pip dependencies.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from app.config import settings
from app.daytona_client import get_daytona

logger = logging.getLogger(__name__)


WORKSPACE_ROOT = "/workspace"

# Per-sandbox "install completed" cache. Avoids re-running the
# ~60s Chromium download on every audit. Entries are added ONLY after a
# verified successful install (browser binary present on disk).
_INSTALL_CACHE: set[str] = set()


# =============================================================================
# AUDIT_SCRIPT_SOURCE -- the Python source that runs INSIDE the VM.
# =============================================================================
# IMPORTANT: this is an r'''...''' raw triple-single-quoted string. The inner
# Python source MUST NOT contain a triple-single-quote sequence (it would
# terminate the outer raw string). Use double quotes everywhere inside.
# It is verified via ast.parse at the bottom of this file (see _self_check).
# Only stdlib + playwright are used. No other deps.

AUDIT_SCRIPT_SOURCE = r'''#!/usr/bin/env python3
"""ArcForge in-VM browser audit -- runs Playwright inside the Daytona sandbox.

Inputs (via sys.argv):
  sys.argv[1] = frontend_url  (e.g. http://localhost:5173)
  sys.argv[2] = backend_url   (optional, may be "-" sentinel)

Output: a single JSON line on stdout with one of two shapes:

  success:
    {"status": "success", "title": "...", "url": "...",
     "http_status": 200, "error_logs": [...], "console_errors": [...],
     "dom_snapshot": "<html>...</html>", "screenshot_b64": "..."}

  failure:
    {"status": "failed", "error": "...", "error_logs": [...],
     "traceback": "..."}

The script is best-effort: any exception is caught and printed as a
"failed" JSON envelope so the host orchestrator can parse a result
regardless of the failure mode.
"""
import asyncio
import base64
import json
import sys
import traceback


async def main():
    if len(sys.argv) < 2:
        print(json.dumps({"status": "failed", "error": "missing frontend_url arg"}))
        return
    frontend_url = sys.argv[1]
    # backend_url is informational only (the page itself will call it via XHR).
    backend_url = sys.argv[2] if len(sys.argv) > 2 and sys.argv[2] != "-" else None

    errors = []

    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        print(json.dumps({
            "status": "failed",
            "error": "playwright not installed: " + str(exc),
            "error_logs": errors,
            "traceback": traceback.format_exc(),
        }))
        return

    async with async_playwright() as p:
        browser = None
        try:
            browser = await p.chromium.launch(
                headless=True,
                args=[
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--no-first-run",
                ],
            )
            page = await browser.new_page(viewport={"width": 1280, "height": 720})

            # Capture console.error and pageerror into the same list so the
            # host sees ONE merged stream of runtime issues.
            def _on_console(msg):
                try:
                    if msg.type == "error":
                        errors.append("console:" + str(msg.text))
                except Exception:
                    pass

            def _on_pageerror(exc):
                try:
                    errors.append("pageerror:" + str(exc))
                except Exception:
                    pass

            page.on("console", _on_console)
            page.on("pageerror", _on_pageerror)

            # Navigate. wait_until=domcontentloaded is faster than
            # networkidle and is sufficient for catching obvious runtime
            # errors (failed XHRs surface as console errors anyway).
            resp = await page.goto(
                frontend_url, wait_until="domcontentloaded", timeout=15000,
            )
            # Give SPA routers + lazy chunks a moment to settle.
            await asyncio.sleep(1.0)

            title = await page.title()
            html = await page.content()
            status_code = resp.status if resp is not None else None

            # Full-page screenshot. PNG is large but lossless -- the AI
            # needs pixel-perfect rendering for visual verification.
            screenshot_bytes = await page.screenshot(full_page=True)
            shot_b64 = base64.b64encode(screenshot_bytes).decode("ascii")

            # Truncate DOM to keep the JSON response sane (the host already
            # allows 10mb bodies, but 50k chars is plenty for the LLM to
            # reason about structure without blowing the context window).
            dom_snapshot = html[:50000]
            if len(html) > 50000:
                dom_snapshot = dom_snapshot + "...[truncated]"

            print(json.dumps({
                "status": "success",
                "title": title,
                "url": frontend_url,
                "backend_url": backend_url,
                "http_status": status_code,
                "error_logs": errors,
                "console_errors": errors,
                "dom_snapshot": dom_snapshot,
                "screenshot_b64": shot_b64,
            }))
        except Exception as exc:
            # Navigation timeout, network error, browser crash, etc.
            print(json.dumps({
                "status": "failed",
                "error": str(exc),
                "error_logs": errors,
                "traceback": traceback.format_exc(),
            }))
        finally:
            if browser is not None:
                try:
                    await browser.close()
                except Exception:
                    pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as exc:
        # Top-level safety net so the host always gets JSON, never a stack
        # trace on stderr that it would have to parse heuristically.
        print(json.dumps({
            "status": "failed",
            "error": "top-level: " + str(exc),
            "error_logs": [],
            "traceback": traceback.format_exc(),
        }))
'''


# =============================================================================
# BrowserEngineManager
# =============================================================================


class BrowserEngineManager:
    """Manage Playwright installs and audits inside Daytona MicroVMs.

    The class is intentionally stateless beyond the module-level install
    cache -- it's safe to use the singleton (or a fresh instance) from any
    async context. The Daytona SDK client is obtained lazily on each call
    via ``get_daytona()`` (matches the pattern in workspace_coordinator.py).
    """

    # ------------------------------------------------------------------
    # 1. Idempotent Chromium installer
    # ------------------------------------------------------------------

    async def ensure_browser_installed(self, sandbox_id: str) -> dict[str, Any]:
        """Ensure Playwright + Chromium are installed in the sandbox VM.

        Idempotent: skips the install entirely if the sandbox is already
        in the in-process ``_INSTALL_CACHE`` set, or if a quick filesystem
        probe finds the chromium binary on disk.

        Returns a dict with shape:
            {"installed": bool, "browser_path": str|None,
             "install_log": str, "duration_ms": int}

        NEVER raises -- any failure is reflected as ``installed=False`` with
        the install_log populated so callers (and the operator reading
        daytona-service logs) can diagnose.
        """
        t0 = time.monotonic()

        # 1a. Hot path: this sandbox already audited once in this process.
        if sandbox_id in _INSTALL_CACHE:
            return {
                "installed": True,
                "browser_path": None,  # not re-probed for speed
                "install_log": "cached (already installed in this process)",
                "duration_ms": 0,
            }

        sandbox = None
        try:
            daytona = get_daytona()
            sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "ensure_browser_installed: failed to resolve sandbox %s: %s",
                sandbox_id, exc,
            )
            return {
                "installed": False,
                "browser_path": None,
                "install_log": f"sandbox resolve failed: {exc}",
                "duration_ms": elapsed,
            }

        # 1b. Filesystem probe -- is chromium already on disk?
        #     ~/.cache/ms-playwright/chromium-*/chrome-linux/chrome
        #     is where `playwright install chromium` lands the binary.
        probe_cmd = (
            "ls -1 ~/.cache/ms-playwright/chromium-*/chrome-linux/chrome "
            "2>/dev/null | head -n1"
        )
        try:
            probe_result = await asyncio.to_thread(
                sandbox.process.exec, probe_cmd, "/home/daytona", None, 10,
            )
            probe_out = (getattr(probe_result, "result", "") or "").strip()
            probe_exit = getattr(probe_result, "exit_code", None)
            probe_exit = -1 if probe_exit is None else int(probe_exit)
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "ensure_browser_installed: probe failed in %s: %s",
                sandbox_id, exc,
            )
            return {
                "installed": False,
                "browser_path": None,
                "install_log": f"probe failed: {exc}",
                "duration_ms": elapsed,
            }

        if probe_exit == 0 and probe_out and "chrome" in probe_out:
            # Chromium binary is already on disk. Cache + return.
            _INSTALL_CACHE.add(sandbox_id)
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.info(
                "Chromium already installed in sandbox %s at %s (%d ms)",
                sandbox_id, probe_out, elapsed,
            )
            return {
                "installed": True,
                "browser_path": probe_out,
                "install_log": "pre-installed (filesystem probe hit)",
                "duration_ms": elapsed,
            }

        # 1c. Cold path: install pip + playwright + chromium + deps.
        logger.info(
            "Installing Playwright + Chromium in sandbox %s (first audit -- "
            "this will take ~60-90s)...",
            sandbox_id,
        )

        install_log_parts: list[str] = []
        install_timeout = max(60, settings.browser_install_timeout_s)

        # Step 1: pip install playwright (quiet to keep the log sane).
        step1_cmd = "pip3 install --quiet playwright 2>&1"
        try:
            r1 = await asyncio.to_thread(
                sandbox.process.exec, step1_cmd, "/home/daytona", None,
                install_timeout,
            )
            out1 = (getattr(r1, "result", "") or "")[-2000:]
            exit1 = getattr(r1, "exit_code", None)
            exit1 = -1 if exit1 is None else int(exit1)
            install_log_parts.append(
                f"[pip install playwright] exit={exit1}\n{out1}"
            )
        except Exception as exc:
            install_log_parts.append(f"[pip install playwright] EXCEPTION: {exc}")
            elapsed = int((time.monotonic() - t0) * 1000)
            return {
                "installed": False,
                "browser_path": None,
                "install_log": "\n".join(install_log_parts),
                "duration_ms": elapsed,
            }

        # Step 2: playwright install chromium (~150MB download).
        step2_cmd = "python3 -m playwright install chromium 2>&1"
        try:
            r2 = await asyncio.to_thread(
                sandbox.process.exec, step2_cmd, "/home/daytona", None,
                install_timeout,
            )
            out2 = (getattr(r2, "result", "") or "")[-3000:]
            exit2 = getattr(r2, "exit_code", None)
            exit2 = -1 if exit2 is None else int(exit2)
            install_log_parts.append(
                f"[playwright install chromium] exit={exit2}\n{out2}"
            )
        except Exception as exc:
            install_log_parts.append(
                f"[playwright install chromium] EXCEPTION: {exc}"
            )
            elapsed = int((time.monotonic() - t0) * 1000)
            return {
                "installed": False,
                "browser_path": None,
                "install_log": "\n".join(install_log_parts),
                "duration_ms": elapsed,
            }

        # Step 3: install-deps (best-effort -- requires root, may fail
        # silently on the python snapshot if libs are already present).
        step3_cmd = "sudo python3 -m playwright install-deps chromium 2>&1"
        try:
            r3 = await asyncio.to_thread(
                sandbox.process.exec, step3_cmd, "/home/daytona", None,
                install_timeout,
            )
            out3 = (getattr(r3, "result", "") or "")[-2000:]
            exit3 = getattr(r3, "exit_code", None)
            exit3 = -1 if exit3 is None else int(exit3)
            install_log_parts.append(
                f"[playwright install-deps] exit={exit3} (best-effort)\n{out3}"
            )
        except Exception as exc:
            # Non-fatal: install-deps often fails on minimal containers but
            # the python snapshot usually has the libs already.
            install_log_parts.append(
                f"[playwright install-deps] EXCEPTION (best-effort, ignored): {exc}"
            )

        # 1d. Re-probe to confirm the binary actually landed on disk.
        try:
            probe2 = await asyncio.to_thread(
                sandbox.process.exec, probe_cmd, "/home/daytona", None, 10,
            )
            probe2_out = (getattr(probe2, "result", "") or "").strip()
            probe2_exit = getattr(probe2, "exit_code", None)
            probe2_exit = -1 if probe2_exit is None else int(probe2_exit)
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "ensure_browser_installed: re-probe failed in %s: %s",
                sandbox_id, exc,
            )
            return {
                "installed": False,
                "browser_path": None,
                "install_log": "\n".join(install_log_parts)
                                + f"\n[re-probe] EXCEPTION: {exc}",
                "duration_ms": elapsed,
            }

        elapsed = int((time.monotonic() - t0) * 1000)
        if probe2_exit == 0 and probe2_out and "chrome" in probe2_out:
            _INSTALL_CACHE.add(sandbox_id)
            logger.info(
                "Chromium installed OK in sandbox %s at %s (%d ms, log=%d chars)",
                sandbox_id, probe2_out, elapsed,
                len("\n".join(install_log_parts)),
            )
            return {
                "installed": True,
                "browser_path": probe2_out,
                "install_log": "\n".join(install_log_parts),
                "duration_ms": elapsed,
            }

        # Install ran but the binary is not where we expect. Surface the
        # full log so the operator can diagnose.
        logger.error(
            "Chromium install completed but binary not found in sandbox %s "
            "(re-probe exit=%s out=%r)",
            sandbox_id, probe2_exit, probe2_out,
        )
        return {
            "installed": False,
            "browser_path": None,
            "install_log": "\n".join(install_log_parts)
                            + f"\n[re-probe] exit={probe2_exit} out={probe2_out}",
            "duration_ms": elapsed,
        }

    # ------------------------------------------------------------------
    # 2. Audit execution -- the AI's "eyes"
    # ------------------------------------------------------------------

    async def execute_audit(
        self,
        sandbox_id: str,
        frontend_url: str,
        backend_url: str | None = None,
        validation_blueprint: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Run a Playwright audit against the live preview URL in the VM.

        Flow:
          1. Auto-install Playwright + Chromium (idempotent, ~60s first time).
          2. Write ``AUDIT_SCRIPT_SOURCE`` to ``/workspace/.browser-audit.py``.
          3. ``sandbox.process.exec("python3 /workspace/.browser-audit.py "
             "<frontend_url> <backend_url|->", "/workspace", None,
             browser_audit_timeout_s)``.
          4. Parse the LAST non-empty line of stdout as JSON (the script
             prints exactly one JSON envelope).
          5. Return the parsed dict. On ANY failure (install, write, exec,
             parse, timeout), return a ``status="failed"`` envelope with the
             error message -- NEVER raises.

        The ``validation_blueprint`` argument is currently informational;
        the in-VM script is intentionally generic (navigate, capture
        console errors + DOM + screenshot). Blueprint-aware assertions
        live in the host backend's heuristic evaluator (Module 4's
        ``evaluateAgainstBlueprint``).
        """
        t0 = time.monotonic()
        audit_timeout = max(30, settings.browser_audit_timeout_s)
        script_path = settings.browser_audit_script_path

        # 2a. Pre-flight: sandbox resolvable?
        try:
            daytona = get_daytona()
            sandbox = await asyncio.to_thread(daytona.get, sandbox_id)
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "execute_audit: sandbox %s resolve failed: %s",
                sandbox_id, exc,
            )
            return self._failed(
                f"sandbox resolve failed: {exc}", elapsed,
            )

        # 2b. Lazy install (best-effort -- on failure, we still attempt the
        #     audit so the script can return its own "playwright not
        #     installed" envelope).
        try:
            install_result = await self.ensure_browser_installed(sandbox_id)
        except Exception as exc:
            # ensure_browser_installed is contract-bound to never raise,
            # but defend against bugs in the contract.
            install_result = {
                "installed": False,
                "install_log": f"ensure_browser_installed raised: {exc}",
                "duration_ms": 0,
            }
            logger.exception(
                "ensure_browser_installed raised unexpectedly for %s",
                sandbox_id,
            )

        if not install_result.get("installed"):
            elapsed = int((time.monotonic() - t0) * 1000)
            return self._failed(
                "Playwright/Chromium not installed in VM: "
                + (install_result.get("install_log") or "")[:500],
                elapsed,
            )

        # 2c. Write the audit script into the VM.
        try:
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                AUDIT_SCRIPT_SOURCE.encode("utf-8"), script_path,
            )
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "execute_audit: failed to upload audit script to %s in %s: %s",
                script_path, sandbox_id, exc,
            )
            return self._failed(
                f"failed to upload audit script: {exc}", elapsed,
            )

        # 2d. Run the audit. backend_url may be None -- pass "-" sentinel
        #     so argv[2] always exists (the script handles it).
        backend_arg = backend_url if backend_url else "-"
        # shlex.quote the URLs in case they contain shell metachars.
        import shlex
        run_cmd = (
            f"python3 {shlex.quote(script_path)} "
            f"{shlex.quote(frontend_url)} {shlex.quote(backend_arg)}"
        )
        logger.info(
            "execute_audit: running browser audit in sandbox %s "
            "(frontend=%s, backend=%s, timeout=%ds)",
            sandbox_id, frontend_url, backend_arg, audit_timeout,
        )

        try:
            result = await asyncio.to_thread(
                sandbox.process.exec, run_cmd, WORKSPACE_ROOT, None,
                audit_timeout,
            )
        except Exception as exc:
            elapsed = int((time.monotonic() - t0) * 1000)
            logger.error(
                "execute_audit: process.exec failed in %s: %s",
                sandbox_id, exc,
            )
            return self._failed(
                f"process.exec failed: {exc}", elapsed,
            )

        elapsed = int((time.monotonic() - t0) * 1000)
        raw_stdout = (getattr(result, "result", "") or "")
        raw_exit = getattr(result, "exit_code", None)
        exit_code = -1 if raw_exit is None else int(raw_exit)

        # 2e. Parse the last non-empty line of stdout as JSON (the script
        #     prints exactly one JSON envelope; allow for stray stderr
        #     lines that got merged into the captured output).
        parsed: dict[str, Any] | None = None
        last_json_line: str = ""
        for line in reversed(raw_stdout.split("\n")):
            line = line.strip()
            if not line:
                continue
            if line.startswith("{") and line.endswith("}"):
                last_json_line = line
                break

        if last_json_line:
            try:
                parsed = json.loads(last_json_line)
            except Exception as exc:
                logger.warning(
                    "execute_audit: JSON parse failed in %s (err=%s, line=%s)",
                    sandbox_id, exc, last_json_line[:200],
                )

        if parsed is not None and isinstance(parsed, dict):
            # Inject duration_ms so callers know the total wall time.
            parsed["duration_ms"] = elapsed
            # Normalize keys so the response always has the full set
            # even if the script produced a partial envelope.
            parsed.setdefault("status", "success" if exit_code == 0 else "failed")
            parsed.setdefault("title", None)
            parsed.setdefault("url", frontend_url)
            parsed.setdefault("backend_url", backend_url)
            parsed.setdefault("http_status", None)
            parsed.setdefault("error_logs", [])
            parsed.setdefault("console_errors", [])
            parsed.setdefault("dom_snapshot", None)
            parsed.setdefault("screenshot_b64", None)
            parsed.setdefault("error", None)
            if exit_code != 0 and parsed.get("status") == "success":
                # Defensive: if the script crashed AFTER printing success
                # (very unlikely), downgrade. In practice the script's own
                # finally / except handles every path.
                logger.warning(
                    "execute_audit: script exit=%d but status=success in %s "
                    "-- trusting script's own status",
                    exit_code, sandbox_id,
                )
            logger.info(
                "execute_audit: %s sandbox=%s status=%s http=%s errors=%d "
                "dom=%d chars shot=%d chars (exit=%d, %d ms)",
                "OK" if parsed.get("status") == "success" else "FAIL",
                sandbox_id,
                parsed.get("status"),
                parsed.get("http_status"),
                len(parsed.get("error_logs") or []),
                len(parsed.get("dom_snapshot") or ""),
                len(parsed.get("screenshot_b64") or ""),
                exit_code, elapsed,
            )
            return parsed

        # 2f. No parseable JSON envelope -- synthesize a failed result with
        #     the raw stdout/stderr so the operator can see what happened.
        logger.error(
            "execute_audit: no JSON envelope in stdout (sandbox=%s, exit=%d, "
            "stdout_tail=%r)",
            sandbox_id, exit_code, raw_stdout[-500:],
        )
        return self._failed(
            f"audit script produced no JSON (exit={exit_code}, "
            f"stdout_tail={raw_stdout[-300:]!r})",
            elapsed,
            error_logs=[],
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _failed(
        error: str, elapsed_ms: int,
        error_logs: list[str] | None = None,
    ) -> dict[str, Any]:
        """Construct a uniform 'failed' audit result envelope."""
        return {
            "status": "failed",
            "title": None,
            "url": None,
            "backend_url": None,
            "http_status": None,
            "error_logs": error_logs if error_logs is not None else [],
            "console_errors": error_logs if error_logs is not None else [],
            "dom_snapshot": None,
            "screenshot_b64": None,
            "duration_ms": elapsed_ms,
            "error": error,
        }


# =============================================================================
# Self-check (run at import time -- fails fast if AUDIT_SCRIPT_SOURCE is
# not valid Python, which would break every audit).
# =============================================================================

def _self_check() -> None:
    """Verify AUDIT_SCRIPT_SOURCE parses as valid Python.

    A failure here is a programmer error (the embedded audit script is
    malformed) and would break EVERY audit. Fail-fast at import time is
    much better than failing per-audit at runtime.
    """
    import ast
    try:
        ast.parse(AUDIT_SCRIPT_SOURCE)
    except SyntaxError as exc:
        # Re-raise with context so the developer sees the embedded line
        # number that broke.
        raise RuntimeError(
            f"AUDIT_SCRIPT_SOURCE is not valid Python: {exc} "
            f"(line {exc.lineno}, offset {exc.offset})"
        ) from exc


_self_check()


# =============================================================================
# Singleton
# =============================================================================

browser_engine = BrowserEngineManager()
