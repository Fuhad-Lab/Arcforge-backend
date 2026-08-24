"""Agent sidecar installer — plants the In-VM Orchestrator into a sandbox.

Implements the "In-VM Sidecar" architecture:

  1. Generate a per-VM shared secret (AGENT_TOKEN).
  2. Write orchestrator.py + ecosystem.config.js + watchdog.sh into the
     DISK-backed /home/daytona/.system/ directory (survives VM stop/start)
     and symlink /workspace/.system -> it (so the canonical path works even
     though /workspace may be a tmpfs RAM disk).
  3. pip install fastapi + uvicorn (best-effort, retried).
  4. Launch under PM2 ("agent-brain") when available — otherwise under the
     shell watchdog, which mirrors PM2's autorestart semantics.
  5. Verify the daemon answers /health on port 9000.
  6. Open the Daytona preview link for port 9000 so the browser can reach
     the daemon's WebSocket directly.

The token is persisted at /home/daytona/.system/agent_token (mode 600) so
the host can re-broker credentials to the VM's owner on demand
(``get_agent_info``) without storing secrets in any database.

Every step is best-effort: a failed install NEVER fails workspace creation —
the platform falls back to the host-side SSE pipeline for that project.
"""

from __future__ import annotations

import asyncio
import logging
import secrets
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

# Physical, disk-backed home INSIDE the VM. /workspace may be tmpfs.
SIDE_CAR_HOME = "/home/daytona/.system"
VM_CANONICAL_PATH = "/workspace/.system"      # symlink -> SIDE_CAR_HOME
ORCHESTRATOR_PORT = 9000

# Where the sidecar source templates live on THIS host (the Render service).
_SIDECAR_SRC_DIR = Path(__file__).resolve().parent.parent / "agent_sidecar"

# pip deps the orchestrator needs beyond the stdlib.
_PIP_DEPS = ("fastapi", "uvicorn[standard]")


class AgentInstaller:
    """Install / inspect / broker the in-VM orchestrator daemon."""

    # ------------------------------------------------------------------
    # Install
    # ------------------------------------------------------------------

    async def install(
        self,
        sandbox: Any,
        llm_config: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        """Install + launch the orchestrator inside `sandbox`.

        Returns {"installed", "port", "token", "launcher", "preview_url"}.
        Never raises — failures degrade to {"installed": False, ...}.
        """
        t0 = time.monotonic()
        token = secrets.token_urlsafe(32)
        llm = {
            "url": (llm_config or {}).get("url", ""),
            "key": (llm_config or {}).get("key", ""),
            "model": (llm_config or {}).get("model", "glm-5.2"),
        }
        result: dict[str, Any] = {
            "installed": False,
            "port": ORCHESTRATOR_PORT,
            "token": token,
            "launcher": None,
            "preview_url": None,
        }

        async def arun(cmd: str, timeout: float = 60.0) -> str:
            res = await asyncio.to_thread(
                sandbox.process.exec, cmd, "/home/daytona", None, int(timeout),
            )
            return (getattr(res, "result", "") or "") + (getattr(res, "stderr", "") or "")

        try:
            # 1) Read the sidecar templates from this host.
            orchestrator_src = (_SIDECAR_SRC_DIR / "orchestrator.py").read_text("utf-8")
            ecosystem_src = self._render_ecosystem(token, llm)
            watchdog_src = (_SIDECAR_SRC_DIR / "watchdog.sh").read_text("utf-8")

            # 2) Write the daemon + supervisor configs into the VM.
            #    /home/daytona is the disk-backed user home — state.db,
            #    logs and the daemon itself survive VM stop/start.
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                orchestrator_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/orchestrator.py",
            )
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                ecosystem_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/ecosystem.config.js",
            )
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                watchdog_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/watchdog.sh",
            )
            # Token file (mode 600) — the host re-reads this to broker
            # credentials to the VM's owner; it is never stored elsewhere.
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                token.encode("utf-8"),
                f"{SIDE_CAR_HOME}/agent_token",
            )

            # 3) Prepare dirs, symlink the canonical path, persist the LLM
            #    config as the daemon's env file (mode 600).
            env_lines = "\n".join([
                f"ORCH_PORT={ORCHESTRATOR_PORT}",
                f"ORCH_TOKEN={token}",
                "ORCH_WORKSPACE=/workspace",
                f"ORCH_SYSTEM_DIR={SIDE_CAR_HOME}",
                f"ORCH_LLM_URL={llm['url']}",
                f"ORCH_LLM_KEY={llm['key']}",
                f"ORCH_LLM_MODEL={llm['model']}",
            ])
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                env_lines.encode("utf-8"),
                f"{SIDE_CAR_HOME}/orchestrator.env",
            )
            prep = (
                f"mkdir -p {SIDE_CAR_HOME} && "
                f"chown -R daytona:daytona {SIDE_CAR_HOME} 2>/dev/null; "
                # Canonical path: /workspace/.system -> disk-backed home
                f"sudo ln -sfn {SIDE_CAR_HOME} {VM_CANONICAL_PATH} 2>/dev/null "
                f"|| ln -sfn {SIDE_CAR_HOME} {VM_CANONICAL_PATH} 2>/dev/null; "
                f"chmod 600 {SIDE_CAR_HOME}/agent_token {SIDE_CAR_HOME}/orchestrator.env; "
                f"chmod +x {SIDE_CAR_HOME}/orchestrator.py {SIDE_CAR_HOME}/watchdog.sh; "
                f"echo PREP_OK"
            )
            prep_out = await arun(prep, 20)
            if "PREP_OK" not in prep_out:
                logger.warning("sidecar prep incomplete in %s: %s", sandbox.id, prep_out[-200:])

            # 4) Python deps (best-effort, retried — VMs have internet).
            pip_out = await arun(
                "pip install --quiet --disable-pip-version-check "
                + " ".join(f"'{d}'" for d in _PIP_DEPS)
                + " 2>&1 | tail -n 3; "
                + "python3 -c 'import fastapi, uvicorn' 2>/dev/null "
                + "&& echo PIP_OK || echo PIP_FAIL",
                timeout=180,
            )
            pip_ok = "PIP_OK" in pip_out
            if not pip_ok:
                logger.warning(
                    "sidecar pip install failed in %s: %s", sandbox.id, pip_out[-300:],
                )
                return result  # installed=False — platform falls back to SSE

            # 5) Launch: PM2 preferred, watchdog fallback. Both source the
            #    env file so the token/LLM config never appears in `ps`.
            launch = (
                # idempotent: stop any previous supervisor + daemon
                "(pm2 delete agent-brain 2>/dev/null || true); "
                "pkill -f orchestrator.py 2>/dev/null; "
                "pkill -f watchdog.sh 2>/dev/null; "
                "sleep 0.5; "
                # PM2 (requires node — the python snapshot ships it)
                "if command -v npm >/dev/null 2>&1; then "
                "npm install -g pm2 >/dev/null 2>&1 || true; fi; "
                "if command -v pm2 >/dev/null 2>&1; then "
                "set -a; . /home/daytona/.system/orchestrator.env; set +a; "
                "pm2 start /home/daytona/.system/ecosystem.config.js "
                "--update-env >/dev/null 2>&1 && pm2 save >/dev/null 2>&1; "
                "echo LAUNCHER=pm2; "
                "else "
                "set -a; . /home/daytona/.system/orchestrator.env; set +a; "
                "nohup bash /home/daytona/.system/watchdog.sh "
                "> /home/daytona/.system/watchdog.log 2>&1 < /dev/null & "
                "echo LAUNCHER=watchdog; "
                "fi"
            )
            launch_out = await arun(launch, 120)
            if "LAUNCHER=pm2" in launch_out:
                result["launcher"] = "pm2"
            elif "LAUNCHER=watchdog" in launch_out:
                result["launcher"] = "watchdog"
            else:
                logger.warning("sidecar launch ambiguous in %s: %s",
                               sandbox.id, launch_out[-300:])
                return result

            # 6) Verify the daemon answers /health on the sidecar port.
            verify = await arun(
                "sleep 3; for i in 1 2 3 4 5; do "
                f"code=$(curl -s -o /dev/null -w '%{{http_code}}' "
                f"http://localhost:{ORCHESTRATOR_PORT}/health --max-time 3); "
                "[ \"$code\" = \"200\" ] && break; sleep 2; done; "
                "echo VERIFY_$code",
                timeout=45,
            )
            if "VERIFY_200" not in verify:
                # Surface the daemon log tail for debugging.
                tail = await arun(
                    f"tail -n 20 {SIDE_CAR_HOME}/pm2-err.log "
                    f"{SIDE_CAR_HOME}/orchestrator.log 2>/dev/null", 15,
                )
                logger.warning(
                    "sidecar health check failed in %s (verify=%s, tail=%s)",
                    sandbox.id, verify.strip()[-80:], tail[-400:],
                )
                return result

            # 7) Open the Daytona preview link for the sidecar port so the
            #    browser can connect its WebSocket directly.
            preview_url = await self._preview_link(sandbox)
            result["preview_url"] = preview_url
            result["installed"] = True
            logger.info(
                "agent sidecar installed in %s via %s in %.1fs (preview=%s)",
                sandbox.id, result["launcher"], time.monotonic() - t0,
                bool(preview_url),
            )
            return result

        except Exception as exc:
            logger.warning("agent sidecar install failed in %s: %s",
                           getattr(sandbox, "id", "?"), exc)
            return result

    # ------------------------------------------------------------------
    # Introspection / credential brokering
    # ------------------------------------------------------------------

    async def get_agent_info(self, sandbox: Any) -> dict[str, Any]:
        """Read the sidecar's token + health and open its preview URL.

        Called by the Node backend (JWT + ownership-checked) so the studio
        frontend can connect its WebSocket. The token NEVER transits any
        database — it lives only inside the VM and this broker response.
        """
        info: dict[str, Any] = {
            "installed": False,
            "port": ORCHESTRATOR_PORT,
            "url": None,
            "token": None,
            "alive": False,
        }
        try:
            out = await asyncio.to_thread(
                sandbox.process.exec,
                f"cat {SIDE_CAR_HOME}/agent_token 2>/dev/null; echo ---; "
                f"curl -s -o /dev/null -w '%{{http_code}}' "
                f"http://localhost:{ORCHESTRATOR_PORT}/health --max-time 3",
                "/home/daytona", None, 15,
            )
            text = (getattr(out, "result", "") or "")
            if "---" in text:
                token_part, _, health_part = text.partition("---")
                token = token_part.strip()
                alive = health_part.strip().endswith("200")
                if token:
                    info.update({
                        "installed": True,
                        "token": token,
                        "alive": alive,
                        "url": await self._preview_link(sandbox) if alive else None,
                    })
        except Exception as exc:
            logger.warning("agent-info probe failed for %s: %s",
                           getattr(sandbox, "id", "?"), exc)
        return info

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _preview_link(self, sandbox: Any) -> str | None:
        """Daytona preview URL for the sidecar port (opens the port if
        closed). Returns the public https URL the browser connects to
        (wss:// for WebSocket, https:// for the REST fallback)."""
        try:
            link = await asyncio.to_thread(
                sandbox.get_preview_link, ORCHESTRATOR_PORT,
            )
            url = getattr(link, "url", None)
            return str(url) if url else None
        except Exception as exc:
            logger.warning("preview link unavailable for %s: %s",
                           getattr(sandbox, "id", "?"), exc)
            return None

    def _render_ecosystem(self, token: str, llm: dict[str, str]) -> str:
        """Render ecosystem.config.js with the env baked in (the install
        command also exports the env file before `pm2 start`, so both
        paths carry identical config)."""
        template = (_SIDECAR_SRC_DIR / "ecosystem.config.js").read_text("utf-8")
        env_block = (
            "      env: {\n"
            '        NODE_ENV: "production",\n'
            f'        ORCH_PORT: "{ORCHESTRATOR_PORT}",\n'
            f'        ORCH_TOKEN: "{token}",\n'
            '        ORCH_WORKSPACE: "/workspace",\n'
            f'        ORCH_SYSTEM_DIR: "{SIDE_CAR_HOME}",\n'
            f'        ORCH_LLM_URL: "{llm["url"]}",\n'
            f'        ORCH_LLM_KEY: "{llm["key"]}",\n'
            f'        ORCH_LLM_MODEL: "{llm["model"]}",\n'
            "      },"
        )
        # Replace the placeholder env block (the template's env: { ... })
        import re
        rendered = re.sub(r"      env: \{[\s\S]*?\},", env_block, template, count=1)
        return rendered


agent_installer = AgentInstaller()
