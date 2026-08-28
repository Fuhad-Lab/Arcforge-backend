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
import json
import logging
import os
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

# pip deps the sidecar daemon needs beyond the stdlib. The orchestrator
# uses fastapi+uvicorn (its HTTP+WS server). curl_cffi is kept so the
# tunnel_client.py (still deployed for backward compat in non-REVERSE
# mode — see ORCH_LLM_URL) has its WS transport available. aiohttp is
# likewise kept for the same reason. y-py powers the v6 Yjs CRDT bridge
# (yjs_bridge.py — multiplayer file sync); the bridge degrades to a
# no-op without it, but shipping it blocking keeps /yjs live from boot.
#
# In REVERSE mode (the new default — ORCH_LLM_URL=reverse-tunnel://),
# the orchestrator handles the LLM bridge ITSELF over its own
# /reverse-tunnel WS endpoint, so the tunnel_client daemon is no longer
# started (see ecosystem.config.js — only agent-brain is launched).
_PIP_DEPS = ("fastapi", "uvicorn[standard]", "aiohttp", "curl_cffi", "y-py")

# Ports the generated app's dev server may listen on, in probe-priority
# order. 3000 = the mandated Next.js frontend (`next dev -p 3000`);
# 5173 = legacy Vite apps from older generations. The orchestrator's
# debugger phase launches the right one for the framework it detects.
APP_PORTS = (3000, 5173)


class AgentInstaller:
    """Install / inspect / broker the in-VM orchestrator daemon."""

    # ------------------------------------------------------------------
    # Install
    # ------------------------------------------------------------------

    async def install(
        self,
        sandbox: Any,
        llm_config: dict[str, str] | None = None,
        skills: list[dict[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Install + launch the orchestrator inside `sandbox`.

        `skills` — the platform's 17-skill catalog ({name, instruction}),
        written verbatim to {SIDE_CAR_HOME}/skills.json. The orchestrator
        loads it at boot and injects the skills into its Architect/
        Developer prompts (the in-VM twin of the host pipeline's
        god-mode-protocol skill injection).

        Returns {"installed", "port", "token", "launcher", "preview_url"}.
        Never raises — failures degrade to {"installed": False, ...}.
        """
        t0 = time.monotonic()
        token = secrets.token_urlsafe(32)
        # ROLE-ROUTED MODELS (2026-09-26 NVIDIA routing — live head-to-head,
        # see orchestrator.py's routing note + worklog Task 28). The chief
        # primary rides in from the backend (llm_config.model — Render's
        # SINGLE_MODE_MODEL); the per-agent roles are deployment constants,
        # each overridable via this daytona-service's own env vars.
        llm = {
            "url": (llm_config or {}).get("url", ""),
            "key": (llm_config or {}).get("key", ""),
            "model": (llm_config or {}).get("model", "nvidia/nemotron-3-ultra-550b-a55b"),
            "chief_fallback_model": os.environ.get(
                "ORCH_CHIEF_FALLBACK_MODEL", "openai/gpt-oss-120b"),
            "agent_model": os.environ.get(
                "ORCH_AGENT_MODEL", "nvidia/nemotron-3-super-120b-a12b"),
            "frontend_model": os.environ.get(
                "ORCH_FRONTEND_MODEL", "minimaxai/minimax-m3"),
            "backend_model": os.environ.get(
                "ORCH_BACKEND_MODEL", "deepseek-ai/deepseek-v4-pro-0813"),
            "debugger_model": os.environ.get(
                "ORCH_DEBUGGER_MODEL", "nvidia/nemotron-3-super-120b-a12b"),
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
            tunnel_src = (_SIDECAR_SRC_DIR / "tunnel_client.py").read_text("utf-8")
            # v3 swarm sidecar modules:
            #   vm_browser.py     — Browser Vision Engine (Playwright bridge)
            #   skills_server.py  — MCP-style skills host (per-agent scopes)
            vm_browser_src = (_SIDECAR_SRC_DIR / "vm_browser.py").read_text("utf-8")
            skills_src = (_SIDECAR_SRC_DIR / "skills_server.py").read_text("utf-8")
            # v6 multiplayer/protocol modules:
            #   yjs_bridge.py  — Yjs CRDT file sync (mounted at /yjs)
            #   acp_server.py  — Agent Client Protocol (mounted at /acp)
            yjs_src = (_SIDECAR_SRC_DIR / "yjs_bridge.py").read_text("utf-8")
            acp_src = (_SIDECAR_SRC_DIR / "acp_server.py").read_text("utf-8")

            # --- Derive the WS tunnel config from the host env ------------
            # The VM's AI client points at localhost:7777; the tunnel_client
            # bridges that over a single WebSocket to the backend's /api/tunnel
            # endpoint. The backend injects the real NVIDIA key (never in the
            # VM) and forwards to NVIDIA (US region, unblocked by the EU
            # egress filter). Direct mode (VM→NVIDIA) is OFF — the VM has no
            # NVIDIA key by design.
            backend_url = os.environ.get("ARCFORGE_BACKEND_URL", "").rstrip("/")
            proxy_secret = os.environ.get("ARCFORGE_AGENT_PROXY_SECRET", "")
            # Convert https://host -> wss://host  (http:// -> ws://)
            if backend_url.startswith("https://"):
                tunnel_ws_url = "wss://" + backend_url[len("https://"):]
            elif backend_url.startswith("http://"):
                tunnel_ws_url = "ws://" + backend_url[len("http://"):]
            else:
                tunnel_ws_url = ""  # not configured — tunnel will error loudly

            ecosystem_src = self._render_ecosystem(
                token, llm, tunnel_ws_url, proxy_secret,
            )
            watchdog_src = (_SIDECAR_SRC_DIR / "watchdog.sh").read_text("utf-8")

            # 2) Write the daemons + supervisor configs into the VM.
            #    /home/daytona is the disk-backed user home — state.db,
            #    logs and the daemons themselves survive VM stop/start.
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                orchestrator_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/orchestrator.py",
            )
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                tunnel_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/tunnel_client.py",
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
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                vm_browser_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/vm_browser.py",
            )
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                skills_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/skills_server.py",
            )
            # v6 — the CRDT bridge + the ACP server ride along with the
            # daemon (mounted by orchestrator.py at /yjs and /acp on the
            # same port-9000 preview URL the studio already brokers).
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                yjs_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/yjs_bridge.py",
            )
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                acp_src.encode("utf-8"),
                f"{SIDE_CAR_HOME}/acp_server.py",
            )
            # Token file (mode 600) — the host re-reads this to broker
            # credentials to the VM's owner; it is never stored elsewhere.
            await asyncio.to_thread(
                sandbox.fs.upload_file,
                token.encode("utf-8"),
                f"{SIDE_CAR_HOME}/agent_token",
            )
            # Platform skills — planted as the MCP-style catalog consumed
            # by the in-VM skills server (per-agent scopes, segregation
            # enforced server-side). No secrets inside.
            if skills:
                await asyncio.to_thread(
                    sandbox.fs.upload_file,
                    json.dumps(skills, ensure_ascii=False).encode("utf-8"),
                    f"{SIDE_CAR_HOME}/skills.json",
                )

            # 3) Prepare dirs, symlink the canonical path, persist the
            #    tunnel + LLM config as the daemons' env file (mode 600).
            #
            #    LLM ROUTING — always via the IN-VM WS TUNNEL now. The
            #    VM's AI client points at localhost:7777 (tunnel_client);
            #    the tunnel bridges over a WS to the backend, which injects
            #    the real NVIDIA key and forwards to NVIDIA (US region).
            #    The VM holds NO NVIDIA key — direct mode is OFF.
            #
            #    We still probe whether the VM can reach the backend (HTTP
            #    healthz) as a readiness hint: if the backend is reachable,
            #    the WS tunnel will work once PM2 starts the tunnel_client.
            backend_reachable = False
            if backend_url:
                try:
                    probe_out = await arun(
                        f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 8 "
                        f"{backend_url}/api/healthz",
                        15,
                    )
                    pcode = probe_out.strip().splitlines()[-1].strip()
                    backend_reachable = (
                        pcode.isdigit() and pcode not in ("000", "")
                    )
                except Exception:
                    backend_reachable = False
            # Optional connectivity hint — does the VM's egress even see
            # NVIDIA? (EU VMs are blocked.) NEVER enables direct mode; just
            # logged for region diagnostics.
            nvidia_hint = "?"
            try:
                hint_out = await arun(
                    f"curl -s -o /dev/null -w '%{{http_code}}' --max-time 6 "
                    f"{llm['url']}",
                    12,
                )
                nvidia_hint = hint_out.strip().splitlines()[-1].strip()
            except Exception:
                nvidia_hint = "000"
            logger.info(
                "sidecar tunnel routing for %s: backend=%s(nvidia_hint=%s) "
                "ws_url=%s",
                sandbox.id, backend_reachable, nvidia_hint,
                tunnel_ws_url or "<unset>",
            )
            llm_ready = "1" if backend_reachable else "0"
            # In reverse-tunnel mode, LLM readiness doesn't depend on the
            # VM's egress — it depends on the BACKEND's ability to dial IN
            # via the signed daytonaproxy01.eu URL (always possible — that's
            # the same path the frontend uses for its /ws). So we always
            # report llm_ready=1 in reverse mode, even if the egress probe
            # to *.onrender.com fails (which it does in EU region).
            llm_ready_reverse = "1"
            env_lines = "\n".join([
                f"ORCH_PORT={ORCHESTRATOR_PORT}",
                f"ORCH_TOKEN={token}",
                "ORCH_WORKSPACE=/workspace",
                f"ORCH_SYSTEM_DIR={SIDE_CAR_HOME}",
                # The orchestrator's AI client uses the IN-PROCESS reverse
                # tunnel — it sends `req` frames over its own /reverse-tunnel
                # WS endpoint to the backend (which dialed IN via the signed
                # daytonaproxy01.eu URL). Bypasses the EU egress filter
                # entirely. The VM never holds the NVIDIA key — the backend
                # injects it server-side before forwarding to NVIDIA.
                "ORCH_LLM_URL=reverse-tunnel://",
                # Dummy key — kept for backward compat. In reverse-tunnel
                # mode the orchestrator never sends it (the backend injects
                # the real NVIDIA Bearer token server-side).
                "ORCH_LLM_KEY=tunnel-injected",
                f"ORCH_LLM_MODEL={llm['model']}",
                # ROLE-ROUTED MODEL CHAINS (see orchestrator.py): chief gets
                # a fallback (503/schema-drift), each agent role its primary
                # + the proven nemotron-3-super fallback. Sustained 429s or
                # 503s sticky-demote a primary for 10 min (ORCH_MODEL_DEMOTE_S).
                f"ORCH_CHIEF_FALLBACK_MODEL={llm['chief_fallback_model']}",
                f"ORCH_AGENT_MODEL={llm['agent_model']}",
                f"ORCH_FRONTEND_MODEL={llm['frontend_model']}",
                f"ORCH_BACKEND_MODEL={llm['backend_model']}",
                f"ORCH_DEBUGGER_MODEL={llm['debugger_model']}",
                # 1 = clients (frontend) route generation through the in-VM
                # agent. In reverse mode this is ALWAYS 1 because the path
                # doesn't depend on the VM's egress filter.
                f"ORCH_LLM_READY={llm_ready_reverse}",
                # --- Tunnel config (read by orchestrator.py for /reverse-tunnel
                # auth — the shared AGENT_PROXY_SECRET between VM and backend) ---
                f"TUNNEL_TOKEN={proxy_secret}",
                # --- Backend wake (Render-sleep recovery, 2026-08-28) ---------
                # The sidecar POSTs /api/tunnel/wake when its reverse tunnel
                # is dead (backend suspended/redeployed) and every ~4 min
                # while a task is active. Inbound HTTP is the ONLY thing that
                # revives a suspended Render free-tier service — WS traffic
                # does not count. ORCH_SANDBOX_ID is the sidecar's
                # self-identity for that call (the tunnel connection carries
                # no identity until it exists).
                # ORCH_WAKE_EDGE_URL is the vm-wake SUPABASE EDGE FUNCTION:
                # the EU-region sandbox egress filter resets TLS to
                # *.onrender.com (verified live), so the wake RELAYS through
                # Supabase (reachable) which fetches the backend server-side.
                # Host env ARCFORGE_WAKE_EDGE_URL supplies it; unset → the
                # sidecar falls back to the direct POST (non-EU regions).
                f"ORCH_BACKEND_URL={backend_url}",
                f"ORCH_SANDBOX_ID={sandbox.id}",
                f"ORCH_WAKE_EDGE_URL={os.environ.get('ARCFORGE_WAKE_EDGE_URL', '')}",
                # Vision model for the Browser Vision Engine — routed through
                # the reverse tunnel (/vlm path) to NVIDIA on the backend side;
                # the VM never holds a provider key.
                "ORCH_VLM_MODEL=meta/llama-3.2-11b-vision-instruct",
                "ORCH_VLM_ENABLED=1",
                # Kept for backward compat (the old tunnel_client.py reads
                # these — harmless if tunnel_client isn't running).
                f"TUNNEL_BACKEND_WS_URL={tunnel_ws_url}",
                "TUNNEL_LISTEN_PORT=7777",
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
                f"chmod +x {SIDE_CAR_HOME}/orchestrator.py "
                f"{SIDE_CAR_HOME}/tunnel_client.py {SIDE_CAR_HOME}/watchdog.sh; "
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
                + "python3 -c 'import fastapi, uvicorn, curl_cffi, aiohttp' 2>/dev/null "
                + "&& echo PIP_OK || echo PIP_FAIL",
                timeout=240,
            )
            pip_ok = "PIP_OK" in pip_out
            if not pip_ok:
                logger.warning(
                    "sidecar pip install failed in %s: %s", sandbox.id, pip_out[-300:],
                )
                return result  # installed=False — platform falls back to SSE

            # 5) Launch: PM2 preferred (manages BOTH tunnel-client +
            #    agent-brain), watchdog fallback. Both source the env file
            #    so the token/tunnel/LLM config never appears in `ps`.
            # 4.5) THE AGENT TOOLBELT (background, never blocks the daemon):
            #      · Playwright + Chromium — browser_tool (navigate/
            #        console_spy/interact/screenshot)
            #      · pyflakes — Python lint fallback
            #      · langgraph + tree-sitter — the StateGraph orchestration
            #        engine + repo-mapper (the daemon ships an identical
            #        built-in engine + regex fallback until these land)
            #      · typescript-language-server / pyright / bash-language-
            #        server — real LSP daemons for verify_file (CLI cascade
            #        answers until they land)
            #      Best-effort — every layer degrades gracefully.
            try:
                await asyncio.to_thread(
                    sandbox.process.exec,
                    "nohup bash -c '"
                    "pip install --quiet --disable-pip-version-check "
                    "playwright pyflakes 2>/dev/null; "
                    "pip install --quiet --disable-pip-version-check "
                    "langgraph tree-sitter 2>/dev/null && "
                    "(pip install --quiet --disable-pip-version-check "
                    "tree-sitter-languages 2>/dev/null || "
                    "pip install --quiet --disable-pip-version-check "
                    "tree-sitter-language-pack 2>/dev/null) || true; "
                    "npm install -g typescript-language-server pyright "
                    "bash-language-server >/dev/null 2>&1 || true; "
                    "python3 -m playwright install chromium --with-deps "
                    "> /home/daytona/.system/playwright-install.log 2>&1' "
                    "> /home/daytona/.system/toolbelt-install.log 2>&1 "
                    "< /dev/null &",
                    "/home/daytona", None, 10,
                )
                logger.info("sidecar: agent toolbelt install launched in background")
            except Exception as exc:  # noqa: BLE001
                logger.info("sidecar: agent toolbelt background install skipped: %s", exc)

            launch = (
                # idempotent: stop any previous supervisor + daemons
                "(pm2 delete tunnel-client 2>/dev/null || true); "
                "(pm2 delete agent-brain 2>/dev/null || true); "
                "pkill -f tunnel_client.py 2>/dev/null; "
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

            # 6) Verify the orchestrator daemon answers on :9000. In
            #    reverse-tunnel mode (the new default), there's only ONE
            #    daemon — agent-brain. The old tunnel-client on :7777 is
            #    not started (see ecosystem.config.js). The orchestrator's
            #    /reverse-tunnel WS endpoint will accept the backend's
            #    inbound dial AFTER install — driven by the backend's
            #    reverse-tunnel-client service (not the installer).
            verify = await arun(
                "sleep 3; "
                # Wait for the orchestrator /health (max ~20s).
                "for i in 1 2 3 4 5 6 7 8 9 10; do "
                f"ocode=$(curl -s -o /dev/null -w '%{{http_code}}' "
                f"http://localhost:{ORCHESTRATOR_PORT}/health --max-time 3); "
                "[ \"$ocode\" = \"200\" ] && break; sleep 2; done; "
                "echo VERIFY_$ocode",
                timeout=60,
            )
            if "VERIFY_200" not in verify:
                # Surface the daemon logs for debugging.
                tail = await arun(
                    f"tail -n 20 {SIDE_CAR_HOME}/pm2-tunnel-err.log "
                    f"{SIDE_CAR_HOME}/pm2-err.log "
                    f"{SIDE_CAR_HOME}/tunnel_client.log "
                    f"{SIDE_CAR_HOME}/orchestrator.log 2>/dev/null", 15,
                )
                logger.warning(
                    "sidecar health check failed in %s (verify=%s, tail=%s)",
                    sandbox.id, verify.strip()[-120:], tail[-500:],
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

        Additionally probes the generated app's dev-server ports (3000
        Next.js / 5173 legacy Vite) from INSIDE the VM. When one answers,
        a SIGNED Daytona preview URL is minted for it and returned as
        app_url/app_port — the studio Preview tab iframes that URL for
        the REAL live preview (closing the old design gap where the VM
        flow never fed the preview surface). Null until a server is up.
        """
        info: dict[str, Any] = {
            "installed": False,
            "port": ORCHESTRATOR_PORT,
            "url": None,
            "token": None,
            "alive": False,
            "app_url": None,
            "app_port": None,
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

        # ── App dev-server probe (best-effort; never fails the call) ────
        app_port = await self._probe_app_port(sandbox)
        if app_port:
            try:
                link = await asyncio.to_thread(
                    sandbox.create_signed_preview_url, app_port, 86400,
                )
                url = getattr(link, "url", None)
                if url:
                    info["app_url"] = str(url)
                    info["app_port"] = app_port
            except Exception as exc:
                logger.info("signed app preview link unavailable for %s: %s",
                            getattr(sandbox, "id", "?"), exc)
        return info

    async def _probe_app_port(self, sandbox: Any) -> int | None:
        """Return the first APP_PORTS port answering HTTP inside the VM.

        One shell round-trip probes every candidate port. Any HTTP status
        OTHER than 000 (connect failure) counts as UP — a dev server that
        answers 404 for / is still a live server worth previewing; only
        curl's 000 (could not connect) means "nothing listens here".
        """
        probe = "; ".join(
            f"printf '{port}='; curl -s -o /dev/null -w '%{{http_code}}' "
            f"http://localhost:{port}/ --max-time 2"
            for port in APP_PORTS
        )
        try:
            out = await asyncio.to_thread(
                sandbox.process.exec, probe, "/home/daytona", None, 12,
            )
            text = (getattr(out, "result", "") or "")
        except Exception:
            return None
        for port in APP_PORTS:
            marker = f"{port}="
            if marker in text:
                code = text.split(marker, 1)[1][:3].strip()
                if code.isdigit() and code != "000":
                    return port
        return None

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    async def _preview_link(self, sandbox: Any) -> str | None:
        """SIGNED Daytona preview URL for the sidecar port.

        The plain preview link (get_preview_link) is gated behind Daytona
        dashboard session auth — useless for ArcForge end users. The SIGNED
        URL (create_signed_preview_url) embeds its auth token in the
        hostname, is self-contained, and works from any browser. Verified
        live: REST + WebSocket both flow through it.

        Each agent-info probe mints a fresh 24h signed URL, so an active
        studio never sees an expired link.
        """
        try:
            link = await asyncio.to_thread(
                sandbox.create_signed_preview_url,
                ORCHESTRATOR_PORT, 86400,
            )
            url = getattr(link, "url", None)
            return str(url) if url else None
        except Exception as exc:
            logger.warning("signed preview link unavailable for %s: %s",
                           getattr(sandbox, "id", "?"), exc)
            return None

    def _render_ecosystem(
        self,
        token: str,
        llm: dict[str, str],
        tunnel_ws_url: str,
        tunnel_token: str,
    ) -> str:
        """Render ecosystem.config.js with the agent-brain env baked in.

        In reverse-tunnel mode (the new default), there is only ONE
        app in the ecosystem — agent-brain. The template carries a
        single placeholder marker /* __ORCH_ENV__ */ (inside the
        agent-brain app's env). We replace it with the rendered env
        lines. The install command ALSO sources orchestrator.env before
        `pm2 start --update-env`, so both paths carry identical config.

        The old /* __TUNNEL_ENV__ */ marker is gone (the tunnel-client
        app was removed in Task 15). If the placeholder is somehow still
        present in the template (someone reverted only the template),
        the .replace below is a no-op — no error."""
        template = (_SIDECAR_SRC_DIR / "ecosystem.config.js").read_text("utf-8")
        # agent-brain env: the orchestrator's config. The AI client uses
        # the IN-PROCESS reverse-tunnel WS endpoint — ORCH_LLM_URL is the
        # sentinel "reverse-tunnel://" (the orchestrator detects this and
        # routes through rt_mux.send_req instead of urllib). The LLM key
        # is a dummy (the backend injects the real NVIDIA key server-side).
        orch_env = (
            f'ORCH_PORT: "{ORCHESTRATOR_PORT}",\n'
            f'        ORCH_TOKEN: "{token}",\n'
            '        ORCH_WORKSPACE: "/workspace",\n'
            f'        ORCH_SYSTEM_DIR: "{SIDE_CAR_HOME}",\n'
            '        ORCH_LLM_URL: "reverse-tunnel://",\n'
            '        ORCH_LLM_KEY: "tunnel-injected",\n'
            f'        ORCH_LLM_MODEL: "{llm["model"]}",\n'
            f'        ORCH_CHIEF_FALLBACK_MODEL: "{llm["chief_fallback_model"]}",\n'
            f'        ORCH_AGENT_MODEL: "{llm["agent_model"]}",\n'
            f'        ORCH_FRONTEND_MODEL: "{llm["frontend_model"]}",\n'
            f'        ORCH_BACKEND_MODEL: "{llm["backend_model"]}",\n'
            f'        ORCH_DEBUGGER_MODEL: "{llm["debugger_model"]}",\n'
            '        ORCH_LLM_READY: "1",\n'
            '        ORCH_VLM_MODEL: "meta/llama-3.2-11b-vision-instruct",\n'
            '        ORCH_VLM_ENABLED: "1",\n'
            f'        TUNNEL_TOKEN: "{tunnel_token}",'
        )
        # If the old template (with __TUNNEL_ENV__) is somehow still
        # deployed, drop a harmless empty placeholder to avoid leaving
        # the literal string in the rendered output.
        rendered = template.replace("/* __TUNNEL_ENV__ */", "")
        rendered = rendered.replace("/* __ORCH_ENV__ */", orch_env)
        return rendered


agent_installer = AgentInstaller()
