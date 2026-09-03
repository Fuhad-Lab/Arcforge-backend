"""Agent sidecar package.

Contains the In-VM Orchestrator daemon ("Shadow Agent") source files that
are written into every Daytona MicroVM at workspace creation:

  orchestrator.py       FastAPI daemon — SQLite state + WS manager + task queue
  ecosystem.config.js   PM2 process config ("agent-brain", autorestart)
  watchdog.sh           PM2-less supervisor fallback

See app/services/agent_installer.py for the host-side install flow.
"""
