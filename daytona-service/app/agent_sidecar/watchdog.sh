#!/usr/bin/env bash
# ArcForge orchestrator watchdog — PM2-less supervisor fallback.
#
# Used when `npm install -g pm2` is unavailable in the VM (no npm, no
# network, broken node). Semantics mirror PM2's autorestart loop:
#
#   while true; do
#     python3 orchestrator.py   # blocks until the daemon exits
#     sleep 1                    # restart_delay
#   done
#
# Launched detached via nohup so it survives the installing exec session,
# the WebSocket clients, and any terminal disconnect. The orchestrator's
# own crash-recovery (re-enqueue pending/running tasks from state.db)
# makes each restart lossless.
#
# NOTE: this script deliberately does NOT pkill other watchdog instances —
# a `pkill -f watchdog.sh` here would match THIS process and kill it. The
# installer's launch command kills prior watchdogs before starting this one.
#
# Usage: nohup bash /workspace/.system/watchdog.sh > /workspace/.system/watchdog.log 2>&1 < /dev/null &

SYSTEM_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SYSTEM_DIR"

# Idempotent: kill any previous DAEMON instance before starting (the
# pattern matches the python process, never this shell).
pkill -f "orchestrator\.py" 2>/dev/null
sleep 0.5

# Loop forever — restart the daemon 1s after any exit.
while true; do
    echo "[watchdog $(date -u +%FT%TZ)] starting orchestrator"
    python3 "$SYSTEM_DIR/orchestrator.py"
    code=$?
    echo "[watchdog $(date -u +%FT%TZ)] orchestrator exited ($code) — restarting in 1s"
    sleep 1
done
