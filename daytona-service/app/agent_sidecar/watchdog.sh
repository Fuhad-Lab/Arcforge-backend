#!/usr/bin/env bash
# ArcForge orchestrator + tunnel watchdog — PM2-less supervisor fallback.
#
# Used when `npm install -g pm2` is unavailable in the VM (no npm, no
# network, broken node). Semantics mirror PM2's autorestart loop but for
# TWO processes:
#
#   1. tunnel_client.py  — the WS tunnel daemon (localhost:7777 -> backend).
#      MUST be up before the orchestrator can reach the LLM, so we start it
#      first and give it 2s to connect its WebSocket.
#   2. orchestrator.py   — the brain (port 9000, SQLite state.db, multi-
#      agent pipeline, Bearer auth, crash-recovery).
#
# Loop:
#   while true; do
#     [tunnel down?]  -> restart tunnel_client.py
#     [orch down?]    -> restart orchestrator.py
#     sleep 0.5
#   done
#
# Launched detached via nohup so it survives the installing exec session,
# the WebSocket clients, and any terminal disconnect. The orchestrator's
# own crash-recovery (re-enqueue pending/running tasks from state.db) and
# the tunnel_client's WS auto-reconnect (1s→2s→5s→10s→30s backoff) make
# each restart lossless.
#
# NOTE: this script deliberately does NOT pkill other watchdog instances —
# a `pkill -f watchdog.sh` here would match THIS process and kill it. The
# installer's launch command kills prior watchdogs before starting this one.
#
# Usage: nohup bash /workspace/.system/watchdog.sh > /workspace/.system/watchdog.log 2>&1 < /dev/null &

SYSTEM_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SYSTEM_DIR"

# Idempotent: kill any previous DAEMON instance before starting (the
# patterns match the python processes, never this shell).
pkill -f "tunnel_client\.py" 2>/dev/null
pkill -f "orchestrator\.py" 2>/dev/null
sleep 0.5

TUNNEL_PID=""
ORCH_PID=""

start_tunnel() {
    echo "[watchdog $(date -u +%FT%TZ)] starting tunnel_client"
    python3 "$SYSTEM_DIR/tunnel_client.py" \
        > "$SYSTEM_DIR/tunnel_client.log" 2>&1 &
    TUNNEL_PID=$!
}

start_orch() {
    echo "[watchdog $(date -u +%FT%TZ)] starting orchestrator"
    python3 "$SYSTEM_DIR/orchestrator.py" \
        >> "$SYSTEM_DIR/orchestrator.log" 2>&1 &
    ORCH_PID=$!
}

# Start the tunnel first — give it 2s to dial the backend WS so the
# orchestrator's first LLM call has a live localhost:7777 to hit.
start_tunnel
sleep 2
start_orch

# Loop forever — restart whichever child died 1s after exit.
while true; do
    if ! kill -0 "$TUNNEL_PID" 2>/dev/null; then
        echo "[watchdog $(date -u +%FT%TZ)] tunnel_client died ($?) — restarting in 1s"
        sleep 1
        start_tunnel
    fi
    if ! kill -0 "$ORCH_PID" 2>/dev/null; then
        echo "[watchdog $(date -u +%FT%TZ)] orchestrator died ($?) — restarting in 1s"
        sleep 1
        start_orch
    fi
    sleep 0.5
done
