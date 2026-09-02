#!/usr/bin/env bash
# ArcForge orchestrator + engine watchdog — PM2-less supervisor fallback.
#
# Used when `npm install -g pm2` is unavailable in the VM (no npm, no
# network, broken node). Semantics mirror PM2's autorestart loop but for
# THREE processes:
#
#   1. tunnel_client.py  — the WS tunnel daemon (localhost:7777 -> backend).
#      MUST be up before the orchestrator can reach the LLM, so we start it
#      first and give it 2s to connect its WebSocket.
#   2. orchestrator.py   — the brain (port 9000, SQLite state.db, multi-
#      agent pipeline, Bearer auth, crash-recovery).
#   3. forgvi-engine     — the Forgvi 2.0 engine (node, port 8799).
#      Strictly OPTIONAL: if node is missing (or the engine tree was
#      never installed at $SYSTEM_DIR/engine/src/server.js) the engine
#      simply stays down — the sidecar keeps working without it. The
#      engine's env (PORT/ENGINE_* — incl. the LLM token) rides in via
#      the orchestrator.env exports this watchdog inherited at launch.
#
# Loop:
#   while true; do
#     [tunnel down?]  -> restart tunnel_client.py
#     [orch down?]    -> restart orchestrator.py
#     [engine down?]  -> restart engine (only if it was ever startable)
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
# patterns match the python/node processes, never this shell).
pkill -f "tunnel_client\.py" 2>/dev/null
pkill -f "orchestrator\.py" 2>/dev/null
pkill -f "engine/src/server\.js" 2>/dev/null
sleep 0.5

TUNNEL_PID=""
ORCH_PID=""
ENGINE_PID=""
ENGINE_ENABLED=1

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

start_engine() {
    # Forgvi 2.0 engine — OPTIONAL third process. Resilient by design:
    #   · node missing (broken/partial image)      -> disable, never retry
    #   · engine tree never installed (no vendored -> disable, never retry
    #     files on this host, engine not uploaded)
    #   · node_modules not ready yet (the background -> retry below; the
    #     npm install is still running — exit is instant, 5s cadence)
    # The sidecar itself (tunnel + orchestrator) is unaffected either way.
    if [ "$ENGINE_ENABLED" != "1" ]; then
        return
    fi
    if ! command -v node >/dev/null 2>&1; then
        echo "[watchdog $(date -u +%FT%TZ)] node missing — forgvi-engine disabled"
        ENGINE_ENABLED=0
        ENGINE_PID=""
        return
    fi
    if [ ! -f "$SYSTEM_DIR/engine/src/server.js" ]; then
        echo "[watchdog $(date -u +%FT%TZ)] engine tree missing — forgvi-engine disabled"
        ENGINE_ENABLED=0
        ENGINE_PID=""
        return
    fi
    echo "[watchdog $(date -u +%FT%TZ)] starting forgvi-engine"
    node "$SYSTEM_DIR/engine/src/server.js" \
        >> "$SYSTEM_DIR/engine.log" 2>&1 &
    ENGINE_PID=$!
}

# Start the tunnel first — give it 2s to dial the backend WS so the
# orchestrator's first LLM call has a live localhost:7777 to hit.
start_tunnel
sleep 2
start_orch
# The engine is started AFTER the brain: its LLM path depends on the
# orchestrator's /llm/v1 proxy being up (ENGINE_ORCH_BASE=:9000).
sleep 1
start_engine

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
    # Engine restarts on a 5s cadence (not 1s): while the background
    # `npm install` is still populating node_modules the engine exits
    # instantly, and a 1s loop would spin hot for the 1-3 min it takes.
    if [ "$ENGINE_ENABLED" = "1" ] && [ -n "$ENGINE_PID" ] \
       && ! kill -0 "$ENGINE_PID" 2>/dev/null; then
        echo "[watchdog $(date -u +%FT%TZ)] forgvi-engine died ($?) — restarting in 5s"
        sleep 5
        start_engine
    fi
    sleep 0.5
done
