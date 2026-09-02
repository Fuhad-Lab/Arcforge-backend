// ArcForge PM2 process configuration — agent-brain (in reverse-tunnel mode)
//
// Written into every Daytona MicroVM at /workspace/.system/ecosystem.config.js
// (physically /home/daytona/.system/) and started with:
//
//     pm2 start /workspace/.system/ecosystem.config.js && pm2 save
//
// ONE ecosystem file, TWO processes (one `pm2 start` covers both):
//
//   1. agent-brain    — the orchestrator daemon (port 9000, SQLite
//      state.db, multi-agent pipeline Architect→Developer→Debugger,
//      Bearer auth, crash-recovery). Its AI client sends `req` frames
//      over the IN-PROCESS /reverse-tunnel WS endpoint to the ArcForge
//      backend (which dialed IN via the signed daytonaproxy01.eu URL).
//      The backend INJECTS the real NVIDIA key (never present in the
//      VM) and streams res/chunk/done frames back down the WS. This
//      bypasses Daytona's EU egress filter — the filter never sees any
//      outbound TLS from the VM (the only outbound traffic from the VM
//      is the frontend's existing /ws connection, which it already
//      allows). See app/agent_sidecar/orchestrator.py — the
//       /reverse-tunnel WS endpoint + the rt_mux multiplexer.
//
//   2. forgvi-engine  — the Forgvi 2.0 engine (Node.js/express, port
//      8799, PM2-supervised). Installed from the vendored tree at
//      app/engine_sidecar/ (src/*.js, package.json, .prime-agent/
//      models.json, vendor/*.tgz) which the installer uploads to
//      /home/daytona/.system/engine/. Its LLM path runs through the
//      orchestrator's /llm/v1 proxy (ENGINE_ORCH_BASE) — the SAME
//      reverse-tunnel + token as agent-brain, so the engine holds no
//      provider key either. The engine's first start typically races
//      its own background `npm install` (1-3 min): node exits with
//      MODULE_NOT_FOUND until node_modules lands and PM2's autorestart
//      (exponential backoff) keeps retrying until it sticks.
//
//      The engine app is wrapped in __ENGINE_APP_START/END__ markers —
//      when the engine source tree is NOT vendored on the host, the
//      installer STRIPS the whole app from this file (a PM2 app whose
//      script doesn't exist would fail `pm2 start`); the __ENGINE_ENV__
//      placeholder inside its env block mirrors __ORCH_ENV__ and is
//      replaced with literal values at generation time.
//
// PM2 guarantees the daemon survives crashes: if the python process dies
// (OOM, unhandled exception, VM hiccup) PM2 restarts it within
// restart_delay ms. The orchestrator's own crash-recovery (re-enqueue
// pending/running tasks from state.db) makes each restart lossless.
//
// The env blocks carry placeholder markers (__ORCH_ENV__ / __ENGINE_ENV__)
// that the host installer replaces with literal values at generation
// time — see app/services/agent_installer.py::_render_ecosystem. The
// installer ALSO writes an orchestrator.env file (the agent-brain config
// + the engine's env lines, including ENGINE_BUSY_FILE for the
// orchestrator's /status engine_busy heartbeat) and sources it before
// `pm2 start --update-env`, so both paths carry identical config. Token +
// LLM config are NEVER checked into any repo.

module.exports = {
  apps: [
    {
      name: "agent-brain",                    // the orchestrator daemon
      script: "python3",
      args: "/workspace/.system/orchestrator.py",
      cwd: "/workspace/.system",
      interpreter: "none",                    // script IS the interpreter invocation
      autorestart: true,                      // <- crash => instant restart
      watch: false,
      max_restarts: 999,                      // effectively unlimited
      min_uptime: "10s",
      restart_delay: 1000,                    // 1s between restarts
      max_memory_restart: "512M",             // OOM guard
      kill_timeout: 5000,
      exp_backoff_restart_delay: 250,
      time: true,                             // timestamped logs
      out_file: "/workspace/.system/pm2-out.log",
      error_file: "/workspace/.system/pm2-err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        /* __ORCH_ENV__ */
      },
    },
    /* __ENGINE_APP_START__ */
    {
      name: "forgvi-engine",                   // the Forgvi 2.0 engine
      script: "node",
      args: "/home/daytona/.system/engine/src/server.js",
      cwd: "/home/daytona/.system/engine",
      interpreter: "none",                    // script IS the interpreter invocation
      autorestart: true,                      // <- crash => instant restart
      watch: false,
      max_restarts: 999,                      // effectively unlimited
      min_uptime: "10s",
      restart_delay: 5000,                    // npm install may still be running
      max_memory_restart: "1G",               // OOM guard
      kill_timeout: 5000,
      exp_backoff_restart_delay: 250,         // back off while npm install runs
      time: true,                             // timestamped logs
      out_file: "/workspace/.system/engine-out.log",
      error_file: "/workspace/.system/engine-err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        /* __ENGINE_ENV__ */
      },
    },
    /* __ENGINE_APP_END__ */
  ],
};
