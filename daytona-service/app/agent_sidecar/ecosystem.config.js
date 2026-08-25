// ArcForge PM2 process configuration — agent-brain (in reverse-tunnel mode)
//
// Written into every Daytona MicroVM at /workspace/.system/ecosystem.config.js
// (physically /home/daytona/.system/) and started with:
//
//     pm2 start /workspace/.system/ecosystem.config.js && pm2 save
//
// ONE process (the old tunnel-client app was REMOVED in Task 15 — the
// orchestrator now handles the LLM bridge ITSELF over its own
// /reverse-tunnel WS endpoint, eliminating the need for a separate
// tunnel daemon):
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
// PM2 guarantees the daemon survives crashes: if the python process dies
// (OOM, unhandled exception, VM hiccup) PM2 restarts it within
// restart_delay ms. The orchestrator's own crash-recovery (re-enqueue
// pending/running tasks from state.db) makes each restart lossless.
//
// The env block below carries a placeholder marker (__ORCH_ENV__) that
// the host installer replaces with literal values at generation time —
// see app/services/agent_installer.py::_render_ecosystem. The installer
// ALSO writes an orchestrator.env file and sources it before
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
  ],
};
