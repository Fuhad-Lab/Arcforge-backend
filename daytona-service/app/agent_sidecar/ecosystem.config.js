// ArcForge PM2 process configuration — "agent-brain"
//
// Written into every Daytona MicroVM at /workspace/.system/ecosystem.config.js
// (physically /home/daytona/.system/) and started with:
//
//     pm2 start /workspace/.system/ecosystem.config.js && pm2 save
//
// PM2 guarantees the orchestrator daemon survives crashes: if the python
// process dies (OOM, unhandled exception, VM hiccup) PM2 restarts it within
// restart_delay ms. On boot, the daemon itself re-enqueues any task that was
// pending/running when it died (crash recovery in orchestrator.py), so no
// user work is ever lost.
//
// The env block is populated by the host at install time (token + LLM config
// are passed as literal values by the install command — never checked into
// any repo).
module.exports = {
  apps: [
    {
      name: "agent-brain",                    // pm2 start orchestrator.py --name agent-brain
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
        // Populated by the install command via process.env at generation
        // time — see app/services/workspace_coordinator.py
        // (ORCH_PORT / ORCH_TOKEN / ORCH_LLM_* are injected there).
      },
    },
  ],
};
