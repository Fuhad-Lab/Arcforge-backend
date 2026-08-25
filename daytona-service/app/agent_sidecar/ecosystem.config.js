// ArcForge PM2 process configuration — tunnel-client + agent-brain
//
// Written into every Daytona MicroVM at /workspace/.system/ecosystem.config.js
// (physically /home/daytona/.system/) and started with:
//
//     pm2 start /workspace/.system/ecosystem.config.js && pm2 save
//
// TWO processes (PM2 starts them in array order — tunnel-client first):
//
//   1. tunnel-client  — the In-VM WS tunnel daemon. Exposes a local HTTP
//      server on 127.0.0.1:7777 (the VM's "AI endpoint") and bridges every
//      inbound request over a single persistent WebSocket to the ArcForge
//      backend's /api/tunnel endpoint. The backend INJECTS the real NVIDIA
//      key (never present in the VM) and forwards to NVIDIA (US region,
//      unblocked), then streams the response back down the WS. This bypasses
//      Daytona's EU egress filter (which drops outbound TLS to *.nvidia.com
//      and *.onrender.com) — the filter only sees VM↔backend WS traffic.
//
//   2. agent-brain    — the orchestrator daemon (port 9000, SQLite state.db,
//      multi-agent pipeline Architect→Developer→Debugger, Bearer auth,
//      crash-recovery). Its AI client POSTs to http://localhost:7777/v1/
//      chat/completions (the tunnel) with a DUMMY Authorization header that
//      the tunnel strips at the edge. The orchestrator's own LLM retry loop
//      (3 attempts, 5s apart) tolerates the brief startup gap if the tunnel
//      hasn't connected its WS yet — no native PM2 deps are needed.
//
// PM2 guarantees both survive crashes: if either python process dies (OOM,
// unhandled exception, VM hiccup) PM2 restarts it within restart_delay ms.
// The orchestrator's own crash-recovery (re-enqueue pending/running tasks
// from state.db) makes each restart lossless; the tunnel_client reconnects
// its WS with backoff (1s→2s→5s→10s→30s cap).
//
// The env blocks below carry placeholder markers (__TUNNEL_ENV__ /
// __ORCH_ENV__) that the host installer replaces with literal values at
// generation time — see app/services/agent_installer.py::_render_ecosystem.
// The installer ALSO writes an orchestrator.env file and sources it before
// `pm2 start --update-env`, so both paths carry identical config. Token +
// LLM config are NEVER checked into any repo.

module.exports = {
  apps: [
    {
      // In-VM WS tunnel daemon — MUST be up before agent-brain can reach
      // the LLM. Listed first so PM2 starts it first. If the WS isn't
      // connected yet when agent-brain makes its first LLM call, the
      // orchestrator's retry loop (3× / 5s) rides it out.
      name: "tunnel-client",
      script: "python3",
      args: "/workspace/.system/tunnel_client.py",
      cwd: "/workspace/.system",
      interpreter: "none",                    // script IS the interpreter invocation
      autorestart: true,                      // <- crash => instant restart
      watch: false,
      max_restarts: 20,                      // cap so a misconfigured token doesn't spin forever
      min_uptime: "5s",
      restart_delay: 1000,                    // 1s between restarts
      max_memory_restart: "256M",             // OOM guard
      kill_timeout: 5000,
      exp_backoff_restart_delay: 250,
      time: true,                             // timestamped logs
      out_file: "/workspace/.system/pm2-tunnel-out.log",
      error_file: "/workspace/.system/pm2-tunnel-err.log",
      merge_logs: true,
      env: {
        NODE_ENV: "production",
        /* __TUNNEL_ENV__ */
      },
    },
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
