/**
 * Forgvi Engine — HTTP server.
 *
 * The engine the Forge frontend talks to (NEXT_PUBLIC_FORGVI_ENGINE_URL,
 * or the in-VM engine's signed Daytona preview URL):
 *
 *   GET  /health                 liveness + kernel/model status
 *   POST /runs                   start a goal run {objective, acceptance, budgets}
 *   GET  /runs/:id               run state + completion report
 *   GET  /runs/:id/events        SSE — journal replay + live events
 *   POST /runs/:id/abort         abort a running goal
 *   POST /runs/:id/answer        answer a pending ask_user question
 *   GET  /runs                   list runs (diagnostics)
 *
 * CORS: origin allowlist (the Forge site + local dev + Render previews).
 * The engine is browser-reachable only from those origins.
 */

// Startup hygiene for the kernel: no version checks, no network on boot.
process.env.PI_OFFLINE ??= "1";
process.env.PI_SKIP_VERSION_CHECK ??= "1";

import express from "express";
import { unlinkSync, existsSync, writeFileSync } from "node:fs";
import { kernelModelId, kernelReady, requiredKeyEnv, ENGINE_IN_VM } from "./kernel.js";
import { RunManager } from "./goal-loop.js";
import { listPersonas, loadPersonas, G } from "./personas.js";
import { describeCatalog, registrySize } from "./registry.js";
import "./nodes.js"; // registers the capability graph at boot

const PORT = Number(process.env.PORT ?? 8080);
const PERSIST_DIR = process.env.ENGINE_PERSIST_DIR ?? null;

/** Heartbeat file the 1.0 orchestrator reads to report engine_busy on
 * /status (which the backend's tunnel sweeper uses to keep the reverse
 * tunnel alive while an in-VM run works). Touched every 10s while any run
 * is active; removed when the engine goes idle. */
const BUSY_FILE = process.env.ENGINE_BUSY_FILE ?? null;
const ALLOWED_ORIGINS = new Set(
  [
    "https://forgeyn.com.ng",
    "https://www.forgeyn.com.ng",
    "http://localhost:3000",
    "http://localhost:3001",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:3001",
    ...(process.env.ENGINE_EXTRA_ORIGINS ?? "").split(",").map((s) => s.trim()).filter(Boolean),
  ].filter(Boolean),
);

const app = express();
app.disable("x-powered-by");
app.use(express.json({ limit: "256kb" }));

/** CORS — HOST MODE ONLY: origin allowlist, credentials never used.
 *
 * IN-VM MODE MUST NOT EMIT CORS HEADERS AT ALL. The signed Daytona preview
 * proxy in front of the in-VM engine already terminates browser CORS: it
 * echoes the request Origin and answers OPTIONS preflights itself. If this
 * engine also set Access-Control-Allow-Origin, the browser would receive
 * the header TWICE and reject the whole response — every fetch from the
 * site would die with a CORS TypeError (the live "engine is not
 * responding" bug, observed 2026-09-03). One header, from the proxy. */
app.use((req, res, next) => {
  if (ENGINE_IN_VM) {
    // Preflights are answered by the proxy before they ever reach the
    // engine; if one still slips through, a bare 204 is a valid answer
    // (the proxy decorates it with its own CORS headers).
    if (req.method === "OPTIONS") {
      res.sendStatus(204);
      return;
    }
    next();
    return;
  }
  const origin = req.headers.origin;
  if (origin && ALLOWED_ORIGINS.has(origin)) {
    res.setHeader("Access-Control-Allow-Origin", origin);
    res.setHeader("Vary", "Origin");
    res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
    res.setHeader("Access-Control-Allow-Headers", "Content-Type");
    res.setHeader("Access-Control-Max-Age", "86400");
  }
  if (req.method === "OPTIONS") {
    res.sendStatus(204);
    return;
  }
  next();
});

const manager = new RunManager();

// ── Busy heartbeat (in-VM mode) ──────────────────────────────────────────
// While any run is active, keep the heartbeat file fresh so the VM's
// orchestrator daemon (and through it the backend's tunnel sweeper) knows
// the engine is mid-run and must keep the LLM reverse tunnel connected.
// Touch = write a tiny stamp (utimes cannot create a missing file).
if (BUSY_FILE) {
  const stampBusy = () => {
    try {
      writeFileSync(BUSY_FILE, String(Date.now()), "utf8");
    } catch {
      /* heartbeat must never kill the engine */
    }
  };
  const clearBusy = () => {
    try {
      if (existsSync(BUSY_FILE)) unlinkSync(BUSY_FILE);
    } catch {
      /* best-effort */
    }
  };
  setInterval(() => {
    if (manager.countActive() > 0) stampBusy();
    else clearBusy();
  }, 10_000).unref?.();
  // Set the initial state immediately (an engine booted mid-run after PM2
  // restart counts as busy — its recovered runs already finished honestly,
  // so idle is the correct initial state; the interval takes over from here).
  if (manager.countActive() > 0) stampBusy();
  else clearBusy();
}

app.get("/", (_req, res) => {
  // 200 on the bare URL — uptime monitors (UptimeRobot etc.) pointed at the
  // service root must see a healthy response, not a 404 from an unrouted
  // path. Liveness ≠ kernel readiness: /health carries the full truth
  // (503 when the model provider key is missing); this route only answers
  // "the engine process is alive".
  const ready = kernelReady();
  res.status(200).json({
    ok: true,
    engine: "forgvi",
    version: "1.0.0",
    mode: ENGINE_IN_VM ? "in-vm" : "host",
    kernel_ready: ready,
    hint: ready ? "GET /health for status; POST /runs to start a goal" : "kernel not configured — see /health",
  });
});

app.get("/health", (_req, res) => {
  const ready = kernelReady();
  res.status(ready ? 200 : 503).json({
    ok: ready,
    status: ready ? "ok" : "degraded",
    engine: "forgvi",
    version: "1.0.0",
    kernel: "prime-agent@0.8.1",
    model: kernelModelId(),
    // Where this engine process lives — "in-vm" (inside the project's
    // Daytona sandbox, tools native, journal on VM disk) or "host" (the
    // Render deployment, VM-bound via REST grants).
    mode: ENGINE_IN_VM ? "in-vm" : "host",
    persisted: Boolean(PERSIST_DIR),
    activeRuns: manager.countActive(),
    totalRuns: manager.runs.size,
    personas: G.prompts.size,
    registryNodes: registrySize(),
  });
});

/** Persona roster (Vube pillar 4 — dynamic profiles from .prime/prompts/). */
app.get("/personas", (_req, res) => {
  res.json({
    dir: G.promptsDir,
    personas: listPersonas(),
  });
});

/** Reload personas from disk (edit .prime/prompts/*.md → no redeploy). */
app.post("/personas/reload", (_req, res) => {
  const count = loadPersonas();
  res.json({ ok: true, loaded: count, dir: G.promptsDir });
});

/** Capability registry catalog (Vube pillar 3 — discoverable nodes). */
app.get("/nodes", (req, res) => {
  res.type("text/plain").send(describeCatalog({ kind: req.query.kind }));
});

app.post("/runs", (req, res) => {
  if (!kernelReady()) {
    res.status(503).json({ error: `engine kernel is not configured (missing ${requiredKeyEnv()})` });
    return;
  }
  // `budgets` (sent by older frontends) is accepted and IGNORED — runs are
  // free and dynamic: no timer, no iteration cap, no token budget (user
  // mandate 2026-09-05). The run ends when the judge passes every
  // criterion or the user aborts.
  const { objective, acceptance, workspaceGrant } = req.body ?? {};
  try {
    const run = manager.start({ objective, acceptance, workspaceGrant });
    res.status(201).json({
      runId: run.runId,
      goalId: run.goalId,
      status: run.status,
      workspace: run.workspace
        ? { sandboxId: run.workspace.sandboxId, projectId: run.workspace.projectId, bound: true }
        : { bound: false },
    });
  } catch (error) {
    const message = String(error?.message ?? error);
    const isValidation =
      message.includes("acceptance criteria") ||
      message.includes("objective is required") ||
      Boolean(error?.validation);
    res.status(isValidation ? 400 : 429).json({ error: message });
  }
});

app.get("/runs", (_req, res) => {
  res.json({
    runs: [...manager.runs.values()].map((run) => manager.view(run)),
  });
});

app.get("/runs/:id", (req, res) => {
  const run = manager.get(req.params.id);
  if (!run) {
    res.status(404).json({ error: "run not found" });
    return;
  }
  res.json(manager.view(run));
});

app.post("/runs/:id/abort", (req, res) => {
  const run = manager.get(req.params.id);
  if (!run) {
    res.status(404).json({ error: "run not found" });
    return;
  }
  const reason = String(req.body?.reason ?? "user abort");
  const ok = manager.abort(req.params.id, reason);
  res.json({ ok, runId: run.runId, status: run.status, aborted: ok });
});

/** Answer a pending ask_user question (the studio's question card).
 * The blocked chief tool resolves with the answer; the journal's
 * question_resolved event locks every rendered card. Unknown ids settle
 * honestly (400) — a stale card after a restart should not pretend. */
app.post("/runs/:id/answer", (req, res) => {
  const run = manager.get(req.params.id);
  if (!run) {
    res.status(404).json({ error: "run not found" });
    return;
  }
  if (run.status !== "running") {
    res.status(409).json({ error: "run is not running" });
    return;
  }
  const questionId = String(req.body?.question_id ?? "").trim();
  const answer = String(req.body?.answer ?? "").trim();
  if (!questionId || !answer) {
    res.status(400).json({ error: "question_id and answer are required" });
    return;
  }
  const ok = run.interactions?.resolve(questionId, answer) ?? false;
  if (!ok) {
    res.status(400).json({ error: "unknown or already-answered question_id" });
    return;
  }
  res.json({ ok: true, question_id: questionId });
});

/** SSE — journal replay + live events, heartbeat every 15s. */
app.get("/runs/:id/events", (req, res) => {
  const run = manager.get(req.params.id);
  if (!run) {
    res.status(404).json({ error: "run not found" });
    return;
  }
  const since = Number(req.query.since ?? 0) || 0;

  res.writeHead(200, {
    "Content-Type": "text/event-stream",
    "Cache-Control": "no-cache, no-transform",
    Connection: "keep-alive",
    "X-Accel-Buffering": "no",
  });
  res.write("retry: 3000\n\n");

  const write = (frame) => res.write(frame);
  const detach = run.journal.attach(write, since);

  const heartbeat = setInterval(() => {
    try {
      res.write(": ping\n\n");
    } catch {
      /* closed */
    }
  }, 15_000);

  // Terminal hygiene: when the journal closes, the client has the full
  // replay + the forge-close frame — actually END the response so the
  // EventSource fires onerror/onclose and every consumer (frontend poll
  // fallback included) settles immediately instead of hanging on pings.
  const closeWatcher = setInterval(() => {
    if (run.journal.closed) {
      clearInterval(closeWatcher);
      clearInterval(heartbeat);
      detach();
      try {
        res.end();
      } catch {
        /* already closed */
      }
    }
  }, 1_000);

  const close = new Set(req.socket ? [req.socket] : []);
  req.on("close", () => {
    clearInterval(heartbeat);
    clearInterval(closeWatcher);
    detach();
    close.clear();
  });
});

/** 404 + error handling. */
app.use((req, res) => res.status(404).json({ error: "not found" }));
app.use((error, _req, res, _next) => {
  console.error("[forgvi] unhandled error:", error);
  res.status(500).json({ error: "internal engine error" });
});

const server = app.listen(PORT, "0.0.0.0", () => {
  console.log(`[forgvi] engine listening on :${PORT} (mode=${ENGINE_IN_VM ? "in-vm" : "host"}${PERSIST_DIR ? ", journal persisted" : ""})`);
  console.log(`[forgvi] kernel: ${kernelReady() ? `ready (${kernelModelId()})` : `NOT READY — set ${requiredKeyEnv()}`}`);
  console.log(`[forgvi] personas: ${G.prompts.size} loaded from ${G.promptsDir}`);
  console.log(`[forgvi] registry: ${registrySize()} nodes`);
  startKeepAlive();
});

/**
 * Render free-tier spin-down guard (incident 2026-09-03, same as the
 * daytona service): free web services sleep after 15 min without inbound
 * traffic; the cold start can outlast external monitor timeouts (Uptime
 * Robot's default 30s) which then report "can't be reached". Self-ping
 * the PUBLIC health endpoint (Render injects RENDER_EXTERNAL_URL) every
 * 10 minutes so the idle timer never expires. No-ops locally.
 *
 * 2026-09-04 — KEEP-ALIVE MESH (the "daytona service is still going down"
 * incident): self-ping alone guards ONE service; if that service's own
 * loop dies (restart, event-loop starvation, deploy race) nothing brings
 * the idle timer back. The engine — the most reliable always-on Node
 * service — now ALSO pings its two critical siblings on every cycle:
 *   DAYTONA_KEEPALIVE_URL  (default arcforge-daytona /health) — the
 *                          sandbox factory; a cold daytona = every new
 *                          forge boot times out (the user-visible
 *                          "daytona is down").
 *   BACKEND_KEEPALIVE_URL  (default arcforge-backend /api/healthz) —
 *                          auth + workspace grants.
 * Inbound traffic through the public URL is inbound traffic regardless
 * of who initiated it, so a sibling's ping is exactly as good as a
 * self-ping — but now three independent loops cover each service.
 */
function startKeepAlive() {
  const base = (process.env.RENDER_EXTERNAL_URL ?? "").replace(/\/+$/, "");
  const enabled = (process.env.KEEPALIVE_ENABLED ?? "1") !== "0";
  if (!enabled || !base) return;
  const intervalMs = Number(process.env.KEEPALIVE_INTERVAL_SECONDS ?? 600) * 1000;
  const siblings = [
    process.env.DAYTONA_KEEPALIVE_URL ?? "https://arcforge-daytona.onrender.com/health",
    process.env.BACKEND_KEEPALIVE_URL ?? "https://arcforge-backend.onrender.com/api/healthz",
  ].filter(Boolean);
  const ping = async (url) => {
    try {
      const res = await fetch(url, { signal: AbortSignal.timeout(60_000) });
      console.log(`[forgvi] keep-alive ping ${new URL(url).pathname} -> ${res.status}`);
    } catch (error) {
      console.warn(`[forgvi] keep-alive ping failed (${url}): ${error?.message ?? error}`);
    }
  };
  const cycle = () => {
    void ping(`${base}/health`);
    // Stagger sibling pings by 30s so a shared-rate-limited edge sees them separately.
    siblings.forEach((url, i) => setTimeout(() => void ping(url), 30_000 * (i + 1)).unref?.());
  };
  setTimeout(cycle, 30_000).unref();
  setInterval(cycle, intervalMs).unref();
}

const shutdown = () => {
  console.log("[forgvi] shutting down");
  server.close(() => process.exit(0));
  setTimeout(() => process.exit(0), 5000).unref();
};
process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
