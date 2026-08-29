/**
 * /api/llm — LLM proxy for the In-VM agent orchestrator ("Shadow Agent").
 *
 * WHY THIS EXISTS: the Daytona eu-region sandboxes are geo-blocked from
 * integrate.api.nvidia.com (and most US endpoints) — the in-VM daemon
 * cannot call the LLM provider directly (verified live: connection reset).
 * The backend, hosted on Render, IS reachable from the VMs and holds the
 * platform's NVIDIA credentials.
 *
 * The daemon's ORCH_LLM_URL points here; requests are forwarded verbatim
 * to the single-mode (Solo · GLM) endpoint with the server-side key —
 * the key never enters any VM.
 *
 * Auth: shared secret via the X-Agent-Token header — the per-VM token
 * generated at workspace creation (the same one the daemon already holds).
 * The installer writes the proxy URL + this token into orchestrator.env,
 * so the daemon authenticates to the proxy with what it already has.
 */
import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { getSingleModeLlmConfig } from "../services/agent-platform";

const router: IRouter = Router();

/** Backend base URL as seen from the VMs — configured once on Render. */
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "").replace(/\/+$/, "");

/**
 * POST /api/llm/chat — OpenAI-compatible chat-completions passthrough.
 *
 * Body: { model?, messages, temperature?, max_tokens?, response_format? }
 * Auth: X-Agent-Token: <per-VM shared secret> (set at workspace creation).
 */
router.post("/chat", async (req: Request, res: Response, next: NextFunction) => {
  try {
    // Auth: the caller must present the sidecar shared secret.
    const agentToken = req.header("x-agent-token") || "";
    const expected = process.env.AGENT_PROXY_SECRET || "";
    if (!expected || agentToken !== expected) {
      res.status(401).json({ error: "unauthorized" });
      return;
    }

    const body = req.body as {
      model?: unknown;
      messages?: unknown;
      temperature?: unknown;
      max_tokens?: unknown;
      response_format?: unknown;
    };
    if (!Array.isArray(body.messages)) {
      res.status(400).json({ error: "messages array is required" });
      return;
    }

    const cfg = getSingleModeLlmConfig();
    const upstream = await fetch(cfg.url, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${cfg.key}`,
      },
      body: JSON.stringify({
        model: typeof body.model === "string" && body.model ? body.model : cfg.model,
        messages: body.messages,
        temperature: typeof body.temperature === "number" ? body.temperature : 0,
        max_tokens: typeof body.max_tokens === "number" ? body.max_tokens : 16384,
        ...(body.response_format ? { response_format: body.response_format } : {}),
      }),
      signal: AbortSignal.timeout(300_000),
    });

    const text = await upstream.text();
    res.status(upstream.status).set("Content-Type", upstream.headers.get("content-type") || "application/json").send(text);
  } catch (error) {
    logger.warn(
      { err: error instanceof Error ? error.message : error },
      "llm proxy: forward failed",
    );
    res.status(502).json({ error: "llm upstream failed" });
  }
});

/** GET /api/llm/health — liveness for the proxy (agent-token auth). */
router.get("/health", (req: Request, res: Response) => {
  const agentToken = req.header("x-agent-token") || "";
  const expected = process.env.AGENT_PROXY_SECRET || "";
  if (!expected || agentToken !== expected) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  res.json({ ok: true, base: PUBLIC_BASE_URL || null });
});

/**
 * GET /api/llm/tunnel-info — discovery for the VM-side installer.
 *
 * Returns the tunnel WS path, whether a token is required, and whether
 * the NVIDIA upstream is configured. The VM installer calls this (with
 * the X-Agent-Token) to pin the tunnel endpoint + verify the backend
 * has a key before it rewrites the orchestrator's base_url to
 * http://localhost:7777/v1.
 */
router.get("/tunnel-info", (req: Request, res: Response) => {
  const agentToken = req.header("x-agent-token") || "";
  const expected = process.env.AGENT_PROXY_SECRET || "";
  if (!expected || agentToken !== expected) {
    res.status(401).json({ error: "unauthorized" });
    return;
  }
  const cfg = getSingleModeLlmConfig();
  // Derive a public base URL hint for the VM (host-only — the VM
  // actually talks to localhost:7777, but this tells the installer the
  // upstream is configured).
  const baseUrl =
    (process.env.NVIDIA_NIM_BASE_URL || cfg.url || "").replace(
      /\/chat\/completions$/,
      "",
    ) || null;
  res.json({
    tunnelPath: "/api/tunnel",
    requiresToken: true,
    nvidiaConfigured: Boolean(cfg.key),
    baseUrl,
  });
});

export default router;
