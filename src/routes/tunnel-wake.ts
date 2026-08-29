/**
 * POST /api/tunnel/wake — force-dial a sandbox's reverse tunnel.
 *
 * WHY THIS EXISTS (incident 2026-08-28, live on the 9-scenario matrix):
 * a long headless agent build ran with no studio browser open, so the
 * backend received zero inbound HTTP for >15 min and Render (free tier)
 * suspended the process. Suspension killed every reverse-tunnel WS —
 * and nothing could re-dial: the tunnel sweeper runs INSIDE the sleeping
 * process, and its `parked` map (quota hygiene) would skip stale project
 * rows anyway. For ~20 minutes every sub-agent LLM call failed with
 * "reverse-tunnel WS not connected (backend hasn't dialed in)" and the
 * build died reporting honestly.
 *
 * The only actor that KNOWS a build is in flight and can reach the
 * backend over plain HTTP is the VM sidecar itself. Inbound HTTP is
 * also exactly what wakes a suspended Render service. So the sidecar
 * POSTs this endpoint (a) reactively when an LLM call hits a dead
 * tunnel and (b) proactively every ~4 min while a task is active
 * (keepalive — WS traffic does NOT count as Render activity).
 *
 * AUTH: X-Agent-Token: <AGENT_PROXY_SECRET> — the same shared secret
 * the VM already holds as TUNNEL_TOKEN (set at sidecar install time).
 * The body carries the sandbox's own id (ORCH_SANDBOX_ID in the VM):
 * the reverse-tunnel connection carries no identity until it exists,
 * so this self-identification is the ONLY way for a VM to request its
 * own tunnel. A caller holding the secret could force-dial arbitrary
 * sandboxes — acceptable: the secret never leaves the backend/VM trust
 * boundary, and a forced dial of a live sandbox is a no-op.
 *
 * The endpoint deliberately BYPASSES sweeper eligibility (parked /
 * stale / delegation checks): a wake only ever fires from a sidecar
 * with an in-flight task — precisely the case where the tunnel must
 * exist regardless of row staleness.
 */
import { Router, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { getAgentInfo } from "../services/daytona-workspace";
import {
  ensureReverseTunnel,
  isReverseTunnelConnected,
} from "../services/reverse-tunnel-client";

const router = Router();

/** How long to wait for the forced dial to reach OPEN after ensure(). */
const DIAL_SETTLE_TIMEOUT_MS = 10_000;
const DIAL_POLL_INTERVAL_MS = 500;

async function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

router.post(
  "/tunnel/wake",
  async (req: Request, res: Response, _next: NextFunction) => {
    try {
      const expected = process.env.AGENT_PROXY_SECRET || "";
      if (!expected) {
        res.status(500).json({ ok: false, error: "AGENT_PROXY_SECRET not set" });
        return;
      }
      const token = req.headers["x-agent-token"];
      if (typeof token !== "string" || token !== expected) {
        res.status(401).json({ ok: false, error: "bad tunnel token" });
        return;
      }

      const sandboxId = typeof req.body?.sandboxId === "string"
        ? req.body.sandboxId
        : "";
      if (!sandboxId) {
        res.status(400).json({ ok: false, error: "sandboxId required" });
        return;
      }

      // Resolve the sandbox's current signed sidecar URL (one Daytona read
      // through the daytona-service — the same path agent-info uses).
      let info;
      try {
        info = await getAgentInfo(sandboxId);
      } catch (err) {
        res.status(200).json({
          ok: false,
          error: `agent-info probe failed: ${
            err instanceof Error ? err.message : String(err)
          }`,
        });
        return;
      }
      if (!info.alive || !info.url) {
        res.status(200).json({
          ok: false,
          error: "sandbox sidecar not alive",
          alive: false,
        });
        return;
      }

      const wasConnected = isReverseTunnelConnected(sandboxId);
      ensureReverseTunnel(sandboxId, info.url);

      // ensureReverseTunnel fires the WS dial asynchronously — poll briefly
      // so the caller (a sidecar mid-LLM-retry) learns whether the bridge
      // is actually up before it burns a retry attempt on it.
      const deadline = Date.now() + DIAL_SETTLE_TIMEOUT_MS;
      let connected = wasConnected;
      while (!connected && Date.now() < deadline) {
        await sleep(DIAL_POLL_INTERVAL_MS);
        connected = isReverseTunnelConnected(sandboxId);
      }

      logger.info(
        { sandboxId, wasConnected, connected },
        "tunnel wake: force-dial requested",
      );
      res.status(200).json({ ok: true, wasConnected, connected });
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : err },
        "tunnel wake: handler failed",
      );
      res.status(200).json({
        ok: false,
        error: err instanceof Error ? err.message : String(err),
      });
    }
  },
);

export default router;
