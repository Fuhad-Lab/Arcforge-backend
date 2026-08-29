import { Router, type IRouter } from "express";
import healthRouter from "./health";
import agentRouter from "./agent";
import generateRouter from "./generate";
import mcpRouter from "./mcp";
import workspaceRouter from "./workspace";
import dbRouter from "./db";
import llmRouter from "./llm";
import tunnelWakeRouter from "./tunnel-wake";
import authOauthRouter from "./auth-oauth";
import connectorsRouter from "./connectors";

const router: IRouter = Router();

// /api/healthz — PUBLIC (Render health check). Must stay unauthenticated.
router.use(healthRouter);

// /api/llm/* — LLM proxy for the In-VM agent sidecar (X-Agent-Token auth).
router.use("/llm", llmRouter);

// /api/tunnel/wake — VM-side reverse-tunnel force-dial (X-Agent-Token auth;
// Render-sleep recovery — the sidecar calls this when its tunnel is down).
router.use(tunnelWakeRouter);

// /api/auth/github/* — GitHub Sign-In (edge-relayed; anonymous-safe routes:
// signed OAuth state + single-use one-time codes provide the security).
router.use(authOauthRouter);

// /api/connectors/* — generic connector system (GROUP 2). The OAuth
// callback inside is public-by-design (signed state); everything else
// requires a JWT via requireAuth inside the router.
router.use(connectorsRouter);

// /api/db/* — frontend data routes (JWT required; applied inside db.ts).
router.use("/db", dbRouter);

// /api/generate — SSE pipeline (JWT required; applied inside generate.ts).
router.use(generateRouter);

// /api/workspace/* — VM orchestration (JWT required; applied inside workspace.ts).
router.use("/workspace", workspaceRouter);

// /api/projects/* + /api/mcp/* — agent tooling (JWT required; applied at the
// router level inside agent.ts / mcp.ts).
router.use(agentRouter);
router.use(mcpRouter);

export default router;
