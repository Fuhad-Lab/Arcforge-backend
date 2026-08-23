import { Router, type IRouter } from "express";
import healthRouter from "./health";
import agentRouter from "./agent";
import generateRouter from "./generate";
import mcpRouter from "./mcp";
import workspaceRouter from "./workspace";

const router: IRouter = Router();

router.use(healthRouter);
router.use(agentRouter);
router.use(generateRouter);
router.use(mcpRouter);
router.use("/workspace", workspaceRouter);

export default router;
