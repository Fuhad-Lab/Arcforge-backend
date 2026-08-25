import express, { type Express } from "express";
import { createServer } from "http";
import cors from "cors";
import pinoHttp from "pino-http";
import router from "./routes";
import { logger } from "./lib/logger";
import { wsSync } from "./services/websocket-sync";
import { attachTunnelServer } from "./routes/tunnel";

const app: Express = express();

// Create HTTP server for Express + WebSocket
export const server = createServer(app);

// Attach WebSocket sync on /ws (frontend live-collaboration)
wsSync.attach(server);

// Attach the Inbound Reverse Proxy Tunnel on /api/tunnel.
// The VM opens a long-lived WS here to bypass Daytona's EU-region egress
// firewall (which blocks *.nvidia.com AND *.onrender.com over TLS). The
// VM's HTTP requests to localhost:7777 are bridged over this WS, the
// backend injects the NVIDIA API key server-side, and forwards to NVIDIA.
// See src/routes/tunnel.ts for the full protocol + rationale.
attachTunnelServer(server);

app.use(
  pinoHttp({
    logger,
    serializers: {
      req(req) {
        return {
          id: req.id,
          method: req.method,
          url: req.url?.split("?")[0],
        };
      },
      res(res) {
        return {
          statusCode: res.statusCode,
        };
      },
    },
  }),
);
app.use(cors());
// 10 MB limit: project creation carries the app logo as a base64 data URL,
// and workspace file writes can carry multi-file code payloads. The Express
// default (100 KB) rejected those with 413 "request entity too large".
app.use(express.json({ limit: "10mb" }));
app.use(express.urlencoded({ extended: true, limit: "10mb" }));

app.use((req, res, next) => {
  if (req.path.startsWith("/api/")) {
    res.setHeader("Cache-Control", "no-store");
  }
  next();
});

app.use("/api", router);

// Root route — service heartbeat
app.get("/", (_req, res) => {
  res.json({
    service: "Arcforge Backend",
    version: "1.0.0",
    status: "operational",
    endpoints: {
      api: "/api",
      websocket: "/ws",
      health: "/api/health",
      docs: "https://github.com/Fuhad-Lab/Arcforge-backend#readme",
    },
  });
});

app.use((error: unknown, _req: express.Request, res: express.Response, _next: express.NextFunction) => {
  const message = error instanceof Error ? error.message : "Unexpected server error";
  res.status(500).json({ error: message });
});

export default app;
