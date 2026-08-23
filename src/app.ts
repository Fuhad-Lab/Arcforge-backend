import express, { type Express } from "express";
import { createServer } from "http";
import cors from "cors";
import pinoHttp from "pino-http";
import router from "./routes";
import { logger } from "./lib/logger";
import { wsSync } from "./services/websocket-sync";

const app: Express = express();

// Create HTTP server for Express + WebSocket
export const server = createServer(app);

// Attach WebSocket sync on /ws
wsSync.attach(server);

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
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

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
