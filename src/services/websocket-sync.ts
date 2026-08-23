import { WebSocketServer, WebSocket } from "ws";
import type { Server } from "http";
import { logger } from "../lib/logger";
import {
  isSupabaseConfigured,
  dbSaveWorkspaceFile,
  dbGetWorkspaceFiles,
} from "../lib/supabase-db";

// ─── TYPES ───────────────────────────────────────────────────────────

type ClientMessage =
  | { type: "file:read"; path: string }
  | { type: "file:write"; path: string; content: string }
  | { type: "file:delete"; path: string }
  | { type: "file:move"; source: string; destination: string }
  | { type: "cursor:move"; path: string; line: number; column: number }
  | { type: "ping" }
  | { type: "subscribe"; projectId: string };

type ServerMessage =
  | { type: "file:updated"; path: string; content: string; userId: string }
  | { type: "file:deleted"; path: string; userId: string }
  | { type: "file:moved"; source: string; destination: string; userId: string }
  | { type: "cursor:moved"; path: string; line: number; column: number; userId: string }
  | { type: "pong" }
  | { type: "error"; message: string }
  | { type: "sync:full"; files: { path: string; content: string }[] };

interface ConnectedClient {
  ws: WebSocket;
  projectId: string | null;
  userId: string;
  isAlive: boolean;
}

// ─── WEBSOCKET SYNC MANAGER ─────────────────────────────────────────

export class WebSocketSyncManager {
  private wss: WebSocketServer | null = null;
  private clients: Set<ConnectedClient> = new Set();
  private heartbeatInterval: ReturnType<typeof setInterval> | null = null;

  /**
   * Attach the WebSocket server to an existing HTTP server.
   */
  attach(server: Server): void {
    this.wss = new WebSocketServer({ server, path: "/ws" });

    this.wss.on("connection", (ws: WebSocket, req) => {
      const userId = req.headers["x-user-id"] as string || "anonymous";
      const client: ConnectedClient = { ws, projectId: null, userId, isAlive: true };
      this.clients.add(client);
      logger.info({ userId, clientCount: this.clients.size }, "WS client connected");

      ws.on("message", (data) => {
        try {
          const msg = JSON.parse(data.toString()) as ClientMessage;
          this.handleMessage(client, msg).catch((err) => {
            this.send(client, { type: "error", message: err.message });
          });
        } catch {
          this.send(client, { type: "error", message: "Invalid JSON" });
        }
      });

      ws.on("close", () => {
        this.clients.delete(client);
        logger.info({ userId, clientCount: this.clients.size }, "WS client disconnected");
      });

      ws.on("pong", () => { client.isAlive = true; });
    });

    // Heartbeat: terminate dead connections every 30s
    this.heartbeatInterval = setInterval(() => {
      for (const client of this.clients) {
        if (!client.isAlive) {
          client.ws.terminate();
          this.clients.delete(client);
          continue;
        }
        client.isAlive = false;
        client.ws.ping();
      }
    }, 30_000);

    logger.info("WebSocket sync server attached");
  }

  /**
   * Handle an incoming client message.
   */
  private async handleMessage(client: ConnectedClient, msg: ClientMessage): Promise<void> {
    switch (msg.type) {
      case "subscribe":
        client.projectId = msg.projectId;
        // Send full file sync on subscribe
        if (isSupabaseConfigured() && msg.projectId) {
          const files = await dbGetWorkspaceFiles(msg.projectId);
          this.send(client, {
            type: "sync:full",
            files: files.map((f) => ({ path: f.path, content: f.content })),
          });
        }
        return;

      case "file:write":
        if (!client.projectId) {
          this.send(client, { type: "error", message: "Not subscribed to a project" });
          return;
        }
        // Persist to DB
        if (isSupabaseConfigured()) {
          await dbSaveWorkspaceFile(client.projectId, msg.path, msg.content, "");
        }
        // Broadcast to all other clients in the same project
        this.broadcast(client.projectId, client.userId, {
          type: "file:updated",
          path: msg.path,
          content: msg.content,
          userId: client.userId,
        });
        return;

      case "file:delete":
        if (!client.projectId) {
          this.send(client, { type: "error", message: "Not subscribed to a project" });
          return;
        }
        this.broadcast(client.projectId, client.userId, {
          type: "file:deleted",
          path: msg.path,
          userId: client.userId,
        });
        return;

      case "file:move":
        if (!client.projectId) {
          this.send(client, { type: "error", message: "Not subscribed to a project" });
          return;
        }
        this.broadcast(client.projectId, client.userId, {
          type: "file:moved",
          source: msg.source,
          destination: msg.destination,
          userId: client.userId,
        });
        return;

      case "cursor:move":
        if (!client.projectId) return;
        this.broadcast(client.projectId, client.userId, {
          type: "cursor:moved",
          path: msg.path,
          line: msg.line,
          column: msg.column,
          userId: client.userId,
        });
        return;

      case "ping":
        this.send(client, { type: "pong" });
        return;

      default:
        this.send(client, { type: "error", message: `Unknown message type: ${(msg as any).type}` });
    }
  }

  /**
   * Send a message to a single client.
   */
  private send(client: ConnectedClient, msg: ServerMessage): void {
    if (client.ws.readyState === WebSocket.OPEN) {
      client.ws.send(JSON.stringify(msg));
    }
  }

  /**
   * Broadcast a message to all clients in a project except the sender.
   */
  private broadcast(projectId: string, excludeUserId: string, msg: ServerMessage): void {
    for (const client of this.clients) {
      if (client.projectId === projectId && client.userId !== excludeUserId) {
        this.send(client, msg);
      }
    }
  }

  /**
   * Shut down the WebSocket server.
   */
  shutdown(): void {
    if (this.heartbeatInterval) clearInterval(this.heartbeatInterval);
    for (const client of this.clients) client.ws.close();
    this.clients.clear();
    this.wss?.close();
  }
}

export const wsSync = new WebSocketSyncManager();
