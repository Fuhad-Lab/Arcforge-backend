/**
 * Supabase MCP executor — GROUP 2 session 2.
 *
 * The agent-side `supabase_mcp` tool (daytona-service/app/agent_sidecar/
 * orchestrator.py) bridges through the reverse tunnel to this backend; the
 * `/mcp/supabase` tunnel branch (routes/tunnel.ts + services/reverse-tunnel-
 * client.ts) resolves sandbox → project → user and calls
 * `callSupabaseMcpTool(userId, tool, args)` here.
 *
 * WHAT THIS MODULE OWNS
 * ─────────────────────
 *  • A per-user client for the OFFICIAL hosted Supabase MCP server
 *    (https://mcp.supabase.com/mcp) using the Streamable HTTP transport
 *    with JSON-RPC 2.0: `initialize` → `notifications/initialized` →
 *    `tools/call`, session id captured from the initialize response
 *    headers (`mcp-session-id`), sent back as `Mcp-Session-Id`.
 *  • Capability gating: every MCP tool maps to a connector-registry
 *    capability ("supabase.database.write" ⊇ ".database.read" ⊇
 *    ".projects.read"); the user's stored grant (vault row) decides.
 *  • Token refresh ON USE: Supabase OAuth access tokens are short-lived;
 *    expired-ish tokens are refreshed via /v1/oauth/token
 *    (grant_type=refresh_token) and rotated back into the vault.
 *
 * SECURITY
 * ─────────
 * The user's OAuth token is read from the encrypted vault and used ONLY
 * here, server-side. It NEVER appears in: tool results returned to the
 * VM, tunnel frames, logs, or error messages. MCP responses are passed
 * through sanitized (the official server returns content blocks, never
 * credentials) and truncated to a bounded size.
 */
import { logger } from "../lib/logger";
import { getProjectRowBySandbox } from "../lib/project-lookup";
import { connectorCredentials, getConnector } from "./connector-registry";
import {
  getConnection,
  getTokens,
  parseGrantedCapabilities,
  rotateTokens,
} from "./connector-vault";

// ─── Constants ──────────────────────────────────────────────────────────

/** Official hosted Supabase MCP server (Streamable HTTP endpoint). */
const MCP_ENDPOINT = "https://mcp.supabase.com/mcp";
/** JSON-RPC protocol version the server negotiates at initialize. */
const MCP_PROTOCOL_VERSION = "2025-03-26";
/** HTTP timeout for a single MCP request (apply_migration can be slow). */
const MCP_CALL_TIMEOUT_MS = 120_000;
/** HTTP timeout for initialize (should be fast). */
const MCP_INIT_TIMEOUT_MS = 30_000;
/** Session cache TTL — re-initialize before the server expires us. */
const SESSION_TTL_MS = 50 * 60 * 1000;
/** Refresh the access token when it expires within this window. */
const REFRESH_WINDOW_MS = 5 * 60 * 1000;
/** Max characters of a single text content block returned to the VM. */
const MAX_CONTENT_CHARS = 6000;

// ─── Capability gating ──────────────────────────────────────────────────

/**
 * MCP tool → minimum connector capability required to call it.
 * Unknown tools are denied (the map is the allow-list).
 */
const TOOL_CAPABILITY: Record<string, string> = {
  // supabase.projects.read
  list_organizations: "supabase.projects.read",
  get_organization: "supabase.projects.read",
  list_projects: "supabase.projects.read",
  get_project: "supabase.projects.read",
  search_docs: "supabase.projects.read",
  // supabase.database.read
  list_tables: "supabase.database.read",
  list_extensions: "supabase.database.read",
  list_migrations: "supabase.database.read",
  get_advisors: "supabase.database.read",
  query_logs: "supabase.database.read",
  get_project_url: "supabase.database.read",
  get_publishable_keys: "supabase.database.read",
  generate_typescript_types: "supabase.database.read",
  list_edge_functions: "supabase.database.read",
  get_edge_function: "supabase.database.read",
  list_branches: "supabase.database.read",
  // supabase.database.write
  apply_migration: "supabase.database.write",
  execute_sql: "supabase.database.write",
  deploy_edge_function: "supabase.database.write",
  create_branch: "supabase.database.write",
  delete_branch: "supabase.database.write",
  merge_branch: "supabase.database.write",
  reset_branch: "supabase.database.write",
  rebase_branch: "supabase.database.write",
  pause_project: "supabase.database.write",
  restore_project: "supabase.database.write",
  create_project: "supabase.database.write",
  get_cost: "supabase.database.write",
  confirm_cost: "supabase.database.write",
};

/**
 * Capability hierarchy: write (3) ⊇ read (2) ⊇ projects.read (1).
 * A granted capability of level N satisfies a requirement of level ≤ N.
 */
const CAPABILITY_LEVEL: Record<string, number> = {
  "supabase.projects.read": 1,
  "supabase.database.read": 2,
  "supabase.database.write": 3,
};

/** Does `granted` (a set of capability ids, null = full grant) permit
 *  `required`? */
function capabilitySatisfied(granted: string[] | null, required: string): boolean {
  if (granted === null) return true; // full grant (backward compat)
  const need = CAPABILITY_LEVEL[required] ?? 99;
  return granted.some((cap) => (CAPABILITY_LEVEL[cap] ?? 0) >= need);
}

// ─── Types ──────────────────────────────────────────────────────────────

export type McpContentBlock = { type: string; text?: string };

export type McpToolOutcome =
  | { ok: true; result: { content: McpContentBlock[] } }
  | { ok: false; error: string; needs_connector?: true; capability?: string };

export type McpForwardEvent =
  | { kind: "head"; status: number; headers: Record<string, string> }
  | { kind: "chunk"; body: string };

// ─── Session cache ──────────────────────────────────────────────────────

interface McpSession {
  sessionId: string;
  expiresAt: number;
}

const sessions = new Map<string, McpSession>();

function cachedSession(userId: string): string | null {
  const s = sessions.get(userId);
  if (!s) return null;
  if (Date.now() >= s.expiresAt) {
    sessions.delete(userId);
    return null;
  }
  return s.sessionId;
}

// ─── Token handling ─────────────────────────────────────────────────────

/** Refresh-on-use: swap a near-expiry access token for a fresh pair via
 *  the Supabase OAuth token endpoint (Basic auth, like the code exchange).
 *  Returns the (possibly rotated) access token, or null when no usable
 *  credential exists. NEVER logs token material. */
async function freshAccessToken(userId: string): Promise<string | null> {
  const tokens = await getTokens(userId, "supabase");
  if (!tokens) return null;

  const expiresAtMs = tokens.expiresAt ? new Date(tokens.expiresAt).getTime() : 0;
  const stale = !expiresAtMs || expiresAtMs - Date.now() <= REFRESH_WINDOW_MS;
  if (!stale) return tokens.accessToken;

  if (!tokens.refreshToken) {
    // No refresh token — the stored access token is the best we have.
    return tokens.accessToken;
  }

  const connector = getConnector("supabase");
  const creds = connector ? connectorCredentials(connector) : null;
  if (!creds) {
    logger.warn("supabase-mcp: connector OAuth client not configured — using the stored token as-is");
    return tokens.accessToken;
  }

  try {
    const res = await fetch("https://api.supabase.com/v1/oauth/token", {
      method: "POST",
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Accept: "application/json",
        Authorization: `Basic ${Buffer.from(`${creds.clientId}:${creds.clientSecret}`).toString("base64")}`,
      },
      body: new URLSearchParams({
        grant_type: "refresh_token",
        refresh_token: tokens.refreshToken,
      }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!res.ok) {
      const detail = await res.text().catch(() => "");
      logger.warn(
        { status: res.status, detail: detail.slice(0, 160) },
        "supabase-mcp: token refresh failed — trying the stored access token",
      );
      return tokens.accessToken;
    }
    const json = (await res.json()) as {
      access_token?: string;
      refresh_token?: string;
      expires_in?: number;
    };
    if (!json.access_token) return tokens.accessToken;
    await rotateTokens(userId, "supabase", {
      accessToken: json.access_token,
      refreshToken: json.refresh_token || null,
      expiresAt: json.expires_in
        ? new Date(Date.now() + json.expires_in * 1000).toISOString()
        : null,
    }).catch(() => undefined);
    // A rotated token invalidates any cached MCP session binding? No —
    // sessions are keyed by user, but the server binds them to the token;
    // drop the cached session so the next call re-initializes cleanly.
    sessions.delete(userId);
    return json.access_token;
  } catch (err: unknown) {
    logger.warn(
      { err: err instanceof Error ? err.message : "unknown" },
      "supabase-mcp: token refresh threw — trying the stored access token",
    );
    return tokens.accessToken;
  }
}

// ─── JSON-RPC plumbing ──────────────────────────────────────────────────

interface JsonRpcResponse {
  jsonrpc?: string;
  id?: number | string | null;
  result?: unknown;
  error?: { code?: number; message?: string; data?: unknown };
}

/** Parse an MCP HTTP response body: plain JSON or SSE `data:` lines
 *  (take the first data: JSON object). */
function parseMcpBody(contentType: string, text: string): JsonRpcResponse | null {
  const trimmed = text.trim();
  if (!trimmed) return null;
  if (contentType.toLowerCase().includes("text/event-stream")) {
    for (const line of trimmed.split("\n")) {
      if (line.startsWith("data:")) {
        const payload = line.slice(5).trim();
        if (!payload) continue;
        try {
          return JSON.parse(payload) as JsonRpcResponse;
        } catch {
          /* try the next data line */
        }
      }
    }
    return null;
  }
  try {
    return JSON.parse(trimmed) as JsonRpcResponse;
  } catch {
    return null;
  }
}

let rpcId = 0;

/** One MCP HTTP POST. Returns {sessionId?} plus the parsed JSON-RPC body.
 *  The Authorization header is built here and never logged. */
async function mcpPost(
  accessToken: string,
  payload: unknown,
  sessionId?: string,
  timeoutMs = MCP_CALL_TIMEOUT_MS,
): Promise<{ status: number; body: JsonRpcResponse | null; sessionId: string | null }> {
  const res = await fetch(MCP_ENDPOINT, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${accessToken}`,
      "Content-Type": "application/json",
      Accept: "application/json, text/event-stream",
      ...(sessionId ? { "Mcp-Session-Id": sessionId } : {}),
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const contentType = res.headers.get("content-type") || "";
  const text = await res.text().catch(() => "");
  return {
    status: res.status,
    body: parseMcpBody(contentType, text),
    sessionId: res.headers.get("mcp-session-id"),
  };
}

/** Initialize (or re-initialize) the per-user MCP session. Throws on
 *  failure — the caller converts it into an honest error result. */
async function ensureSession(userId: string, accessToken: string): Promise<string> {
  const cached = cachedSession(userId);
  if (cached) return cached;

  const init = await mcpPost(
    accessToken,
    {
      jsonrpc: "2.0",
      id: ++rpcId,
      method: "initialize",
      params: {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: "arcforge", version: "1.0" },
      },
    },
    undefined,
    MCP_INIT_TIMEOUT_MS,
  );
  if (init.status !== 200 || !init.sessionId) {
    throw new Error(
      `MCP initialize failed (HTTP ${init.status}${init.body?.error?.message ? `: ${init.body.error.message}` : ""})`,
    );
  }
  // Initialized notification (no id) — errors here are non-fatal.
  await mcpPost(
    accessToken,
    { jsonrpc: "2.0", method: "notifications/initialized" },
    init.sessionId,
    MCP_INIT_TIMEOUT_MS,
  ).catch(() => undefined);

  sessions.set(userId, { sessionId: init.sessionId, expiresAt: Date.now() + SESSION_TTL_MS });
  return init.sessionId;
}

// ─── Executor ───────────────────────────────────────────────────────────

/** Call one official Supabase MCP tool on the user's behalf.
 *
 * Failure modes are typed so the agent-side tool can react:
 *  • no connection / capability not granted → {ok:false, needs_connector,
 *    capability} — the VM turns this into a request_connector call;
 *  • upstream/protocol errors → {ok:false, error} (honest, sanitized).
 */
export async function callSupabaseMcpTool(
  userId: string,
  tool: string,
  args: Record<string, unknown>,
): Promise<McpToolOutcome> {
  const toolName = typeof tool === "string" ? tool.trim() : "";
  const required = TOOL_CAPABILITY[toolName];
  if (!required) {
    return {
      ok: false,
      error: `unknown supabase MCP tool "${toolName || "(missing)"}" — the supported set: ${Object.keys(TOOL_CAPABILITY).sort().join(", ")}`,
    };
  }

  // 1. Connection + capability grant check (vault row).
  const row = await getConnection(userId, "supabase");
  if (!row || row.status !== "connected") {
    return {
      ok: false,
      error: "supabase is not connected for this account",
      needs_connector: true,
      capability: required,
    };
  }
  const granted = parseGrantedCapabilities(row);
  if (!capabilitySatisfied(granted, required)) {
    return {
      ok: false,
      error: `this supabase connection was granted only [${(granted ?? []).join(", ")}] — the tool "${toolName}" needs ${required}`,
      needs_connector: true,
      capability: required,
    };
  }

  // 2. Fresh token (refresh-on-use).
  const accessToken = await freshAccessToken(userId);
  if (!accessToken) {
    return {
      ok: false,
      error: "supabase is not connected for this account (no stored credential)",
      needs_connector: true,
      capability: required,
    };
  }

  // 3. Session + tools/call (one re-initialize retry on session loss).
  for (let attempt = 1; attempt <= 2; attempt++) {
    let sessionId: string;
    try {
      sessionId = await ensureSession(userId, accessToken);
    } catch (err: unknown) {
      return {
        ok: false,
        error: `MCP session could not be established: ${err instanceof Error ? err.message : "unknown"}`,
      };
    }
    const call = await mcpPost(
      accessToken,
      {
        jsonrpc: "2.0",
        id: ++rpcId,
        method: "tools/call",
        params: {
          name: toolName,
          arguments: args && typeof args === "object" ? args : {},
        },
      },
      sessionId,
    );

    if (call.status === 404 || call.status === 400) {
      // Session expired/invalid server-side → drop the cache and retry
      // once with a fresh initialize (400 is what the server returns for
      // an unknown Mcp-Session-Id on some versions).
      sessions.delete(userId);
      if (attempt === 1) continue;
    }

    if (call.status !== 200) {
      return {
        ok: false,
        error: `MCP server HTTP ${call.status}${call.body?.error?.message ? `: ${call.body.error.message}` : ""}`,
      };
    }
    if (call.body?.error) {
      return { ok: false, error: `MCP error: ${call.body.error.message ?? "unknown"}` };
    }

    const result = call.body?.result as
      | { content?: McpContentBlock[]; isError?: boolean }
      | undefined;
    if (!result || !Array.isArray(result.content)) {
      return { ok: false, error: "MCP server returned no content blocks" };
    }
    // Sanitize + truncate — no credential material is expected from the
    // official server, and the sizes stay bounded for the tunnel.
    const content = result.content.slice(0, 20).map((block) => ({
      type: typeof block?.type === "string" ? block.type : "text",
      text:
        typeof block?.text === "string"
          ? block.text.length > MAX_CONTENT_CHARS
            ? `${block.text.slice(0, MAX_CONTENT_CHARS)}…[truncated]`
            : block.text
          : undefined,
    }));
    if (result.isError) {
      const text = content.map((c) => c.text ?? "").join("\n").slice(0, 800);
      return { ok: false, error: text || "the MCP tool reported an error" };
    }
    logger.info({ userId, tool: toolName }, "supabase-mcp: tool call ok");
    return { ok: true, result: { content } };
  }
  return { ok: false, error: "MCP session could not be re-established" };
}

// ─── Tunnel branch (`/mcp/<connector>`) ─────────────────────────────────

export interface McpTunnelConn {
  sandboxId: string;
}

export interface McpTunnelFrame {
  id?: string;
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string;
}

/** A complete JSON result envelope: head + one body chunk. */
async function* jsonEvents(
  status: number,
  payload: unknown,
): AsyncGenerator<McpForwardEvent, void, void> {
  yield { kind: "head", status, headers: { "content-type": "application/json" } };
  yield { kind: "chunk", body: JSON.stringify(payload) };
}

/**
 * Handle one `/mcp/<connector>` req frame (mirrors handleGithubTunnel in
 * services/github-proxy.ts): resolve sandbox → project → user, parse
 * {tool, args} from the frame body, execute via the vault-backed MCP
 * client, and yield head/chunk events.
 *
 * Status mapping: 200 ok · 403 needs_connector / no-owner · 400 bad tool.
 * The inbound Authorization header (if any slipped through the VM-side
 * client's stripping) is IGNORED — the vault token is injected
 * server-side only, inside callSupabaseMcpTool.
 */
export async function* handleMcpTunnel(
  conn: McpTunnelConn,
  frame: McpTunnelFrame,
): AsyncGenerator<McpForwardEvent, void, void> {
  // Connector = path segment after /mcp/ (query string already stripped
  // by the caller; be defensive anyway).
  const path = (frame.path || "").split("?")[0];
  const connector = path.replace(/^\/mcp\//, "").replace(/\/.*$/, "").trim();

  if (connector !== "supabase") {
    yield* jsonEvents(400, {
      ok: false,
      error: `unsupported MCP connector "${connector || "(missing)"}" — only "supabase" is available (/mcp/supabase)`,
    });
    return;
  }

  let parsed: { tool?: unknown; args?: unknown };
  try {
    parsed = JSON.parse(frame.body || "{}") as { tool?: unknown; args?: unknown };
  } catch {
    yield* jsonEvents(400, {
      ok: false,
      error: "invalid /mcp frame: body must be JSON {tool, args}",
    });
    return;
  }
  const tool = typeof parsed.tool === "string" ? parsed.tool : "";
  const args =
    parsed.args && typeof parsed.args === "object" && !Array.isArray(parsed.args)
      ? (parsed.args as Record<string, unknown>)
      : {};

  if (!tool) {
    yield* jsonEvents(400, { ok: false, error: "the mcp tool name is required ({tool, args})" });
    return;
  }

  // Sandbox → project → owning user. No owner → honest 403.
  let userId: string;
  try {
    const project = await getProjectRowBySandbox(conn.sandboxId);
    if (!project) {
      yield* jsonEvents(403, { ok: false, error: "no project owner for this sandbox" });
      return;
    }
    userId = project.user_id;
  } catch (err: unknown) {
    logger.warn(
      { sandboxId: conn.sandboxId, err: err instanceof Error ? err.message : err },
      "supabase-mcp: sandbox→project lookup failed",
    );
    yield* jsonEvents(403, { ok: false, error: "no project owner for this sandbox" });
    return;
  }

  try {
    const outcome = await callSupabaseMcpTool(userId, tool, args);
    if (outcome.ok) {
      yield* jsonEvents(200, { ok: true, result: outcome.result });
      return;
    }
    yield* jsonEvents(outcome.needs_connector ? 403 : 400, {
      ok: false,
      error: outcome.error,
      ...(outcome.needs_connector ? { needs_connector: true, capability: outcome.capability } : {}),
    });
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "mcp tunnel failure";
    logger.warn({ sandboxId: conn.sandboxId, err: message }, "supabase-mcp: request failed");
    yield* jsonEvents(500, { ok: false, error: `mcp tunnel failure: ${message}` });
  }
}
