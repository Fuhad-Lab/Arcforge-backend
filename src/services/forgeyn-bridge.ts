/**
 * Forgeyn Base bridge — the 1.0-lane tunnel executor.
 *
 * The in-VM orchestrator's `forgeyn_base` tool (daytona-service/app/
 * agent_sidecar/orchestrator.py) bridges through the reverse tunnel to
 * this backend; the `/mcp/forgeyn` tunnel branch (routes/tunnel.ts +
 * services/reverse-tunnel-client.ts — the intentionally-duplicated pair)
 * resolves sandbox → project → user and proxies to the `forge-ops` Supabase
 * edge function, which owns the whole master-key provisioning flow:
 *
 *   {op: "create_database", organization, project, password?}
 *     → the user's Forgeyn Base account is created/connected (master key),
 *       the organization + project (a private Neon Postgres) provision,
 *       and the connection is vaulted (forgeyn_connections).
 *   {op: "run_sql" | "list_tables" | "list_projects" | "create_app" |
 *        "list_apps" | "whoami", args}
 *     → operations on the user's CONNECTED database (vaulted API key).
 *
 * WHY A PROXY, not a second implementation: the edge function is the
 * single source of truth for the provisioning logic, the vault, and the
 * master key. This module only resolves the caller's identity (the same
 * trust chain the supabase-mcp executor uses) and relays the call with
 * the engine relay key — zero duplicated credentials logic to drift.
 *
 * SECURITY
 * ────────
 * The vaulted password/API key never enters the VM: the edge function
 * holds them server-side and returns sanitized results only. The relay
 * key travels backend→edge (server-to-server), never through the VM.
 */
import { logger } from "../lib/logger";
import { getProjectRowBySandbox } from "../lib/project-lookup";

// ─── Config ──────────────────────────────────────────────────────────────

const FORGEYN_EDGE_URL = (process.env.FORGEYN_EDGE_URL || "").replace(/\/+$/, "");
const FORGEYN_RELAY_KEY = (process.env.FORGEYN_RELAY_KEY || "").trim();

/** Max chars of the JSON result returned to the VM (bounded, like the
 *  supabase-mcp executor's truncation). */
const MAX_RESULT_CHARS = 12_000;

export interface ForgeynTunnelConn {
  sandboxId: string;
}

export interface ForgeynTunnelFrame {
  id?: string;
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string;
}

export interface ForgeynForwardEvent {
  kind: "head" | "chunk";
  status?: number;
  headers?: Record<string, string>;
  body?: string;
}

/** A complete JSON result envelope: head + one body chunk. */
async function* jsonEvents(
  status: number,
  payload: unknown,
): AsyncGenerator<ForgeynForwardEvent, void, void> {
  yield { kind: "head", status, headers: { "content-type": "application/json" } };
  yield { kind: "chunk", body: JSON.stringify(payload) };
}

/** True when the bridge is configured (honest needs-config result otherwise). */
function bridgeConfigured(): boolean {
  return Boolean(FORGEYN_EDGE_URL && FORGEYN_RELAY_KEY);
}

/** One call into the forge-ops edge function (server-to-server relay). */
async function callForgeOps(
  payload: Record<string, unknown>,
): Promise<{ status: number; data: Record<string, unknown> | null }> {
  const res = await fetch(`${FORGEYN_EDGE_URL}/forge-ops`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      "X-Engine-Relay-Key": FORGEYN_RELAY_KEY,
    },
    body: JSON.stringify(payload),
    signal: AbortSignal.timeout(180_000),
  });
  const text = await res.text();
  let data: Record<string, unknown> | null = null;
  try {
    data = text ? (JSON.parse(text) as Record<string, unknown>) : null;
  } catch {
    data = { ok: false, error: `forge-ops returned a non-JSON body (HTTP ${res.status})` };
  }
  return { status: res.status, data };
}

/**
 * Handle one `/mcp/forgeyn` req frame (mirrors handleMcpTunnel for
 * supabase): resolve sandbox → project → user, parse {op, args} from the
 * frame body, relay through the forge-ops edge function, yield head/chunk.
 *
 * Status mapping: 200 ok (incl. honest needs_credentials) · 403 no-owner ·
 * 400 bad frame · 503 bridge not configured.
 */
export async function* handleForgeynTunnel(
  conn: ForgeynTunnelConn,
  frame: ForgeynTunnelFrame,
): AsyncGenerator<ForgeynForwardEvent, void, void> {
  let parsed: { op?: unknown; tool?: unknown; args?: unknown; organization?: unknown; project?: unknown; password?: unknown };
  try {
    parsed = JSON.parse(frame.body || "{}");
  } catch {
    yield* jsonEvents(400, {
      ok: false,
      error: "invalid /mcp/forgeyn frame: body must be JSON {op, args}",
    });
    return;
  }

  const op = typeof parsed.op === "string" ? parsed.op : typeof parsed.tool === "string" ? parsed.tool : "";
  if (!op) {
    yield* jsonEvents(400, {
      ok: false,
      error: 'the operation is required — {op: "create_database", organization, project} or {op: "run_sql"|"list_tables"|…, args}',
    });
    return;
  }

  if (!bridgeConfigured()) {
    yield* jsonEvents(503, {
      ok: false,
      error: "the Forgeyn Base bridge is not configured on the backend (FORGEYN_EDGE_URL + FORGEYN_RELAY_KEY)",
    });
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
      "forgeyn-bridge: sandbox→project lookup failed",
    );
    yield* jsonEvents(403, { ok: false, error: "no project owner for this sandbox" });
    return;
  }

  try {
    let payload: Record<string, unknown>;
    if (op === "create_database") {
      payload = {
        action: "create-database",
        userId,
        organization: typeof parsed.organization === "string" ? parsed.organization : "",
        project: typeof parsed.project === "string" ? parsed.project : "",
        ...(typeof parsed.password === "string" && parsed.password ? { password: parsed.password } : {}),
      };
    } else {
      const args =
        parsed.args && typeof parsed.args === "object" && !Array.isArray(parsed.args)
          ? (parsed.args as Record<string, unknown>)
          : {};
      payload = { action: "op", userId, tool: op, args };
    }

    const { status, data } = await callForgeOps(payload);

    // The edge function's honest outcomes (needs_credentials, needs_database,
    // ok:false errors) pass through verbatim — the orchestrator's tool
    // wrapper turns them into model-readable guidance.
    let body = JSON.stringify(data ?? { ok: false, error: `forge-ops HTTP ${status}` });
    if (body.length > MAX_RESULT_CHARS) {
      body = body.slice(0, MAX_RESULT_CHARS);
    }
    yield { kind: "head", status: 200, headers: { "content-type": "application/json" } };
    yield { kind: "chunk", body };
  } catch (err: unknown) {
    const message = err instanceof Error ? err.message : "forgeyn tunnel failure";
    logger.warn({ sandboxId: conn.sandboxId, err: message }, "forgeyn-bridge: request failed");
    yield* jsonEvents(500, { ok: false, error: `forgeyn tunnel failure: ${message}` });
  }
}
