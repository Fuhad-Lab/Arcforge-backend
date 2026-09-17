/**
 * MCP OAuth 2.1 proxy — user-added external MCP servers.
 *
 * This module is the platform's multi-tenant, in-process equivalent of the
 * open-source "mcp-oauth2-proxy" companion service: it fronts every
 * user-added remote MCP server and owns the complete OAuth 2.1 + PKCE
 * lifecycle so no auth logic leaks into the core backend routes —
 *
 *   • DYNAMIC DISCOVERY (MCP spec / RFC 9728 + RFC 8414):
 *     /.well-known/oauth-protected-resource → authorization_servers →
 *     /.well-known/oauth-authorization-server → endpoints + scopes.
 *   • DYNAMIC CLIENT REGISTRATION (RFC 7591): the platform registers ITSELF
 *     with the target server's authorization server at connect time — no
 *     hardcoded client credentials per provider.
 *   • AUTHORIZATION CODE + PKCE (S256) — the MCP specification's grant.
 *   • TOKEN EXCHANGE + REFRESH-ON-USE: tokens are AES-256-GCM encrypted in
 *     the existing connector vault (connector_id = "mcp:<uuid>"), silently
 *     refreshed when near expiry.
 *   • BEARER-INJECTED JSON-RPC EXECUTOR: initialize → notifications/
 *     initialized → tools/list → tools/call with `Authorization: Bearer
 *     <token>` added server-side ONLY. The clean tool schema passes back
 *     to the AI; the token never reaches the browser, the VM, or logs.
 *
 * UPSTREAM_URL (the guide's env var) is per-user here: each mcp_servers
 * row is one proxy instance. The redirect target is the same connector-ops
 * edge function the fixed connectors use — the browser round-trip is
 * identical, only the token exchange is PKCE-bound.
 *
 * Server rows: public.mcp_servers (RLS on, zero policies — service-role
 * only). Tokens: public.connector_connections via connector-vault.
 */
import crypto from "node:crypto";
import dns from "node:dns/promises";
import { logger } from "../lib/logger";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";
import { getProjectRowBySandbox } from "../lib/project-lookup";
import {
  decryptToken,
  encryptToken,
  expiryDate,
  getTokens,
  mintState,
  rotateTokens,
  upsertConnection,
  type OAuthState,
} from "./connector-vault";

// ─── Constants ──────────────────────────────────────────────────────────

const EDGE_BASE = (process.env.EDGE_FUNCTION_BASE_URL || "").replace(/\/+$/, "");
const PROXY_CLIENT_NAME = process.env.MCP_PROXY_CLIENT_NAME || "Forgeyn";

const DISCOVERY_TIMEOUT_MS = 12_000;
const REGISTRATION_TIMEOUT_MS = 12_000;
const TOKEN_TIMEOUT_MS = 20_000;
const MCP_INIT_TIMEOUT_MS = 30_000;
const MCP_CALL_TIMEOUT_MS = 120_000;
const MCP_LIST_TIMEOUT_MS = 45_000;
/** Refresh the access token when it expires within this window. */
const REFRESH_WINDOW_MS = 5 * 60 * 1000;
/** Session cache TTL — re-initialize before the server expires us. */
const SESSION_TTL_MS = 50 * 60 * 1000;
const MCP_PROTOCOL_VERSION = "2025-03-26";
const MAX_CONTENT_CHARS = 6000;
const MAX_TOOLS_CACHED = 200;

// ─── Errors ─────────────────────────────────────────────────────────────

/** Machine-readable failure codes — mirrored by the frontend copy. */
export type McpErrorCode =
  | "invalid_url"
  | "unreachable"
  | "auth_discovery_failed"
  | "registration_failed"
  | "registration_unsupported"
  | "authorize_rejected"
  | "key_rejected"
  | "exchange_failed"
  | "no_token"
  | "internal";

export class McpError extends Error {
  code: McpErrorCode;
  detail?: string;
  constructor(code: McpErrorCode, message: string, detail?: string) {
    super(message);
    this.code = code;
    this.detail = detail;
  }
}

// ─── SSRF guard ─────────────────────────────────────────────────────────

/** Validate + normalize a user-supplied MCP server URL. https only (the
 *  guide's UPSTREAM_URL model), standard ports, and the host must not
 *  resolve into private/loopback/link-local space — the backend fetches
 *  this URL server-side on behalf of a platform user. */
export async function normalizeServerUrl(raw: string): Promise<URL> {
  const trimmed = (raw || "").trim();
  let url: URL;
  try {
    url = new URL(trimmed);
  } catch {
    throw new McpError("invalid_url", "That does not look like a valid URL.");
  }
  if (url.protocol !== "https:") {
    throw new McpError(
      "invalid_url",
      "Only https:// MCP server URLs are supported (the URL must be a remote streamable-HTTP server).",
    );
  }
  if (url.port && url.port !== "443") {
    throw new McpError("invalid_url", "Only the standard https port is supported.");
  }
  if (url.username || url.password) {
    throw new McpError("invalid_url", "URLs with embedded credentials are not supported.");
  }
  // NOTE (user fix 2026-09-16): the root path IS a valid MCP endpoint —
  // Vercel's hosted MCP server lives at https://mcp.vercel.com/ exactly.
  // The old "include the full endpoint path" rejection made Vercel
  // un-addable; only genuinely malformed URLs are rejected now.
  const host = url.hostname.toLowerCase().replace(/\.$/, "");
  if (host === "localhost" || host.endsWith(".localhost") || host.endsWith(".internal") || host.endsWith(".local")) {
    throw new McpError("invalid_url", "Local and internal hostnames cannot be added.");
  }
  // Resolve every address the host maps to and refuse private ranges.
  try {
    const records = await dns.lookup(host, { all: true });
    for (const r of records) {
      const ip = r.address;
      const parts = ip.split(".").map(Number);
      const isV4Private =
        parts.length === 4 &&
        (parts[0] === 10 ||
          parts[0] === 127 ||
          (parts[0] === 192 && parts[1] === 168) ||
          (parts[0] === 172 && parts[1] >= 16 && parts[1] <= 31) ||
          (parts[0] === 169 && parts[1] === 254) ||
          parts[0] === 0);
      const isV6Private =
        ip === "::1" || ip.startsWith("fe80:") || ip.startsWith("fc") || ip.startsWith("fd") || ip === "::";
      if (isV4Private || isV6Private) {
        throw new McpError("invalid_url", "That host resolves to a private network address.");
      }
    }
  } catch (err) {
    if (err instanceof McpError) throw err;
    throw new McpError("unreachable", `The host "${host}" could not be resolved.`);
  }
  url.hash = "";
  return url;
}

// ─── DB rows ────────────────────────────────────────────────────────────

export interface McpServerRow {
  id: string;
  user_id: string;
  server_url: string;
  label: string | null;
  status: string;
  auth_required: boolean;
  /** How the stored credential was obtained: "oauth" (PKCE round-trip),
   *  "api_key" (user-pasted bearer key — never expires, never refreshes),
   *  NULL for open servers (user fix 2026-09-16: the Render fallback). */
  auth_mode: string | null;
  authorization_endpoint: string | null;
  token_endpoint: string | null;
  registration_endpoint: string | null;
  revocation_endpoint: string | null;
  scopes: string | null;
  client_id_enc: string | null;
  client_secret_enc: string | null;
  server_name: string | null;
  server_version: string | null;
  tool_count: number | null;
  tools_cache: unknown;
  account_label: string | null;
  connected_at: string | null;
  created_at: string | null;
}

function serversTable() {
  if (!isSupabaseConfigured()) throw new McpError("internal", "Storage is not configured.");
  return getServiceSupabase().from("mcp_servers");
}

export async function getServerRow(userId: string, serverId: string): Promise<McpServerRow | null> {
  const { data, error } = await serversTable()
    .select("*")
    .eq("user_id", userId)
    .eq("id", serverId)
    .maybeSingle();
  if (error) {
    logger.error({ err: error.message, serverId }, "mcp-proxy: row read failed");
    return null;
  }
  return (data as McpServerRow) ?? null;
}

async function getServerRowByUrl(userId: string, serverUrl: string): Promise<McpServerRow | null> {
  const { data, error } = await serversTable()
    .select("*")
    .eq("user_id", userId)
    .eq("server_url", serverUrl)
    .maybeSingle();
  if (error) {
    logger.error({ err: error.message }, "mcp-proxy: row read-by-url failed");
    return null;
  }
  return (data as McpServerRow) ?? null;
}

async function upsertServerRow(values: Partial<McpServerRow> & { user_id: string; server_url: string }) {
  const { error } = await getServiceSupabase().from("mcp_servers").upsert(
    { ...values, updated_at: new Date().toISOString() },
    { onConflict: "user_id,server_url" },
  );
  if (error) {
    logger.error({ err: error.message }, "mcp-proxy: row upsert failed");
    throw new McpError("internal", "Could not save the MCP server.");
  }
}

export async function deleteServerRow(userId: string, serverId: string): Promise<void> {
  await serversTable().delete().eq("user_id", userId).eq("id", serverId);
}

function hostLabel(serverUrl: string): string {
  try {
    return new URL(serverUrl).hostname;
  } catch {
    return serverUrl.slice(0, 60);
  }
}

// ─── JSON-RPC transport (Bearer injected server-side) ───────────────────

interface JsonRpcResponse {
  jsonrpc?: string;
  id?: number | string | null;
  result?: unknown;
  error?: { code?: number; message?: string; data?: unknown };
}

function parseRpcBody(contentType: string, text: string): JsonRpcResponse | null {
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
          /* next data line */
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

/** One MCP HTTP POST. The Authorization header is built HERE and never
 *  logged — this is the proxy's interception point. */
async function rpcPost(
  serverUrl: string,
  accessToken: string | null,
  payload: unknown,
  sessionId?: string,
  timeoutMs = MCP_CALL_TIMEOUT_MS,
): Promise<{ status: number; body: JsonRpcResponse | null; sessionId: string | null; wwwAuthenticate: string | null }> {
  const res = await fetch(serverUrl, {
    method: "POST",
    headers: {
      ...(accessToken ? { Authorization: `Bearer ${accessToken}` } : {}),
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
    body: parseRpcBody(contentType, text),
    sessionId: res.headers.get("mcp-session-id"),
    wwwAuthenticate: res.headers.get("www-authenticate"),
  };
}

// ─── OAuth discovery (RFC 9728 → RFC 8414) ──────────────────────────────

export interface McpAuthMetadata {
  authRequired: true;
  /** The authorization server issuer. */
  issuer: string;
  authorizationEndpoint: string;
  tokenEndpoint: string;
  registrationEndpoint: string | null;
  revocationEndpoint: string | null;
  scopesSupported: string | null;
}

async function fetchWellKnown(url: string): Promise<Record<string, unknown> | null> {
  try {
    const res = await fetch(url, {
      headers: { Accept: "application/json", "User-Agent": "arcforge-mcp-proxy" },
      signal: AbortSignal.timeout(DISCOVERY_TIMEOUT_MS),
    });
    if (!res.ok) return null;
    const json = (await res.json().catch(() => null)) as Record<string, unknown> | null;
    return json && typeof json === "object" ? json : null;
  } catch {
    return null;
  }
}

/** RFC 9728 protected-resource metadata for the MCP server URL. Tries the
 *  path-form well-known first (2025-06-18 MCP revision), then the root
 *  form, and accepts the 401 WWW-Authenticate header as a fallback hint. */
async function protectedResource(server: URL): Promise<string[] | null> {
  const origin = server.origin;
  const path = server.pathname.replace(/\/+$/, "");
  const candidates = path && path !== "/"
    ? [
        `${origin}/.well-known/oauth-protected-resource${path}`,
        `${origin}/.well-known/oauth-protected-resource`,
      ]
    : [`${origin}/.well-known/oauth-protected-resource`];
  for (const candidate of candidates) {
    const meta = await fetchWellKnown(candidate);
    if (meta && Array.isArray(meta.authorization_servers)) {
      const servers = (meta.authorization_servers as unknown[]).filter(
        (s): s is string => typeof s === "string" && /^https?:\/\//.test(s),
      );
      if (servers.length > 0) return servers;
    }
  }
  return null;
}

/** RFC 8414 authorization-server metadata for an issuer. */
async function authorizationServer(issuer: string): Promise<Record<string, unknown> | null> {
  const trimmed = issuer.replace(/\/+$/, "");
  let candidates: string[];
  try {
    const u = new URL(trimmed);
    const path = u.pathname.replace(/\/+$/, "");
    candidates =
      path && path !== "/"
        ? [`${trimmed}/.well-known/oauth-authorization-server`, `${u.origin}/.well-known/oauth-authorization-server${path}`]
        : [`${u.origin}/.well-known/oauth-authorization-server`];
  } catch {
    return null;
  }
  for (const candidate of candidates) {
    const meta = await fetchWellKnown(candidate);
    if (meta && typeof meta.authorization_endpoint === "string" && typeof meta.token_endpoint === "string") {
      return meta;
    }
  }
  return null;
}

/** Build the normalized auth metadata from a raw AS document. */
function asMetadata(issuer: string, as: Record<string, unknown>): McpAuthMetadata {
  const scopes = Array.isArray(as.scopes_supported)
    ? (as.scopes_supported as unknown[]).filter((s): s is string => typeof s === "string").join(" ")
    : null;
  return {
    authRequired: true,
    issuer,
    authorizationEndpoint: as.authorization_endpoint as string,
    tokenEndpoint: as.token_endpoint as string,
    registrationEndpoint: typeof as.registration_endpoint === "string" ? as.registration_endpoint : null,
    revocationEndpoint: typeof as.revocation_endpoint === "string" ? as.revocation_endpoint : null,
    scopesSupported: scopes && scopes.length > 0 ? scopes.slice(0, 400) : null,
  };
}

/** Does this AS document accept PUBLIC clients (token_endpoint_auth_method
 *  "none")? Allowlisted MCP providers (Vercel, Render) publish a separate
 *  MCP-facing authorization-server document that supports public clients +
 *  PKCE, while their general web-integration metadata only lists
 *  client_secret_* methods — the MCP one is the one this proxy must use. */
function supportsPublicClients(as: Record<string, unknown>): boolean {
  return (
    Array.isArray(as.token_endpoint_auth_methods_supported) &&
    (as.token_endpoint_auth_methods_supported as unknown[]).includes("none")
  );
}

/** AS metadata published on the MCP server's own origin (both the root
 *  well-known form and the path-inserted form) — Vercel's MCP-facing
 *  document lives here. Returns [] when the origin publishes none. */
async function serverOriginAuthServers(server: URL): Promise<Array<{ issuer: string; as: Record<string, unknown> }>> {
  const origin = server.origin;
  const path = server.pathname.replace(/\/+$/, "");
  const candidates =
    path && path !== "/"
      ? [
          `${origin}/.well-known/oauth-authorization-server${path}`,
          `${origin}/.well-known/oauth-authorization-server`,
        ]
      : [`${origin}/.well-known/oauth-authorization-server`];
  const found: Array<{ issuer: string; as: Record<string, unknown> }> = [];
  for (const candidate of candidates) {
    const meta = await fetchWellKnown(candidate);
    if (
      meta &&
      typeof meta.authorization_endpoint === "string" &&
      typeof meta.token_endpoint === "string" &&
      !found.some((f) => f.as.authorization_endpoint === meta.authorization_endpoint)
    ) {
      found.push({ issuer: origin, as: meta });
    }
  }
  return found;
}

/**
 * Discover whether an MCP server needs OAuth and, if so, its complete
 * authorization-server metadata.
 *
 * Probe order (per the MCP authorization spec):
 *  1. A bare JSON-RPC initialize — 2xx means the server is open (no auth).
 *  2. 401 → RFC 9728 protected-resource metadata → RFC 8414 AS metadata.
 *  3. 401 with a WWW-Authenticate resource hint → use it.
 */
export async function discoverMcpAuth(server: URL): Promise<McpAuthMetadata | { authRequired: false }> {
  // 1. Open-server probe.
  let probeWwwAuth: string | null = null;
  let probeStatus = 0;
  try {
    const probe = await rpcPost(
      server.toString(),
      null,
      {
        jsonrpc: "2.0",
        id: ++rpcId,
        method: "initialize",
        params: {
          protocolVersion: MCP_PROTOCOL_VERSION,
          capabilities: {},
          clientInfo: { name: "arcforge-mcp-proxy", version: "1.0" },
        },
      },
      undefined,
      MCP_INIT_TIMEOUT_MS,
    );
    probeStatus = probe.status;
    probeWwwAuth = probe.wwwAuthenticate;
    if (probe.status === 200) return { authRequired: false };
  } catch (err) {
    throw new McpError(
      "unreachable",
      "The MCP server did not respond.",
      err instanceof Error ? err.message : undefined,
    );
  }

  if (probeStatus !== 401 && probeStatus !== 403) {
    throw new McpError(
      "auth_discovery_failed",
      `The server answered HTTP ${probeStatus} to an unauthenticated MCP initialize — it does not behave like a streamable-HTTP MCP server.`,
    );
  }

  // 2/3. Discover the authorization server.
  // RFC 9728 §5.2: the 401's WWW-Authenticate header carries
  // resource_metadata="<direct URL of the protected-resource document>" —
  // fetch THAT document directly (live-verified against mcp.notion.com
  // and mcp.supabase.com), and only fall back to the well-known path
  // forms on the server's own origin.
  let issuers: string[] | null = null;
  if (probeWwwAuth) {
    const match = probeWwwAuth.match(/resource_metadata[^=]*=\s*"?([^",\s]+)"?/i);
    if (match) {
      const meta = await fetchWellKnown(new URL(match[1], server.origin).toString());
      const servers =
        meta && Array.isArray(meta.authorization_servers)
          ? (meta.authorization_servers as unknown[]).filter(
              (s): s is string => typeof s === "string" && /^https?:\/\//.test(s),
            )
          : [];
      if (servers.length > 0) issuers = servers;
    }
  }
  if (!issuers) issuers = await protectedResource(server);
  if (!issuers) {
    throw new McpError(
      "auth_discovery_failed",
      "This server requires authorization but exposes no OAuth discovery metadata (RFC 9728) — it cannot be connected automatically.",
    );
  }

  const candidates: Array<{ issuer: string; as: Record<string, unknown> }> = [];
  for (const issuer of issuers.slice(0, 3)) {
    const as = await authorizationServer(issuer);
    if (as) candidates.push({ issuer, as });
  }
  // USER FIX (2026-09-16, the Vercel invalid_client incident): some
  // providers (Vercel) publish their MCP-facing AS metadata on the MCP
  // server's own origin (mcp.vercel.com/.well-known/oauth-authorization-
  // server) while the RFC 9728 authorization_servers entry (vercel.com)
  // serves the web-integration document — confidential clients only, a
  // different token endpoint, no registration. Also probe the server's
  // own origin and PREFER any document that accepts public clients.
  for (const candidate of await serverOriginAuthServers(server)) {
    if (!candidates.some((c) => c.as.authorization_endpoint === candidate.as.authorization_endpoint && c.as.token_endpoint === candidate.as.token_endpoint)) {
      candidates.push(candidate);
    }
  }
  if (candidates.length === 0) {
    throw new McpError(
      "auth_discovery_failed",
      "The server's authorization server metadata could not be read (RFC 8414).",
    );
  }
  const preferred = candidates.find((c) => supportsPublicClients(c.as)) ?? candidates[0];
  if (preferred !== candidates[0]) {
    logger.info(
      { server: server.hostname, issuer: preferred.issuer },
      "mcp-proxy: chose the public-client authorization-server document",
    );
  }
  return asMetadata(preferred.issuer, preferred.as);
}

// ─── Pre-registered client directory (allowlisted providers) ───────────

/** USER FIX (2026-09-16, the invalid_client incident): major cloud
 *  providers (Vercel, Render) protect their hosted MCP routes with OAuth
 *  CLIENT ALLOWLISTS — unrecognized client ids are rejected with
 *  {"error":"invalid_client"} no matter how correct the PKCE handshake is.
 *  Forgeyn therefore registers itself with the provider's partner dashboard
 *  beforehand and the resulting client id is resolved HERE, before any
 *  RFC 7591 dynamic registration or platform-URL fallback runs.
 *
 *  Resolution order per server host:
 *   1. Environment overrides — the services that need them carry the ids
 *      as env vars (VERCEL_CLIENT_ID, RENDER_MCP_CLIENT_ID) so nothing is
 *      hardcoded in the repo.
 *   2. The platform-owned `public.mcp_client_registry` table (RLS on, zero
 *      policies — service-role only): server_host → client_id (+ optional
 *      encrypted secret for future confidential clients). Editable without
 *      a redeploy; this is where the provisioned production client keys
 *      live (Vercel's cl_… today, Render's when their approval lands).
 */
const ENV_CLIENT_BY_HOST: Array<{ host: string; envVar: string }> = [
  { host: "mcp.vercel.com", envVar: "VERCEL_CLIENT_ID" },
  { host: "mcp.render.com", envVar: "RENDER_MCP_CLIENT_ID" },
];

interface DirectoryClientRow {
  server_host: string;
  client_id: string;
  client_secret_enc: string | null;
}

async function lookupRegisteredClient(serverUrl: string): Promise<RegisteredClient | null> {
  let host = "";
  try {
    host = new URL(serverUrl).hostname.toLowerCase();
  } catch {
    return null;
  }
  // 1. Env override (the service that runs this proxy).
  const envEntry = ENV_CLIENT_BY_HOST.find((e) => e.host === host);
  if (envEntry) {
    const clientId = process.env[envEntry.envVar] || "";
    if (clientId) {
      logger.info({ host, envVar: envEntry.envVar }, "mcp-proxy: using the pre-registered OAuth client (env)");
      return { clientId, clientSecret: null };
    }
  }
  // 2. Platform client registry (DB).
  if (isSupabaseConfigured()) {
    try {
      const { data, error } = await getServiceSupabase()
        .from("mcp_client_registry")
        .select("server_host,client_id,client_secret_enc")
        .eq("server_host", host)
        .maybeSingle();
      if (!error && data) {
        const row = data as DirectoryClientRow;
        const secret = row.client_secret_enc ? decryptToken(row.client_secret_enc) : null;
        logger.info({ host }, "mcp-proxy: using the pre-registered OAuth client (registry)");
        return { clientId: row.client_id, clientSecret: secret };
      }
    } catch (err) {
      logger.warn(
        { host, err: err instanceof Error ? err.message : "unknown" },
        "mcp-proxy: client registry lookup failed — continuing to dynamic registration",
      );
    }
  }
  return null;
}

/** The platform's registered OAuth callback — the SAME edge function the
 *  fixed connectors use (connector-ops relays ?code&state to the backend). */
export function mcpRedirectUri(): string {
  if (!EDGE_BASE) {
    throw new McpError(
      "internal",
      "EDGE_FUNCTION_BASE_URL is not configured on the server — the OAuth callback cannot be registered.",
    );
  }
  return `${EDGE_BASE}/connector-ops`;
}

export interface RegisteredClient {
  clientId: string;
  clientSecret: string | null;
}

/** Register this platform as an OAuth client with the target authorization
 *  server. Pre-registered (allowlisted) clients win FIRST — Vercel/Render
 *  reject unknown clients with invalid_client outright, so the directory
 *  entry (env → mcp_client_registry) is checked before anything else.
 *  Falls back to RFC 7591 dynamic registration when the AS offers it, and
 *  finally to the public-client identity (client_id = the platform URL)
 *  for servers that accept any public client + PKCE. */
export async function registerMcpClient(
  meta: McpAuthMetadata,
  serverUrl: string,
): Promise<RegisteredClient> {
  // 1. Pre-registered directory client (env → DB registry). No
  //  registration call — the provider's allowlist already knows this
  //  client, and a self-registered one would be rejected anyway.
  const registered = await lookupRegisteredClient(serverUrl);
  if (registered) return registered;

  if (!meta.registrationEndpoint) {
    // Per modern MCP OAuth 2.1 practice the client id can be the platform
    // URL — works with servers that accept any public client + PKCE.
    return { clientId: "https://forgeyn.com.ng", clientSecret: null };
  }
  try {
    const res = await fetch(meta.registrationEndpoint, {
      method: "POST",
      headers: { "Content-Type": "application/json", Accept: "application/json" },
      body: JSON.stringify({
        client_name: PROXY_CLIENT_NAME,
        redirect_uris: [mcpRedirectUri()],
        grant_types: ["authorization_code", "refresh_token"],
        response_types: ["code"],
        token_endpoint_auth_method: "none",
        client_type: "public",
      }),
      signal: AbortSignal.timeout(REGISTRATION_TIMEOUT_MS),
    });
    if (!res.ok) {
      const detail = (await res.text().catch(() => "")).slice(0, 300);
      throw new McpError(
        "registration_failed",
        "The server's authorization server rejected the client registration.",
        `HTTP ${res.status}: ${detail}`,
      );
    }
    const json = (await res.json().catch(() => null)) as { client_id?: string; client_secret?: string } | null;
    if (!json || typeof json.client_id !== "string" || !json.client_id) {
      throw new McpError("registration_failed", "The registration response carried no client_id.");
    }
    return {
      clientId: json.client_id,
      clientSecret: typeof json.client_secret === "string" && json.client_secret ? json.client_secret : null,
    };
  } catch (err) {
    if (err instanceof McpError) throw err;
    throw new McpError(
      "registration_failed",
      "The dynamic client registration endpoint was unreachable.",
      err instanceof Error ? err.message : undefined,
    );
  }
}

function decryptClient(row: McpServerRow): RegisteredClient {
  const clientId = row.client_id_enc ? decryptToken(row.client_id_enc) : null;
  if (!clientId) throw new McpError("internal", "The stored client registration could not be read.");
  const clientSecret = row.client_secret_enc ? decryptToken(row.client_secret_enc) : null;
  return { clientId, clientSecret };
}

// ─── PKCE + authorize URL ───────────────────────────────────────────────

export function mintPkce(): { verifier: string; challenge: string } {
  const verifier = crypto.randomBytes(48).toString("base64url");
  const challenge = crypto.createHash("sha256").update(verifier).digest("base64url");
  return { verifier, challenge };
}

/** Build the provider authorize URL (authorization_code + PKCE S256).
 *  The `resource` parameter (RFC 8707) binds the issued token to the MCP
 *  server audience, per the MCP authorization spec. */
export function buildMcpAuthorizeUrl(
  row: McpServerRow,
  client: RegisteredClient,
  state: string,
  codeChallenge: string,
): string {
  const params = new URLSearchParams({
    response_type: "code",
    client_id: client.clientId,
    redirect_uri: mcpRedirectUri(),
    state,
    code_challenge: codeChallenge,
    code_challenge_method: "S256",
    resource: row.server_url,
  });
  if (row.scopes) params.set("scope", row.scopes);
  return `${row.authorization_endpoint}?${params.toString()}`;
}

/** ── AUTHORIZE PRE-FLIGHT (user fix 2026-09-16, the invalid_client
 *  incident): allowlisted providers reject unknown clients / unregistered
 *  redirect URIs at the AUTHORIZE step with a raw JSON error page — the
 *  exact dead end the user hit on Vercel and Render. One cheap GET of the
 *  authorize URL (no code is issued, the state is never consumed) BEFORE
 *  the browser navigates turns that into an actionable error: the exact
 *  callback URL to register, and the API-key alternative when the provider
 *  simply does not know our client. 2xx/3xx = the flight passes.
 *
 *  Network hiccups do NOT block the flow (same contract as the Supabase
 *  pre-flight on the fixed connectors). */
export async function preflightMcpAuthorize(authorizeUrl: string, serverUrl: string): Promise<void> {
  let host = "";
  try {
    host = new URL(serverUrl).hostname.toLowerCase();
  } catch {
    /* keep the generic message */
  }
  try {
    const res = await fetch(authorizeUrl, {
      method: "GET",
      redirect: "manual",
      headers: { "User-Agent": "arcforge-mcp-proxy", Accept: "application/json" },
      signal: AbortSignal.timeout(REGISTRATION_TIMEOUT_MS),
    });
    if (res.status < 400) return;
    const body = await res.text().catch(() => "");
    let providerError = "";
    let providerDescription = "";
    try {
      const parsed = JSON.parse(body) as { error?: string; error_description?: string };
      providerError = typeof parsed.error === "string" ? parsed.error : "";
      providerDescription = typeof parsed.error_description === "string" ? parsed.error_description : "";
    } catch {
      /* HTML error page — the status code is all we have */
    }
    logger.warn(
      { host, status: res.status, error: providerError, detail: body.slice(0, 200) },
      "mcp-proxy: authorize pre-flight rejected the request",
    );
    const redirectUri = mcpRedirectUri();
    const idHint = host ? ` for ${host}` : "";
    if (providerError === "invalid_client" || res.status === 401) {
      throw new McpError(
        "authorize_rejected",
        `The provider rejected this platform's OAuth client${idHint} ("unknown or inactive client") — its MCP routes are protected by a client allowlist. ` +
          `If you have an API key for this service, connect with "Use API key" instead; otherwise the platform's client must be registered with the provider first. ` +
          `The callback URL to register is: ${redirectUri}`,
        `HTTP ${res.status}${providerDescription ? `: ${providerDescription}` : ""}`,
      );
    }
    throw new McpError(
      "authorize_rejected",
      `The provider rejected the authorization request before login (HTTP ${res.status}${providerError ? `, ${providerError}` : ""}) — ` +
        `this usually means the OAuth client's registered redirect URI does not match. The exact callback URL to register is: ${redirectUri}`,
      body.slice(0, 200) || undefined,
    );
  } catch (err) {
    if (err instanceof McpError) throw err;
    logger.warn(
      { host, err: err instanceof Error ? err.message : "unknown" },
      "mcp-proxy: authorize pre-flight unreachable (continuing — the browser will show the provider's own error)",
    );
  }
}

// ─── Token exchange + refresh ───────────────────────────────────────────

interface TokenSet {
  accessToken: string;
  refreshToken: string | null;
  expiresAt: string | null;
}

async function tokenRequest(
  row: McpServerRow,
  client: RegisteredClient,
  params: Record<string, string>,
): Promise<TokenSet> {
  const body = new URLSearchParams({ client_id: client.clientId, ...params });
  const headers: Record<string, string> = {
    "Content-Type": "application/x-www-form-urlencoded",
    Accept: "application/json",
  };
  if (client.clientSecret) {
    headers.Authorization = `Basic ${Buffer.from(`${client.clientId}:${client.clientSecret}`).toString("base64")}`;
  }
  const res = await fetch(row.token_endpoint!, {
    method: "POST",
    headers,
    body,
    signal: AbortSignal.timeout(TOKEN_TIMEOUT_MS),
  });
  const text = await res.text().catch(() => "");
  if (!res.ok) {
    throw new McpError("exchange_failed", "The token exchange was rejected.", `HTTP ${res.status}: ${text.slice(0, 300)}`);
  }
  let json: { access_token?: string; refresh_token?: string; expires_in?: number } | null = null;
  try {
    json = JSON.parse(text);
  } catch {
    /* some ASs answer form-urlencoded */
    const parsed = new URLSearchParams(text);
    const at = parsed.get("access_token");
    if (at) json = { access_token: at, refresh_token: parsed.get("refresh_token") ?? undefined };
  }
  if (!json || !json.access_token) {
    throw new McpError("no_token", "The provider completed consent but returned no usable token.");
  }
  return {
    accessToken: json.access_token,
    refreshToken: json.refresh_token || null,
    expiresAt: expiryDate(json.expires_in),
  };
}

export async function exchangeMcpCode(row: McpServerRow, code: string, verifier: string): Promise<TokenSet> {
  return tokenRequest(row, decryptClient(row), {
    grant_type: "authorization_code",
    code,
    redirect_uri: mcpRedirectUri(),
    code_verifier: verifier,
    resource: row.server_url,
  });
}

// ─── Session cache + tools ──────────────────────────────────────────────

const sessions = new Map<string, { sessionId: string; expiresAt: number }>();

function sessionKey(userId: string, serverId: string): string {
  return `${userId}:${serverId}`;
}

function cachedSession(userId: string, serverId: string): string | null {
  const s = sessions.get(sessionKey(userId, serverId));
  if (!s) return null;
  if (Date.now() >= s.expiresAt) {
    sessions.delete(sessionKey(userId, serverId));
    return null;
  }
  return s.sessionId;
}

/** Core refresh round-trip for a user-added server (no staleness check —
 *  the caller decides when). Outcome mirrors the shared connector engine
 *  (connector-oauth.ts): "rotated" (vault updated), "dead" (the provider
 *  definitively rejected the refresh token), "skipped" (transient/no
 *  credential). Never throws, never logs token material. */
async function doRefreshServerTokens(
  row: McpServerRow,
): Promise<"rotated" | "dead" | "skipped"> {
  const connectorId = `mcp:${row.id}`;
  const tokens = await getTokens(row.user_id, connectorId);
  if (!tokens?.refreshToken || !row.token_endpoint) return "skipped";
  try {
    const fresh = await tokenRequest(row, decryptClient(row), {
      grant_type: "refresh_token",
      refresh_token: tokens.refreshToken,
    });
    await rotateTokens(row.user_id, connectorId, fresh);
    sessions.delete(sessionKey(row.user_id, row.id));
    return "rotated";
  } catch (err) {
    // tokenRequest throws McpError("exchange_failed") with the HTTP status
    // embedded in detail — 4xx = the provider rejected the refresh token
    // (dead: reconnect is the only honest path); 5xx/timeouts = transient
    // (skipped: keep the stored pair, the access token may still work).
    let dead = false;
    if (err instanceof McpError) {
      const status = /HTTP (\d{3})/.exec(err.detail ?? "")?.[1];
      dead = !status || Number(status) < 500;
    }
    logger.warn(
      { serverId: row.id, dead, err: err instanceof Error ? err.message : "unknown" },
      dead
        ? "mcp-proxy: provider rejected the refresh token — reconnect required"
        : "mcp-proxy: token refresh failed (transient)",
    );
    return dead ? "dead" : "skipped";
  }
}

/** In-flight refresh dedup: many OAuth providers issue SINGLE-USE refresh
 *  tokens — the list route, the status route, and a concurrent tool call
 *  must share ONE provider round-trip or the loser burns the rotated
 *  token and the server reads dead. Keyed by user+server. */
const inflightServerRefreshes = new Map<string, Promise<"rotated" | "dead" | "skipped">>();

function refreshServerTokens(
  row: McpServerRow,
): Promise<"rotated" | "dead" | "skipped"> {
  const key = `${row.user_id}:${row.id}`;
  const existing = inflightServerRefreshes.get(key);
  if (existing) return existing;
  const pending = doRefreshServerTokens(row).finally(() => {
    inflightServerRefreshes.delete(key);
  });
  inflightServerRefreshes.set(key, pending);
  return pending;
}

/** Refresh-on-use for a user-added server (the proxy's transparent token
 *  refresh). Returns null when no stored credential exists — the server
 *  may legitimately be auth-free. */
async function freshMcpToken(row: McpServerRow): Promise<string | null> {
  const connectorId = `mcp:${row.id}`;
  let tokens = await getTokens(row.user_id, connectorId);
  if (!tokens) return null;

  const expiresAtMs = tokens.expiresAt ? new Date(tokens.expiresAt).getTime() : 0;
  const stale = Boolean(tokens.expiresAt) && expiresAtMs - Date.now() <= REFRESH_WINDOW_MS;
  if (!stale) return tokens.accessToken;

  // Best-effort renewal (deduped) — then re-read: a rotation replaced
  // the pair; a dead/skipped outcome leaves the stored access token,
  // which either still works or the server 401s it honestly.
  await refreshServerTokens(row);
  tokens = await getTokens(row.user_id, connectorId);
  return tokens?.accessToken ?? null;
}

async function ensureMcpSession(
  row: McpServerRow,
  accessToken: string | null,
): Promise<{ sessionId: string | null; serverInfo: { name: string; version: string } | null }> {
  const key = sessionKey(row.user_id, row.id);
  const cached = cachedSession(row.user_id, row.id);
  if (cached) return { sessionId: cached, serverInfo: null };
  const init = await rpcPost(
    row.server_url,
    accessToken,
    {
      jsonrpc: "2.0",
      id: ++rpcId,
      method: "initialize",
      params: {
        protocolVersion: MCP_PROTOCOL_VERSION,
        capabilities: {},
        clientInfo: { name: "arcforge-mcp-proxy", version: "1.0" },
      },
    },
    undefined,
    MCP_INIT_TIMEOUT_MS,
  );
  if (init.status !== 200) {
    throw new McpError(
      "unreachable",
      `MCP initialize failed (HTTP ${init.status}).`,
      init.body?.error?.message,
    );
  }
  if (init.sessionId) {
    await rpcPost(
      row.server_url,
      accessToken,
      { jsonrpc: "2.0", method: "notifications/initialized" },
      init.sessionId,
      MCP_INIT_TIMEOUT_MS,
    ).catch(() => undefined);
    sessions.set(key, { sessionId: init.sessionId, expiresAt: Date.now() + SESSION_TTL_MS });
  }
  const info = (init.body?.result as { serverInfo?: { name?: string; version?: string } } | undefined)?.serverInfo;
  const serverInfo = info?.name ? { name: info.name, version: info.version ?? "" } : null;
  return { sessionId: init.sessionId, serverInfo };
}

export interface McpToolInfo {
  name: string;
  description?: string;
}

/** Live tools/list against the user's server. Updates the row's cache. */
export async function listMcpTools(row: McpServerRow): Promise<McpToolInfo[]> {
  const accessToken = await freshMcpToken(row);
  const { sessionId, serverInfo } = await ensureMcpSession(row, accessToken);
  const call = await rpcPost(
    row.server_url,
    accessToken,
    { jsonrpc: "2.0", id: ++rpcId, method: "tools/list", params: {} },
    sessionId ?? undefined,
    MCP_LIST_TIMEOUT_MS,
  );
  if (call.status !== 200 || call.body?.error) {
    throw new McpError(
      "unreachable",
      `tools/list failed (HTTP ${call.status}).`,
      call.body?.error?.message,
    );
  }
  const result = call.body?.result as { tools?: Array<{ name?: string; description?: string }> } | undefined;
  const tools: McpToolInfo[] = (result?.tools ?? [])
    .filter((t) => typeof t?.name === "string")
    .slice(0, MAX_TOOLS_CACHED)
    .map((t) => ({ name: t.name as string, description: t.description?.slice(0, 300) }));
  // Snapshot the server identity + tool catalog for the UI / AI surface.
  await upsertServerRow({
    user_id: row.user_id,
    server_url: row.server_url,
    server_name: serverInfo?.name ?? row.server_name,
    server_version: serverInfo?.version ?? row.server_version,
    tool_count: tools.length,
    tools_cache: { tools, refreshed_at: new Date().toISOString() },
  });
  return tools;
}

// ─── User-facing operations (routes call these) ─────────────────────────

export interface AddMcpOutcome {
  serverId: string;
  /** Present when the server requires OAuth — the browser navigates here. */
  authorizeUrl?: string;
  /** True when the server was open and is already usable. */
  connected?: boolean;
  toolCount?: number;
  serverName?: string | null;
  label: string;
}

/**
 * The "add an MCP server link" entry point: validate the URL, discover its
 * auth model, register the platform as a client, and either connect
 * immediately (open server) or mint the OAuth authorize URL (PKCE S256).
 * `landingBase` is the validated origin the OAuth round-trip must return
 * the user to (allowedLandingBase at the route).
 *
 * API-KEY MODE (user fix 2026-09-16, the Render fallback): when `apiKey`
 * is supplied, OAuth is skipped entirely — the key is validated with a live
 * MCP initialize, stored encrypted in the vault as the server's bearer
 * credential, and every outgoing MCP request gets `Authorization: Bearer
 * <key>` injected server-side (the proxy's interception point). Providers
 * whose OAuth registration is pending (Render) or any bearer-token MCP
 * server connect this way.
 */
export async function addMcpServer(
  userId: string,
  rawUrl: string,
  landingBase: string,
  apiKey?: string,
): Promise<AddMcpOutcome> {
  const server = await normalizeServerUrl(rawUrl);
  const serverUrl = server.toString();
  const label = hostLabel(serverUrl);

  if (apiKey && apiKey.trim()) {
    return connectServerWithKey(userId, serverUrl, label, apiKey.trim());
  }

  const discovery = await discoverMcpAuth(server);

  if (!discovery.authRequired) {
    // Open server: connect now, snapshot its tools.
    await upsertServerRow({
      user_id: userId,
      server_url: serverUrl,
      label,
      status: "connecting",
      auth_required: false,
      auth_mode: null,
    });
    const row = await getServerRowByUrl(userId, serverUrl);
    if (!row) throw new McpError("internal", "Could not save the MCP server.");
    try {
      const tools = await listMcpTools(row);
      await upsertServerRow({
        user_id: userId,
        server_url: serverUrl,
        status: "connected",
        connected_at: new Date().toISOString(),
      });
      return { serverId: row.id, connected: true, toolCount: tools.length, label };
    } catch (err) {
      await upsertServerRow({ user_id: userId, server_url: serverUrl, status: "error" });
      throw err;
    }
  }

  // OAuth server: register the platform, then mint the authorize URL.
  const client = await registerMcpClient(discovery, serverUrl);
  await upsertServerRow({
    user_id: userId,
    server_url: serverUrl,
    label,
    status: "connecting",
    auth_required: true,
    auth_mode: "oauth",
    authorization_endpoint: discovery.authorizationEndpoint,
    token_endpoint: discovery.tokenEndpoint,
    registration_endpoint: discovery.registrationEndpoint,
    revocation_endpoint: discovery.revocationEndpoint,
    // SCOPE POLICY (verified live against mcp.vercel.com, 2026-09-17):
    // request EVERY scope the server's AS advertises in
    // scopes_supported. For Vercel that is exactly "openid email
    // offline_access profile" — the identity set — and all of them were
    // granted on the real consent round-trip. There is NO scope to add
    // for project/deployment access: Vercel's AS filters out scopes not
    // configured on the app ("integration:all" and friends are silently
    // dropped), and API-permission scopes are in private beta. Account-
    // level tool access comes from the RFC 8707 `resource` binding (the
    // token is issued FOR mcp.vercel.com, which executes tools with the
    // user's own account access — verified: the live tools/list returns
    // the full authenticated catalog, deploy_to_vercel included). When
    // Vercel opens API-permission scopes, they will appear here in
    // scopes_supported and this line picks them up automatically.
    scopes: discovery.scopesSupported,
    client_id_enc: encryptToken(client.clientId),
    client_secret_enc: client.clientSecret ? encryptToken(client.clientSecret) : null,
  });
  const row = await getServerRowByUrl(userId, serverUrl);
  if (!row) throw new McpError("internal", "Could not save the MCP server.");

  const { verifier, challenge } = mintPkce();
  const state = mintState(
    {
      purpose: "connector",
      connector: `mcp:${row.id}`,
      userId,
      landingBase,
      codeVerifier: verifier,
    },
    15 * 60 * 1000,
  );
  const authorizeUrl = buildMcpAuthorizeUrl(row, client, state, challenge);
  // Turn the provider's raw invalid_client / redirect-mismatch error page
  // into an actionable message BEFORE the browser navigates.
  await preflightMcpAuthorize(authorizeUrl, serverUrl);
  return { serverId: row.id, authorizeUrl, label };
}

/** Shared API-key connection engine (add-time and the /connect-key
 * route): validate the key with a live initialize, store it encrypted,
 * snapshot the tool catalog. The key never appears in logs, tool results,
 * or the browser — only the vault and the injected Authorization header. */
async function connectServerWithKey(
  userId: string,
  serverUrl: string,
  label: string,
  apiKey: string,
): Promise<AddMcpOutcome> {
  if (apiKey.length < 8 || apiKey.length > 512) {
    throw new McpError("key_rejected", "That API key does not look valid (length).", `len=${apiKey.length}`);
  }
  await upsertServerRow({
    user_id: userId,
    server_url: serverUrl,
    label,
    status: "connecting",
    auth_required: true,
    auth_mode: "api_key",
  });
  const row = await getServerRowByUrl(userId, serverUrl);
  if (!row) throw new McpError("internal", "Could not save the MCP server.");

  // Validate: a bare initialize with the key as the Bearer token.
  let probe: Awaited<ReturnType<typeof rpcPost>>;
  try {
    probe = await rpcPost(
      serverUrl,
      apiKey,
      {
        jsonrpc: "2.0",
        id: ++rpcId,
        method: "initialize",
        params: {
          protocolVersion: MCP_PROTOCOL_VERSION,
          capabilities: {},
          clientInfo: { name: "arcforge-mcp-proxy", version: "1.0" },
        },
      },
      undefined,
      MCP_INIT_TIMEOUT_MS,
    );
  } catch (err) {
    await upsertServerRow({ user_id: userId, server_url: serverUrl, status: "error" }).catch(() => undefined);
    throw new McpError(
      "unreachable",
      "The MCP server did not respond.",
      err instanceof Error ? err.message : undefined,
    );
  }
  if (probe.status === 401 || probe.status === 403) {
    await upsertServerRow({ user_id: userId, server_url: serverUrl, status: "error" }).catch(() => undefined);
    throw new McpError(
      "key_rejected",
      "The server rejected this API key (HTTP " + probe.status + ") — check that the key is valid, active, and has the right scopes.",
    );
  }
  if (probe.status !== 200) {
    await upsertServerRow({ user_id: userId, server_url: serverUrl, status: "error" }).catch(() => undefined);
    throw new McpError(
      "unreachable",
      `The server answered HTTP ${probe.status} to the key-authenticated initialize — it does not behave like a bearer-token MCP server.`,
    );
  }

  // Store the key as the server's bearer credential (AES-256-GCM in the
  // vault; no refresh, no expiry — keys are revoked by deleting them).
  await upsertConnection(userId, `mcp:${row.id}`, {
    accessToken: apiKey,
    refreshToken: null,
    expiresAt: null,
  }, {
    accountLabel: label,
    grantedCapabilities: null,
  });
  sessions.delete(sessionKey(userId, row.id));

  try {
    const tools = await listMcpTools(row);
    await upsertServerRow({
      user_id: userId,
      server_url: serverUrl,
      status: "connected",
      connected_at: new Date().toISOString(),
    });
    logger.info({ userId, serverId: row.id, toolCount: tools.length }, "mcp-proxy: connected with API key (stored encrypted; value never logged)");
    return { serverId: row.id, connected: true, toolCount: tools.length, label };
  } catch (err) {
    // The key WORKS (initialize passed) — the connection stands even if
    // the catalog snapshot hiccups.
    await upsertServerRow({
      user_id: userId,
      server_url: serverUrl,
      status: "connected",
      connected_at: new Date().toISOString(),
    }).catch(() => undefined);
    logger.warn(
      { userId, serverId: row.id, err: err instanceof Error ? err.message : "unknown" },
      "mcp-proxy: API-key connect stood, but the tool snapshot failed",
    );
    return { serverId: row.id, connected: true, toolCount: 0, label };
  }
}

/** Connect an EXISTING server row with a user-pasted API key (the
 * "secondary connection fallback" for stuck OAuth rows — e.g. Render
 * while their client approval is pending). */
export async function connectMcpWithKey(
  userId: string,
  serverId: string,
  apiKey: string,
): Promise<AddMcpOutcome> {
  const row = await getServerRow(userId, serverId);
  if (!row) throw new McpError("internal", "Unknown MCP server.");
  return connectServerWithKey(userId, row.server_url, row.label || hostLabel(row.server_url), apiKey);
}

/** Complete the OAuth round-trip for a user-added server (called from
 *  completeConnectorOAuth when the state's connector is "mcp:<uuid>").
 *  Returns the final redirect URL — never tokens.
 *
 *  `landing` is connector-oauth.ts's FULL landing resolver: it already
 *  appends the post-OAuth path (the state's returnPath, defaulting to
 *  /connectors) to the validated origin. The query is appended directly
 *  to that URL — an extra "/connectors" segment here landed users on
 *  forgeyn.com.ng/connectors/connectors (an unknown SPA path the
 *  frontend resolves to the homepage). USER FIX 2026-09-17. */
export async function completeMcpOAuth(
  code: string,
  state: OAuthState,
  landing: (s: OAuthState) => string,
): Promise<string> {
  const serverId = state.connector!.slice("mcp:".length);
  const landingUrl = landing(state);
  const fail = (message: string) =>
    `${landingUrl}?connected=${encodeURIComponent(`mcp:${serverId}`)}&status=error&message=${message}`;

  const row = await getServerRow(state.userId!, serverId);
  if (!row) return fail("internal");
  try {
    if (!code || !state.codeVerifier) return fail("exchange_failed");
    const tokens = await exchangeMcpCode(row, code, state.codeVerifier);
    await upsertConnection(state.userId!, `mcp:${serverId}`, tokens, {
      scopes: row.scopes ?? undefined,
      accountLabel: row.label ?? undefined,
      grantedCapabilities: null,
    });
    // Snapshot the tool catalog so the connection is immediately usable
    // and visible ("ready for AI consumption").
    let toolCount: number | null = null;
    let serverName: string | null = null;
    try {
      const tools = await listMcpTools(row);
      toolCount = tools.length;
      serverName = row.server_name;
    } catch {
      /* the connection stands; the catalog refreshes later */
    }
    await upsertServerRow({
      user_id: state.userId!,
      server_url: row.server_url,
      status: "connected",
      connected_at: new Date().toISOString(),
      ...(toolCount !== null ? { tool_count: toolCount } : {}),
    });
    logger.info(
      { userId: state.userId, serverId, toolCount },
      "mcp-proxy: connected (token stored encrypted; value never logged)",
    );
    return `${landingUrl}?connected=${encodeURIComponent(`mcp:${serverId}`)}&status=ok&label=${encodeURIComponent(row.label || serverId)}`;
  } catch (err) {
    await upsertServerRow({ user_id: state.userId!, server_url: row.server_url, status: "error" }).catch(
      () => undefined,
    );
    const code2 = err instanceof McpError ? err.code : "internal";
    logger.warn(
      { serverId, err: err instanceof Error ? err.message : "unknown" },
      "mcp-proxy: OAuth completion failed",
    );
    return fail(code2);
  }
}

// ─── Executor (AI consumption) ──────────────────────────────────────────

export type McpContentBlock = { type: string; text?: string };

export type McpToolOutcome =
  | { ok: true; result: { content: McpContentBlock[] } }
  | { ok: false; error: string; needs_connector?: true };

/** Call one tool on a user-added MCP server. The vault token is injected
 *  server-side; results are sanitized + truncated exactly like the
 *  Supabase executor. */
export async function callUserMcpServer(
  userId: string,
  serverId: string,
  tool: string,
  args: Record<string, unknown>,
): Promise<McpToolOutcome> {
  const toolName = (tool || "").trim();
  if (!toolName) return { ok: false, error: "the mcp tool name is required" };
  const row = await getServerRow(userId, serverId);
  if (!row || row.status !== "connected") {
    return {
      ok: false,
      error: `mcp server ${serverId} is not connected for this account`,
      needs_connector: true,
    };
  }
  const accessToken = await freshMcpToken(row);
  if (row.auth_required && !accessToken) {
    return {
      ok: false,
      error: `mcp server ${row.label ?? serverId} is not connected (no stored credential)`,
      needs_connector: true,
    };
  }

  for (let attempt = 1; attempt <= 2; attempt++) {
    let sessionId: string | null;
    try {
      ({ sessionId } = await ensureMcpSession(row, accessToken));
    } catch (err) {
      return {
        ok: false,
        error: `MCP session could not be established: ${err instanceof Error ? err.message : "unknown"}`,
      };
    }
    const call = await rpcPost(
      row.server_url,
      accessToken,
      {
        jsonrpc: "2.0",
        id: ++rpcId,
        method: "tools/call",
        params: { name: toolName, arguments: args && typeof args === "object" ? args : {} },
      },
      sessionId ?? undefined,
    );
    if (call.status === 404 || call.status === 400) {
      sessions.delete(sessionKey(userId, serverId));
      if (attempt === 1) continue;
    }
    if (call.status === 401 || call.status === 403) {
      return {
        ok: false,
        error: `the MCP server rejected the stored authorization (HTTP ${call.status}) — reconnect the server`,
        needs_connector: true,
      };
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
    const result = call.body?.result as { content?: McpContentBlock[]; isError?: boolean } | undefined;
    if (!result || !Array.isArray(result.content)) {
      return { ok: false, error: "MCP server returned no content blocks" };
    }
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
    logger.info({ userId, serverId, tool: toolName }, "mcp-proxy: tool call ok");
    return { ok: true, result: { content } };
  }
  return { ok: false, error: "MCP session could not be re-established" };
}

// ─── Sanitized list (routes) ────────────────────────────────────────────

export interface SanitizedMcpServer {
  id: string;
  server_url: string;
  label: string;
  status: "pending" | "connecting" | "connected" | "error" | "expired";
  auth_required: boolean;
  /** "oauth" | "api_key" | null (open) — drives the UI's auth-model chip
   *  and the "Use API key" affordance. */
  auth_mode: string | null;
  server_name: string | null;
  server_version: string | null;
  tool_count: number | null;
  tools: McpToolInfo[];
  scopes: string | null;
  connected_at: string | null;
  created_at: string | null;
}

function cachedTools(row: McpServerRow): McpToolInfo[] {
  const cache = row.tools_cache as { tools?: McpToolInfo[] } | null;
  return Array.isArray(cache?.tools) ? (cache!.tools as McpToolInfo[]).slice(0, 100) : [];
}

export function sanitizeServer(row: McpServerRow): SanitizedMcpServer {
  let status: SanitizedMcpServer["status"] =
    row.status === "connected" || row.status === "connecting" || row.status === "error" ? row.status : "pending";
  if (status === "connected" && row.auth_required) {
    // Token freshness is derived lazily by the caller (status route); here
    // the stored status is authoritative.
    status = "connected";
  }
  return {
    id: row.id,
    server_url: row.server_url,
    label: row.label || hostLabel(row.server_url),
    status,
    auth_required: row.auth_required,
    auth_mode: row.auth_mode ?? (row.auth_required ? "oauth" : null),
    server_name: row.server_name,
    server_version: row.server_version,
    tool_count: row.tool_count,
    tools: cachedTools(row),
    scopes: row.scopes,
    connected_at: row.connected_at,
    created_at: row.created_at,
  };
}

export async function listUserServers(userId: string): Promise<SanitizedMcpServer[]> {
  const { data, error } = await serversTable()
    .select("*")
    .eq("user_id", userId)
    .order("created_at", { ascending: true });
  if (error) {
    logger.error({ err: error.message }, "mcp-proxy: list failed");
    throw new McpError("internal", "Could not list MCP servers.");
  }
  const rows = (data as McpServerRow[]) ?? [];
  // One vault read for token freshness on auth-required servers.
  const authed = rows.filter((r) => r.auth_required);
  let expiryById = new Map<string, string | null>();
  if (authed.length > 0 && isSupabaseConfigured()) {
    const { data: tokenRows } = await getServiceSupabase()
      .from("connector_connections")
      .select("connector_id,token_expires_at")
      .eq("user_id", userId)
      .in("connector_id", authed.map((r) => `mcp:${r.id}`));
    if (Array.isArray(tokenRows)) {
      expiryById = new Map(
        (tokenRows as Array<{ connector_id: string; token_expires_at: string | null }>).map((t) => [
          t.connector_id.slice("mcp:".length),
          t.token_expires_at,
        ]),
      );
    }
  }
  // SELF-HEALING STATUS (user fix 2026-09-16 — the disconnect cure, same
  // engine as the fixed connectors): OAuth access tokens are short-lived
  // (the live Expo row: connected 15:39, expired 16:39); renew stale ones
  // with the provider BEFORE deriving status so a valid refresh token in
  // the vault keeps the server "connected" instead of flipping to
  // "Reconnect required". Best-effort and deduped; "dead" (provider
  // rejected the refresh) degrades to the honest expired verdict below.
  const refreshOutcomes = await Promise.all(
    rows.map(async (row) => {
      if (row.status !== "connected" || !row.auth_required || row.auth_mode === "api_key") {
        return "fresh" as const;
      }
      const expiresAt = expiryById.get(row.id) ?? null;
      if (!expiresAt) return "fresh" as const;
      if (new Date(expiresAt).getTime() - Date.now() > REFRESH_WINDOW_MS) return "fresh" as const;
      return refreshServerTokens(row);
    }),
  );
  return rows.map((row, index) => {
    const sanitized = sanitizeServer(row);
    // Expiry is an OAUTH concept — an API-key credential has no expiry and
    // no refresh; it stays "connected" until the server 401s a call (the
    // executor then asks for a reconnect honestly). USER FIX 2026-09-16:
    // the old "no expiry info ⇒ expired" rule mislabeled every key-based
    // connection as expired the moment it was created.
    if (sanitized.status === "connected" && row.auth_required && sanitized.auth_mode !== "api_key") {
      const expiresAt = expiryById.get(row.id) ?? null;
      const stale = expiresAt !== null && new Date(expiresAt).getTime() < Date.now();
      // "rotated" just renewed the token — the pre-refresh expiry map is
      // stale by design and the server stays connected.
      if (stale && refreshOutcomes[index] !== "rotated") {
        sanitized.status = "expired";
      }
    }
    return sanitized;
  });
}

// ─── Tunnel branch (`/mcp/server/<id>`, `/mcp/servers`) ────────────────

export interface McpTunnelFrame {
  id?: string;
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string;
}

export type McpForwardEvent =
  | { kind: "head"; status: number; headers: Record<string, string> }
  | { kind: "chunk"; body: string };

async function* jsonEvents(status: number, payload: unknown): AsyncGenerator<McpForwardEvent, void, void> {
  yield { kind: "head", status, headers: { "content-type": "application/json" } };
  yield { kind: "chunk", body: JSON.stringify(payload) };
}

/** Handle one `/mcp/server/<id>` or `/mcp/servers` req frame from the VM:
 *  resolve sandbox → project → user, then execute against the user's
 *  registered server with the vault token (never in the VM). */
export async function* handleUserMcpTunnel(
  conn: { sandboxId: string },
  frame: McpTunnelFrame,
): AsyncGenerator<McpForwardEvent, void, void> {
  const path = (frame.path || "").split("?")[0];

  let userId: string;
  try {
    const project = await getProjectRowBySandbox(conn.sandboxId);
    if (!project) {
      yield* jsonEvents(403, { ok: false, error: "no project owner for this sandbox" });
      return;
    }
    userId = project.user_id;
  } catch (err) {
    logger.warn({ sandboxId: conn.sandboxId, err: err instanceof Error ? err.message : err }, "mcp-proxy: sandbox→project lookup failed");
    yield* jsonEvents(403, { ok: false, error: "no project owner for this sandbox" });
    return;
  }

  // /mcp/servers — the agent-side catalog of the user's connected servers.
  if (path === "/mcp/servers" || path === "/mcp/servers/") {
    try {
      const servers = await listUserServers(userId);
      yield* jsonEvents(200, {
        ok: true,
        servers: servers
          .filter((s) => s.status === "connected")
          .map((s) => ({
            id: s.id,
            label: s.label,
            server_url: s.server_url,
            server_name: s.server_name,
            tool_count: s.tool_count,
            tools: s.tools.map((t) => t.name),
          })),
      });
    } catch (err) {
      yield* jsonEvents(500, { ok: false, error: err instanceof Error ? err.message : "list failed" });
    }
    return;
  }

  const serverId = path.replace(/^\/mcp\/server\//, "").replace(/\/.*$/, "").trim();
  if (!/^[0-9a-fA-F-]{10,40}$/.test(serverId)) {
    yield* jsonEvents(400, {
      ok: false,
      error: 'bad mcp server path — use /mcp/server/<id> with the id from /mcp/servers, or /mcp/servers to list',
    });
    return;
  }

  let parsed: { tool?: unknown; args?: unknown; action?: unknown };
  try {
    parsed = JSON.parse(frame.body || "{}") as { tool?: unknown; args?: unknown; action?: unknown };
  } catch {
    yield* jsonEvents(400, { ok: false, error: "invalid /mcp/server frame: body must be JSON {tool, args}" });
    return;
  }

  // Tool catalog for one server (lets the agent discover arguments).
  if (parsed.action === "list_tools" || parsed.tool === "__list_tools") {
    const row = await getServerRow(userId, serverId);
    if (!row || row.status !== "connected") {
      yield* jsonEvents(403, { ok: false, error: `mcp server ${serverId} is not connected`, needs_connector: true });
      return;
    }
    try {
      const tools = await listMcpTools(row);
      yield* jsonEvents(200, { ok: true, tools });
    } catch (err) {
      yield* jsonEvents(400, { ok: false, error: err instanceof Error ? err.message : "tools/list failed" });
    }
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

  try {
    const outcome = await callUserMcpServer(userId, serverId, tool, args);
    if (outcome.ok) {
      yield* jsonEvents(200, { ok: true, result: outcome.result });
      return;
    }
    yield* jsonEvents(outcome.needs_connector ? 403 : 400, {
      ok: false,
      error: outcome.error,
      ...(outcome.needs_connector ? { needs_connector: true } : {}),
    });
  } catch (err) {
    const message = err instanceof Error ? err.message : "mcp tunnel failure";
    logger.warn({ sandboxId: conn.sandboxId, err: message }, "mcp-proxy: request failed");
    yield* jsonEvents(500, { ok: false, error: `mcp tunnel failure: ${message}` });
  }
}
