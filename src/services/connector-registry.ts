/**
 * Generic connector registry — GROUP 2.
 *
 * A connector is an external service (Supabase, GitHub, …) that the Chief
 * Agent or any delegated subagent may use on the authenticated user's
 * behalf. Every connector declares CAPABILITIES (fine-grained, e.g.
 * "supabase.database.write"); the backend resolves capability → connector,
 * checks the user's authorization, and grants ONLY the minimum scoped
 * capability. Subagents declare capabilities; they never receive raw
 * credentials (tokens stay in the backend vault, encrypted at rest).
 *
 * Adding a future connector = adding an entry here — no changes to the
 * Chief Agent pipeline, the routes, or the frontend contract.
 */

export type ConnectorAuthMethod =
  | "oauth_supabase"
  | "oauth_github_app"
  | "oauth_github";

/** Which Supabase Edge Function is the OAuth provider's REGISTERED
 *  callback target for this connector. The redirect_uri sent at authorize
 *  and exchange time is resolved from this — it must match the callback
 *  URL registered on the provider's app exactly. */
export type ConnectorRedirectKind = "connector-ops" | "auth-oauth";

export interface ConnectorCapability {
  /** Capability identifier, e.g. "supabase.database.write". */
  id: string;
  /** Human description shown in connection UIs. */
  description: string;
  /** Minimum permissions the user grants when authorizing it. */
  minPermissions: string;
}

export interface ConnectorDefinition {
  /** Stable identifier, e.g. "supabase". */
  id: string;
  name: string;
  description: string;
  category: string;
  /** OAuth mechanism this connector uses (generic flow, no provider-specific
   *  logic leaks into the Chief Agent — only this registry knows it). */
  authMethod: ConnectorAuthMethod;
  /** Registered OAuth callback edge function (see ConnectorRedirectKind). */
  redirectKind: ConnectorRedirectKind;
  /** Capability catalogue the agent may request. */
  capabilities: ConnectorCapability[];
  /** Whether delegated subagents may be granted scoped access. */
  delegationSupported: boolean;
  /** Env var names holding this connector's OAuth client credentials
   *  (Render environment variables — never in source, never in the frontend). */
  envClientId: string;
  envClientSecret: string;
  /**
   * Additional OAuth authorize scopes. Used by "oauth_github" (classic
   * OAuth App flow — scopes are requested per-authorization); Supabase
   * scopes are configured on the OAuth app itself (the scope param is
   * deprecated) and GitHub App permissions are baked into the app.
   */
  authorizeScopes?: string;
  /** Backend tool / MCP capability this connector unlocks. */
  mcpCapability: string;
}

export const CONNECTORS: ConnectorDefinition[] = [
  {
    id: "supabase",
    name: "Supabase",
    description:
      "Manage your Supabase projects, run migrations, and execute SQL on your " +
      "behalf through the official Supabase MCP server.",
    category: "Database",
    authMethod: "oauth_supabase",
    redirectKind: "connector-ops",
    delegationSupported: true,
    envClientId: "SUPABASE_OAUTH_CLIENT_ID",
    envClientSecret: "SUPABASE_OAUTH_CLIENT_SECRET",
    mcpCapability: "supabase-mcp",
    capabilities: [
      {
        id: "supabase.database.write",
        description: "Apply migrations and execute SQL on your Supabase projects.",
        minPermissions: "Database write (migrations, SQL queries)",
      },
      {
        id: "supabase.database.read",
        description: "Inspect schemas, tables, and extensions of your projects.",
        minPermissions: "Database read (schema inspection)",
      },
      {
        id: "supabase.projects.read",
        description: "List your Supabase organizations and projects.",
        minPermissions: "Account read (projects list)",
      },
    ],
  },
  {
    id: "github",
    name: "GitHub",
    description:
      "Import repositories, sync workspace code, and manage repos through the " +
      "Forgeyn GitHub connection.",
    category: "Code",
    // 2026-09-12 (user fix): the GitHub App client secret stored in env was
    // found INVALID against GitHub ("incorrect_client_credentials" — verified
    // live), so every token exchange failed and the connector read as broken
    // even though consent worked. The GitHub connector now runs on the
    // VERIFIED-WORKING classic OAuth App pair (GITHUB_SIGNIN_* — the same
    // credentials GitHub Sign-In uses, proven valid in production) with
    // explicit repo scopes. The "oauth_github_app" flow stays supported for
    // future connectors; if the App secret is ever regenerated, its flow can
    // return without pipeline changes.
    authMethod: "oauth_github",
    redirectKind: "auth-oauth",
    delegationSupported: true,
    envClientId: "GITHUB_SIGNIN_CLIENT_ID",
    envClientSecret: "GITHUB_SIGNIN_CLIENT_SECRET",
    authorizeScopes: "read:user user:email repo",
    mcpCapability: "github-rest",
    capabilities: [
      {
        id: "github.repos.write",
        description: "Create repositories and push workspace code on your behalf.",
        minPermissions: "Repository contents (read/write)",
      },
      {
        id: "github.repos.read",
        description: "Read repositories and their metadata.",
        minPermissions: "Repository contents (read)",
      },
      {
        id: "github.user.read",
        description: "Read your GitHub profile and account info.",
        minPermissions: "User profile (read)",
      },
    ],
  },
];

export function getConnector(id: string): ConnectorDefinition | undefined {
  return CONNECTORS.find((c) => c.id === id);
}

/** Resolve which connector a capability belongs to, e.g.
 *  "supabase.database.write" → the supabase connector. */
export function resolveCapability(
  capability: string,
): { connector: ConnectorDefinition; capability: ConnectorCapability } | null {
  for (const connector of CONNECTORS) {
    const cap = connector.capabilities.find((c) => c.id === capability);
    if (cap) return { connector, capability: cap };
  }
  return null;
}

export function connectorCredentials(
  connector: ConnectorDefinition,
): { clientId: string; clientSecret: string } | null {
  const clientId = process.env[connector.envClientId] || "";
  const clientSecret = process.env[connector.envClientSecret] || "";
  if (!clientId || !clientSecret) return null;
  return { clientId, clientSecret };
}

/** Sanitized, browser-safe connector metadata (no env names, no secrets). */
export function connectorMetadata(connector: ConnectorDefinition): Record<string, unknown> {
  return {
    id: connector.id,
    name: connector.name,
    description: connector.description,
    category: connector.category,
    capabilities: connector.capabilities.map((c) => ({
      id: c.id,
      description: c.description,
      min_permissions: c.minPermissions,
    })),
    delegation_supported: connector.delegationSupported,
    method: connector.authMethod,
    mcp_capability: connector.mcpCapability,
  };
}
