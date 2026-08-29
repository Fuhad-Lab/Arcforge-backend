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

export type ConnectorAuthMethod = "oauth_supabase" | "oauth_github_app";

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
  /** Capability catalogue the agent may request. */
  capabilities: ConnectorCapability[];
  /** Whether delegated subagents may be granted scoped access. */
  delegationSupported: boolean;
  /** Env var names holding this connector's OAuth client credentials
   *  (Render environment variables — never in source, never in the frontend). */
  envClientId: string;
  envClientSecret: string;
  /** Additional OAuth authorize scopes (GitHub sign-in only; Supabase scopes
   *  are configured on the OAuth app itself — the scope param is deprecated). */
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
      "Forge-AI App Builder GitHub App.",
    category: "Code",
    authMethod: "oauth_github_app",
    delegationSupported: true,
    envClientId: "GITHUB_APP_CLIENT_ID",
    envClientSecret: "GITHUB_APP_CLIENT_SECRET",
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
