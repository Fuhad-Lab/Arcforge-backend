/**
 * Shared project-row lookups (Supabase `projects` table).
 *
 * Extracted from `src/routes/workspace.ts` so services that must NOT import
 * route modules (e.g. `services/github-proxy.ts`, which is imported by the
 * reverse-tunnel client, which workspace.ts itself imports) can resolve
 * project rows without creating an import cycle.
 */
import { getServiceSupabase, isSupabaseConfigured } from "./supabase-db";

export type ProjectRow = {
  id: string;
  user_id: string;
  name: string;
  logo_url: string | null;
  session_id: string | null;
  sandbox_id: string | null;
};

/** Column list shared by both lookups (kept identical to the router's). */
const PROJECT_COLUMNS = "id,user_id,name,logo_url,session_id,sandbox_id";

/**
 * Fetch a project row by id. Returns null when not found (or DB down).
 * Callers MUST compare `row.user_id === req.userId` before acting.
 */
export async function getProjectRow(projectId: string): Promise<ProjectRow | null> {
  if (!isSupabaseConfigured()) return null;
  const supabase = getServiceSupabase();
  const { data, error } = await supabase
    .from("projects")
    .select(PROJECT_COLUMNS)
    .eq("id", projectId)
    .maybeSingle();
  if (error) throw new Error(`project lookup: ${error.message}`);
  return (data as ProjectRow) ?? null;
}

/**
 * Look up a project row by its sandbox_id. Used by the preview proxy
 * routes (ownership guard) and by the GitHub tunnel proxy (sandbox →
 * project → user → settings.github_pat). Returns null when no project
 * owns this sandbox (or DB down).
 */
export async function getProjectRowBySandbox(
  sandboxId: string,
): Promise<ProjectRow | null> {
  if (!isSupabaseConfigured()) return null;
  const supabase = getServiceSupabase();
  const { data, error } = await supabase
    .from("projects")
    .select(PROJECT_COLUMNS)
    .eq("sandbox_id", sandboxId)
    .maybeSingle();
  if (error) throw new Error(`project lookup by sandbox: ${error.message}`);
  return (data as ProjectRow) ?? null;
}
