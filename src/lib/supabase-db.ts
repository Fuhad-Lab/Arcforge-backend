import { createClient, type SupabaseClient } from "@supabase/supabase-js";
import { logger } from "./logger";

// ─── DATABASE ROW TYPES (match Supabase tables) ──────────────────────────

export type DbProject = {
  id: string;
  user_id: string;
  name: string;
  description: string;
  stack: string | null;
  mode: string;
  status: string;
  spec: unknown | null;
  agreed_contract: unknown | null;
  preview_html: string | null;
  session_id: string | null;
  skills_used: string[];
  phases_completed: string[];
  negotiation_rounds: number;
  created_at: string;
  updated_at: string;
};

export type DbAgentMessage = {
  id: string;
  project_id: string;
  from_role: string;
  to_role: string;
  subject: string;
  content: string;
  created_at: string;
};

export type DbWorkspaceFile = {
  id: string;
  project_id: string;
  path: string;
  content: string;
  language: string | null;
  created_at: string;
  updated_at: string;
};

export type DbGeneration = {
  id: string;
  project_id: string;
  kind: string;
  status: string;
  backend_code: string | null;
  frontend_code: string | null;
  diagnostics: unknown[] | null;
  skills_used: string[];
  phases_completed: string[];
  negotiation_rounds: number;
  model_config: unknown | null;
  summary: string | null;
  html: string | null;
  model: string | null;
  duration_ms: number | null;
  created_at: string;
};

export type DbChatMessage = {
  id: string;
  project_id: string;
  user_id: string;
  role: string;
  content: string;
  meta: unknown | null;
  created_at: string;
};

export type DbSkillLog = {
  id: string;
  project_id: string;
  skill_id: string;
  phase: string;
  invoked_at: string;
};

// ─── SUPABASE CLIENT SINGLETON ───────────────────────────────────────────

let _client: SupabaseClient | null = null;

function getSupabaseClient(): SupabaseClient {
  if (_client) return _client;

  const supabaseUrl = process.env.SUPABASE_URL;
  const supabaseKey = process.env.SUPABASE_SERVICE_ROLE_KEY;

  if (!supabaseUrl) {
    throw new Error(
      "SUPABASE_URL environment variable is not set. " +
        "Database persistence is disabled.",
    );
  }
  if (!supabaseKey) {
    throw new Error(
      "SUPABASE_SERVICE_ROLE_KEY environment variable is not set. " +
        "Database persistence is disabled.",
    );
  }

  _client = createClient(supabaseUrl, supabaseKey, {
    auth: {
      persistSession: false,
      autoRefreshToken: false,
    },
  });

  logger.info({ url: supabaseUrl }, "Supabase client initialized (service role)");
  return _client;
}

/**
 * Check whether Supabase is configured and available.
 * Returns false if env vars are missing so callers can gracefully degrade.
 */
export function isSupabaseConfigured(): boolean {
  return !!(process.env.SUPABASE_URL && process.env.SUPABASE_SERVICE_ROLE_KEY);
}

// ─── PROJECT OPERATIONS ──────────────────────────────────────────────────

/**
 * Create a new project row in Supabase.
 */
export async function dbCreateProject(
  userId: string,
  prompt: string,
  mode: string,
): Promise<DbProject> {
  const client = getSupabaseClient();

  // Derive a short name from the prompt (first 80 chars, trimmed)
  const name = prompt.length > 80 ? `${prompt.slice(0, 77)}...` : prompt;

  const { data, error } = await client
    .from("projects")
    .insert({
      user_id: userId,
      name,
      description: prompt,
      mode,
      status: "created",
      skills_used: [],
      phases_completed: [],
      negotiation_rounds: 0,
    })
    .select()
    .single();

  if (error) {
    logger.error({ userId, error }, "dbCreateProject failed");
    throw new Error(`dbCreateProject: ${error.message}`);
  }

  logger.info({ projectId: data.id, userId }, "Project created in DB");
  return data as DbProject;
}

/**
 * Get a single project by ID with all related data.
 */
export async function dbGetProject(
  id: string,
): Promise<DbProject | null> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("projects")
    .select()
    .eq("id", id)
    .single();

  if (error) {
    if (error.code === "PGRST116") return null; // not found
    logger.error({ projectId: id, error }, "dbGetProject failed");
    throw new Error(`dbGetProject: ${error.message}`);
  }

  return data as DbProject;
}

/**
 * List all projects for a given user, newest first.
 */
export async function dbListProjects(
  userId: string,
): Promise<DbProject[]> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("projects")
    .select()
    .eq("user_id", userId)
    .order("updated_at", { ascending: false });

  if (error) {
    logger.error({ userId, error }, "dbListProjects failed");
    throw new Error(`dbListProjects: ${error.message}`);
  }

  return (data ?? []) as DbProject[];
}

/**
 * Partially update a project row. `data` is a partial record.
 */
export async function dbUpdateProject(
  id: string,
  data: Record<string, unknown>,
): Promise<DbProject> {
  const client = getSupabaseClient();

  const { dbData, ...rest } = data;
  const updatePayload = {
    ...rest,
    updated_at: new Date().toISOString(),
  };

  const { data: result, error } = await client
    .from("projects")
    .update(updatePayload as never)
    .eq("id", id)
    .select()
    .single();

  if (error) {
    logger.error({ projectId: id, error }, "dbUpdateProject failed");
    throw new Error(`dbUpdateProject: ${error.message}`);
  }

  return result as DbProject;
}

/**
 * Delete a project and all cascading related rows.
 */
export async function dbDeleteProject(id: string): Promise<void> {
  const client = getSupabaseClient();

  const { error } = await client.from("projects").delete().eq("id", id);

  if (error) {
    logger.error({ projectId: id, error }, "dbDeleteProject failed");
    throw new Error(`dbDeleteProject: ${error.message}`);
  }

  logger.info({ projectId: id }, "Project deleted from DB");
}

// ─── AGENT MESSAGES ───────────────────────────────────────────────────────

/**
 * Save an inter-agent message.
 */
export async function dbSaveAgentMessage(
  projectId: string,
  from: string,
  to: string,
  subject: string,
  content: string,
): Promise<DbAgentMessage> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("agent_messages")
    .insert({
      project_id: projectId,
      from_role: from,
      to_role: to,
      subject,
      content,
    })
    .select()
    .single();

  if (error) {
    logger.error(
      { projectId, from, to, error },
      "dbSaveAgentMessage failed",
    );
    throw new Error(`dbSaveAgentMessage: ${error.message}`);
  }

  return data as DbAgentMessage;
}

/**
 * Get all agent messages for a project, ordered chronologically.
 */
export async function dbGetAgentMessages(
  projectId: string,
): Promise<DbAgentMessage[]> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("agent_messages")
    .select()
    .eq("project_id", projectId)
    .order("created_at", { ascending: true });

  if (error) {
    logger.error({ projectId, error }, "dbGetAgentMessages failed");
    throw new Error(`dbGetAgentMessages: ${error.message}`);
  }

  return (data ?? []) as DbAgentMessage[];
}

// ─── WORKSPACE FILES ──────────────────────────────────────────────────────

/**
 * Upsert a workspace file for a project.
 */
export async function dbSaveWorkspaceFile(
  projectId: string,
  filePath: string,
  content: string,
  language: string,
): Promise<DbWorkspaceFile> {
  const client = getSupabaseClient();

  // Try update first, then insert (upsert via on_conflict)
  const { data, error } = await client
    .from("workspace_files")
    .upsert(
      {
        project_id: projectId,
        path: filePath,
        content,
        language,
      },
      {
        onConflict: "project_id,path",
      },
    )
    .select()
    .single();

  if (error) {
    logger.error(
      { projectId, path: filePath, error },
      "dbSaveWorkspaceFile failed",
    );
    throw new Error(`dbSaveWorkspaceFile: ${error.message}`);
  }

  return data as DbWorkspaceFile;
}

/**
 * Get all workspace files for a project.
 */
export async function dbGetWorkspaceFiles(
  projectId: string,
): Promise<DbWorkspaceFile[]> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("workspace_files")
    .select()
    .eq("project_id", projectId)
    .order("path", { ascending: true });

  if (error) {
    logger.error({ projectId, error }, "dbGetWorkspaceFiles failed");
    throw new Error(`dbGetWorkspaceFiles: ${error.message}`);
  }

  return (data ?? []) as DbWorkspaceFile[];
}

/**
 * Delete a workspace file by project ID and path.
 */
export async function dbDeleteWorkspaceFile(
  projectId: string,
  filePath: string,
): Promise<void> {
  const client = getSupabaseClient();

  const { error } = await client
    .from("workspace_files")
    .delete()
    .eq("project_id", projectId)
    .eq("path", filePath);

  if (error) {
    logger.error(
      { projectId, path: filePath, error },
      "dbDeleteWorkspaceFile failed",
    );
    throw new Error(`dbDeleteWorkspaceFile: ${error.message}`);
  }
}

// ─── GENERATIONS ──────────────────────────────────────────────────────────

/**
 * Save a pipeline generation run.
 */
export async function dbSaveGeneration(
  projectId: string,
  data: {
    kind?: string;
    status: string;
    backend_code?: string | null;
    frontend_code?: string | null;
    diagnostics?: unknown[];
    skills_used?: string[];
    phases_completed?: string[];
    negotiation_rounds?: number;
    model_config?: unknown;
    summary?: string | null;
    html?: string | null;
    model?: string | null;
    duration_ms?: number | null;
  },
): Promise<DbGeneration> {
  const client = getSupabaseClient();

  const { data: result, error } = await client
    .from("generations")
    .insert({
      project_id: projectId,
      kind: data.kind ?? "full_pipeline",
      status: data.status,
      backend_code: data.backend_code ?? null,
      frontend_code: data.frontend_code ?? null,
      diagnostics: data.diagnostics ?? null,
      skills_used: data.skills_used ?? [],
      phases_completed: data.phases_completed ?? [],
      negotiation_rounds: data.negotiation_rounds ?? 0,
      model_config: data.model_config ?? null,
      summary: data.summary ?? null,
      html: data.html ?? null,
      model: data.model ?? null,
      duration_ms: data.duration_ms ?? null,
    })
    .select()
    .single();

  if (error) {
    logger.error({ projectId, error }, "dbSaveGeneration failed");
    throw new Error(`dbSaveGeneration: ${error.message}`);
  }

  logger.info(
    { projectId, generationId: result!.id, status: data.status },
    "Generation saved to DB",
  );
  return result as DbGeneration;
}

// ─── CHAT MESSAGES ────────────────────────────────────────────────────────

/**
 * Save a user-facing chat message.
 */
export async function dbSaveChatMessage(
  projectId: string,
  userId: string,
  role: string,
  content: string,
  meta?: unknown,
): Promise<DbChatMessage> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("chat_messages")
    .insert({
      project_id: projectId,
      user_id: userId,
      role,
      content,
      meta: meta ?? null,
    })
    .select()
    .single();

  if (error) {
    logger.error({ projectId, error }, "dbSaveChatMessage failed");
    throw new Error(`dbSaveChatMessage: ${error.message}`);
  }

  return data as DbChatMessage;
}

/**
 * Get chat history for a project.
 */
export async function dbGetChatMessages(
  projectId: string,
): Promise<DbChatMessage[]> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("chat_messages")
    .select()
    .eq("project_id", projectId)
    .order("created_at", { ascending: true });

  if (error) {
    logger.error({ projectId, error }, "dbGetChatMessages failed");
    throw new Error(`dbGetChatMessages: ${error.message}`);
  }

  return (data ?? []) as DbChatMessage[];
}

// ─── SKILL LOGS ───────────────────────────────────────────────────────────

/**
 * Log a skill invocation for a project.
 */
export async function dbLogSkill(
  projectId: string,
  skillId: string,
  phase: string,
): Promise<DbSkillLog> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("skill_logs")
    .insert({
      project_id: projectId,
      skill_id: skillId,
      phase,
    })
    .select()
    .single();

  if (error) {
    logger.error(
      { projectId, skillId, phase, error },
      "dbLogSkill failed",
    );
    throw new Error(`dbLogSkill: ${error.message}`);
  }

  return data as DbSkillLog;
}

/**
 * Get all skill logs for a project.
 */
export async function dbGetSkillLogs(
  projectId: string,
): Promise<DbSkillLog[]> {
  const client = getSupabaseClient();

  const { data, error } = await client
    .from("skill_logs")
    .select()
    .eq("project_id", projectId)
    .order("invoked_at", { ascending: true });

  if (error) {
    logger.error({ projectId, error }, "dbGetSkillLogs failed");
    throw new Error(`dbGetSkillLogs: ${error.message}`);
  }

  return (data ?? []) as DbSkillLog[];
}
