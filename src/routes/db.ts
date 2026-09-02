/**
 * /api/db — database-backed routes for the frontend (via the Supabase Edge
 * Function `db-ops`, which forwards the user's Authorization header).
 *
 * Auth: requireAuth (JWT) — every query is scoped to req.userId.
 * This backend is the ONLY service allowed to use the Supabase service role.
 */
import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";
import {
  getServiceSupabase,
  ensureUserRow,
  isSupabaseConfigured,
  type DbProject,
  type DbUser,
} from "../lib/supabase-db";

const router: IRouter = Router();

// JWT auth on every /api/db route.
router.use(requireAuth);

// ─── HELPERS ───────────────────────────────────────────────────────────────

type ChatMessageRow = {
  role: string;
  content: string;
  created_at: string;
};

/** platforms column is stored as a comma-joined string (or already an array). */
function toPlatformsArray(value: string | string[] | null | undefined): string[] {
  if (Array.isArray(value)) return value.filter((p) => typeof p === "string");
  if (typeof value === "string" && value.length > 0) {
    return value
      .split(",")
      .map((p) => p.trim())
      .filter(Boolean);
  }
  return [];
}

function toPlatformsColumn(value: unknown): string | null {
  if (Array.isArray(value)) {
    const arr = value.filter((p): p is string => typeof p === "string");
    return arr.length > 0 ? arr.join(",") : null;
  }
  if (typeof value === "string" && value.trim().length > 0) return value.trim();
  return null;
}

function requireSupabase(res: Response): ReturnType<typeof getServiceSupabase> | null {
  if (!isSupabaseConfigured()) {
    res.status(503).json({ error: "Database is not configured on this backend" });
    return null;
  }
  return getServiceSupabase();
}

/** CamelCase settings payload — NEVER includes raw API key values. */
function toSettings(row: DbUser) {
  return {
    email: row.email ?? "",
    displayName: row.display_name ?? row.name ?? "",
    aiModel: row.ai_model ?? "",
    theme: row.theme ?? "dark",
    defaultPlatforms: toPlatformsArray(row.default_platforms),
    emailBuildNotifications: row.email_build_notifications ?? true,
    weeklyDigest: row.weekly_digest ?? false,
    hasNvidiaKey: Boolean(row.nvidia_api_key),
    hasDaytonaKey: Boolean(row.daytona_api_key),
    // GitHub PAT is write-only (contract C6): the settings response carries
    // ONLY the connected boolean — the PAT value never leaves the DB.
    githubConnected: Boolean(row.github_pat),
  };
}

// ─── GET /api/db/sessions — list the user's recent projects ────────────────

router.get("/sessions", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const { data, error } = await supabase
      .from("projects")
      .select(
        "id,name,logo_url,platforms,session_id,status,sandbox_id,created_at,updated_at",
      )
      .eq("user_id", req.userId)
      .order("updated_at", { ascending: false })
      .limit(50);

    if (error) throw new Error(`sessions list: ${error.message}`);

    const rows = (data ?? []) as Array<
      Pick<DbProject, "id" | "name" | "logo_url" | "platforms" | "session_id" | "status" | "sandbox_id" | "created_at" | "updated_at">
    >;

    res.json({
      sessions: rows.map((row) => ({
        id: row.session_id || row.id,
        title: row.name,
        logoUrl: row.logo_url,
        platforms: toPlatformsArray(row.platforms),
        projectId: row.id,
        createdAt: row.created_at,
        updatedAt: row.updated_at,
        _count: { messages: 0 },
      })),
    });
  } catch (error) {
    next(error);
  }
});

// ─── POST /api/db/projects — create a draft project ────────────────────────

router.post("/projects", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const body = req.body ?? {};
    const name = typeof body.name === "string" ? body.name.trim() : "";
    if (!name) {
      res.status(400).json({ error: "name is required" });
      return;
    }
    const logoUrl = typeof body.logoUrl === "string" ? body.logoUrl : null;
    const sessionId = typeof body.sessionId === "string" && body.sessionId ? body.sessionId : null;
    const description = typeof body.description === "string" ? body.description : null;
    // GROUP 3: imported repositories create the project with mode "import"
    // (GitHub App flow); everything else stays "single".
    const mode = body.mode === "import" ? "import" : "single";

    // FK SAFETY NET — fixes "projects_user_id_fkey" violation for brand-new
    // auth users who have never visited /settings (no public.users row yet).
    // The service-role client bypasses RLS; onConflict id = idempotent.
    // This runs BEFORE the projects INSERT so the constraint is satisfiable.
    await ensureUserRow(req.userId ?? "", req.userEmail ?? null);

    const { data, error } = await supabase
      .from("projects")
      .insert({
        user_id: req.userId,
        name,
        description,
        logo_url: logoUrl,
        platforms: toPlatformsColumn(body.platforms),
        session_id: sessionId,
        status: "draft",
        mode,
        skills_used: [],
        phases_completed: [],
        negotiation_rounds: 0,
      })
      .select("id,name,logo_url,platforms,session_id,created_at,updated_at")
      .single();

    if (error) throw new Error(`create project: ${error.message}`);

    res.status(201).json({
      project: {
        id: data.id,
        name: data.name,
        logoUrl: data.logo_url,
        platforms: toPlatformsArray(data.platforms),
        sessionId: data.session_id,
        createdAt: data.created_at,
        updatedAt: data.updated_at,
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── PUT /api/db/projects/:id — update a draft project's meta ──────────────
// THE FORGE TRIGGER path: startBuildFlow creates the project row the
// moment the user clicks forge (auto name, no logo), the build wizard
// runs on top of that background work, and its choices (final name,
// logo, platforms) land HERE. Ownership is enforced — a user can only
// ever update their own project.

router.put("/projects/:id", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const projectId = req.params.id;
    if (!projectId) {
      res.status(400).json({ error: "project id is required" });
      return;
    }

    const body = req.body ?? {};
    const name = typeof body.name === "string" ? body.name.trim() : "";
    if (!name) {
      res.status(400).json({ error: "name is required" });
      return;
    }
    const logoUrl = typeof body.logoUrl === "string" && body.logoUrl ? body.logoUrl : null;

    // Ownership guard: the row must exist AND belong to the caller.
    const { data: existing, error: findError } = await supabase
      .from("projects")
      .select("id,user_id,name,logo_url,platforms,session_id,created_at,updated_at")
      .eq("id", projectId)
      .eq("user_id", req.userId)
      .maybeSingle();

    if (findError) throw new Error(`find project: ${findError.message}`);
    if (!existing) {
      res.status(404).json({ error: "Project not found" });
      return;
    }

    const { data, error } = await supabase
      .from("projects")
      .update({
        name,
        logo_url: logoUrl,
        platforms: toPlatformsColumn(body.platforms) ?? existing.platforms,
      })
      .eq("id", projectId)
      .eq("user_id", req.userId)
      .select("id,name,logo_url,platforms,session_id,created_at,updated_at")
      .single();

    if (error) throw new Error(`update project: ${error.message}`);

    res.status(200).json({
      project: {
        id: data.id,
        name: data.name,
        logoUrl: data.logo_url,
        platforms: toPlatformsArray(data.platforms),
        sessionId: data.session_id,
        createdAt: data.created_at,
        updatedAt: data.updated_at,
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/db/sessions/:sessionId — session detail + chat history ───────

router.get("/sessions/:sessionId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    // Primary lookup by session_id; fallback to the project UUID for rows
    // created by direct backend generations (session_id may be null there).
    // engine_run_* (optional, Forgvi 2.0 run-in-flight record) rides along
    // so a re-mounted studio can re-attach to a live run.
    const PROJECT_SESSION_COLUMNS =
      "id,user_id,name,logo_url,platforms,session_id,sandbox_id,engine_run_id,engine_run_origin,engine_run_status,engine_run_objective";
    const PROJECT_SESSION_BASE_COLUMNS =
      "id,user_id,name,logo_url,platforms,session_id,sandbox_id";
    /** Select with a deploy-order shield: if the engine_run_* columns are
     *  not on the table yet (schema applied after this deploy), retry with
     *  the base column set and read engineRun as null — the studio just
     *  loses re-attach until the columns land, never the whole session. */
    const selectProject = async (match: { column: "session_id" | "id"; value: string }) => {
      const full = await supabase
        .from("projects")
        .select(PROJECT_SESSION_COLUMNS)
        .eq(match.column, match.value)
        .eq("user_id", req.userId)
        .maybeSingle();
      if (!full.error) return { data: full.data, error: null };
      if (full.error.message.includes("engine_run")) {
        logger.warn("engine_run_* columns missing — get-session degrades to base columns");
        const base = await supabase
          .from("projects")
          .select(PROJECT_SESSION_BASE_COLUMNS)
          .eq(match.column, match.value)
          .eq("user_id", req.userId)
          .maybeSingle();
        return { data: base.data, error: base.error };
      }
      return { data: full.data, error: full.error };
    };

    let { data: project, error } = await selectProject({ column: "session_id", value: String(req.params.sessionId) });

    if (error) throw new Error(`session lookup: ${error.message}`);

    if (!project) {
      const fallback = await selectProject({ column: "id", value: String(req.params.sessionId) });
      if (fallback.error) throw new Error(`session lookup: ${fallback.error.message}`);
      project = fallback.data;
    }

    if (!project) {
      res.status(404).json({ error: "Session not found" });
      return;
    }

    // Chat history (most recent 200 messages, chronological).
    let messages: Array<{ role: string; content: string }> = [];
    try {
      const { data: chatRows, error: chatError } = await supabase
        .from("chat_messages")
        .select("role,content,created_at")
        .eq("project_id", project.id)
        .order("created_at", { ascending: true })
        .limit(200);

      if (chatError) {
        logger.warn({ err: chatError.message }, "chat history lookup failed");
      } else {
        messages = ((chatRows ?? []) as ChatMessageRow[]).map((m) => ({
          role: m.role,
          content: m.content,
        }));
      }
    } catch (err: unknown) {
      logger.warn({ err: err instanceof Error ? err.message : err }, "chat history lookup failed");
    }

    // The Forgvi 2.0 run-in-flight record — surfaced ONLY while the run
    // is still going (terminal statuses settle the row and read as null
    // here, so a remount never re-attaches a finished run).
    const engineRunCols = project as unknown as {
      engine_run_id?: string | null;
      engine_run_origin?: string | null;
      engine_run_status?: string | null;
      engine_run_objective?: string | null;
    };
    const engineRun =
      engineRunCols.engine_run_id && engineRunCols.engine_run_status === "running"
        ? {
            runId: engineRunCols.engine_run_id,
            origin: engineRunCols.engine_run_origin === "vm" ? ("vm" as const) : ("render" as const),
            status: "running",
            objective: engineRunCols.engine_run_objective ?? null,
          }
        : null;

    res.json({
      session: {
        id: project.session_id || project.id,
        title: project.name,
        project: {
          id: project.id,
          name: project.name,
          logoUrl: project.logo_url,
          platforms: toPlatformsArray(project.platforms),
          sandboxId: project.sandbox_id,
          engineRun,
        },
        messages,
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── DELETE /api/db/sessions/:sessionId — delete a project ─────────────────

router.delete("/sessions/:sessionId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const { error } = await supabase
      .from("projects")
      .delete()
      .or(`session_id.eq.${req.params.sessionId},id.eq.${req.params.sessionId}`)
      .eq("user_id", req.userId);

    if (error) throw new Error(`delete session: ${error.message}`);

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/db/settings — user settings (upsert-on-read) ─────────────────

router.get("/settings", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    let { data: row, error } = await supabase
      .from("users")
      .select("*")
      .eq("id", req.userId)
      .maybeSingle();

    if (error) throw new Error(`settings lookup: ${error.message}`);

    if (!row) {
      // First read — create the settings row.
      const { data: inserted, error: insertError } = await supabase
        .from("users")
        .upsert(
          { id: req.userId, email: req.userEmail ?? "" },
          { onConflict: "id" },
        )
        .select("*")
        .single();

      if (insertError) throw new Error(`settings bootstrap: ${insertError.message}`);
      row = inserted;
    }

    res.json({ settings: toSettings(row as DbUser) });
  } catch (error) {
    next(error);
  }
});

// ─── PUT /api/db/settings — update user settings ───────────────────────────

router.put("/settings", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const body = req.body ?? {};
    const patch: Record<string, unknown> = {};

    if (typeof body.displayName === "string") patch.display_name = body.displayName;
    if (typeof body.aiModel === "string") patch.ai_model = body.aiModel;
    if (typeof body.theme === "string") patch.theme = body.theme;
    if (body.defaultPlatforms !== undefined) {
      patch.default_platforms = toPlatformsColumn(body.defaultPlatforms);
    }
    if (typeof body.emailBuildNotifications === "boolean") {
      patch.email_build_notifications = body.emailBuildNotifications;
    }
    if (typeof body.weeklyDigest === "boolean") {
      patch.weekly_digest = body.weeklyDigest;
    }
    // API keys: optional string values; empty string clears the stored key.
    if (typeof body.nvidiaApiKey === "string") {
      patch.nvidia_api_key = body.nvidiaApiKey.trim() ? body.nvidiaApiKey.trim() : null;
    }
    if (typeof body.daytonaApiKey === "string") {
      patch.daytona_api_key = body.daytonaApiKey.trim() ? body.daytonaApiKey.trim() : null;
    }
    // GitHub PAT (contract C6): write-only — trimmed, empty string clears
    // (null). NEVER echoed back; toSettings exposes githubConnected only.
    if (typeof body.githubPat === "string") {
      patch.github_pat = body.githubPat.trim() ? body.githubPat.trim() : null;
    }

    const { data: row, error } = await supabase
      .from("users")
      .upsert(
        {
          id: req.userId,
          email: req.userEmail ?? "",
          ...patch,
        },
        { onConflict: "id" },
      )
      .select("*")
      .single();

    if (error) throw new Error(`settings update: ${error.message}`);

    // NEVER echo back raw API key values — only booleans.
    res.json({ settings: toSettings(row as DbUser) });
  } catch (error) {
    next(error);
  }
});

// ─── DELETE /api/db/account — delete the user's account + data ─────────────

router.delete("/account", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    // Chat messages first (explicit — protects against missing FK cascades).
    const chatError = (
      await supabase.from("chat_messages").delete().eq("user_id", req.userId)
    ).error;
    if (chatError) {
      logger.warn({ err: chatError.message }, "account deletion: chat cleanup failed");
    }

    // Explicitly remove the user's projects (related rows cascade via FK).
    const projectsError = (
      await supabase.from("projects").delete().eq("user_id", req.userId)
    ).error;
    if (projectsError) throw new Error(`account deletion (projects): ${projectsError.message}`);

    const userError = (await supabase.from("users").delete().eq("id", req.userId)).error;
    if (userError) throw new Error(`account deletion (user): ${userError.message}`);

    res.json({ ok: true });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/db/account/export — GDPR-style data export ───────────────────

router.get("/account/export", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const { data: projects, error: projectsError } = await supabase
      .from("projects")
      .select("*")
      .eq("user_id", req.userId);

    if (projectsError) throw new Error(`export (projects): ${projectsError.message}`);

    const projectRows = (projects ?? []) as DbProject[];
    const projectIds = projectRows.map((p) => p.id);

    let messages: Array<Record<string, unknown>> = [];
    const filter =
      projectIds.length > 0
        ? `user_id.eq.${req.userId},project_id.in.(${projectIds.join(",")})`
        : `user_id.eq.${req.userId}`;

    const { data: chatRows, error: chatError } = await supabase
      .from("chat_messages")
      .select("*")
      .or(filter)
      .order("created_at", { ascending: true });

    if (chatError) {
      logger.warn({ err: chatError.message }, "export (messages) failed");
    }
    messages = (chatRows ?? []) as Array<Record<string, unknown>>;

    res.json({ projects: projectRows, messages });
  } catch (error) {
    next(error);
  }
});

export default router;
