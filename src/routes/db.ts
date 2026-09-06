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

// ─── GET /api/db/templates — the Templates tab listing (Task 50) ──────────
// Every PUBLIC project, newest first. Public is the DB default, so all
// projects appear here until the owner flips them private. The creator's
// email comes from public.users; contributorsCount is a PostgREST count
// embed on project_contributors.
//
// AUTH-EXEMPT (registered BEFORE the requireAuth middleware): public
// templates are public by definition — anonymous visitors on the landing
// page browse the same grid; isYours is simply false without a session.

router.get("/templates", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const { data, error } = await supabase
      .from("projects")
      .select("id,user_id,name,description,logo_url,platforms,updated_at,project_contributors(count)")
      .eq("visibility", "public")
      .order("updated_at", { ascending: false })
      .limit(60);

    if (error) throw new Error(`templates list: ${error.message}`);

    type TemplateRow = {
      id: string;
      user_id: string;
      name: string;
      description: string | null;
      logo_url: string | null;
      platforms: string | string[] | null;
      updated_at: string;
      project_contributors: Array<{ count: number }> | null;
    };
    const rows = (data ?? []) as TemplateRow[];

    // Creator emails in ONE users query (avoids 60 sequential round-trips).
    const creatorIds = Array.from(new Set(rows.map((r) => r.user_id)));
    const emailById = new Map<string, string>();
    if (creatorIds.length > 0) {
      const { data: userRows, error: userError } = await supabase
        .from("users")
        .select("id,email")
        .in("id", creatorIds);
      if (userError) {
        logger.warn({ err: userError.message }, "templates list: creator emails lookup failed");
      } else {
        for (const u of (userRows ?? []) as Array<{ id: string; email: string | null }>) {
          if (u.email) emailById.set(u.id, u.email);
        }
      }
    }

    res.json({
      templates: rows.map((row) => ({
        id: row.id,
        name: row.name,
        description: row.description,
        logoUrl: row.logo_url,
        platforms: toPlatformsArray(row.platforms),
        updatedAt: row.updated_at,
        creatorEmail: emailById.get(row.user_id) ?? null,
        contributorsCount: row.project_contributors?.[0]?.count ?? 0,
        isYours: row.user_id === req.userId,
      })),
    });
  } catch (error) {
    next(error);
  }
});

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

/** Contributor row (migration 005) as stored by PostgREST. */
type ContributorRow = {
  user_id: string;
  user_email: string | null;
  contributed_at: string;
};

/** Uniform contributor shape for the templates + template-session APIs. */
function toContributor(row: ContributorRow) {
  return {
    userId: row.user_id,
    email: row.user_email ?? null,
    contributedAt: row.contributed_at,
  };
}

/** PostgREST .eq("id", <uuid>) 400s on garbage — pre-validate ids. */
function isUuid(value: unknown): value is string {
  return typeof value === "string" && /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
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

// ─── PUT /api/db/projects/:id — update a project (owner-only) ──────────────
// Owner-scoped update (Task 50): name / logo / platforms / description /
// session linkage AND visibility ("public" | "private"). The user_id filter
// in the UPDATE means a foreign project simply matches zero rows → 404.

router.put("/projects/:id", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const projectId = String(req.params.id || "");
    if (!isUuid(projectId)) {
      res.status(404).json({ error: "Project not found" });
      return;
    }

    const body = req.body ?? {};
    const patch: Record<string, unknown> = {};

    if (typeof body.name === "string" && body.name.trim()) patch.name = body.name.trim();
    if (body.logoUrl !== undefined) {
      patch.logo_url = typeof body.logoUrl === "string" && body.logoUrl ? body.logoUrl : null;
    }
    if (body.platforms !== undefined) patch.platforms = toPlatformsColumn(body.platforms);
    if (typeof body.description === "string") patch.description = body.description;
    if (typeof body.sessionId === "string" && body.sessionId) patch.session_id = body.sessionId;
    if (body.visibility !== undefined) {
      if (body.visibility !== "public" && body.visibility !== "private") {
        res.status(400).json({ error: "visibility must be 'public' or 'private'" });
        return;
      }
      patch.visibility = body.visibility;
    }

    if (Object.keys(patch).length === 0) {
      res.status(400).json({ error: "No updatable fields provided" });
      return;
    }

    const { data, error } = await supabase
      .from("projects")
      .update({ ...patch, updated_at: new Date().toISOString() })
      .eq("id", projectId)
      .eq("user_id", req.userId)
      .select("id,name,logo_url,platforms,session_id,description,visibility,updated_at")
      .maybeSingle();

    if (error) throw new Error(`update project: ${error.message}`);
    if (!data) {
      res.status(404).json({ error: "Project not found (or not yours)" });
      return;
    }

    res.json({
      project: {
        id: data.id,
        name: data.name,
        logoUrl: data.logo_url,
        platforms: toPlatformsArray(data.platforms),
        sessionId: data.session_id,
        description: data.description,
        visibility: data.visibility,
        updatedAt: data.updated_at,
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/db/templates/:projectId — template detail page (Task 50) ────
// Public (or owned-by-caller) project + creator + contributors. The response
// carries the sandbox coordinates so the studio can open the SHARED code.

router.get("/templates/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const projectId = String(req.params.projectId || "");
    if (!isUuid(projectId)) {
      res.status(404).json({ error: "Template not found" });
      return;
    }

    const { data: project, error } = await supabase
      .from("projects")
      .select("id,user_id,name,description,logo_url,session_id,sandbox_id,visibility")
      .eq("id", projectId)
      .maybeSingle();

    if (error) throw new Error(`template lookup: ${error.message}`);
    if (!project) {
      res.status(404).json({ error: "Template not found" });
      return;
    }
    if (project.visibility !== "public" && project.user_id !== req.userId) {
      // Owner may inspect their own (private) template page; everyone else 403.
      res.status(403).json({ error: "Forbidden — project belongs to another user" });
      return;
    }

    // Creator identity.
    const { data: creatorRow, error: creatorError } = await supabase
      .from("users")
      .select("id,email")
      .eq("id", project.user_id)
      .maybeSingle();
    if (creatorError) {
      logger.warn({ err: creatorError.message }, "template detail: creator lookup failed");
    }

    // Contributors (chronological — the collaboration story of the project).
    const { data: contribRows, error: contribError } = await supabase
      .from("project_contributors")
      .select("user_id,user_email,contributed_at")
      .eq("project_id", project.id)
      .order("contributed_at", { ascending: true });
    if (contribError) {
      logger.warn({ err: contribError.message }, "template detail: contributors lookup failed");
    }

    res.json({
      template: {
        id: project.id,
        name: project.name,
        description: project.description,
        logoUrl: project.logo_url,
        sessionId: project.session_id,
        sandboxId: project.sandbox_id,
        visibility: project.visibility,
        creator: {
          userId: project.user_id,
          email: (creatorRow as { email: string | null } | null)?.email ?? null,
        },
        contributors: ((contribRows ?? []) as ContributorRow[]).map(toContributor),
      },
    });
  } catch (error) {
    next(error);
  }
});

// ─── GET /api/db/sessions/:sessionId — session detail + chat history ───────
// Task 50: owner path (unchanged shape, per-user messages) + TEMPLATE path
// — a public project opened by a non-owner returns the SAME shape with
// messages: [] (their own chat starts fresh; the creator's chat is NEVER
// leaked) plus isTemplate/owner/contributors.

router.get("/sessions/:sessionId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const SESSION_COLUMNS = "id,user_id,name,logo_url,platforms,session_id,sandbox_id,visibility";
    type SessionProject = {
      id: string;
      user_id: string;
      name: string;
      logo_url: string | null;
      platforms: string | string[] | null;
      session_id: string | null;
      sandbox_id: string | null;
      visibility: string;
    };

    // ── 1. OWNER PATH (unchanged shape): the caller's own project by
    // session_id first, then by project UUID.
    let { data: project, error } = await supabase
      .from("projects")
      .select(SESSION_COLUMNS)
      .eq("session_id", req.params.sessionId)
      .eq("user_id", req.userId)
      .maybeSingle();

    if (error) throw new Error(`session lookup: ${error.message}`);

    if (!project && isUuid(req.params.sessionId)) {
      const fallback = await supabase
        .from("projects")
        .select(SESSION_COLUMNS)
        .eq("id", req.params.sessionId)
        .eq("user_id", req.userId)
        .maybeSingle();
      if (fallback.error) throw new Error(`session lookup: ${fallback.error.message}`);
      project = fallback.data;
    }

    if (project) {
      const owned = project as SessionProject;

      // Chat history (most recent 200 messages, chronological) — PER-USER
      // chats now: the creator sees only their own conversation. Legacy rows
      // written before per-user chats (user_id NULL — only the owner could
      // write then) are still shown to the owner so nobody loses history.
      let messages: Array<{ role: string; content: string }> = [];
      try {
        const { data: chatRows, error: chatError } = await supabase
          .from("chat_messages")
          .select("role,content,created_at")
          .eq("project_id", owned.id)
          .or(`user_id.eq.${req.userId},user_id.is.null`)
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

      res.json({
        session: {
          id: owned.session_id || owned.id,
          title: owned.name,
          project: {
            id: owned.id,
            name: owned.name,
            logoUrl: owned.logo_url,
            platforms: toPlatformsArray(owned.platforms),
            sandboxId: owned.sandbox_id,
            visibility: owned.visibility,
          },
          messages,
        },
      });
      return;
    }

    // ── 2. TEMPLATE PATH (Task 50): not the owner's project — look the
    // session/project up WITHOUT the user filter. Public → serve the SHARED
    // project with an EMPTY chat (the caller's own chat starts fresh; the
    // creator's chat must NEVER leak). Private/foreign → existing 404.
    let shared: SessionProject | null = null;
    const sharedSession = await supabase
      .from("projects")
      .select(SESSION_COLUMNS)
      .eq("session_id", req.params.sessionId)
      .maybeSingle();
    if (sharedSession.error) {
      throw new Error(`session lookup: ${sharedSession.error.message}`);
    }
    shared = (sharedSession.data ?? null) as SessionProject | null;

    if (!shared && isUuid(req.params.sessionId)) {
      const byId = await supabase
        .from("projects")
        .select(SESSION_COLUMNS)
        .eq("id", req.params.sessionId)
        .maybeSingle();
      if (byId.error) throw new Error(`session lookup: ${byId.error.message}`);
      shared = (byId.data ?? null) as SessionProject | null;
    }

    if (!shared || shared.visibility !== "public") {
      res.status(404).json({ error: "Session not found" });
      return;
    }

    // Contributors (chronological) — the collaboration story of the template.
    let contributors: Array<ReturnType<typeof toContributor>> = [];
    try {
      const { data: contribRows, error: contribError } = await supabase
        .from("project_contributors")
        .select("user_id,user_email,contributed_at")
        .eq("project_id", shared.id)
        .order("contributed_at", { ascending: true });
      if (contribError) {
        logger.warn({ err: contribError.message }, "template session: contributors lookup failed");
      } else {
        contributors = ((contribRows ?? []) as ContributorRow[]).map(toContributor);
      }
    } catch (err: unknown) {
      logger.warn({ err: err instanceof Error ? err.message : err }, "template session: contributors lookup failed");
    }

    res.json({
      session: {
        id: shared.session_id || shared.id,
        title: shared.name,
        project: {
          id: shared.id,
          name: shared.name,
          logoUrl: shared.logo_url,
          platforms: toPlatformsArray(shared.platforms),
          sandboxId: shared.sandbox_id,
          visibility: shared.visibility,
        },
        // The caller's OWN chat on the shared project — starts fresh. The
        // real history (if any) is loaded per-user via the db-ops
        // get-messages action with the user_id filter.
        messages: [],
        isTemplate: true,
        owner: false,
        contributors,
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

// ─── GET /api/db/messages/:projectId — per-user chat persistence ───────────
// Architecture note: the db-ops EDGE function forwards here. Only THIS
// backend touches the Supabase database — the edge function is a pure
// proxy (user mandate). The logic mirrors the previous edge-side
// implementation exactly: the owner OR any authenticated user on a PUBLIC
// project (templates) may read/write; chats are PER-USER (the creator's
// conversation never displays to anyone else); a non-owner's chat counts
// as a contribution (project_contributors upsert, best-effort).
router.get("/messages/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const projectId = String(req.params.projectId || "");
    if (!isUuid(projectId)) {
      res.status(400).json({ error: "A valid projectId is required." });
      return;
    }

    const access = await resolveProjectAccess(supabase, projectId, req.userId);
    if (!access) {
      res.status(403).json({ error: "Forbidden" });
      return;
    }

    const { data: rows, error } = await supabase
      .from("chat_messages")
      .select("role,content,meta,created_at")
      .eq("project_id", projectId)
      // Owner also matches legacy user_id-NULL rows (pre-per-user era);
      // non-owners are matched STRICTLY — the creator's chat never leaks.
      .or(
        access.isOwner
          ? `user_id.eq.${req.userId},user_id.is.null`
          : `user_id.eq.${req.userId}`,
      )
      .order("created_at", { ascending: true })
      .limit(2000);

    if (error) throw new Error(`messages read: ${error.message}`);

    res.json({
      messages: ((rows ?? []) as Array<ChatMessageRow & { meta: unknown }>).map((m) => ({
        role: m.role,
        content: m.content,
        meta: m.meta ?? null,
      })),
    });
  } catch (error) {
    next(error);
  }
});

// ─── PUT /api/db/messages/:projectId — replace the CALLER's conversation ───
// The frontend always sends the full current message list — delete the
// caller's rows then insert (idempotent). project_id and user_id are
// forced SERVER-side; only role/content/meta come from the payload.
router.put("/messages/:projectId", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const supabase = requireSupabase(res);
    if (!supabase) return;

    const projectId = String(req.params.projectId || "");
    if (!isUuid(projectId)) {
      res.status(400).json({ error: "A valid projectId is required." });
      return;
    }

    const access = await resolveProjectAccess(supabase, projectId, req.userId);
    if (!access) {
      res.status(403).json({ error: "Forbidden" });
      return;
    }

    const raw = (req.body || {}) as { messages?: unknown };
    if (!Array.isArray(raw.messages)) {
      res.status(400).json({ error: "messages must be an array" });
      return;
    }
    // Sanitize rows — only role/content/meta are stored.
    const rows: Array<{ role: string; content: string; meta: Record<string, unknown> | null }> = [];
    for (const m of raw.messages) {
      if (!m || typeof m !== "object") continue;
      const obj = m as Record<string, unknown>;
      const role = typeof obj.role === "string" ? obj.role.slice(0, 32) : "assistant";
      const content = typeof obj.content === "string" ? obj.content.slice(0, 200_000) : "";
      const meta =
        obj.meta && typeof obj.meta === "object" && !Array.isArray(obj.meta)
          ? (obj.meta as Record<string, unknown>)
          : null;
      rows.push({ role, content, meta });
    }

    // 1. Delete ONLY the caller's existing messages for this project.
    //    (Filters AND-compose: project_id = X AND (user_id = caller [OR
    //    user_id IS NULL for the owner's legacy rows]).)
    let del = supabase
      .from("chat_messages")
      .delete()
      .eq("project_id", projectId)
      .eq("user_id", req.userId);
    if (access.isOwner) {
      // The owner's scope also clears legacy user_id-NULL rows — rows they
      // are about to rewrite with their user_id stamped.
      del = supabase
        .from("chat_messages")
        .delete()
        .eq("project_id", projectId)
        .or(`user_id.eq.${req.userId},user_id.is.null`);
    }
    const delRes = await del;
    if (delRes.error) throw new Error(`messages clear: ${delRes.error.message}`);

    // 2. Insert the caller's new conversation with their user_id stamped.
    if (rows.length > 0) {
      const ins = await supabase
        .from("chat_messages")
        .insert(
          rows.map((r) => ({
            project_id: projectId,
            user_id: req.userId,
            role: r.role,
            content: r.content,
            meta: r.meta,
          })),
        );
      if (ins.error) throw new Error(`messages write: ${ins.error.message}`);
    }

    // 3. Contributor upsert — a non-owner's chat IS their contribution
    //    (best-effort: never fails the save).
    if (!access.isOwner) {
      try {
        await supabase
          .from("project_contributors")
          .upsert(
            {
              project_id: projectId,
              user_id: req.userId,
              user_email: req.userEmail ?? null,
              contributed_at: new Date().toISOString(),
            },
            { onConflict: "project_id,user_id" },
          );
      } catch (err) {
        logger.warn(
          { err: err instanceof Error ? err.message : err },
          "contributor upsert failed (non-fatal)",
        );
      }
    }

    res.json({ ok: true, count: rows.length });
  } catch (error) {
    next(error);
  }
});

/** Shared access probe for the per-user chat routes: the project must
 *  exist AND the caller must be the owner or the project must be public. */
async function resolveProjectAccess(
  supabase: ReturnType<typeof getServiceSupabase>,
  projectId: string,
  userId: string | undefined,
): Promise<{ isOwner: boolean } | null> {
  if (!userId) return null;
  const { data, error } = await supabase
    .from("projects")
    .select("id,user_id,visibility")
    .eq("id", projectId)
    .maybeSingle();
  if (error) {
    logger.warn({ err: error.message }, "messages access probe failed");
    return null;
  }
  if (!data) return null;
  const row = data as { id: string; user_id: string; visibility: string };
  const isOwner = row.user_id === userId;
  if (!isOwner && row.visibility !== "public") return null;
  return { isOwner };
}

export default router;
