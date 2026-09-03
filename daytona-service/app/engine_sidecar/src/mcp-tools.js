/**
 * Forgvi Engine — MCP capability surface for Forgvi 2.0 (GitHub + Supabase),
 * the same tool surface Forgvi 1.0's in-VM swarm exposes (github_node +
 * supabase_mcp), implemented engine-side so 2.0 runs get connector parity.
 *
 * GitHub   (GITHUB_TOKEN)   actions: rest | sync_workspace
 * Supabase (SUPABASE_ACCESS_TOKEN + SUPABASE_PROJECT_REF)
 *          tools: execute_sql | apply_migration | list_tables | list_projects
 *
 * Both mirror 1.0's honest "needs configuration" outcomes: an unset token
 * returns a structured needs-config result (never a crash), so the chief can
 * tell the user exactly which env var to set. Tokens are read per call from
 * the environment — never stored, never logged.
 *
 * sync_workspace commits the run's workspace to a repo through the GitHub
 * git DATA API (blobs → tree → commit → ref) — no clone, no credentials in
 * the VM, works identically for VM-bound and local-disk runs.
 */

const GITHUB_API = process.env.GITHUB_API_BASE?.replace(/\/$/, "") || "https://api.github.com";
const SUPABASE_API = process.env.SUPABASE_API_BASE?.replace(/\/$/, "") || "https://api.supabase.com";

const USER_AGENT = "forgvi-engine/2.0";

/** JSON fetch helper with timeouts + honest error surfaces. */
async function jsonFetch(url, { method = "GET", body, headers = {}, timeoutMs = 60_000 } = {}) {
  const res = await fetch(url, {
    method,
    headers: {
      "User-Agent": USER_AGENT,
      Accept: "application/json",
      ...(body !== undefined ? { "Content-Type": "application/json" } : {}),
      ...headers,
    },
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    /* non-JSON body */
  }
  return { status: res.status, ok: res.ok, data, text };
}

// ── GitHub ─────────────────────────────────────────────────────────────

/** One GitHub REST call with the engine's token. */
export async function githubRest({ method = "GET", path = "", body } = {}) {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      ok: false,
      needs_config: "GITHUB_TOKEN",
      hint: "Set GITHUB_TOKEN on the engine (a GitHub PAT with repo scope) to enable GitHub operations.",
    };
  }
  const cleanPath = String(path ?? "").replace(/^\/+/, "");
  if (!cleanPath) return { ok: false, error: "github: `path` is required (e.g. /repos/owner/name/contents)" };
  const res = await jsonFetch(`${GITHUB_API}/${cleanPath}`, {
    method: String(method ?? "GET").toUpperCase(),
    body,
    headers: {
      Authorization: `Bearer ${token}`,
      "X-GitHub-Api-Version": "2022-11-28",
    },
    timeoutMs: 90_000,
  });
  if (!res.ok) {
    return { ok: false, status: res.status, error: `github: HTTP ${res.status}`, detail: (res.text ?? "").slice(0, 400) };
  }
  return { ok: true, status: res.status, data: res.data };
}

/** List the workspace files (VM via find, or local dir). Excludes build junk. */
async function listWorkspaceFiles({ vm, localCwd }) {
  const EXCLUDE = "node_modules .next .git dist .turbo coverage __pycache__ .cache";
  if (vm) {
    const run = await vm.exec(
      `cd /workspace 2>/dev/null || cd .; find . -type f $(printf -- "-name %s -o " ${EXCLUDE.split(" ").map((e) => JSON.stringify(e)).join(" ")}) -name .keep -prune -o -type f -print | grep -vE "node_modules|\\.next/|\\.git/|dist/|\\.turbo/|coverage/|__pycache__|\\.cache/" | head -400`,
      { timeoutMs: 30_000 },
    );
    if (run.exitCode !== 0 && !run.stdout.trim()) {
      return { ok: false, error: `workspace listing failed: ${(run.stderr || run.stdout).slice(0, 300)}` };
    }
    return { ok: true, files: run.stdout.split("\n").map((s) => s.trim()).filter(Boolean).map((p) => p.replace(/^\.\//, "")) };
  }
  if (localCwd) {
    const { execSync } = await import("node:child_process");
    try {
      const out = execSync(
        `find . -type f | grep -vE "node_modules|\\.next/|\\.git/|dist/|\\.turbo/|coverage/|__pycache__|\\.cache/" | head -400`,
        { cwd: localCwd, encoding: "utf-8", timeout: 30_000 },
      );
      return { ok: true, files: out.split("\n").map((s) => s.trim()).filter(Boolean).map((p) => p.replace(/^\.\//, "")) };
    } catch (error) {
      return { ok: false, error: `workspace listing failed: ${String(error?.message ?? error).slice(0, 300)}` };
    }
  }
  return { ok: false, error: "github: no workspace bound to this run" };
}

/**
 * Commit the entire workspace to a repo through the git data API.
 * {repo: "owner/name", message, branch? (default: repo default branch)}
 */
export async function githubSyncWorkspace({ repo, message, branch }, ctx = {}) {
  const token = process.env.GITHUB_TOKEN;
  if (!token) {
    return {
      ok: false,
      needs_config: "GITHUB_TOKEN",
      hint: "Set GITHUB_TOKEN on the engine to enable workspace sync.",
    };
  }
  const repoPath = String(repo ?? "").replace(/^\/+|\/+$/g, "");
  if (!/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repoPath)) {
    return { ok: false, error: "github: `repo` must look like owner/name" };
  }
  const headers = {
    Authorization: `Bearer ${token}`,
    "X-GitHub-Api-Version": "2022-11-28",
  };
  const commitMessage = String(message ?? "Forgevi 2.0 workspace sync").slice(0, 200);

  // 0. Repo meta (default branch).
  const meta = await jsonFetch(`${GITHUB_API}/repos/${repoPath}`, { headers });
  if (!meta.ok) return { ok: false, error: `github: repo ${repoPath} HTTP ${meta.status}` };
  const targetBranch = String(branch ?? meta.data?.default_branch ?? "main");

  // 1. Branch head (base commit).
  const ref = await jsonFetch(`${GITHUB_API}/repos/${repoPath}/git/ref/heads/${targetBranch}`, { headers });
  if (!ref.ok) {
    return { ok: false, error: `github: branch ${targetBranch} not reachable (HTTP ${ref.status}) — push the branch first or use the default branch` };
  }
  const baseCommitSha = ref.data?.object?.sha;

  // 2. Workspace files.
  const listing = await listWorkspaceFiles(ctx);
  if (!listing.ok) return { ok: false, error: listing.error };
  if (listing.files.length === 0) return { ok: false, error: "github: workspace is empty — nothing to sync" };

  // 3. Blobs (parallel batches of 8).
  const tree = [];
  const BATCH = 8;
  for (let i = 0; i < listing.files.length; i += BATCH) {
    const batch = listing.files.slice(i, i + BATCH);
    const blobs = await Promise.all(
      batch.map(async (rel) => {
        try {
          let content;
          if (ctx.vm) {
            content = await ctx.vm.readFile(rel.startsWith("/workspace/") ? rel : `/workspace/${rel}`);
          } else {
            const { readFile } = await import("node:fs/promises");
            const { resolve } = await import("node:path");
            content = await readFile(resolve(ctx.localCwd, rel), "utf-8");
          }
          const blob = await jsonFetch(`${GITHUB_API}/repos/${repoPath}/git/blobs`, {
            method: "POST",
            headers,
            body: { content, encoding: "utf-8" },
            timeoutMs: 90_000,
          });
          if (!blob.ok) return { rel, error: `blob HTTP ${blob.status}` };
          return { rel, sha: blob.data?.sha };
        } catch (error) {
          return { rel, error: String(error?.message ?? error).slice(0, 200) };
        }
      }),
    );
    for (const b of blobs) {
      if (b.error) continue; // unreadable files are skipped, not fatal
      tree.push({ path: b.rel.replace(/^\/+/, ""), mode: "100644", type: "blob", sha: b.sha });
    }
  }
  if (tree.length === 0) return { ok: false, error: "github: no readable files to sync" };

  // 4. Tree.
  const treeRes = await jsonFetch(`${GITHUB_API}/repos/${repoPath}/git/trees`, {
    method: "POST",
    headers,
    body: { base_tree: baseCommitSha, tree },
    timeoutMs: 90_000,
  });
  if (!treeRes.ok) return { ok: false, error: `github: tree HTTP ${treeRes.status}`, detail: (treeRes.text ?? "").slice(0, 300) };

  // 5. Commit + 6. ref update.
  const commit = await jsonFetch(`${GITHUB_API}/repos/${repoPath}/git/commits`, {
    method: "POST",
    headers,
    body: { message: commitMessage, tree: treeRes.data?.sha, parents: [baseCommitSha] },
  });
  if (!commit.ok) return { ok: false, error: `github: commit HTTP ${commit.status}` };
  const pushed = await jsonFetch(`${GITHUB_API}/repos/${repoPath}/git/refs/heads/${targetBranch}`, {
    method: "PATCH",
    headers,
    body: { sha: commit.data?.sha, force: false },
  });
  if (!pushed.ok) return { ok: false, error: `github: ref update HTTP ${pushed.status} (branch moved? sync again)` };
  return {
    ok: true,
    repo: repoPath,
    branch: targetBranch,
    commit: commit.data?.sha,
    files: tree.length,
    url: `https://github.com/${repoPath}/commit/${commit.data?.sha}`,
  };
}

// ── Supabase (Management API — same tool names as 1.0) ────────────────

/** One Supabase tool call. Mirrors 1.0's supabase_mcp surface. */
export async function supabaseMcp({ tool, args } = {}) {
  const accessToken = process.env.SUPABASE_ACCESS_TOKEN;
  const projectRef = process.env.SUPABASE_PROJECT_REF;
  const headers = accessToken ? { Authorization: `Bearer ${accessToken}` } : {};
  const name = String(tool ?? "").trim();
  const a = args && typeof args === "object" ? args : {};

  if (name === "list_projects") {
    if (!accessToken) return { ok: false, needs_config: "SUPABASE_ACCESS_TOKEN" };
    const res = await jsonFetch(`${SUPABASE_API}/v1/projects`, { headers, timeoutMs: 30_000 });
    if (!res.ok) return { ok: false, status: res.status, error: `supabase: HTTP ${res.status}` };
    return {
      ok: true,
      projects: (res.data ?? []).map((p) => ({ id: p.id, ref: p.ref, name: p.name, region: p.region, status: p.status })),
    };
  }

  if (!accessToken || !projectRef) {
    return {
      ok: false,
      needs_config: [!accessToken && "SUPABASE_ACCESS_TOKEN", !projectRef && "SUPABASE_PROJECT_REF"].filter(Boolean).join(" + "),
      hint: "Set SUPABASE_ACCESS_TOKEN (sbp_ service token) and SUPABASE_PROJECT_REF on the engine.",
    };
  }
  const base = `${SUPABASE_API}/v1/projects/${projectRef}`;

  // The /database/query endpoint (verified live on this token class) is the
  // workhorse: tables + migrations route through SQL because the dedicated
  // Management endpoints are permission-gated on some sbp_ tokens.
  const sqlLiteral = (value) => `'${String(value).replace(/'/g, "''")}'`;
  const sqlQuery = async (query, timeoutMs = 60_000) => {
    const res = await jsonFetch(`${base}/database/query`, {
      method: "POST",
      headers,
      body: { query },
      timeoutMs,
    });
    if (!res.ok) return { ok: false, status: res.status, error: `supabase: HTTP ${res.status}`, detail: (res.text ?? "").slice(0, 400) };
    return { ok: true, rows: Array.isArray(res.data) ? res.data : [] };
  };

  switch (name) {
    case "list_tables": {
      const res = await sqlQuery(
        `SELECT table_name, string_agg(column_name || ' ' || data_type, ', ' ORDER BY ordinal_position) AS columns
         FROM information_schema.columns WHERE table_schema = 'public'
         GROUP BY table_name ORDER BY table_name`,
        30_000,
      );
      if (!res.ok) return res;
      return {
        ok: true,
        tables: (res.rows ?? []).map((r) => ({ schema: "public", name: r.table_name, columns: String(r.columns ?? "").split(", ") })),
      };
    }
    case "execute_sql": {
      const query = String(a.query ?? a.sql ?? "").trim();
      if (!query) return { ok: false, error: "supabase: execute_sql needs `query`" };
      return sqlQuery(query);
    }
    case "apply_migration": {
      const query = String(a.query ?? a.sql ?? "").trim();
      const migrationName = String(a.name ?? `forgvi_${Date.now()}`).replace(/[^a-z0-9_]/gi, "_").slice(0, 60);
      if (!query) return { ok: false, error: "supabase: apply_migration needs `query`" };
      // Versioned bookkeeping: a private migrations table records what ran.
      const run = await sqlQuery(
        `CREATE TABLE IF NOT EXISTS _forgvi_migrations (
           id bigint generated always as identity primary key,
           name text unique,
           applied_at timestamptz default now()
         );`,
      );
      if (!run.ok) return run;
      const already = await sqlQuery(`SELECT name FROM _forgvi_migrations WHERE name = ${sqlLiteral(migrationName)}`);
      if (already.ok && (already.rows ?? []).length > 0) {
        return { ok: true, applied: migrationName, already_applied: true };
      }
      const applied = await sqlQuery(query);
      if (!applied.ok) return applied;
      const record = await sqlQuery(
        `INSERT INTO _forgvi_migrations (name) VALUES (${sqlLiteral(migrationName)}) ON CONFLICT (name) DO NOTHING`,
      );
      if (!record.ok) {
        return { ok: true, applied: migrationName, warning: "migration ran but bookkeeping failed" };
      }
      return { ok: true, applied: migrationName };
    }
    default:
      return {
        ok: false,
        error: "supabase: unknown tool — use execute_sql | apply_migration | list_tables | list_projects",
      };
  }
}
