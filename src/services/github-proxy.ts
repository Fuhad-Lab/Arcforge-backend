/**
 * GitHub tunnel proxy — the `/tunnel/github` branch of the reverse tunnel.
 *
 * WHY THIS EXISTS
 * ───────────────
 * The in-VM agent orchestrator's `github` tool (contract C3) needs to call
 * api.github.com on the user's behalf (list repos, create a repo, sync the
 * workspace…). The user's GitHub PAT lives ONLY in this backend's settings
 * table (users.github_pat — write-only, never returned by /settings and
 * never sent to the VM). Daytona's EU egress filter blocks direct
 * api.github.com calls for most paths anyway, and the PAT must never enter
 * the VM — so the calls are bridged through the SAME reverse tunnel the
 * LLM traffic uses:
 *
 *   VM tool → localhost:7777/tunnel/github → req frame over the tunnel WS
 *          → this backend routes by path prefix → api.github.com (PAT
 *            injected server-side) → res/chunk/done frames flow back.
 *
 * FRAME BODY (the req frame's `body` — a JSON string from the VM):
 *   {"action":"rest","method":"GET","path":"/user/repos","body":{...}}
 *   {"action":"sync_workspace","repo":"owner/name"|"auto-create:<name>","message":"..."}
 *
 * IDENTITY: the tunnel connection carries the caller's `sandboxId` —
 * sandbox → project (getProjectRowBySandbox) → user → settings.github_pat.
 * No PAT means an honest ok:false JSON the agent surfaces to the user via
 * ask_user.
 *
 * RESULT CONVENTION (matches the tool layer):
 *   - Business outcomes — including GitHub 4xx/5xx bodies, a missing PAT,
 *     and fetch failures — are returned as HTTP-ish responses with the
 *     upstream status + body (for `rest`) or as `{"ok":false,"error":…}`
 *     JSON envelopes with status 200. The tool layer reports ok:false.
 *   - `error` frames are reserved for transport failures of the tunnel
 *     itself (the outer handleReqFrame catch) — never for GitHub business
 *     errors.
 *
 * EVENT SHAPE: identical to `forwardToNvidia` in nvidia-forwarder.ts —
 * `{kind:"head", status, headers}` first, then `{kind:"chunk", body}`.
 */
import { logger } from "../lib/logger";
import { SKIP_DIRS, TEXT_EXTENSIONS, TEXT_FILENAMES } from "./github-filters";
import { getProjectRowBySandbox } from "../lib/project-lookup";
import { getServiceSupabase, isSupabaseConfigured } from "../lib/supabase-db";
import { getWorkspaceFileTree, proxyToDaytona } from "./daytona-workspace";

// ─── Types ──────────────────────────────────────────────────────────────

/** Same shape as NvidiaForwardEvent (kept structural so the tunnel
 *  handlers' mirror-send logic is identical for both forwarders). */
export type GithubForwardEvent =
  | { kind: "head"; status: number; headers: Record<string, string> }
  | { kind: "chunk"; body: string };

/** Minimal connection identity — both tunnel handlers satisfy this. */
export interface GithubTunnelConn {
  sandboxId: string;
}

export interface GithubTunnelFrame {
  id?: string;
  method?: string;
  path?: string;
  headers?: Record<string, string>;
  body?: string;
}

type RestAction = {
  action: "rest";
  method?: string;
  path?: string;
  body?: unknown;
};

type SyncAction = {
  action: "sync_workspace";
  repo?: string;
  message?: string;
};

type GithubAction = RestAction | SyncAction | { action?: string };

// ─── Constants ──────────────────────────────────────────────────────────

const GITHUB_API_BASE = "https://api.github.com";
/** Max bytes of a GitHub response forwarded back through the tunnel. */
const MAX_BODY_BYTES = 512 * 1024;
/** sync_workspace caps (task contract): ≤400 files, ≤2MB total. */
const SYNC_MAX_FILES = 400;
const SYNC_MAX_TOTAL_BYTES = 2 * 1024 * 1024;
/** Contents-API commit concurrency (task contract). */
const SYNC_CONCURRENCY = 4;
/** Per-call GitHub fetch timeout. */
const GH_FETCH_TIMEOUT_MS = 30_000;
/** Per-file VM read timeout (through the daytona-service). */
const VM_READ_TIMEOUT_MS = 30_000;
/** File-tree depth when scanning the VM for sync_workspace. */
const SYNC_TREE_DEPTH = 10;

// Text-file / skip-dir filters live in ./github-filters (shared with the
// GROUP 3 repository-import route — one definition of "importable text").

// ─── Helpers ────────────────────────────────────────────────────────────

/** A complete JSON result envelope: head 200 + one body chunk. */
async function* jsonEvents(payload: unknown): AsyncGenerator<GithubForwardEvent, void, void> {
  yield { kind: "head", status: 200, headers: { "content-type": "application/json" } };
  yield { kind: "chunk", body: JSON.stringify(payload) };
}

/** A GitHub-API-shaped fetch with the PAT injected server-side. */
async function ghFetch(
  pat: string,
  apiPath: string,
  method = "GET",
  body?: unknown,
): Promise<Response> {
  const hasBody = body !== undefined && body !== null && method !== "GET" && method !== "HEAD";
  return fetch(`${GITHUB_API_BASE}${apiPath}`, {
    method,
    headers: {
      Authorization: `Bearer ${pat}`,
      Accept: "application/vnd.github+json",
      "X-GitHub-Api-Version": "2022-11-28",
      "User-Agent": "arcforge-backend",
      ...(hasBody ? { "Content-Type": "application/json" } : {}),
    },
    body: hasBody ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(GH_FETCH_TIMEOUT_MS),
  });
}

/** Encode a repo-relative path for the contents API (per-segment). */
function encodeGitPath(path: string): string {
  return path.split("/").map(encodeURIComponent).join("/");
}

/**
 * Read the response body capped at MAX_BODY_BYTES — truncated bodies get
 * an appended note so the agent knows the payload was cut short.
 */
async function readCapped(res: Response): Promise<string> {
  const reader = res.body?.getReader();
  if (!reader) {
    // No stream API available (or empty body) — fall back to text().
    return capText(await res.text());
  }
  const parts: Buffer[] = [];
  let received = 0;
  for (;;) {
    const { value: bytes, done } = await reader.read();
    if (done) break;
    if (!bytes || bytes.length === 0) continue;
    const room = MAX_BODY_BYTES - received;
    if (room <= 0) {
      try {
        await reader.cancel();
      } catch {
        /* noop — already closed */
      }
      return `${Buffer.concat(parts).toString("utf-8")}\n[arcforge: response truncated at ${MAX_BODY_BYTES} bytes]`;
    }
    if (bytes.length > room) {
      parts.push(Buffer.from(bytes.subarray(0, room)));
      received = MAX_BODY_BYTES;
      try {
        await reader.cancel();
      } catch {
        /* noop */
      }
      return `${Buffer.concat(parts).toString("utf-8")}\n[arcforge: response truncated at ${MAX_BODY_BYTES} bytes]`;
    }
    parts.push(Buffer.from(bytes));
    received += bytes.length;
  }
  return Buffer.concat(parts).toString("utf-8");
}

/** Text()-based fallback cap. */
function capText(text: string): string {
  const bytes = Buffer.byteLength(text, "utf-8");
  if (bytes <= MAX_BODY_BYTES) return text;
  return `${Buffer.from(text, "utf-8").subarray(0, MAX_BODY_BYTES).toString("utf-8")}\n[arcforge: response truncated at ${MAX_BODY_BYTES} bytes]`;
}

/** Strip hop-by-hop headers the VM does not need (mirrors nvidia-forwarder). */
function forwardableHeaders(res: Response): Record<string, string> {
  const out: Record<string, string> = {};
  res.headers.forEach((value, name) => {
    const lower = name.toLowerCase();
    if (lower === "content-length" || lower === "transfer-encoding" || lower === "connection") {
      return;
    }
    out[name] = value;
  });
  return out;
}

// ─── Identity resolution ────────────────────────────────────────────────

/**
 * Resolve the caller's GitHub PAT: sandbox → project → user →
 * users.github_pat. Returns null when no PAT is stored. Throws on lookup
 * failures (caught by the outer envelope as ok:false).
 */
async function loadGithubPat(sandboxId: string): Promise<string | null> {
  if (!isSupabaseConfigured()) return null;
  const project = await getProjectRowBySandbox(sandboxId);
  if (!project) {
    throw new Error("no project owns this sandbox — cannot resolve a GitHub account");
  }
  const supabase = getServiceSupabase();
  const { data, error } = await supabase
    .from("users")
    .select("github_pat")
    .eq("id", project.user_id)
    .maybeSingle();
  if (error) throw new Error(`github_pat lookup: ${error.message}`);
  const pat = (data as { github_pat?: string | null } | null)?.github_pat;
  return pat && pat.length > 0 ? pat : null;
}

// ─── Action: rest ───────────────────────────────────────────────────────

/** Forward a raw GitHub REST call with the PAT injected. */
async function* handleRest(
  pat: string,
  action: RestAction,
): AsyncGenerator<GithubForwardEvent, void, void> {
  const method = (typeof action.method === "string" && action.method ? action.method : "GET").toUpperCase();
  let apiPath = typeof action.path === "string" ? action.path : "";
  if (!apiPath.startsWith("/")) apiPath = `/${apiPath}`;

  // Authorization from the VM (if any slipped through) is dropped —
  // ghFetch is the ONLY place the PAT is attached.
  const res = await ghFetch(pat, apiPath, method, action.body);

  yield { kind: "head", status: res.status, headers: forwardableHeaders(res) };
  yield { kind: "chunk", body: await readCapped(res) };
}

// ─── Action: sync_workspace ─────────────────────────────────────────────

type TreeNode = {
  name?: string;
  path?: string;
  type?: string;
  size?: number;
  children?: TreeNode[];
};

type WorkspaceFile = { path: string; content: string };

/** Flatten a nested file tree into a list of VM-absolute file paths. */
function flattenTree(node: TreeNode, out: string[]): void {
  for (const child of node.children ?? []) {
    if ((child.type ?? "file") === "directory") {
      flattenTree(child, out);
    } else if (typeof child.path === "string" && child.path) {
      out.push(child.path);
    }
  }
}

/** VM path (/workspace/frontend/a.ts) → repo-relative path (frontend/a.ts). */
function toRelative(vmPath: string): string {
  return vmPath.replace(/^\/workspace\/?/, "");
}

/** Should this VM file be synced? (text only, frontend/backend + manifests,
 *  skip node_modules/.next/.git). */
function shouldSync(vmPath: string): boolean {
  const rel = toRelative(vmPath);
  if (!rel) return false;
  const segments = rel.split("/");
  if (segments.some((seg) => SKIP_DIRS.has(seg))) return false;
  const inScope =
    rel.startsWith("frontend/") ||
    rel.startsWith("backend/") ||
    rel === "package.json" ||
    rel === "requirements.txt";
  if (!inScope) return false;
  const name = segments[segments.length - 1].toLowerCase();
  if (TEXT_FILENAMES.has(name)) return true;
  const dot = name.lastIndexOf(".");
  if (dot <= 0) return false;
  return TEXT_EXTENSIONS.has(name.slice(dot));
}

/**
 * Pull the workspace file list + contents from the VM. Daytona is reachable
 * ONLY via the Python daytona-service from this backend — the file tree and
 * per-file reads are proxied exactly like the studio's Files Tab does
 * (getWorkspaceFileTree + the /read proxy), with a small read pool.
 */
async function collectWorkspaceFiles(sandboxId: string): Promise<WorkspaceFile[]> {
  const tree = (await getWorkspaceFileTree(sandboxId, SYNC_TREE_DEPTH)) as TreeNode | null;
  if (!tree || typeof tree !== "object") {
    throw new Error("empty file tree from the workspace VM");
  }
  const allPaths: string[] = [];
  flattenTree(tree, allPaths);
  const candidates = allPaths.filter(shouldSync);

  const files: WorkspaceFile[] = [];
  let totalBytes = 0;
  let cursor = 0;

  const worker = async (): Promise<void> => {
    for (;;) {
      const i = cursor++;
      if (i >= candidates.length) return;
      if (files.length >= SYNC_MAX_FILES || totalBytes >= SYNC_MAX_TOTAL_BYTES) return;
      const vmPath = candidates[i];
      try {
        const result = (await proxyToDaytona(
          `/${encodeURIComponent(sandboxId)}/read?path=${encodeURIComponent(vmPath)}`,
          { timeoutMs: VM_READ_TIMEOUT_MS },
        )) as { content?: unknown } | null;
        const content = typeof result?.content === "string" ? result.content : "";
        if (!content) continue;
        const size = Buffer.byteLength(content, "utf-8");
        if (totalBytes + size > SYNC_MAX_TOTAL_BYTES) continue; // would bust the cap
        totalBytes += size;
        files.push({ path: toRelative(vmPath), content });
      } catch (err: unknown) {
        // Unreadable single files are skipped — the sync continues.
        logger.debug(
          { sandboxId, vmPath, err: err instanceof Error ? err.message : err },
          "github sync: skipping unreadable workspace file",
        );
      }
    }
  };

  await Promise.all([worker(), worker(), worker(), worker()]);

  // Deterministic order (concurrent reads finish out of order).
  files.sort((a, b) => (a.path < b.path ? -1 : a.path > b.path ? 1 : 0));
  return files;
}

/** PUT one file via the contents API (learns the existing blob sha first). */
async function putFileContents(
  pat: string,
  repo: string,
  file: WorkspaceFile,
  message: string,
): Promise<{ ok: boolean; path: string; error?: string }> {
  const apiPath = `/repos/${repo}/contents/${encodeGitPath(file.path)}`;
  try {
    // PUT /contents requires the file's blob sha when it already exists.
    let sha: string | undefined;
    const getRes = await ghFetch(pat, apiPath);
    if (getRes.ok) {
      const body = (await getRes.json().catch(() => null)) as { sha?: string } | null;
      sha = body?.sha;
    }
    const putRes = await ghFetch(pat, apiPath, "PUT", {
      message,
      content: Buffer.from(file.content, "utf-8").toString("base64"),
      ...(sha ? { sha } : {}),
    });
    if (!putRes.ok) {
      const detail = await putRes.text().catch(() => "");
      return { ok: false, path: file.path, error: `HTTP ${putRes.status}: ${detail.slice(0, 300)}` };
    }
    return { ok: true, path: file.path };
  } catch (err: unknown) {
    return {
      ok: false,
      path: file.path,
      error: err instanceof Error ? err.message : "commit failed",
    };
  }
}

/** Commit the whole workspace to GitHub (contents API, concurrency 4). */
async function* handleSyncWorkspace(
  pat: string,
  sandboxId: string,
  action: SyncAction,
): AsyncGenerator<GithubForwardEvent, void, void> {
  const repoSpec = typeof action.repo === "string" ? action.repo.trim() : "";
  const message =
    typeof action.message === "string" && action.message.trim()
      ? action.message.trim()
      : "ArcForge workspace sync";

  if (!repoSpec) {
    yield* jsonEvents({
      ok: false,
      error: 'sync_workspace requires a repo ("owner/name" or "auto-create:<name>")',
    });
    return;
  }

  // 1. Gather the workspace files (Daytona is reachable only via the
  //    Python daytona-service — see collectWorkspaceFiles).
  const files = await collectWorkspaceFiles(sandboxId);
  if (files.length === 0) {
    yield* jsonEvents({
      ok: false,
      error: "no syncable files found in the workspace (text files under frontend/ and backend/ plus package.json/requirements.txt)",
    });
    return;
  }

  // 2. Resolve the repo — auto-create when requested.
  let repo: string;
  let created = false;
  if (repoSpec.startsWith("auto-create:")) {
    const name = repoSpec.slice("auto-create:".length).trim();
    if (!name) {
      yield* jsonEvents({ ok: false, error: "auto-create:<name> — the repo name is empty" });
      return;
    }
    const createRes = await ghFetch(pat, "/user/repos", "POST", { name, private: true });
    if (createRes.ok) {
      const body = (await createRes.json().catch(() => null)) as { full_name?: string } | null;
      if (!body?.full_name) {
        yield* jsonEvents({ ok: false, error: "repo auto-create returned no full_name" });
        return;
      }
      repo = body.full_name;
      created = true;
    } else {
      const detail = await createRes.text().catch(() => "");
      // 422 "already exists" is tolerable — reuse the caller's own repo.
      const alreadyExists = createRes.status === 422 && /already exists/i.test(detail);
      if (!alreadyExists) {
        yield* jsonEvents({
          ok: false,
          error: `repo auto-create failed (HTTP ${createRes.status}): ${detail.slice(0, 300)}`,
        });
        return;
      }
      const userRes = await ghFetch(pat, "/user");
      const userBody = (await userRes.json().catch(() => null)) as { login?: string } | null;
      if (!userBody?.login) {
        yield* jsonEvents({ ok: false, error: "repo already exists but the login could not be resolved" });
        return;
      }
      repo = `${userBody.login}/${name}`;
    }
  } else {
    repo = repoSpec;
    const checkRes = await ghFetch(pat, `/repos/${repo}`);
    if (checkRes.status === 404) {
      yield* jsonEvents({
        ok: false,
        error: `repo ${repo} not found (or the token cannot see it) — pass auto-create:<name> to create one`,
      });
      return;
    }
  }

  // 3. Learn the current head (empty repo / missing ref is fine — the
  //    contents API creates the first commit on empty repos).
  let head: string | null = null;
  try {
    const refRes = await ghFetch(pat, `/repos/${repo}/git/ref/heads/main`);
    if (refRes.ok) {
      const body = (await refRes.json().catch(() => null)) as
        | { object?: { sha?: string } }
        | null;
      head = body?.object?.sha ?? null;
    }
  } catch {
    /* best-effort — sync proceeds without the head info */
  }

  // 4. Commit every file via the contents API, concurrency 4.
  const results: Array<{ ok: boolean; path: string; error?: string }> = [];
  let cursor = 0;
  const worker = async (): Promise<void> => {
    for (;;) {
      const i = cursor++;
      if (i >= files.length) return;
      results.push(await putFileContents(pat, repo, files[i], message));
    }
  };
  await Promise.all(
    Array.from({ length: Math.min(SYNC_CONCURRENCY, files.length) }, worker),
  );

  const failures = results.filter((r) => !r.ok);
  const committed = results.length - failures.length;
  if (committed === 0) {
    yield* jsonEvents({
      ok: false,
      error: `all ${files.length} file commits failed — first error: ${failures[0]?.error ?? "unknown"}`,
    });
    return;
  }

  logger.info(
    { sandboxId, repo, files: files.length, committed, failed: failures.length, created },
    "github sync: workspace committed",
  );

  yield* jsonEvents({
    ok: true,
    repo,
    url: `https://github.com/${repo}`,
    created,
    files: files.length,
    committed,
    failed: failures.length,
    head,
    ...(failures.length
      ? { failures: failures.slice(0, 10).map((f) => ({ path: f.path, error: f.error })) }
      : {}),
  });
}

// ─── Entry point ────────────────────────────────────────────────────────

/**
 * Handle one `/tunnel/github` req frame: parse the action JSON, resolve
 * sandbox → user → PAT, execute, and yield head/chunk events.
 *
 * Never throws for GitHub-side problems (missing PAT, GitHub 4xx/5xx,
 * fetch failures) — those surface as ok:false JSON envelopes with status
 * 200 per the tool-layer convention. Transport failures of the tunnel
 * itself remain the outer handleReqFrame's error-frame concern.
 */
export async function* handleGithubTunnel(
  conn: GithubTunnelConn,
  frame: GithubTunnelFrame,
): AsyncGenerator<GithubForwardEvent, void, void> {
  try {
    let action: GithubAction;
    try {
      action = JSON.parse(frame.body || "{}") as GithubAction;
    } catch {
      yield* jsonEvents({
        ok: false,
        error: "invalid /tunnel/github frame: body must be JSON {action, ...}",
      });
      return;
    }

    const actionName = typeof (action as { action?: unknown }).action === "string"
      ? (action as { action: string }).action
      : "";

    // Validate the action name BEFORE any DB lookup — a malformed frame
    // deserves its own error, not a misleading "no GitHub account".
    if (actionName !== "rest" && actionName !== "sync_workspace") {
      yield* jsonEvents({
        ok: false,
        error: `unknown github action "${actionName || "(missing)"}" — expected "rest" or "sync_workspace"`,
      });
      return;
    }

    // Resolve the user's PAT (sandbox → project → user → settings).
    const pat = await loadGithubPat(conn.sandboxId);
    if (!pat) {
      // Honest, agent-actionable result — the tool layer turns this into
      // an ask_user so the user adds a token in Settings.
      yield* jsonEvents({
        ok: false,
        error: "no GitHub account connected — ask the user to add a GitHub token in Settings",
      });
      return;
    }

    if (actionName === "rest") {
      yield* handleRest(pat, action as RestAction);
      return;
    }
    yield* handleSyncWorkspace(pat, conn.sandboxId, action as SyncAction);
  } catch (err: unknown) {
    // Fetch/lookup failures → the ok:false JSON convention with status 200.
    const message = err instanceof Error ? err.message : "github tunnel failure";
    logger.warn({ sandboxId: conn.sandboxId, err: message }, "github-proxy: request failed");
    yield* jsonEvents({ ok: false, error: `github tunnel failure: ${message}` });
  }
}
