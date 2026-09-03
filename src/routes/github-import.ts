/**
 * GitHub repository importing — GROUP 3.
 *
 * Frontend → Supabase Edge Function (connector-ops) → THIS backend →
 * GitHub App (Forge-AI App Builder). The browser never talks to
 * api.github.com and never sees a token.
 *
 * Credentials: the GitHub App's client id/secret live ONLY in backend env
 * (GITHUB_APP_CLIENT_ID / GITHUB_APP_CLIENT_SECRET — Render env vars).
 * They are COMPLETELY SEPARATE from the GitHub Sign-In OAuth app
 * (GITHUB_SIGNIN_CLIENT_ID / GITHUB_SIGNIN_CLIENT_SECRET) — never mixed,
 * never shared, never in source.
 *
 * Identity: the caller is the authenticated Supabase user (requireAuth).
 * Their GitHub App user-to-server token is resolved from the encrypted
 * connector vault (connector "github"). Users can only see / import the
 * repositories their own GitHub authorization (installation) actually
 * permits — GitHub enforces this server-side on every call.
 *
 * Routes (mounted under /api in routes/index.ts):
 *   GET  /api/github/repos     → repos the caller's token may access
 *   POST /api/github/import    → {project_id, repo, ref?} — create the VM,
 *                                pull the repo tarball, write text files
 *                                into the workspace.
 *
 * Token lifecycle: GitHub App user-to-server tokens expire (~8h default).
 * On expiry the stored refresh token is exchanged (grant_type=
 * refresh_token) and rotated in the vault. When that fails the connection
 * is honestly reported as expired → the frontend re-authorizes.
 */
import { gunzipSync } from "node:zlib";
import { Router, type IRouter, type Request, type Response, type NextFunction } from "express";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";
import { getProjectRow, type ProjectRow } from "../lib/project-lookup";
import { connectorCredentials, getConnector } from "../services/connector-registry";
import {
  getConnection,
  getTokens,
  markConnectionStatus,
  rotateTokens,
} from "../services/connector-vault";
import {
  ensureProjectSandbox,
  writeWorkspaceFilesBulk,
} from "../services/daytona-workspace";
import { getServiceSupabase } from "../lib/supabase-db";
import { SKIP_DIRS, TEXT_EXTENSIONS, TEXT_FILENAMES } from "../services/github-filters";

const router: IRouter = Router();
router.use(requireAuth);

const GITHUB_API_BASE = "https://api.github.com";
const GH_FETCH_TIMEOUT_MS = 30_000;
const GH_DOWNLOAD_TIMEOUT_MS = 120_000;

/** Import caps (honest, enforced server-side). */
const IMPORT_MAX_FILES = 400;
const IMPORT_MAX_TOTAL_BYTES = 8 * 1024 * 1024; // 8 MB of text across files
const IMPORT_MAX_FILE_BYTES = 256 * 1024; // one file ≤ 256 KB
const IMPORT_MAX_TARBALL_BYTES = 64 * 1024 * 1024; // gz download ≤ 64 MB
/** Repo-listing caps. */
const LIST_MAX_INSTALLATIONS = 10;
const LIST_MAX_REPOS = 150;

// ─── GitHub App token resolution (vault + refresh-on-expiry) ─────────────

interface GithubTokenState {
  ok: true;
  accessToken: string;
}
interface GithubTokenFailure {
  ok: false;
  reason: "not_connected" | "expired" | "error";
  message: string;
}

function ghHeaders(token: string): Record<string, string> {
  return {
    Authorization: `Bearer ${token}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28",
    "User-Agent": "arcforge-backend",
  };
}

async function ghFetch(
  token: string,
  apiPath: string,
  init: { method?: string; body?: unknown; timeoutMs?: number } = {},
): Promise<globalThis.Response> {
  const method = init.method ?? "GET";
  const hasBody = init.body !== undefined && init.body !== null && method !== "GET" && method !== "HEAD";
  return fetch(`${GITHUB_API_BASE}${apiPath}`, {
    method,
    headers: {
      ...ghHeaders(token),
      ...(hasBody ? { "Content-Type": "application/json" } : {}),
    },
    body: hasBody ? JSON.stringify(init.body) : undefined,
    signal: AbortSignal.timeout(init.timeoutMs ?? GH_FETCH_TIMEOUT_MS),
    redirect: init.method === "GET" && apiPath.includes("/tarball/") ? "follow" : undefined,
  });
}

/** GitHub App refresh-token exchange (user-to-server). Never logs tokens. */
async function refreshGithubToken(
  userId: string,
  refreshToken: string,
): Promise<{ accessToken: string; refreshToken: string | null; expiresAt: string } | null> {
  const connector = getConnector("github");
  if (!connector) return null;
  const creds = connectorCredentials(connector);
  if (!creds) return null;
  const res = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Accept: "application/json",
      "User-Agent": "arcforge-backend",
    },
    body: JSON.stringify({
      client_id: creds.clientId,
      client_secret: creds.clientSecret,
      grant_type: "refresh_token",
      refresh_token: refreshToken,
    }),
    signal: AbortSignal.timeout(GH_FETCH_TIMEOUT_MS),
  });
  if (!res.ok) return null;
  const json = (await res.json().catch(() => null)) as {
    access_token?: string;
    refresh_token?: string;
    expires_in?: number;
    error?: string;
    error_description?: string;
  } | null;
  if (!json?.access_token) {
    logger.warn({ err: json?.error }, "github-import: token refresh rejected (token values never logged)");
    return null;
  }
  const expiresAt = new Date(Date.now() + (json.expires_in ?? 28800) * 1000).toISOString();
  return {
    accessToken: json.access_token,
    refreshToken: json.refresh_token ?? null,
    expiresAt,
  };
}

/** Resolve the caller's GitHub App user-to-server token, refreshing on
 *  expiry. Returns a discriminated union the routes translate into either
 *  execution or an honest `needs_connector` payload. */
async function resolveGithubToken(userId: string): Promise<GithubTokenState | GithubTokenFailure> {
  const row = await getConnection(userId, "github");
  if (!row || row.status !== "connected") {
    return {
      ok: false,
      reason: "not_connected",
      message: "GitHub is not connected — authorize the Forge-AI App Builder app to list and import repositories.",
    };
  }
  const tokens = await getTokens(userId, "github");
  if (!tokens) {
    return {
      ok: false,
      reason: "not_connected",
      message: "GitHub is not connected — authorize the Forge-AI App Builder app to list and import repositories.",
    };
  }
  const expiresMs = tokens.expiresAt ? new Date(tokens.expiresAt).getTime() : 0;
  if (expiresMs && expiresMs > Date.now() + 60_000) {
    return { ok: true, accessToken: tokens.accessToken };
  }
  // Expired (or expiring within a minute) → attempt the refresh rotation.
  if (tokens.refreshToken) {
    const refreshed = await refreshGithubToken(userId, tokens.refreshToken).catch(() => null);
    if (refreshed) {
      await rotateTokens(userId, "github", {
        accessToken: refreshed.accessToken,
        refreshToken: refreshed.refreshToken,
        expiresAt: refreshed.expiresAt,
      });
      logger.info({ userId }, "github-import: token refreshed on use (value never logged)");
      return { ok: true, accessToken: refreshed.accessToken };
    }
  }
  await markConnectionStatus(userId, "github", "error").catch(() => undefined);
  return {
    ok: false,
    reason: "expired",
    message: "Your GitHub connection expired — reconnect GitHub to continue.",
  };
}

// ─── GET /api/github/repos ────────────────────────────────────────────────

interface SanitizedRepo {
  full_name: string;
  name: string;
  owner: string;
  owner_avatar: string | null;
  description: string | null;
  private: boolean;
  default_branch: string;
  pushed_at: string | null;
  html_url: string;
}

function sanitizeRepo(raw: Record<string, unknown>): SanitizedRepo | null {
  const fullName = typeof raw.full_name === "string" ? raw.full_name : "";
  if (!fullName || !fullName.includes("/")) return null;
  const owner =
    raw.owner && typeof raw.owner === "object" ? (raw.owner as { login?: unknown; avatar_url?: unknown }) : {};
  return {
    full_name: fullName,
    name: typeof raw.name === "string" ? raw.name : fullName.split("/")[1],
    owner: typeof owner.login === "string" ? owner.login : fullName.split("/")[0],
    owner_avatar: typeof owner.avatar_url === "string" ? owner.avatar_url : null,
    description: typeof raw.description === "string" ? raw.description : null,
    private: raw.private === true,
    default_branch: typeof raw.default_branch === "string" ? raw.default_branch : "main",
    pushed_at: typeof raw.pushed_at === "string" ? raw.pushed_at : null,
    html_url: typeof raw.html_url === "string" ? raw.html_url : `https://github.com/${fullName}`,
  };
}

router.get("/github/repos", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const token = await resolveGithubToken(req.userId!);
    if (!token.ok) {
      res.json({
        repos: [] as SanitizedRepo[],
        needs_connector: "github",
        reason: token.reason,
        message: token.message,
      });
      return;
    }

    const byName = new Map<string, SanitizedRepo>();

    // 1. Repositories granted to the GitHub App installations (the
    //    documented enumeration for user-to-server tokens).
    try {
      const instRes = await ghFetch(token.accessToken, "/user/installations?per_page=100");
      if (instRes.ok) {
        const instJson = (await instRes.json()) as {
          installations?: Array<{ id?: number; repository_selection?: string }>;
        };
        const installations = (instJson.installations ?? []).slice(0, LIST_MAX_INSTALLATIONS);
        for (const inst of installations) {
          if (typeof inst.id !== "number") continue;
          const reposRes = await ghFetch(
            token.accessToken,
            `/user/installations/${inst.id}/repositories?per_page=100`,
          );
          if (!reposRes.ok) continue;
          const reposJson = (await reposRes.json()) as { repositories?: Array<Record<string, unknown>> };
          for (const raw of reposJson.repositories ?? []) {
            const repo = sanitizeRepo(raw);
            if (repo && byName.size < LIST_MAX_REPOS) byName.set(repo.full_name, repo);
          }
        }
      }
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : "unknown" },
        "github-import: installation listing failed",
      );
    }

    // 2. Repositories visible to the token via the authenticated-user
    //    listing (public repos the user owns/collaborates on are always
    //    readable) — merged, deduped.
    if (byName.size < LIST_MAX_REPOS) {
      try {
        const userReposRes = await ghFetch(
          token.accessToken,
          "/user/repos?per_page=100&sort=pushed&type=owner",
        );
        if (userReposRes.ok) {
          const reposJson = (await userReposRes.json()) as Array<Record<string, unknown>>;
          for (const raw of reposJson) {
            const repo = sanitizeRepo(raw);
            if (repo && byName.size < LIST_MAX_REPOS) byName.set(repo.full_name, repo);
          }
        }
      } catch (err) {
        logger.warn(
          { err: err instanceof Error ? err.message : "unknown" },
          "github-import: user repo listing failed",
        );
      }
    }

    const repos = [...byName.values()].sort((a, b) =>
      (b.pushed_at ?? "").localeCompare(a.pushed_at ?? ""),
    );
    res.json({ repos });
  } catch (error) {
    next(error);
  }
});

// ─── Tarball → workspace import ──────────────────────────────────────────

/** Minimal ustar/GNU tar parser for the (already gunzipped) tarball:
 *  regular files only, sequential block walk, caps enforced while parsing. */
function parseTarEntries(
  buf: Buffer,
  onEntry: (name: string, content: Buffer) => boolean | void,
): void {
  let offset = 0;
  // Scratch header buffer reused across entries.
  const header = Buffer.alloc(512);
  while (offset + 512 <= buf.length) {
    buf.copy(header, 0, offset, offset + 512);
    // Two consecutive zero blocks terminate the archive.
    if (header.every((byte) => byte === 0)) break;

    const readString = (start: number, length: number): string => {
      const slice = header.subarray(start, start + length);
      const nul = slice.indexOf(0);
      return slice.subarray(0, nul === -1 ? length : nul).toString("utf-8");
    };
    const readOctal = (start: number, length: number): number => {
      const raw = readString(start, length).trim();
      return raw ? parseInt(raw.replace(/^0/, "0") || "0", 8) || 0 : 0;
    };

    let name = readString(0, 100);
    const size = readOctal(124, 12);
    const typeflag = String.fromCharCode(header[156] || 0x30);
    const prefix = readString(345, 155);
    if (prefix) name = `${prefix}/${name}`;

    offset += 512;
    if (offset + size > buf.length) break; // truncated archive — stop honestly

    const dataStart = offset;
    offset += Math.ceil(size / 512) * 512;

    // Directories ('5'), symlinks ('2'/'1'), pax headers ('x'/'g') and GNU
    // long-name blocks ('L') carry no file content we need — skip.
    if (typeflag === "0" || typeflag === "\0" || typeflag === "" || typeflag === "7") {
      const content = buf.subarray(dataStart, dataStart + size);
      const stop = onEntry(name, content);
      if (stop === true) return;
    }
  }
}

/** Should a tar entry be imported? Strips the tarball's leading
 *  `<owner>-<repo>-<ref>/` directory, filters junk + binary files. */
function importableRelPath(tarName: string): string | null {
  const normalized = tarName.replace(/\\/g, "/").replace(/^\.?\//, "");
  const segments = normalized.split("/").filter((s) => s !== "" && s !== ".");
  if (segments.length < 2) return null; // top-level entries belong to the wrapper dir
  if (segments.some((s) => SKIP_DIRS.has(s) || s === ".venv" || s === "dist-cache")) return null;
  const rel = segments.slice(1).join("/"); // strip owner-repo-sha/
  if (!rel) return null;
  const name = segments[segments.length - 1].toLowerCase();
  if (name.startsWith(".ds_") || name.endsWith(".lockb") || name === ".gitmodules") return null;
  if (name === ".gitattributes" || name === ".gitignore") return rel; // keep: useful context
  if (TEXT_FILENAMES.has(name)) return rel;
  const dot = name.lastIndexOf(".");
  if (dot <= 0) return null; // extension-less files (except known ones) are skipped
  return TEXT_EXTENSIONS.has(name.slice(dot)) ? rel : null;
}

interface ImportResult {
  ok: boolean;
  error?: string;
  needs_connector?: "github";
  repo?: string;
  ref?: string;
  default_branch?: string;
  sandbox_id?: string;
  files_imported?: number;
  files_skipped?: number;
  bytes?: number;
  truncated?: boolean;
}

router.post("/github/import", async (req: Request, res: Response, next: NextFunction) => {
  try {
    const body = (req.body ?? {}) as { project_id?: string; repo?: string; ref?: string };
    const projectId = typeof body.project_id === "string" ? body.project_id : "";
    const repoSpec = typeof body.repo === "string" ? body.repo.trim() : "";
    if (!projectId || !/^[0-9a-f-]{36}$/i.test(projectId)) {
      res.status(400).json({ ok: false, error: "project_id (uuid) is required" });
      return;
    }
    if (!repoSpec || !/^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/.test(repoSpec)) {
      res.status(400).json({ ok: false, error: 'repo must be "owner/name"' });
      return;
    }

    // ── Ownership: the project must belong to the caller (isolation). ──
    const row: ProjectRow | null = await getProjectRow(projectId);
    if (!row) {
      res.status(404).json({ ok: false, error: "Project not found" });
      return;
    }
    if (row.user_id !== req.userId) {
      res.status(403).json({ ok: false, error: "Forbidden — project belongs to another user" });
      return;
    }

    // ── Token: user's own GitHub App authorization only. ──
    const token = await resolveGithubToken(req.userId!);
    if (!token.ok) {
      const result: ImportResult = { ok: false, error: token.message, needs_connector: "github" };
      res.json(result);
      return;
    }

    // ── Repo access check: GitHub itself enforces the installation's
    //    repository selection + the app's permissions. A 404/403 means the
    //    caller's authorization genuinely cannot see this repo. ──
    const repoRes = await ghFetch(token.accessToken, `/repos/${repoSpec}`);
    if (repoRes.status === 404 || repoRes.status === 403) {
      res.json({
        ok: false,
        error:
          `${repoSpec} is not accessible with your GitHub authorization. Install the ` +
          "Forge-AI App Builder GitHub App on the owning account and grant it access to this repository.",
      });
      return;
    }
    if (!repoRes.ok) {
      res.json({ ok: false, error: `GitHub returned HTTP ${repoRes.status} for ${repoSpec}` });
      return;
    }
    const repoInfo = (await repoRes.json()) as {
      full_name?: string;
      default_branch?: string;
      private?: boolean;
    };
    const defaultBranch = typeof repoInfo.default_branch === "string" ? repoInfo.default_branch : "main";
    const ref = typeof body.ref === "string" && body.ref.trim() ? body.ref.trim() : defaultBranch;

    // ── Download the tarball (gzipped tar) with a hard size cap. ──
    const tarballRes = await ghFetch(token.accessToken, `/repos/${repoSpec}/tarball/${encodeURIComponent(ref)}`, {
      timeoutMs: GH_DOWNLOAD_TIMEOUT_MS,
    });
    if (!tarballRes.ok || !tarballRes.body) {
      res.json({ ok: false, error: `Could not download ${repoSpec} (${ref}): HTTP ${tarballRes.status}` });
      return;
    }
    const chunks: Buffer[] = [];
    let downloaded = 0;
    const reader = tarballRes.body.getReader();
    for (;;) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) {
        downloaded += value.byteLength;
        if (downloaded > IMPORT_MAX_TARBALL_BYTES) {
          try {
            await reader.cancel();
          } catch {
            /* already closed */
          }
          res.json({
            ok: false,
            error: `${repoSpec} is too large to import (archive exceeds ${IMPORT_MAX_TARBALL_BYTES / (1024 * 1024)} MB).`,
          });
          return;
        }
        chunks.push(Buffer.from(value));
      }
    }
    const gz = Buffer.concat(chunks);
    chunks.length = 0;

    // ── Gunzip + walk the tar, importing text files under the caps. ──
    let files: Array<{ path: string; content: string }> = [];
    let skipped = 0;
    let totalBytes = 0;
    let truncated = false;
    let stopped = false;
    try {
      const tar = gunzipSync(gz);
      parseTarEntries(tar, (name, content): boolean | void => {
        const rel = importableRelPath(name);
        if (rel === null) {
          skipped += 1;
          return;
        }
        if (content.byteLength > IMPORT_MAX_FILE_BYTES) {
          skipped += 1;
          return;
        }
        if (
          files.length >= IMPORT_MAX_FILES ||
          totalBytes + content.byteLength > IMPORT_MAX_TOTAL_BYTES
        ) {
          truncated = true;
          stopped = true;
          return true; // stop walking
        }
        files.push({ path: `/workspace/${rel}`, content: content.toString("utf-8") });
        totalBytes += content.byteLength;
      });
    } catch (err) {
      logger.warn(
        { err: err instanceof Error ? err.message : "unknown", repo: repoSpec },
        "github-import: tarball extraction failed",
      );
      res.json({ ok: false, error: `Could not extract the ${repoSpec} archive.` });
      return;
    }
    void stopped;

    if (files.length === 0) {
      res.json({
        ok: false,
        error: `No importable text files were found in ${repoSpec} (binary-only or empty repository).`,
      });
      return;
    }

    // ── Provision/reuse the project VM (ownership already verified). ──
    const ensured = await ensureProjectSandbox(row, {
      saveSandboxId: async (sandboxId) => {
        const { error } = await getServiceSupabase()
          .from("projects")
          .update({ sandbox_id: sandboxId, updated_at: new Date().toISOString() })
          .eq("id", row.id);
        if (error) throw new Error(`sandbox_id save: ${error.message}`);
      },
    });

    // ── Write the imported files into the VM workspace. ──
    await writeWorkspaceFilesBulk(ensured.sandbox_id, files);

    logger.info(
      {
        userId: req.userId,
        projectId: row.id,
        repo: repoSpec,
        ref,
        files: files.length,
        skipped,
        truncated,
      },
      "github-import: repository imported",
    );

    const result: ImportResult = {
      ok: true,
      repo: repoSpec,
      ref,
      default_branch: defaultBranch,
      sandbox_id: ensured.sandbox_id,
      files_imported: files.length,
      files_skipped: skipped,
      bytes: totalBytes,
      truncated,
    };
    res.json(result);
    files = [];
  } catch (error) {
    next(error);
  }
});

export default router;
