/**
 * MODULE 2 (Part A) — Host-side port-forwarding reverse proxy.
 *
 * The Daytona MicroVM exposes its internal ports only inside the VM
 * network namespace. Render (and any single-port host) cannot listen
 * on every preview port, so we implement a HTTP reverse-proxy LAYER
 * inside the ArcForge backend itself:
 *
 *   Browser → /api/preview/<sandboxId>/<port>/<path>
 *                     │
 *                     ▼
 *     ArcForge backend → runWorkspaceTerminal(sandboxId,
 *                       `curl -sS -i --max-time 8 http://localhost:<port><path>`)
 *                     │
 *                     ▼
 *     Parse status line + headers + blank line + body, stream back
 *
 * The result is a stable, externally-routable URL per (sandbox, port)
 * pair that the frontend can embed in an <iframe> or hit directly.
 *
 * Public preview URLs:
 *   ${PREVIEW_BASE_URL}/api/preview/<sandboxId>/<port>/
 *
 * When PREVIEW_BASE_URL is unset, the URL is RELATIVE so the frontend
 * can resolve it against its own backend origin (NEXT_PUBLIC_BACKEND_URL).
 *
 * SECURITY: the routes in routes/workspace.ts gate every preview
 * request behind requireAuth + a projects.user_id ownership check.
 * This service only operates on (sandboxId, port, path) — it never
 * trusts caller-supplied data beyond sanitizing the port range and
 * path-shape.
 */
import { logger } from "../lib/logger";
import {
  isSandboxAlive,
  runWorkspaceTerminal,
  type TerminalResult,
} from "./daytona-workspace";

export type PreviewResolution = {
  sandbox_id: string;
  port: number;
  /** Public/loopback URL the frontend can embed (always ends with `/`). */
  preview_url: string;
  /** http://localhost:{port} inside the VM — for diagnostics only. */
  internal_url: string;
  /** Did the port respond to a 2-second TCP/HTTP probe? */
  alive: boolean;
};

export type ProxiedResponse = {
  /** Parsed HTTP status code (e.g. 200). 0 when curl failed entirely. */
  status: number;
  /** Lower-cased header → value map (no Set-Cookie / array semantics). */
  headers: Record<string, string>;
  /** Raw body bytes (curl -i in-VM, decoded as UTF-8 → Buffer). */
  body: Buffer;
  /** True when the underlying curl exec returned no usable response. */
  failed: boolean;
};

// `--max-time` is in seconds. Keep it just under the hard 10s cap so
// the terminal RPC has time to return before the caller's 502 timeout.
const PROXY_CURL_TIMEOUT_S = 8;
const PROXY_HARD_TIMEOUT_MS = 10_000;
const PROBE_CURL_TIMEOUT_S = 2;
const PROBE_HARD_TIMEOUT_MS = 4_000;

/**
 * Build the public preview URL for a (sandbox, port) pair.
 * The URL always ends with a trailing slash so appended paths stay
 * relative. When PREVIEW_BASE_URL is unset, the URL is relative so
 * the frontend can resolve against its own backend origin.
 *
 *   buildPreviewUrl("sbx-abc123", 5173)
 *     → "https://arcforge-backend.onrender.com/api/preview/sbx-abc123/5173/"
 *   buildPreviewUrl("sbx-abc123", 5173)   // PREVIEW_BASE_URL unset
 *     → "/api/preview/sbx-abc123/5173/"
 */
export function buildPreviewUrl(sandboxId: string, port: number): string {
  const base = (process.env.PREVIEW_BASE_URL ?? "").replace(/\/+$/, "");
  return `${base}/api/preview/${encodeURIComponent(sandboxId)}/${port}/`;
}

/**
 * Probe whether a port is bound inside the VM (best-effort, 2s timeout).
 * Returns false on: invalid port, dead/unknown sandbox, terminal RPC
 * failure, or curl exit code 7 (connection refused).
 */
export async function probePort(
  sandboxId: string,
  port: number,
): Promise<boolean> {
  if (!Number.isInteger(port) || port < 1 || port > 65535) return false;
  if (!(await isSandboxAlive(sandboxId))) return false;
  // `%{http_code}` → "000" when curl couldn't connect, "200" etc. on
  // a real HTTP response. We treat any 3-digit code as "alive" so that
  // servers returning 404/500 (still bound, just unhappy) report alive.
  const probeCmd = `curl -sS -o /dev/null -w '%{http_code}' --max-time ${PROBE_CURL_TIMEOUT_S} http://localhost:${port}/`;
  let result: TerminalResult | null = null;
  try {
    result = await runWorkspaceTerminal(
      sandboxId,
      probeCmd,
      "/workspace",
      PROBE_HARD_TIMEOUT_MS,
    );
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, port, err: err instanceof Error ? err.message : err },
      "preview-proxy: probePort terminal call failed",
    );
    return false;
  }
  if (!result) return false;
  const code = (result.stdout ?? "").trim();
  return /^\d{3}$/.test(code);
}

/**
 * Normalize an in-VM HTTP path: empty/`/` both target root, anything
 * else must start with `/`. Trims trailing whitespace.
 */
function normalizeVmPath(path: string): string {
  let p = (path ?? "").trim();
  if (!p || p === "/") return "/";
  if (!p.startsWith("/")) p = "/" + p;
  return p;
}

/**
 * Escape a URL for shell passing. We JSON.stringify then strip the
 * surrounding quotes — the result is a single-quoted-safe bash word
 * because curl accepts double-quoted URLs with embedded single quotes.
 *
 * NOTE: we never interpolate user-controlled data into the command
 * outside this single quoted/JSON-encapsulated segment, so shell
 * injection is contained.
 */
function shellQuoteUrl(url: string): string {
  // Use single quotes around the whole URL — bash doesn't interpret
  // anything inside single quotes. If the URL contains a single quote
  // (legal but rare in query strings), close-quote, escape, re-open.
  return "'" + url.replace(/'/g, "'\\''") + "'";
}

/**
 * Fetch the full HTTP response from an in-VM port and stream it back.
 *
 * Implementation: runs `curl -sS -i --max-time 8 'http://localhost:<port><path>?<query>'`
 * via `runWorkspaceTerminal`. The `-i` flag prepends the HTTP status
 * line + response headers to stdout, followed by `\r\n\r\n`, then the
 * raw body. We split at that boundary and parse the status line + each
 * header line.
 *
 * Binary fidelity: the daytona terminal endpoint returns `stdout` as a
 * UTF-8 `str` (see app/services/workspace_coordinator.py), so non-UTF-8
 * body bytes become U+FFFD. Dev previews are predominantly HTML/JS/CSS
 * so this is acceptable; binary-asset fidelity is a known limitation.
 *
 * On curl failure (timeout, connection refused, dead VM), returns
 * `{status: 0, headers: {}, body: <stderr-bytes>, failed: true}` so
 * the route handler can map to a 502.
 */
export async function proxyThroughVm(
  sandboxId: string,
  port: number,
  path: string,
  queryString?: string,
): Promise<ProxiedResponse> {
  if (!Number.isInteger(port) || port < 1 || port > 65535) {
    return {
      status: 0,
      headers: {},
      body: Buffer.alloc(0),
      failed: true,
    };
  }

  const vmPath = normalizeVmPath(path);
  const qs = queryString && queryString.length > 0 ? `?${queryString}` : "";
  const url = `http://localhost:${port}${vmPath}${qs}`;
  const command = `curl -sS -i --max-time ${PROXY_CURL_TIMEOUT_S} ${shellQuoteUrl(url)}`;

  let result: TerminalResult | null = null;
  try {
    result = await runWorkspaceTerminal(
      sandboxId,
      command,
      "/workspace",
      PROXY_HARD_TIMEOUT_MS,
    );
  } catch (err: unknown) {
    logger.warn(
      { sandboxId, port, path: vmPath, err: err instanceof Error ? err.message : err },
      "preview-proxy: curl through VM threw",
    );
    return {
      status: 0,
      headers: {},
      body: Buffer.from(""),
      failed: true,
    };
  }

  if (!result) {
    return { status: 0, headers: {}, body: Buffer.alloc(0), failed: true };
  }

  // Curl error path: empty stdout, non-empty stderr (curl exit 7/28).
  const stdout = result.stdout ?? "";
  const stderr = result.stderr ?? "";
  if (stdout.length === 0) {
    return {
      status: 0,
      headers: {},
      body: Buffer.from(stderr, "utf-8"),
      failed: true,
    };
  }

  // Split headers from body at the first `\r\n\r\n` boundary (HTTP
  // standard). Fall back to `\n\n` for servers that omit the CR.
  let headerBlock: string;
  let bodyText: string;
  const crlfBoundary = stdout.indexOf("\r\n\r\n");
  if (crlfBoundary >= 0) {
    headerBlock = stdout.slice(0, crlfBoundary);
    bodyText = stdout.slice(crlfBoundary + 4);
  } else {
    const lfBoundary = stdout.indexOf("\n\n");
    if (lfBoundary >= 0) {
      headerBlock = stdout.slice(0, lfBoundary);
      bodyText = stdout.slice(lfBoundary + 2);
    } else {
      // No boundary at all — treat the entire blob as a body and
      // synthesize a 200 (curl -i would normally inject a status line,
      // so this branch is rare in practice).
      headerBlock = "";
      bodyText = stdout;
    }
  }

  let status = 0;
  const headers: Record<string, string> = {};
  for (const line of headerBlock.split(/\r?\n/)) {
    if (line.length === 0) continue;
    if (line.startsWith("HTTP/")) {
      // "HTTP/1.1 200 OK"  →  status = 200
      const parts = line.split(/\s+/);
      const code = parseInt(parts[1] ?? "", 10);
      if (!Number.isNaN(code) && code > 0) status = code;
      continue;
    }
    const colon = line.indexOf(":");
    if (colon > 0) {
      const key = line.slice(0, colon).trim().toLowerCase();
      const val = line.slice(colon + 1).trim();
      if (key) headers[key] = val;
    }
  }

  if (status === 0) {
    // We got bytes but no HTTP status line — curl most likely errored
    // mid-response. Surface the stderr so the caller can decide.
    return {
      status: 0,
      headers,
      body: Buffer.from(bodyText || stderr, "utf-8"),
      failed: true,
    };
  }

  return {
    status,
    headers,
    body: Buffer.from(bodyText, "utf-8"),
    failed: false,
  };
}

/**
 * Build a PreviewResolution for a (sandbox, port) pair. Probes the
 * port liveness in parallel with the URL construction. Never throws —
 * on probe failure, `alive` is false but the URL is still returned so
 * the frontend can show a "preview not yet ready" frame.
 */
export async function resolvePreview(
  sandboxId: string,
  port: number,
): Promise<PreviewResolution> {
  const alive = await probePort(sandboxId, port);
  return {
    sandbox_id: sandboxId,
    port,
    preview_url: buildPreviewUrl(sandboxId, port),
    internal_url: `http://localhost:${port}`,
    alive,
  };
}
