/**
 * NVIDIA forwarder — the core of the Inbound Reverse Proxy Tunnel.
 *
 * WHY THIS EXISTS
 * ───────────────
 * Daytona eu-region sandboxes are behind an egress firewall that drops
 * outbound TLS to *.nvidia.com (and to *.onrender.com). The In-VM AI
 * orchestrator therefore cannot reach NVIDIA directly NOR can it call the
 * backend's existing public `/api/llm/chat` HTTPS proxy.
 *
 * The fix: a WebSocket tunnel. The VM opens a long-lived WS to
 * `/api/tunnel` on the backend (WS is allowed by the filter). For each
 * HTTP request the VM wants to make to NVIDIA, it sends a `req` frame
 * over the WS with the method/path/headers/body. The backend forwards
 * the request to NVIDIA — INJECTING the server-side API key (the key
 * never enters the VM) — and streams the response back down the WS as
 * `res` / `chunk` / `done` frames (see `src/routes/tunnel.ts`).
 *
 * This module is the actual HTTP-to-NVIDIA forwarder. It is an
 * `AsyncGenerator` so the tunnel router can stream chunks back to the
 * VM as they arrive from NVIDIA (essential for `text/event-stream`
 * completions). For non-streaming calls it yields a single chunk with
 * the full body — same code path, trivially reconstructed on the VM
 * side by concatenating `body` strings.
 *
 * SECURITY
 * ──────────────────────────────────────────────────────────────
 * • The NVIDIA key is read here, server-side only. It is NEVER sent
 *   to the VM. Any stray `Authorization` header on the inbound `req`
 *   frame is dropped (the VM must not hold the key).
 * • `host` / `content-length` / hop-by-hop headers are stripped and
 *   recomputed — the inbound headers came from inside the VM and may
 *   carry the VM's `host: localhost:7777` value, which must not be
 *   forwarded to NVIDIA.
 */
import { logger } from "../lib/logger";
import { getSingleModeLlmConfig } from "./agent-platform";

// ─── Types ──────────────────────────────────────────────────────────────

export type NvidiaForwardEvent =
  | { kind: "head"; status: number; headers: Record<string, string> }
  | { kind: "chunk"; body: string };

export interface ForwardParams {
  method: string;
  /** Path as sent by the VM, e.g. "/v1/chat/completions". */
  path: string;
  /** Headers from the inbound `req` frame (Authorization will be stripped). */
  headers: Record<string, string>;
  /** Raw request body as a string (JSON for chat completions). */
  bodyString: string;
}

// ─── Helpers ────────────────────────────────────────────────────────────

/**
 * Resolve the upstream LLM base URL + API key (GROQ since 2026-08-27;
 * NVIDIA kept only as a legacy fallback).
 *
 * Priority:
 *  1. `GROQ_API_KEY` (+ optional `GROQ_BASE_URL`, default
 *     `https://api.groq.com/openai/v1`) — the primary provider.
 *  2. `NVIDIA_NIM_API_KEY` + `NVIDIA_NIM_BASE_URL` env vars (legacy
 *     tunnel-only config — kept so a stale Render env var can still
 *     intentionally pin the old provider).
 *  3. `getSingleModeLlmConfig()` from `agent-platform.ts` — the shared
 *     source of truth for url+key+model used by the existing
 *     `/api/llm/chat` HTTP proxy (now Groq-first too).
 *
 * All provider URLs are FULL chat endpoints
 * (`https://api.groq.com/openai/v1/chat/completions`), so we strip the
 * trailing `/chat/completions` to recover a `/v1`-style base. We also
 * strip a trailing `/v1` so the VM-supplied `path` (which starts with
 * `/v1/...`) can be appended verbatim without double-`/v1` — Groq's
 * `/openai/v1` base becomes `/openai`, and `/v1/chat/completions` from
 * the VM reconstructs exactly `https://api.groq.com/openai/v1/chat/completions`.
 */
function resolveLlmEndpoint(): { baseUrl: string; key: string } {
  const cfg = getSingleModeLlmConfig();

  const groqKey = process.env.GROQ_API_KEY;
  const nimKey = process.env.NVIDIA_NIM_API_KEY;
  const nimBase = process.env.NVIDIA_NIM_BASE_URL;

  let base: string;
  let key: string;
  if (groqKey) {
    key = groqKey;
    base = (process.env.GROQ_BASE_URL || "https://api.groq.com/openai/v1").trim();
  } else if (nimKey) {
    key = nimKey;
    base = (nimBase || cfg.url || "https://integrate.api.nvidia.com/v1").trim();
  } else {
    key = cfg.key;
    base = (cfg.url || "https://api.groq.com/openai/v1").trim();
  }

  // 1) Trim trailing slashes.
  base = base.replace(/\/+$/, "");
  // 2) Strip a "/chat/completions" tail (single-mode URLs have it).
  base = base.replace(/\/chat\/completions$/i, "");
  // 3) Strip a trailing "/v1" so we can append the VM's "/v1/..." path
  //    verbatim without producing a double "/v1/v1/...". (If the base
  //    has a deeper path like /openai/v1 we only strip the final "/v1".)
  base = base.replace(/\/v1$/i, "");

  return { baseUrl: base, key };
}

/**
 * Build the final upstream URL. `path` arrives as `/v1/chat/completions`
 * (leading slash guaranteed by the protocol, but we normalize defensively).
 */
function buildUpstreamUrl(baseUrl: string, path: string): string {
  const safePath = path.startsWith("/") ? path : `/${path}`;
  return `${baseUrl}${safePath}`;
}

/** Lowercase header set we MUST strip from the inbound frame. */
const STRIPPED_INBOUND_HEADERS = new Set([
  "authorization",
  "host",
  "content-length",
  "connection",
  "transfer-encoding",
  "keep-alive",
  "proxy-connection",
  "upgrade",
  "te",
  "trailer",
  "x-agent-token", // never forward the tunnel auth to NVIDIA
]);

/** Build the outbound header map: strip hop-by-hop, inject NVIDIA key. */
function buildOutboundHeaders(
  inbound: Record<string, string>,
  key: string,
  bodyString: string,
  method: string,
): Record<string, string> {
  const out: Record<string, string> = {};

  for (const [name, value] of Object.entries(inbound)) {
    if (STRIPPED_INBOUND_HEADERS.has(name.toLowerCase())) continue;
    // Skip empty values (defensive — VM-side might send empty strings).
    if (value === undefined || value === null) continue;
    out[name] = value;
  }

  // Inject the server-side NVIDIA key. This is the whole point of the
  // tunnel — the VM never holds the key, so a stray Authorization from
  // the VM (if any slipped through) is overwritten here.
  out["Authorization"] = `Bearer ${key}`;

  // Recompute Content-Length from the actual body bytes we're sending.
  // (Inbound Content-Length was stripped above; the VM's value may have
  // been wrong anyway, and fetch will reject mismatched lengths.)
  if (bodyString && method.toUpperCase() !== "GET" && method.toUpperCase() !== "HEAD") {
    out["Content-Length"] = String(Buffer.byteLength(bodyString, "utf8"));
  }

  return out;
}

// ─── Forwarder ──────────────────────────────────────────────────────────

/**
 * Forward an HTTP request to NVIDIA and yield response events.
 *
 * Yields:
 *   1. `{ kind:"head", status, headers }` — once, with the upstream
 *      status line + response headers. (Only yielded on 2xx; non-2xx
 *      throws before yielding head, so the caller can map it to an
 *      `error` frame without first sending a `res` frame.)
 *   2. `{ kind:"chunk", body }` — one or more body chunks. For
 *      `text/event-stream` responses (NVIDIA streaming), many chunks
 *      are yielded as they arrive. For non-streaming responses, a
 *      single chunk with the full body.
 *
 * Throws on fetch failure (network/timeout) or non-2xx upstream
 * response — the caller (tunnel router) catches and sends an `error`
 * frame to the VM with the upstream status + truncated detail in the
 * `message` field.
 */
export async function* forwardToNvidia(
  params: ForwardParams,
): AsyncGenerator<NvidiaForwardEvent, void, void> {
  const { method, path, headers, bodyString } = params;
  const { baseUrl, key } = resolveLlmEndpoint();

  if (!key) {
    throw new Error(
      "LLM API key not configured on the tunnel server " +
        "(set GROQ_API_KEY, or legacy NVIDIA_NIM_API_KEY/SINGLE_MODE_API_KEY).",
    );
  }

  const url = buildUpstreamUrl(baseUrl, path);
  const outboundHeaders = buildOutboundHeaders(headers, key, bodyString, method);
  const hasBody =
    bodyString && method.toUpperCase() !== "GET" && method.toUpperCase() !== "HEAD";

  // Retry connect-level failures ("fetch failed" — DNS blips, TLS
  // handshake timeouts, TCP resets). integrate.api.nvidia.com regularly
  // takes 10s+ even for tiny completions and intermittently fails the
  // FIRST connect attempt from Render; a single transient failure used
  // to abort the whole in-VM generation (surfaced to users as
  // "reverse-tunnel: NVIDIA upstream unreachable: fetch failed").
  // Safe because we retry ONLY before any chunk is yielded — the caller
  // has received nothing yet, so re-sending the request cannot
  // duplicate output. Non-2xx responses are NOT retried (those are the
  // upstream's real answer — quota/auth/model errors must surface).
  const CONNECT_ATTEMPTS = 3;
  const BACKOFF_MS = [750, 2_000];

  /** Extract undici's real failure reason from err.cause (the old code
   *  logged only "fetch failed", hiding the actual errno and making
   *  incidents undiagnosable). */
  const causeMessage = (err: unknown): string => {
    const cause = (err as { cause?: unknown } | null)?.cause;
    if (cause instanceof Error) {
      const code = (cause as { code?: unknown }).code;
      return code ? `${cause.message} (${String(code)})` : cause.message;
    }
    return cause !== undefined ? String(cause) : "";
  };

  let response: Response | null = null;
  let lastErr: unknown = null;
  for (let attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
    try {
      response = await fetch(url, {
        method: method.toUpperCase(),
        headers: outboundHeaders,
        body: hasBody ? bodyString : undefined,
        // 15 min cap — Nemotron-3.5-lightning is a reasoning model; a 16384-token
        // developer-phase generation (full app as JSON) takes ~6-10 min once
        // reasoning + code are both produced. The previous 5-min cap aborted
        // mid-stream, the VM retried, and every retry hit the same 5-min wall —
        // the generation never completed. 15 min gives the reasoning model room
        // to finish without the backend killing the fetch.
        signal: AbortSignal.timeout(900_000),
      });
      break; // connect + status line received — stop retrying
    } catch (err) {
      lastErr = err;
      const message = err instanceof Error ? err.message : "unknown fetch failure";
      const causeMsg = causeMessage(err);
      if (attempt < CONNECT_ATTEMPTS) {
        logger.warn(
          { err: message, cause: causeMsg, url, method, attempt, nextRetryInMs: BACKOFF_MS[attempt - 1] },
          "nvidia-forwarder: connect failed — retrying",
        );
        await new Promise((r) => setTimeout(r, BACKOFF_MS[attempt - 1]));
      } else {
        logger.warn(
          { err: message, cause: causeMsg, url, method, attempt },
          "nvidia-forwarder: fetch failed (all attempts exhausted)",
        );
      }
    }
  }

  if (!response) {
    const message =
      lastErr instanceof Error ? lastErr.message : "unknown fetch failure";
    const causeMsg = causeMessage(lastErr);
    throw new Error(
      `LLM upstream unreachable: ${message}${causeMsg ? ` (cause: ${causeMsg})` : ""}`,
    );
  }

  // Non-2xx: throw before yielding head. The caller will send an
  // `error` frame; the upstream status + truncated body is in the
  // message so the VM-side HTTP server can synthesize a useful
  // response to the orchestrator's client.
  if (!response.ok) {
    let detail = "";
    try {
      const text = await response.text();
      detail = text.length > 512 ? `${text.slice(0, 512)}...[truncated]` : text;
    } catch {
      detail = "<unreadable body>";
    }
    logger.warn(
      { status: response.status, url, method, detailPreview: detail.slice(0, 200) },
      "llm-forwarder: upstream non-2xx",
    );
    throw new Error(`LLM upstream HTTP ${response.status}: ${detail}`);
  }

  // 2xx — yield the head (status + headers) first.
  const responseHeaders: Record<string, string> = {};
  response.headers.forEach((value, name) => {
    // Skip hop-by-hop / framing headers — the VM doesn't need them and
    // some (content-length) would be wrong after we re-chunk.
    const lower = name.toLowerCase();
    if (
      lower === "content-length" ||
      lower === "transfer-encoding" ||
      lower === "connection"
    ) {
      return;
    }
    responseHeaders[name] = value;
  });
  yield { kind: "head", status: response.status, headers: responseHeaders };

  // Body: stream for text/event-stream, single chunk otherwise.
  const contentType = response.headers.get("content-type") || "";
  const isStream = contentType.toLowerCase().includes("text/event-stream");

  if (isStream && response.body) {
    const reader = response.body.getReader();
    const decoder = new TextDecoder("utf-8");
    try {
      for (;;) {
        const { value: bytes, done } = await reader.read();
        if (done) break;
        if (bytes) {
          const text = decoder.decode(bytes, { stream: true });
          if (text) yield { kind: "chunk", body: text };
        }
      }
      // Flush the decoder's trailing bytes (if any).
      const tail = decoder.decode();
      if (tail) yield { kind: "chunk", body: tail };
    } finally {
      try {
        reader.releaseLock();
      } catch {
        /* noop — already released */
      }
    }
  } else {
    const text = await response.text();
    if (text) yield { kind: "chunk", body: text };
  }
}
