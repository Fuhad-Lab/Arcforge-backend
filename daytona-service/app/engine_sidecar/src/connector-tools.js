/**
 * Forgvi Engine — connector + interaction tools (GROUP 2 parity for 2.0).
 *
 * The tools Forgvi 1.0's swarm has, given to the 2.0 chief:
 *
 *   github             — the user's GitHub account, REST + workspace sync.
 *                        Proxied through the 1.0 orchestrator daemon
 *                        (localhost:9000 → reverse tunnel → the backend
 *                        injects the user's PAT server-side; the token
 *                        NEVER enters the VM).
 *   supabase           — official Supabase MCP operations (apply_migration,
 *                        execute_sql, list_tables, …) on the user's
 *                        CONNECTED Supabase account, same tunnel bridge.
 *   request_connector  — ask the user to connect an account (OAuth consent
 *                        round-trip). Blocks through the 1.0 orchestrator's
 *                        connector machinery: the SAME consent card, OAuth
 *                        callback and vault flow 1.0 uses — the engine's
 *                        HTTP call simply waits for the verdict.
 *   ask_user           — pause and ask the user a question. Engine-native:
 *                        the question lands in the run's journal
 *                        (question_request event → the studio's question
 *                        card), the tool blocks, and POST /runs/:id/answer
 *                        resolves it. Works in-VM AND on the host engine
 *                        because the journal + SSE are the engine's own.
 *
 * Transport selection:
 *   - IN-VM (ENGINE_IN_VM=1): the engine lives in the project's sandbox,
 *     so the orchestrator is a localhost dial (ENGINE_ORCH_BASE, default
 *     http://127.0.0.1:9000) authenticated with ENGINE_LLM_TOKEN (the
 *     VM's ORCH_TOKEN — never a provider key).
 *   - Host engine: no localhost orchestrator exists, so the connector
 *     tools degrade HONESTLY (a clear "available in the workspace VM"
 *     result the model can act on). ask_user stays fully available.
 */

import { randomUUID } from "node:crypto";
import { Type } from "typebox";

/** The 1.0 orchestrator daemon's base URL (in-VM mode). */
const ORCH_BASE = (process.env.ENGINE_ORCH_BASE ?? "http://127.0.0.1:9000").replace(/\/$/, "");
/** The shared VM secret the orchestrator's /internal routes require. */
const ORCH_TOKEN = process.env.ENGINE_LLM_TOKEN ?? "";
/** Consent round-trips can take minutes — wait generously. */
const CONNECTOR_TIMEOUT_MS = Number(process.env.ENGINE_CONNECTOR_TIMEOUT_MS ?? 620_000);

/** True when the connector bridge is reachable (engine runs in the VM). */
export function connectorBridgeAvailable() {
  return Boolean(ORCH_TOKEN);
}

/**
 * One POST to the 1.0 orchestrator's /internal/mcp/* bridge.
 * Returns the parsed JSON (any shape) or throws on transport failure.
 */
async function orchPost(path, body, timeoutMs = 120_000) {
  const res = await fetch(`${ORCH_BASE}${path}`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${ORCH_TOKEN}`,
    },
    body: JSON.stringify(body ?? {}),
    signal: AbortSignal.timeout(timeoutMs),
  });
  const text = await res.text();
  let data = null;
  try {
    data = text ? JSON.parse(text) : null;
  } catch {
    /* non-JSON — surface as text below */
  }
  if (!res.ok) {
    const detail = data?.detail ?? text.slice(0, 300);
    throw new Error(`orchestrator bridge HTTP ${res.status} on ${path}: ${detail}`);
  }
  return data ?? {};
}

/** Truncate long tool results so the chief's context stays healthy. */
function clip(text, max = 4000) {
  const str = String(text ?? "");
  return str.length > max ? str.slice(0, max) + "…[truncated]" : str;
}

/** The honest in-VM-only degradation text. */
const VM_ONLY = (what) =>
  `${what} is available when the Forgvi engine runs inside your workspace VM. ` +
  `This run is served by the shared host engine — tell the user to reopen the project ` +
  `so the in-VM engine can serve the run, or proceed without ${what} for now.`;

// ---------------------------------------------------------------------------
// ask_user — engine-native, journal-backed
// ---------------------------------------------------------------------------

/** How long ask_user waits for the studio's answer before giving up. */
const QUESTION_TIMEOUT_MS = Number(process.env.ENGINE_QUESTION_TIMEOUT_MS ?? 600_000);

/**
 * RunInteractions — the per-run ask_user registry. The goal loop owns one;
 * the journal carries every question to the studio (replayable on
 * reconnect), and POST /runs/:id/answer resolves the blocked tool.
 */
export class RunInteractions {
  constructor(journal) {
    this.journal = journal;
    /** @type {Map<string, {resolve: (answer: string|null) => void, timer: any}>} */
    this.pending = new Map();
  }

  /**
   * Ask the user a question. Emits `question_request` into the journal and
   * blocks until the answer arrives (or the timeout — resolved with null,
   * honestly, so the chief can proceed on best judgment like 1.0 does).
   */
  ask({ question, options = [], context = "" }) {
    const questionId = "q-" + randomUUID().slice(0, 8);
    return new Promise((resolve) => {
      const settle = (answer) => {
        const entry = this.pending.get(questionId);
        if (!entry) return;
        this.pending.delete(questionId);
        clearTimeout(entry.timer);
        this.journal.emit(
          {
            type: "question_resolved",
            question_id: questionId,
            answered: answer != null,
            answer: answer ?? undefined,
          },
          { role: "chief" },
        );
        resolve(answer);
      };
      const timer = setTimeout(() => settle(null), QUESTION_TIMEOUT_MS);
      this.pending.set(questionId, { resolve: settle, timer });
      this.journal.emit(
        {
          type: "question_request",
          question_id: questionId,
          question,
          options: options.slice(0, 6),
          context,
        },
        { role: "chief" },
      );
    });
  }

  /** Resolve a pending question (POST /runs/:id/answer). Returns false on unknown id. */
  resolve(questionId, answer) {
    const entry = this.pending.get(String(questionId ?? ""));
    if (!entry) return false;
    entry.resolve(answer == null ? null : String(answer).slice(0, 4000));
    return true;
  }

  /** Any unresolved questions (abort-time honesty). */
  hasPending() {
    return this.pending.size > 0;
  }

  /** Abort every pending question (run ending — unblock the tools). */
  close() {
    for (const [, entry] of this.pending) {
      clearTimeout(entry.timer);
      entry.resolve(null);
    }
    this.pending.clear();
  }
}

/**
 * ask_user tool — "pause and ask the user". The studio renders the
 * question card; the answer becomes the tool result.
 */
export function createAskUserTool(interactions) {
  return {
    name: "ask_user",
    label: "ask_user",
    description:
      "Pause and ask the user a question. Use it whenever a decision materially " +
      "changes what gets built (auth provider, styling direction, data model choices, " +
      "external services) or a blocker needs the user's input. The question appears as " +
      "an interactive card in the studio chat; the tool blocks until the user answers " +
      "(or times out — then proceed on best judgment and say so). Options are optional " +
      "short choice labels (max 6).",
    promptSnippet:
      "ask_user: pause and ask the user a question (interactive card in the studio)",
    parameters: Type.Object({
      question: Type.String({ description: "The question to show the user (one clear sentence)" }),
      options: Type.Optional(
        Type.Array(Type.String(), {
          description: "Optional short choice labels (max 6) the user can click",
          maxItems: 6,
        }),
      ),
      context: Type.Optional(
        Type.String({ description: "Why you're asking / what depends on the answer (short)" }),
      ),
    }),
    async execute(_toolCallId, params) {
      const answer = await interactions.ask({
        question: String(params.question ?? "").slice(0, 600),
        options: Array.isArray(params.options) ? params.options.map((o) => String(o).slice(0, 120)) : [],
        context: String(params.context ?? "").slice(0, 400),
      });
      const text =
        answer == null
          ? "The user did not answer in time — proceed on best judgment and note the assumption in your report."
          : `The user answered:\n${answer}`;
      return {
        content: [{ type: "text", text }],
        details: { answered: answer != null },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// github — REST + workspace sync through the 1.0 tunnel bridge
// ---------------------------------------------------------------------------

export function createGithubTool() {
  return {
    name: "github",
    label: "github",
    description:
      "Operate on the user's CONNECTED GitHub account (their PAT is injected " +
      "server-side — never in this VM). Two actions: 'rest' (any GitHub API call: " +
      "method + path like /user/repos or /repos/{owner}/{repo}/contents/{path}, " +
      "optional body object) and 'sync_workspace' (push the current /workspace " +
      "to a repo they own: repo 'owner/name', optional commit message).",
    promptSnippet: "github: the user's GitHub account — REST calls + workspace sync",
    promptGuidelines: [
      "Use the github tool for repository operations on the user's account (list repos, " +
        "read/create files, open the workspace as a repo). If it reports the account is " +
        "not connected, call request_connector first.",
    ],
    parameters: Type.Object({
      action: Type.Optional(
        Type.String({ description: "'rest' (default) or 'sync_workspace'" }),
      ),
      method: Type.Optional(
        Type.String({ description: "HTTP method for action=rest (default GET)" }),
      ),
      path: Type.Optional(
        Type.String({ description: "GitHub API path for action=rest, e.g. /user/repos" }),
      ),
      body: Type.Optional(Type.Record(Type.String(), Type.Any(), { description: "JSON body for action=rest" })),
      repo: Type.Optional(
        Type.String({ description: "Target repo 'owner/name' for action=sync_workspace" }),
      ),
      message: Type.Optional(
        Type.String({ description: "Commit message for action=sync_workspace" }),
      ),
    }),
    async execute(_toolCallId, params) {
      if (!connectorBridgeAvailable()) {
        return { content: [{ type: "text", text: VM_ONLY("the github tool") }], details: { unavailable: true } };
      }
      const action = String(params.action ?? "rest").toLowerCase();
      const payload = { action };
      if (action === "sync_workspace") {
        payload.repo = String(params.repo ?? "");
        payload.message = String(params.message ?? "Forgvi workspace sync");
      } else {
        payload.method = String(params.method ?? "GET").toUpperCase();
        payload.path = String(params.path ?? "");
        if (params.body && typeof params.body === "object") payload.body = params.body;
      }
      if (action === "rest" && !payload.path) {
        return {
          content: [{ type: "text", text: "github: rest action needs a path (e.g. /user/repos)" }],
          details: { ok: false },
        };
      }
      let data;
      try {
        data = await orchPost("/internal/mcp/github", payload);
      } catch (error) {
        return {
          content: [{ type: "text", text: `github tool failed: ${String(error?.message ?? error).slice(0, 400)}` }],
          details: { ok: false, transport: true },
        };
      }
      const status = Number(data?.status ?? 0);
      const bodyText = String(data?.body ?? "");
      const text =
        status >= 400
          ? `github tool: backend returned HTTP ${status}: ${clip(bodyText, 1200)}`
          : clip(bodyText);
      return {
        content: [{ type: "text", text }],
        details: { ok: status < 400, status },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// supabase — official MCP operations through the 1.0 tunnel bridge
// ---------------------------------------------------------------------------

export function createSupabaseTool() {
  return {
    name: "supabase",
    label: "supabase",
    description:
      "Run official Supabase MCP tools on the user's CONNECTED Supabase account " +
      "(the OAuth token is injected server-side — never in this VM). Real " +
      "schema/SQL operations: apply_migration (name + query SQL), execute_sql, " +
      "list_tables, list_projects, get_project_url, etc. Pass the MCP tool name " +
      "in 'tool' and its arguments as an object in 'args'.",
    promptSnippet: "supabase: real database/schema operations on the user's Supabase account",
    promptGuidelines: [
      "When the plan needs a real database, use the supabase tool (apply_migration for " +
        "schema, execute_sql for data) — never fake a database with JSON files when " +
        "Supabase is available. If it reports the account is not connected, call " +
        "request_connector first.",
    ],
    parameters: Type.Object({
      tool: Type.String({ description: "MCP tool name, e.g. apply_migration, execute_sql, list_tables" }),
      args: Type.Optional(
        Type.Record(Type.String(), Type.Any(), {
          description: "The MCP tool's arguments, e.g. {name, query} for apply_migration",
        }),
      ),
    }),
    async execute(_toolCallId, params) {
      if (!connectorBridgeAvailable()) {
        return { content: [{ type: "text", text: VM_ONLY("the supabase tool") }], details: { unavailable: true } };
      }
      const tool = String(params.tool ?? "").trim();
      if (!tool) {
        return {
          content: [
            { type: "text", text: "supabase: needs {tool: <mcp tool>, args: {...}} (e.g. apply_migration, execute_sql, list_tables)" },
          ],
          details: { ok: false },
        };
      }
      let data;
      try {
        data = await orchPost("/internal/mcp/supabase", { tool, args: params.args ?? {} });
      } catch (error) {
        return {
          content: [{ type: "text", text: `supabase tool failed: ${String(error?.message ?? error).slice(0, 400)}` }],
          details: { ok: false, transport: true },
        };
      }
      if (data?.needs_connector) {
        return {
          content: [
            {
              type: "text",
              text:
                `supabase is not connected (${String(data?.error ?? "")}). ` +
                `Call request_connector {connector:'supabase', capability:'${String(data?.capability ?? "supabase.database.write")}', reason:'<why>'} ` +
                `first, wait for the user's consent, then retry this tool.`,
            },
          ],
          details: { ok: false, needs_connector: true },
        };
      }
      if (!data?.ok) {
        return {
          content: [{ type: "text", text: `supabase (${tool}) failed: ${clip(data?.error, 1200)}` }],
          details: { ok: false },
        };
      }
      return {
        content: [{ type: "text", text: clip(data?.result) }],
        details: { ok: true, tool },
      };
    },
  };
}

// ---------------------------------------------------------------------------
// request_connector — OAuth consent round-trip through the 1.0 machinery
// ---------------------------------------------------------------------------

export function createRequestConnectorTool() {
  return {
    name: "request_connector",
    label: "request_connector",
    description:
      "Ask the user to connect an external account (OAuth consent). Use it when a " +
      "tool reports the account is not connected: {connector: 'supabase'|'github', " +
      "capability: e.g. 'supabase.database.write', reason: why you need it}. The " +
      "user sees a consent card, completes OAuth on the provider's site, and this " +
      "call resolves with the verdict (granted or declined). No token, no secret " +
      "ever enters this VM — only a receipt.",
    promptSnippet: "request_connector: ask the user to connect Supabase/GitHub (OAuth consent)",
    parameters: Type.Object({
      connector: Type.String({ description: "'supabase' or 'github'" }),
      capability: Type.Optional(
        Type.String({ description: "e.g. supabase.database.write, github.repos.write" }),
      ),
      reason: Type.Optional(Type.String({ description: "Why the build needs this account (shown to the user)" })),
    }),
    async execute(_toolCallId, params) {
      if (!connectorBridgeAvailable()) {
        return { content: [{ type: "text", text: VM_ONLY("request_connector") }], details: { unavailable: true } };
      }
      const connector = String(params.connector ?? "").trim().toLowerCase();
      if (connector !== "supabase" && connector !== "github") {
        return {
          content: [{ type: "text", text: "request_connector: needs {connector: 'supabase'|'github', capability, reason}" }],
          details: { ok: false },
        };
      }
      let data;
      try {
        // Blocks through the 1.0 orchestrator's connector_request_wait —
        // the SAME consent card + OAuth + vault flow Forgvi 1.0 uses.
        data = await orchPost(
          "/internal/mcp/connector",
          {
            connector,
            capability: String(params.capability ?? ""),
            reason: String(params.reason ?? ""),
          },
          CONNECTOR_TIMEOUT_MS,
        );
      } catch (error) {
        const message = String(error?.message ?? error);
        const timedOut = /timeout|aborted/i.test(message);
        return {
          content: [
            {
              type: "text",
              text: timedOut
                ? `no response for the ${connector} connection in time — proceed without it or ask the user what to do`
                : `request_connector failed: ${message.slice(0, 400)}`,
            },
          ],
          details: { ok: false, timedOut },
        };
      }
      const granted = Boolean(data?.granted);
      const text = granted
        ? `${connector} connected (${String(data?.capability ?? "")}) — ` +
          (connector === "supabase"
            ? "real schema/SQL operations are now available via the supabase tool"
            : "GitHub repository tools are available via the github tool")
        : `the user ${data?.declined ? "declined to connect" : "did not answer about"} ${connector} — proceed without it or ask the user what to do`;
      return {
        content: [{ type: "text", text }],
        details: { granted, capability: data?.capability },
      };
    },
  };
}
