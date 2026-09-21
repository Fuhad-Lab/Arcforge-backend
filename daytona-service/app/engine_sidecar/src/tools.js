/**
 * Forgvi Engine — the chief's Vube tool surface (custom prime-agent tools).
 *
 * The LLM-facing layer over the registry + the capability modules:
 *   orchestrate    dynamic multi-agent dispatch (the Replit pattern — the
 *                  chief writes the graph at runtime; no static pipelines)
 *   github         GitHub REST + git-data-API workspace sync (host mode:
 *                  the engine's GITHUB_TOKEN; in-VM mode uses the 1.0
 *                  tunnel bridge from connector-tools.js instead)
 *   supabase       Supabase MCP surface (execute_sql | apply_migration |
 *                  list_tables | list_projects; host mode: engine tokens)
 *   scaffold_vube  lay the Vube monorepo + pre-installed stack down
 *   list_nodes     registry discovery (agents find + link functions
 *                  dynamically)
 *
 * ask_user and request_connector live in connector-tools.js (the
 * engine-native question flow + the 1.0 consent round-trip).
 *
 * Every tool returns an honest, model-readable text result; failures and
 * needs-config outcomes are DATA (the chief and the judge read them),
 * never silent.
 */

import { Type } from "typebox";
import { dispatchParallel } from "./orchestrator-utils.js";
import { githubRest, githubSyncWorkspace, supabaseMcp } from "./mcp-tools.js";
import { scaffoldVubeWorkspace } from "./scaffold.js";
import { describeCatalog, callNode, registrySize } from "./registry.js";

/** Tool result helper — plain text content the model + judge read verbatim. */
const text = (value) => ({
  content: [{ type: "text", text: typeof value === "string" ? value : JSON.stringify(value, null, 2) }],
  details: typeof value === "string" ? { text: value } : value,
});

const MAX_TRACKS = 5;

/**
 * Build the per-run Vube tool set. `ctx` = { run, journal, mailbox, vm, localCwd }.
 * `ctx.vm` is MUTABLE — createChiefSession assigns it after the workspace
 * resolves, so every tool reads `ctx.vm` at CALL time, never build time.
 * (ask_user + request_connector come from connector-tools.js; the in-VM
 * github/supabase bridge does too — these are the HOST-mode direct-token
 * versions, wired only into the host branches of createChiefSession.)
 * Returns ToolDefinition[] (prime-agent customTools).
 */
export function buildChiefTools(ctx) {
  const { run, journal, mailbox } = ctx;

  const orchestrate = {
    name: "orchestrate",
    label: "Orchestrate specialists",
    description:
      "Dynamically dispatch specialist subagents in parallel. Provide 1-5 tracks, each {persona, task}. Personas are discovered at runtime from .prime/prompts/ — call list_nodes or see the roster in your briefing. Specialists return specs/code/checklists; you apply their output with bash/edit so the work carries tool evidence.",
    parameters: Type.Object({
      tracks: Type.Array(
        Type.Object({
          persona: Type.String({ description: "Persona key from the roster (e.g. frontend_expert, logic_architect)" }),
          task: Type.String({ description: "The concrete, self-contained task for that specialist" }),
        }),
        { minItems: 1, maxItems: MAX_TRACKS, description: "1-5 parallel specialist tracks" },
      ),
      reason: Type.Optional(Type.String({ description: "One line: why this dispatch shape now" })),
    }),
    executionMode: "sequential",
    async execute(_id, params) {
      const { results, invalid } = await dispatchParallel(params.tracks, {
        journal,
        mailbox,
        iteration: run?.iteration,
        // The debugger's terminal: terminal personas (qa_verifier) get a
        // bash tool rooted at the run's own workspace — in-VM natively,
        // VM-bound runs through the daytona-service, local runs on disk.
        workspace: { vm: ctx.vm ?? null, localCwd: ctx.vm ? null : (ctx.localCwd ?? run?.localCwd ?? null) },
      });
      if (run) {
        for (const r of results) {
          run.evidence.push({
            kind: "orchestration",
            name: `dispatch ${r.persona}`,
            status: r.ok ? "pass" : "fail",
            output: (r.report || r.error || "").slice(0, 400),
          });
        }
      }
      const summary = [
        ...(invalid.length ? [`REJECTED TRACKS:\n${invalid.map((i) => `- ${i.persona}: ${i.error}`).join("\n")}`] : []),
        ...results.map(
          (r, i) =>
            `SPECIALIST ${i + 1}/${results.length} — ${r.persona}${r.ok ? "" : " (FAILED)"}\n${(r.report || r.error || "(empty report)").slice(0, 6000)}`,
        ),
      ].join("\n\n");
      return text(summary);
    },
  };

  const github = {
    name: "github",
    label: "GitHub (MCP)",
    description:
      "GitHub operations (1.0 parity). action=rest: any GitHub REST call (method, path, body) — e.g. path=/user/repos to list repos, /repos/OWNER/NAME/issues to list issues. action=sync_workspace: commit the entire current workspace to a repo ({repo: 'owner/name', message, branch?}).",
    parameters: Type.Object({
      action: Type.Union([Type.Literal("rest"), Type.Literal("sync_workspace")], { description: "rest | sync_workspace" }),
      method: Type.Optional(Type.String({ description: "HTTP method for action=rest (default GET)" })),
      path: Type.Optional(Type.String({ description: "API path for action=rest, e.g. /repos/owner/name/contents" })),
      body: Type.Optional(Type.Record(Type.String(), Type.Any(), { description: "JSON body for action=rest" })),
      repo: Type.Optional(Type.String({ description: "owner/name for action=sync_workspace" })),
      message: Type.Optional(Type.String({ description: "Commit message for sync_workspace" })),
      branch: Type.Optional(Type.String({ description: "Target branch for sync_workspace (default: repo default)" })),
    }),
    executionMode: "sequential",
    async execute(_id, params) {
      let result;
      if (params.action === "sync_workspace") {
        result = await githubSyncWorkspace(
          { repo: params.repo, message: params.message, branch: params.branch },
          { vm: ctx.vm, localCwd: ctx.vm ? null : run?.localCwd },
        );
      } else {
        result = await githubRest({ method: params.method, path: params.path, body: params.body });
      }
      if (run) {
        run.evidence.push({
          kind: "tool",
          name: `github ${params.action}: ${params.path ?? params.repo ?? ""}`,
          status: result.ok ? "pass" : "fail",
          output: JSON.stringify(result).slice(0, 400),
        });
      }
      return text(result);
    },
  };

  const supabase = {
    name: "supabase",
    label: "Supabase (MCP)",
    description:
      "Supabase project operations (1.0 parity): execute_sql (read/write SQL, returns rows), apply_migration (versioned DDL — idempotent guards required), list_tables (public schema), list_projects. The project is bound server-side; no credentials in code or the VM.",
    parameters: Type.Object({
      tool: Type.Union(
        [Type.Literal("execute_sql"), Type.Literal("apply_migration"), Type.Literal("list_tables"), Type.Literal("list_projects")],
        { description: "execute_sql | apply_migration | list_tables | list_projects" },
      ),
      args: Type.Optional(Type.Record(Type.String(), Type.Any(), { description: "{query, name?} — the tool's arguments" })),
    }),
    executionMode: "sequential",
    async execute(_id, params) {
      const result = await supabaseMcp({ tool: params.tool, args: params.args });
      if (run) {
        run.evidence.push({
          kind: "tool",
          name: `supabase ${params.tool}`,
          status: result.ok ? "pass" : "fail",
          output: JSON.stringify(result).slice(0, 400),
        });
      }
      return text(result);
    },
  };

  const scaffoldVube = {
    name: "scaffold_vube",
    label: "Scaffold the Vube monorepo",
    description:
      "Lay down the Vube platform monorepo in the workspace: apps/web-client (Next.js with the FULL pre-installed premium UI stack — Tailwind, shadcn/ui, Motion, Lenis, GSAP, R3F, Radix, Lucide, Sonner, Vaul, Embla, TanStack Query, Zustand, RHF, Zod), apps/api-server, apps/execution-engine, packages/vube-types, packages/vube-ui, infrastructure. Use it FIRST when the goal is a web application; then npm install inside apps/web-client before running the dev server.",
    parameters: Type.Object({
      app_name: Type.Optional(Type.String({ description: "Kebab-case app name (default vube-app)" })),
    }),
    executionMode: "sequential",
    async execute(_id, params) {
      const result = await scaffoldVubeWorkspace({ vm: ctx.vm, localCwd: ctx.vm ? null : run?.localCwd }, { appName: params.app_name });
      if (run) {
        run.evidence.push({
          kind: "tool",
          name: "scaffold_vube",
          status: result.written ? "pass" : "fail",
          output: `wrote ${result.written ?? 0} files (${result.error ?? "ok"})`,
        });
        if (result.written) {
          journal?.emit(
            { type: "scaffold_written", files: (result.files ?? []).length, root: result.root },
            { role: "chief", iteration: run.iteration },
          );
        }
      }
      return text(result);
    },
  };

  const listNodes = {
    name: "list_nodes",
    label: "Discover registry nodes",
    description:
      "List the engine's capability registry — the addressable functions (dispatch, mcp, workspace, persona) with their dynamic input schemas. Use it to discover what can be linked into your orchestration.",
    parameters: Type.Object({
      kind: Type.Optional(Type.String({ description: "Filter by kind: dispatch | mcp | workspace | persona" })),
      call: Type.Optional(Type.String({ description: "Optionally execute a node by id with no/JSON args after listing" })),
    }),
    async execute(_id, params) {
      const catalog = describeCatalog({ kind: params.kind });
      let callResult = null;
      if (params.call) {
        callResult = await callNode(params.call, {});
      }
      return text(`REGISTRY (${registrySize()} nodes):\n${catalog}${callResult ? `\n\nCALL RESULT:\n${JSON.stringify(callResult, null, 2).slice(0, 2000)}` : ""}`);
    },
  };

  return { orchestrate, github, supabase, scaffoldVube, listNodes };
}
