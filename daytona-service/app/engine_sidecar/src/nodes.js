/**
 * Forgvi Engine — registry node registration (Vube pillar 3 wiring).
 *
 * Registers every engine capability into the Universal Node Registry at
 * boot so agents (and the chief's list_nodes tool) can discover and link
 * functions dynamically. Nodes are thin bridges over the capability
 * modules — the same handlers the chief's tools call.
 */

import { registerNode } from "./registry.js";
import { createSchema } from "./schemas.js";
import { dispatchParallel, spawnSpecialistAgent } from "./orchestrator-utils.js";
import { githubRest, githubSyncWorkspace, supabaseMcp } from "./mcp-tools.js";
import { scaffoldVubeWorkspace } from "./scaffold.js";
import { composeSpecialistPrompt, getPersona, listPersonas } from "./personas.js";
import { runHardGates } from "./hard-gates.js";

// ── persona nodes ─────────────────────────────────────────────────────
registerNode({
  id: "persona.get",
  kind: "persona",
  label: "Get persona profile",
  description: "Fetch one persona's composed system prompt by key.",
  schema: createSchema("persona.get", [{ name: "key", type: "string", required: true }]),
  handler: async ({ key }) => {
    const persona = getPersona(key);
    if (!persona) return { ok: false, error: `unknown persona "${key}"` };
    return { ok: true, key: persona.key, name: persona.name, systemPrompt: persona.markdown };
  },
});

registerNode({
  id: "persona.spawn",
  kind: "persona",
  label: "Spawn specialist agent",
  description: "Spawn one rlm-style specialist (persona key + task) and await its report.",
  schema: createSchema("persona.spawn", [
    { name: "persona", type: "string", required: true },
    { name: "task", type: "string", required: true },
  ]),
  handler: async ({ persona, task }, ctx) => spawnSpecialistAgent(persona, task, { signal: ctx?.signal }),
});

registerNode({
  id: "persona.list",
  kind: "persona",
  label: "List personas",
  description: "The loaded persona roster (from .prime/prompts/).",
  schema: createSchema("persona.list", []),
  handler: async () => listPersonas(),
});

// ── dispatch nodes ────────────────────────────────────────────────────
registerNode({
  id: "orchestrate.dispatch",
  kind: "dispatch",
  label: "Parallel specialist dispatch",
  description: "Dispatch 1-5 specialist tracks concurrently (the asyncio.gather equivalent).",
  schema: createSchema("orchestrate.dispatch", [
    {
      name: "tracks",
      type: "array",
      required: true,
      description: "[{persona, task}] (JSON array or comma list)",
    },
  ]),
  handler: async ({ tracks }, ctx) => {
    let parsed = tracks;
    if (typeof parsed === "string") {
      try {
        parsed = JSON.parse(parsed);
      } catch {
        return { ok: false, error: "orchestrate.dispatch: tracks must be JSON" };
      }
    }
    if (!Array.isArray(parsed)) return { ok: false, error: "orchestrate.dispatch: tracks must be an array" };
    return dispatchParallel(parsed, {
      journal: ctx?.journal,
      mailbox: ctx?.mailbox,
      iteration: ctx?.iteration,
    });
  },
});

// ── MCP nodes (1.0 parity) ────────────────────────────────────────────
registerNode({
  id: "mcp.github.rest",
  kind: "mcp",
  label: "GitHub REST",
  description: "Any GitHub REST API call with the engine's token.",
  schema: createSchema("mcp.github.rest", [
    { name: "method", type: "string", required: false },
    { name: "path", type: "string", required: true },
    { name: "body", type: "object", required: false },
  ]),
  handler: async (input) => githubRest(input),
});

registerNode({
  id: "mcp.github.sync_workspace",
  kind: "mcp",
  label: "GitHub workspace sync",
  description: "Commit the workspace to a repo via the git data API.",
  schema: createSchema("mcp.github.sync_workspace", [
    { name: "repo", type: "string", required: true },
    { name: "message", type: "string", required: false },
    { name: "branch", type: "string", required: false },
  ]),
  handler: async (input, ctx) => githubSyncWorkspace(input, { vm: ctx?.vm, localCwd: ctx?.localCwd }),
});

registerNode({
  id: "mcp.supabase",
  kind: "mcp",
  label: "Supabase MCP",
  description: "execute_sql | apply_migration | list_tables | list_projects.",
  schema: createSchema("mcp.supabase", [
    { name: "tool", type: "string", required: true },
    { name: "args", type: "object", required: false },
  ]),
  handler: async (input) => supabaseMcp(input),
});

// ── workspace nodes ───────────────────────────────────────────────────
registerNode({
  id: "workspace.scaffold_vube",
  kind: "workspace",
  label: "Scaffold the Vube monorepo",
  description: "Write the full Vube platform monorepo + pre-installed stack into the workspace.",
  schema: createSchema("workspace.scaffold_vube", [{ name: "appName", type: "string", required: false }]),
  handler: async ({ appName }, ctx) =>
    scaffoldVubeWorkspace({ vm: ctx?.vm, localCwd: ctx?.localCwd }, { appName }),
});

registerNode({
  id: "workspace.vm_exec",
  kind: "workspace",
  label: "VM exec",
  description: "Run a shell command in the run's VM (no-op shape without one).",
  schema: createSchema("workspace.vm_exec", [
    { name: "command", type: "string", required: true },
    { name: "cwd", type: "string", required: false },
  ]),
  handler: async ({ command, cwd }, ctx) => {
    if (!ctx?.vm) return { ok: false, error: "workspace.vm_exec: no VM bound" };
    return ctx.vm.exec(command, { cwd: cwd ?? "/workspace" });
  },
});

// ── utility nodes ─────────────────────────────────────────────────────
registerNode({
  id: "prompt.compose_specialist",
  kind: "utility",
  label: "Compose specialist prompt",
  description: "Persona system prompt + assigned task (the spec's Step B composition).",
  schema: createSchema("prompt.compose_specialist", [
    { name: "persona", type: "string", required: true },
    { name: "task", type: "string", required: true },
  ]),
  handler: async ({ persona, task }) => composeSpecialistPrompt(persona, task),
});

// ── verification nodes ────────────────────────────────────────────────
registerNode({
  id: "verify.hard_gates",
  kind: "verification",
  label: "Run the deterministic hard gates",
  description:
    "Run the deterministic production-readiness gates against the run's workspace: production build proof and the dev-server port law. Gate failures make run completion impossible. (The auth-persistence law — real database chosen with the user + cookies, never sqlite/localStorage — is system-prompt law, not a machine gate.)",
  schema: createSchema("verify.hard_gates", [
    { name: "command", type: "string", required: false, description: "unused — gates run their fixed probe set" },
  ]),
  handler: async (_input, ctx) =>
    runHardGates({
      workspace: { vm: ctx?.vm ?? null, localCwd: ctx?.localCwd ?? null },
      evidence: ctx?.evidence ?? [],
    }),
});
