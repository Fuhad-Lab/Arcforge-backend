/**
 * Forgvi Engine — the Universal Node Registry (Vube spec pillar 3, the
 * execution_core/registry.py equivalent).
 *
 * A single decoupled registry where every capability — dispatchers, MCP
 * bridges, workspace operations, persona spawners — registers itself with a
 * name, a kind, a dynamic schema, and a handler. Agents (and the chief's
 * tools) discover and link functions DYNAMICALLY through this registry at
 * runtime instead of importing static graphs:
 *
 *   registerNode({ id, kind, label, description, schema, handler })
 *   listNodes({ kind })        → discover
 *   callNode(id, input)        → link + execute (schema-validated)
 *   describeCatalog()          → prompt-ready text for LLM discovery
 *
 * Nodes are cheap, idempotent, and side-effect-free to re-register
 * (last registration wins) so runtime composition stays fluid.
 */

import { createSchema, validate } from "./schemas.js";

/** @typedef {Object} NodeEntry
 * @property {string} id registry id (e.g. "vm.exec")
 * @property {string} kind capability family (dispatch | mcp | workspace | persona | utility)
 * @property {string} label human-readable
 * @property {string} description for discovery prompts
 * @property {DynSchema} schema dynamic input schema
 * @property {(input: any, ctx: any) => Promise<any>} handler
 */

/** The registry itself — module-level singleton (one engine, one graph). */
const NODES = new Map();

/**
 * Register (or replace) a node. The decoupled decorator equivalent:
 *   const dispatch = registerNode({ id: "orchestrate.dispatch", ... });
 */
export function registerNode(entry) {
  if (!entry?.id || typeof entry.handler !== "function") {
    throw new Error(`registry: node needs id + handler (${entry?.id ?? "?"})`);
  }
  NODES.set(String(entry.id), {
    id: String(entry.id),
    kind: String(entry.kind ?? "utility"),
    label: String(entry.label ?? entry.id),
    description: String(entry.description ?? ""),
    schema: entry.schema ?? createSchema(entry.id, []),
    handler: entry.handler,
    registeredAt: Date.now(),
  });
  return entry.id;
}

/** Look a node up. */
export function getNode(id) {
  return NODES.get(String(id)) ?? null;
}

/** Discover nodes — optional filters by kind. */
export function listNodes({ kind } = {}) {
  const all = [...NODES.values()];
  return kind ? all.filter((n) => n.kind === kind) : all;
}

/**
 * Link + execute: validate the input against the node's dynamic schema,
 * then dispatch to the handler. Validation failures return an honest
 * structured error (never throw raw).
 */
export async function callNode(id, input, ctx) {
  const node = getNode(id);
  if (!node) {
    return { ok: false, error: `registry: unknown node "${id}"` };
  }
  const { ok, value, errors } = validate(node.schema, input ?? {});
  if (!ok) {
    return { ok: false, error: `registry: "${id}" input invalid — ${errors.join("; ")}` };
  }
  try {
    const result = await node.handler(value, ctx ?? {});
    return { ok: true, result };
  } catch (error) {
    return { ok: false, error: `registry: "${id}" failed — ${String(error?.message ?? error)}` };
  }
}

/** Prompt-ready catalog (what an LLM sees to discover + link nodes). */
export function describeCatalog({ kind } = {}) {
  const nodes = listNodes({ kind });
  if (nodes.length === 0) return "(registry empty)";
  return nodes
    .map((n) => {
      const fields = n.schema.fields
        .map((f) => `${f.name}:${f.type}${f.required ? "" : "?"}${f.values ? `(${f.values.join("|")})` : ""}`)
        .join(", ");
      return `- ${n.id} [${n.kind}] ${n.label} — ${n.description} (${fields || "no args"})`;
    })
    .join("\n");
}

/** Count for health/diagnostics. */
export function registrySize() {
  return NODES.size;
}
