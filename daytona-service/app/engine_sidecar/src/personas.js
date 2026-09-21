/**
 * Forgvi Engine — dynamic persona & system prompt management (Vube spec pillar 4).
 *
 * Personas are NEVER hardcoded in the source. They live as markdown profile
 * files under `.prime/prompts/` and are loaded into harness memory (`G.prompts`)
 * at boot. The chief and the spawn helper read them from memory; editing or
 * adding a file changes the roster at the next spawn (mtime-based refresh) —
 * no code change, no redeploy.
 *
 * File format:
 *   ---
 *   key: frontend_expert        ← registry key (also the filename default)
 *   name: Frontend Expert
 *   role: frontend
 *   description: one-liner for catalogs
 *   ---
 *   <persona system prompt markdown>
 */

import { readdirSync, readFileSync, statSync } from "node:fs";
import { resolve } from "node:path";

const ENGINE_DIR = resolve(new URL("..", import.meta.url).pathname);
const DEFAULT_PROMPTS_DIR = resolve(ENGINE_DIR, ".prime", "prompts");

/**
 * G — the harness memory state (the spec's `G`). Everything prompt-related
 * resolves through here so runtime code never embeds persona text.
 */
export const G = {
  /** @type {Map<string, Persona>} persona key → profile */
  prompts: new Map(),
  /** Directory the profiles were loaded from. */
  promptsDir: DEFAULT_PROMPTS_DIR,
  /** Last directory scan (for hot-reload checks). */
  loadedAt: 0,
};

/** @typedef {{key: string, name: string, role: string, description: string, markdown: string, path: string, mtimeMs: number}} Persona */

/** Parse `---` frontmatter without dependencies. */
function parseFrontmatter(text) {
  const match = text.match(/^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/);
  if (!match) return { meta: {}, body: text.trim() };
  const meta = {};
  for (const line of match[1].split(/\r?\n/)) {
    const idx = line.indexOf(":");
    if (idx === -1) continue;
    const key = line.slice(0, idx).trim().toLowerCase();
    const value = line.slice(idx + 1).trim();
    if (key) meta[key] = value;
  }
  return { meta, body: match[2].trim() };
}

/** Load (or reload) every profile in the prompts dir into G.prompts. */
export function loadPersonas(dir = DEFAULT_PROMPTS_DIR) {
  G.promptsDir = dir;
  G.prompts.clear();
  let count = 0;
  let entries = [];
  try {
    entries = readdirSync(dir);
  } catch {
    return 0;
  }
  for (const entry of entries) {
    if (!entry.endsWith(".md")) continue;
    const path = resolve(dir, entry);
    try {
      const text = readFileSync(path, "utf-8");
      const { meta, body } = parseFrontmatter(text);
      const key = String(meta.key ?? entry.replace(/\.md$/, ""));
      const mtimeMs = statSync(path).mtimeMs;
      G.prompts.set(key, {
        key,
        name: String(meta.name ?? key),
        role: String(meta.role ?? "specialist"),
        description: String(meta.description ?? ""),
        markdown: body,
        path,
        mtimeMs,
      });
      count++;
    } catch {
      /* unreadable profile — skip, never break the engine */
    }
  }
  G.loadedAt = Date.now();
  return count;
}

/**
 * Hot-reload check: cheap stat on the directory + files. Called before each
 * spawn so an edited profile applies to the next subagent without restart.
 */
function refreshIfChanged() {
  try {
    const entries = readdirSync(G.promptsDir).filter((e) => e.endsWith(".md"));
    let changed = entries.length !== G.prompts.size;
    if (!changed) {
      for (const entry of entries) {
        const path = resolve(G.promptsDir, entry);
        const known = [...G.prompts.values()].find((p) => p.path === path);
        if (!known || statSync(path).mtimeMs !== known.mtimeMs) {
          changed = true;
          break;
        }
      }
    }
    if (changed) loadPersonas(G.promptsDir);
  } catch {
    /* dir unreadable — keep whatever is in memory */
  }
}

/**
 * Fetch a persona's system prompt from harness memory (G.prompts), with the
 * spec's fallback line. Hot-reloads when files changed.
 * @returns {Persona|null}
 */
export function getPersona(key) {
  refreshIfChanged();
  return G.prompts.get(String(key ?? "").trim()) ?? null;
}

/**
 * The composed prompt for `spawn_specialist_agent`: persona system prompt +
 * assigned task, exactly the spec's Step B shape.
 */
export function composeSpecialistPrompt(personaKey, specificTask) {
  const persona = getPersona(personaKey);
  const systemPrompt = persona?.markdown ?? "You are an expert AI software agent.";
  return `${systemPrompt}

---
ASSIGNED TASK:
${specificTask}`;
}

/** Catalog for prompts/discovery surfaces (health, /personas, chief prompt). */
export function listPersonas() {
  refreshIfChanged();
  return [...G.prompts.values()].map(({ key, name, role, description }) => ({
    key,
    name,
    role,
    description,
  }));
}

/** Roster text block for the chief's prompt — the discoverable agent list. */
export function rosterBlock() {
  const list = listPersonas().filter((p) => p.role !== "chief");
  if (list.length === 0) return "(no personas loaded from .prime/prompts)";
  return list
    .map((p) => `- ${p.key} — ${p.name}: ${p.description || p.role}`)
    .join("\n");
}

// Boot load (idempotent — re-runs are explicit via loadPersonas).
loadPersonas();
