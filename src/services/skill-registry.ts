import type { Department, PipelinePhase } from "./god-mode-protocol";

export type AgentMode = "single" | "swarm";

/** In-VM swarm roles a skill can serve (skills-server.py segregation). */
export type VmScope = "chief" | "frontend" | "backend" | "debugger";

export type PlatformSkill = {
  id: string;
  name: string;
  department: Department;
  phases: PipelinePhase[];
  role: "executive" | "design" | "engineering" | "security" | "speed";
  source: string;
  description: string;
  instruction: string;
  requiresConnection?: string;
  /**
   * Which in-VM swarm agents may consult this skill via the skills MCP
   * server. EMPTY = not hosted in the VM (redundant there). STRICT
   * per-agent segregation is the user mandate: the backend agent can
   * never consult "UI/UX Pro Max", the frontend agent never gets the
   * backend-only skills.
   */
  vmScope?: VmScope[];
};

// ─── 🧠 THE EXECUTIVE TEAM ─────────────────────────────────────────────

const EXECUTIVE_SKILLS: PlatformSkill[] = [
  {
    id: "superpowers",
    name: "Superpowers",
    department: "executive",
    phases: ["planning", "research", "contract_negotiation", "frontend", "backend", "qa", "security"],
    role: "executive",
    source: "obra/superpowers",
    description:
      "Forces Plan → Spec → Code workflow. Stops the AI from hallucinating code without a roadmap.",
    instruction:
      "Always produce and follow a plan, then a contract/spec, then implementation. Do not jump directly to code. In swarm mode, the plan is the foundation for inter-agent negotiation.",
    vmScope: ["chief"],
  },
  {
    id: "sequential-thinking",
    name: "Sequential Thinking",
    department: "executive",
    phases: ["planning", "contract_negotiation", "backend"],
    role: "executive",
    source: "modelcontextprotocol/servers/sequential-thinking",
    description:
      "Forces the AI to break complex logic into step-by-step chains before answering.",
    instruction:
      "For complex decisions, reason in ordered steps and verify each dependency before proceeding. Use this during contract negotiation to systematically verify each endpoint.",
    vmScope: ["chief", "backend"],
  },
  {
    id: "memory",
    name: "Memory",
    department: "executive",
    phases: ["planning", "research"],
    role: "executive",
    source: "modelcontextprotocol/server-memory",
    description:
      "Gives the AI a persistent knowledge graph. Remembers user preferences across sessions.",
    instruction:
      "Use durable project context when available and record only reusable decisions, never secrets or transient logs.",
    vmScope: ["chief"],
  },
  {
    id: "linear",
    name: "Linear",
    department: "executive",
    phases: ["planning", "research"],
    role: "executive",
    source: "modelcontextprotocol/servers/linear",
    description:
      "Connects to Linear.app. Reads the official product ticket/spec directly.",
    instruction:
      "Treat the connected project ticket as an authoritative product input when one is provided.",
    requiresConnection: "linear",
  },
];

// ─── 🎨 THE DESIGN DEPARTMENT ──────────────────────────────────────────

const DESIGN_SKILLS: PlatformSkill[] = [
  {
    id: "ui-ux-pro-max",
    name: "UI/UX Pro Max",
    department: "design",
    phases: ["contract_negotiation", "frontend", "qa"],
    role: "design",
    source: "nextlevelbuilder/ui-ux-pro-max-skill",
    description:
      "192+ rules for spacing, typography, and color theory. Prevents AI-slop designs.",
    instruction:
      "Use deliberate hierarchy, spacing, typography, contrast, responsive behavior, accessible states, and avoid generic AI-slop layouts.",
    vmScope: ["frontend"],
  },
  {
    id: "figma-bridge",
    name: "Figma Bridge",
    department: "design",
    phases: ["contract_negotiation", "frontend"],
    role: "design",
    source: "sonnylazuardi/cursor-talk-to-figma",
    description:
      "Reads actual Figma files to extract exact hex codes and pixel values.",
    instruction:
      "When Figma context exists, use its exact measurements, colors, and component intent rather than approximating.",
    requiresConnection: "figma",
  },
  {
    id: "playwright",
    name: "Playwright",
    department: "design",
    phases: ["qa"],
    role: "design",
    source: "modelcontextprotocol/server-playwright",
    description:
      "Spins up a real browser to see and click the website. Verifies buttons and layouts.",
    instruction:
      "Validate important user journeys in a real browser and inspect title, body, HTTP status, and console errors.",
    vmScope: ["frontend", "debugger"],
  },
  {
    id: "puppeteer",
    name: "Puppeteer",
    department: "design",
    phases: ["research", "qa"],
    role: "design",
    source: "modelcontextprotocol/servers/puppeteer",
    description:
      "Browser inspection and competitor layout research.",
    instruction:
      "Use browser inspection for research only; do not copy protected content or send uncontrolled actions.",
    vmScope: [],
  },
];

// ─── ⚙️ THE ENGINEERING DEPARTMENT ─────────────────────────────────────

const ENGINEERING_SKILLS: PlatformSkill[] = [
  {
    id: "postgresql",
    name: "PostgreSQL",
    department: "engineering",
    phases: ["contract_negotiation", "backend"],
    role: "engineering",
    source: "modelcontextprotocol/servers/postgres",
    description:
      "Direct database access. Inspect schemas, run migrations, optimize queries.",
    instruction:
      "Inspect schema before changing data, use parameterized queries, and require explicit approval for destructive mutations.",
    requiresConnection: "postgres",
  },
  {
    id: "filesystem",
    name: "FileSystem",
    department: "engineering",
    phases: ["contract_negotiation", "backend", "frontend"],
    role: "engineering",
    source: "modelcontextprotocol/servers/filesystem",
    description:
      "Manages project files inside an isolated workspace. Supports create, edit, delete, move, copy, read, list, mkdir, and tree operations on files and directories.",
    instruction:
      "Use the workspace to organize code into MULTIPLE files with proper directory structure. Never generate a monolithic single file. Use atomic writes. All paths are relative to the project root.",
    vmScope: ["frontend", "backend"],
  },
  {
    id: "fetch",
    name: "Fetch",
    department: "engineering",
    phases: ["research", "contract_negotiation", "frontend", "backend"],
    role: "engineering",
    source: "modelcontextprotocol/servers/fetch",
    description:
      "Converts web pages to Markdown. Essential for reading live documentation.",
    instruction:
      "Fetch authoritative documentation when library behavior may have changed; cite the retrieved source in the plan.",
    vmScope: ["backend"],
  },
  {
    id: "sentry",
    name: "Sentry",
    department: "engineering",
    phases: ["qa"],
    role: "engineering",
    source: "modelcontextprotocol/servers/sentry",
    description:
      "Reads runtime error events and stack traces.",
    instruction:
      "Use real error context to diagnose failures before proposing a fix; never invent stack traces.",
    requiresConnection: "sentry",
  },
];

// ─── 🔒 SECURITY & DEVOPS ─────────────────────────────────────────────

const SECURITY_SKILLS: PlatformSkill[] = [
  {
    id: "github",
    name: "GitHub",
    department: "security",
    phases: ["security"],
    role: "security",
    source: "modelcontextprotocol/server-github",
    description:
      "Manages PRs, checks CI/CD status, ensures code is committed to the right branch.",
    instruction:
      "Keep changes reviewable, inspect CI status, and never push or merge without an explicit authorization boundary.",
    requiresConnection: "github",
  },
  {
    id: "git",
    name: "Git",
    department: "security",
    phases: ["security"],
    role: "security",
    source: "modelcontextprotocol/server-git",
    description:
      "Local version-control checkpoints. Feature branches and granular commits.",
    instruction:
      "Make granular reversible checkpoints around risky work and inspect the diff before export.",
    vmScope: ["frontend", "backend"],
  },
  {
    id: "brave-search",
    name: "Brave Search",
    department: "security",
    phases: ["research", "security"],
    role: "security",
    source: "modelcontextprotocol/servers/brave-search",
    description:
      "Searches the web for security vulnerabilities and best practices.",
    instruction:
      "Use current security research for dependency and architecture decisions; distinguish search results from verified facts.",
    requiresConnection: "brave-search",
  },
];

// ─── ⚡ VIBE & SPEED TOOLS ─────────────────────────────────────────────

const SPEED_SKILLS: PlatformSkill[] = [
  {
    id: "time",
    name: "Time",
    department: "speed",
    phases: ["backend", "security"],
    role: "speed",
    source: "modelcontextprotocol/servers/time",
    description:
      "Timezone-aware date and scheduling context.",
    instruction:
      "Use an explicit timezone for scheduling and date logic; never assume the server timezone is the user timezone.",
    vmScope: ["frontend", "backend", "debugger"],
  },
  {
    id: "slack",
    name: "Slack",
    department: "speed",
    phases: ["security"],
    role: "speed",
    source: "modelcontextprotocol/servers/slack",
    description:
      "Communicates build and incident updates to the connected team.",
    instruction:
      "Draft notifications by default and require explicit authorization before sending messages.",
    requiresConnection: "slack",
  },
];

// ─── COMPOSITE REGISTRY ────────────────────────────────────────────────

/** All 17 platform skills, ordered by department. */
export const PLATFORM_SKILLS: PlatformSkill[] = [
  ...EXECUTIVE_SKILLS,
  ...DESIGN_SKILLS,
  ...ENGINEERING_SKILLS,
  ...SECURITY_SKILLS,
  ...SPEED_SKILLS,
];

/** Skill IDs that must be acknowledged before export. */
export const REQUIRED_SKILL_IDS = PLATFORM_SKILLS.map((skill) => skill.id);

/** Build a skill:// URI. */
export function skillUri(skill: PlatformSkill): string {
  return `skill://${skill.id}`;
}

/** Backward-compatible flat prompt (used as fallback when no phase is specified). */
export function mandatorySkillPrompt(mode: AgentMode): string {
  const instructions = PLATFORM_SKILLS
    .map((skill, index) => `${index + 1}. ${skill.name}: ${skill.instruction}`)
    .join("\n");
  return [
    `MANDATORY PLATFORM POLICY (${mode.toUpperCase()} MODE): You must apply every one of the ${PLATFORM_SKILLS.length} installed skills below.`,
    "Do not claim a skill was used unless you applied its instruction. If a connection-backed skill is unavailable, state that limitation and continue safely without fabricating data.",
    instructions,
  ].join("\n");
}

/** Get skills grouped by department. Useful for the /mcp/skills endpoint
 *  so the frontend can display them in categories. */
export function skillsByDepartment(): Record<Department, PlatformSkill[]> {
  return {
    executive: EXECUTIVE_SKILLS,
    design: DESIGN_SKILLS,
    engineering: ENGINEERING_SKILLS,
    security: SECURITY_SKILLS,
    speed: SPEED_SKILLS,
  };
}

/**
 * The catalog hosted by the IN-VM skills MCP server (skills_server.py).
 *
 * - connection-backed skills are excluded (the VM cannot reach them);
 * - each skill carries its vmScope so the server enforces STRICT
 *   per-agent segregation (chief / frontend / backend / debugger).
 */
export function vmSkillsCatalog(): Array<{
  name: string;
  scope: VmScope[];
  description: string;
  instruction: string;
  source: string;
}> {
  return PLATFORM_SKILLS
    .filter((s) => !s.requiresConnection && (s.vmScope ?? []).length > 0)
    .map((s) => ({
      name: s.name,
      scope: s.vmScope ?? [],
      description: s.description,
      instruction: s.instruction,
      source: s.source,
    }));
}
