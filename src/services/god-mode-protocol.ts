/**
 * God Mode Protocol — Enhanced with Inter-Agent Negotiation
 *
 * Phase-aware skill activation engine that enforces the mandatory
 * tool-usage chain across the generation pipeline. Now includes:
 *
 *   1. Multi-round contract negotiation between frontend & backend
 *   2. Structured multi-file generation (not single blobs)
 *   3. Conversation history feeding between agents
 *   4. File system operation awareness
 *   5. Negotiation agreement protocol
 *
 * Pipeline phases:
 *   planning              → Executive team (superpowers, sequential-thinking, memory, linear)
 *   research              → Executive + Fetch
 *   contract_negotiation  → Backend & Frontend negotiate API contract (multi-round)
 *   backend               → Engineering department (postgres, filesystem, fetch, sentry)
 *   frontend              → Design department (ui-ux-pro-max, figma-bridge)
 *   qa                    → Design (playwright, puppeteer) + Engineering (sentry)
 *   security              → Security department (github, git, brave-search)
 */

import type { PlatformSkill } from "./skill-registry";

export type PipelinePhase =
  | "planning"
  | "research"
  | "contract_negotiation"
  | "backend"
  | "frontend"
  | "qa"
  | "security";

export type Department =
  | "executive"
  | "design"
  | "engineering"
  | "security"
  | "speed";

// ─── PHASE → DEPARTMENT MAPPING ──────────────────────────────────────

const PHASE_DEPARTMENTS: Record<PipelinePhase, Department[]> = {
  planning: ["executive"],
  research: ["executive"],
  contract_negotiation: ["engineering", "design"],
  backend: ["engineering"],
  frontend: ["design"],
  qa: ["design", "engineering"],
  security: ["security", "speed"],
};

const PHASE_DESCRIPTIONS: Record<PipelinePhase, string> = {
  planning:
    "PLANNING: Use superpowers to produce a plan and contract/spec. Use memory to recall user preferences. Never write code without a valid plan.",
  research:
    "RESEARCH & CONTEXT: Use fetch to read live documentation. Use linear to read official specs/tickets if connected. Never assume API signatures — verify them.",
  contract_negotiation:
    "CONTRACT NEGOTIATION: You MUST coordinate with the other agent. The backend proposes exact endpoints, payloads, and response shapes. The frontend reviews and requests changes. Repeat until both agree. You are building a PUZZLE — every piece must fit.",
  backend:
    "BACKEND CODE GENERATION: Generate MULTIPLE files with proper directory structure. Use the filesystem skill to organize code. Every endpoint from the agreed contract MUST exist. Include models, routes, middleware, and config files.",
  frontend:
    "FRONTEND CODE GENERATION: Generate MULTIPLE files with proper directory structure. Use the agreed contract to call the EXACT endpoints the backend provides. Include components, pages, hooks, services, and types.",
  qa:
    "QUALITY ASSURANCE: Use playwright to visually test the result in a real browser. Use sentry to check for runtime errors. If the test fails, you are NOT done.",
  security:
    "SECURITY OVERRIDE: Use brave-search to check for known vulnerabilities in all packages. Use git to verify the current branch before committing. Use github to manage PRs.",
};

// ─── INTER-AGENT NEGOTIATION TYPES ──────────────────────────────────

export type NegotiatedEndpoint = {
  method: "GET" | "POST" | "PUT" | "DELETE" | "PATCH";
  path: string;
  summary: string;
  request_body?: {
    content_type: string;
    fields: Array<{
      name: string;
      type: string;
      required: boolean;
      description: string;
    }>;
  };
  response: {
    status: number;
    content_type: string;
    fields: Array<{
      name: string;
      type: string;
      description: string;
    }>;
  };
  auth_required: boolean;
};

export type NegotiatedType = {
  name: string;
  fields: Array<{
    name: string;
    type: string;
    required: boolean;
    description: string;
  }>;
};

export type BackendProposal = {
  version: 1;
  endpoints: NegotiatedEndpoint[];
  shared_types: NegotiatedType[];
  auth_scheme?: {
    type: "bearer" | "api_key" | "session" | "none";
    description: string;
  };
  file_structure: Array<{
    path: string;
    purpose: string;
  }>;
  notes?: string;
};

export type FrontendReview = {
  status: "approved" | "needs_changes";
  approved_endpoints: string[];
  change_requests: Array<{
    endpoint: string;
    issue: string;
    requested_change: string;
  }>;
  missing_needs: Array<{
    need: string;
    proposed_endpoint?: {
      method: string;
      path: string;
      purpose: string;
    };
  }>;
  file_structure: Array<{
    path: string;
    purpose: string;
  }>;
};

export type BackendRevision = {
  version: number;
  changes_made: string[];
  revised_endpoints: NegotiatedEndpoint[];
  revised_types: NegotiatedType[];
  rejected_requests: Array<{
    request: string;
    reason: string;
    alternative: string;
  }>;
  notes?: string;
};

export type AgreedContract = {
  endpoints: NegotiatedEndpoint[];
  shared_types: NegotiatedType[];
  auth_scheme: BackendProposal["auth_scheme"];
  backend_files: Array<{ path: string; purpose: string }>;
  frontend_files: Array<{ path: string; purpose: string }>;
  negotiation_rounds: number;
};

// ─── MULTI-FILE GENERATION TYPES ────────────────────────────────────

export type GeneratedFile = {
  path: string;
  content: string;
};

export type MultiFileOutput = {
  files: GeneratedFile[];
  entry_point: string;
  dependencies?: Record<string, string>;
  notes?: string;
};

// ─── NEGOTIATION STATE MACHINE ──────────────────────────────────────

export type NegotiationState =
  | { stage: "idle" }
  | { stage: "backend_proposed"; proposal: BackendProposal; round: number }
  | { stage: "frontend_reviewed"; review: FrontendReview; round: number }
  | { stage: "backend_revised"; revision: BackendRevision; round: number }
  | { stage: "agreed"; contract: AgreedContract; rounds: number }
  | { stage: "max_rounds_exceeded"; lastProposal: BackendProposal | BackendRevision };

export const MAX_NEGOTIATION_ROUNDS = 3;

/**
 * Build the negotiation system prompt for each agent role.
 */
export function negotiationPrompt(role: "backend" | "frontend", state: NegotiationState): string {
  const lines: string[] = [];

  lines.push("## INTER-AGENT NEGOTIATION PROTOCOL");
  lines.push("You are in a MULTI-AGENT SWARM. You are talking to the other agent to agree on a shared API contract.");
  lines.push("Your code and the other agent code must fit together like a PUZZLE.");
  lines.push("Every endpoint, every field name, every data type must be agreed upon by BOTH sides BEFORE any code is written.");
  lines.push("");

  if (role === "backend") {
    lines.push("### YOUR ROLE: BACKEND ARCHITECT");
    lines.push("You propose the API. Be EXACT about:");
    lines.push("  1. Every endpoint: method, path, request body fields (name + type), response fields (name + type)");
    lines.push("  2. Every shared data type: field names, types, required/optional, descriptions");
    lines.push("  3. Authentication scheme: how will the frontend authenticate?");
    lines.push("  4. Your planned file structure: which files you will create and what each does");
    lines.push("");
    lines.push("When the frontend requests changes, you MUST address each request explicitly.");
    lines.push("If you reject a request, explain WHY and offer an ALTERNATIVE.");
  } else {
    lines.push("### YOUR ROLE: FRONTEND ARCHITECT");
    lines.push("You review the backend proposal. Check for:");
    lines.push("  1. Missing endpoints you need (e.g., no delete endpoint, no search/filter)");
    lines.push("  2. Wrong field names or types (e.g., frontend needs user_avatar_url but backend only returns id)");
    lines.push("  3. Missing pagination, sorting, or filtering on list endpoints");
    lines.push("  4. Authentication flow compatibility (e.g., need refresh token endpoint)");
    lines.push("  5. Your planned file structure: which files you will create and what each does");
    lines.push("");
    lines.push("Be SPECIFIC. Do not say I need more fields. Say: On GET /users/:id, I need avatar_url (string), bio (string), and followers_count (number) in the response.");
  }

  if (state.stage !== "idle") {
    lines.push("");
    lines.push("### CURRENT NEGOTIATION STATE:");

    if (state.stage === "backend_proposed") {
      lines.push(`Round ${state.round}/${MAX_NEGOTIATION_ROUNDS} — Backend has proposed the following API:`);
      lines.push("```json");
      lines.push(JSON.stringify(state.proposal, null, 2));
      lines.push("```");
      if (role === "frontend") {
        lines.push("");
        lines.push("Review this proposal carefully. Respond with your FrontendReview JSON.");
      }
    } else if (state.stage === "frontend_reviewed") {
      lines.push(`Round ${state.round}/${MAX_NEGOTIATION_ROUNDS} — Frontend has reviewed. Status: ${state.review.status}`);
      lines.push("```json");
      lines.push(JSON.stringify(state.review, null, 2));
      lines.push("```");
      if (role === "backend") {
        lines.push("");
        if (state.review.status === "approved") {
          lines.push("The frontend approved your proposal. You may proceed to code generation.");
        } else {
          lines.push("The frontend requests changes. Address EACH change request. Provide a BackendRevision JSON.");
        }
      }
    } else if (state.stage === "backend_revised") {
      lines.push(`Round ${state.round}/${MAX_NEGOTIATION_ROUNDS} — Backend has revised the proposal:`);
      lines.push("```json");
      lines.push(JSON.stringify(state.revision, null, 2));
      lines.push("```");
      if (role === "frontend") {
        lines.push("");
        lines.push("The backend has revised. Review the changes. If acceptable, set status to approved.");
      }
    } else if (state.stage === "agreed") {
      lines.push("Contract has been agreed upon. Proceed to code generation using this contract:");
      lines.push("```json");
      lines.push(JSON.stringify(state.contract, null, 2));
      lines.push("```");
    } else if (state.stage === "max_rounds_exceeded") {
      lines.push("WARNING: Maximum negotiation rounds reached. Using the latest proposal as-is.");
      lines.push("```json");
      lines.push(JSON.stringify(state.lastProposal, null, 2));
      lines.push("```");
    }
  }

  return lines.join("\n");
}

/**
 * Build the multi-file generation prompt for an agent.
 */
export function multiFilePrompt(role: "backend" | "frontend", contract: AgreedContract): string {
  const lines: string[] = [];

  lines.push("## MULTI-FILE CODE GENERATION");
  lines.push("You MUST generate MULTIPLE files organized in a proper directory structure.");
  lines.push("Do NOT output a single monolithic file. Organize code into logical modules.");
  lines.push("");

  if (role === "backend") {
    lines.push("### Backend File Generation Rules:");
    lines.push("  - Use the EXACT endpoints from the agreed contract below");
    lines.push("  - Create separate files for: models/schemas, routes, middleware, controllers/services, config");
    lines.push("  - Use proper directory structure (e.g., src/routes/, src/models/, src/middleware/, src/config/)");
    lines.push("  - Include a package.json or requirements.txt with all needed dependencies");
    lines.push("  - The entry_point must be the main server file (e.g., src/index.ts or app.py)");
    lines.push("  - Every endpoint from the contract MUST be implemented");
    lines.push("");
    lines.push("### Your agreed file structure:");
    for (const f of contract.backend_files) {
      lines.push(`  ${f.path} — ${f.purpose}`);
    }
  } else {
    lines.push("### Frontend File Generation Rules:");
    lines.push("  - Create an API client/service file that calls EXACTLY the endpoints from the agreed contract");
    lines.push("  - Create type definitions matching the shared types from the contract");
    lines.push("  - Create separate files for: components, pages, hooks, services, types, styles");
    lines.push("  - Use proper directory structure (e.g., src/components/, src/pages/, src/hooks/, src/services/)");
    lines.push("  - Include a package.json with all needed dependencies");
    lines.push("  - The entry_point must be the main app file (e.g., src/App.tsx or src/main.tsx)");
    lines.push("  - Do NOT invent endpoints that do not exist in the contract");
    lines.push("");
    lines.push("### Your agreed file structure:");
    for (const f of contract.frontend_files) {
      lines.push(`  ${f.path} — ${f.purpose}`);
    }
  }

  lines.push("");
  lines.push("### Agreed Contract (your source of truth):");
  lines.push("```json");
  lines.push(JSON.stringify(contract, null, 2));
  lines.push("```");
  lines.push("");
  lines.push("Return ONLY valid JSON with this exact structure:");
  lines.push("```json");
  lines.push("{\"files\": [{\"path\": \"relative/path/to/file.ext\", \"content\": \"...file content...\"}], \"entry_point\": \"main/file/path.ext\", \"dependencies\": {\"package\": \"version\"}}");
  lines.push("```");

  return lines.join("\n");
}

/**
 * Build a conversation digest from the agent message history.
 */
export function conversationDigest(messages: Array<{
  from: string;
  to: string;
  subject: string;
  content: string;
  createdAt: string;
}>): string {
  if (messages.length === 0) return "";

  const lines: string[] = ["## SWARM CONVERSATION HISTORY"];
  lines.push("Below is the full conversation between agents. Read this carefully before responding.");
  lines.push("");

  for (const msg of messages) {
    lines.push(`### [${msg.from.toUpperCase()} -> ${msg.to.toUpperCase()}] ${msg.subject}`);
    lines.push(`Time: ${msg.createdAt}`);
    lines.push("");
    const content = msg.content.length > 8_000
      ? msg.content.slice(0, 8_000) + "\n\n[... truncated]"
      : msg.content;
    lines.push(content);
    lines.push("");
    lines.push("---");
    lines.push("");
  }

  return lines.join("\n");
}

/**
 * Build the file system operations prompt for an agent.
 */
export function fileSystemPrompt(): string {
  const lines: string[] = [];
  lines.push("## AVAILABLE FILE SYSTEM OPERATIONS");
  lines.push("You have access to the following workspace operations for organizing your code:");
  lines.push("");
  lines.push("  - CREATE FILE: Create a new file at any path. Parent directories are auto-created.");
  lines.push("  - EDIT FILE: Overwrite an existing file content. The file must exist first.");
  lines.push("  - DELETE: Remove a file or directory (directories must be empty, or use recursive delete).");
  lines.push("  - CREATE DIRECTORY: Create a new directory. Parent directories are auto-created.");
  lines.push("  - MOVE/RENAME: Move or rename a file or directory to a new path.");
  lines.push("  - COPY: Copy a file or directory to a new path.");
  lines.push("  - READ: Read the content of an existing file.");
  lines.push("  - LIST: List all files and directories in a given path.");
  lines.push("  - TREE: Get a visual tree representation of the workspace.");
  lines.push("");
  lines.push("All paths are relative to the project workspace root.");
  lines.push("Maximum file size: 1 MB per file.");
  lines.push("");
  lines.push("IMPORTANT: Generate your files as a structured JSON output with multiple files.");
  lines.push("The platform will create all files in the workspace for you.");
  return lines.join("\n");
}

// ─── PHASE-AWARE SKILL FILTERING ────────────────────────────────────

export function skillsForPhase(
  allSkills: PlatformSkill[],
  phase: PipelinePhase,
  activeConnections?: Set<string>,
): PlatformSkill[] {
  const departments = PHASE_DEPARTMENTS[phase];
  return allSkills.filter((skill) => {
    if (!departments.includes(skill.department)) return false;
    if (skill.requiresConnection && activeConnections) {
      return activeConnections.has(skill.requiresConnection);
    }
    return true;
  });
}

/**
 * Build the GOD_MODE_ACTIVE system prompt for a specific phase.
 */
export function godModePrompt(
  phase: PipelinePhase,
  skills: PlatformSkill[],
  mode: "single" | "swarm",
  extraContext?: string,
): string {
  const phaseDescription = PHASE_DESCRIPTIONS[phase];
  const skillInstructions = skills
    .map((skill, index) => `${index + 1}. ${skill.name}: ${skill.instruction}`)
    .join("\n");

  const sections: string[] = [
    "PROTOCOL: GOD_MODE_ACTIVE",
    `PHASE: ${phase.toUpperCase()}`,
    `MODE: ${mode.toUpperCase()}`,
    "",
    "You are a Lead Full-Stack Architect. You DO NOT guess. You DO NOT assume.",
    "You must use your installed tools for every single step of this phase.",
    "",
    "## ACTIVE PHASE DIRECTIVE:",
    phaseDescription,
    "",
    `## MANDATORY SKILLS FOR THIS PHASE (${skills.length} active):`,
    skillInstructions,
  ];

  if (extraContext) {
    sections.push("");
    sections.push(extraContext);
  }

  sections.push("");
  sections.push("## IRON RULES:");
  sections.push("- Do not claim a skill was used unless you applied its instruction.");
  sections.push("- If a connection-backed skill is unavailable, state that limitation and continue safely without fabricating data.");
  sections.push("- Every output MUST be traceable to a skill instruction or a user requirement.");

  return sections.join("\n");
}

/**
 * Build the full GOD_MODE_ACTIVE protocol header.
 */
export function protocolHeader(mode: "single" | "swarm"): string {
  const lines: string[] = [];
  lines.push("# PROTOCOL: GOD_MODE_ACTIVE");
  lines.push(`You are a Lead Full-Stack Architect operating in ${mode.toUpperCase()} mode.`);
  lines.push("You DO NOT guess. You DO NOT assume.");
  lines.push("You must use your installed tools for every single step of the development lifecycle.");
  lines.push("");
  lines.push("## MANDATORY TOOL USAGE CHAIN:");
  lines.push("1. PLANNING -> superpowers (plan) + memory (recall preferences)");
  lines.push("   Constraint: Never write code without a valid superpowers plan.");
  lines.push("2. RESEARCH & CONTEXT -> fetch (read docs) + linear (read specs)");
  lines.push("   Constraint: Never assume API signatures — verify with live docs.");
  lines.push("3. CONTRACT NEGOTIATION -> Backend & Frontend negotiate API contract");
  lines.push("   Constraint: Both agents MUST agree on every endpoint, field, and type BEFORE coding.");
  lines.push("4. BACKEND WORK -> postgres (schema) + filesystem (multi-file) + sequential-thinking (logic)");
  lines.push("   Constraint: Generate multiple files. Every contracted endpoint MUST exist.");
  lines.push("5. FRONTEND WORK -> ui-ux-pro-max (tokens) + filesystem (multi-file) + figma-bridge (layout)");
  lines.push("   Constraint: Generate multiple files. Call ONLY contracted endpoints.");
  lines.push("6. QUALITY ASSURANCE -> playwright (visual test) + sentry (error check)");
  lines.push("   Constraint: If the test fails, you are NOT done.");
  lines.push("");
  lines.push("## INTER-AGENT COMMUNICATION RULES:");
  lines.push("- The frontend and backend MUST talk to each other before writing code.");
  lines.push("- They negotiate: endpoints, data types, field names, auth flow, error handling.");
  lines.push("- This is NOT optional. Code generation is BLOCKED until both agents agree.");
  lines.push("- The agreed contract becomes the SOURCE OF TRUTH for both sides.");
  lines.push("");
  lines.push("## FILE SYSTEM OPERATIONS:");
  lines.push("- Agents generate MULTIPLE files in proper directory structures.");
  lines.push("- Available operations: create, edit, delete, move, copy, read, list, mkdir, tree.");
  lines.push("- Maximum file size: 1 MB. All paths are relative to project workspace.");
  lines.push("");
  lines.push("## SECURITY OVERRIDE:");
  lines.push("- ALWAYS use brave-search to check for security vulnerabilities in packages.");
  lines.push("- NEVER commit without git branch verification.");
  lines.push("- NEVER export without the security phase completing successfully.");
  return lines.join("\n");
}

// ─── SECURITY GATE ──────────────────────────────────────────────────

export type SecurityDiagnostic = {
  severity: "error" | "warning";
  source: "security";
  message: string;
};

export function securityGateDiagnostics(
  skillsUsed: string[],
): SecurityDiagnostic[] {
  const diagnostics: SecurityDiagnostic[] = [];
  const securitySkills = ["skill://git", "skill://github", "skill://brave-search"];
  const missingSecurity = securitySkills.filter((s) => !skillsUsed.includes(s));
  if (missingSecurity.length === securitySkills.length) {
    diagnostics.push({
      severity: "warning",
      source: "security" as const,
      message:
        "Security phase skills (git, github, brave-search) were not invoked. " +
        "Export proceeds but no vulnerability scan was performed.",
    });
  }
  return diagnostics;
}

// ─── PHASE HELPERS ──────────────────────────────────────────────────

export function phaseForRole(role: string): PipelinePhase {
  switch (role) {
    case "leader":
      return "planning";
    case "backend":
      return "backend";
    case "frontend":
      return "frontend";
    case "debugger":
      return "qa";
    default:
      return "planning";
  }
}

export const PHASE_ORDER: PipelinePhase[] = [
  "planning",
  "research",
  "contract_negotiation",
  "backend",
  "frontend",
  "qa",
  "security",
];

export const DEPARTMENTS: Record<Department, string> = {
  executive: "The Executive Team (Brain & Methodology)",
  design: "The Design Department (Frontend & UX)",
  engineering: "The Engineering Department (Backend & Data)",
  security: "Security & DevOps",
  speed: "Vibe & Speed Tools",
};
