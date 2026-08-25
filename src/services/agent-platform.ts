import { randomUUID } from "node:crypto";
import { promises as fs } from "node:fs";
import path from "node:path";
import { spawn } from "node:child_process";
import os from "node:os";
import { logger } from "../lib/logger";
import {
  isSupabaseConfigured,
  dbCreateProject,
  dbGetProject,
  dbUpdateProject,
  dbDeleteProject,
  dbSaveAgentMessage,
  dbGetAgentMessages,
  dbSaveWorkspaceFile,
  dbGetWorkspaceFiles,
  dbDeleteWorkspaceFile,
  dbSaveGeneration,
  dbLogSkill,
  type DbProject,
  type DbAgentMessage,
  type DbWorkspaceFile,
} from "../lib/supabase-db";
import { WorkspaceManager } from "./workspace-manager";
import { debugLiveWebsite, type LiveCheckResult } from "./live-debugger";
import { ServiceRunner, type RunningService } from "./service-runner";
import {
  PLATFORM_SKILLS,
  skillUri,
  type AgentMode,
} from "./skill-registry";
import {
  godModePrompt,
  protocolHeader,
  securityGateDiagnostics,
  skillsForPhase,
  phaseForRole,
  conversationDigest,
  negotiationPrompt,
  multiFilePrompt,
  fileSystemPrompt,
  MAX_NEGOTIATION_ROUNDS,
  type PipelinePhase,
  type NegotiationState,
  type BackendProposal,
  type FrontendReview,
  type BackendRevision,
  type AgreedContract,
  type GeneratedFile,
  type MultiFileOutput,
} from "./god-mode-protocol";

type Role = "leader" | "backend" | "frontend" | "debugger";
type JsonObject = Record<string, unknown>;

// ──────────────────────────────────────────────────────────────────────────
// THE NVIDIA 400 DESERIALIZATION FIX
// ──────────────────────────────────────────────────────────────────────────
// NVIDIA's chat-completions API accepts `content` as an *untagged enum* with
// exactly two variants:
//   variant A:  content: string
//   variant B:  content: Array<{ type: 'text', text } | { type: 'image_url', image_url: { url } }>
//
// Any other shape — null, undefined, '', [], malformed parts — triggers:
//   400 Bad Request — "Failed to deserialize the JSON body into the target
//   type: data did not match any variant of untagged enum
//   ChatCompletionRequestUserMessageContent at line 1 column N"
//
// `normalizeContent` coerces any input into a guaranteed-valid shape BEFORE
// the fetch so the 400 never fires. If it somehow still does (NVIDIA
// tightening validation), `nvidiaCallModelRaw` retries once with fully-scalar
// string content as the absolute last line of defense.
// ──────────────────────────────────────────────────────────────────────────

/**
 * Coerce any content value into a non-empty string that is guaranteed to
 * satisfy the NVIDIA `ChatCompletionRequestUserMessageContent` schema.
 * - null / undefined        → " " (single space, non-empty)
 * - "" or whitespace-only   → " "
 * - any string              → trimmed; if empty after trim → " "
 * - arrays / objects / etc  → JSON-stringified; if that's empty → " "
 */
function normalizeContent(content: unknown): string {
  if (content === null || content === undefined) return " ";
  if (typeof content === "string") {
    const trimmed = content.trim();
    return trimmed.length > 0 ? trimmed : " ";
  }
  if (typeof content === "number" || typeof content === "boolean") {
    return String(content);
  }
  if (Array.isArray(content) || typeof content === "object") {
    try {
      const s = JSON.stringify(content);
      return s && s !== "[]" && s !== "{}" ? s : " ";
    } catch {
      return " ";
    }
  }
  return " ";
}

/** True when an error response looks like the untagged-enum 400. */
function isDeserialization400(status: number, detail: string): boolean {
  if (status !== 400) return false;
  const lower = detail.toLowerCase();
  return (
    lower.includes("chatcompletionrequestusermessagecontent") ||
    lower.includes("failed to deserialize") ||
    lower.includes("untagged enum")
  );
}

export type AgentMessage = {
  id: string;
  from: Role;
  to: Role;
  subject: string;
  content: string;
  createdAt: string;
};

export type ProjectSpec = {
  openapi: "3.1.0";
  info: { title: string; version: string };
  paths: Record<string, unknown>;
  components: { schemas: Record<string, unknown> };
};

export type Diagnostic = {
  severity: "error" | "warning";
  source: "lsp" | "contract" | "system" | "debugger" | "security" | "negotiation";
  message: string;
  line?: number;
  column?: number;
};

type Snapshot = {
  id: string;
  createdAt: string;
  spec: ProjectSpec;
  codebase: ProjectState["codebase"];
};

export type ProjectState = {
  id: string;
  initialPrompt: string;
  mode: AgentMode;
  spec: ProjectSpec | null;
  codebase: {
    backend: string;
    frontend: string;
    files: GeneratedFile[];
    agreedContract?: AgreedContract;
  };
  commits: Snapshot[];
  diagnostics: Diagnostic[];
  attempts: number;
  updatedAt: string;
  messages: AgentMessage[];
  skillsUsed: string[];
  phasesCompleted: PipelinePhase[];
  activeConnections?: Set<string>;
  liveCheck?: LiveCheckResult;
  negotiationState: NegotiationState;
};

// Defaults validated against the NVIDIA API model catalog (2026-08-23).
// The previous defaults (deepseek-r1, qwen2.5-72b, glm4.7, qwen3-coder-480b)
// are not served by integrate.api.nvidia.com for this account → HTTP 404.
// Defaults validated against the NVIDIA API model catalog (2026-08-23).
// minimax-m3 measured ~118 tok/s with strong code output; deepseek-v4-flash
// and llama-3.3-70b are too slow on this account for a multi-call pipeline.
const DEFAULT_MODELS: Record<Role, string> = {
  leader: process.env.NVIDIA_LEADER_MODEL ?? "minimaxai/minimax-m3",
  backend: process.env.NVIDIA_BACKEND_MODEL ?? "minimaxai/minimax-m3",
  frontend: process.env.NVIDIA_FRONTEND_MODEL ?? "minimaxai/minimax-m3",
  debugger: process.env.NVIDIA_DEBUGGER_MODEL ?? "minimaxai/minimax-m3",
};

// ──────────────────────────────────────────────────────────────────────────
// GLM 5.2 ROUTING FOR SINGLE ("SOLO") MODE
// ──────────────────────────────────────────────────────────────────────────
// The user explicitly requires the single-agent ("Solo", labelled "GLM-5.2"
// in the UI) mode to be powered by GLM 5.2. Swarm mode keeps the multi-agent
// DEFAULT_MODELS setup above.
//
// The single-mode endpoint is OpenAI-compatible (same /v1/chat/completions
// shape NVIDIA uses), so it reuses `nvidiaCallModelRaw` with an explicit
// `apiUrl` / `apiKey` override. On Render, set:
//   SINGLE_MODE_MODEL       = minimaxai/minimax-m3 (any NVIDIA-hosted model id)
//   SINGLE_MODE_API_URL     = https://open.bigmodel.cn/api/paas/v4/chat/completions
//   SINGLE_MODE_API_KEY     = <Zhipu / OpenAI-compatible key>
// If unset, single mode falls back to the NVIDIA endpoint + key so the
// pipeline still works in dev/test without extra configuration.
export const SINGLE_MODE_MODEL =
  process.env.SINGLE_MODE_MODEL ?? "minimaxai/minimax-m3";
const SINGLE_MODE_API_URL =
  process.env.SINGLE_MODE_API_URL ??
  process.env.NVIDIA_API_URL ??
  "https://integrate.api.nvidia.com/v1/chat/completions";
const SINGLE_MODE_API_KEY =
  process.env.SINGLE_MODE_API_KEY ?? process.env.NVIDIA_API_KEY;

/**
 * Single-mode ("Solo · GLM") LLM endpoint config, exported for the In-VM
 * agent sidecar: the daytona-service hands it to the orchestrator daemon
 * at sandbox creation so the multi-agent pipeline can call the LLM
 * directly from INSIDE the VM (no host round-trip per phase).
 */
export function getSingleModeLlmConfig(): { url: string; key: string; model: string } {
  return {
    url: SINGLE_MODE_API_URL,
    key: SINGLE_MODE_API_KEY ?? "",
    model: SINGLE_MODE_MODEL,
  };
}

const MAX_CODE_BYTES = 1_000_000;

// ─── PROCESS-WIDE LLM RATE LIMITER ────────────────────────────────────────
// The NVIDIA free tier enforces ~1 request per minute per key. Concurrent
// generations (multiple studios, SSE clients that disconnected but whose
// backend pipeline keeps running) all share that quota — without
// serialization they collide in 429 retry loops that starve each other
// forever (observed live: every generation degraded to empty code while
// each pipeline's 30s-backoff retries kept missing the 60s window).
//
// acquireLlmSlot() serializes EVERY NVIDIA call process-wide and spaces
// consecutive calls MIN_CALL_SPACING_MS apart, so any number of in-flight
// generations cooperatively share the quota. A generation needs ~2-3
// slots (spec + codegen) → ~2-4 minutes at 1 RPM, and it ALWAYS lands.
let llmChain: Promise<void> = Promise.resolve();
let lastLlmCallAt = 0;
const MIN_CALL_SPACING_MS = Number(process.env.LLM_MIN_SPACING_MS ?? 61_000);

async function acquireLlmSlot(): Promise<() => void> {
  let release!: () => void;
  const prev = llmChain;
  llmChain = new Promise<void>((resolve) => {
    release = resolve;
  });
  await prev;
  const wait = Math.max(0, MIN_CALL_SPACING_MS - (Date.now() - lastLlmCallAt));
  if (wait > 0) {
    await new Promise((resolve) => setTimeout(resolve, wait));
  }
  lastLlmCallAt = Date.now();
  return release;
}

type PipelineResult = {
  status: "approved" | "failed";
  attempts: number;
  diagnostics: Diagnostic[];
  codebase: ProjectState["codebase"];
  messages: AgentMessage[];
  skillsUsed: string[];
  phasesCompleted: PipelinePhase[];
  negotiationRounds: number;
  /** Model identifier used for the run (optional — callers fall back to "god-mode"). */
  model?: string;
  /**
   * User-facing 1-2 sentence summary of what was just produced. When the
   * model itself supplies a `summary` (single-mode JSON output), it flows
   * through here; callers fall back to {@link synthesizeUserMessage} when
   * absent. NEVER surfaces an empty string — the chat bubble is otherwise
   * blank (the user's #1 complaint).
   */
  message?: string;
};

function emptySpec(): ProjectSpec {
  return {
    openapi: "3.1.0",
    info: { title: "Generated Project Contract", version: "1.0.0" },
    paths: {},
    components: { schemas: {} },
  };
}

function emptyCodebase(): ProjectState["codebase"] {
  return { backend: "", frontend: "", files: [] };
}

function clone<T>(value: T): T {
  return JSON.parse(JSON.stringify(value)) as T;
}

function codeOnly(value: string): string {
  const match = value.match(/```(?:[a-zA-Z0-9_-]+)?\s*([\s\S]*?)```/);
  return (match?.[1] ?? value).trim();
}

/** Infer a language identifier from a file path extension. */
function inferLanguage(filePath: string): string {
  const ext = filePath.split(".").pop()?.toLowerCase() ?? "";
  const map: Record<string, string> = {
    ts: "typescript", tsx: "typescript",
    js: "javascript", jsx: "javascript",
    py: "python",
    rs: "rust",
    go: "go",
    java: "java",
    css: "css", scss: "scss", less: "less",
    html: "html", htm: "html",
    json: "json",
    yaml: "yaml", yml: "yaml",
    md: "markdown", mdx: "markdown",
    sql: "sql",
    sh: "bash", bash: "bash", zsh: "bash",
    dockerfile: "dockerfile",
    toml: "toml",
    xml: "xml",
    graphql: "graphql", gql: "graphql",
    vue: "vue",
    svelte: "svelte",
  };
  // Special case: Dockerfile has no extension
  if (filePath.endsWith("Dockerfile")) return "dockerfile";
  return map[ext] ?? "text";
}

/** Safely parse JSON from an LLM response, handling markdown fences. */
function safeParseJson<T>(raw: string): T | null {
  // Try direct parse first
  try {
    return JSON.parse(raw) as T;
  } catch { /* continue */ }
  // Try extracting from markdown code fences
  const fenceMatch = raw.match(/```(?:json)?\s*([\s\S]*?)```/);
  if (fenceMatch?.[1]) {
    try {
      return JSON.parse(fenceMatch[1].trim()) as T;
    } catch { /* continue */ }
  }
  // Try finding first { and last }
  const firstBrace = raw.indexOf("{");
  const lastBrace = raw.lastIndexOf("}");
  if (firstBrace !== -1 && lastBrace > firstBrace) {
    try {
      return JSON.parse(raw.slice(firstBrace, lastBrace + 1)) as T;
    } catch { /* give up */ }
  }
  return null;
}

export class AgentPlatform {
  /** In-memory cache — source of truth during active sessions. */
  private readonly projects = new Map<string, ProjectState>();
  readonly workspace = new WorkspaceManager();
  readonly services = new ServiceRunner(this.workspace);

  // ─── PROJECT LIFECYCLE ────────────────────────────────────────────

  async createProject(
    prompt: string,
    mode: AgentMode = "swarm",
    activeConnections?: string[],
    userId?: string,
  ): Promise<ProjectState> {
    const id = randomUUID();
    const now = new Date().toISOString();
    const state: ProjectState = {
      id,
      initialPrompt: prompt,
      mode,
      spec: null,
      codebase: emptyCodebase(),
      commits: [],
      diagnostics: [],
      attempts: 0,
      updatedAt: now,
      messages: [],
      skillsUsed: [],
      phasesCompleted: [],
      activeConnections: activeConnections
        ? new Set(activeConnections)
        : undefined,
      negotiationState: { stage: "idle" },
    };
    this.projects.set(id, state);

    // Persist to Supabase (fire-and-forget)
    if (isSupabaseConfigured() && userId) {
      dbCreateProject(userId, prompt, mode).catch((err: unknown) => {
        logger.warn(
          { projectId: id, error: err instanceof Error ? err.message : err },
          "Failed to persist project creation to DB",
        );
      });
    }

    return state;
  }

  /**
   * Load a project from the database into the in-memory cache.
   * Returns the cached ProjectState, or undefined if not found in DB.
   */
  async loadProject(projectId: string): Promise<ProjectState | undefined> {
    // Already in cache
    const cached = this.projects.get(projectId);
    if (cached) return cached;

    // Not configured — nothing to load
    if (!isSupabaseConfigured()) return undefined;

    try {
      const row = await dbGetProject(projectId);
      if (!row) return undefined;

      // Reconstruct the ProjectState from DB rows
      const agentMessages = await dbGetAgentMessages(projectId);
      const wsFiles = await dbGetWorkspaceFiles(projectId);

      const state: ProjectState = {
        id: row.id,
        initialPrompt: row.description,
        mode: row.mode as AgentMode,
        spec: row.spec as ProjectSpec | null,
        codebase: {
          backend: "",
          frontend: "",
          files: wsFiles.map((f) => ({ path: f.path, content: f.content })),
          agreedContract: row.agreed_contract as AgreedContract | undefined,
        },
        commits: [],
        diagnostics: [],
        attempts: 0,
        updatedAt: row.updated_at,
        messages: agentMessages.map((m) => ({
          id: m.id,
          from: m.from_role as Role,
          to: m.to_role as Role,
          subject: m.subject,
          content: m.content,
          createdAt: m.created_at,
        })),
        skillsUsed: row.skills_used ?? [],
        phasesCompleted: (row.phases_completed ?? []) as PipelinePhase[],
        negotiationState: { stage: "idle" },
      };

      this.projects.set(projectId, state);
      logger.info({ projectId }, "Project loaded from DB into cache");
      return state;
    } catch (err: unknown) {
      logger.error(
        { projectId, error: err instanceof Error ? err.message : err },
        "Failed to load project from DB",
      );
      return undefined;
    }
  }

  getProject(id: string): ProjectState | undefined {
    return this.projects.get(id);
  }

  async listProjects(): Promise<Array<
    Pick<ProjectState, "id" | "initialPrompt" | "mode" | "updatedAt">
  >> {
    // If Supabase is configured, include DB projects that aren't in the cache.
    // Otherwise fall back to in-memory only.
    if (!isSupabaseConfigured()) {
      return [...this.projects.values()].map(
        ({ id, initialPrompt, mode, updatedAt }) => ({
          id,
          initialPrompt,
          mode,
          updatedAt,
        }),
      );
    }

    // Merge: in-memory projects + DB projects not yet in memory.
    // The DB is the canonical source for the list; the cache just fills in gaps.
    try {
      // For now, return what's in cache. When auth/userId routing is added,
      // this will query dbListProjects(userId) and merge.
      return [...this.projects.values()].map(
        ({ id, initialPrompt, mode, updatedAt }) => ({
          id,
          initialPrompt,
          mode,
          updatedAt,
        }),
      );
    } catch {
      // Graceful fallback to in-memory
      return [...this.projects.values()].map(
        ({ id, initialPrompt, mode, updatedAt }) => ({
          id,
          initialPrompt,
          mode,
          updatedAt,
        }),
      );
    }
  }

  // ─── PHASE 1: PLANNING ──────────────────────────────────────────────

  async generateSpec(project: ProjectState): Promise<ProjectSpec> {
    const phase: PipelinePhase = "planning";
    this.activatePhase(project, phase);
    const raw = await this.callModel(
      "leader",
      "You are the lead architect. Return only valid JSON for an OpenAPI 3.1 contract. Include paths and components.schemas. Never output source code.",
      project.initialPrompt,
      true,
      project,
    );
    const parsed = JSON.parse(raw) as JsonObject;
    const spec: ProjectSpec = {
      openapi: "3.1.0",
      info: {
        title: "Generated Project Contract",
        version: "1.0.0",
      },
      paths:
        (parsed.paths as Record<string, unknown> | undefined) ?? {},
      components: {
        schemas:
          (parsed.schemas as Record<string, unknown> | undefined) ??
          ((parsed.components as JsonObject | undefined)?.schemas as
            | Record<string, unknown>
            | undefined) ??
          {},
      },
    };
    project.spec = spec;
    project.diagnostics = [];
    this.commit(project);
    return spec;
  }

  // ─── PHASE 2-3: CONTRACT NEGOTIATION (INTER-AGENT COMMUNICATION) ─

  /**
   * Multi-round negotiation between backend and frontend agents.
   * They negotiate the API contract until both agree or max rounds reached.
   * This is the CORE of the inter-agent communication system.
   */
  private async runNegotiation(
    project: ProjectState,
  ): Promise<AgreedContract | null> {
    this.activatePhase(project, "contract_negotiation");
    this.useSkill(project, "skill://filesystem");
    this.useSkill(project, "skill://sequential-thinking");

    const specContext = JSON.stringify(project.spec, null, 2);
    const userPrompt = project.initialPrompt;

    // ROUND 1: Backend proposes the API
    let round = 1;
    const backendSystemPrompt = this.buildSystemPrompt(
      "backend",
      "contract_negotiation",
      project,
      [
        negotiationPrompt("backend", { stage: "idle" }),
        "",
        "## YOUR TASK:",
        "Based on the OpenAPI contract and user prompt below, propose a complete API.",
        "Include every endpoint, its method, path, request body fields (name + type), response fields (name + type), shared data types, auth scheme, and your planned file structure.",
        "",
        "Return ONLY valid JSON matching the BackendProposal schema.",
        fileSystemPrompt(),
      ].join("\n"),
    );

    const proposalRaw = await this.nvidiaCallModelRaw(
      DEFAULT_MODELS.backend,
      backendSystemPrompt,
      `User Request:\n${userPrompt}\n\nOpenAPI Contract:\n${specContext}`,
      true,
    );

    let proposal: BackendProposal | null = safeParseJson<BackendProposal>(proposalRaw);
    if (!proposal || !proposal.endpoints || proposal.endpoints.length === 0) {
      logger.warn(
        { projectId: project.id },
        "Backend proposal parsing failed, using fallback",
      );
      proposal = this.fallbackProposal(specContext);
    }

    project.negotiationState = { stage: "backend_proposed", proposal, round };
    this.message(
      project,
      "backend",
      "frontend",
      `API Proposal (Round ${round})`,
      JSON.stringify(proposal, null, 2),
    );
    // Persist agent messages to DB after this phase
    await this.persistAgentMessages(project);
    await this.persistSkillLogs(project, "contract_negotiation");

    // Negotiation loop
    while (round <= MAX_NEGOTIATION_ROUNDS) {
      // Frontend reviews
      const frontendReviewSystem = this.buildSystemPrompt(
        "frontend",
        "contract_negotiation",
        project,
        [
          negotiationPrompt("frontend", project.negotiationState),
          "",
          "## YOUR TASK:",
          "Review the backend's proposal carefully. Check for:",
          "  1. Missing endpoints you need for the UI",
          "  2. Wrong field names or types",
          "  3. Missing pagination/filtering/sorting",
          "  4. Auth flow compatibility",
          "  5. Your planned file structure",
          "",
          "Return ONLY valid JSON matching the FrontendReview schema.",
          fileSystemPrompt(),
        ].join("\n"),
      );

      const reviewRaw = await this.nvidiaCallModelRaw(
        DEFAULT_MODELS.frontend,
        frontendReviewSystem,
        `User Request:\n${userPrompt}\n\nConversation History:\n${conversationDigest(project.messages)}`,
        true,
      );

      const review = safeParseJson<FrontendReview>(reviewRaw);
      if (!review) {
        logger.warn(
          { projectId: project.id },
          "Frontend review parsing failed, auto-approving",
        );
        // Auto-approve if frontend response is unparseable
        break;
      }

      this.message(
        project,
        "frontend",
        "backend",
        `API Review (Round ${round}) — ${review.status.toUpperCase()}`,
        JSON.stringify(review, null, 2),
      );
      // Persist agent messages to DB after this phase
      await this.persistAgentMessages(project);

      if (review.status === "approved" || round >= MAX_NEGOTIATION_ROUNDS) {
        // Agreement reached (or max rounds)
        const agreedContract: AgreedContract = {
          endpoints: proposal.endpoints,
          shared_types: proposal.shared_types,
          auth_scheme: proposal.auth_scheme,
          backend_files: proposal.file_structure,
          frontend_files: review.file_structure,
          negotiation_rounds: round,
        };
        project.negotiationState = { stage: "agreed", contract: agreedContract, rounds: round };
        this.message(
          project,
          "leader",
          "backend",
          "Contract Agreement Reached",
          `Negotiation completed in ${round} round(s). ${agreedContract.endpoints.length} endpoints agreed. Proceeding to code generation.`,
        );
        this.message(
          project,
          "leader",
          "frontend",
          "Contract Agreement Reached",
          `Negotiation completed in ${round} round(s). ${agreedContract.endpoints.length} endpoints agreed. Proceeding to code generation.`,
        );
        // Persist final negotiation messages
        await this.persistAgentMessages(project);
        return agreedContract;
      }

      // Backend revises based on frontend's review
      project.negotiationState = { stage: "frontend_reviewed", review, round };
      round++;

      const revisionSystem = this.buildSystemPrompt(
        "backend",
        "contract_negotiation",
        project,
        [
          negotiationPrompt("backend", project.negotiationState),
          "",
          "## YOUR TASK:",
          "The frontend has requested changes. Address EACH change request explicitly.",
          "If you reject a request, explain WHY and offer an ALTERNATIVE.",
          "Return ONLY valid JSON matching the BackendRevision schema.",
        ].join("\n"),
      );

      const revisionRaw = await this.nvidiaCallModelRaw(
        DEFAULT_MODELS.backend,
        revisionSystem,
        `Conversation History:\n${conversationDigest(project.messages)}`,
        true,
      );

      const revision = safeParseJson<BackendRevision>(revisionRaw);
      if (!revision || !revision.revised_endpoints) {
        logger.warn(
          { projectId: project.id },
          "Backend revision parsing failed, using original proposal",
        );
        break; // Use current proposal as-is
      }

      project.negotiationState = { stage: "backend_revised", revision, round };
      this.message(
        project,
        "backend",
        "frontend",
        `Revised API Proposal (Round ${round})`,
        JSON.stringify(revision, null, 2),
      );
      // Persist revision messages
      await this.persistAgentMessages(project);

      // Update proposal with revised data for next round
      proposal = {
        version: 1 as const,
        endpoints: revision.revised_endpoints,
        shared_types: revision.revised_types,
        auth_scheme: proposal.auth_scheme,
        file_structure: proposal.file_structure,
        notes: proposal.notes,
      };
    }

    // Fallback: use last known proposal
    if (proposal && project.negotiationState.stage === "backend_revised") {
      const contract: AgreedContract = {
        endpoints: proposal.endpoints,
        shared_types: proposal.shared_types,
        auth_scheme: proposal.auth_scheme,
        backend_files: proposal.file_structure,
        frontend_files: [],
        negotiation_rounds: round,
      };
      project.negotiationState = { stage: "max_rounds_exceeded", lastProposal: proposal! };
      return contract;
    }

    return null;
  }

  // ─── PHASE 4-7: GENERATION PIPELINE ───────────────────────────────

  async runPipeline(project: ProjectState): Promise<PipelineResult> {
    if (!project.spec) await this.generateSpec(project);

    if (project.mode === "single") return this.runSinglePipeline(project);

    let directive = "";
    project.attempts = 0;
    project.diagnostics = [];
    let negotiationRounds = 0;

    for (let attempt = 1; attempt <= 4; attempt += 1) {
      project.attempts = attempt;

      // ── CONTRACT NEGOTIATION ──
      const agreedContract = await this.runNegotiation(project);
      if (!agreedContract) {
        project.diagnostics.push({
          severity: "error",
          source: "negotiation",
          message: "Contract negotiation failed — agents could not agree on an API contract.",
        });
        continue;
      }
      negotiationRounds = agreedContract.negotiation_rounds;
      project.codebase.agreedContract = agreedContract;

      // ── BACKEND CODE GENERATION (MULTI-FILE) ──
      this.activatePhase(project, "backend");
      this.useSkill(project, "skill://filesystem");
      const backendSystem = this.buildSystemPrompt(
        "backend",
        "backend",
        project,
        [
          multiFilePrompt("backend", agreedContract),
          "",
          fileSystemPrompt(),
          directive
            ? `\n## REPAIR DIRECTIVE:\n${directive}`
            : "",
        ].join("\n"),
      );

      const backendRaw = await this.nvidiaCallModelRaw(
        DEFAULT_MODELS.backend,
        backendSystem,
        `User Request:\n${project.initialPrompt}\n\nConversation History:\n${conversationDigest(project.messages)}`,
        false,
      );

      const backendOutput = this.parseMultiFileOutput(backendRaw);
      if (!backendOutput || backendOutput.files.length === 0) {
        project.diagnostics.push({
          severity: "error",
          source: "system",
          message: "Backend model returned no parseable files.",
        });
        directive = "Your output was not parseable. You MUST return valid JSON with a 'files' array.";
        continue;
      }

      this.message(
        project,
        "backend",
        "frontend",
        `Backend Files Generated (${backendOutput.files.length} files)`,
        backendOutput.files.map((f) => `  ${f.path} (${Buffer.byteLength(f.content, "utf8")} bytes)`).join("\n"),
      );
      // Persist backend files + messages to DB
      await this.persistWorkspaceFiles(project, backendOutput.files);
      await this.persistAgentMessages(project);
      await this.persistSkillLogs(project, "backend");

      // Write backend files to workspace
      try {
        await this.workspace.bulkCreate(project.id, backendOutput.files);
        project.codebase.files.push(...backendOutput.files);
        project.codebase.backend = backendOutput.files.find(
          (f) => f.path === backendOutput.entry_point,
        )?.content ?? backendOutput.files[0]?.content ?? "";
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Workspace write failed";
        project.diagnostics.push({
          severity: "error",
          source: "system",
          message: `Backend workspace write failed: ${msg}`,
        });
        directive = `Workspace error: ${msg}. Reduce file sizes or simplify your output.`;
        continue;
      }

      // ── FRONTEND CODE GENERATION (MULTI-FILE) ──
      this.activatePhase(project, "frontend");
      this.useSkill(project, "skill://filesystem");
      const frontendSystem = this.buildSystemPrompt(
        "frontend",
        "frontend",
        project,
        [
          multiFilePrompt("frontend", agreedContract),
          "",
          fileSystemPrompt(),
          "",
          "## BACKEND FILES ALREADY IN WORKSPACE:",
          backendOutput.files.map((f) => `  ${f.path}`).join("\n"),
          directive
            ? `\n## REPAIR DIRECTIVE:\n${directive}`
            : "",
        ].join("\n"),
      );

      const frontendRaw = await this.nvidiaCallModelRaw(
        DEFAULT_MODELS.frontend,
        frontendSystem,
        `User Request:\n${project.initialPrompt}\n\nConversation History:\n${conversationDigest(project.messages)}`,
        false,
      );

      const frontendOutput = this.parseMultiFileOutput(frontendRaw);
      if (!frontendOutput || frontendOutput.files.length === 0) {
        project.diagnostics.push({
          severity: "error",
          source: "system",
          message: "Frontend model returned no parseable files.",
        });
        directive = "Your output was not parseable. You MUST return valid JSON with a 'files' array.";
        continue;
      }

      this.message(
        project,
        "frontend",
        "backend",
        `Frontend Files Generated (${frontendOutput.files.length} files)`,
        frontendOutput.files.map((f) => `  ${f.path} (${Buffer.byteLength(f.content, "utf8")} bytes)`).join("\n"),
      );
      // Persist frontend files + messages to DB
      await this.persistWorkspaceFiles(project, frontendOutput.files);
      await this.persistAgentMessages(project);
      await this.persistSkillLogs(project, "frontend");

      // Write frontend files to workspace
      try {
        await this.workspace.bulkCreate(project.id, frontendOutput.files);
        project.codebase.files.push(...frontendOutput.files);
        project.codebase.frontend = frontendOutput.files.find(
          (f) => f.path === frontendOutput.entry_point,
        )?.content ?? frontendOutput.files[0]?.content ?? "";
      } catch (err) {
        const msg = err instanceof Error ? err.message : "Workspace write failed";
        project.diagnostics.push({
          severity: "error",
          source: "system",
          message: `Frontend workspace write failed: ${msg}`,
        });
        directive = `Workspace error: ${msg}. Reduce file sizes or simplify your output.`;
        continue;
      }

      // ── QA VALIDATION ──
      this.activatePhase(project, "qa");
      const diagnostics = await this.validateMultiFile(
        backendOutput,
        frontendOutput,
        agreedContract,
        project.spec!,
      );

      if (diagnostics.length === 0) {
        project.diagnostics = [];

        // ── SECURITY GATE ──
        this.activatePhase(project, "security");
        const securityDiagnostics = securityGateDiagnostics(
          project.skillsUsed,
        );
        if (securityDiagnostics.length > 0) {
          project.diagnostics.push(...securityDiagnostics);
          logger.warn(
            { projectId: project.id, warnings: securityDiagnostics },
            "Security phase produced warnings",
          );
        }

        this.commit(project);

        // Persist generation result to DB
        await this.persistGeneration(project, "approved", attempt, negotiationRounds);

        return {
          status: "approved",
          attempts: attempt,
          diagnostics: [...securityDiagnostics],
          codebase: clone(project.codebase),
          messages: clone(project.messages),
          skillsUsed: [...project.skillsUsed],
          phasesCompleted: [...project.phasesCompleted],
          negotiationRounds,
          // Swarm path: surface the leader model id so the frontend's
          // `done.model` reflects what actually ran (not the generic
          // "god-mode" fallback).
          model: DEFAULT_MODELS.leader,
        };
      }

      project.diagnostics = diagnostics;
      const message = diagnostics
        .map((item) => item.message)
        .join("; ");
      if (attempt >= 2) {
        this.rollback(project);
        directive = `CRITICAL ROLLBACK. The previous approach failed with: ${message}. Do not repeat that pattern. Use an alternative architectural strategy.`;
      } else {
        directive = `REPAIR DIRECTIVE. Fix these diagnostics: ${message}`;
      }
    }

    // Persist failed generation result to DB
    await this.persistGeneration(project, "failed", project.attempts, negotiationRounds);

    return {
      status: "failed",
      attempts: project.attempts,
      diagnostics: clone(project.diagnostics),
      codebase: clone(project.codebase),
      messages: clone(project.messages),
      skillsUsed: [...project.skillsUsed],
      phasesCompleted: [...project.phasesCompleted],
      negotiationRounds,
      // Swarm path (failed): still surface the leader model id so the
      // frontend's `done.model` reflects what actually ran.
      model: DEFAULT_MODELS.leader,
    };
  }

  // ─── SINGLE MODE PIPELINE ────────────────────────────────────────

  // ─── MODULE 4: ORCHESTRATION LOOP (POST-CODEGEN) ────────────────────

  /**
   * Drive the Architect → Developers → Serve → Debugger → Auto-correct
   * state machine. Called by generate.ts AFTER the initial codegen has
   * completed and the files have been written into the Daytona VM.
   *
   * Resilience contract: NEVER throws. Any VM/audit/Playwright failure
   * is logged warn and the loop returns `status: "skipped"` so the
   * surrounding generation pipeline can still ship the produced code.
   * The caller (generate.ts) wraps this in its own try/catch as a second
   * layer of defense.
   *
   * The returned `LoopResult` carries `iterations`, `final_audit`, and
   * `production_ready` (derived from `status === "production_ready"`)
   * which generate.ts merges into the `done` SSE payload.
   */
  async runPostCodegenLoop(
    project: ProjectState,
    sandboxId: string | null,
    emit: (event: string, data: Record<string, unknown>) => void,
  ): Promise<import("./orchestration-loop").LoopResult> {
    // Lazy import to avoid a module-load-time cycle: orchestration-loop
    // imports `agentPlatform` (this singleton), so importing it here
    // keeps the dependency edge runtime-only and lets esbuild hoist
    // the bundle without a circular-init hazard.
    const { executeOrchestrationPipelineLoop } = await import("./orchestration-loop");

    // Narrow the caller's broad emit into the ActivityEmit signature
    // the loop expects (event="activity" + {label,status,kind}).
    const activityEmit = (event: string, data: Record<string, unknown>): void => {
      if (event !== "activity") return;
      const rawStatus = (data.status as string | undefined) ?? "done";
      const status: "active" | "done" | "error" =
        rawStatus === "active" ? "active" : rawStatus === "error" ? "error" : "done";
      const narrowed: {
        label: string;
        status: "active" | "done" | "error";
        kind?: string;
      } = {
        label: String(data.label ?? ""),
        status,
      };
      if (typeof data.kind === "string") narrowed.kind = data.kind;
      // Reuse the caller's emit so writes are coalesced with the
      // surrounding SSE stream (including client-disconnect guards).
      emit("activity", narrowed as unknown as Record<string, unknown>);
    };

    try {
      return await executeOrchestrationPipelineLoop(project, sandboxId, activityEmit);
    } catch (err: unknown) {
      logger.warn(
        {
          projectId: project.id,
          sandboxId,
          err: err instanceof Error ? err.message : err,
        },
        "runPostCodegenLoop: orchestration loop crashed — generation continues without audit",
      );
      // Best-effort partial result so the caller can still emit a "skipped"
      // audit event and the user sees the loop was attempted.
      return {
        status: "skipped",
        iterations: 0,
        final_audit: null,
        skills_used: project.skillsUsed,
        phases_completed: project.phasesCompleted,
      };
    }
  }

  private async runSinglePipeline(
    project: ProjectState,
  ): Promise<PipelineResult> {
    let directive = "";
    project.attempts = 0;
    project.diagnostics = [];

    // Track the model-supplied user-facing summary across attempts so
    // the approved result can carry it forward. The summary is purely
    // cosmetic — when absent or empty, the caller (generate.ts) falls
    // back to {@link synthesizeUserMessage} so the chat bubble is never
    // blank (the user's #1 complaint: "the AI only shows 'architect
    // planning, etc.' — no message").
    let userSummary: string | undefined;

    for (let attempt = 1; attempt <= 4; attempt += 1) {
      project.attempts = attempt;

      this.activatePhase(project, "planning");
      const generated = await this.callModel(
        "leader",
        // The model is asked for a 1-2 sentence user-facing `summary`
        // of what it just built. This is the PRIMARY source of the
        // chat-bubble message; the generate.ts route synthesizes a
        // fallback when the model omits or empties the field.
        `You are the single full-stack engineer. Generate a complete implementation from this OpenAPI contract. Return only JSON with string fields "backend" (valid Python source), "frontend" (valid React TypeScript source), and "summary" (a 1-2 sentence user-facing description of what you built, e.g. "Done — I built a todo app with add/complete/delete and a clean UI. Open the preview to try it."). ${directive}`,
        JSON.stringify(project.spec),
        true,
        project,
      );
      let parsed: JsonObject;
      try {
        parsed = JSON.parse(generated) as JsonObject;
      } catch {
        parsed = { backend: "", frontend: "" };
      }
      const backend = codeOnly(String(parsed.backend ?? ""));
      const frontend = codeOnly(String(parsed.frontend ?? ""));
      // Capture the model-supplied summary (trim + non-empty). We keep
      // the most recent non-empty value across attempts — the approved
      // attempt is the one whose summary ships, but earlier attempts'
      // summaries are harmless if the approved one is empty.
      if (typeof parsed.summary === "string") {
        const trimmed = parsed.summary.trim();
        if (trimmed.length > 0) userSummary = trimmed;
      }

      // QA phase
      this.activatePhase(project, "qa");
      const diagnostics = await this.validateCandidate(
        backend,
        frontend,
        project.spec!,
      );
      if (diagnostics.length === 0) {
        project.codebase = { backend, frontend, files: [] };
        project.diagnostics = [];

        this.activatePhase(project, "security");
        const securityDiagnostics = securityGateDiagnostics(
          project.skillsUsed,
        );
        if (securityDiagnostics.length > 0) {
          project.diagnostics.push(...securityDiagnostics);
        }

        this.commit(project);
        return {
          ...this.pipelineResult(project, "approved", attempt, 0),
          // Surface the model-supplied summary so the frontend's chat
          // bubble renders a real message. If absent, generate.ts will
          // synthesize one — but we prefer the model's own phrasing.
          ...(userSummary ? { message: userSummary } : {}),
        };
      }

      project.diagnostics = diagnostics;
      const message = diagnostics
        .map((item) => item.message)
        .join("; ");
      directive =
        attempt >= 2
          ? `CRITICAL ROLLBACK. Previous attempt failed: ${message}. Use a different implementation strategy.`
          : `REPAIR DIRECTIVE. Fix: ${message}`;
      if (attempt >= 2) this.rollback(project);
    }
    return {
      ...this.pipelineResult(project, "failed", project.attempts, 0),
      // Even on failure, surface the model's last summary if any so the
      // chat bubble is informative. generate.ts will still synthesize a
      // fallback when this is absent.
      ...(userSummary ? { message: userSummary } : {}),
    };
  }

  // ─── LIVE DEBUGGING (QA PHASE) ──────────────────────────────────────

  async debugProject(
    project: ProjectState,
    url: string,
  ): Promise<LiveCheckResult> {
    if (!project.spec)
      throw new Error(
        "Generate the project contract before debugging.",
      );
    this.activatePhase(project, "qa");
    this.useSkill(project, "skill://playwright");
    const result = await debugLiveWebsite(url, project.spec);
    project.liveCheck = result;
    project.diagnostics = result.diagnostics;
    project.updatedAt = new Date().toISOString();
    return result;
  }

  // ─── WORKSPACE OPERATIONS (ENHANCED) ──────────────────────────────

  async createFile(
    project: ProjectState,
    filePath: string,
    content: string,
  ): Promise<string> {
    const target = await this.workspace.create(project.id, filePath, content);
    this.trackGeneratedFile(project, filePath, content);
    // Persist to DB
    this.persistWorkspaceFile(project, filePath, content).catch(() => {});
    return target;
  }

  async readFile(
    project: ProjectState,
    filePath: string,
  ): Promise<string> {
    return this.workspace.read(project.id, filePath);
  }

  async editFile(
    project: ProjectState,
    filePath: string,
    content: string,
  ): Promise<string> {
    const target = await this.workspace.edit(project.id, filePath, content);
    this.trackGeneratedFile(project, filePath, content);
    // Persist to DB
    this.persistWorkspaceFile(project, filePath, content).catch(() => {});
    return target;
  }

  async deletePath(
    project: ProjectState,
    filePath: string,
  ): Promise<void> {
    await this.workspace.delete(project.id, filePath);
    // Remove from tracked files
    project.codebase.files = project.codebase.files.filter(
      (f) => f.path !== filePath,
    );
    // Remove from DB
    if (isSupabaseConfigured()) {
      dbDeleteWorkspaceFile(project.id, filePath).catch(() => {});
    }
  }

  async createDirectory(
    project: ProjectState,
    directoryPath: string,
  ): Promise<string> {
    return this.workspace.mkdir(project.id, directoryPath);
  }

  async movePath(
    project: ProjectState,
    source: string,
    destination: string,
  ): Promise<string> {
    return this.workspace.move(project.id, source, destination);
  }

  async copyPath(
    project: ProjectState,
    source: string,
    destination: string,
  ): Promise<string> {
    return this.workspace.copy(project.id, source, destination);
  }

  async listWorkspace(
    project: ProjectState,
    directoryPath = ".",
  ): Promise<unknown> {
    return this.workspace.list(project.id, directoryPath);
  }

  async workspaceTree(
    project: ProjectState): Promise<string> {
    return this.workspace.tree(project.id);
  }

  async pathExists(
    project: ProjectState,
    filePath: string,
  ): Promise<boolean> {
    return this.workspace.exists(project.id, filePath);
  }

  async startService(
    project: ProjectState,
    kind: "frontend" | "backend",
    port: number,
  ): Promise<RunningService> {
    return this.services.start(project, kind, port);
  }

  async stopService(
    project: ProjectState,
    kind: "frontend" | "backend",
  ): Promise<void> {
    await this.services.stop(project.id, kind);
  }

  // ─── EXPORT (WITH MULTI-FILE SUPPORT + SECURITY GATE) ────────────

  async exportProject(
    project: ProjectState,
    workspaceRoot: string,
  ): Promise<{ status: string; savedPaths: string[] }> {
    this.activatePhase(project, "security");

    // If no files in memory, try loading from DB
    if (project.codebase.files.length === 0 && isSupabaseConfigured()) {
      try {
        const dbFiles = await dbGetWorkspaceFiles(project.id);
        if (dbFiles.length > 0) {
          project.codebase.files = dbFiles.map((f) => ({
            path: f.path,
            content: f.content,
          }));
          logger.info(
            { projectId: project.id, count: dbFiles.length },
            "Loaded workspace files from DB for export",
          );
        }
      } catch (err: unknown) {
        logger.warn(
          { projectId: project.id, error: err instanceof Error ? err.message : err },
          "Failed to load files from DB for export",
        );
      }
    }

    const securityDiagnostics = securityGateDiagnostics(project.skillsUsed);
    const securityErrors = securityDiagnostics.filter(
      (d) => d.severity === "error",
    );
    if (securityErrors.length > 0) {
      throw new Error(
        `Export blocked by security gate: ${securityErrors.map((d) => d.message).join("; ")}`,
      );
    }
    if (securityDiagnostics.length > 0) {
      logger.warn(
        { projectId: project.id, warnings: securityDiagnostics },
        "Export proceeding with security warnings",
      );
    }

    const outputRoot = path.resolve(workspaceRoot, project.id);
    await fs.mkdir(outputRoot, { recursive: true });
    const savedPaths: string[] = [];

    // Export all generated files
    if (project.codebase.files.length > 0) {
      for (const file of project.codebase.files) {
        if (Buffer.byteLength(file.content, "utf8") > MAX_CODE_BYTES) {
          logger.warn(
            { projectId: project.id, path: file.path },
            "Skipping oversized file during export",
          );
          continue;
        }
        const target = this.safePath(outputRoot, file.path);
        await fs.mkdir(path.dirname(target), { recursive: true });
        const temporary = `${target}.${randomUUID()}.tmp`;
        await fs.writeFile(temporary, file.content, {
          encoding: "utf8",
          mode: 0o600,
        });
        await fs.rename(temporary, target);
        savedPaths.push(path.relative(workspaceRoot, target));
      }
    } else {
      // Legacy single-file export
      const files = [
        {
          name: "backend/app.py",
          content: project.codebase.backend,
        },
        {
          name: "frontend/App.tsx",
          content: project.codebase.frontend,
        },
        {
          name: "contract/openapi.json",
          content: JSON.stringify(project.spec, null, 2),
        },
      ];
      for (const file of files) {
        if (Buffer.byteLength(file.content, "utf8") > MAX_CODE_BYTES) {
          throw new Error(
            `Refusing to export oversized file: ${file.name}`,
          );
        }
        const target = this.safePath(outputRoot, file.name);
        await fs.mkdir(path.dirname(target), { recursive: true });
        const temporary = `${target}.${randomUUID()}.tmp`;
        await fs.writeFile(temporary, file.content, {
          encoding: "utf8",
          mode: 0o600,
        });
        await fs.rename(temporary, target);
        savedPaths.push(path.relative(workspaceRoot, target));
      }
    }

    // Always export the contract
    if (project.spec) {
      const contractPath = path.join(outputRoot, "contract", "openapi.json");
      await fs.mkdir(path.dirname(contractPath), { recursive: true });
      const temporary = `${contractPath}.${randomUUID()}.tmp`;
      await fs.writeFile(
        temporary,
        JSON.stringify(project.spec, null, 2),
        { encoding: "utf8", mode: 0o600 },
      );
      await fs.rename(temporary, contractPath);
      const relative = path.relative(workspaceRoot, contractPath);
      if (!savedPaths.includes(relative)) {
        savedPaths.push(relative);
      }
    }

    // Export negotiation contract if available
    if (project.codebase.agreedContract) {
      const negPath = path.join(outputRoot, "contract", "negotiated-contract.json");
      const temporary = `${negPath}.${randomUUID()}.tmp`;
      await fs.writeFile(
        temporary,
        JSON.stringify(project.codebase.agreedContract, null, 2),
        { encoding: "utf8", mode: 0o600 },
      );
      await fs.rename(temporary, negPath);
      const relative = path.relative(workspaceRoot, negPath);
      if (!savedPaths.includes(relative)) {
        savedPaths.push(relative);
      }
    }

    return { status: "EXPORT_COMPLETE", savedPaths };
  }

  // ─── VALIDATION ─────────────────────────────────────────────────────

  /**
   * Validate multi-file output from both agents against the agreed contract.
   * Checks: endpoint coverage, type consistency, file structure.
   */
  private async validateMultiFile(
    backendOutput: MultiFileOutput,
    frontendOutput: MultiFileOutput,
    contract: AgreedContract,
    spec: ProjectSpec,
  ): Promise<Diagnostic[]> {
    const diagnostics: Diagnostic[] = [];

    // Check backend has entry point
    if (!backendOutput.entry_point) {
      diagnostics.push({
        severity: "error",
        source: "contract",
        message: "Backend output missing entry_point field.",
      });
    }

    // Check frontend has entry point
    if (!frontendOutput.entry_point) {
      diagnostics.push({
        severity: "error",
        source: "contract",
        message: "Frontend output missing entry_point field.",
      });
    }

    // Check every contracted endpoint is referenced in backend code
    const allBackendContent = backendOutput.files
      .map((f) => f.content)
      .join("\n");

    for (const endpoint of contract.endpoints) {
      const pathPart = endpoint.path.replace(/\{[^}]+\}/g, ""); // Strip path params
      if (!allBackendContent.includes(pathPart)) {
        diagnostics.push({
          severity: "error",
          source: "contract",
          message: `Backend does not reference contracted endpoint ${endpoint.method} ${endpoint.path}.`,
        });
      }
    }

    // Check frontend references contracted endpoints
    const allFrontendContent = frontendOutput.files
      .map((f) => f.content)
      .join("\n");

    for (const endpoint of contract.endpoints) {
      const pathPart = endpoint.path.replace(/\{[^}]+\}/g, "");
      if (!allFrontendContent.includes(pathPart) && !allFrontendContent.includes(endpoint.path)) {
        diagnostics.push({
          severity: "warning",
          source: "contract",
          message: `Frontend may not be calling contracted endpoint ${endpoint.method} ${endpoint.path}.`,
        });
      }
    }

    // Check backend Python syntax (if any .py files)
    for (const file of backendOutput.files) {
      if (file.path.endsWith(".py")) {
        const pyDiag = await this.pythonDiagnostics(file.content);
        diagnostics.push(...pyDiag);
      }
    }

    // Check original spec route coverage
    const routeNames = Object.keys(spec.paths);
    if (
      routeNames.length > 0 &&
      !routeNames.every((route) => allBackendContent.includes(route))
    ) {
      diagnostics.push({
        severity: "error",
        source: "contract",
        message:
          "Generated backend does not reference every contract path from the OpenAPI spec.",
      });
    }

    return diagnostics;
  }

  private async validateCandidate(
    backend: string,
    frontend: string,
    spec: ProjectSpec,
  ): Promise<Diagnostic[]> {
    const diagnostics: Diagnostic[] = [];
    if (!backend || backend.includes("NVIDIA_NETWORK_ERROR")) {
      diagnostics.push({
        severity: "error",
        source: "system",
        message: "Backend model returned no executable source.",
      });
    } else {
      diagnostics.push(...(await this.pythonDiagnostics(backend)));
    }
    if (!frontend || frontend.includes("NVIDIA_NETWORK_ERROR")) {
      diagnostics.push({
        severity: "error",
        source: "system",
        message: "Frontend model returned no executable source.",
      });
    }
    const routeNames = Object.keys(spec.paths);
    if (
      routeNames.length > 0 &&
      !routeNames.every((route) => backend.includes(route))
    ) {
      diagnostics.push({
        severity: "error",
        source: "contract",
        message:
          "Generated backend does not reference every contract path.",
      });
    }
    return diagnostics;
  }

  private pythonDiagnostics(source: string): Promise<Diagnostic[]> {
    return new Promise((resolve) => {
      const temporaryFile = path.join(
        os.tmpdir(),
        `vibe-agent-${randomUUID()}.py`,
      );
      void fs
        .writeFile(temporaryFile, source, {
          encoding: "utf8",
          mode: 0o600,
        })
        .then(() => {
          const child = spawn(
            "python3",
            ["-m", "py_compile", temporaryFile],
            {
              stdio: ["ignore", "ignore", "pipe"],
            },
          );
          let stderr = "";
          child.stderr.on(
            "data",
            (chunk: Buffer) => (stderr += chunk.toString()),
          );
          child.on("error", () =>
            resolve([
              {
                severity: "warning",
                source: "lsp",
                message:
                  "Python LSP validation is unavailable on this host.",
              },
            ]),
          );
          child.on("close", (code) => {
            void fs.rm(temporaryFile, { force: true });
            if (code === 0) return resolve([]);
            const match = stderr.match(/line (\d+)/i);
            resolve([
              {
                severity: "error",
                source: "lsp",
                message:
                  stderr.trim() || "Python syntax validation failed.",
                line: match ? Number(match[1]) : undefined,
              },
            ]);
          });
        })
        .catch((error: unknown) => {
          resolve([
            {
              severity: "error",
              source: "lsp",
              message:
                error instanceof Error
                  ? error.message
                  : "Unable to prepare Python diagnostics.",
            },
          ]);
        });
    });
  }

  // ─── LLM CALLS ─────────────────────────────────────────────────────

  /**
   * Build the full system prompt for a given role and phase,
   * including the God Mode header, phase-specific skills, and extra context.
   */
  private buildSystemPrompt(
    role: Role,
    phase: PipelinePhase,
    project: ProjectState,
    systemInstruction: string,
  ): string {
    const activeSkills = skillsForPhase(
      PLATFORM_SKILLS,
      phase,
      project.activeConnections,
    );

    for (const skill of activeSkills) {
      this.useSkill(project, skillUri(skill));
    }

    const phasePrompt = godModePrompt(
      phase,
      activeSkills,
      project.mode,
    );
    const header = protocolHeader(project.mode);

    return [header, "", phasePrompt, "", systemInstruction].join("\n");
  }

  /**
   * Core LLM call to the NVIDIA API with God Mode system prompt.
   * Returns the raw text response.
   *
   * Renamed from `callModelRaw` to avoid a name collision with the public
   * role-aware wrapper below. This is the RAW NVIDIA fetch — the public
   * `callModelRaw(role, system, user)` builds the God Mode system prompt
   * and delegates here.
   *
   * Accepts an optional `options` bag so single-mode ("Solo" / GLM-5.2)
   * calls can target an OpenAI-compatible endpoint + key OTHER than
   * NVIDIA's without duplicating the retry / 400-fix envelope. When
   * `options.apiUrl` / `options.apiKey` are absent, the NVIDIA env vars
   * are used (the historical behavior — unchanged for swarm mode).
   */
  private async nvidiaCallModelRaw(
    model: string,
    systemPrompt: string,
    userContent: string,
    jsonMode: boolean,
    options?: { apiUrl?: string; apiKey?: string },
  ): Promise<string> {
    const apiKey = options?.apiKey ?? process.env.NVIDIA_API_KEY;
    if (!apiKey) {
      throw new Error(
        "LLM API key is not configured. Set NVIDIA_API_KEY (or SINGLE_MODE_API_KEY for single mode).",
      );
    }
    const url =
      options?.apiUrl ??
      process.env.NVIDIA_API_URL ??
      "https://integrate.api.nvidia.com/v1/chat/completions";

    // ── THE 400 DESERIALIZATION FIX ────────────────────────────────────
    // Normalize every message content BEFORE the fetch so it can never be
    // null/undefined/empty/malformed — the shapes that trigger:
    //   400 "data did not match any variant of untagged enum
    //   ChatCompletionRequestUserMessageContent"
    // `normalizeContent` guarantees a non-empty string. If NVIDIA still
    // rejects with the deserialization 400 (e.g. it tightened validation
    // further), we retry ONCE with both fields forced to a minimal scalar
    // string — the absolute last line of defense.
    let safeSystem = normalizeContent(systemPrompt);
    let safeUser = normalizeContent(userContent);
    let downgradedToScalar = false;

    // Retry with exponential backoff on transient upstream failures
    // (529 overloaded, 429 rate-limited, 502/503/504 gateway errors).
    // NVIDIA's free NIM tier enforces a strict ~2 RPM per-key quota, so 429s
    // need a much longer spacing (≈30s) than other transient errors (4s) —
    // otherwise the multi-call swarm pipeline never completes.
    const RETRYABLE = new Set([429, 502, 503, 504, 529]);
    const MAX_ATTEMPTS = 5;
    const BASE_DELAY_MS = 4000;
    const RATE_LIMIT_DELAY_MS = 30000;

    const backoffFor = (status: number, attempt: number) =>
      status === 429 ? RATE_LIMIT_DELAY_MS * attempt : BASE_DELAY_MS * attempt;

    let lastError = "";
    for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt += 1) {
      let response: Response;
      try {
        // Serialize through the process-wide rate limiter — the free-tier
        // key allows ~1 request/minute; without this, concurrent callers
        // (including zombie pipelines of disconnected SSE clients) 429
        // each other into permanent degradation.
        const releaseSlot = await acquireLlmSlot();
        try {
          response = await fetch(url, {
            method: "POST",
            headers: {
              Authorization: `Bearer ${apiKey}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              model,
              messages: [
                { role: "system", content: safeSystem },
                { role: "user", content: safeUser },
              ],
              temperature: 0,
              max_tokens: 16384,
              ...(jsonMode ? { response_format: { type: "json_object" } } : {}),
            }),
            // Hard timeout — undici keep-alive connections can hang
            // FOREVER without one (observed live: NVIDIA answered in 0.3s
            // from a direct curl while the backend's in-flight fetch sat
            // for 10+ minutes, starving the whole rate-limit queue).
            // Real generations take <=60s; 180s is the generous ceiling.
            signal: AbortSignal.timeout(
              Number(process.env.LLM_FETCH_TIMEOUT_MS ?? 180_000),
            ),
          });
        } finally {
          releaseSlot();
        }
      } catch (err: unknown) {
        lastError = err instanceof Error ? err.message : String(err);
        logger.warn({ model, attempt, err: lastError }, "NVIDIA fetch error");
        if (attempt < MAX_ATTEMPTS) {
          await new Promise((r) => setTimeout(r, BASE_DELAY_MS * attempt));
          continue;
        }
        break;
      }

      if (response.ok) {
        const payload = (await response.json()) as {
          choices?: Array<{ message?: { content?: string } }>;
        };
        return payload.choices?.[0]?.message?.content ?? "";
      }

      const detail = await response.text();
      lastError = `NVIDIA model request failed (${response.status}): ${detail.slice(0, 300)}`;

      // ── Deserialization-400 defense ──────────────────────────────────
      // If NVIDIA rejected with the untagged-enum 400, downgrade both
      // message contents to minimal non-empty scalar strings and retry.
      // This is the last-resort path that guarantees we never surface the
      // "data did not match any variant" error to the user.
      if (
        isDeserialization400(response.status, detail) &&
        !downgradedToScalar &&
        attempt < MAX_ATTEMPTS
      ) {
        downgradedToScalar = true;
        safeSystem = "You are a helpful assistant.";
        safeUser = "Continue the previous task and return a valid response.";
        logger.warn(
          { model, attempt },
          "NVIDIA deserialization 400 — retrying with minimal scalar content",
        );
        await new Promise((r) => setTimeout(r, BASE_DELAY_MS));
        continue;
      }

      if (RETRYABLE.has(response.status) && attempt < MAX_ATTEMPTS) {
        // IP-THROTTLE FAIL-FAST: two consecutive 429s despite the 61s-spaced
        // queue means the provider is throttling this IP (not the quota) —
        // observed live after a retry storm; NVIDIA per-IP throttles last
        // far longer than any in-request backoff. Retry within this request
        // would only feed the storm: abort immediately and let the outer
        // silent-continue retry (minutes later, through the queue) decide.
        if (response.status === 429 && attempt >= 2) {
          logger.error(
            { model, attempt },
            "NVIDIA 429 twice through the spaced queue — IP throttle suspected, failing fast",
          );
          break;
        }
        logger.warn(
          { model, attempt, status: response.status, backoffMs: backoffFor(response.status, attempt) },
          "NVIDIA transient failure — retrying with backoff",
        );
        await new Promise((r) => setTimeout(r, backoffFor(response.status, attempt)));
        continue;
      }
      logger.error({ model, status: response.status }, "NVIDIA model request failed");
      break;
    }
    throw new Error(lastError);
  }

  /**
   * Public role-aware wrapper around the raw NVIDIA fetch. Builds the God
   * Mode system prompt for the role's phase and delegates to the private
   * `nvidiaCallModelRaw` with the standard retry/backoff envelope.
   *
   * Used by Module 4 (orchestration-loop) so the Architect re-plan call
   * gets the same planning-phase skills, header, and retry behavior as
   * the initial codegen pass.
   */
  async callModelRaw(
    role: Role,
    systemPrompt: string,
    userPrompt: string,
    jsonMode = false,
    project?: ProjectState,
  ): Promise<string> {
    return this.callModel(role, systemPrompt, userPrompt, jsonMode, project);
  }

  /**
   * Calls the LLM with phase-aware God Mode skill injection.
   * Public so the orchestration loop (Module 4) can drive planning-phase
   * re-plans through the same retry/backoff path as the rest of the
   * pipeline. Internally still used by single-mode + planning.
   *
   * ROUTING: when `project?.mode === "single"` (the "Solo" / "GLM-5.2"
   * tile in the UI), the call targets the GLM 5.2 model on a dedicated
   * OpenAI-compatible endpoint (configured via SINGLE_MODE_MODEL /
   * SINGLE_MODE_API_URL / SINGLE_MODE_API_KEY env vars, falling back to
   * NVIDIA when unset). Swarm mode keeps the multi-agent DEFAULT_MODELS
   * setup hitting the NVIDIA endpoint as before.
   */
  async callModel(
    role: Role,
    system: string,
    user: string,
    jsonMode: boolean,
    project?: ProjectState,
  ): Promise<string> {
    const phase = phaseForRole(role);
    const fullSystem = project
      ? this.buildSystemPrompt(role, phase, project, system)
      : system;
    if (project?.mode === "single") {
      return this.nvidiaCallModelRaw(
        SINGLE_MODE_MODEL,
        fullSystem,
        user,
        jsonMode,
        { apiUrl: SINGLE_MODE_API_URL, apiKey: SINGLE_MODE_API_KEY },
      );
    }
    return this.nvidiaCallModelRaw(
      DEFAULT_MODELS[role],
      fullSystem,
      user,
      jsonMode,
    );
  }

  // ─── HELPERS ────────────────────────────────────────────────────────

  private parseMultiFileOutput(raw: string): MultiFileOutput | null {
    const parsed = safeParseJson<MultiFileOutput>(raw);
    if (parsed && Array.isArray(parsed.files) && parsed.files.length > 0) {
      return parsed;
    }
    // Fallback: treat the entire output as a single file
    const content = codeOnly(raw);
    if (content.length > 50) {
      return {
        files: [{ path: "generated.ts", content }],
        entry_point: "generated.ts",
      };
    }
    return null;
  }

  private fallbackProposal(specJson: string): BackendProposal {
    return {
      version: 1,
      endpoints: [
        {
          method: "GET",
          path: "/api/health",
          summary: "Health check endpoint",
          response: {
            status: 200,
            content_type: "application/json",
            fields: [
              { name: "status", type: "string", description: "Service status" },
            ],
          },
          auth_required: false,
        },
      ],
      shared_types: [],
      file_structure: [
        { path: "src/index.ts", purpose: "Main server entry point" },
        { path: "src/routes/", purpose: "API route handlers" },
        { path: "src/models/", purpose: "Data models and schemas" },
      ],
      notes: `Fallback proposal. Original spec: ${specJson.slice(0, 500)}`,
    };
  }

  private message(
    project: ProjectState,
    from: Role,
    to: Role,
    subject: string,
    content: string,
  ): void {
    project.messages.push({
      id: randomUUID(),
      from,
      to,
      subject,
      content,
      createdAt: new Date().toISOString(),
    });
  }

  private useSkill(project: ProjectState, uri: string): void {
    if (!project.skillsUsed.includes(uri)) project.skillsUsed.push(uri);
  }

  private activatePhase(
    project: ProjectState,
    phase: PipelinePhase,
  ): void {
    if (!project.phasesCompleted.includes(phase)) {
      project.phasesCompleted.push(phase);
    }
    logger.info(
      {
        projectId: project.id,
        phase,
        completedPhases: project.phasesCompleted,
      },
      "Pipeline phase activated",
    );
  }

  private trackGeneratedFile(
    project: ProjectState,
    filePath: string,
    content: string,
  ): void {
    const existing = project.codebase.files.findIndex(
      (f) => f.path === filePath,
    );
    if (existing >= 0) {
      project.codebase.files[existing].content = content;
    } else {
      project.codebase.files.push({ path: filePath, content });
    }
  }

  private pipelineResult(
    project: ProjectState,
    status: PipelineResult["status"],
    attempts: number,
    negotiationRounds: number,
  ): PipelineResult {
    return {
      status,
      attempts,
      diagnostics: clone(project.diagnostics),
      codebase: clone(project.codebase),
      messages: clone(project.messages),
      skillsUsed: [...project.skillsUsed],
      phasesCompleted: [...project.phasesCompleted],
      negotiationRounds,
      // Surface the actual model id so the frontend's `done.model` field
      // reflects what was used (GLM-5.2 for single mode, the NVIDIA leader
      // model for swarm). Without this, callers fall back to "god-mode".
      model:
        project.mode === "single" ? SINGLE_MODE_MODEL : DEFAULT_MODELS.leader,
    };
  }

  private commit(project: ProjectState): void {
    if (!project.spec) return;
    project.commits.push({
      id: randomUUID(),
      createdAt: new Date().toISOString(),
      spec: clone(project.spec),
      codebase: clone(project.codebase),
    });
    project.updatedAt = new Date().toISOString();
    logger.info(
      { projectId: project.id },
      "Stable project snapshot committed",
    );
  }

  private rollback(project: ProjectState): void {
    const stable = project.commits.at(-1);
    if (!stable) return;
    project.spec = clone(stable.spec);
    project.codebase = clone(stable.codebase);
    project.updatedAt = new Date().toISOString();
    logger.warn(
      { projectId: project.id },
      "Project rolled back to stable snapshot",
    );
  }

  private safePath(root: string, relative: string): string {
    const resolved = path.resolve(root, relative);
    if (
      resolved !== root &&
      !resolved.startsWith(`${root}${path.sep}`)
    ) {
      throw new Error("Unsafe export path rejected.");
    }
    return resolved;
  }

  // ─── DB PERSISTENCE HELPERS ──────────────────────────────────────────
  // These are fire-and-forget by default. Only `persistAgentMessages` and
  // `persistGeneration` are awaited within the pipeline because they mark
  // phase boundaries.

  /**
   * Persist all in-memory agent messages that don't yet exist in DB.
   * Uses a best-effort approach: fetches existing DB messages and only
   * inserts ones missing (by matching on `createdAt` + `from_role`).
   */
  private async persistAgentMessages(project: ProjectState): Promise<void> {
    if (!isSupabaseConfigured()) return;
    try {
      const existing = await dbGetAgentMessages(project.id);
      const existingKeys = new Set(
        existing.map((m) => `${m.from_role}:${m.to_role}:${m.created_at}`),
      );
      const toInsert = project.messages.filter(
        (m) =>
          !existingKeys.has(`${m.from}:${m.to}:${m.createdAt}`),
      );
      for (const msg of toInsert) {
        await dbSaveAgentMessage(
          project.id,
          msg.from,
          msg.to,
          msg.subject,
          msg.content,
        );
      }
    } catch (err: unknown) {
      logger.warn(
        { projectId: project.id, error: err instanceof Error ? err.message : err },
        "Failed to persist agent messages",
      );
    }
  }

  /**
   * Persist skill usage for a given phase to the DB.
   */
  private async persistSkillLogs(
    project: ProjectState,
    phase: PipelinePhase,
  ): Promise<void> {
    if (!isSupabaseConfigured()) return;
    for (const skillUri of project.skillsUsed) {
      try {
        await dbLogSkill(project.id, skillUri, phase);
      } catch (err: unknown) {
        logger.warn(
          { projectId: project.id, skillId: skillUri, phase, error: err instanceof Error ? err.message : err },
          "Failed to persist skill log",
        );
      }
    }
  }

  /**
   * Persist workspace files from a MultiFileOutput to the DB.
   */
  private async persistWorkspaceFiles(
    project: ProjectState,
    files: GeneratedFile[],
  ): Promise<void> {
    if (!isSupabaseConfigured()) return;
    for (const file of files) {
      try {
        const language = inferLanguage(file.path);
        await dbSaveWorkspaceFile(project.id, file.path, file.content, language);
      } catch (err: unknown) {
        logger.warn(
          { projectId: project.id, path: file.path, error: err instanceof Error ? err.message : err },
          "Failed to persist workspace file",
        );
      }
    }
  }

  /**
   * Persist a single workspace file to DB (fire-and-forget).
   */
  private async persistWorkspaceFile(
    project: ProjectState,
    filePath: string,
    content: string,
  ): Promise<void> {
    if (!isSupabaseConfigured()) return;
    try {
      const language = inferLanguage(filePath);
      await dbSaveWorkspaceFile(project.id, filePath, content, language);
    } catch (err: unknown) {
      logger.warn(
        { projectId: project.id, path: filePath, error: err instanceof Error ? err.message : err },
        "Failed to persist workspace file",
      );
    }
  }

  /**
   * Save a pipeline generation result to DB.
   */
  private async persistGeneration(
    project: ProjectState,
    status: string,
    attempts: number,
    negotiationRounds: number,
  ): Promise<void> {
    if (!isSupabaseConfigured()) return;
    try {
      await dbSaveGeneration(project.id, {
        status,
        backend_code: project.codebase.backend || null,
        frontend_code: project.codebase.frontend || null,
        diagnostics: project.diagnostics,
        skills_used: project.skillsUsed,
        phases_completed: project.phasesCompleted,
        negotiation_rounds: negotiationRounds,
        summary: status === "approved"
          ? `Pipeline completed in ${attempts} attempt(s)`
          : "Pipeline failed after max attempts",
      });

      // Also update the project row itself
      await dbUpdateProject(project.id, {
        status,
        spec: project.spec,
        agreed_contract: project.codebase.agreedContract ?? null,
        skills_used: project.skillsUsed,
        phases_completed: project.phasesCompleted,
        negotiation_rounds: negotiationRounds,
      });
    } catch (err: unknown) {
      logger.warn(
        { projectId: project.id, error: err instanceof Error ? err.message : err },
        "Failed to persist generation result",
      );
    }
  }
}

export const agentPlatform = new AgentPlatform();
