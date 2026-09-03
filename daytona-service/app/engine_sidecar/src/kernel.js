/**
 * Forgvi Engine — kernel wrapper.
 *
 * The kernel is prime-agent (PrimeIntellect-ai/prime-agent v0.8.1, forked
 * as the foundation of Forgvi 2.0). This module owns:
 *   - the provider registry (models.json, OpenAI-compatible providers —
 *     NVIDIA NIM by default; aihubmix + aihubmix-alt available via env)
 *   - chief sessions (bash + edit tools)
 *   - judge sessions (no tools — the independent verifier)
 *
 * Provider selection is env-driven so switching providers never needs a
 * code change or redeploy:
 *   ENGINE_PROVIDER      "nvidia" (default) | "aihubmix" | "aihubmix-alt"
 *                         | "vm-tunnel" (the in-VM mode — see ENGINE_IN_VM)
 *   ENGINE_MODEL         override (defaults to the provider default below)
 *   ENGINE_API_KEY       generic override (wins over the provider's own env)
 *   NVIDIA_API_KEY       key for the nvidia provider
 *   AIHUBMIX_API_KEY     key for the aihubmix / aihubmix-alt providers
 *   AIHUBMIX_BASE_URL    optional: swap the aihubmix baseUrl at runtime
 *
 * aihubmix-alt points at the preferred mirror (api.inferera.com) for when
 * the default aihubmix.com route is unreachable from the host network.
 *
 * IN-VM MODE (ENGINE_IN_VM=1) — "the engine lives inside the Daytona
 * sandbox", exactly like Forgvi 1.0's orchestrator:
 *   - The engine process runs inside the project's VM (PM2-supervised by
 *     the sidecar installer, port 8799, journal on the VM's disk).
 *   - The chief's bash/edit tools execute LOCALLY inside the VM, rooted
 *     at ENGINE_VM_WORKSPACE_ROOT (/workspace) — no REST round-trips, no
 *     engine-host disk, the VM IS the workspace by construction.
 *   - LLM calls go to the "vm-tunnel" provider: the 1.0 orchestrator
 *     daemon (localhost:9000) proxies them through its /reverse-tunnel
 *     WebSocket to the ArcForge backend, which injects the NVIDIA key
 *     server-side. The key NEVER enters the VM (the EU egress filter
 *     blocks *.nvidia.com anyway — the tunnel is the only way out).
 *   - The workspace grant is not required: the engine can only be reached
 *     through the SIGNED Daytona preview URL the backend brokers to the
 *     project's owner, and it can only affect its own VM.
 *
 * Chief tool execution has two other backends, chosen per run:
 *   - VM-bound via REST (the host-engine default in production): the run
 *     carries a verified workspace grant, and bash/edit operate on the
 *     project's Daytona sandbox through the daytona-service — the SAME
 *     /workspace filesystem Forgvi 1.0's in-VM swarm uses.
 *   - local fallback (dev / unbound runs): a per-run directory on disk.
 *
 * The rest of the engine NEVER talks to an LLM directly; everything goes
 * through the kernel. One owner per subsystem.
 */

import { mkdirSync } from "node:fs";
import { resolve } from "node:path";
import {
  AuthStorage,
  createAgentSession,
  createBashTool,
  createEditTool,
  ModelRegistry,
  SessionManager,
  SettingsManager,
} from "prime-agent";
import { createVmWorkspace } from "./vm-operations.js";
import {
  createAskUserTool,
  createGithubTool,
  createRequestConnectorTool,
  createSupabaseTool,
} from "./connector-tools.js";
import { buildChiefTools } from "./tools.js";

/**
 * The chief's tool allowlist — bash + edit (the core workspace tools) plus
 * the Vube surface: ask_user, connector tools, orchestrate, scaffold_vube,
 * list_nodes. Without an allowlist prime-agent would also activate its
 * built-in ipython tool, which would run a LOCAL python kernel and silently
 * diverge from the VM workspace (the in-VM branch deliberately omits the
 * allowlist — there the engine lives inside the VM and prime-agent's own
 * tool defaults are acceptable).
 */
const CHIEF_TOOLS = [
  "bash",
  "edit",
  "ask_user",
  "github",
  "supabase",
  "request_connector",
  "orchestrate",
  "scaffold_vube",
  "list_nodes",
];

/** True when the engine process itself runs INSIDE a Daytona sandbox. */
export const ENGINE_IN_VM = process.env.ENGINE_IN_VM === "1";

/** Root the chief's tools at when running inside the VM. */
const VM_WORKSPACE_ROOT = process.env.ENGINE_VM_WORKSPACE_ROOT ?? "/workspace";

const ENGINE_DIR = resolve(new URL("..", import.meta.url).pathname);
const MODELS_JSON = resolve(ENGINE_DIR, ".prime-agent", "models.json");
const WORKSPACE_ROOT = process.env.ENGINE_WORKSPACE_ROOT
  ? resolve(process.env.ENGINE_WORKSPACE_ROOT)
  : resolve(ENGINE_DIR, "workspace");

/**
 * Provider table — one row per provider defined in .prime-agent/models.json.
 *   keyEnv:      the env var the provider's API key lives in
 *   defaultModel: used when ENGINE_MODEL is unset
 *
 * 2026-09-04: the default is Nemotron 3 ULTRA 550B (user mandate — the
 * weaker super-120b shipped a syntax error + an SSR-unsafe localStorage
 * read that a human had to fix). Ultra verified live on the NVIDIA NIM
 * catalog with correct OpenAI tool-calling (prime-agent compatible);
 * ENGINE_MODEL="nvidia/nemotron-3-super-120b-a12b" still pins the old one.
 */
const PROVIDERS = {
  nvidia: { keyEnv: "NVIDIA_API_KEY", defaultModel: "nvidia/nemotron-3-ultra-550b-a55b" },
  aihubmix: { keyEnv: "AIHUBMIX_API_KEY", defaultModel: "coding-glm-5.3" },
  "aihubmix-alt": { keyEnv: "AIHUBMIX_API_KEY", defaultModel: "coding-glm-5.3" },
  // The in-VM provider: the 1.0 orchestrator daemon (localhost:9000)
  // proxies OpenAI-format requests through its reverse tunnel. The "key"
  // is the VM's own ORCH_TOKEN (a per-VM secret the proxy validates on
  // localhost) — never a provider key, which stays on the backend.
  "vm-tunnel": { keyEnv: "ENGINE_LLM_TOKEN", defaultModel: "nvidia/nemotron-3-ultra-550b-a55b" },
};

const ENGINE_PROVIDER = (process.env.ENGINE_PROVIDER ?? "nvidia").toLowerCase();
const PROVIDER = PROVIDERS[ENGINE_PROVIDER] ?? PROVIDERS.nvidia;
const MODEL_ID = process.env.ENGINE_MODEL ?? PROVIDER.defaultModel;

/** The env var a operator must set for the active provider (health msgs). */
export function requiredKeyEnv() {
  return PROVIDER.keyEnv;
}

let _auth = null;
let _registry = null;
let _model = null;

/** Lazy singletons for auth + model registry + resolved model. */
function getModel() {
  if (_model) return _model;
  const apiKey = process.env.ENGINE_API_KEY ?? process.env[PROVIDER.keyEnv];
  if (!apiKey) {
    throw new Error(
      `${PROVIDER.keyEnv} is not set — the engine cannot reach its ${ENGINE_PROVIDER} model provider`
    );
  }
  _auth = AuthStorage.create(resolve(ENGINE_DIR, ".prime-agent", "auth.json"));
  _auth.setRuntimeApiKey(ENGINE_PROVIDER, apiKey);
  _registry = ModelRegistry.create(_auth, MODELS_JSON);
  _model = _registry.find(ENGINE_PROVIDER, MODEL_ID);
  if (!_model) {
    throw new Error(
      `model "${MODEL_ID}" is not defined for provider "${ENGINE_PROVIDER}" in .prime-agent/models.json`
    );
  }
  return _model;
}

/** True when the kernel can resolve its model + key (health check). */
export function kernelReady() {
  try {
    const model = getModel();
    return Boolean(model && model.id);
  } catch {
    return false;
  }
}

/** Provider/model identity for /health and reports. */
export function kernelModelId() {
  try {
    const model = getModel();
    return `${model.provider}/${model.id}`;
  } catch {
    return null;
  }
}

/**
 * Chief session — the primary agent of a run.
 *
 * Three backends, chosen by mode:
 *
 *  1. IN-VM (ENGINE_IN_VM=1): the engine process runs inside the project's
 *     Daytona sandbox, so the chief's bash + edit tools execute NATIVELY in
 *     the VM, rooted at /workspace — the same filesystem Forgvi 1.0, the
 *     studio's Files/Preview tabs, and the terminal all use. No REST
 *     round-trips, nothing on any engine host. The tool allowlist
 *     ("bash" + "edit") keeps prime-agent's local ipython tool off.
 *
 *  2. VM-bound via REST (host engine + verified workspace grant): bash/edit
 *     operate on the project's Daytona sandbox through the daytona-service.
 *
 *  3. local fallback (dev / unbound host runs): a per-run directory on disk.
 *
 * The session persists across iterations (the chief keeps its context); the
 * goal loop drives it turn by turn.
 */
export async function createChiefSession({ runId, sessionId, workspace, interactions, runCtx = {} }) {
  const model = getModel();

  if (ENGINE_IN_VM) {
    mkdirSync(VM_WORKSPACE_ROOT, { recursive: true });
    // The Vube tools read ctx.vm at CALL time — the in-VM engine has no REST
    // VM client; the workspace IS this process's filesystem.
    runCtx.localCwd = VM_WORKSPACE_ROOT;
    const vube = buildChiefTools(runCtx);
    const { session } = await createAgentSession({
      model,
      thinkingLevel: "off",
      authStorage: _auth,
      modelRegistry: _registry,
      cwd: VM_WORKSPACE_ROOT,
      sessionManager: SessionManager.inMemory(),
      settingsManager: SettingsManager.inMemory({
        compaction: { enabled: false },
        enableBuiltinSkills: false,
      }),
      customTools: [
        createBashTool(VM_WORKSPACE_ROOT),
        createEditTool(VM_WORKSPACE_ROOT),
        // GROUP 2 parity: the connector + interaction tools 1.0's swarm has.
        // In-VM, the github/supabase/request_connector bridge is the 1.0
        // orchestrator daemon on localhost (the tunnel keeps every token
        // server-side); ask_user is engine-native (journal + answer route).
        ...(interactions ? [createAskUserTool(interactions)] : []),
        createGithubTool(),
        createSupabaseTool(),
        createRequestConnectorTool(),
        // The Vube surface: dynamic orchestration + monorepo scaffold +
        // registry discovery.
        vube.orchestrate,
        vube.scaffoldVube,
        vube.listNodes,
      ],
      sessionStartEvent: {
        type: "session_start",
        sessionId,
        source: "forgvi-engine-vm",
        objective: undefined,
      },
    });
    return { session, cwd: VM_WORKSPACE_ROOT, vm: null, inVm: true };
  }

  if (workspace?.sandboxId) {
    const vm = createVmWorkspace(workspace);
    // The chief's tools read ctx.vm at CALL time — assign once resolved.
    runCtx.vm = vm;
    runCtx.localCwd = null;
    const vube = buildChiefTools(runCtx);
    const { session } = await createAgentSession({
      model,
      thinkingLevel: "off",
      authStorage: _auth,
      modelRegistry: _registry,
      cwd: vm.workspaceRoot,
      sessionManager: SessionManager.inMemory(),
      settingsManager: SettingsManager.inMemory({
        compaction: { enabled: false },
        enableBuiltinSkills: false,
      }),
      customTools: [
        createBashTool(vm.workspaceRoot, { operations: vm.bashOperations }),
        createEditTool(vm.workspaceRoot, { operations: vm.editOperations }),
        // ask_user (engine-native) + request_connector (honest on host).
        ...(interactions ? [createAskUserTool(interactions)] : []),
        createRequestConnectorTool(),
        // Host mode: github + supabase run DIRECT against the engine's own
        // tokens (GITHUB_TOKEN / SUPABASE_*) — functional today; the
        // tunnel-bridged per-user versions live in the in-VM branch.
        vube.github,
        vube.supabase,
        // The Vube surface.
        vube.orchestrate,
        vube.scaffoldVube,
        vube.listNodes,
      ],
      tools: CHIEF_TOOLS,
      sessionStartEvent: {
        type: "session_start",
        sessionId,
        source: "forgvi-engine",
        objective: undefined,
      },
    });
    return { session, cwd: vm.workspaceRoot, vm };
  }

  const cwd = resolve(WORKSPACE_ROOT, runId);
  mkdirSync(cwd, { recursive: true });
  runCtx.localCwd = cwd;
  const vube = buildChiefTools(runCtx);
  const { session } = await createAgentSession({
    model,
    thinkingLevel: "off",
    authStorage: _auth,
    modelRegistry: _registry,
    cwd,
    sessionManager: SessionManager.inMemory(),
    settingsManager: SettingsManager.inMemory({
      compaction: { enabled: false },
      enableBuiltinSkills: false,
    }),
    customTools: [
      createBashTool(cwd),
      createEditTool(cwd),
      // Unbound local runs still get ask_user (engine-native); the
      // connector tools report unavailability honestly.
      ...(interactions ? [createAskUserTool(interactions)] : []),
      createRequestConnectorTool(),
      vube.github,
      vube.supabase,
      vube.orchestrate,
      vube.scaffoldVube,
      vube.listNodes,
    ],
    tools: CHIEF_TOOLS,
    sessionStartEvent: {
      type: "session_start",
      sessionId,
      source: "forgvi-engine",
      objective: undefined,
    },
  });
  return { session, cwd, vm: null };
}

/**
 * Specialist (rlm-style subagent) session — the advisor the chief spawns
 * through orchestrate. Tool-less by default: specialists return specs,
 * code, and checklists; the CHIEF applies them so every artifact carries
 * tool evidence the judge can verify (and concurrent specialists never
 * race the shared workspace). The persona system prompt is composed into
 * the first prompt by orchestrator-utils (spawnSpecialistAgent).
 *
 * DEBUGGER TERMINAL ACCESS (user mandate 2026-09-04): a specialist with
 * `tools: ["bash"]` — the QA Verifier / debugger — gets a REAL shell in
 * the run's workspace so it can run its own probes (npm run build,
 * tsc --noEmit, curl the dev server, grep for banned patterns) instead
 * of auditing secondhand state. `workspace` mirrors the chief's backend:
 *   - in-VM: native bash rooted at the VM workspace root
 *   - host VM-bound: bash through the daytona-service REST operations
 *   - local: bash rooted at the run's local directory
 */
export async function createSpecialistSession({ tools, workspace } = {}) {
  const model = getModel();
  const wantsBash = Array.isArray(tools) && tools.includes("bash");
  const customTools = [];
  if (wantsBash) {
    if (workspace?.vm) {
      customTools.push(createBashTool(workspace.vm.workspaceRoot, { operations: workspace.vm.bashOperations }));
    } else {
      const root = workspace?.localCwd ?? ENGINE_DIR;
      try {
        mkdirSync(root, { recursive: true });
      } catch {
        /* exists */
      }
      customTools.push(createBashTool(root));
    }
  }
  const { session } = await createAgentSession({
    model,
    thinkingLevel: "off",
    authStorage: _auth,
    modelRegistry: _registry,
    cwd: workspace?.vm ? workspace.vm.workspaceRoot : (workspace?.localCwd ?? ENGINE_DIR),
    sessionManager: SessionManager.inMemory(),
    settingsManager: SettingsManager.inMemory({
      compaction: { enabled: false },
      enableBuiltinSkills: false,
    }),
    ...(customTools.length > 0 ? { customTools, tools: ["bash"] } : { noTools: "all" }),
    sessionStartEvent: {
      type: "session_start",
      sessionId: `specialist-${Date.now()}`,
      source: "forgvi-engine-specialist",
      objective: undefined,
    },
  });
  return { session, lastAssistantText: () => lastAssistantText(session) };
}

/**
 * Judge session — the independent verifier. No tools, no workspace, no
 * memory of the chief's reasoning: it sees only the objective, the
 * acceptance criteria, the chief's report, and the evidence. Fresh
 * instance per verification turn so verdicts never anchor on the run's
 * own conversation.
 */
export async function createJudgeSession() {
  const model = getModel();
  const { session } = await createAgentSession({
    model,
    thinkingLevel: "off",
    authStorage: _auth,
    modelRegistry: _registry,
    cwd: ENGINE_DIR,
    sessionManager: SessionManager.inMemory(),
    settingsManager: SettingsManager.inMemory({
      compaction: { enabled: false },
      enableBuiltinSkills: false,
    }),
    noTools: "all",
  });
  return session;
}

/** Extract the last assistant text from a session's messages. */
export function lastAssistantText(session) {
  const last = [...session.messages].reverse().find((m) => m.role === "assistant");
  return (last?.content ?? [])
    .filter((c) => c.type === "text")
    .map((c) => c.text)
    .join("\n")
    .trim();
}
