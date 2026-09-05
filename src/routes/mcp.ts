import { Router, type IRouter } from "express";
import {
  PLATFORM_SKILLS,
  skillUri,
  skillsByDepartment,
} from "../services/skill-registry";
import { DEPARTMENTS, type Department } from "../services/god-mode-protocol";
import { agentPlatform } from "../services/agent-platform";
import { execFile } from "node:child_process";
import { promisify } from "node:util";
import { logger } from "../lib/logger";
import { requireAuth } from "../middleware/auth";

const execFileAsync = promisify(execFile);
const router: IRouter = Router();

// JWT auth at the router level — these routes were previously fully public.
// Path-scoped auth (see connectors.ts note): this router mounts
// unprefixed, so gate only its own route family.
router.use("/mcp", requireAuth);

// ─── RESOURCES ────────────────────────────────────────────────────────

const resources = [
  {
    uri: "rules://architecture/anti-spaghetti",
    name: "Anti-spaghetti architecture rules",
    description: "Mandatory contract-first and bounded-repair rules.",
    text: "Treat the canonical OpenAPI contract as the single source of truth. Never add uncontracted routes. Validate generated code before export. After two consecutive failures, rollback and choose a different strategy.",
  },
  {
    uri: "rules://architecture/god-mode",
    name: "God Mode protocol rules",
    description: "Phase-aware mandatory tool usage chain with inter-agent negotiation.",
    text: "PROTOCOL: GOD_MODE_ACTIVE. Every pipeline phase activates only its relevant department skills. Planning -> Executive. Contract Negotiation -> Backend + Frontend (they TALK to each other). Backend -> Engineering (multi-file). Frontend -> Design (multi-file). QA -> Design + Engineering. Security -> Security + Speed. Agents negotiate API contract before coding. Code generation is BLOCKED until both agents agree.",
  },
  {
    uri: "rules://architecture/negotiation",
    name: "Inter-agent negotiation protocol",
    description: "Rules for frontend-backend contract negotiation.",
    text: "The backend proposes exact endpoints, field names, types, and auth schemes. The frontend reviews and requests changes. This loops for up to 3 rounds. Code generation is BLOCKED until both agents approve the contract. The agreed contract is the source of truth for both sides.",
  },
  {
    uri: "design://core-principles",
    name: "Core design principles",
    description: "UI guidance injected on demand.",
    text: "Prefer accessible, responsive interfaces with deliberate color, strong hierarchy, useful empty states, and feedback for every user action. Do not invent product workflows outside the contract.",
  },
];

const skillResources = PLATFORM_SKILLS.map((skill) => ({
  uri: skillUri(skill),
  name: skill.name,
  description: skill.description,
  text: `${skill.name} (${skill.source})\n\n${skill.instruction}`,
  role: skill.role,
  department: skill.department,
  phases: skill.phases,
  requiresConnection: skill.requiresConnection,
}));

// ─── TOOLS ────────────────────────────────────────────────────────────

const tools = [
  // Resource tools
  {
    name: "search_resources",
    description: "Search hosted architecture and design resources.",
    inputSchema: {
      type: "object",
      properties: { query: { type: "string" } },
      required: ["query"],
    },
  },
  {
    name: "get_resource",
    description: "Read a hosted resource by URI.",
    inputSchema: {
      type: "object",
      properties: { uri: { type: "string" } },
      required: ["uri"],
    },
  },
  // Web & Git tools
  {
    name: "fetch_url",
    description: "Fetch a web page and convert it to Markdown. Essential for reading live documentation.",
    inputSchema: {
      type: "object",
      properties: { url: { type: "string", format: "uri" } },
      required: ["url"],
    },
  },
  {
    name: "git_status",
    description: "Check the current git branch, recent commits, and working tree status.",
    inputSchema: { type: "object", properties: {} },
  },
  {
    name: "git_diff",
    description: "Show the diff of uncommitted changes.",
    inputSchema: {
      type: "object",
      properties: { staged: { type: "boolean", default: false } },
    },
  },
  {
    name: "web_search",
    description: "Search the web for security vulnerabilities, best practices, or documentation.",
    inputSchema: {
      type: "object",
      properties: { query: { type: "string" } },
      required: ["query"],
    },
  },
  {
    name: "list_dependencies",
    description: "List project dependencies with versions for security auditing.",
    inputSchema: { type: "object", properties: {} },
  },
  // Workspace file system tools
  {
    name: "workspace_read_file",
    description: "Read the content of a file in the project workspace.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative file path" },
      },
      required: ["project_id", "path"],
    },
  },
  {
    name: "workspace_create_file",
    description: "Create a new file in the project workspace. Parent directories are auto-created.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative file path" },
        content: { type: "string", description: "File content" },
      },
      required: ["project_id", "path", "content"],
    },
  },
  {
    name: "workspace_edit_file",
    description: "Overwrite an existing file in the workspace. The file must exist.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative file path" },
        content: { type: "string", description: "New file content" },
      },
      required: ["project_id", "path", "content"],
    },
  },
  {
    name: "workspace_delete",
    description: "Delete a file or directory from the workspace.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative path to delete" },
      },
      required: ["project_id", "path"],
    },
  },
  {
    name: "workspace_mkdir",
    description: "Create a directory in the workspace. Parent directories are auto-created.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative directory path" },
      },
      required: ["project_id", "path"],
    },
  },
  {
    name: "workspace_move",
    description: "Move or rename a file or directory to a new path.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        source: { type: "string", description: "Current relative path" },
        destination: { type: "string", description: "New relative path" },
      },
      required: ["project_id", "source", "destination"],
    },
  },
  {
    name: "workspace_copy",
    description: "Copy a file or directory to a new path.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        source: { type: "string", description: "Source relative path" },
        destination: { type: "string", description: "Destination relative path" },
      },
      required: ["project_id", "source", "destination"],
    },
  },
  {
    name: "workspace_list",
    description: "List files and directories in a workspace path.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Directory path (default: root)" },
      },
      required: ["project_id"],
    },
  },
  {
    name: "workspace_tree",
    description: "Get a visual tree representation of the entire workspace.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        max_depth: { type: "number", description: "Max depth (default: 4)" },
      },
      required: ["project_id"],
    },
  },
  {
    name: "workspace_exists",
    description: "Check if a file or directory exists in the workspace.",
    inputSchema: {
      type: "object",
      properties: {
        project_id: { type: "string", description: "The project UUID" },
        path: { type: "string", description: "Relative path to check" },
      },
      required: ["project_id", "path"],
    },
  },
];

// ─── TOOL DISPATCH IMPLEMENTATIONS ───────────────────────────────────

async function dispatchFetchUrl(url: string): Promise<string> {
  try {
    const response = await fetch(url, {
      headers: {
        "User-Agent": "Arcforge-MCP-Fetch/1.0",
        Accept: "text/html,text/markdown,application/json",
      },
      signal: AbortSignal.timeout(15_000),
    });
    if (!response.ok) {
      return `Fetch failed: HTTP ${response.status} ${response.statusText}`;
    }
    const contentType = response.headers.get("content-type") ?? "";
    const body = await response.text();
    const truncated =
      body.length > 50_000
        ? body.slice(0, 50_000) + "\n\n[... truncated at 50KB]"
        : body;
    if (contentType.includes("json")) {
      return `Source: ${url}\nType: application/json\n\n${truncated}`;
    }
    const text = truncated
      .replace(new RegExp("<script[\\s\\S]*?</script>", "gi"), "")
      .replace(new RegExp("<style[\\s\\S]*?</style>", "gi"), "")
      .replace(/<[^>]+>/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    return `Source: ${url}\n\n${text}`;
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Unknown fetch error";
    logger.warn({ url, err: message }, "MCP fetch_url failed");
    return `Fetch error: ${message}`;
  }
}

async function dispatchGitStatus(): Promise<string> {
  try {
    const { stdout: branch } = await execFileAsync("git", [
      "rev-parse",
      "--abbrev-ref",
      "HEAD",
    ]);
    const { stdout: log } = await execFileAsync("git", [
      "log",
      "--oneline",
      "-5",
    ]);
    const { stdout: status } = await execFileAsync("git", [
      "status",
      "--short",
    ]);
    return [
      `Branch: ${branch.trim()}`,
      "",
      "Recent commits:",
      log.trim(),
      "",
      "Working tree:",
      status.trim() || "(clean)",
    ].join("\n");
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Git not available";
    return `Git error: ${message}`;
  }
}

async function dispatchGitDiff(staged: boolean): Promise<string> {
  try {
    const args = staged ? ["diff", "--cached"] : ["diff"];
    const { stdout } = await execFileAsync("git", args);
    const truncated =
      stdout.length > 30_000
        ? stdout.slice(0, 30_000) + "\n\n[... truncated at 30KB]"
        : stdout;
    return truncated.trim() || "(no changes)";
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Git not available";
    return `Git diff error: ${message}`;
  }
}

async function dispatchWebSearch(query: string): Promise<string> {
  const apiKey = process.env.BRAVE_API_KEY;
  if (apiKey) {
    try {
      const url = `https://api.search.brave.com/res/v1/web/search?q=${encodeURIComponent(query)}&count=5`;
      const response = await fetch(url, {
        headers: {
          Accept: "application/json",
          "X-Subscription-Token": apiKey,
        },
        signal: AbortSignal.timeout(10_000),
      });
      if (!response.ok) {
        return `Brave Search API error: HTTP ${response.status}`;
      }
      const data = (await response.json()) as {
        web?: Array<{
          title?: string;
          url?: string;
          description?: string;
        }>;
      };
      const results = data.web?.map(
        (r, i) =>
          `${i + 1}. ${r.title ?? "(no title)"}\n   ${r.url ?? ""}\n   ${r.description ?? ""}`,
      );
      return results?.join("\n\n") ?? "No results found.";
    } catch (error) {
      logger.warn({ err: error }, "Brave Search failed, falling back");
    }
  }
  try {
    const ddgUrl = `https://html.duckduckgo.com/html/?q=${encodeURIComponent(query)}`;
    const response = await fetch(ddgUrl, {
      headers: { "User-Agent": "Arcforge-MCP/1.0" },
      signal: AbortSignal.timeout(10_000),
    });
    const html = await response.text();
    const results: string[] = [];
    const regex = new RegExp(
      'class="result__a"[^>]*>([^<]+)</a>.*?class="result__snippet"[^>]*>([^<]*)',
      "gs",
    );
    let match;
    while ((match = regex.exec(html)) !== null && results.length < 5) {
      results.push(`${results.length + 1}. ${match[1].trim()}\n   ${match[2].trim()}`);
    }
    return (
      results.join("\n\n") ||
      `No results found for: ${query}`
    );
  } catch (error) {
    const message =
      error instanceof Error ? error.message : "Search unavailable";
    return `Search error: ${message}`;
  }
}

async function dispatchListDependencies(): Promise<string> {
  try {
    const { stdout } = await execFileAsync("pnpm", ["list", "--depth", "0"]);
    return stdout.trim();
  } catch {
    try {
      const fs = await import("node:fs/promises");
      const raw = await fs.readFile("package.json", "utf8");
      const pkg = JSON.parse(raw);
      const deps = {
        ...pkg.dependencies,
        ...pkg.devDependencies,
      };
      return Object.entries(deps)
        .map(([name, version]) => `${name}: ${version}`)
        .join("\n");
    } catch {
      return "Could not read dependencies.";
    }
  }
}

// ─── ROUTES ───────────────────────────────────────────────────────────

router.get("/mcp/resources", (_req, res) =>
  res.json({ resources }),
);

router.get("/mcp/skills", (_req, res) => {
  const grouped = skillsByDepartment();
  const departmentList = Object.entries(DEPARTMENTS).map(
    ([key, label]) => ({
      department: key,
      label,
      skills: (grouped[key as Department] ?? []).map((s) => ({
        id: s.id,
        name: s.name,
        description: s.description,
        phases: s.phases,
        requiresConnection: s.requiresConnection ?? null,
      })),
    }),
  );
  res.json({
    totalSkills: PLATFORM_SKILLS.length,
    departments: departmentList,
  });
});

router.get("/mcp/tools", (_req, res) => res.json({ tools }));

router.post("/mcp/call", async (req, res, next) => {
  const name = typeof req.body?.name === "string" ? req.body.name : "";
  const args =
    req.body?.arguments && typeof req.body.arguments === "object"
      ? (req.body.arguments as Record<string, unknown>)
      : {};
  if (!name) {
    res.status(400).json({ error: "name and arguments are required" });
    return;
  }

  try {
    // ── Built-in resource tools ──
    if (name === "get_resource") {
      const uri = String(args.uri ?? "");
      const resource = [...resources, ...skillResources].find(
        (item) => item.uri === uri,
      );
      if (!resource) {
        res.status(404).json({ error: "resource not found" });
        return;
      }
      res.json({ content: [{ type: "text", text: resource.text }] });
      return;
    }

    if (name === "search_resources") {
      const query = String(args.query ?? "").toLowerCase();
      const matches = [...resources, ...skillResources].filter(
        (item) =>
          `${item.name} ${item.description} ${("text" in item ? item.text : "")}`
            .toLowerCase()
            .includes(query),
      );
      res.json({
        content: [
          { type: "text", text: JSON.stringify(matches) },
        ],
      });
      return;
    }

    // ── Web & Git tools ──
    if (name === "fetch_url") {
      const url = String(args.url ?? "");
      if (!url || !/^https?:\/\//.test(url)) {
        res.status(400).json({ error: "a valid http(s) URL is required" });
        return;
      }
      const text = await dispatchFetchUrl(url);
      res.json({ content: [{ type: "text", text }] });
      return;
    }

    if (name === "git_status") {
      const text = await dispatchGitStatus();
      res.json({ content: [{ type: "text", text }] });
      return;
    }

    if (name === "git_diff") {
      const staged = Boolean(args.staged);
      const text = await dispatchGitDiff(staged);
      res.json({ content: [{ type: "text", text }] });
      return;
    }

    if (name === "web_search") {
      const query = String(args.query ?? "");
      if (!query) {
        res.status(400).json({ error: "query is required" });
        return;
      }
      const text = await dispatchWebSearch(query);
      res.json({ content: [{ type: "text", text }] });
      return;
    }

    if (name === "list_dependencies") {
      const text = await dispatchListDependencies();
      res.json({ content: [{ type: "text", text }] });
      return;
    }

    // ── Workspace file system tools ──
    const projectId = String(args.project_id ?? "");
    if (!projectId) {
      res.status(400).json({ error: "project_id is required for workspace operations" });
      return;
    }
    const project = agentPlatform.getProject(projectId);
    if (!project) {
      res.status(404).json({ error: "project not found" });
      return;
    }

    if (name === "workspace_read_file") {
      const filePath = String(args.path ?? "");
      if (!filePath) {
        res.status(400).json({ error: "path is required" });
        return;
      }
      try {
        const content = await agentPlatform.readFile(project, filePath);
        res.json({ content: [{ type: "text", text: content }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Read failed";
        res.status(404).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_create_file") {
      const filePath = String(args.path ?? "");
      const content = String(args.content ?? "");
      if (!filePath || filePath.endsWith("/")) {
        res.status(400).json({ error: "valid file path is required" });
        return;
      }
      try {
        const savedPath = await agentPlatform.createFile(project, filePath, content);
        res.json({ content: [{ type: "text", text: `File created: ${savedPath}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Create failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_edit_file") {
      const filePath = String(args.path ?? "");
      const content = String(args.content ?? "");
      if (!filePath) {
        res.status(400).json({ error: "path is required" });
        return;
      }
      try {
        const savedPath = await agentPlatform.editFile(project, filePath, content);
        res.json({ content: [{ type: "text", text: `File edited: ${savedPath}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Edit failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_delete") {
      const targetPath = String(args.path ?? "");
      if (!targetPath) {
        res.status(400).json({ error: "path is required" });
        return;
      }
      try {
        await agentPlatform.deletePath(project, targetPath);
        res.json({ content: [{ type: "text", text: `Deleted: ${targetPath}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Delete failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_mkdir") {
      const dirPath = String(args.path ?? "");
      if (!dirPath) {
        res.status(400).json({ error: "path is required" });
        return;
      }
      try {
        const created = await agentPlatform.createDirectory(project, dirPath);
        res.json({ content: [{ type: "text", text: `Directory created: ${created}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Mkdir failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_move") {
      const source = String(args.source ?? "");
      const destination = String(args.destination ?? "");
      if (!source || !destination) {
        res.status(400).json({ error: "source and destination are required" });
        return;
      }
      try {
        const moved = await agentPlatform.movePath(project, source, destination);
        res.json({ content: [{ type: "text", text: `Moved: ${source} -> ${moved}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Move failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_copy") {
      const source = String(args.source ?? "");
      const destination = String(args.destination ?? "");
      if (!source || !destination) {
        res.status(400).json({ error: "source and destination are required" });
        return;
      }
      try {
        const copied = await agentPlatform.copyPath(project, source, destination);
        res.json({ content: [{ type: "text", text: `Copied: ${source} -> ${copied}` }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Copy failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_list") {
      const dirPath = String(args.path ?? ".");
      try {
        const entries = await agentPlatform.listWorkspace(project, dirPath);
        res.json({ content: [{ type: "text", text: JSON.stringify(entries, null, 2) }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "List failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_tree") {
      try {
        const tree = await agentPlatform.workspaceTree(project);
        res.json({ content: [{ type: "text", text: tree }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Tree failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    if (name === "workspace_exists") {
      const filePath = String(args.path ?? "");
      if (!filePath) {
        res.status(400).json({ error: "path is required" });
        return;
      }
      try {
        const exists = await agentPlatform.pathExists(project, filePath);
        res.json({ content: [{ type: "text", text: exists ? "true" : "false" }] });
      } catch (error) {
        const msg = error instanceof Error ? error.message : "Check failed";
        res.status(500).json({ error: msg });
      }
      return;
    }

    res.status(404).json({ error: `tool not found: ${name}` });
  } catch (error) {
    next(error);
  }
});

export default router;
