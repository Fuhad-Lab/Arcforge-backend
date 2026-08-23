import { logger } from "../lib/logger";
import type { ProjectState } from "./agent-platform";
import { daytonaExecutor, type ContainerKind } from "./daytona-executor";

type ServiceKind = "frontend" | "backend";

export type RunningService = {
  kind: ServiceKind;
  pid: number | undefined;
  port: number;
  url: string;
};

export class ServiceRunner {
  constructor(
    // WorkspaceManager is kept for API compatibility but no longer used
    // directly — Daytona sandboxes are self-contained.
    private readonly _workspace?: unknown,
  ) {}

  /**
   * Start a service for the given project in a Daytona sandbox.
   * Uses the project's generated files to build and launch the sandbox.
   */
  async start(
    project: ProjectState,
    kind: ContainerKind,
    port: number,
  ): Promise<RunningService> {
    const files = project.codebase.files;
    if (files.length === 0) {
      throw new Error(
        "No generated files available. Run the pipeline first to generate code.",
      );
    }

    // Determine the entry point based on kind
    let entryPoint: string;
    if (kind === "frontend") {
      // For frontend we serve the whole directory, entry point is unused
      // but still required by the API. Use the first .html or .tsx file.
      const htmlFile = files.find((f) => f.path.endsWith(".html"));
      const tsxFile = files.find((f) => f.path.endsWith(".tsx") || f.path.endsWith(".ts"));
      entryPoint = htmlFile?.path ?? tsxFile?.path ?? "index.html";
    } else {
      // For backend, find the entry point — prefer files named index.ts/js, server.ts/js, app.ts/js, main.ts/js
      const candidates = ["index.ts", "index.js", "server.ts", "server.js", "app.ts", "app.js", "main.ts", "main.js"];
      const match = files.find((f) =>
        candidates.some((c) => f.path.endsWith(c)),
      );
      entryPoint = match?.path ?? files[0].path;
    }

    logger.info(
      { projectId: project.id, kind, port, fileCount: files.length, entryPoint },
      "Starting Daytona sandbox for generated service",
    );

    const info = await daytonaExecutor.startContainer(
      project.id,
      files,
      entryPoint,
      port,
      kind,
    );

    return {
      kind: info.kind,
      pid: undefined, // Docker containers don't have a local PID
      port: info.port,
      url: info.url,
    };
  }

  /**
   * Stop the Daytona sandbox for the given project + kind.
   */
  async stop(projectId: string, kind: ContainerKind): Promise<void> {
    await daytonaExecutor.stopContainer(projectId, kind);
  }
}
