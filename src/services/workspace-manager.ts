import { promises as fs, type Dirent } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";

const MAX_FILE_BYTES = 1_000_000;

export type WorkspaceEntry = {
  path: string;
  type: "file" | "directory";
  size?: number;
  modifiedAt?: string;
};

export type WorkspaceFileOperation = {
  action: "create" | "edit" | "delete" | "mkdir" | "move" | "copy" | "read";
  path: string;
  content?: string;
  destination?: string;
};

export class WorkspaceManager {
  constructor(private readonly root = path.resolve("./generated-workspaces")) {}

  // ─── FILE CRUD ────────────────────────────────────────────────────

  async create(projectId: string, relativePath: string, content = ""): Promise<string> {
    const target = this.resolve(projectId, relativePath);
    if (Buffer.byteLength(content, "utf8") > MAX_FILE_BYTES) {
      throw new Error(`File exceeds the ${MAX_FILE_BYTES / 1_000} KB workspace limit.`);
    }
    await fs.mkdir(path.dirname(target), { recursive: true });
    const temporary = `${target}.${randomUUID()}.tmp`;
    await fs.writeFile(temporary, content, { encoding: "utf8", mode: 0o600 });
    await fs.rename(temporary, target);
    return target;
  }

  async read(projectId: string, relativePath: string): Promise<string> {
    const target = this.resolve(projectId, relativePath);
    return fs.readFile(target, "utf8");
  }

  async edit(projectId: string, relativePath: string, content: string): Promise<string> {
    const target = this.resolve(projectId, relativePath);
    // Verify file exists before overwriting
    try {
      await fs.access(target);
    } catch {
      throw new Error(`Cannot edit: file does not exist at ${relativePath}`);
    }
    if (Buffer.byteLength(content, "utf8") > MAX_FILE_BYTES) {
      throw new Error(`File exceeds the ${MAX_FILE_BYTES / 1_000} KB workspace limit.`);
    }
    const temporary = `${target}.${randomUUID()}.tmp`;
    await fs.writeFile(temporary, content, { encoding: "utf8", mode: 0o600 });
    await fs.rename(temporary, target);
    return target;
  }

  async delete(projectId: string, relativePath: string): Promise<void> {
    const target = this.resolve(projectId, relativePath);
    await fs.access(target); // Ensure exists before deleting
    await fs.rm(target, { recursive: true, force: false });
  }

  // ─── DIRECTORY OPERATIONS ─────────────────────────────────────────

  async mkdir(projectId: string, relativePath: string): Promise<string> {
    const target = this.resolve(projectId, relativePath);
    await fs.mkdir(target, { recursive: true });
    return target;
  }

  async deleteEmptyDirectory(projectId: string, relativePath: string): Promise<void> {
    const target = this.resolve(projectId, relativePath);
    const entries = await fs.readdir(target);
    if (entries.length > 0) {
      throw new Error(`Cannot delete non-empty directory: ${relativePath}`);
    }
    await fs.rmdir(target);
  }

  // ─── MOVE / COPY / RENAME ─────────────────────────────────────────

  async move(projectId: string, source: string, destination: string): Promise<string> {
    const srcPath = this.resolve(projectId, source);
    const destPath = this.resolve(projectId, destination);
    await fs.access(srcPath); // Ensure source exists
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    await fs.rename(srcPath, destPath);
    return destPath;
  }

  async copy(projectId: string, source: string, destination: string): Promise<string> {
    const srcPath = this.resolve(projectId, source);
    const destPath = this.resolve(projectId, destination);
    const stat = await fs.stat(srcPath);
    await fs.mkdir(path.dirname(destPath), { recursive: true });
    if (stat.isDirectory()) {
      await this.copyDirRecursive(srcPath, destPath);
    } else {
      await fs.copyFile(srcPath, destPath);
    }
    return destPath;
  }

  // ─── LISTING & INSPECTION ─────────────────────────────────────────

  async list(projectId: string, relativePath = "."): Promise<WorkspaceEntry[]> {
    const directory = this.resolve(projectId, relativePath);
    const entries = await fs.readdir(directory, { withFileTypes: true });
    return Promise.all(
      entries.map(async (entry) => {
        const entryPath = path.join(directory, entry.name);
        if (entry.isDirectory()) {
          return {
            path: path.relative(this.projectRoot(projectId), entryPath),
            type: "directory" as const,
            modifiedAt: (await fs.stat(entryPath)).mtime.toISOString(),
          };
        }
        const stat = await fs.stat(entryPath);
        return {
          path: path.relative(this.projectRoot(projectId), entryPath),
          type: "file" as const,
          size: stat.size,
          modifiedAt: stat.mtime.toISOString(),
        };
      }),
    );
  }

  async exists(projectId: string, relativePath: string): Promise<boolean> {
    try {
      await fs.access(this.resolve(projectId, relativePath));
      return true;
    } catch {
      return false;
    }
  }

  async stat(projectId: string, relativePath: string): Promise<WorkspaceEntry | null> {
    try {
      const target = this.resolve(projectId, relativePath);
      const stat = await fs.stat(target);
      const relative = path.relative(this.projectRoot(projectId), target);
      return {
        path: relative,
        type: stat.isDirectory() ? "directory" : "file",
        size: stat.isFile() ? stat.size : undefined,
        modifiedAt: stat.mtime.toISOString(),
      };
    } catch {
      return null;
    }
  }

  projectRoot(projectId: string): string {
    return this.resolve(projectId, ".");
  }

  // ─── BULK OPERATIONS (for multi-file generation) ───────────────────

  /**
   * Write multiple files atomically. If any file fails, all are rolled back.
   * Returns the list of successfully written file paths.
   */
  async bulkCreate(
    projectId: string,
    files: Array<{ path: string; content: string }>,
  ): Promise<string[]> {
    const written: string[] = [];
    const backups = new Map<string, string>();

    try {
      for (const file of files) {
        const target = this.resolve(projectId, file.path);
        // Backup existing file if it exists
        try {
          const existing = await fs.readFile(target, "utf8");
          backups.set(file.path, existing);
        } catch {
          // File doesn't exist, no backup needed
        }
        await this.create(projectId, file.path, file.content);
        written.push(file.path);
      }
      return written;
    } catch (error) {
      // Rollback: restore all backups
      for (const filePath of written) {
        const backup = backups.get(filePath);
        if (backup !== undefined) {
          try {
            await this.create(projectId, filePath, backup);
          } catch {
            // Best effort rollback
          }
        } else {
          try {
            await fs.rm(this.resolve(projectId, filePath), { force: true });
          } catch {
            // Best effort cleanup
          }
        }
      }
      throw error;
    }
  }

  /**
   * Get a tree representation of the workspace for agent context.
   * Limits depth to prevent token blowup.
   */
  async tree(projectId: string, maxDepth = 4): Promise<string> {
    const root = this.projectRoot(projectId);
    const lines: string[] = [];
    await this.buildTree(root, "", lines, 0, maxDepth);
    return lines.length > 0 ? lines.join("\n") : "(empty workspace)";
  }

  // ─── PRIVATE HELPERS ──────────────────────────────────────────────

  private async copyDirRecursive(src: string, dest: string): Promise<void> {
    await fs.mkdir(dest, { recursive: true });
    const entries = await fs.readdir(src, { withFileTypes: true });
    for (const entry of entries) {
      const srcPath = path.join(src, entry.name);
      const destPath = path.join(dest, entry.name);
      if (entry.isDirectory()) {
        await this.copyDirRecursive(srcPath, destPath);
      } else {
        await fs.copyFile(srcPath, destPath);
      }
    }
  }

  private async buildTree(
    dir: string,
    prefix: string,
    lines: string[],
    depth: number,
    maxDepth: number,
  ): Promise<void> {
    if (depth > maxDepth) {
      lines.push(`${prefix}... (depth limit)`);
      return;
    }
    let entries: Dirent[];
    try {
      entries = await fs.readdir(dir, { withFileTypes: true });
    } catch {
      return;
    }
    // Sort: directories first, then files, alphabetically
    entries.sort((a, b) => {
      if (a.isDirectory() && !b.isDirectory()) return -1;
      if (!a.isDirectory() && b.isDirectory()) return 1;
      return a.name.localeCompare(b.name);
    });
    for (let i = 0; i < entries.length; i++) {
      const isLast = i === entries.length - 1;
      const connector = isLast ? "└── " : "├── ";
      const entryPath = path.join(dir, entries[i].name);
      lines.push(`${prefix}${connector}${entries[i].name}${entries[i].isDirectory() ? "/" : ""}`);
      if (entries[i].isDirectory()) {
        const childPrefix = prefix + (isLast ? "    " : "│   ");
        await this.buildTree(entryPath, childPrefix, lines, depth + 1, maxDepth);
      }
    }
  }

  private resolve(projectId: string, relativePath: string): string {
    if (!/^[0-9a-f-]{36}$/i.test(projectId)) throw new Error("Invalid project id.");
    const projectRoot = path.resolve(this.root, projectId);
    const resolved = path.resolve(projectRoot, relativePath);
    if (resolved !== projectRoot && !resolved.startsWith(`${projectRoot}${path.sep}`)) {
      throw new Error("Unsafe workspace path rejected.");
    }
    return resolved;
  }
}
