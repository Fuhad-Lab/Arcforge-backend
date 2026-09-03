/**
 * Shared text-file filters for GitHub operations (GROUP 3).
 *
 * Used by BOTH the in-VM agent's /tunnel/github proxy (sync_workspace) and
 * the repository-import route — one definition of "which files are text
 * and safe to move between GitHub and a workspace".
 */

/** Directory segments never imported from / synced to GitHub. */
export const SKIP_DIRS = new Set(["node_modules", ".next", ".git"]);

/** Extensions considered text (binary files are never moved). */
export const TEXT_EXTENSIONS = new Set([
  ".ts", ".tsx", ".js", ".jsx", ".mjs", ".cjs", ".json", ".css", ".scss", ".sass",
  ".less", ".html", ".htm", ".md", ".mdx", ".txt", ".py", ".yml", ".yaml", ".toml",
  ".sql", ".sh", ".svg", ".xml", ".cfg", ".ini", ".conf", ".env", ".example", ".lock",
  ".vue", ".svelte", ".rb", ".go", ".rs", ".java", ".kt", ".swift", ".php", ".c",
  ".h", ".cpp", ".cs", ".prisma", ".graphql", ".gql", ".tf", ".dockerfile",
]);

/** Extension-less / dot files that are still text. */
export const TEXT_FILENAMES = new Set([
  "dockerfile", "makefile", "procfile", ".gitignore", ".dockerignore",
  ".npmrc", ".editorconfig", ".env", ".env.example", ".env.local",
  ".gitattributes", ".babelrc", ".eslintrc", ".prettierrc",
]);
