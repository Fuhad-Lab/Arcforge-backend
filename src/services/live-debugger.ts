import { logger } from "../lib/logger";
import type { Diagnostic, ProjectSpec } from "./agent-platform";
import { existsSync, readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";

export type LiveCheckResult = {
  status: "passed" | "failed" | "unavailable";
  url: string;
  diagnostics: Diagnostic[];
  checkedAt: string;
};

function ensureBrowserLibraries(): void {
  if (process.env.LD_LIBRARY_PATH?.split(":").some((path) => existsSync(join(path, "libgbm.so.1")))) {
    return;
  }

  // Replit's Nix packages are available in /nix/store but are not always
  // added to the workflow process' dynamic linker path. Chromium requires
  // libgbm even in headless mode, so add the package's lib directory before
  // launching it. Keep an explicitly configured path untouched.
  try {
    const storeLibs = readdirSync("/nix/store")
      .filter((entry) => /(?:^|-)mesa-|(?:^|-)alsa-lib-|(?:^|-)libdrm-/.test(entry))
      .map((entry) => join("/nix/store", entry, "lib"));
    const is64BitElf = (library: string) => {
      try {
        return readFileSync(library)[4] === 2;
      } catch {
        return false;
      }
    };
    const libraryDirs = storeLibs.filter((path) =>
      ["libgbm.so.1", "libasound.so.2", "libdrm.so.2"].some(
        (name) => existsSync(join(path, name)) && is64BitElf(join(path, name)),
      ),
    );
    if (libraryDirs.length > 0) {
      const discovered = [...new Set(libraryDirs)].join(":");
      process.env.LD_LIBRARY_PATH = process.env.LD_LIBRARY_PATH
        ? `${discovered}:${process.env.LD_LIBRARY_PATH}`
        : discovered;
    }
  } catch {
    // The normal system linker path is sufficient outside Nix-based hosts.
  }
}

export async function debugLiveWebsite(
  url: string,
  spec: ProjectSpec,
): Promise<LiveCheckResult> {
  const checkedAt = new Date().toISOString();
  try {
    ensureBrowserLibraries();
    const { chromium } = await import("playwright");
    const browser = await chromium.launch({ headless: true });
    try {
      const page = await browser.newPage();
      const consoleErrors: string[] = [];
      page.on("console", (message) => {
        if (message.type() === "error") consoleErrors.push(message.text());
      });
      const response = await page.goto(url, { waitUntil: "domcontentloaded", timeout: 20_000 });
      const title = await page.title();
      const diagnostics: Diagnostic[] = [];
      if (!response || response.status() >= 400) {
        diagnostics.push({
          severity: "error",
          source: "debugger",
          message: `Live website returned HTTP ${response?.status() ?? "no response"}.`,
        });
      }
      if (!title.trim()) {
        diagnostics.push({
          severity: "warning",
          source: "debugger",
          message: "Live website has no document title.",
        });
      }
      for (const error of consoleErrors) {
        diagnostics.push({ severity: "error", source: "debugger", message: `Browser console: ${error}` });
      }
      const expectedPaths = Object.keys(spec.paths);
      if (expectedPaths.length > 0) {
        const body = await page.locator("body").innerText().catch(() => "");
        if (!body.trim()) {
          diagnostics.push({
            severity: "error",
            source: "debugger",
            message: "Live website rendered an empty document body.",
          });
        }
      }
      return {
        status: diagnostics.some((item) => item.severity === "error") ? "failed" : "passed",
        url,
        diagnostics,
        checkedAt,
      };
    } finally {
      await browser.close();
    }
  } catch (error) {
    logger.warn({ err: error, url }, "Playwright live debugger unavailable");
    return {
      status: "unavailable",
      url,
      diagnostics: [
        {
          severity: "warning",
          source: "debugger",
          message: error instanceof Error ? error.message : "Playwright could not start.",
        },
      ],
      checkedAt,
    };
  }
}