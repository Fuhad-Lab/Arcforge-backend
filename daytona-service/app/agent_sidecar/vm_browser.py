#!/usr/bin/env python3
"""ArcForge Browser Vision Engine (MCP-Playwright Bridge) — the "Eyes" of the swarm.

Lives inside the Daytona MicroVM next to the orchestrator daemon and gives
the Frontend Agent and the Debugger Agent REAL eyes on the app they are
building:

    browser_tool(action="navigate",     url="http://localhost:3000")
        → Page Title + the ACCESSIBILITY TREE (a text representation of the
          DOM: buttons, links, inputs, headings). NEVER raw HTML — the tree
          is what an LLM can reason about cheaply.

    browser_tool(action="console_spy")
        → every console error/warning, page error, and failed HTTP response
          (status >= 400) captured since the last navigate/interact. This is
          THE Integration Link: the Frontend Agent calls it right after the
          app is served; a `GET /api/data 500` is immediately attributable.

    browser_tool(action="interact",     selector="button#submit",
                 do="click"|"type"|"fill"|"press", value="…")
        → performs the action and returns a short a11y excerpt so the agent
          can see the effect. The Debugger Agent uses this for End-to-End
          testing against plan.md.

    browser_tool(action="screenshot",   filename="fit-check.png")
        → saves a .png under /workspace/.system/screenshots/. When a Vision
          Model is configured (ORCH_VLM_MODEL), the image is ALSO sent to
          the VLM (through the reverse tunnel — the VM holds no key) and the
          model's CSS/styling verdict is returned alongside the file path.

DESIGN NOTES
────────────
• Playwright is installed by the agent installer (`playwright install
  chromium --with-deps`) in the BACKGROUND at workspace boot. Until it is
  ready every action returns {"ok": false, "error": "browser engine not
  installed yet"} — agents degrade gracefully (curl checks / retry).
• One persistent headless Chromium instance, one page. The console/network
  listeners are attached ONCE at page creation; console_spy just reads the
  ring buffer (cleared on each navigate/interact so reports stay scoped to
  the current check).
• SYNC Playwright API — the orchestrator's worker thread is the only caller,
  so no asyncio juggling is required.
• The a11y tree: Playwright's ariaSnapshot() when available, with a custom
  DOM walker fallback (works on any version and returns exactly the
  Buttons/Links/Inputs/Headings the spec asks for).
"""

from __future__ import annotations

import base64
import json
import os
import threading
import time
from typing import Any, Callable, Dict, List, Optional

SCREENSHOT_DIR = os.environ.get(
    "ORCH_SCREENSHOT_DIR",
    os.path.join(os.environ.get("ORCH_SYSTEM_DIR", "/home/daytona/.system"),
                 "screenshots"),
)
NAV_TIMEOUT_MS = int(os.environ.get("ORCH_BROWSER_NAV_TIMEOUT_MS", "45000"))
MAX_CONSOLE_ENTRIES = 200

# DOM walker executed with page.evaluate — extracts the interactive/semantic
# skeleton of the page as indented text (a11y-tree style, LLM-friendly).
_DOM_WALKER = r"""
() => {
  const skip = new Set(['SCRIPT','STYLE','NOSCRIPT','SVG','PATH','LINK','META','HEAD']);
  const lines = [];
  const label = (el) => {
    const t = (el.getAttribute('aria-label') || el.innerText || el.value ||
               el.getAttribute('placeholder') || el.getAttribute('alt') || '')
      .trim().replace(/\s+/g, ' ').slice(0, 80);
    return t ? ' "' + t + '"' : '';
  };
  const walk = (el, depth) => {
    if (lines.length > 400) return;
    if (skip.has(el.tagName)) return;
    let tag = el.tagName.toLowerCase();
    if (el.id) tag += '#' + el.id;
    const cls = (typeof el.className === 'string' && el.className.trim())
      ? '.' + el.className.trim().split(/\s+/).slice(0, 3).join('.') : '';
    let bullet = '';
    if (tag.startsWith('button')) bullet = 'button ';
    else if (tag === 'a' || tag.startsWith('a ')) bullet = 'link ';
    else if (['input','textarea','select'].includes(tag.split('#')[0])) bullet = 'input ';
    else if (/^h[1-6]$/.test(tag.split('#')[0].split('.')[0])) bullet = 'heading ';
    lines.push('  '.repeat(depth) + (bullet || '') + tag + cls + label(el));
    for (const c of el.children) walk(c, depth + 1);
  };
  walk(document.body, 0);
  return lines.slice(0, 400).join('\n');
}
"""


class BrowserEngine:
    """Singleton Playwright bridge. Safe for one caller thread (the task
    worker); every public method returns a JSON-serialisable dict."""

    def __init__(self, vlm_fn: Optional[Callable[[str, str], str]] = None) -> None:
        self._vlm_fn = vlm_fn          # vlm_fn(image_path, question) -> str
        self._lock = threading.Lock()
        self._pw = None                # playwright module
        self._browser = None
        self._page = None
        self._console: List[Dict[str, str]] = []
        self._launch_error = ""
        os.makedirs(SCREENSHOT_DIR, exist_ok=True)

    # ── lifecycle ────────────────────────────────────────────────────────

    def _ensure_page(self):
        """Launch chromium + one page on first use. Returns (page, error)."""
        if self._page is not None:
            return self._page, ""
        try:
            from playwright.sync_api import sync_playwright  # noqa: PLC0415
        except Exception as exc:  # not installed
            self._launch_error = f"playwright not installed: {exc}"
            return None, self._launch_error
        try:
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(
                headless=True,
                args=["--no-sandbox", "--disable-dev-shm-usage",
                      "--disable-gpu", "--font-render-hinting=none"],
            )
            ctx = self._browser.new_context(
                viewport={"width": 1280, "height": 800},
                device_scale_factor=1,
            )
            self._page = ctx.new_page()
            self._attach_listeners(self._page)
            return self._page, ""
        except Exception as exc:
            self._launch_error = f"chromium launch failed: {exc}"
            try:
                if self._browser:
                    self._browser.close()
            except Exception:
                pass
            self._pw = None
            self._browser = None
            self._page = None
            return None, self._launch_error

    def _attach_listeners(self, page) -> None:
        """The console spy — attached once, feeding the ring buffer."""
        def _console(msg) -> None:
            try:
                kind = msg.type
                if kind in ("error", "warning"):
                    self._console.append({"kind": f"console.{kind}", "text": msg.text[:500]})
            except Exception:
                pass

        def _page_error(err) -> None:
            try:
                self._console.append({"kind": "pageerror", "text": str(err)[:500]})
            except Exception:
                pass

        def _response(resp) -> None:
            try:
                if resp.status >= 400:
                    req = resp.request
                    self._console.append({
                        "kind": "network",
                        "text": f"{resp.status} {req.method} {resp.url[:200]}",
                    })
            except Exception:
                pass

        page.on("console", _console)
        page.on("pageerror", _page_error)
        page.on("response", _response)

    def close(self) -> None:
        with self._lock:
            try:
                if self._browser:
                    self._browser.close()
            except Exception:
                pass
            try:
                if self._pw:
                    self._pw.stop()
            except Exception:
                pass
            self._pw = None
            self._browser = None
            self._page = None

    def _truncate_console(self) -> None:
        if len(self._console) > MAX_CONSOLE_ENTRIES:
            del self._console[:len(self._console) - MAX_CONSOLE_ENTRIES]

    # ── public actions (the MCP tool surface) ────────────────────────────

    def navigate(self, url: str) -> Dict[str, Any]:
        page, err = self._ensure_page()
        if page is None:
            return {"ok": False, "error": err}
        self._console.clear()
        try:
            resp = page.goto(url, wait_until="domcontentloaded", timeout=NAV_TIMEOUT_MS)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except Exception:
                pass  # networkidle often times out on dev servers (HMR ws) — fine
            time.sleep(1.0)  # let client-side fetches fire + consoles land
            title = page.title() or "(untitled)"
            tree = self._a11y_tree(page)
            status = resp.status if resp else 0
            return {
                "ok": True,
                "title": title,
                "http_status": status,
                "accessibility_tree": tree,
                "note": "Use console_spy next to see errors/warnings.",
            }
        except Exception as exc:
            return {"ok": False, "error": f"navigate failed: {str(exc)[:300]}"}

    def _a11y_tree(self, page) -> str:
        """Accessibility tree as text — ariaSnapshot when available, else a
        custom DOM walker. NEVER raw HTML."""
        try:
            snap = page.locator("body").aria_snapshot()
            if snap and snap.strip():
                return snap[:6000]
        except Exception:
            pass
        try:
            return str(page.evaluate(_DOM_WALKER))[:6000] or "(empty page)"
        except Exception as exc:
            return f"(a11y extraction failed: {str(exc)[:120]})"

    def console_spy(self) -> Dict[str, Any]:
        page, err = self._ensure_page()
        if page is None:
            return {"ok": False, "error": err}
        self._truncate_console()
        return {
            "ok": True,
            "errors": [e for e in self._console if e["kind"] != "console.warning"],
            "warnings": [e for e in self._console if e["kind"] == "console.warning"],
            "total": len(self._console),
        }

    def interact(self, selector: str, do: str, value: str = "") -> Dict[str, Any]:
        page, err = self._ensure_page()
        if page is None:
            return {"ok": False, "error": err}
        if not selector:
            return {"ok": False, "error": "selector is required"}
        do = (do or "click").lower()
        self._console.clear()
        try:
            loc = page.locator(selector).first
            loc.wait_for(state="visible", timeout=8000)
            if do == "click":
                loc.click(timeout=8000)
            elif do in ("type", "fill"):
                if do == "fill" or not value:
                    loc.fill(value or "", timeout=8000)
                else:
                    loc.press_sequentially(value, delay=25, timeout=15000)
            elif do == "press":
                loc.press(value or "Enter", timeout=8000)
            elif do == "hover":
                loc.hover(timeout=8000)
            elif do == "select":
                loc.select_option(value, timeout=8000)
            else:
                return {"ok": False, "error": f"unknown action '{do}' (click|type|fill|press|hover|select)"}
            time.sleep(0.8)  # let the UI + any fetch settle
            excerpt = self._a11y_tree(page).splitlines()
            return {
                "ok": True,
                "action": f"{do} {selector}",
                "page_after": "\n".join(excerpt[:60]),
                "note": "Call console_spy to check for errors after this action.",
            }
        except Exception as exc:
            return {"ok": False,
                    "error": f"interact failed on '{selector}': {str(exc)[:300]}"}

    def screenshot(self, filename: str, vlm_question: str = "") -> Dict[str, Any]:
        page, err = self._ensure_page()
        if page is None:
            return {"ok": False, "error": err}
        if not filename:
            filename = f"shot-{int(time.time())}.png"
        if not filename.endswith(".png"):
            filename += ".png"
        safe = os.path.basename(filename.replace("/", "_"))
        path = os.path.join(SCREENSHOT_DIR, safe)
        try:
            page.screenshot(path=path, full_page=False)
        except Exception as exc:
            return {"ok": False, "error": f"screenshot failed: {str(exc)[:200]}"}
        out: Dict[str, Any] = {"ok": True, "path": path, "file": safe}
        if self._vlm_fn is not None:
            try:
                out["vision_check"] = self._vlm_fn(
                    path, vlm_question or
                    "Review this web app screenshot. Does the UI look correctly "
                    "styled and laid out (alignment, contrast, spacing, no broken "
                    "elements)? List concrete visual defects if any, else say LOOKS_GOOD.")
            except Exception as exc:
                out["vision_check_error"] = str(exc)[:200]
        return out

    def health(self) -> Dict[str, Any]:
        installed = False
        try:
            import playwright  # noqa: F401, PLC0415
            installed = True
        except Exception:
            installed = False
        return {
            "playwright_installed": installed,
            "browser_live": self._page is not None,
            "last_launch_error": self._launch_error,
            "screenshot_dir": SCREENSHOT_DIR,
        }


_ENGINE: Optional[BrowserEngine] = None
_ENGINE_LOCK = threading.Lock()


def get_browser_engine(vlm_fn: Optional[Callable[[str, str], str]] = None) -> BrowserEngine:
    global _ENGINE
    with _ENGINE_LOCK:
        if _ENGINE is None:
            _ENGINE = BrowserEngine(vlm_fn=vlm_fn)
        return _ENGINE
