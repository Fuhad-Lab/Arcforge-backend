"""degit-style template fetcher — greenfield provisioning for new workspaces.

Implements the architecture mandate: "Your backend triggers degit to instantly
pull a fast, un-versioned frontend framework template (Next.js) into the
microVM."

How it works (host side, pure stdlib — no git binary, no clones, no history):

  1. Resolve a template spec: ``owner/repo/subdir`` or ``owner/repo/subdir@ref``
     (ref = branch / tag / commit SHA; defaults to HEAD). Default spec comes
     from ``ARCFORGE_TEMPLATE_SPEC``, falling back to the platform's own
     ``Fuhad-Lab/arcforge-templates/nextjs-starter``.
  2. Download the repository tarball in one HTTP GET from GitHub's codeload
     API (``https://codeload.github.com/{owner}/{repo}/tar.gz/{ref}``) — a
     private-repo ``GITHUB_TOKEN`` env var is attached as a Bearer header
     when present, but public templates need no credentials at all.
  3. Extract in memory with tarfile: strip the leading ``{repo}-{ref}/``
     component, keep only the files under the requested subdirectory and
     flatten that prefix away, so the result maps 1:1 onto
     ``/workspace/frontend`` inside a fresh MicroVM.
  4. Skip anything a greenfield scaffold must NOT carry: ``.git*`` entries
     (dir + files), ``node_modules`` trees, package-manager lockfiles,
     binaries (non-UTF-8) and oversized blobs (> 256 KiB).

A 10-minute module-level cache keeps repeated workspace creations from
re-downloading the same tarball (typical burst: many users, same starter).

Callers on the workspace-creation path must use :func:`fetch_template_safe`
which NEVER raises — on any failure it logs a warning and returns ``{}`` so
provisioning falls back to the in-VM minimal scaffold.
"""

from __future__ import annotations

import io
import logging
import os
import tarfile
import time
import urllib.error
import urllib.parse
import urllib.request

logger = logging.getLogger(__name__)

# The platform's default greenfield frontend template (public repo).
DEFAULT_TEMPLATE_SPEC = "Fuhad-Lab/arcforge-templates/nextjs-starter"

_CODELOAD_URL = "https://codeload.github.com/{owner}/{repo}/tar.gz/{ref}"

_DOWNLOAD_TIMEOUT_S = 20.0
_MAX_FILE_BYTES = 256 * 1024  # skip any single template file larger than this

# Package-manager lockfiles — the VM installs its own pinned deps.
_LOCKFILE_NAMES = frozenset(
    {"package-lock.json", "yarn.lock", "pnpm-lock.yaml", "bun.lock", "bun.lockb"},
)

# Module-level cache: {spec: (fetched_at_monotonic, files)} with a 10-min TTL
# so a burst of workspace creations downloads a template at most once.
_TEMPLATE_CACHE: dict[str, tuple[float, dict[str, str]]] = {}
_CACHE_TTL_S = 600.0


# ---------------------------------------------------------------------------
# Spec handling
# ---------------------------------------------------------------------------


def _resolve_spec(spec: str | None) -> str:
    """Explicit spec > ARCFORGE_TEMPLATE_SPEC env > platform default."""
    if spec and spec.strip():
        return spec.strip()
    env_spec = os.environ.get("ARCFORGE_TEMPLATE_SPEC", "").strip()
    return env_spec or DEFAULT_TEMPLATE_SPEC


def _parse_spec(spec: str) -> tuple[str, str, str, str]:
    """Split a spec into ``(owner, repo, subdir, ref)``.

    Accepts ``owner/repo/subdir@ref`` (subdir optional, ref optional —
    defaults to HEAD). Raises ValueError on malformed input.
    """
    ref = "HEAD"
    if "@" in spec:
        spec, _, raw_ref = spec.rpartition("@")
        raw_ref = raw_ref.strip()
        if raw_ref:
            ref = raw_ref
    parts = [p for p in spec.strip().strip("/").split("/") if p]
    if len(parts) < 2:
        raise ValueError(
            f"Malformed template spec {spec!r} — expected owner/repo[/subdir][@ref]",
        )
    owner, repo = parts[0], parts[1]
    subdir = "/".join(parts[2:]).strip("/")
    return owner, repo, subdir, ref


# ---------------------------------------------------------------------------
# Download + extraction
# ---------------------------------------------------------------------------


def _download_tarball(owner: str, repo: str, ref: str) -> bytes:
    """Fetch the codeload tarball for owner/repo@ref into memory."""
    url = _CODELOAD_URL.format(
        owner=urllib.parse.quote(owner, safe=""),
        repo=urllib.parse.quote(repo, safe=""),
        ref=urllib.parse.quote(ref, safe="/"),
    )
    headers = {
        # codeload rejects requests without a User-Agent now and then.
        "User-Agent": "arcforge-degit/1.0",
        "Accept": "application/x-gzip",
    }
    token = os.environ.get("GITHUB_TOKEN", "").strip()
    if token:
        # Optional — only needed when the template repo is private.
        headers["Authorization"] = f"Bearer {token}"

    request = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(request, timeout=_DOWNLOAD_TIMEOUT_S) as resp:
            return resp.read()
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"codeload returned HTTP {exc.code} for {url}") from exc


def _extract_files(tarball: bytes, subdir: str) -> dict[str, str]:
    """Extract text files from a GitHub tarball into {relative_path: content}.

    The leading ``{repo}-{ref}/`` component is stripped, then (when a subdir
    was requested) only files under that prefix are kept with the prefix
    flattened away. Skips .git*, node_modules, lockfiles, binaries and files
    larger than _MAX_FILE_BYTES.
    """
    prefix = f"{subdir}/" if subdir else ""
    files: dict[str, str] = {}

    with tarfile.open(fileobj=io.BytesIO(tarball), mode="r:gz") as tar:
        for member in tar:
            if not member.isfile():
                continue  # directories, symlinks, devices — never planted

            # Strip the leading "{repo}-{ref}/" component. GitHub mangles the
            # ref into the root dir name (e.g. "arcforge-templates-main"), so
            # drop the first component unconditionally (standard degit
            # behaviour) rather than guessing the exact mangling.
            parts = member.name.split("/")
            if len(parts) < 2:
                continue  # the stripped root dir itself (or a stray root file)
            rel = "/".join(parts[1:])
            if not rel or rel.endswith("/"):
                continue

            # Subdirectory filter + flatten (app/… inside the template
            # becomes app/… at the destination root).
            if prefix:
                if not rel.startswith(prefix):
                    continue
                rel = rel[len(prefix):]
                if not rel:
                    continue

            rel_parts = rel.split("/")
            if any(p.startswith(".git") for p in rel_parts):
                continue  # .git, .github, .gitignore, .gitattributes, …
            if "node_modules" in rel_parts:
                continue
            if rel_parts[-1] in _LOCKFILE_NAMES:
                continue
            if member.size > _MAX_FILE_BYTES:
                logger.debug("degit: skipping oversized file %s (%d bytes)", rel, member.size)
                continue

            handle = tar.extractfile(member)
            if handle is None:
                continue
            raw = handle.read()
            try:
                files[rel] = raw.decode("utf-8")
            except UnicodeDecodeError:
                logger.debug("degit: skipping binary file %s", rel)
                continue

    return files


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def fetch_template(spec: str | None = None) -> dict[str, str]:
    """Fetch a template's text files as {relative_path: content}.

    ``spec`` — ``owner/repo/subdir`` or ``owner/repo/subdir@ref``. When None
    the ARCFORGE_TEMPLATE_SPEC env var is used, then the platform default.

    Results are cached per-spec for 10 minutes. Raises on network / parse /
    extraction errors — callers on the provisioning path should prefer
    :func:`fetch_template_safe`.
    """
    resolved = _resolve_spec(spec)
    owner, repo, subdir, ref = _parse_spec(resolved)

    cached = _TEMPLATE_CACHE.get(resolved)
    if cached is not None and (time.monotonic() - cached[0]) < _CACHE_TTL_S:
        logger.debug("degit: cache hit for %s", resolved)
        return dict(cached[1])

    logger.info("degit: fetching template %s (ref=%s, subdir=%s)", resolved, ref, subdir or "<root>")
    tarball = _download_tarball(owner, repo, ref)
    files = _extract_files(tarball, subdir)
    if not files:
        logger.warning("degit: extracted 0 files from %s — spec subdir/ref correct?", resolved)

    _TEMPLATE_CACHE[resolved] = (time.monotonic(), files)
    logger.info("degit: template %s resolved to %d files", resolved, len(files))
    return dict(files)


def fetch_template_safe(spec: str | None = None) -> dict[str, str]:
    """Never-raise wrapper for :func:`fetch_template`.

    Greenfield template planting is strictly best-effort: on ANY failure we
    log a warning and return ``{}`` so the caller keeps provisioning with the
    in-VM minimal scaffold instead of failing workspace creation.
    """
    try:
        return fetch_template(spec)
    except Exception as exc:  # noqa: BLE001 — template failure must never propagate
        logger.warning(
            "degit: template fetch failed for %r: %s — falling back to in-VM scaffold",
            spec if spec else _resolve_spec(spec), exc,
        )
        return {}
