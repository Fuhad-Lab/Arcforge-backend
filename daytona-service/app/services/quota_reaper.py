"""Quota reaper — frees Daytona org quota consumed by idle workspaces.

WHY THIS EXISTS (incident 2026-08-26):
  The Daytona org quota is 10 GiB of TOTAL memory. Two stale
  `daytona-medium` (4 Gi) workspace sandboxes left over from testing
  were still `started` and silently consumed 8/10 GiB, so every new
  sandbox creation failed with:

      DaytonaBadRequestError: Total memory limit exceeded.
      Maximum allowed: 10GiB.

  …which surfaced in the product as "the studio page doesn't connect"
  (vm-ops init → 500 → no VM → no agent).

  The `auto_stop_interval=1800` (30 min) DID NOT save us because ANY
  sandbox list/read call refreshes `lastActivityAt` on every listed
  sandbox (verified live: a bare GET /sandbox moves lastActivityAt for
  all sandboxes within ~5 s). The /ready health probe called the
  unfiltered list(), so anyone pinging /ready kept every sandbox
  permanently "active" and the auto-stop never fired.

  Reaping rule: workspace sandboxes whose lastActivityAt (as returned
  in the listing response — which PREDATES the listing's own
  activity-refresh side effect, so the values are trustworthy) is
  older than the configured idle timeout get DELETED, as do
  Error-state workspace corpses (they hold quota and can never boot).

  Workspaces are reproducible: /workspace/init re-provisions and
  scaffolds on demand, and generated files only live for the life of a
  build session. Deleting idle workspaces is the intended posture
  (that is exactly what sandbox_idle_timeout_seconds configures).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from app.config import settings
from app.daytona_client import get_daytona

logger = logging.getLogger(__name__)

# Error markers that mean "org quota exhausted" (memory/cpu/disk/concurrency).
# Daytona wraps these in DaytonaBadRequestError with human-readable details.
_QUOTA_MARKERS = (
    "total memory limit exceeded",
    "total cpu limit exceeded",
    "total disk limit exceeded",
    "concurrency limit",
    "quota",
)


def is_quota_error(exc: BaseException) -> bool:
    """True when the exception text looks like a Daytona quota rejection."""
    text = str(exc).lower()
    return any(marker in text for marker in _QUOTA_MARKERS)


def _parse_iso(ts: str | None) -> datetime | None:
    if not ts:
        return None
    try:
        # Daytona returns e.g. "2026-08-26T12:04:12.872Z" (or +00:00).
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_idle(sandbox: Any, now: datetime, min_age_s: int) -> bool:
    """True when the sandbox's last real activity is older than min_age_s.

    IMPORTANT: `last_activity_at` is read from the listing response we
    ALREADY hold. The listing call itself refreshes lastActivityAt
    server-side, but asynchronously — the response snapshot still
    carries the PRE-refresh value (verified live), so this check sees
    the sandbox's true last activity, not our own probe.
    """
    last = _parse_iso(getattr(sandbox, "last_activity_at", None))
    if last is None:
        # No activity recorded — fall back to creation time.
        created = _parse_iso(getattr(sandbox, "created_at", None))
        last = created
    if last is None:
        return False  # Cannot reason about age — do not delete.
    last = last.astimezone(timezone.utc)
    age = (now - last).total_seconds()
    return age >= min_age_s


async def reap_idle_workspaces(
    min_age_seconds: int | None = None,
) -> list[str]:
    """Delete idle workspace sandboxes to free org quota.

    Deletes any sandbox labeled type=workspace that is ANY of:
      - in Error state (a boot corpse holding quota), or
      - idle (last_activity_at older than min_age_seconds), or
      - older than sandbox_max_lifetime_seconds since CREATION (an
        absolute cap immune to lastActivityAt poisoning — see the
        incident note below).

    Never touches sandboxes without the workspace label, and never
    touches recently-active workspaces. Returns the ids deleted.
    """
    min_age = min_age_seconds if min_age_seconds is not None else int(
        settings.sandbox_idle_timeout_seconds
    )
    max_lifetime = int(settings.sandbox_max_lifetime_seconds)
    daytona = get_daytona()

    def _list_all() -> list[Any]:
        return list(daytona.list())

    try:
        sandboxes = await asyncio.to_thread(_list_all)
    except Exception:
        logger.exception("Reaper: failed to list sandboxes")
        return []

    now = datetime.now(timezone.utc)
    reaped: list[str] = []

    for sbx in sandboxes:
        labels = getattr(sbx, "labels", None) or {}
        if labels.get("type") != "workspace":
            continue  # Not ours — never touch foreign sandboxes.

        sbx_id = getattr(sbx, "id", "?")
        state = str(getattr(sbx, "state", "") or "").lower()
        name = getattr(sbx, "name", "?")

        if "error" in state:
            reason = "error-state corpse"
        elif _is_idle(sbx, now, min_age):
            reason = f"idle > {min_age}s"
        elif max_lifetime > 0 and _is_older_than(sbx, now, max_lifetime):
            # Absolute lifetime cap: catches sandboxes whose lastActivityAt
            # is kept artificially fresh by periodic probes (see the sweeper
            # incident) — creation time cannot be refreshed by anyone.
            reason = f"created > {max_lifetime}s ago (lifetime cap)"
        else:
            continue  # Healthy and recently active — keep it.

        try:
            await asyncio.to_thread(daytona.delete, sbx, 60, False)
            reaped.append(sbx_id)
            logger.warning(
                "Reaper: deleted sandbox %s (%s) — %s", sbx_id, name, reason
            )
        except Exception:
            logger.exception("Reaper: failed to delete sandbox %s", sbx_id)

    if not reaped:
        logger.info("Reaper: nothing to delete (%d sandboxes scanned)", len(sandboxes))
    return reaped


async def reap_forever() -> None:
    """Periodic reaping loop — scheduled from app.main's lifespan.

    Runs every settings.reaper_interval_seconds (default 35 min), which
    MUST exceed sandbox_idle_timeout_seconds: each run's list() call
    refreshes every sandbox's lastActivityAt server-side (verified live),
    so listing more often than the idle threshold would reset the very
    clock the idle rule depends on (self-poisoning) and nothing would
    ever look idle again.
    """
    interval = max(60, int(settings.reaper_interval_seconds))
    delay = max(5, int(settings.reaper_first_run_delay_seconds))

    async def _run() -> None:
        try:
            reaped = await reap_idle_workspaces()
            if reaped:
                logger.warning("Reaper loop: deleted %d idle sandbox(es)", len(reaped))
        except Exception:
            logger.exception("Reaper loop: run failed")

    await asyncio.sleep(delay)
    while True:
        await _run()
        await asyncio.sleep(interval)


def _is_older_than(sandbox: Any, now: datetime, max_age_s: int) -> bool:
    """True when the sandbox was CREATED more than max_age_s ago.

    Creation time is the one clock nobody can refresh — the reliable
    backstop when lastActivityAt is being kept fresh by external probes.
    """
    created = _parse_iso(getattr(sandbox, "created_at", None))
    if created is None:
        return False
    age = (now - created.astimezone(timezone.utc)).total_seconds()
    return age >= max_age_s


async def force_free_quota(exclude_user_id: str | None = None) -> list[str]:
    """EMERGENCY quota release for quota-blocked sandbox creation.

    Deletes the OLDEST workspace sandboxes (by created_at) — never any
    labeled with exclude_user_id (the requesting user's own sandboxes,
    which may be mid-build) — up to settings.quota_force_free_max.

    Only invoked after reap_idle_workspaces() failed to free enough
    quota for a retry to succeed: the alternative is failing the user's
    NEW build while corpses hold the org quota hostage. Oldest-first
    minimizes the chance of hitting an in-flight build (builds are
    minutes old, corpses are hours old).
    """
    max_deletes = max(0, int(settings.quota_force_free_max))
    if max_deletes == 0:
        return []

    daytona = get_daytona()

    def _list_all() -> list[Any]:
        return list(daytona.list())

    try:
        sandboxes = await asyncio.to_thread(_list_all)
    except Exception:
        logger.exception("force_free_quota: failed to list sandboxes")
        return []

    candidates: list[tuple[datetime, Any, str]] = []
    for sbx in sandboxes:
        labels = getattr(sbx, "labels", None) or {}
        if labels.get("type") != "workspace":
            continue
        if exclude_user_id and labels.get("user_id") == exclude_user_id:
            continue  # Never free the requester's own sandboxes.
        created = _parse_iso(getattr(sbx, "created_at", None))
        if created is None:
            continue
        candidates.append((created.astimezone(timezone.utc), sbx, str(getattr(sbx, "id", "?"))))

    # Oldest first.
    candidates.sort(key=lambda c: c[0])

    freed: list[str] = []
    for _created, sbx, sbx_id in candidates[:max_deletes]:
        try:
            await asyncio.to_thread(daytona.delete, sbx, 60, False)
            freed.append(sbx_id)
            logger.warning(
                "force_free_quota: deleted sandbox %s (%s) — quota-blocked creation",
                sbx_id, getattr(sbx, "name", "?"),
            )
        except Exception:
            logger.exception("force_free_quota: failed to delete sandbox %s", sbx_id)
    return freed
