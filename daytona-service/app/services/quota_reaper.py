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

    Deletes any sandbox labeled type=workspace that is EITHER:
      - in Error state (a boot corpse holding quota), or
      - idle (last_activity_at older than min_age_seconds).

    Never touches sandboxes without the workspace label, and never
    touches recently-active workspaces. Returns the ids deleted.
    """
    min_age = min_age_seconds if min_age_seconds is not None else int(
        settings.sandbox_idle_timeout_seconds
    )
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
