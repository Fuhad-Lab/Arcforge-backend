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

    Also reaps type=probe sandboxes (disposable engine/health probes —
    incident 2026-09-02: a "engine-probe" sandbox created by an engine
    integration test lived for 21+ HOURS at 4 GiB because the reaper
    only ever looked at type=workspace; with only 2 sandbox slots on
    the free tier, one leaked probe is half the org's capacity). Probes
    get a short absolute lifetime cap (probe_max_lifetime_seconds,
    default 1 h) — creation time cannot be refreshed by anyone.

    Never touches sandboxes with other/missing type labels (foreign
    sandboxes), and never touches recently-active workspaces. Returns
    the ids deleted.
    """
    min_age = min_age_seconds if min_age_seconds is not None else int(
        settings.sandbox_idle_timeout_seconds
    )
    max_lifetime = int(settings.sandbox_max_lifetime_seconds)
    probe_max_lifetime = max(60, int(getattr(settings, "probe_max_lifetime_seconds", 3600)))
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
        sandbox_type = labels.get("type")
        if sandbox_type not in ("workspace", "probe"):
            continue  # Not ours — never touch foreign sandboxes.

        sbx_id = getattr(sbx, "id", "?")
        state = str(getattr(sbx, "state", "") or "").lower()
        name = getattr(sbx, "name", "?")

        if sandbox_type == "probe":
            # Disposable by design: a probe exists to answer one health
            # question. Lifetime-capped on CREATION time (refresh-proof);
            # error-state probes are corpses. Never idle-gated — a probe
            # whose activity stays fresh is still dead weight.
            if "error" in state:
                reason = "error-state probe corpse"
            elif _is_older_than(sbx, now, probe_max_lifetime):
                reason = f"probe older than {probe_max_lifetime}s (lifetime cap)"
            else:
                continue
        elif "error" in state:
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


async def force_free_quota(
    exclude_user_id: str | None = None,
    protect_recent_seconds: int | None = None,
) -> list[str]:
    """EMERGENCY quota release for quota-blocked sandbox creation.

    Deletes the OLDEST workspace sandboxes (by created_at) up to
    settings.quota_force_free_max.

    Incident 2026-09-02 ("my new build always fails"): the org has only
    2 concurrent sandbox slots (10 GiB / 4 GiB each) and ONE real user,
    so virtually every sandbox in the org carries that user's user_id.
    The old blanket rule ("never free the requester's sandboxes") meant
    force_free could NEVER free anything — every new build failed while
    the user's own day-old sandboxes held the quota hostage.

    Incident 2026-09-03 16:04 (live, run c92327d2 — the "daytona is going
    down" root cause): the requester-only 30-minute shield let force_free
    delete a 60-minute-old sandbox that was MID-RUN (an active engine
    build shows no Daytona-API activity, so age was the only signal).
    PROTECTION IS NOW OWNER-BLIND: every workspace sandbox younger than
    protect_recent_seconds (default 7200s ≥ the max run budget) is
    treated as possibly mid-build and shielded — whichever user's init
    triggered the emergency. Everything older (same user or not) is a
    corpse by comparison and is freed oldest-first. The requesting
    project's own sandbox cannot be collateral: its creation is the call
    that just failed, so it does not exist yet.

    Only invoked after reap_idle_workspaces() failed to free enough
    quota for a retry to succeed. When everything is young (a user
    genuinely running two concurrent builds), the new creation FAILS
    HONESTLY with the quota error instead of silently killing a live
    build — the correct trade: a visible boot error beats a dead run.
    Oldest-first minimizes the chance of hitting an in-flight build.
    """
    max_deletes = max(0, int(settings.quota_force_free_max))
    if max_deletes == 0:
        return []
    if protect_recent_seconds is None:
        protect_recent_seconds = max(
            0, int(getattr(settings, "force_free_protect_recent_seconds", 7200))
        )

    daytona = get_daytona()

    def _list_all() -> list[Any]:
        return list(daytona.list())

    try:
        sandboxes = await asyncio.to_thread(_list_all)
    except Exception:
        logger.exception("force_free_quota: failed to list sandboxes")
        return []

    candidates: list[tuple[datetime, Any, str]] = []
    now = datetime.now(timezone.utc)
    for sbx in sandboxes:
        labels = getattr(sbx, "labels", None) or {}
        sandbox_type = labels.get("type")
        if sandbox_type not in ("workspace", "probe"):
            continue  # Foreign sandboxes are never force-freed.
        if sandbox_type == "probe":
            # Probes are disposable by design — never shielded, even the
            # requester's own (a probe blocking a real build is a bug).
            pass
        created = _parse_iso(getattr(sbx, "created_at", None))
        if created is None:
            continue
        created = created.astimezone(timezone.utc)
        if sandbox_type == "workspace":
            # OWNER-BLIND in-flight shield: a young sandbox may be mid-run
            # (engine runs are up to 90 min and generate no Daytona-API
            # activity). Age is the only honest signal — protect every
            # young workspace, not just the requester's.
            age_s = (now - created).total_seconds()
            if age_s < protect_recent_seconds:
                continue  # Possibly mid-build — protect.
        candidates.append((created, sbx, str(getattr(sbx, "id", "?"))))

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
