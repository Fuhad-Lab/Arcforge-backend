"""Render free-tier keep-alive (incident 2026-09-03: "Uptime Robot can't be reached").

ROOT CAUSE
----------
arcforge-daytona runs on the Render **free plan**. Free web services spin
down after 15 minutes without inbound traffic. Waking a spun-down service
(Render calls it a "cold start") takes ~30-60s, which is LONGER than the
30-second timeout Uptime Robot uses by default — so a monitor pinging a
sleeping service sees a timeout and reports "can't be reached / down",
even though the service itself is perfectly healthy (the request it never
saw answered actually woke it, and the *next* check usually passes).

 forgvi-engine stays "up" on the same monitor because its native Node
 runtime cold-starts in well under 30s, so every wake-up ping succeeds.

FIX
---
A self-ping loop: the service periodically issues a GET to its OWN public
URL (Render injects ``RENDER_EXTERNAL_URL`` on every service). That inbound
traffic resets the 15-minute idle timer, so the service never spins down
and every monitor — Uptime Robot included — always gets a fast 200.

Notes:
- The ping goes through the PUBLIC url (not localhost:8000) so it counts
  as real inbound traffic at Render's edge.
- /health is the target: static 200, zero side effects, no Daytona calls
  (unlike /ready which can 503 and must not be used by pingers).
- All failures are swallowed and logged: a keep-alive must never take the
  service down with it.
"""

from __future__ import annotations

import asyncio
import logging
import os

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

# Render's idle spin-down threshold is 15 minutes; ping at 10 to stay safely
# under it even if one ping is skipped by an event-loop hiccup.
_DEFAULT_INTERVAL_S = 600
_PING_TIMEOUT_S = 60.0


async def keepalive_forever() -> None:
    """Ping this service's own public URL on a fixed cadence, forever.

    No-ops (single INFO log) when not running on Render (no
    RENDER_EXTERNAL_URL), so local dev and tests are unaffected.
    """
    external_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not settings.keepalive_enabled or not external_url:
        logger.info(
            "Keep-alive disabled (enabled=%s, RENDER_EXTERNAL_URL=%s)",
            settings.keepalive_enabled,
            external_url or "<unset>",
        )
        return

    target = f"{external_url}{settings.keepalive_path}"
    interval = settings.keepalive_interval_seconds

    # Delay the first ping: on a fresh deploy the edge needs a moment to
    # route the new instance; pinging immediately after boot can hit a
    # stale route and log a spurious failure.
    await asyncio.sleep(min(30.0, interval / 10.0))

    logger.info("Keep-alive ON: GET %s every %ss (free-tier spin-down guard)", target, interval)

    async with httpx.AsyncClient(timeout=_PING_TIMEOUT_S) as client:
        while True:
            try:
                resp = await client.get(target)
                logger.debug("Keep-alive ping %s -> %s", target, resp.status_code)
            except Exception:  # noqa: BLE001 — never let a ping kill the loop
                logger.warning("Keep-alive ping failed (will retry)", exc_info=True)
            await asyncio.sleep(interval)
