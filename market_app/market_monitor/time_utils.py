from __future__ import annotations

from market_monitor.timebase import utcnow


def utc_now_iso() -> str:
    return utcnow().isoformat()
