"""Economic calendar adapters — normalized events from configured upstreams only."""

from __future__ import annotations



from datetime import date

from typing import Any



from hptl.news.economic_calendar_engine import affected_markets_for_event, fetch_enriched_calendar



__all__ = ["affected_markets_for_event", "fetch_normalized_events"]





def fetch_normalized_events(

    *,

    start: date,

    end: date,

    catalyst_cfg: dict[str, Any],

) -> dict[str, Any]:

    """Pull economic calendar when API keys exist; else explicit not-configured payload."""

    bundle = fetch_enriched_calendar(start=start, end=end, catalyst_cfg=catalyst_cfg)

    return {

        "window_start": bundle.get("window_start"),

        "window_end": bundle.get("window_end"),

        "events": bundle.get("events") or [],

        "sources": bundle.get("sources") or {},

        "errors": bundle.get("errors") or [],

        "wired": bundle.get("wired", False),

        "message": bundle.get("message") or "",

        "provider": bundle.get("provider"),

        "fetched_at": bundle.get("fetched_at"),

    }

