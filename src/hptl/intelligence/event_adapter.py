"""Economic calendar adapters — normalized events from configured upstreams only."""
from __future__ import annotations

import os
from datetime import date, datetime, timezone
from typing import Any

from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED
from hptl.news.calendar_downloader import download_calendar_window
from hptl.news.calendar_parser import parse_finnhub_rows, parse_trading_economics_rows


def _flatten_catalyst_keywords(instrument_block: dict[str, Any]) -> list[str]:
    groups = instrument_block.get("catalyst_keyword_groups")
    if not isinstance(groups, dict):
        return []
    out: list[str] = []
    for kws in groups.values():
        if isinstance(kws, list):
            out.extend(str(x) for x in kws if x)
    return out


def _macro_sensitivity_tokens(instrument_block: dict[str, Any]) -> list[str]:
    sens = instrument_block.get("macro_sensitivities")
    if not isinstance(sens, list):
        return []
    tokens: list[str] = []
    for s in sens:
        if not s:
            continue
        t = str(s).replace("_", " ").lower()
        tokens.append(t)
        tokens.append(str(s).lower())
    return tokens


def affected_markets_for_event(
    event_name: str,
    *,
    catalyst_cfg: dict[str, Any],
) -> list[str]:
    """Tag instruments when event text matches that instrument's config keywords only."""
    inst = catalyst_cfg.get("instruments")
    if not isinstance(inst, dict):
        return []
    en = event_name.lower()
    hit: list[str] = []
    for market, block in inst.items():
        if not isinstance(block, dict):
            continue
        matched = False
        for kw in _flatten_catalyst_keywords(block):
            if kw and str(kw).lower() in en:
                matched = True
                break
        if not matched:
            for tok in _macro_sensitivity_tokens(block):
                if tok and tok in en:
                    matched = True
                    break
        if matched:
            hit.append(str(market))
    return sorted(set(hit))


def _importance_rank(importance: str) -> int:
    s = (importance or "").strip().lower()
    if s in ("high", "3", "star star star", "***"):
        return 3
    if s in ("medium", "2", "star star", "**"):
        return 2
    if s in ("low", "1", "star", "*"):
        return 1
    return 0


def _normalize_calendar_record(
    rec: Any,
    *,
    catalyst_cfg: dict[str, Any],
) -> dict[str, Any]:
    """Map ``CalendarEventRecord`` to dashboard dict (``affected_markets`` from config match)."""
    ts: datetime = rec.event_timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    d = ts.date().isoformat()
    aff = affected_markets_for_event(rec.event_name, catalyst_cfg=catalyst_cfg)
    return {
        "date": d,
        "event_name": rec.event_name,
        "country": rec.country,
        "importance": rec.importance,
        "importance_rank": _importance_rank(rec.importance),
        "affected_markets": aff,
        "source": rec.source,
        "event_timestamp": ts.isoformat(),
    }


def fetch_normalized_events(
    *,
    start: date,
    end: date,
    catalyst_cfg: dict[str, Any],
) -> dict[str, Any]:
    """Pull Finnhub / Trading Economics when API keys exist; else explicit not-configured."""
    has_fh = bool(os.getenv("FINNHUB_API_KEY", "").strip())
    has_te = bool(os.getenv("TRADINGECONOMICS_API_KEY", "").strip())

    bundle = download_calendar_window(start, end)
    events: list[dict[str, Any]] = []

    if has_fh:
        for rec in parse_finnhub_rows(bundle.finnhub or []):
            events.append(_normalize_calendar_record(rec, catalyst_cfg=catalyst_cfg))
    if has_te:
        for rec in parse_trading_economics_rows(bundle.trading_economics or []):
            events.append(_normalize_calendar_record(rec, catalyst_cfg=catalyst_cfg))

    events.sort(key=lambda e: (e.get("date") or "", e.get("event_name") or ""))

    fh_count = sum(1 for e in events if e.get("source") == "finnhub")
    te_count = sum(1 for e in events if e.get("source") == "trading_economics")

    fh_status: str = SOURCE_NOT_CONFIGURED if not has_fh else "finnhub"
    te_status: str = SOURCE_NOT_CONFIGURED if not has_te else "trading_economics"

    return {
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "events": events,
        "sources": {
            "finnhub": {"status": fh_status, "count": fh_count},
            "trading_economics": {"status": te_status, "count": te_count},
        },
        "errors": list(bundle.errors),
    }
