"""Classify calendar events into macro-environment tags (not trade signals).

Tags describe liquidity / growth / inflation *context* only.
"""
from __future__ import annotations

import re
from dataclasses import replace

from hptl.news.contracts import CalendarEventRecord

_TAG_SET = frozenset(
    {
        "risk_on",
        "risk_off",
        "inflationary",
        "deflationary",
        "growth_positive",
        "growth_negative",
    }
)


def _norm_name(name: str) -> str:
    return re.sub(r"\s+", " ", name.lower()).strip()


def _markets_for_country(country: str) -> tuple[str, ...]:
    c = country.upper()
    if c in {"US", "USA", "UNITED STATES"}:
        return ("rates", "usd", "equities_us", "gold")
    if c in {"EU", "EMU", "EZ", "GERMANY", "FRANCE"}:
        return ("eur", "rates_eu", "equities_eu")
    if c in {"GB", "UK", "UNITED KINGDOM"}:
        return ("gbp", "rates_uk", "equities_uk")
    if c in {"JP", "JAPAN"}:
        return ("jpy", "rates_jp", "equities_jp")
    return ("fx", "rates_global")


def _tags_and_bias(event: CalendarEventRecord) -> tuple[tuple[str, ...], str, tuple[str, ...]]:
    """Return (macro_tags, risk_bias, affected_markets)."""
    n = _norm_name(event.event_name)
    tags: set[str] = set()
    bias = "neutral"

    if any(k in n for k in ("cpi", "core cpi", "pce", "inflation")):
        tags.add("inflationary")
        tags.add("risk_off")
        bias = "caution_liquidity"
    if any(k in n for k in ("ppi", "producer price")):
        tags.add("inflationary")
        bias = bias or "caution_liquidity"
    if "gdp" in n:
        tags.add("growth_positive")
        if event.surprise is not None and event.surprise < 0:
            tags.add("growth_negative")
    if any(k in n for k in ("pmi", "ism manufacturing", "services pmi")):
        tags.add("growth_positive")
        if event.surprise is not None and event.surprise < 0:
            tags.add("growth_negative")
    if any(k in n for k in ("nonfarm", "nfp", "payrolls", "employment")):
        tags.add("growth_positive")
        tags.add("risk_on")
    if "unemployment" in n and "rate" in n:
        tags.add("growth_negative")
        if event.surprise is not None and event.surprise < 0:
            tags.add("risk_off")
    if any(k in n for k in ("fomc", "fed chair", "powell", "federal reserve")):
        tags.add("risk_off")
        bias = "policy_uncertainty"
    if any(k in n for k in ("interest rate", "rate decision", "ecb", "boe", "boj")):
        tags.add("risk_off")
        bias = "policy_uncertainty"
    if any(k in n for k in ("speech", "testimony", "press conference")):
        tags.add("risk_off")
        bias = "communication_risk"

    if not tags:
        tags.add("risk_on")

    tags = {t for t in tags if t in _TAG_SET}
    if not tags:
        tags.add("risk_on")

    mkts = _markets_for_country(event.country)
    return (tuple(sorted(tags)), bias, mkts)


def score_calendar_events(events: list[CalendarEventRecord]) -> list[CalendarEventRecord]:
    """Attach macro_tags, risk_bias, and affected_markets to each event."""
    scored: list[CalendarEventRecord] = []
    for e in events:
        tags, bias, mkts = _tags_and_bias(e)
        scored.append(
            replace(
                e,
                macro_tags=tags,
                risk_bias=bias,
                affected_markets=mkts,
            )
        )
    return scored


def score_calendar_dataframe(df):
    """Optional pandas helper: expects parser columns; returns new DataFrame with tags split."""
    import pandas as pd

    if df is None or df.empty:
        return pd.DataFrame()
    records: list[CalendarEventRecord] = []
    for _, row in df.iterrows():
        am = str(row.get("affected_markets") or "")
        mt = str(row.get("macro_tags") or "")
        records.append(
            CalendarEventRecord(
                event_name=str(row.get("event_name") or ""),
                country=str(row.get("country") or ""),
                importance=str(row.get("importance") or ""),
                forecast=row.get("forecast"),
                actual=row.get("actual"),
                previous=row.get("previous"),
                surprise=row.get("surprise"),
                risk_bias=str(row.get("risk_bias") or "unscored"),
                affected_markets=tuple(x.strip() for x in am.split(",") if x.strip()),
                event_timestamp=pd.to_datetime(row.get("event_timestamp"), utc=True).to_pydatetime(),
                source=str(row.get("source") or ""),
                macro_tags=tuple(x.strip() for x in mt.split(",") if x.strip()),
            )
        )
    scored = score_calendar_events(records)
    from hptl.news.calendar_parser import records_to_dataframe

    return records_to_dataframe(scored)
