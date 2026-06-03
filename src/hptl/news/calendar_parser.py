"""Parse raw calendar payloads into ``CalendarEventRecord`` rows."""
from __future__ import annotations

import math
from datetime import datetime, timezone
from typing import Any, Iterable

import pandas as pd

from hptl.news.contracts import CalendarEventRecord


def _to_float(x: Any) -> float | None:
    if x is None or x == "":
        return None
    try:
        v = float(x)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(v):
        return None
    return v


def _parse_ts(value: Any) -> datetime:
    ts = pd.to_datetime(value, errors="coerce", utc=True)
    if pd.isna(ts):
        return datetime.now(timezone.utc)
    return ts.to_pydatetime()


def parse_finnhub_rows(rows: Iterable[dict[str, Any]]) -> list[CalendarEventRecord]:
    out: list[CalendarEventRecord] = []
    for r in rows:
        name = str(r.get("event") or r.get("name") or "").strip()
        if not name:
            continue
        country = str(r.get("country") or "").strip()
        forecast = _to_float(r.get("estimate"))
        actual = _to_float(r.get("actual"))
        previous = _to_float(r.get("previous"))
        surprise = None
        if actual is not None and forecast is not None:
            surprise = actual - forecast
        ts = _parse_ts(r.get("time") or r.get("date"))
        out.append(
            CalendarEventRecord(
                event_name=name,
                country=country,
                importance=str(r.get("impact") or r.get("importance") or "unknown"),
                forecast=forecast,
                actual=actual,
                previous=previous,
                surprise=surprise,
                risk_bias="unscored",
                affected_markets=(),
                event_timestamp=ts,
                source="finnhub",
                macro_tags=(),
                raw=dict(r),
            )
        )
    return out


def parse_trading_economics_rows(rows: Iterable[dict[str, Any]]) -> list[CalendarEventRecord]:
    out: list[CalendarEventRecord] = []
    for r in rows:
        name = str(r.get("Event") or r.get("event") or "").strip()
        if not name:
            continue
        country = str(r.get("Country") or r.get("country") or "").strip()
        forecast = _to_float(r.get("Forecast") or r.get("forecast"))
        actual = _to_float(r.get("Actual") or r.get("actual"))
        previous = _to_float(r.get("Previous") or r.get("previous"))
        surprise = None
        if actual is not None and forecast is not None:
            surprise = actual - forecast
        ts = _parse_ts(r.get("Date") or r.get("date") or r.get("Reference"))
        importance = str(r.get("Importance") or r.get("importance") or "unknown")
        out.append(
            CalendarEventRecord(
                event_name=name,
                country=country,
                importance=importance,
                forecast=forecast,
                actual=actual,
                previous=previous,
                surprise=surprise,
                risk_bias="unscored",
                affected_markets=(),
                event_timestamp=ts,
                source="trading_economics",
                macro_tags=(),
                raw=dict(r),
            )
        )
    return out


def records_to_dataframe(records: list[CalendarEventRecord]) -> pd.DataFrame:
    if not records:
        return pd.DataFrame(
            columns=[
                "event_name",
                "country",
                "importance",
                "forecast",
                "actual",
                "previous",
                "surprise",
                "risk_bias",
                "affected_markets",
                "event_timestamp",
                "source",
                "macro_tags",
            ]
        )
    rows = []
    for e in records:
        rows.append(
            {
                "event_name": e.event_name,
                "country": e.country,
                "importance": e.importance,
                "forecast": e.forecast,
                "actual": e.actual,
                "previous": e.previous,
                "surprise": e.surprise,
                "risk_bias": e.risk_bias,
                "affected_markets": ",".join(e.affected_markets),
                "event_timestamp": e.event_timestamp,
                "source": e.source,
                "macro_tags": ",".join(e.macro_tags),
            }
        )
    return pd.DataFrame(rows)
