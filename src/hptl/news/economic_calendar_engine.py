"""Economic calendar ingestion, surprise, interpretation, and instrument mapping."""
from __future__ import annotations

import json
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, load_catalyst_config
from hptl.intelligence.macro_event_filter import is_macro_calendar_event
from hptl.news.calendar_downloader import download_calendar_window
from hptl.news.calendar_interpretation import interpret_calendar_event
from hptl.news.calendar_parser import parse_finnhub_rows, parse_trading_economics_rows
from hptl.news.calendar_scoring import score_calendar_events
from hptl.news.calendar_surprise import compute_surprise_fields
from hptl.news.contracts import CalendarEventRecord
from hptl.news.economic_calendar_provider import (
    live_feeds_disabled,
    resolve_economic_calendar_provider,
)

# First-order US macro prints touch most tracked USD-sensitive contracts.
_US_TRACKED = (
    "NASDAQ / NQ",
    "S&P 500 / ES",
    "Dow / YM",
    "Gold",
    "Silver",
    "Crude Oil / CL",
    "Natural Gas / NG",
    "Euro FX / 6E",
    "British Pound / 6B",
    "Japanese Yen / 6J",
    "Copper / HG",
)

_COUNTRY_TRACKED: dict[str, tuple[str, ...]] = {
    "US": _US_TRACKED,
    "USA": _US_TRACKED,
    "UNITED STATES": _US_TRACKED,
    "EU": ("Euro FX / 6E", "Gold", "S&P 500 / ES"),
    "EMU": ("Euro FX / 6E", "Gold"),
    "GB": ("British Pound / 6B", "Gold"),
    "UK": ("British Pound / 6B", "Gold"),
    "JP": ("Japanese Yen / 6J", "Gold", "NASDAQ / NQ"),
    "JAPAN": ("Japanese Yen / 6J", "Gold"),
    "CN": ("Copper / HG", "Crude Oil / CL", "Soybeans"),
    "CHINA": ("Copper / HG", "Crude Oil / CL", "Soybeans"),
}


def _importance_rank(importance: str) -> int:
    s = (importance or "").strip().lower()
    if s in ("high", "3", "star star star", "***"):
        return 3
    if s in ("medium", "2", "star star", "**"):
        return 2
    if s in ("low", "1", "star", "*"):
        return 1
    return 0


def _currency_from_country(country: str) -> str:
    c = country.upper()
    return {
        "US": "USD",
        "USA": "USD",
        "UNITED STATES": "USD",
        "EU": "EUR",
        "EMU": "EUR",
        "GB": "GBP",
        "UK": "GBP",
        "JP": "JPY",
        "JAPAN": "JPY",
        "CN": "CNY",
        "CHINA": "CNY",
    }.get(c, "")


def _unit_from_raw(raw: dict[str, Any], source: str) -> str:
    if source == "finnhub":
        return str(raw.get("unit") or "").strip()
    return str(raw.get("Unit") or raw.get("unit") or "").strip()


def _source_url(raw: dict[str, Any], source: str) -> str | None:
    for key in ("url", "URL", "source_url", "SourceURL"):
        v = raw.get(key)
        if v:
            return str(v).strip()
    return None


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


def affected_markets_for_event(event_name: str, *, catalyst_cfg: dict[str, Any]) -> list[str]:
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


def _tracked_markets_for_event(event_name: str, country: str, *, catalyst_cfg: dict[str, Any]) -> list[str]:
    from_kw = affected_markets_for_event(event_name, catalyst_cfg=catalyst_cfg)
    country_mkts = list(_COUNTRY_TRACKED.get(country.upper().strip(), ()))
    n = event_name.lower()
    extra: list[str] = []
    if "crude" in n and "inventor" in n:
        extra.append("Crude Oil / CL")
    if "natural gas" in n and "storage" in n:
        extra.append("Natural Gas / NG")
    if any(k in n for k in ("corn", "wheat", "soybean", "usda", "acreage")):
        extra.extend(["Corn", "Wheat", "Soybeans"])
    if "coffee" in n:
        extra.append("Coffee")
    if "cocoa" in n:
        extra.append("Cocoa")
    merged = sorted(set(from_kw) | set(country_mkts) | set(extra))
    return merged


def _record_to_event_dict(
    rec: CalendarEventRecord,
    *,
    catalyst_cfg: dict[str, Any],
) -> dict[str, Any]:
    ts = rec.event_timestamp
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    surprise = compute_surprise_fields(actual=rec.actual, forecast=rec.forecast, previous=rec.previous)
    aff = _tracked_markets_for_event(rec.event_name, rec.country, catalyst_cfg=catalyst_cfg)
    row: dict[str, Any] = {
        "event_name": rec.event_name,
        "country": rec.country,
        "currency": _currency_from_country(rec.country),
        "event_timestamp": ts.isoformat(),
        "date": ts.date().isoformat(),
        "importance": rec.importance,
        "importance_rank": _importance_rank(rec.importance),
        "forecast": rec.forecast,
        "actual": rec.actual,
        "previous": rec.previous,
        "unit": _unit_from_raw(rec.raw, rec.source),
        "source": rec.source,
        "source_url": _source_url(rec.raw, rec.source),
        "affected_markets": aff,
        "macro_tags": list(rec.macro_tags),
        "risk_bias": rec.risk_bias,
        "released": rec.actual is not None,
        **surprise,
    }
    row["interpretation"] = interpret_calendar_event(row)
    row["impact_label"] = _impact_label(row)
    return row


def _impact_label(ev: dict[str, Any]) -> str:
    rank = int(ev.get("importance_rank") or 0)
    if rank >= 3:
        return "high"
    if rank >= 2:
        return "medium"
    return "low"


def fetch_enriched_calendar(
    *,
    start: date | None = None,
    end: date | None = None,
    catalyst_cfg: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Pull, normalize, score, and interpret calendar rows from configured provider(s)."""
    cfg = catalyst_cfg or load_catalyst_config()
    today = date.today()
    start_d = start or (today - timedelta(days=7))
    end_d = end or (today + timedelta(days=14))
    provider = resolve_economic_calendar_provider()

    if provider == "none":
        reason = "Not wired — add API key"
        if live_feeds_disabled():
            reason = "Not wired — HPTL_SKIP_LIVE_FEEDS is set"
        return {
            "wired": False,
            "message": reason,
            "provider": provider,
            "window_start": start_d.isoformat(),
            "window_end": end_d.isoformat(),
            "fetched_at": datetime.now(timezone.utc).isoformat(),
            "events": [],
            "sources": {},
            "errors": [],
        }

    bundle = download_calendar_window(start_d, end_d)
    records: list[CalendarEventRecord] = []
    if provider in ("finnhub", "both"):
        records.extend(parse_finnhub_rows(bundle.finnhub or []))
    if provider in ("trading_economics", "both"):
        records.extend(parse_trading_economics_rows(bundle.trading_economics or []))

    scored = score_calendar_events(records)
    events = [_record_to_event_dict(r, catalyst_cfg=cfg) for r in scored]
    events = [e for e in events if is_macro_calendar_event(str(e.get("event_name") or ""))]
    events = _dedupe_events(events)
    events.sort(key=lambda e: (e.get("event_timestamp") or "", e.get("event_name") or ""))

    return {
        "wired": True,
        "message": "",
        "provider": provider,
        "window_start": start_d.isoformat(),
        "window_end": end_d.isoformat(),
        "fetched_at": bundle.fetched_at.isoformat(),
        "events": events,
        "sources": {
            "finnhub": {"status": "finnhub" if provider in ("finnhub", "both") else SOURCE_NOT_CONFIGURED},
            "trading_economics": {
                "status": "trading_economics" if provider in ("trading_economics", "both") else SOURCE_NOT_CONFIGURED
            },
        },
        "errors": list(bundle.errors),
    }


def _dedupe_events(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Prefer Trading Economics row when both providers return the same print."""
    by_key: dict[tuple[str, str, str], dict[str, Any]] = {}
    for e in events:
        key = (
            str(e.get("date") or "")[:10],
            str(e.get("event_name") or "").lower().strip(),
            str(e.get("country") or "").upper().strip(),
        )
        cur = by_key.get(key)
        if cur is None:
            by_key[key] = e
            continue
        if cur.get("source") != "trading_economics" and e.get("source") == "trading_economics":
            by_key[key] = e
    return list(by_key.values())


def _has_release_surprise(ev: dict[str, Any]) -> bool:
    if not ev.get("released") or ev.get("actual") is None:
        return False
    direction = ev.get("direction_vs_forecast")
    if direction not in ("beat", "miss"):
        return False
    mag = str(ev.get("magnitude_vs_forecast") or "")
    rank = int(ev.get("importance_rank") or 0)
    return rank >= 3 or mag in ("medium", "large")


def _event_risk_for_market(events: list[dict[str, Any]], market: str, *, today: date) -> str:
    if not events:
        return "clean"
    week_end = today + timedelta(days=7)
    lookback = today - timedelta(days=2)
    high_today = False
    high_week = False
    released_surprise = False
    for ev in events:
        aff = ev.get("affected_markets") or []
        if market not in aff:
            continue
        try:
            ed = date.fromisoformat(str(ev.get("date") or "")[:10])
        except ValueError:
            continue
        rank = int(ev.get("importance_rank") or 0)
        if ev.get("released") and lookback <= ed <= today and _has_release_surprise(ev):
            released_surprise = True
        if rank < 3:
            continue
        if ed < today:
            continue
        if ed == today:
            high_today = True
        if today <= ed <= week_end:
            high_week = True
    if high_today:
        return "high_today"
    if released_surprise:
        return "released_surprise"
    if high_week:
        return "high_this_week"
    return "clean"


def build_calendar_views(bundle: dict[str, Any], *, markets: list[str] | None = None) -> dict[str, Any]:
    """Split upcoming vs released + per-market event risk badges."""
    events: list[dict[str, Any]] = list(bundle.get("events") or [])
    today = date.today()
    now = datetime.now(timezone.utc)

    upcoming: list[dict[str, Any]] = []
    released: list[dict[str, Any]] = []
    for ev in events:
        ts_s = str(ev.get("event_timestamp") or "")
        try:
            ts = datetime.fromisoformat(ts_s.replace("Z", "+00:00"))
        except ValueError:
            ts = now
        is_future = ev.get("actual") is None and ts.date() >= today
        if is_future:
            upcoming.append(ev)
        elif ev.get("released"):
            released.append(ev)
        elif ts.date() >= today:
            upcoming.append(ev)

    upcoming.sort(key=lambda e: (e.get("event_timestamp") or "", e.get("importance_rank") or 0), reverse=False)
    released.sort(key=lambda e: (e.get("event_timestamp") or ""), reverse=True)

    high_upcoming = [e for e in upcoming if int(e.get("importance_rank") or 0) >= 3][:12]
    high_released = [e for e in released if int(e.get("importance_rank") or 0) >= 2][:12]

    risk_by_market: dict[str, str] = {}
    if markets:
        for m in markets:
            risk_by_market[m] = _event_risk_for_market(events, m, today=today)

    return {
        "upcoming_high_impact": high_upcoming,
        "latest_released": high_released,
        "event_risk_by_market": risk_by_market,
    }


def default_export_path() -> Path:
    return PROJECT_ROOT / "web-dashboard" / "public" / "data" / "economic_calendar_latest.json"


def write_economic_calendar_export(
    path: Path | None = None,
    *,
    markets: list[str] | None = None,
    catalyst_cfg: dict[str, Any] | None = None,
) -> Path:
    bundle = fetch_enriched_calendar(catalyst_cfg=catalyst_cfg)
    views = build_calendar_views(bundle, markets=markets)
    if not bundle.get("wired") and markets:
        views["event_risk_by_market"] = {m: "not_wired" for m in markets}
    doc = {**bundle, **views}
    p = path or default_export_path()
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return p
