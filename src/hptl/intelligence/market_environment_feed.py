"""Build ``market_environment_feed`` payloads for confluence rows (trust-first, no invented text)."""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from typing import Any

from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, instrument_profile, load_catalyst_config
from hptl.intelligence.event_adapter import fetch_normalized_events
from hptl.news.economic_calendar_engine import build_calendar_views, fetch_enriched_calendar
from hptl.intelligence.finnhub_news_adapter import fetch_finnhub_headlines
from hptl.intelligence.macro_event_filter import is_macro_calendar_event
from hptl.intelligence.weather_adapter import WEATHER_ENABLED_MARKETS, fetch_weather_summaries
from hptl.news.economic_calendar_provider import finnhub_api_key

IMPACT_LABELS = frozenset({"supportive", "neutral", "contradicting", "risk_event", "unknown"})
IMPORTANCE_LEVELS = frozenset({"low", "medium", "high"})

# Dashboard caps — compact, no article spam.
MAX_NEWS_PER_MARKET = 3
MAX_EVENTS_PER_MARKET = 6
MAX_WEATHER_PER_MARKET = 2
MAX_RELATED_PER_MARKET = 2


@dataclass
class FeedBuildCache:
    """Shared upstream pulls across instruments."""

    catalyst_cfg: dict[str, Any]
    events_bundle: dict[str, Any] | None = None
    finnhub_articles: list[dict[str, Any]] | None = None
    weather_by_market: dict[str, list[dict[str, Any]]] = field(default_factory=dict)
    headlines_by_market: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _importance_from_rank(rank: int) -> str:
    if rank >= 3:
        return "high"
    if rank == 2:
        return "medium"
    if rank == 1:
        return "low"
    return "low"


def _risk_level_from_rank(rank: int) -> str:
    if rank >= 3:
        return "elevated"
    if rank == 2:
        return "moderate"
    return "low"


def _classification_from_sentiment(score: float | None) -> str:
    if score is None:
        return "neutral"
    if score > 0:
        return "supportive"
    if score < 0:
        return "contradicting"
    return "neutral"


def _impact_label_from_sentiment(score: float | None, *, is_event: bool, importance_rank: int = 0) -> str:
    if is_event and importance_rank >= 3:
        return "risk_event"
    if score is None:
        return "unknown"
    if score > 0:
        return "supportive"
    if score < 0:
        return "contradicting"
    return "neutral"


def _confidence_from_importance(rank: int) -> str:
    if rank >= 3:
        return "high"
    if rank == 2:
        return "medium"
    return "low"


def _event_explanation(event: dict[str, Any]) -> str:
    interp = str(event.get("interpretation") or "").strip()
    if interp:
        return interp
    country = str(event.get("country") or "").strip()
    ts = str(event.get("event_timestamp") or event.get("date") or "").strip()
    parts = []
    if country:
        parts.append(country)
    act = event.get("actual")
    fc = event.get("forecast")
    if act is not None and fc is not None:
        parts.append(f"actual {act} vs forecast {fc}")
    elif act is not None:
        parts.append(f"actual {act}")
    if ts:
        parts.append(f"scheduled {ts[:16].replace('T', ' ')} UTC")
    return " · ".join(parts) if parts else "Macro calendar entry."


def _news_explanation(headline: dict[str, Any]) -> str:
    tags = headline.get("catalyst_tags") or []
    if tags:
        return f"Matched catalyst tags: {', '.join(str(t) for t in tags[:4])}."
    return "Headline matched instrument keyword filters."


def _make_record(
    *,
    market: str,
    provider: str,
    source: str,
    category: str,
    title: str,
    summary: str,
    importance: str,
    impact_label: str,
    event_time: str | None,
    fetched_at: str,
    url: str | None = None,
    raw_payload: Any = None,
) -> dict[str, Any]:
    imp = importance if importance in IMPORTANCE_LEVELS else "low"
    impact = impact_label if impact_label in IMPACT_LABELS else "unknown"
    return {
        "market": market,
        "provider": provider,
        "source": source,
        "category": category,
        "title": title,
        "summary": summary,
        "importance": imp,
        "impact_label": impact,
        "event_time": event_time,
        "fetched_at": fetched_at,
        "url": url,
        "raw_payload": raw_payload,
    }


def _record_to_news_item(rec: dict[str, Any], market: str) -> dict[str, Any]:
    classification = rec["impact_label"]
    if classification == "risk_event":
        classification = "risk"
    elif classification not in ("supportive", "neutral", "contradicting", "risk"):
        classification = "neutral"
    conf = "high" if rec["importance"] == "high" else "medium" if rec["importance"] == "medium" else "low"
    return {
        "source": rec["source"],
        "published_at": rec["event_time"] or rec["fetched_at"],
        "fetched_at": rec["fetched_at"],
        "headline": rec["title"],
        "url": rec.get("url"),
        "related_instruments": [market],
        "classification": classification,
        "explanation": rec["summary"],
        "confidence": conf,
    }


def _record_to_event_item(rec: dict[str, Any], market: str, *, related: list[str] | None = None) -> dict[str, Any]:
    rank = 3 if rec["importance"] == "high" else 2 if rec["importance"] == "medium" else 1
    classification = "risk" if rec["impact_label"] == "risk_event" else "neutral"
    conf = _confidence_from_importance(rank)
    inst = related if related else [market]
    return {
        "source": rec["source"],
        "published_at": rec["event_time"] or rec["fetched_at"],
        "fetched_at": rec["fetched_at"],
        "headline": rec["title"],
        "url": rec.get("url"),
        "related_instruments": inst,
        "classification": classification,
        "explanation": rec["summary"],
        "confidence": conf,
        "risk_level": _risk_level_from_rank(rank),
    }


def _filter_events_for_market(events: list[dict[str, Any]], market: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for ev in events:
        name = str(ev.get("event_name") or "")
        if not is_macro_calendar_event(name):
            continue
        aff = ev.get("affected_markets") or []
        if market not in aff:
            continue
        out.append(ev)
    out.sort(key=lambda e: (e.get("event_timestamp") or "", e.get("event_name") or ""))
    return out


def _events_to_records(market: str, events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    fetched_at = _now_iso()
    records: list[dict[str, Any]] = []
    for ev in events[:MAX_EVENTS_PER_MARKET]:
        rank = int(ev.get("importance_rank") or 0)
        provider = "trading_economics" if ev.get("source") == "trading_economics" else "finnhub"
        source_label = "Trading Economics" if provider == "trading_economics" else "Finnhub calendar"
        title = str(ev.get("event_name") or "").strip()
        if ev.get("released") and ev.get("actual") is not None:
            d_fc = ev.get("direction_vs_forecast")
            if d_fc:
                title = f"{title} ({d_fc} vs forecast)"
        records.append(
            _make_record(
                market=market,
                provider=provider,
                source=source_label,
                category="macro_event",
                title=title,
                summary=_event_explanation(ev),
                importance=_importance_from_rank(rank),
                impact_label=_impact_label_from_sentiment(None, is_event=True, importance_rank=rank),
                event_time=str(ev.get("event_timestamp") or ""),
                fetched_at=fetched_at,
                url=ev.get("source_url"),
                raw_payload=ev,
            )
        )
    return records


def _headlines_to_records(market: str, headlines: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for h in headlines[:MAX_NEWS_PER_MARKET]:
        title = str(h.get("title") or "").strip()
        if not title:
            continue
        score = h.get("sentiment_score")
        try:
            score_f = float(score) if score is not None else None
        except (TypeError, ValueError):
            score_f = None
        fetched_at = str(h.get("fetched_at") or _now_iso())
        published = str(h.get("date") or fetched_at)
        records.append(
            _make_record(
                market=market,
                provider="finnhub",
                source="Finnhub",
                category="headline",
                title=title,
                summary=_news_explanation(h),
                importance="medium" if h.get("catalyst_tags") else "low",
                impact_label=_impact_label_from_sentiment(score_f, is_event=False),
                event_time=published,
                fetched_at=fetched_at,
                url=str(h.get("url") or "") or None,
                raw_payload=h.get("raw_payload"),
            )
        )
    return records


def _weather_to_records(market: str, weather_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for w in weather_rows[:MAX_WEATHER_PER_MARKET]:
        summary = str(w.get("summary") or "").strip()
        if not summary:
            continue
        fetched_at = str(w.get("fetched_at") or _now_iso())
        imp = str(w.get("importance") or "medium").strip().lower()
        if imp not in IMPORTANCE_LEVELS:
            imp = "medium"
        tags = w.get("risk_tags") if isinstance(w.get("risk_tags"), list) else []
        title = str(w.get("region") or "Weather")
        if tags:
            title = f"{title} ({tags[0]})"
        impact = "risk_event" if imp == "high" and tags else "unknown"
        records.append(
            _make_record(
                market=market,
                provider="openweather",
                source=str(w.get("source") or "OpenWeather"),
                category="weather",
                title=title,
                summary=summary,
                importance=imp,
                impact_label=impact,
                event_time=fetched_at,
                fetched_at=fetched_at,
                raw_payload={"signals": w.get("signals"), **(w.get("raw_payload") or {})},
            )
        )
    return records


def _related_records(
    market: str,
    *,
    all_records_by_market: dict[str, list[dict[str, Any]]],
    catalyst_cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    prof = instrument_profile(catalyst_cfg, market) or {}
    related = prof.get("related_markets")
    if not isinstance(related, list):
        return []
    out: list[dict[str, Any]] = []
    for peer in related:
        peer = str(peer).strip()
        if not peer or peer == market:
            continue
        peer_recs = all_records_by_market.get(peer) or []
        for rec in peer_recs:
            if rec.get("category") != "macro_event":
                continue
            if rec.get("importance") not in ("high", "medium"):
                continue
            clone = dict(rec)
            clone["category"] = "related_market"
            clone["market"] = market
            clone["summary"] = f"{peer}: {rec['summary']}"
            out.append(clone)
            if len(out) >= MAX_RELATED_PER_MARKET:
                return out
    return out


def _ensure_cache(cache: FeedBuildCache) -> None:
    today = date.today()
    window_end = today + timedelta(days=14)
    if cache.events_bundle is None:
        bundle = fetch_enriched_calendar(start=today, end=window_end, catalyst_cfg=cache.catalyst_cfg)
        inst = cache.catalyst_cfg.get("instruments")
        market_ids = list(inst.keys()) if isinstance(inst, dict) else []
        views = build_calendar_views(bundle, markets=market_ids)
        cache.events_bundle = {**bundle, **views}
    if cache.finnhub_articles is None and finnhub_api_key():
        from hptl.intelligence.finnhub_news_adapter import _fetch_finnhub_category_news

        cache.finnhub_articles = _fetch_finnhub_category_news("general")


def build_market_environment_feed(
    market: str,
    *,
    catalyst_cfg: dict[str, Any] | None = None,
    cache: FeedBuildCache | None = None,
) -> dict[str, Any]:
    """Per-instrument feed: unified ``records`` plus legacy ``news_items`` / ``event_items``."""
    cfg = catalyst_cfg or load_catalyst_config()
    c = cache or FeedBuildCache(catalyst_cfg=cfg)
    c.catalyst_cfg = cfg
    _ensure_cache(c)

    events_bundle = c.events_bundle or {"events": []}
    inst_events = _filter_events_for_market(list(events_bundle.get("events") or []), market)

    if market not in c.headlines_by_market:
        rows, _ = fetch_finnhub_headlines(
            instrument=market,
            catalyst_cfg=cfg,
            cached_articles=c.finnhub_articles,
        )
        c.headlines_by_market[market] = rows

    if market in WEATHER_ENABLED_MARKETS and market not in c.weather_by_market:
        wrows, _ = fetch_weather_summaries(market)
        c.weather_by_market[market] = wrows

    records: list[dict[str, Any]] = []
    records.extend(_events_to_records(market, inst_events))
    records.extend(_headlines_to_records(market, c.headlines_by_market.get(market) or []))
    records.extend(_weather_to_records(market, c.weather_by_market.get(market) or []))

    news_items = [_record_to_news_item(r, market) for r in records if r["category"] in ("headline", "related_market")]
    event_items = [_record_to_event_item(r, market) for r in records if r["category"] in ("macro_event", "weather")]

    fh_active = bool(finnhub_api_key())
    sources_status: dict[str, Any] = {
        "calendar": events_bundle.get("sources") if isinstance(events_bundle, dict) else {},
        "finnhub_news": "finnhub" if fh_active else SOURCE_NOT_CONFIGURED,
        "finnhub_news_active": fh_active,
        "weather": "openweather" if (c.weather_by_market.get(market) or []) else SOURCE_NOT_CONFIGURED,
    }

    cal = events_bundle if isinstance(events_bundle, dict) else {}
    inst_cal_events = _filter_events_for_market(list(cal.get("events") or []), market)
    upcoming = [e for e in inst_cal_events if e.get("actual") is None and int(e.get("importance_rank") or 0) >= 3][:8]
    released = sorted(
        [e for e in inst_cal_events if e.get("released")],
        key=lambda e: str(e.get("event_timestamp") or ""),
        reverse=True,
    )[:8]

    calendar_catalysts = {
        "wired": bool(cal.get("wired")),
        "message": cal.get("message") or ("Not wired — add API key" if not cal.get("wired") else ""),
        "provider": cal.get("provider"),
        "upcoming_high_impact": upcoming,
        "latest_released": released,
        "event_risk": (events_bundle.get("event_risk_by_market") or {}).get(market),
    }

    return {
        "live_bundle_last_checked_at": _now_iso(),
        "records": records,
        "news_items": news_items[:MAX_NEWS_PER_MARKET],
        "event_items": event_items[:MAX_EVENTS_PER_MARKET + MAX_WEATHER_PER_MARKET],
        "weather_feed_connected": bool(c.weather_by_market.get(market)),
        "weather_snapshot": (c.weather_by_market.get(market) or [])[:MAX_WEATHER_PER_MARKET],
        "sources_status": sources_status,
        "calendar_catalysts": calendar_catalysts,
    }


def build_all_market_environment_feeds(
    markets: list[str],
    *,
    catalyst_cfg: dict[str, Any] | None = None,
    skip_network: bool = False,
) -> dict[str, dict[str, Any]]:
    """Build feeds for all markets; attaches related-market catalyst lines after primary pass."""
    if skip_network or os.getenv("HPTL_SKIP_LIVE_FEEDS", "").strip() in ("1", "true", "yes"):
        return {m: {} for m in markets}

    cfg = catalyst_cfg or load_catalyst_config()
    cache = FeedBuildCache(catalyst_cfg=cfg)
    primary: dict[str, dict[str, Any]] = {}
    records_by_market: dict[str, list[dict[str, Any]]] = {}

    for market in markets:
        feed = build_market_environment_feed(market, catalyst_cfg=cfg, cache=cache)
        primary[market] = feed
        records_by_market[market] = list(feed.get("records") or [])

    for market in markets:
        related = _related_records(market, all_records_by_market=records_by_market, catalyst_cfg=cfg)
        if not related:
            continue
        feed = primary[market]
        merged_records = list(feed.get("records") or []) + related
        feed["records"] = merged_records
        for rec in related:
            if rec.get("category") == "headline":
                feed.setdefault("news_items", []).append(_record_to_news_item(rec, market))
            else:
                feed.setdefault("event_items", []).append(
                    _record_to_event_item(rec, market, related=[str(rec.get("title") or "")[:40]])
                )
        feed["news_items"] = (feed.get("news_items") or [])[:MAX_NEWS_PER_MARKET]
        feed["event_items"] = (feed.get("event_items") or [])[: MAX_EVENTS_PER_MARKET + MAX_WEATHER_PER_MARKET]

    return primary


def attach_feeds_to_latest_records(
    records: list[dict[str, Any]],
    *,
    markets: list[str] | None = None,
) -> None:
    """Mutate confluence ``records`` — live feed only on the latest COT week."""
    if not records:
        return
    latest_date = max(str(r.get("date") or "") for r in records)
    target_markets = markets or sorted({str(r.get("market") or "") for r in records if r.get("market")})
    if not os.getenv("HPTL_SKIP_LIVE_FEEDS", "").strip().lower() in ("1", "true", "yes"):
        try:
            from hptl.news.economic_calendar_engine import write_economic_calendar_export

            write_economic_calendar_export(markets=target_markets)
        except Exception:
            pass
        try:
            from hptl.intelligence.weather_context_export import write_weather_context_export

            write_weather_context_export()
        except Exception:
            pass
    feeds = build_all_market_environment_feeds(target_markets)
    for rec in records:
        if str(rec.get("date") or "") != latest_date:
            rec["market_environment_feed"] = {}
            continue
        m = str(rec.get("market") or "")
        rec["market_environment_feed"] = feeds.get(m) or {}
