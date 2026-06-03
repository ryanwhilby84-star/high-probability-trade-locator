"""Orchestrate catalyst config, news, calendar, impulse, and lexicon-only sentiment signals."""
from __future__ import annotations

from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

from hptl.intelligence.catalyst_loader import (
    SOURCE_NOT_CONFIGURED,
    instrument_profile,
    load_catalyst_config,
)
from hptl.intelligence.event_adapter import fetch_normalized_events
from hptl.intelligence.impulse_adapter import compute_simple_impulse
from hptl.intelligence.news_adapter import fetch_normalized_headlines
from hptl.news.contracts import SentimentInterferenceLevel


def _sentiment_interference_block(headlines: list[dict[str, Any]]) -> dict[str, Any]:
    """Lexicon-scored headlines only; no model inference."""
    vals: list[float] = []
    for h in headlines:
        s = h.get("sentiment_score")
        if s is None:
            continue
        try:
            vals.append(float(s))
        except (TypeError, ValueError):
            continue
    if not vals:
        return {
            "sentiment_interference": None,
            "emotional_flow_score": None,
            "headline_sentiment_spread": None,
            "availability": SOURCE_NOT_CONFIGURED,
            "notes": "No lexicon-scored headlines (configure title_sentiment_lexicon and matching titles).",
        }
    spread = max(vals) - min(vals) if len(vals) > 1 else abs(vals[0])
    mean_abs = sum(abs(v) for v in vals) / len(vals)
    if spread <= 0.01 and mean_abs < 0.25:
        level = SentimentInterferenceLevel.LOW.value
    elif spread <= 1.01 and mean_abs < 0.75:
        level = SentimentInterferenceLevel.MODERATE.value
    elif spread <= 1.99:
        level = SentimentInterferenceLevel.HIGH.value
    else:
        level = SentimentInterferenceLevel.EXTREME.value
    return {
        "sentiment_interference": level,
        "emotional_flow_score": round(mean_abs, 4),
        "headline_sentiment_spread": round(spread, 4),
        "scored_headline_count": len(vals),
        "availability": "title_sentiment_lexicon",
        "notes": "Derived only from catalyst_config global lexicon matches on retrieved titles.",
    }


def build_intelligence_bundle(
    instrument: str,
    *,
    catalyst_cfg: dict[str, Any] | None = None,
    catalyst_config_path: Path | str | None = None,
    event_window_start: date | None = None,
    event_window_end: date | None = None,
    include_rss: bool = True,
    include_newsapi: bool = True,
    include_gdelt: bool = True,
) -> dict[str, Any]:
    """Single payload for dashboard / API: explicit per-source status, no synthetic stories."""
    cfg = catalyst_cfg or load_catalyst_config(catalyst_config_path)
    profile = instrument_profile(cfg, instrument)

    today = date.today()
    w0 = event_window_start or today
    w1 = event_window_end or today

    news = fetch_normalized_headlines(
        instrument=instrument,
        catalyst_cfg=cfg,
        from_date=today,
        to_date=today,
        include_rss=include_rss,
        include_newsapi=include_newsapi,
        include_gdelt=include_gdelt,
    )
    events_bundle = fetch_normalized_events(start=w0, end=w1, catalyst_cfg=cfg)
    inst_events = [e for e in events_bundle["events"] if instrument in (e.get("affected_markets") or [])]
    high_importance = [e for e in inst_events if int(e.get("importance_rank") or 0) >= 2]

    impulse = compute_simple_impulse(instrument, catalyst_cfg=cfg)
    sent = _sentiment_interference_block(news.get("headlines") or [])

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instrument": instrument,
        "catalyst_profile": profile,
        "news": news,
        "economic_events": {
            **events_bundle,
            "instrument_matched_events": inst_events,
            "instrument_high_importance_events": high_importance,
        },
        "impulse": impulse,
        "sentiment_interference": sent,
        "dashboard_fields": {
            "intelligence_headline_count": len(news.get("headlines") or []),
            "intelligence_event_count": len(inst_events),
            "intelligence_high_importance_event_count": len(high_importance),
            "intelligence_impulse_score": impulse.get("impulse_score"),
            "intelligence_impulse_availability": impulse.get("availability"),
            "intelligence_sentiment_interference": sent.get("sentiment_interference"),
            "intelligence_sentiment_availability": sent.get("availability"),
            "intelligence_news_sources": news.get("sources"),
            "intelligence_calendar_sources": (events_bundle.get("sources") if isinstance(events_bundle, dict) else {}),
        },
    }
