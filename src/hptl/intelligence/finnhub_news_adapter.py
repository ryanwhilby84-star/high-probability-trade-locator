"""Finnhub market headlines — keyword-filtered per instrument (no article bodies)."""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from hptl.config import get_settings
from hptl.news.economic_calendar_provider import finnhub_api_key
from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, instrument_profile
from hptl.intelligence.news_adapter import _lexicon_sentiment, _tag_headline


def _fetch_finnhub_category_news(category: str = "general") -> list[dict[str, Any]]:
    token = finnhub_api_key()
    if not token:
        return []
    settings = get_settings()
    params = {"category": category, "token": token}
    url = f"https://finnhub.io/api/v1/news?{urlencode(params)}"
    try:
        req = Request(url, headers={"User-Agent": "hptl-intelligence/1.0"})
        with urlopen(req, timeout=settings.request_timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8", errors="replace"))
    except OSError:
        return []
    except ValueError:
        return []
    return [x for x in data if isinstance(x, dict)] if isinstance(data, list) else []


def _headline_relevant(title: str, *, instrument: str, catalyst_cfg: dict[str, Any]) -> bool:
    inst_tags, cat_tags = _tag_headline(title, instrument=instrument, catalyst_cfg=catalyst_cfg)
    return bool(inst_tags or cat_tags)


def fetch_finnhub_headlines(
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
    cached_articles: list[dict[str, Any]] | None = None,
    max_items: int = 8,
) -> tuple[list[dict[str, Any]], str]:
    """Instrument-scoped Finnhub headlines; requires ``FINNHUB_API_KEY``."""
    if not finnhub_api_key():
        return [], SOURCE_NOT_CONFIGURED
    prof = instrument_profile(catalyst_cfg, instrument)
    if not prof:
        return [], SOURCE_NOT_CONFIGURED

    articles = cached_articles if cached_articles is not None else _fetch_finnhub_category_news("general")
    fetched_at = datetime.now(timezone.utc).isoformat()
    lex = {}
    g = catalyst_cfg.get("global")
    if isinstance(g, dict) and isinstance(g.get("title_sentiment_lexicon"), dict):
        lex = g["title_sentiment_lexicon"]

    rows: list[dict[str, Any]] = []
    for art in articles:
        title = str(art.get("headline") or art.get("title") or "").strip()
        if not title or not _headline_relevant(title, instrument=instrument, catalyst_cfg=catalyst_cfg):
            continue
        ts = art.get("datetime")
        published = ""
        if ts is not None:
            try:
                published = datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat()
            except (TypeError, ValueError, OSError):
                published = ""
        if not published:
            published = fetched_at
        inst_tags, cat_tags = _tag_headline(title, instrument=instrument, catalyst_cfg=catalyst_cfg)
        rows.append(
            {
                "date": published,
                "source": "finnhub",
                "title": title,
                "url": str(art.get("url") or ""),
                "instrument_tags": inst_tags,
                "catalyst_tags": sorted(set(cat_tags)),
                "sentiment_score": _lexicon_sentiment(title, lex),
                "fetched_at": fetched_at,
                "raw_payload": {"id": art.get("id"), "category": art.get("category"), "related": art.get("related")},
            }
        )
        if len(rows) >= max_items:
            break
    return rows, "finnhub" if rows else "finnhub:no_match"
