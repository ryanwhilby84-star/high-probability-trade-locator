"""Fetch article/headline lists from the GDELT 2.0 DOC API (raw JSON).

This is ingestion only; interpretation belongs in ``headline_classifier`` and
``narrative_engine``. No trading logic.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests

from hptl.config import get_settings


@dataclass(frozen=True)
class GdeltArticle:
    url: str
    title: str
    seendate: str
    domain: str
    language: str
    raw: dict[str, Any]


def fetch_gdelt_doc_list(
    query: str,
    *,
    mode: str = "artlist",
    maxrecords: int = 50,
    format_: str = "json",
) -> list[GdeltArticle]:
    """Query GDELT DOC API v2; returns empty list on HTTP/parse errors."""
    settings = get_settings()
    base = "https://api.gdeltproject.org/api/v2/doc/doc"
    params = {
        "query": query,
        "mode": mode,
        "maxrecords": str(maxrecords),
        "format": format_,
    }
    url = f"{base}?{urlencode(params)}"
    try:
        r = requests.get(url, timeout=settings.request_timeout_seconds)
        r.raise_for_status()
    except requests.RequestException:
        return []
    try:
        payload = r.json()
    except ValueError:
        return []
    articles = payload.get("articles") if isinstance(payload, dict) else None
    if not isinstance(articles, list):
        return []
    out: list[GdeltArticle] = []
    for a in articles:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        if not title:
            continue
        out.append(
            GdeltArticle(
                url=str(a.get("url") or ""),
                title=title,
                seendate=str(a.get("seendate") or ""),
                domain=str(a.get("domain") or ""),
                language=str(a.get("language") or ""),
                raw=a,
            )
        )
    return out


def default_macro_query_window() -> str:
    """Reasonable default macro/geopolitical query string for GDELT."""
    return "(inflation OR recession OR FOMC OR fed OR ECB OR sanctions OR oil supply OR geopolitical)"
