"""News ingestion adapters (RSS, GDELT, NewsAPI) — normalized headline records only.

No fabricated headlines: empty lists mean upstream returned nothing or the source
was not configured / unreachable.
"""
from __future__ import annotations

import json
import os
import xml.etree.ElementTree as ET
from datetime import date, datetime, timezone
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from hptl.config import get_settings
from hptl.intelligence.catalyst_loader import SOURCE_NOT_CONFIGURED, instrument_profile
from hptl.news.gdelt_downloader import fetch_gdelt_doc_list


def _iso_date(d: date) -> str:
    return d.isoformat()


def _parse_rss_or_atom(xml_bytes: bytes, *, source_label: str) -> list[dict[str, str]]:
    """Minimal RSS 2.0 / Atom parser (stdlib only)."""
    out: list[dict[str, str]] = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return out

    def strip_ns(tag: str) -> str:
        return tag.rsplit("}", 1)[-1] if "}" in tag else tag

    # Atom: entry/title, entry/link[@href], entry/updated|published
    if strip_ns(root.tag).lower() == "feed":
        for entry in root.findall(".//{*}entry") + root.findall("entry"):
            title_el = entry.find("{*}title") or entry.find("title")
            link_el = entry.find("{*}link") or entry.find("link")
            date_el = entry.find("{*}updated") or entry.find("updated") or entry.find("{*}published")
            title = (title_el.text or "").strip() if title_el is not None else ""
            href = ""
            if link_el is not None:
                href = (link_el.get("href") or link_el.text or "").strip()
            when = (date_el.text or "").strip() if date_el is not None else ""
            if title:
                out.append({"title": title, "url": href, "published": when, "raw_source": source_label})
        return out

    # RSS: channel/item
    for item in root.findall(".//item"):
        title_el = item.find("title")
        link_el = item.find("link")
        pub_el = item.find("pubDate") or item.find("{*}pubDate")
        title = (title_el.text or "").strip() if title_el is not None else ""
        url = (link_el.text or "").strip() if link_el is not None else ""
        pub = (pub_el.text or "").strip() if pub_el is not None else ""
        if title:
            out.append({"title": title, "url": url, "published": pub, "raw_source": source_label})
    return out


def _fetch_url_bytes(url: str, timeout: int) -> bytes | None:
    try:
        req = Request(url, headers={"User-Agent": "hptl-intelligence/1.0"})
        with urlopen(req, timeout=timeout) as resp:
            return resp.read()
    except OSError:
        return None


def _env_rss_urls() -> list[str]:
    raw = os.getenv("HPTL_RSS_FEED_URLS", os.getenv("RSS_FEED_URLS", "")).strip()
    if not raw:
        return []
    return [u.strip() for u in raw.split(",") if u.strip()]


def _lexicon_sentiment(title: str, lex: dict[str, Any]) -> float | None:
    """Score in [-1, 1] from config lexicon only; ``None`` if lexicon absent."""
    if not isinstance(lex, dict):
        return None
    pos = lex.get("positive") if isinstance(lex.get("positive"), list) else []
    neg = lex.get("negative") if isinstance(lex.get("negative"), list) else []
    if not pos and not neg:
        return None
    t = title.lower()
    p_hit = any(str(p).lower() in t for p in pos if p)
    n_hit = any(str(n).lower() in t for n in neg if n)
    if p_hit and not n_hit:
        return 1.0
    if n_hit and not p_hit:
        return -1.0
    if p_hit and n_hit:
        return 0.0
    return None


def _tag_headline(
    title: str,
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
) -> tuple[list[str], list[str]]:
    """Tag using catalyst keyword groups for ``instrument`` only."""
    prof = instrument_profile(catalyst_cfg, instrument) or {}
    groups = prof.get("catalyst_keyword_groups")
    instrument_tags: list[str] = []
    catalyst_tags: list[str] = []
    if not isinstance(groups, dict):
        return instrument_tags, catalyst_tags
    tl = title.lower()
    for group_name, kws in groups.items():
        if not isinstance(kws, list):
            continue
        for kw in kws:
            if kw and str(kw).lower() in tl:
                catalyst_tags.append(str(group_name))
                if instrument not in instrument_tags:
                    instrument_tags.append(instrument)
                break
    return instrument_tags, catalyst_tags


def _normalize_headline(
    *,
    published: str,
    source: str,
    title: str,
    url: str,
    instrument: str,
    catalyst_cfg: dict[str, Any],
) -> dict[str, Any]:
    inst_tags, cat_tags = _tag_headline(title, instrument=instrument, catalyst_cfg=catalyst_cfg)
    lex = {}
    g = catalyst_cfg.get("global")
    if isinstance(g, dict) and isinstance(g.get("title_sentiment_lexicon"), dict):
        lex = g["title_sentiment_lexicon"]
    return {
        "date": published or datetime.now(timezone.utc).isoformat(),
        "source": source,
        "title": title,
        "url": url,
        "instrument_tags": inst_tags,
        "catalyst_tags": sorted(set(cat_tags)),
        "sentiment_score": _lexicon_sentiment(title, lex),
    }


def fetch_rss_headlines(
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
    feed_urls: list[str] | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """Fetch configured RSS/Atom feeds; returns ``([], SOURCE_NOT_CONFIGURED)`` if none."""
    urls = feed_urls if feed_urls is not None else _env_rss_urls()
    if not urls:
        return [], SOURCE_NOT_CONFIGURED
    settings = get_settings()
    rows: list[dict[str, Any]] = []
    for u in urls:
        body = _fetch_url_bytes(u, settings.request_timeout_seconds)
        if not body:
            continue
        for raw in _parse_rss_or_atom(body, source_label=u):
            rows.append(
                _normalize_headline(
                    published=raw.get("published") or "",
                    source=f"rss:{raw['raw_source']}",
                    title=raw["title"],
                    url=raw.get("url") or "",
                    instrument=instrument,
                    catalyst_cfg=catalyst_cfg,
                )
            )
    return rows, "rss"


def fetch_newsapi_headlines(
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
    query: str | None = None,
    from_date: date | None = None,
    to_date: date | None = None,
) -> tuple[list[dict[str, Any]], str]:
    """NewsAPI ``everything`` — requires ``NEWSAPI_KEY``."""
    key = os.getenv("NEWSAPI_KEY", "").strip()
    if not key:
        return [], SOURCE_NOT_CONFIGURED
    settings = get_settings()
    prof = instrument_profile(catalyst_cfg, instrument) or {}
    q = (query or "").strip()
    if not q:
        # deterministic query from first keyword group only (no free-text invention)
        groups = prof.get("catalyst_keyword_groups") if isinstance(prof.get("catalyst_keyword_groups"), dict) else {}
        parts: list[str] = []
        for kws in list(groups.values())[:2]:
            if isinstance(kws, list):
                parts.extend(str(x) for x in kws[:2] if x)
        q = " OR ".join(parts) if parts else instrument.split("/")[0].strip()
    fd = from_date or date.today()
    td = to_date or date.today()
    params = {
        "q": q,
        "from": _iso_date(fd),
        "to": _iso_date(td),
        "language": "en",
        "sortBy": "publishedAt",
        "pageSize": "50",
        "apiKey": key,
    }
    url = f"https://newsapi.org/v2/everything?{urlencode(params)}"
    try:
        req = Request(url, headers={"User-Agent": "hptl-intelligence/1.0"})
        with urlopen(req, timeout=settings.request_timeout_seconds) as resp:
            payload = json.loads(resp.read().decode("utf-8", errors="replace"))
    except OSError:
        return [], "newsapi:error"
    except ValueError:
        return [], "newsapi:parse_error"
    if not isinstance(payload, dict) or payload.get("status") != "ok":
        return [], "newsapi:upstream_error"
    articles = payload.get("articles")
    if not isinstance(articles, list):
        return [], "newsapi:no_articles"
    out: list[dict[str, Any]] = []
    for a in articles:
        if not isinstance(a, dict):
            continue
        title = str(a.get("title") or "").strip()
        if not title:
            continue
        src = str((a.get("source") or {}).get("name") or "newsapi")
        url_s = str(a.get("url") or "")
        pub = str(a.get("publishedAt") or "")
        out.append(
            _normalize_headline(
                published=pub,
                source=f"newsapi:{src}",
                title=title,
                url=url_s,
                instrument=instrument,
                catalyst_cfg=catalyst_cfg,
            )
        )
    return out, "newsapi"


def fetch_gdelt_headlines(
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
    query: str | None = None,
    maxrecords: int = 40,
) -> tuple[list[dict[str, Any]], str]:
    """GDELT DOC API (no key). Empty + message if query empty / no results."""
    prof = instrument_profile(catalyst_cfg, instrument) or {}
    q = (query or "").strip()
    if not q:
        groups = prof.get("catalyst_keyword_groups") if isinstance(prof.get("catalyst_keyword_groups"), dict) else {}
        tokens: list[str] = []
        for kws in list(groups.values())[:2]:
            if isinstance(kws, list):
                tokens.extend(str(x) for x in kws[:2] if x)
        if not tokens:
            return [], SOURCE_NOT_CONFIGURED
        q = " OR ".join(tokens)
    arts = fetch_gdelt_doc_list(q, maxrecords=maxrecords)
    out: list[dict[str, Any]] = []
    for a in arts:
        out.append(
            _normalize_headline(
                published=a.seendate,
                source=f"gdelt:{a.domain or 'gdelt'}",
                title=a.title,
                url=a.url,
                instrument=instrument,
                catalyst_cfg=catalyst_cfg,
            )
        )
    return out, "gdelt"


def fetch_normalized_headlines(
    *,
    instrument: str,
    catalyst_cfg: dict[str, Any],
    from_date: date | None = None,
    to_date: date | None = None,
    include_rss: bool = True,
    include_newsapi: bool = True,
    include_gdelt: bool = True,
) -> dict[str, Any]:
    """Run all configured headline adapters; each slice reports availability."""
    out: dict[str, Any] = {
        "instrument": instrument,
        "headlines": [],
        "sources": {},
    }
    if include_rss:
        rows, status = fetch_rss_headlines(instrument=instrument, catalyst_cfg=catalyst_cfg)
        out["sources"]["rss"] = {"status": status, "count": len(rows)}
        out["headlines"].extend(rows)
    if include_newsapi:
        rows, status = fetch_newsapi_headlines(
            instrument=instrument, catalyst_cfg=catalyst_cfg, from_date=from_date, to_date=to_date
        )
        out["sources"]["newsapi"] = {"status": status, "count": len(rows)}
        out["headlines"].extend(rows)
    if include_gdelt:
        rows, status = fetch_gdelt_headlines(instrument=instrument, catalyst_cfg=catalyst_cfg)
        out["sources"]["gdelt"] = {"status": status, "count": len(rows)}
        out["headlines"].extend(rows)
    return out
