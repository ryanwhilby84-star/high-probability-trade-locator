"""Build instrument-scoped catalyst headline bundles from GDELT (config-driven matches only)."""
from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.intelligence.catalyst_loader import load_catalyst_config
from hptl.news.gdelt_downloader import fetch_gdelt_doc_list

SOURCE_NOT_CONFIGURED = "source not configured"


def _normalize_date(seendate: str) -> str:
    s = (seendate or "").strip()
    if len(s) >= 8 and s[:8].isdigit():
        return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return s or ""


def _lexicon_sentiment(title: str, lex: dict[str, Any]) -> str:
    """Title-only lexicon hit; no model."""
    if not isinstance(lex, dict):
        return "unscored"
    pos = lex.get("positive") if isinstance(lex.get("positive"), list) else []
    neg = lex.get("negative") if isinstance(lex.get("negative"), list) else []
    t = title.lower()
    p_hit = any(str(p).lower() in t for p in pos if p)
    n_hit = any(str(n).lower() in t for n in neg if n)
    if p_hit and not n_hit:
        return "positive"
    if n_hit and not p_hit:
        return "negative"
    if p_hit and n_hit:
        return "mixed"
    return "neutral"


def _load_news_map(cfg: dict[str, Any], instrument: str) -> list[dict[str, Any]]:
    inst = cfg.get("instruments") if isinstance(cfg.get("instruments"), dict) else {}
    block = inst.get(instrument)
    if not isinstance(block, dict):
        return []
    m = block.get("news_catalyst_map")
    if not isinstance(m, list):
        return []
    out: list[dict[str, Any]] = []
    for row in m:
        if not isinstance(row, dict):
            continue
        match = str(row.get("match") or "").strip()
        ct = str(row.get("catalyst_type") or "general").strip() or "general"
        if match:
            out.append({"match": match, "catalyst_type": ct})
    return sorted(out, key=lambda r: len(r["match"]), reverse=True)


def _gdelt_or_query(phrases: list[str], *, max_terms: int = 18) -> str:
    """OR-query for GDELT DOC API (length-safe)."""
    parts: list[str] = []
    for p in phrases[:max_terms]:
        p = p.strip()
        if not p:
            continue
        if re.search(r"[^\w]", p, flags=re.UNICODE) or re.search(r"\s", p):
            esc = p.replace("\\", "\\\\").replace('"', '\\"')
            parts.append(f'"{esc}"')
        else:
            parts.append(p)
    if not parts:
        return "natural gas"
    return "(" + " OR ".join(parts) + ")"


def _fetch_gdelt_for_instrument(news_map: list[dict[str, Any]], *, maxrecords: int) -> tuple[list[Any], str]:
    """Try a broad headline pull first (GDELT is sensitive to long queries), then a tighter OR fallback."""
    primary = "natural gas"
    arts = fetch_gdelt_doc_list(primary, maxrecords=maxrecords)
    query_used = primary
    if arts:
        return arts, query_used

    phrases = [r["match"] for r in news_map if r.get("match")]
    fallback = _gdelt_or_query(phrases[:12], max_terms=12)
    arts = fetch_gdelt_doc_list(fallback, maxrecords=maxrecords)
    return arts, fallback


def _phrase_in_title(title: str, phrase: str) -> bool:
    """Substring match; short single-token phrases use word boundaries to reduce noise."""
    p = phrase.strip()
    if not p:
        return False
    if " " not in p and len(p) <= 5:
        return re.search(rf"\b{re.escape(p)}\b", title, flags=re.IGNORECASE) is not None
    return p.lower() in title.lower()


def _match_title(title: str, news_map: list[dict[str, Any]]) -> list[dict[str, str]]:
    hits: list[dict[str, str]] = []
    seen: set[str] = set()
    for row in news_map:
        m = row["match"]
        if _phrase_in_title(title, m) and m.lower() not in seen:
            seen.add(m.lower())
            hits.append({"keyword": m, "catalyst_type": row["catalyst_type"]})
    return hits


def _primary_catalyst_type(hits: list[dict[str, str]]) -> str:
    if not hits:
        return "none"
    return hits[0]["catalyst_type"]


def _relevance_score(hit_count: int) -> float:
    base = 0.28
    step = 0.14
    return round(min(1.0, base + step * max(0, hit_count)), 3)


def build_instrument_gdelt_news_payload(
    *,
    catalyst_cfg: dict[str, Any] | None = None,
    instrument: str = "Natural Gas / NG",
    maxrecords: int = 80,
) -> dict[str, Any]:
    """Fetch GDELT headlines, keep rows that match ``news_catalyst_map`` only."""
    cfg = catalyst_cfg or load_catalyst_config()
    news_map = _load_news_map(cfg, instrument)
    lex = {}
    g = cfg.get("global")
    if isinstance(g, dict) and isinstance(g.get("title_sentiment_lexicon"), dict):
        lex = g["title_sentiment_lexicon"]

    if not news_map:
        return {
            "instrument": instrument,
            "status": "not_configured",
            "message": SOURCE_NOT_CONFIGURED,
            "headlines": [],
            "matched_catalyst_tags": [],
            "query_used": None,
        }

    articles, query_used = _fetch_gdelt_for_instrument(news_map, maxrecords=maxrecords)

    if not articles:
        return {
            "instrument": instrument,
            "status": "fetch_empty",
            "message": "No headlines returned from GDELT (network, query, or upstream empty).",
            "headlines": [],
            "matched_catalyst_tags": [],
            "query_used": query_used,
        }

    headlines: list[dict[str, Any]] = []
    tag_counts: dict[str, int] = {}
    seen_url: set[str] = set()

    for a in articles:
        url = (a.url or "").strip()
        dedupe_key = url or a.title
        if dedupe_key in seen_url:
            continue
        hits = _match_title(a.title, news_map)
        if not hits:
            continue
        seen_url.add(dedupe_key)
        for h in hits:
            tag_counts[h["catalyst_type"]] = tag_counts.get(h["catalyst_type"], 0) + 1
        mkw = [h["keyword"] for h in hits]
        headlines.append(
            {
                "date": _normalize_date(a.seendate),
                "title": a.title,
                "source": a.domain or "gdelt",
                "url": url,
                "matched_keywords": mkw,
                "instrument": instrument,
                "catalyst_type": _primary_catalyst_type(hits),
                "rough_sentiment": _lexicon_sentiment(a.title, lex),
                "relevance_score": _relevance_score(len(set(mkw))),
            }
        )

    matched_tags = sorted(tag_counts.keys(), key=lambda k: (-tag_counts[k], k))

    if not headlines:
        return {
            "instrument": instrument,
            "status": "ok",
            "message": "GDELT returned results but no titles matched configured catalyst keywords.",
            "headlines": [],
            "matched_catalyst_tags": [],
            "query_used": query_used,
        }

    return {
        "instrument": instrument,
        "status": "ok",
        "message": None,
        "headlines": headlines[:40],
        "matched_catalyst_tags": matched_tags,
        "query_used": query_used,
    }


def build_news_catalysts_document(
    *,
    catalyst_cfg: dict[str, Any] | None = None,
    instruments: tuple[str, ...] = ("Natural Gas / NG",),
) -> dict[str, Any]:
    """Full dashboard document (extend ``instruments`` when more maps exist)."""
    cfg = catalyst_cfg or load_catalyst_config()
    out: dict[str, Any] = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "upstream": "gdelt_doc_api_v2",
        "instruments": {},
    }
    for ins in instruments:
        if _load_news_map(cfg, ins):
            out["instruments"][ins] = build_instrument_gdelt_news_payload(
                catalyst_cfg=cfg, instrument=ins, maxrecords=80
            )
        else:
            out["instruments"][ins] = {
                "instrument": ins,
                "status": "not_configured",
                "message": SOURCE_NOT_CONFIGURED,
                "headlines": [],
                "matched_catalyst_tags": [],
                "query_used": None,
            }
    return out


def write_news_catalysts_json(path: Path | None = None, *, catalyst_cfg: dict[str, Any] | None = None) -> Path:
    """Write ``news_catalysts.json`` for the static dashboard."""
    from hptl.config import PROJECT_ROOT

    p = path or (PROJECT_ROOT / "web-dashboard" / "public" / "data" / "news_catalysts.json")
    p.parent.mkdir(parents=True, exist_ok=True)
    doc = build_news_catalysts_document(catalyst_cfg=catalyst_cfg)
    p.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return p
