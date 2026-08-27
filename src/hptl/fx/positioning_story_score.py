"""FX positioning story score — commercial vs non-commercial relationship layer.

Research / attention classification only. Does not modify Relative Strength,
scanner ranking, or valuation.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.cot.legacy_cot_loader import load_legacy_cot_document
from hptl.cot.positioning_percentiles import WINDOW_WEEKS_3Y, empirical_percentile_rank
from hptl.fx.commercial_strength_research import commercial_research_sources
from hptl.fx.currency_map import DX_INSTRUMENT_ID

POSITIONING_STORY_PATH = Path("data/fx_positioning_story_latest.json")
PUBLIC_POSITIONING_STORY_PATH = Path("web-dashboard/public/data/fx_positioning_story_latest.json")

GROUP_COMMERCIALS = "commercials"
GROUP_NONCOMMERCIALS = "noncommercials"

CHANGE_WEEKS_SHORT = 4
CHANGE_WEEKS_LONG = 13
CHANGE_SCALE = 40_000.0

STORY_CONFIRMED_BULLISH = "Confirmed bullish alignment"
STORY_EARLY_BULLISH = "Early bullish rotation"
STORY_COMMERCIAL_ACCUMULATION = "Commercial accumulation"
STORY_NONCOMMERCIAL_CAPITULATION = "Non-commercial capitulation"
STORY_MIXED = "Mixed"
STORY_EARLY_BEARISH = "Early bearish rotation"
STORY_CONFIRMED_BEARISH = "Confirmed bearish alignment"

STORY_STATES: tuple[str, ...] = (
    STORY_CONFIRMED_BULLISH,
    STORY_EARLY_BULLISH,
    STORY_COMMERCIAL_ACCUMULATION,
    STORY_NONCOMMERCIAL_CAPITULATION,
    STORY_MIXED,
    STORY_EARLY_BEARISH,
    STORY_CONFIRMED_BEARISH,
)

FX_STORY_CURRENCY_ORDER: tuple[str, ...] = (
    "EUR",
    "GBP",
    "JPY",
    "CHF",
    "AUD",
    "CAD",
    "NZD",
    "USD",
)


def _finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _orient(value: float | None, invert_cot: bool) -> float | None:
    if value is None:
        return None
    return -value if invert_cot else value


def _clamp_score(raw: float) -> int:
    return int(round(max(-100.0, min(100.0, raw))))


def _current_score(percentile: float | None) -> int | None:
    if percentile is None or not math.isfinite(percentile):
        return None
    return _clamp_score((float(percentile) - 50.0) * 2.0)


def _change_score(delta_4w: float | None, delta_13w: float | None) -> int | None:
    parts: list[tuple[float, float]] = []
    if delta_4w is not None:
        parts.append((math.tanh(delta_4w / CHANGE_SCALE), 0.55))
    if delta_13w is not None:
        parts.append((math.tanh(delta_13w / CHANGE_SCALE), 0.45))
    if not parts:
        return None
    weight_sum = sum(w for _, w in parts)
    raw = sum(v * w for v, w in parts) / weight_sum
    return _clamp_score(raw * 100.0)


def _sorted_group_weeks(instrument: dict[str, Any] | None, group_id: str) -> list[dict[str, Any]]:
    if not instrument:
        return []
    weeks = (instrument.get("groups") or {}).get(group_id, {}).get("weeks") or []
    if not isinstance(weeks, list):
        return []
    return sorted(weeks, key=lambda w: str(w.get("report_date") or ""))


def _oriented_group_nets(weeks: list[dict[str, Any]], *, invert_cot: bool) -> list[float]:
    out: list[float] = []
    for w in weeks:
        net = _orient(_finite(w.get("net")), invert_cot)
        if net is not None:
            out.append(net)
    return out


def _window_delta(oriented_nets: list[float], weeks_back: int) -> float | None:
    if len(oriented_nets) <= weeks_back:
        return None
    return round(oriented_nets[-1] - oriented_nets[-(weeks_back + 1)], 1)


def _group_metrics(weeks: list[dict[str, Any]], *, invert_cot: bool) -> dict[str, Any] | None:
    if not weeks:
        return None
    oriented_nets = _oriented_group_nets(weeks, invert_cot=invert_cot)
    if not oriented_nets:
        return None

    latest = weeks[-1]
    current_net = oriented_nets[-1]
    window = oriented_nets[-WINDOW_WEEKS_3Y:]
    percentile = empirical_percentile_rank(window, current_net)
    pct = None if percentile != percentile else round(float(percentile), 1)

    delta_4w = _window_delta(oriented_nets, CHANGE_WEEKS_SHORT)
    delta_13w = _window_delta(oriented_nets, CHANGE_WEEKS_LONG)

    return {
        "current_net_oriented": round(current_net, 1),
        "percentile_3y": pct,
        "current_score": _current_score(pct),
        "change_4w": delta_4w,
        "change_13w": delta_13w,
        "change_score": _change_score(delta_4w, delta_13w),
        "report_date": str(latest.get("report_date") or "")[:10],
    }


def _rotation_score(
    *,
    commercial_current: int | None,
    commercial_change: int | None,
    noncommercial_current: int | None,
    noncommercial_change: int | None,
    commercial_delta_4w: float | None,
    noncommercial_delta_4w: float | None,
) -> int | None:
    if commercial_change is None and noncommercial_change is None:
        return None

    score = 0.0
    cc = float(commercial_change or 0)
    nc = float(noncommercial_change or 0)
    score += 0.45 * (cc - nc) / 2.0

    if commercial_current is not None and noncommercial_current is not None:
        level_div = (float(noncommercial_current) - float(commercial_current)) / 2.0
        score += 0.20 * level_div

    if commercial_delta_4w is not None and noncommercial_delta_4w is not None:
        if commercial_delta_4w > 2_000 and noncommercial_delta_4w < -2_000:
            score += 18.0
        elif commercial_delta_4w < -2_000 and noncommercial_delta_4w > 2_000:
            score -= 18.0
        elif commercial_delta_4w * noncommercial_delta_4w < 0:
            sign = 1.0 if commercial_delta_4w > 0 else -1.0
            score += 10.0 * sign

    return _clamp_score(score)


def _classify_story(
    *,
    commercial_current: int | None,
    commercial_change: int | None,
    noncommercial_current: int | None,
    noncommercial_change: int | None,
    rotation: int | None,
) -> str:
    cc = commercial_current if commercial_current is not None else 0
    nc = noncommercial_current if noncommercial_current is not None else 0
    cch = commercial_change if commercial_change is not None else 0
    nch = noncommercial_change if noncommercial_change is not None else 0
    rot = rotation if rotation is not None else 0

    if nch <= -35 and nc >= 15:
        return STORY_NONCOMMERCIAL_CAPITULATION

    if cch >= 35 and cc <= -5:
        if rot >= 20 and nch < -10:
            return STORY_EARLY_BULLISH
        return STORY_COMMERCIAL_ACCUMULATION

    if cc >= 40 and nc <= -15 and rot >= 35 and cch > 0:
        return STORY_CONFIRMED_BULLISH

    if rot >= 25 and cch > 12 and nch < -8:
        return STORY_EARLY_BULLISH

    if cc <= -40 and nc >= 15 and rot <= -35 and cch < 0:
        return STORY_CONFIRMED_BEARISH

    if rot <= -25 and cch < -12 and nch > 8:
        return STORY_EARLY_BEARISH

    if cch <= -35 and cc >= 5:
        return STORY_COMMERCIAL_ACCUMULATION

    return STORY_MIXED


def _change_phrase(delta: float | None, *, subject: str, liquidation: bool = False) -> str | None:
    if delta is None:
        return None
    if liquidation and delta < -5_000:
        return f"{subject} is being liquidated"
    if delta > 8_000:
        return f"{subject} improving sharply"
    if delta > 2_000:
        return f"{subject} improving"
    if delta < -8_000:
        return f"{subject} deteriorating sharply"
    if delta < -2_000:
        return f"{subject} deteriorating"
    return f"{subject} is little changed"


def _build_explanation(currency: str, story_state: str, row: dict[str, Any]) -> str:
    comm = _change_phrase(row.get("commercial_change_13w"), subject="Commercial net")
    non = _change_phrase(
        row.get("noncommercial_change_13w"),
        subject="non-commercial net",
        liquidation=True,
    )
    body_parts = [p for p in (comm, non) if p]
    body = " while ".join(body_parts) if body_parts else "Commercial and non-commercial nets show no clear shared trend"

    confirmed = story_state.startswith("Confirmed")
    tail = "confirmed" if confirmed else "not confirmed"
    return f"{currency}: {body}. {story_state}, {tail}."


def compute_currency_positioning_story(
    instrument: dict[str, Any] | None,
    *,
    currency: str,
    cot_market: str,
    invert_cot: bool,
) -> dict[str, Any]:
    commercial_weeks = _sorted_group_weeks(instrument, GROUP_COMMERCIALS)
    noncommercial_weeks = _sorted_group_weeks(instrument, GROUP_NONCOMMERCIALS)

    commercial = _group_metrics(commercial_weeks, invert_cot=invert_cot)
    noncommercial = _group_metrics(noncommercial_weeks, invert_cot=invert_cot)

    if not commercial or not noncommercial:
        return {
            "currency": currency,
            "cot_market": cot_market,
            "available": False,
            "reason": "Missing commercials or noncommercials weekly history.",
            "story_state": STORY_MIXED,
            "story_score": None,
            "commercial_current_score": None,
            "commercial_change_score": None,
            "noncommercial_current_score": None,
            "noncommercial_change_score": None,
            "commercial_noncommercial_rotation_score": None,
            "explanation": f"{currency}: Positioning story unavailable — incomplete Legacy COT groups.",
        }

    rotation = _rotation_score(
        commercial_current=commercial["current_score"],
        commercial_change=commercial["change_score"],
        noncommercial_current=noncommercial["current_score"],
        noncommercial_change=noncommercial["change_score"],
        commercial_delta_4w=commercial["change_4w"],
        noncommercial_delta_4w=noncommercial["change_4w"],
    )

    story_state = _classify_story(
        commercial_current=commercial["current_score"],
        commercial_change=commercial["change_score"],
        noncommercial_current=noncommercial["current_score"],
        noncommercial_change=noncommercial["change_score"],
        rotation=rotation,
    )

    row: dict[str, Any] = {
        "currency": currency,
        "cot_market": cot_market,
        "available": True,
        "invert_cot_applied": invert_cot,
        "report_date": commercial["report_date"] or noncommercial["report_date"],
        "story_state": story_state,
        "story_score": rotation,
        "commercial_current_score": commercial["current_score"],
        "commercial_change_score": commercial["change_score"],
        "noncommercial_current_score": noncommercial["current_score"],
        "noncommercial_change_score": noncommercial["change_score"],
        "commercial_noncommercial_rotation_score": rotation,
        "commercial_percentile_3y": commercial["percentile_3y"],
        "noncommercial_percentile_3y": noncommercial["percentile_3y"],
        "commercial_change_4w": commercial["change_4w"],
        "commercial_change_13w": commercial["change_13w"],
        "noncommercial_change_4w": noncommercial["change_4w"],
        "noncommercial_change_13w": noncommercial["change_13w"],
        "commercials_net_oriented": commercial["current_net_oriented"],
        "noncommercials_net_oriented": noncommercial["current_net_oriented"],
    }
    row["explanation"] = _build_explanation(currency, story_state, row)
    return row


def build_positioning_story(
    legacy_doc: dict[str, Any] | None = None,
    *,
    calendar_week: str = "",
) -> dict[str, Any]:
    doc = legacy_doc or load_legacy_cot_document()
    instruments = doc.get("instruments") or {}
    currencies: dict[str, Any] = {}

    for code in FX_STORY_CURRENCY_ORDER:
        meta = commercial_research_sources().get(code) or {}
        market = str(meta.get("market") or "")
        invert = bool(meta.get("invert_cot"))
        instrument = instruments.get(market)
        currencies[code] = compute_currency_positioning_story(
            instrument,
            currency=code,
            cot_market=market,
            invert_cot=invert,
        )

    return {
        "calendar_week": calendar_week,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "generated_from": "hptl.fx.positioning_story_score",
        "research_only": True,
        "attention_layer": True,
        "no_trade_signals": True,
        "group_ids": [GROUP_COMMERCIALS, GROUP_NONCOMMERCIALS],
        "fx_futures_markets": [
            "Euro FX / 6E",
            "British Pound / 6B",
            "Japanese Yen / 6J",
            "Swiss Franc / 6S",
            "Australian Dollar / 6A",
            "Canadian Dollar / 6C",
            "NZ Dollar / 6N",
            DX_INSTRUMENT_ID,
        ],
        "currency_count": len(FX_STORY_CURRENCY_ORDER),
        "currencies": currencies,
    }


def build_positioning_story_table_rows(story_doc: dict[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code in FX_STORY_CURRENCY_ORDER:
        row = dict((story_doc.get("currencies") or {}).get(code) or {})
        row["currency"] = code
        score = row.get("story_score")
        row["abs_story_score"] = abs(float(score)) if score is not None else -1.0
        rows.append(row)
    rows.sort(key=lambda r: r["abs_story_score"], reverse=True)
    return rows


def write_positioning_story(*, legacy_doc: dict[str, Any] | None = None, calendar_week: str = "") -> Path:
    story_doc = build_positioning_story(legacy_doc, calendar_week=calendar_week)
    POSITIONING_STORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_POSITIONING_STORY_PATH.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(story_doc, indent=2, ensure_ascii=False)
    POSITIONING_STORY_PATH.write_text(text, encoding="utf-8")
    PUBLIC_POSITIONING_STORY_PATH.write_text(text, encoding="utf-8")
    return POSITIONING_STORY_PATH
