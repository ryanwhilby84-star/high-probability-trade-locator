"""FX Setup Ranking Engine V2 — three-layer swing-trader model."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.fx.currency_map import COT_CURRENCY_SOURCES, LEADERBOARD_CURRENCIES, parse_fx_pair
from hptl.fx.fx_valuation_export import DEFAULT_PAIRS
from hptl.fx.fx_valuation_panel import build_fx_valuation_panel
from hptl.prices.price_store import load_price_store
from hptl.setup_ranking.grades import (
    ENGINE_VERSION,
    PillarScore,
    clamp_score,
)
from hptl.setup_ranking.layers import (
    build_layer_debug,
    composite_display_grade,
    derive_action_label,
    score_macro_bias_layer,
    score_trade_readiness_layer,
    score_valuation_edge_layer,
)
from hptl.setup_ranking.split_score_contract import audit_split_scores, split_score_fields
from hptl.setup_ranking.location import score_location_pillar
from hptl.setup_ranking.movement import (
    compute_movement_metrics,
    movement_score_from_metrics,
    percentile_rank,
)

# G10 cross matrix (same universe as RS opportunity board).
G10_PAIRS: tuple[str, ...] = tuple(
    f"{b}/{q}"
    for b in LEADERBOARD_CURRENCIES
    for q in LEADERBOARD_CURRENCIES
    if b != q
)


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return None


def _pair_direction(raw_rs_diff: float | None) -> str:
    if raw_rs_diff is None:
        return "neutral"
    if raw_rs_diff >= 3.0:
        return "long"
    if raw_rs_diff <= -3.0:
        return "short"
    return "neutral"


def _bias_aligns(bias: str | None, direction: str) -> bool:
    b = str(bias or "").lower()
    if direction == "long":
        return "bull" in b
    if direction == "short":
        return "bear" in b
    return "neutral" in b or b == ""


def _score_from_magnitude(value: float | None, *, scale: float = 15.0) -> float:
    if value is None:
        return 0.0
    return clamp_score(min(10.0, abs(float(value)) / scale * 10.0))


def score_relative_strength_pillar(
    *,
    pair: str,
    raw_rs_diff: float | None,
    base_score: float | None,
    quote_score: float | None,
    directional_bias: str | None,
) -> PillarScore:
    direction = _pair_direction(raw_rs_diff)
    mag = _score_from_magnitude(raw_rs_diff, scale=20.0)
    aligned = _bias_aligns(directional_bias or ("Bullish" if direction == "long" else "Bearish" if direction == "short" else "Neutral"), direction)
    score = mag if aligned else clamp_score(mag * 0.45)
    if raw_rs_diff is None:
        return PillarScore(
            key="relative_strength",
            label="Relative Strength",
            score=0.0,
            summary="RS data unavailable.",
            missing=True,
        )
    legs = parse_fx_pair(pair)
    base = legs.base if legs else pair.split("/")[0]
    quote = legs.quote if legs else pair.split("/")[1]
    summary = (
        f"{base} vs {quote}: raw RS differential {raw_rs_diff:+.1f}. "
        f"{directional_bias or direction}."
    )
    return PillarScore(
        key="relative_strength",
        label="Relative Strength",
        score=score,
        bias=directional_bias or direction.title(),
        summary=summary,
        detail=f"Base leg score {base_score}, quote leg score {quote_score}.",
        aligned=aligned,
        meta={"raw_rs_differential": raw_rs_diff, "direction": direction, "base_score": base_score, "quote_score": quote_score},
    )


def score_valuation_pillar(
    *,
    pair: str,
    direction: str,
    val_block: dict[str, Any] | None,
) -> PillarScore:
    if not val_block or val_block.get("supported") is False:
        return PillarScore(
            key="valuation",
            label="Valuation",
            score=0.0,
            summary="Institutional valuation unavailable.",
            missing=True,
        )
    adj_diff = val_block.get("positioning_adjusted_score_differential")
    if adj_diff is None:
        adj_diff = val_block.get("pair_score_differential")
    bias = val_block.get("positioning_bias") or val_block.get("valuation_bias")
    gap = val_block.get("valuation_gap_pct")
    mag = _score_from_magnitude(_num(adj_diff), scale=25.0)
    aligned = _bias_aligns(bias, direction)
    score = mag if aligned else clamp_score(mag * 0.4)
    expl = val_block.get("explanation") or val_block.get("pair_status") or ""
    overlay = val_block.get("macro_positioning_overlay") or {}
    notes = overlay.get("notes") or []
    detail = expl
    if notes:
        detail = f"{expl} · {' · '.join(notes)}" if expl else " · ".join(notes)
    return PillarScore(
        key="valuation",
        label="Valuation",
        score=score,
        bias=bias,
        summary=f"Macro V2 + TFF overlay: {bias}. Gap {gap}%." if gap is not None else f"Macro V2: {bias}.",
        detail=detail,
        aligned=aligned,
        meta={
            "pair_score_differential": val_block.get("pair_score_differential"),
            "positioning_adjusted_score_differential": adj_diff,
            "valuation_gap_pct": gap,
            "policy_rate_diff": val_block.get("policy_rate_diff"),
            "yield_2y_diff": val_block.get("yield_2y_diff"),
            "real_yield_diff": val_block.get("real_yield_diff"),
        },
    )


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _leg_row(confluence_by_market: dict[str, dict[str, Any]], code: str) -> dict[str, Any] | None:
    spec = COT_CURRENCY_SOURCES.get(code.upper())
    if not spec:
        return None
    return confluence_by_market.get(str(spec["market"]))


def score_positioning_pillar(
    *,
    pair: str,
    direction: str,
    confluence_by_market: dict[str, dict[str, Any]],
    val_block: dict[str, Any] | None,
) -> PillarScore:
    legs = parse_fx_pair(pair)
    if not legs:
        return PillarScore(key="positioning", label="Institutional Positioning", score=0.0, missing=True)
    base_row = _leg_row(confluence_by_market, legs.base)
    quote_row = _leg_row(confluence_by_market, legs.quote)
    if not base_row and not quote_row:
        return PillarScore(
            key="positioning",
            label="Institutional Positioning",
            score=0.0,
            summary="No leg COT rows for positioning.",
            missing=True,
        )

    def _leg_score(row: dict[str, Any] | None, *, invert: bool) -> tuple[float, str]:
        if not row:
            return 0.0, "missing"
        bias = str(row.get("cot_bias") or "N/A")
        if invert:
            if "bull" in bias.lower():
                bias = "Bearish"
            elif "bear" in bias.lower():
                bias = "Bullish"
        net = _num(row.get("net_value"))
        chg = _num(row.get("one_week_net_change"))
        pct = _num(row.get("current_net_percentile") or (row.get("rolling_3y_history_context") or {}).get("current_net_percentile"))
        mag = _score_from_magnitude(net, scale=80000.0) * 0.5 + _score_from_magnitude(chg, scale=15000.0) * 0.3
        if pct is not None:
            if pct >= 75:
                mag = clamp_score(mag + 1.0)
            elif pct <= 25:
                mag = clamp_score(mag + 0.5)
        aligned = _bias_aligns(bias, direction)
        return (mag if aligned else mag * 0.35, bias)

    b_spec = COT_CURRENCY_SOURCES.get(legs.base, {})
    q_spec = COT_CURRENCY_SOURCES.get(legs.quote, {})
    b_sc, b_bias = _leg_score(base_row, invert=bool(b_spec.get("invert_cot")))
    q_sc, q_bias = _leg_score(quote_row, invert=bool(q_spec.get("invert_cot")))
    score = clamp_score((b_sc + q_sc) / 2.0)

    # TFF overlay boost for USD legs
    overlay = (val_block or {}).get("macro_positioning_overlay") or {}
    pos_bias = overlay.get("positioning_bias")
    if pos_bias and _bias_aligns(pos_bias, direction):
        score = clamp_score(score + 1.5)

    summary = f"Legs: {legs.base} ({b_bias}), {legs.quote} ({q_bias})."
    if pos_bias:
        summary += f" TFF macro overlay: {pos_bias}."
    return PillarScore(
        key="positioning",
        label="Institutional Positioning",
        score=score,
        bias=pos_bias or b_bias,
        summary=summary,
        detail="Legacy NC leg COT + TFF DXY/Treasury overlay where applicable.",
        aligned=_bias_aligns(pos_bias or b_bias, direction),
        meta={"base_bias": b_bias, "quote_bias": q_bias, "tff_overlay": pos_bias},
    )


def score_seasonality_pillar(
    *,
    pair: str,
    direction: str,
    confluence_by_market: dict[str, dict[str, Any]],
) -> PillarScore:
    legs = parse_fx_pair(pair)
    if not legs:
        return PillarScore(key="seasonality", label="Seasonality", score=0.0, missing=True)

    def _sea(row: dict[str, Any] | None, invert: bool) -> tuple[float, str, bool]:
        if not row or row.get("seasonality_wired") is False:
            return 0.0, "N/A", False
        bias = str(row.get("seasonality_bias") or "N/A")
        if invert:
            if "bull" in bias.lower():
                bias = "Bearish"
            elif "bear" in bias.lower():
                bias = "Bullish"
        raw = _num(row.get("seasonality_score"))
        if raw is not None and abs(raw) <= 1.5:
            mag = clamp_score(raw * 10.0)
        elif raw is not None:
            mag = clamp_score(raw)
        else:
            mag = 5.0 if bias.lower() != "n/a" else 0.0
        aligned = _bias_aligns(bias, direction)
        return (mag if aligned else clamp_score(mag * 0.4), bias, row.get("seasonality_wired", True) is not False)

    b_spec = COT_CURRENCY_SOURCES.get(legs.base, {})
    q_spec = COT_CURRENCY_SOURCES.get(legs.quote, {})
    b_sc, b_bias, b_w = _sea(_leg_row(confluence_by_market, legs.base), bool(b_spec.get("invert_cot")))
    q_sc, q_bias, q_w = _sea(_leg_row(confluence_by_market, legs.quote), bool(q_spec.get("invert_cot")))
    if not b_w and not q_w:
        return PillarScore(key="seasonality", label="Seasonality", score=0.0, summary="Seasonality not wired.", missing=True)
    score = clamp_score((b_sc + q_sc) / 2.0)
    reason = ""
    if base_row := _leg_row(confluence_by_market, legs.base):
        reason = str(base_row.get("seasonality_reason") or "")
    return PillarScore(
        key="seasonality",
        label="Seasonality",
        score=score,
        bias=b_bias if b_bias != "N/A" else q_bias,
        summary=f"Seasonal bias: {legs.base} {b_bias}, {legs.quote} {q_bias}.",
        detail=reason,
        aligned=_bias_aligns(b_bias, direction) or _bias_aligns(q_bias, direction),
        meta={"base_bias": b_bias, "quote_bias": q_bias},
    )


def build_explanation(
    *,
    pair: str,
    macro: Any,
    valuation: Any,
    readiness: Any,
    action_label: str,
    display_grade: str,
    pillars: dict[str, PillarScore],
) -> dict[str, Any]:
    sections = [
        {"pillar": "Macro Bias", "headline": macro.summary, "detail": macro.detail},
        {"pillar": "Valuation Edge", "headline": valuation.summary, "detail": valuation.detail},
        {"pillar": "Trade Readiness", "headline": readiness.summary, "detail": readiness.detail},
    ]
    for key in ("relative_strength", "positioning", "seasonality"):
        p = pillars.get(key)
        if p:
            sections.append({"pillar": p.label, "headline": p.summary, "detail": p.detail})
    return {
        "pair": pair,
        "grade": display_grade,
        "action_label": action_label,
        "sections": sections,
        "result_line": f"{action_label} — {display_grade} ({macro.grade} macro · {valuation.grade} value · {readiness.grade} readiness).",
    }


def build_fx_setup_ranking_payload(
    *,
    pairs: tuple[str, ...] | None = None,
    rs_doc: dict[str, Any] | None = None,
    val_doc: dict[str, Any] | None = None,
    confluence_records: list[dict[str, Any]] | None = None,
    price_doc: dict[str, Any] | None = None,
) -> dict[str, Any]:
    from hptl.config import PROCESSED_DIR, PROJECT_ROOT

    pairs = pairs or G10_PAIRS
    if rs_doc is None:
        rs_doc = _load_json(PROCESSED_DIR / "relative_strength_latest.json") or _load_json(
            PROJECT_ROOT / "web-dashboard" / "public" / "data" / "relative_strength_latest.json"
        )
    if val_doc is None:
        val_doc = _load_json(PROCESSED_DIR / "fx_valuation_latest.json") or _load_json(
            PROJECT_ROOT / "web-dashboard" / "public" / "data" / "fx_valuation_latest.json"
        )
    if price_doc is None:
        price_doc = load_price_store()

    # Latest confluence week rows by market
    confluence_by_market: dict[str, dict[str, Any]] = {}
    if confluence_records:
        for rec in confluence_records:
            m = str(rec.get("market") or "")
            if not m:
                continue
            prev = confluence_by_market.get(m)
            if not prev or str(rec.get("date") or "") >= str(prev.get("date") or ""):
                confluence_by_market[m] = rec

    rs_pairs = {
        str(p.get("pair")): p
        for p in (rs_doc or {}).get("relative_strength", {}).get("pair_differentials")
        or (rs_doc or {}).get("pair_opportunities")
        or []
    }
    rs_legs = {
        str(r.get("currency")): r
        for r in (rs_doc or {}).get("relative_strength", {}).get("leaderboard")
        or (rs_doc or {}).get("currency_leaderboard")
        or []
    }
    val_pairs = {str(p.get("pair")): p for p in (val_doc or {}).get("pairs") or []}
    val_panel_map = dict((val_doc or {}).get("valuation_panels") or {})
    val_currencies = (val_doc or {}).get("currencies") or (val_doc or {}).get("currency_scores") or {}
    val_macro = (val_doc or {}).get("macro_positioning") or {}
    px_inst = (price_doc or {}).get("instruments") or {}

    candidates: list[dict[str, Any]] = []
    movement_raw: list[tuple[str, float]] = []

    for pair in pairs:
        rs = rs_pairs.get(pair) or {}
        raw_diff = _num(rs.get("raw_rs_differential") or rs.get("raw_differential_score"))
        direction = _pair_direction(raw_diff)
        legs = parse_fx_pair(pair)
        base_sc = _num(rs_legs.get(legs.base, {}).get("raw_rs") if legs else None)
        quote_sc = _num(rs_legs.get(legs.quote, {}).get("raw_rs") if legs else None)

        val_block = val_pairs.get(pair)
        px = px_inst.get(pair) or {}
        daily = px.get("daily") or []
        weekly = px.get("weekly") or []

        # zone_focus from stronger leg institutional context
        zone_focus = None
        if legs:
            for code in (legs.base, legs.quote):
                row = _leg_row(confluence_by_market, code)
                if row:
                    inst = row.get("institutional_context") or {}
                    zf = inst.get("zone_focus") or row.get("zone_focus")
                    if zf:
                        zone_focus = str(zf)
                        break

        pillars = {
            "relative_strength": score_relative_strength_pillar(
                pair=pair,
                raw_rs_diff=raw_diff,
                base_score=base_sc,
                quote_score=quote_sc,
                directional_bias=rs.get("directional_bias"),
            ),
            "valuation": score_valuation_pillar(pair=pair, direction=direction, val_block=val_block),
            "positioning": score_positioning_pillar(
                pair=pair,
                direction=direction,
                confluence_by_market=confluence_by_market,
                val_block=val_block,
            ),
            "seasonality": score_seasonality_pillar(
                pair=pair,
                direction=direction,
                confluence_by_market=confluence_by_market,
            ),
            "location": score_location_pillar(
                pair=pair,
                direction=direction,
                daily=daily,
                weekly=weekly,
                zone_focus=zone_focus,
            ),
        }

        val_panel = val_panel_map.get(pair)
        if not val_panel and val_block:
            val_panel = build_fx_valuation_panel(
                pair,
                val_block,
                macro_doc=val_macro,
                currencies=val_currencies,
            ).as_dict()

        macro_layer = score_macro_bias_layer(
            direction=direction,
            rs=pillars["relative_strength"],
            positioning=pillars["positioning"],
            seasonality=pillars["seasonality"],
            val_block=val_block,
        )
        val_edge_layer = score_valuation_edge_layer(pair=pair, direction=direction, val_block=val_block)
        readiness_layer = score_trade_readiness_layer(
            pair=pair,
            direction=direction,
            location=pillars["location"],
        )

        action_label, action_gates, action_note = derive_action_label(
            direction=direction,
            macro=macro_layer,
            valuation=val_edge_layer,
            readiness=readiness_layer,
        )
        display_grade = composite_display_grade(
            macro=macro_layer,
            valuation=val_edge_layer,
            readiness=readiness_layer,
            action_label=action_label,
        )

        if val_panel:
            val_panel = {
                **val_panel,
                "valuation_score": val_edge_layer.score,
                "score_display": f"{round(val_edge_layer.score)}/10",
            }

        # Sync valuation pillar to edge score (not momentum differential)
        pillars["valuation"] = PillarScore(
            key="valuation",
            label="Valuation Edge",
            score=val_edge_layer.score,
            bias=val_block.get("valuation_bias") if val_block else None,
            summary=val_edge_layer.summary,
            detail=val_edge_layer.detail,
            aligned=val_edge_layer.score >= 6.0,
            meta={"valuation_edge": val_edge_layer.as_dict(), "valuation_panel": val_panel},
        )

        setup = round((macro_layer.score + val_edge_layer.score + readiness_layer.score) / 3.0 * 10.0, 1)
        mov_metrics = compute_movement_metrics(daily, weekly)
        raw_mov = (
            (_num(mov_metrics.get("atr_30d_pct")) or 0)
            + (_num(mov_metrics.get("atr_90d_pct")) or 0)
            + (_num(mov_metrics.get("weekly_range_pct")) or 0) * 0.5
            + (_num(mov_metrics.get("trend_expansion_pct")) or 0) * 0.3
        )
        movement_raw.append((pair, raw_mov))

        debug = build_layer_debug(
            pair=pair,
            direction=direction,
            macro=macro_layer,
            valuation=val_edge_layer,
            readiness=readiness_layer,
            action_label=action_label,
            action_gates=action_gates,
            action_note=action_note,
            display_grade=display_grade,
        )

        split_scores = split_score_fields(
            macro=macro_layer,
            valuation=val_edge_layer,
            readiness=readiness_layer,
            action_label=action_label,
        )

        candidates.append(
            {
                "pair": pair,
                "direction": direction,
                "setup_score": setup,
                "grade": display_grade,
                "action_note": action_note,
                "movement_score": 0.0,
                "valuation_score": val_edge_layer.score,
                **split_scores,
                "macro_bias": macro_layer.as_dict(),
                "valuation_edge": val_edge_layer.as_dict(),
                "trade_readiness": readiness_layer.as_dict(),
                "layers": {
                    "macro_bias": macro_layer.as_dict(),
                    "valuation_edge": val_edge_layer.as_dict(),
                    "trade_readiness": readiness_layer.as_dict(),
                },
                "debug": debug,
                "valuation_panel": val_panel,
                "movement_metrics": mov_metrics,
                "pillars": {k: v.as_dict() for k, v in pillars.items()},
                "explanation": build_explanation(
                    pair=pair,
                    macro=macro_layer,
                    valuation=val_edge_layer,
                    readiness=readiness_layer,
                    action_label=action_label,
                    display_grade=display_grade,
                    pillars=pillars,
                ),
                "raw_rs_differential": raw_diff,
                "in_valuation_universe": pair in val_pairs,
            }
        )

    # Movement percentiles across universe
    for cand in candidates:
        pair = cand["pair"]
        raw = next((v for p, v in movement_raw if p == pair), 0.0)
        pct = percentile_rank([v for _, v in movement_raw], raw)
        cand["movement_score"] = movement_score_from_metrics(cand["movement_metrics"], percentile=pct)

    # Rank: trade candidates first, then by readiness + macro (not momentum alone)
    action_rank = {
        "Trade Candidate": 4,
        "Watch Pullback": 3,
        "Watch Breakout": 2,
        "Avoid / No Edge": 1,
    }
    ranked = sorted(
        candidates,
        key=lambda x: (
            -action_rank.get(x.get("action_label") or "", 0),
            -float(x.get("trade_readiness_score") or 0),
            -float(x.get("macro_score") or 0),
            -float(x.get("valuation_edge_score") or 0),
        ),
    )
    for i, row in enumerate(ranked, start=1):
        row["rank"] = i

    top_debug = [r["debug"] for r in ranked[:10]]
    generated_at = datetime.now(timezone.utc).isoformat()
    split_audit = audit_split_scores(ranked, generated_at=generated_at)

    trade_candidates = [r for r in ranked if r.get("action_label") == "Trade Candidate"]

    return {
        "schema_version": 2,
        "engine": ENGINE_VERSION,
        "generated_at": generated_at,
        "calendar_week": (rs_doc or {}).get("calendar_week"),
        "pair_universe": list(pairs),
        "split_score_audit": split_audit,
        "ranking_rules": {
            "layers": [
                "Macro Bias — RS + positioning + seasonality + DXY/Treasury",
                "Valuation Edge — fair-value gap + spot deviation (NOT RS differential magnitude)",
                "Trade Readiness — weekly zone structure, R/R, opposing supply/demand",
            ],
            "action_labels": ["Trade Candidate", "Watch Pullback", "Watch Breakout", "Avoid / No Edge"],
            "trade_candidate_requires": "Trade Readiness B+ (7.0/10) or better",
            "a_plus_requires": "Readiness A (8.0/10) or better",
            "high_macro_poor_readiness": "Watch Pullback (not Trade Candidate)",
            "movement_score": "Cross-sectional ATR percentile (informational only)",
            "default_filter": "action_label != Avoid / No Edge",
        },
        "opportunities": ranked,
        "filtered_opportunities": trade_candidates or [r for r in ranked if r.get("action_label") != "Avoid / No Edge"][:15],
        "debug_top_pairs": top_debug,
    }
