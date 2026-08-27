"""Three-layer FX ranking model — macro bias, valuation edge, trade readiness."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from hptl.setup_ranking.grades import PillarScore, clamp_score, grade_from_score_10

ACTION_TRADE = "Trade Candidate"
ACTION_PULLBACK = "Watch Pullback"
ACTION_BREAKOUT = "Watch Breakout"
ACTION_AVOID = "Avoid / No Edge"


@dataclass
class LayerResult:
    key: str
    label: str
    score: float  # 0-10
    grade: str
    summary: str
    detail: str = ""
    reasons: list[str] = field(default_factory=list)
    gates: list[str] = field(default_factory=list)
    meta: dict[str, Any] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "key": self.key,
            "label": self.label,
            "score": self.score,
            "score_display": f"{round(self.score)}/10",
            "score_100": round(self.score * 10.0, 1),
            "grade": self.grade,
            "summary": self.summary,
            "detail": self.detail,
            "reasons": list(self.reasons),
            "gates": list(self.gates),
            "meta": dict(self.meta),
        }


def _num(v: Any) -> float | None:
    if v is None:
        return None
    try:
        f = float(v)
        return f if f == f else None
    except (TypeError, ValueError):
        return None


def _bias_aligns(bias: str | None, direction: str) -> bool:
    b = str(bias or "").lower()
    if direction == "long":
        return "bull" in b
    if direction == "short":
        return "bear" in b
    return "neutral" in b or b == ""


def _macro_overlay_score(val_block: dict[str, Any] | None, direction: str) -> tuple[float, str]:
    overlay = (val_block or {}).get("macro_positioning_overlay") or {}
    pos_bias = overlay.get("positioning_bias")
    dxy = overlay.get("dxy_positioning") or {}
    treas = overlay.get("treasury_positioning") or {}
    bits: list[str] = []
    score = 5.0
    if pos_bias and _bias_aligns(pos_bias, direction):
        score += 1.5
        bits.append(f"TFF overlay {pos_bias}")
    dxy_label = str(dxy.get("primary_label") or "").lower()
    if direction == "long" and "weak" in dxy_label:
        score += 1.0
        bits.append("Weak dollar supports risk-on longs")
    elif direction == "short" and "strong" in dxy_label:
        score += 1.0
        bits.append("Strong dollar supports USD longs")
    treas_bias = str(treas.get("yield_bias") or treas.get("bond_bias") or "").lower()
    if treas_bias:
        bits.append(f"Treasury regime {treas_bias}")
    return clamp_score(score), "; ".join(bits) if bits else "Neutral macro overlay"


def score_macro_bias_layer(
    *,
    direction: str,
    rs: PillarScore,
    positioning: PillarScore,
    seasonality: PillarScore,
    val_block: dict[str, Any] | None,
) -> LayerResult:
    """Which currency should outperform? RS + positioning + seasonality + DXY/Treasury."""
    gates: list[str] = []
    reasons: list[str] = []

    if direction == "neutral":
        return LayerResult(
            key="macro_bias",
            label="Macro Bias",
            score=0.0,
            grade="C",
            summary="No directional macro bias.",
            reasons=["RS differential too weak for a directional read"],
            gates=["neutral_direction"],
        )

    overlay_sc, overlay_note = _macro_overlay_score(val_block, direction)
    parts: list[tuple[float, float]] = []
    if not rs.missing:
        parts.append((0.40, rs.score))
        reasons.append(f"RS pillar {rs.score}/10 — {rs.summary}")
    if not positioning.missing:
        parts.append((0.30, positioning.score))
        reasons.append(f"Positioning {positioning.score}/10 — {positioning.summary}")
    if not seasonality.missing:
        parts.append((0.15, seasonality.score))
        reasons.append(f"Seasonality {seasonality.score}/10")
    parts.append((0.15, overlay_sc))
    if overlay_note:
        reasons.append(overlay_note)

    weight_sum = sum(w for w, _ in parts) or 1.0
    score = clamp_score(sum(w * s for w, s in parts) / weight_sum)

    if rs.score >= 8.0 and not rs.aligned:
        score = clamp_score(score * 0.75)
        gates.append("rs_magnitude_not_aligned_with_direction")

    grade = grade_from_score_10(score, readiness_grade=None, for_aplus=False)
    return LayerResult(
        key="macro_bias",
        label="Macro Bias",
        score=score,
        grade=grade,
        summary=f"Macro bias {grade} ({score}/10) for {direction} {direction == 'long' and 'base' or 'quote'}.",
        detail=" · ".join(reasons[:4]),
        reasons=reasons,
        gates=gates,
        meta={"overlay_score": overlay_sc, "direction": direction},
    )


def score_valuation_edge_layer(
    *,
    pair: str,
    direction: str,
    val_block: dict[str, Any] | None,
) -> LayerResult:
    """Fair-value edge — not momentum. Penalize chase when macro diff is large but gap is small."""
    gates: list[str] = []
    reasons: list[str] = []

    if not val_block or val_block.get("supported") is False:
        return LayerResult(
            key="valuation_edge",
            label="Valuation Edge",
            score=0.0,
            grade="C",
            summary="Valuation data unavailable.",
            gates=["missing_valuation"],
        )

    bias = str(val_block.get("valuation_bias") or val_block.get("positioning_bias") or "Neutral")
    condition = str(val_block.get("value_condition") or "Fair Value")
    gap = _num(val_block.get("valuation_gap_pct"))
    spot_dev = _num(val_block.get("spot_deviation_pct"))
    pair_diff = _num(val_block.get("pair_score_differential"))
    adj_diff = _num(val_block.get("positioning_adjusted_score_differential"))
    y2 = _num(val_block.get("yield_2y_diff"))
    real_y = _num(val_block.get("real_yield_diff"))
    base_cpi = _num(val_block.get("base_cpi_yoy"))
    quote_cpi = _num(val_block.get("quote_cpi_yoy"))
    inflation_diff = (base_cpi - quote_cpi) if base_cpi is not None and quote_cpi is not None else None

    aligned = _bias_aligns(bias, direction)
    score = 0.0

    # Primary: fair-value gap magnitude when aligned with trade direction
    if gap is not None:
        gap_favors = (gap > 0 and direction == "long") or (gap < 0 and direction == "short")
        gap_mag = abs(gap)
        if gap_favors and aligned:
            score = clamp_score(min(10.0, gap_mag / 1.2))
            reasons.append(f"Fair-value gap {gap:+.1f}% favors {direction} ({condition})")
        elif not gap_favors:
            score = clamp_score(max(0.0, 3.0 - gap_mag / 2.5))
            reasons.append(f"Fair-value gap {gap:+.1f}% opposes {direction}")
            gates.append("value_gap_opposes_direction")
        else:
            score = clamp_score(gap_mag / 2.5)
            reasons.append(f"Gap {gap:+.1f}% present but bias neutral/misaligned")
    else:
        score = 2.0
        reasons.append("No fair-value gap exported — cannot confirm value edge")
        gates.append("missing_fair_value_gap")

    # Spot distance from model fair value (anti-chase)
    if spot_dev is not None:
        if direction == "long":
            if spot_dev > 2.0:
                score = min(score, 4.0)
                gates.append("spot_extended_above_fair_value")
                reasons.append(f"Spot {spot_dev:+.1f}% above fair value — chasing extension")
            elif spot_dev < -0.5:
                score = clamp_score(score + min(2.0, abs(spot_dev) / 2.0))
                reasons.append(f"Spot {spot_dev:+.1f}% below fair value — value remains")
        elif direction == "short":
            if spot_dev < -2.0:
                score = min(score, 4.0)
                gates.append("spot_extended_below_fair_value")
                reasons.append(f"Spot {spot_dev:+.1f}% below fair value — poor short location vs value")
            elif spot_dev > 0.5:
                score = clamp_score(score + min(2.0, spot_dev / 2.0))
                reasons.append(f"Spot {spot_dev:+.1f}% above fair value — supports fade")

    # Hard cap: large macro differential without meaningful gap = momentum not value
    macro_mag = abs(adj_diff if adj_diff is not None else pair_diff or 0.0)
    if macro_mag >= 30.0 and (gap is None or abs(gap) < 4.0):
        prev = score
        score = min(score, 4.0)
        if score < prev:
            gates.append("momentum_without_value_edge")
            reasons.append(
                f"Macro differential {macro_mag:.0f}pts without fair-value gap — momentum not value edge"
            )

    # Rate / real yield support (small additive, not dominant)
    rate_boost = 0.0
    if y2 is not None:
        y2_favors = (y2 > 0 and direction == "long") or (y2 < 0 and direction == "short")
        if y2_favors:
            rate_boost += min(1.0, abs(y2) / 5.0)
            reasons.append(f"2Y differential {y2:+.2f}pp supports direction")
    if real_y is not None:
        ry_favors = (real_y > 0 and direction == "long") or (real_y < 0 and direction == "short")
        if ry_favors:
            rate_boost += min(0.8, abs(real_y) / 4.0)
    if inflation_diff is not None and abs(inflation_diff) >= 0.5:
        reasons.append(f"Inflation differential {inflation_diff:+.1f}pp (context only)")

    overlay = val_block.get("macro_positioning_overlay") or {}
    if _bias_aligns(overlay.get("positioning_bias"), direction):
        rate_boost += 0.5

    score = clamp_score(min(10.0, score + rate_boost))

    if condition.lower().startswith("over") and direction == "long":
        score = min(score, 3.5)
        gates.append("overvalued_blocks_long_edge")
    if condition.lower().startswith("under") and direction == "short":
        score = min(score, 3.5)
        gates.append("undervalued_blocks_short_edge")

    grade = grade_from_score_10(score, readiness_grade=None, for_aplus=False)
    return LayerResult(
        key="valuation_edge",
        label="Valuation Edge",
        score=score,
        grade=grade,
        summary=f"Valuation edge {grade} ({score}/10) — {condition}, gap {gap if gap is not None else 'n/a'}%.",
        detail=" · ".join(reasons[:5]),
        reasons=reasons,
        gates=gates,
        meta={
            "valuation_gap_pct": gap,
            "spot_deviation_pct": spot_dev,
            "value_condition": condition,
            "valuation_bias": bias,
            "pair_score_differential": pair_diff,
        },
    )


def score_trade_readiness_layer(
    *,
    pair: str,
    direction: str,
    location: PillarScore,
) -> LayerResult:
    """Is price in a tradeable location? Weekly structure dominates; daily is confirmatory."""
    gates: list[str] = []
    reasons: list[str] = []
    meta = dict(location.meta or {})

    if location.missing:
        zf = str(location.bias or location.summary or "")
        if zf and zf.lower() not in {"wait", "n/a", ""}:
            sc = 4.0 if location.aligned else 2.0
            return LayerResult(
                key="trade_readiness",
                label="Trade Readiness",
                score=sc,
                grade=grade_from_score_10(sc, readiness_grade=None, for_aplus=False),
                summary=f"Heuristic only — {zf}",
                reasons=["No price bars — zone_focus fallback only"],
                gates=["price_bars_missing"],
                meta={"source": "zone_focus_fallback"},
            )
        return LayerResult(
            key="trade_readiness",
            label="Trade Readiness",
            score=0.0,
            grade="C",
            summary="Trade readiness unavailable.",
            gates=["missing_location_data"],
        )

    weekly_pos = _num(meta.get("weekly_range_position")) or 0.5
    daily_pos = _num(meta.get("daily_range_position")) or 0.5
    spot = _num(meta.get("spot"))
    w_supply = _num(meta.get("weekly_supply"))
    w_demand = _num(meta.get("weekly_demand"))
    d_supply = _num(meta.get("daily_supply"))
    d_demand = _num(meta.get("daily_demand"))
    hvn = _num(meta.get("hvn"))
    lvn = _num(meta.get("lvn"))

    score = 5.0
    state = "Mid-range"

    if direction == "long":
        if weekly_pos >= 0.68:
            score, state = 2.0, "Extended — upper weekly range (near supply)"
            gates.append("weekly_extended_long")
            reasons.append(f"Weekly range position {weekly_pos:.0%} — not a demand entry")
        elif weekly_pos >= 0.52:
            score, state = 4.0, "Upper weekly range — wait for pullback to demand"
            gates.append("weekly_upper_half_long")
            reasons.append(f"Weekly position {weekly_pos:.0%} — macro may be right, location is not")
        elif weekly_pos <= 0.38:
            if daily_pos <= 0.40:
                score, state = 9.5, "At weekly & daily demand — tradeable long zone"
                reasons.append(f"Weekly {weekly_pos:.0%} + daily {daily_pos:.0%} at demand")
            else:
                score, state = 6.5, "Weekly demand held — daily mid-range (confirm on pullback)"
                reasons.append(f"Weekly demand ({weekly_pos:.0%}) but daily bounced ({daily_pos:.0%})")
                gates.append("daily_not_confirming_weekly_demand")
        else:
            score, state = 5.5, "Mid weekly range — not at demand yet"
            reasons.append(f"Weekly mid-range {weekly_pos:.0%} — await demand touch")

        # Penalize if daily says demand but weekly extended (old bug)
        if weekly_pos >= 0.55 and daily_pos <= 0.30:
            score = min(score, 3.5)
            gates.append("daily_demand_weekly_extended_conflict")
            reasons.append("Daily demand zone contradicted by extended weekly location")

    elif direction == "short":
        if weekly_pos <= 0.32:
            score, state = 2.0, "Extended — lower weekly range (near demand)"
            gates.append("weekly_extended_short")
            reasons.append(f"Weekly range position {weekly_pos:.0%} — not a supply entry")
        elif weekly_pos <= 0.48:
            score, state = 4.0, "Lower weekly range — wait for rally to supply"
            gates.append("weekly_lower_half_short")
        elif weekly_pos >= 0.62:
            if daily_pos >= 0.60:
                score, state = 9.5, "At weekly & daily supply — tradeable short zone"
                reasons.append(f"Weekly {weekly_pos:.0%} + daily {daily_pos:.0%} at supply")
            else:
                score, state = 6.5, "Weekly supply zone — daily not fully aligned"
                gates.append("daily_not_confirming_weekly_supply")
        else:
            score, state = 5.5, "Mid weekly range — not at supply yet"
    else:
        score, state = 3.0, "Neutral direction — no readiness edge"

    # Risk/reward to next obstacle
    if spot and w_supply and w_demand and spot > 0:
        if direction == "long":
            upside_pct = (w_supply - spot) / spot * 100.0
            downside_pct = (spot - w_demand) / spot * 100.0
            rr = upside_pct / max(downside_pct, 0.05)
            reasons.append(f"R/R to weekly range ≈ {rr:.1f}:1 (up {upside_pct:.1f}% / down {downside_pct:.1f}%)")
            if rr < 1.2:
                score = clamp_score(score - 2.0)
                gates.append("poor_risk_reward")
            elif rr >= 2.0:
                score = clamp_score(score + 0.5)
        elif direction == "short":
            upside_pct = (spot - w_demand) / spot * 100.0
            downside_pct = (w_supply - spot) / spot * 100.0
            rr = upside_pct / max(downside_pct, 0.05)
            reasons.append(f"R/R to weekly range ≈ {rr:.1f}:1")
            if rr < 1.2:
                score = clamp_score(score - 2.0)
                gates.append("poor_risk_reward")

    # Opposing zone proximity
    if direction == "long" and spot and w_supply:
        dist_supply_pct = (w_supply - spot) / spot * 100.0
        if dist_supply_pct < 0.8:
            score = clamp_score(score - 2.5)
            gates.append("opposing_supply_too_close")
            reasons.append(f"Weekly supply only {dist_supply_pct:.1f}% above spot")
    if direction == "short" and spot and w_demand:
        dist_demand_pct = (spot - w_demand) / spot * 100.0
        if dist_demand_pct < 0.8:
            score = clamp_score(score - 2.5)
            gates.append("opposing_demand_too_close")
            reasons.append(f"Weekly demand only {dist_demand_pct:.1f}% below spot")

    # HVN/LVN context
    if spot and hvn:
        hvn_dist = abs(spot - hvn) / spot * 100.0
        reasons.append(f"Nearest HVN {hvn_dist:.1f}% away")
        if hvn_dist < 0.15 and direction == "long" and weekly_pos >= 0.55:
            score = clamp_score(score - 1.0)
            gates.append("price_at_hvn_under_supply")

    score = clamp_score(score)
    grade = grade_from_score_10(score, readiness_grade=None, for_aplus=False)
    return LayerResult(
        key="trade_readiness",
        label="Trade Readiness",
        score=score,
        grade=grade,
        summary=f"{state} — readiness {grade} ({score}/10).",
        detail=location.detail,
        reasons=reasons,
        gates=gates,
        meta={
            **meta,
            "weekly_range_position": weekly_pos,
            "daily_range_position": daily_pos,
            "location_state": state,
        },
    )


def derive_action_label(
    *,
    direction: str,
    macro: LayerResult,
    valuation: LayerResult,
    readiness: LayerResult,
) -> tuple[str, list[str], str]:
    """Final action label with explicit gates."""
    from hptl.setup_ranking.grades import grade_rank

    gates: list[str] = list(macro.gates) + list(valuation.gates) + list(readiness.gates)
    mr = grade_rank(macro.grade)
    vr = grade_rank(valuation.grade)
    rr = grade_rank(readiness.grade)

    if direction == "neutral":
        gates.append("neutral_direction_no_trade")
        return ACTION_AVOID, gates, "No directional bias — stand aside."

    # Trade Candidate requires readiness B+ (rank >= 3)
    if rr >= 3 and mr >= 3 and vr >= 2:
        if "weekly_extended_long" in gates or "weekly_extended_short" in gates:
            gates.append("trade_candidate_blocked_by_weekly_extension")
            note = "Macro aligned but weekly location extended — wait for pullback."
            return ACTION_PULLBACK, gates, note
        if readiness.score >= 7.0 and macro.score >= 7.0:
            gates.append("passes_trade_candidate_gate")
            return ACTION_TRADE, gates, "Macro, value, and location align for swing entry consideration."

    # High macro, poor readiness
    if mr >= 4 and rr <= 2:
        gates.append("high_macro_poor_readiness_watch_pullback")
        return ACTION_PULLBACK, gates, "Strong macro bias but price not at a tradeable zone — watch pullback."

    if mr >= 3 and rr <= 2:
        gates.append("macro_ok_readiness_weak")
        return ACTION_PULLBACK, gates, "Macro supports direction but location is not ready."

    # Breakout watch: mid readiness, improving macro, near range boundary
    weekly_pos = _num(readiness.meta.get("weekly_range_position"))
    if mr >= 3 and rr == 2 and weekly_pos is not None:
        if (direction == "long" and weekly_pos >= 0.45) or (direction == "short" and weekly_pos <= 0.55):
            gates.append("watch_breakout_or_pullback")
            return ACTION_BREAKOUT, gates, "Macro building — watch for pullback to zone or confirmed breakout."

    if mr <= 2 and vr <= 2:
        return ACTION_AVOID, gates, "Insufficient macro and valuation edge."

    return ACTION_AVOID, gates, "No actionable swing setup — edge or location missing."


def composite_display_grade(
    *,
    macro: LayerResult,
    valuation: LayerResult,
    readiness: LayerResult,
    action_label: str,
) -> str:
    """Display grade capped by readiness per swing-trader rules."""
    from hptl.setup_ranking.grades import grade_rank

    # Base from macro + valuation average
    avg = (macro.score + valuation.score + readiness.score) / 3.0
    base = grade_from_score_10(avg, readiness_grade=None, for_aplus=False)

    rr = grade_rank(readiness.grade)
    br = grade_rank(base)

    # No A+ unless readiness A (rank >= 4)
    if br >= 5 and rr < 4:
        return "A"
    if br >= 5 and rr >= 4:
        return "A+"

    # Trade Candidate cannot show above readiness if readiness is weak
    if action_label == ACTION_PULLBACK and br >= 4:
        return "B+"
    if action_label == ACTION_AVOID:
        return readiness.grade if rr <= 2 else "C"

    return base


def build_layer_debug(
    *,
    pair: str,
    direction: str,
    macro: LayerResult,
    valuation: LayerResult,
    readiness: LayerResult,
    action_label: str,
    action_gates: list[str],
    action_note: str,
    display_grade: str,
) -> dict[str, Any]:
    return {
        "pair": pair,
        "direction": direction,
        "action_label": action_label,
        "display_grade": display_grade,
        "action_note": action_note,
        "macro_high_reason": macro.reasons[0] if macro.reasons else macro.summary,
        "valuation_high_reason": valuation.reasons[0] if valuation.reasons else valuation.summary,
        "readiness_reason": readiness.reasons[0] if readiness.reasons else readiness.summary,
        "macro_score_low_reason": macro.reasons[-1] if macro.score < 6 and macro.reasons else None,
        "valuation_score_low_reason": valuation.reasons[-1] if valuation.score < 6 and valuation.reasons else None,
        "readiness_score_low_reason": readiness.reasons[-1] if readiness.score < 6 and readiness.reasons else None,
        "gates_applied": action_gates,
        "macro_gates": macro.gates,
        "valuation_gates": valuation.gates,
        "readiness_gates": readiness.gates,
    }
