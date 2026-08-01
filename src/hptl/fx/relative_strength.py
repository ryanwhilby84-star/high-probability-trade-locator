"""Relative institutional strength: currency legs → pair differentials."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.fx.currency_map import (
    COT_CURRENCY_SOURCES,
    LEADERBOARD_CURRENCIES,
    FxPairLegs,
    parse_fx_pair,
)
from hptl.markets.instrument_registry import all_instrument_ids, get_instrument, load_registry

RELATIVE_STRENGTH_PATH = Path("data/relative_strength_latest.json")
PUBLIC_RELATIVE_STRENGTH_PATH = Path("web-dashboard/public/data/relative_strength_latest.json")

CONVICTION_HIGH = "HIGH CONVICTION"
CONVICTION_MEDIUM = "MEDIUM CONVICTION"
CONVICTION_LOW = "LOW CONVICTION"
CONVICTION_WATCH = "WATCHLIST ONLY"

FLOW_BULLISH = {
    "long_build",
    "short_covering",
    "accumulation",
}
FLOW_BEARISH = {
    "long_liquidation",
    "profit_taking",
    "short_build",
}


def _finite_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if x == x else default
    except (TypeError, ValueError):
        return default


def _field_float(rec: dict[str, Any], field: str) -> float | None:
    if field not in rec:
        return None
    try:
        x = float(rec.get(field))
        return x if x == x else None
    except (TypeError, ValueError):
        return None


def _weekly_change_input(rec: dict[str, Any]) -> tuple[float, str]:
    for field in ("weekly_change", "one_week_net_change", "net_week_change"):
        value = _field_float(rec, field)
        if value is not None:
            return value, field
    return 0.0, "default_0"


def _open_interest_input(rec: dict[str, Any]) -> float | None:
    value = _field_float(rec, "open_interest")
    if value is not None:
        return value
    groups = rec.get("cot_positioning_groups") or {}
    if isinstance(groups, dict):
        return _field_float(groups, "open_interest")
    return None


def _cot_resolved(rec: dict[str, Any]) -> bool:
    bias = str(rec.get("cot_bias") or "").strip().upper()
    return bool(bias and bias != "N/A" and "no mapped raw COT" not in str(rec.get("missing_reason") or ""))


def _macro_generic(rec: dict[str, Any]) -> bool:
    tx = rec.get("macro_transmission") or (rec.get("institutional_context") or {}).get("macro_transmission") or {}
    return bool(tx.get("generic_rates_only"))


def _flow_direction(inst: dict[str, Any]) -> str:
    fm = str(inst.get("flow_momentum") or "")
    if fm in FLOW_BULLISH:
        return "bullish"
    if fm in FLOW_BEARISH:
        return "bearish"
    return "neutral"


def _regime_direction(inst: dict[str, Any]) -> str:
    reg = str(inst.get("structural_regime") or "")
    if reg in {"structural_bullish", "accumulation"}:
        return "bullish"
    if reg in {"structural_bearish", "distribution"}:
        return "bearish"
    return "neutral"


def score_currency_leg(rec: dict[str, Any], *, currency: str, invert_cot: bool) -> dict[str, Any]:
    """Institutional strength for one currency from its COT dashboard row."""
    inst = rec.get("institutional_context") or {}
    att = inst.get("attention") or {}

    cot_raw = _finite_float(rec.get("cot_score"), 0.0)
    if invert_cot:
        cot_raw = -cot_raw
    cot_component = round(max(-30.0, min(30.0, cot_raw * 3.0)), 1)

    regime_dir = _regime_direction(inst)
    regime_bonus = 0.0
    if regime_dir == "bullish":
        regime_bonus = 8.0
    elif regime_dir == "bearish":
        regime_bonus = -8.0
    weeks = int(inst.get("weeks_in_regime") or 0)
    if weeks >= 4:
        regime_bonus += 4.0 if regime_bonus > 0 else (-4.0 if regime_bonus < 0 else 0)

    flow_dir = _flow_direction(inst)
    w1_raw, weekly_change_source = _weekly_change_input(rec)
    w1 = -w1_raw if invert_cot else w1_raw
    flow_intensity = min(25.0, abs(w1) / 4000.0)
    flow_sign = 1.0 if flow_dir == "bullish" else (-1.0 if flow_dir == "bearish" else (1.0 if w1 > 0 else (-1.0 if w1 < 0 else 0.0)))
    flow_component = round(flow_sign * flow_intensity + regime_bonus * 0.35, 1)

    macro_layer = inst.get("macro_alignment_score")
    if macro_layer is None:
        macro_layer = (inst.get("internal_scores") or {}).get("macro_alignment_score")
    macro_component = round((_finite_float(macro_layer, 50.0) - 50.0) * 0.5, 1)
    if _macro_generic(rec):
        macro_component *= 0.45

    anomaly_component = 0.0
    for a in att.get("alerts") or []:
        kind = a.get("kind", "")
        if kind == "flow_extreme":
            anomaly_component += 8.0
        elif kind == "transition":
            anomaly_component += 6.0
        elif kind == "exhaustion":
            anomaly_component -= 4.0
    anomaly_component = round(max(-12.0, min(18.0, anomaly_component)), 1)

    crowding_penalty = 0.0
    extreme = str(inst.get("positioning_extreme") or "none")
    if extreme in {"euphoric_longs", "crowded_longs"}:
        crowding_penalty += 12.0
    elif extreme in {"crowded_shorts", "capitulation_shorts"}:
        crowding_penalty += 8.0

    macro_contra = 0.0
    align = str(inst.get("macro_alignment") or "")
    if align in {"strong_contradiction", "risk_off_pressure"}:
        macro_contra += 10.0
    if inst.get("flow_l1_l2_conflict"):
        macro_contra += 6.0

    confidence = 1.0
    if _macro_generic(rec):
        confidence -= 0.25
    if str(rec.get("data_status") or "") not in {"complete", "macro_only"}:
        confidence -= 0.15
    confidence = round(max(0.35, min(1.0, confidence)), 2)

    raw = cot_component + flow_component + macro_component + anomaly_component
    penalty = crowding_penalty + macro_contra
    final = round(max(-100.0, min(100.0, raw - penalty)), 1)
    final = round(final * confidence, 1)

    drivers: list[tuple[float, str]] = [
        (abs(cot_component), f"COT positioning ({rec.get('cot_bias', 'N/A')})"),
        (abs(flow_component), f"Flow: {inst.get('flow_momentum_label', flow_dir)}"),
        (abs(macro_component), f"Macro: {inst.get('macro_alignment_label', align)}"),
    ]
    drivers.sort(reverse=True)
    strongest_driver = drivers[0][1] if drivers else "Insufficient data"

    risks: list[tuple[float, str]] = [
        (crowding_penalty, f"Crowding: {extreme.replace('_', ' ')}" if extreme != "none" else ""),
        (macro_contra, "Macro/structure tension" if macro_contra else ""),
    ]
    risks = [(w, t) for w, t in risks if t and w > 0]
    risks.sort(reverse=True)
    biggest_risk = risks[0][1] if risks else "No dominant risk flag"

    return {
        "currency": currency,
        "cot_market": str(rec.get("market") or ""),
        "cot_component": cot_component,
        "macro_component": macro_component,
        "flow_component": flow_component,
        "crowding_penalty": round(crowding_penalty, 1),
        "macro_contradiction_penalty": round(macro_contra, 1),
        "anomaly_component": anomaly_component,
        "confidence_modifier": confidence,
        "penalty_for_missing_data": round(penalty, 1),
        "final_score": final,
        "strongest_driver": strongest_driver,
        "biggest_risk": biggest_risk,
        "data_source": "direct_cot",
        "invert_cot_applied": invert_cot,
        "flow_momentum": inst.get("flow_momentum"),
        "structural_regime": inst.get("structural_regime"),
        "macro_alignment": align,
        "weekly_change_used": w1,
        "weekly_change_raw": w1_raw,
        "weekly_change_source": weekly_change_source,
        "one_week_net_change": _field_float(rec, "one_week_net_change"),
        "net_week_change": _field_float(rec, "net_week_change"),
        "open_interest": _open_interest_input(rec),
        "data_integrity_status": "PASS" if weekly_change_source != "default_0" and _open_interest_input(rec) is not None else "FAIL",
        "fx_flow_input_audit": {
            "currency": currency,
            "weekly_change_used": w1,
            "weekly_change_raw": w1_raw,
            "weekly_change_source": weekly_change_source,
            "one_week_net_change": _field_float(rec, "one_week_net_change"),
            "net_week_change": _field_float(rec, "net_week_change"),
            "open_interest": _open_interest_input(rec),
            "flow_component": flow_component,
        },
    }


def synthesize_usd(leg_scores: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """USD pressure proxy from G10 legs (not a standalone COT contract)."""
    # Positive score = stronger USD vs the referenced G10 currency leg logic.
    contributions: list[float] = []
    weights: list[float] = []
    for code, sign in (
        ("EUR", -1.0),
        ("GBP", -1.0),
        ("AUD", -1.0),
        ("NZD", -1.0),
        ("JPY", 1.0),
        ("CHF", 1.0),
        ("CAD", 1.0),
    ):
        leg = leg_scores.get(code)
        if not leg:
            continue
        contributions.append(sign * float(leg["final_score"]))
        weights.append(float(leg["confidence_modifier"]))

    if not contributions:
        final = 0.0
    else:
        wsum = sum(weights) or 1.0
        final = round(sum(c * w for c, w in zip(contributions, weights, strict=False)) / wsum, 1)

    return {
        "currency": "USD",
        "cot_market": None,
        "cot_component": 0.0,
        "macro_component": round(final * 0.35, 1),
        "flow_component": round(final * 0.4, 1),
        "crowding_penalty": 0.0,
        "macro_contradiction_penalty": 0.0,
        "anomaly_component": 0.0,
        "confidence_modifier": 0.55,
        "penalty_for_missing_data": 0.0,
        "final_score": final,
        "strongest_driver": "Synthesized from G10 COT legs (inverse basket)",
        "biggest_risk": "USD leg is approximate — not direct COT",
        "data_source": "synthetic_usd",
        "synthetic_usd": True,
    }


def conviction_tier(differential: float, conf: float) -> str:
    ad = abs(differential)
    if ad >= 35 and conf >= 0.65:
        return CONVICTION_HIGH
    if ad >= 22 and conf >= 0.50:
        return CONVICTION_MEDIUM
    if ad >= 12 and conf >= 0.35:
        return CONVICTION_LOW
    if ad >= 6:
        return CONVICTION_WATCH
    return CONVICTION_WATCH


def pair_momentum(
    base_leg: dict[str, Any],
    quote_leg: dict[str, Any],
    bias: str,
) -> str:
    """expanding | fading | mixed"""
    bf = _flow_direction(base_leg if "flow_momentum" in base_leg else {"flow_momentum": base_leg.get("flow_momentum")})
    qf = _flow_direction(quote_leg if "flow_momentum" in quote_leg else {"flow_momentum": quote_leg.get("flow_momentum")})
    if bias == "bullish" and bf in {"bullish", "neutral"} and qf in {"bearish", "neutral"}:
        return "expanding"
    if bias == "bearish" and bf in {"bearish", "neutral"} and qf in {"bullish", "neutral"}:
        return "expanding"
    if bf == "neutral" and qf == "neutral":
        return "mixed"
    return "fading"


DISPLAY_PAIR_TOP_N = 15
G10_AUDIT_TOP_N = 56


def _crowding_penalty_pair(base: dict[str, Any], quote: dict[str, Any]) -> tuple[float, list[str], dict[str, float]]:
    penalties: dict[str, float] = {}
    warnings: list[str] = []
    for leg, label in ((base, "base"), (quote, "quote")):
        ex = str(leg.get("positioning_extreme") or "")
        if ex in {"euphoric_longs", "crowded_longs"}:
            penalties[f"crowding_{label}"] = 4.0
            warnings.append(f"{leg.get('currency', label)}: {ex.replace('_', ' ')}")
        elif ex in {"crowded_shorts", "capitulation_shorts"}:
            penalties[f"crowding_{label}"] = 3.0
            warnings.append(f"{leg.get('currency', label)}: {ex.replace('_', ' ')}")
    return round(sum(penalties.values()), 1), warnings, penalties


def audit_pair(
    *,
    pair_id: str,
    base_code: str,
    quote_code: str,
    legs: dict[str, dict[str, Any]],
    in_registry: bool,
    pair_source: str,
) -> dict[str, Any] | None:
    """Full auditable pair row — raw differential is the primary ranking key."""
    base = legs.get(base_code)
    quote = legs.get(quote_code)
    if not base or not quote:
        return None

    if base.get("data_source") == "missing" or quote.get("data_source") == "missing":
        return {
            "pair": pair_id,
            "base": base_code,
            "quote": quote_code,
            "pair_source": pair_source,
            "in_registry": in_registry,
            "raw_differential_score": None,
            "adjusted_opportunity_score": 0.0,
            "downgrade_penalties": {},
            "confidence_score": 0.0,
            "exclusion_reason": "missing_leg_cot",
            "final_rank": None,
            "included_in_display": False,
            "display_exclusion_reason": "missing_leg_cot",
        }

    raw_diff = round(float(base["final_score"]) - float(quote["final_score"]), 1)
    raw_abs = abs(raw_diff)

    if raw_abs < 4:
        bias, arrow = "neutral", "→"
    elif raw_diff > 0:
        bias, arrow = "bullish", "↑"
    else:
        bias, arrow = "bearish", "↓"

    conf = min(float(base["confidence_modifier"]), float(quote["confidence_modifier"]))
    tier = conviction_tier(raw_diff, conf)

    crowd_total, crowd_warn, crowd_parts = _crowding_penalty_pair(base, quote)
    downgrade_penalties: dict[str, float] = dict(crowd_parts)

    if conf < 0.5:
        downgrade_penalties["low_confidence"] = round((0.5 - conf) * 20.0, 1)
    if base.get("synthetic_usd") or quote.get("synthetic_usd"):
        downgrade_penalties["synthetic_usd_leg"] = 6.0
    if not in_registry:
        downgrade_penalties["not_tradable_in_registry"] = 0.0  # informational only — does not change rank

    penalty_total = round(sum(v for k, v in downgrade_penalties.items() if k != "not_tradable_in_registry"), 1)
    adjusted = round(max(0.0, raw_abs * conf - penalty_total), 1)

    display_exclusion: str | None = None
    if not in_registry:
        display_exclusion = "g10_theoretical_cross_not_in_oanda_registry"

    return {
        "pair": pair_id,
        "base": base_code,
        "quote": quote_code,
        "pair_source": pair_source,
        "in_registry": in_registry,
        "raw_differential_score": raw_diff,
        "raw_differential_abs": raw_abs,
        "adjusted_opportunity_score": adjusted,
        "downgrade_penalties": downgrade_penalties,
        "penalty_total": penalty_total,
        "confidence_score": round(conf, 2),
        "conviction": tier,
        "directional_bias": bias,
        "direction_arrow": arrow,
        "base_score": base["final_score"],
        "quote_score": quote["final_score"],
        "differential": raw_diff,
        "macro_alignment_confidence": round(conf, 2),
        "crowding_warning": "; ".join(crowd_warn) if crowd_warn else None,
        "momentum": pair_momentum(base, quote, bias),
        "base_driver": base.get("strongest_driver"),
        "quote_driver": quote.get("strongest_driver"),
        "biggest_risk": base.get("biggest_risk") if raw_abs > 0 else "Flat differential",
        "low_confidence_cross": conf < 0.5 or bool(base.get("synthetic_usd") or quote.get("synthetic_usd")),
        "exclusion_reason": None,
        "display_exclusion_reason": display_exclusion,
        "final_rank": None,
        "rank_by_raw_differential": None,
        "rank_by_adjusted_score": None,
        "included_in_display": False,
        "ranking_method_display": "raw_differential_abs",
    }


def build_pair_opportunity(
    pair: FxPairLegs,
    legs: dict[str, dict[str, Any]],
) -> dict[str, Any] | None:
    """Back-compat wrapper around audit_pair."""
    row = audit_pair(
        pair_id=pair.instrument_id,
        base_code=pair.base,
        quote_code=pair.quote,
        legs=legs,
        in_registry=True,
        pair_source="oanda_registry",
    )
    if row and row.get("exclusion_reason") == "missing_leg_cot":
        return None
    return row


def _finalize_pair_ranks(audit_rows: list[dict[str, Any]], *, display_top_n: int) -> list[dict[str, Any]]:
    """Assign ranks; display board sorted by |raw differential| (not conviction filter)."""
    eligible = [r for r in audit_rows if r.get("raw_differential_score") is not None]
    by_raw = sorted(eligible, key=lambda x: float(x["raw_differential_abs"]), reverse=True)
    by_adj = sorted(eligible, key=lambda x: float(x["adjusted_opportunity_score"]), reverse=True)

    raw_rank = {r["pair"]: i + 1 for i, r in enumerate(by_raw)}
    adj_rank = {r["pair"]: i + 1 for i, r in enumerate(by_adj)}

    for r in audit_rows:
        pid = r["pair"]
        r["rank_by_raw_differential"] = raw_rank.get(pid)
        r["rank_by_adjusted_score"] = adj_rank.get(pid)
        r["final_rank"] = raw_rank.get(pid)

    # Display only the stronger-base direction (avoids GBP/JPY and JPY/GBP both consuming slots).
    by_display = sorted(
        [r for r in eligible if float(r["raw_differential_score"]) > 0],
        key=lambda x: float(x["raw_differential_abs"]),
        reverse=True,
    )
    for r in eligible:
        if float(r["raw_differential_score"]) < 0 and r.get("display_exclusion_reason") is None:
            inv = f"{r['quote']}/{r['base']}"
            r["display_exclusion_reason"] = f"inverse_pair_see_{inv}"

    display_set = {r["pair"] for r in by_display[:display_top_n]}
    for r in audit_rows:
        included = r["pair"] in display_set and r.get("raw_differential_score") is not None
        r["included_in_display"] = included
        if included:
            r["exclusion_reason"] = None
            continue
        if r.get("exclusion_reason"):
            r["display_exclusion_reason"] = r["exclusion_reason"]
        elif r.get("rank_by_raw_differential"):
            r["display_exclusion_reason"] = f"ranked_out_position_{r['rank_by_raw_differential']}"
        else:
            r["display_exclusion_reason"] = r.get("display_exclusion_reason") or "not_ranked"

    return by_raw


def build_g10_pair_audit(legs: dict[str, dict[str, Any]], registry_pairs: set[str]) -> list[dict[str, Any]]:
    """All G10 leaderboard crosses (theoretical + tradable)."""
    codes = [c for c in LEADERBOARD_CURRENCIES if c in legs]
    rows: list[dict[str, Any]] = []
    for base in codes:
        for quote in codes:
            if base == quote:
                continue
            pair_id = f"{base}/{quote}"
            row = audit_pair(
                pair_id=pair_id,
                base_code=base,
                quote_code=quote,
                legs=legs,
                in_registry=pair_id in registry_pairs,
                pair_source="g10_leg_matrix",
            )
            if row:
                rows.append(row)
    return rows


def _commodity_heat_row(rec: dict[str, Any]) -> dict[str, Any] | None:
    if not _cot_resolved(rec):
        return None
    inst = rec.get("institutional_context") or {}
    cot = _finite_float(rec.get("cot_score"), 0.0)
    macro = _finite_float(rec.get("macro_score"), 5.0) - 5.0
    score = round(cot * 6.0 + macro * 4.0, 1)
    return {
        "instrument_id": rec.get("market"),
        "asset_class": (rec.get("instrument_meta") or {}).get("asset_class", "commodities"),
        "score": score,
        "cot_bias": rec.get("cot_bias"),
        "macro_signal": rec.get("macro_regime") or rec.get("macro_signal"),
    }


def build_relative_strength(
    week_records: list[dict[str, Any]],
    *,
    calendar_week: str = "",
) -> dict[str, Any]:
    week_by_market = {str(r.get("market")): r for r in week_records if r.get("market")}
    leg_scores: dict[str, dict[str, Any]] = {}

    for code, meta in COT_CURRENCY_SOURCES.items():
        market = str(meta["market"])
        rec = week_by_market.get(market)
        if not rec or not _cot_resolved(rec):
            leg_scores[code] = {
                "currency": code,
                "final_score": 0.0,
                "confidence_modifier": 0.35,
                "data_source": "missing",
                "strongest_driver": "No COT row this week",
                "biggest_risk": "Missing positioning data",
                "cot_component": 0.0,
                "macro_component": 0.0,
                "flow_component": 0.0,
                "crowding_penalty": 0.0,
                "macro_contradiction_penalty": 0.0,
                "penalty_for_missing_data": 0.0,
                "anomaly_component": 0.0,
            }
            continue
        rec = dict(rec)
        if "open_interest" not in rec:
            rec["open_interest"] = _open_interest_input(rec)
        leg = score_currency_leg(rec, currency=code, invert_cot=bool(meta["invert_cot"]))
        leg["positioning_extreme"] = (rec.get("institutional_context") or {}).get("positioning_extreme")
        leg_scores[code] = leg

    leg_scores["USD"] = synthesize_usd(leg_scores)

    currency_leaderboard = sorted(
        [leg_scores[c] for c in LEADERBOARD_CURRENCIES if c in leg_scores],
        key=lambda x: float(x["final_score"]),
        reverse=True,
    )
    for i, row in enumerate(currency_leaderboard):
        row["rank"] = i + 1

    reg = load_registry()
    registry_pair_ids: set[str] = set()
    registry_only_rows: list[dict[str, Any]] = []
    for iid in all_instrument_ids():
        spec = reg.get(iid)
        if not spec or spec.asset_class != "fx":
            continue
        parsed = parse_fx_pair(iid)
        if not parsed:
            continue
        registry_pair_ids.add(parsed.instrument_id)
        row = audit_pair(
            pair_id=parsed.instrument_id,
            base_code=parsed.base,
            quote_code=parsed.quote,
            legs=leg_scores,
            in_registry=True,
            pair_source="oanda_registry",
        )
        if row:
            registry_only_rows.append(row)

    g10_audit = build_g10_pair_audit(leg_scores, registry_pair_ids)
    g10_ids = {r["pair"] for r in g10_audit}
    em_extra = [r for r in registry_only_rows if r["pair"] not in g10_ids]
    pair_audit_all = g10_audit + em_extra

    by_raw_display = _finalize_pair_ranks(pair_audit_all, display_top_n=DISPLAY_PAIR_TOP_N)
    pair_ops = [r for r in by_raw_display if r.get("included_in_display")]

    high_conviction = [
        r for r in sorted(
            [x for x in pair_audit_all if x.get("conviction") == CONVICTION_HIGH and x.get("raw_differential_score") is not None],
            key=lambda x: float(x["raw_differential_abs"]),
            reverse=True,
        )
    ]

    commodities: list[dict[str, Any]] = []
    for rec in week_records:
        meta = rec.get("instrument_meta") or {}
        ac = meta.get("asset_class") or ""
        if ac not in {"commodities", "metals"}:
            continue
        row = _commodity_heat_row(rec)
        if row:
            commodities.append(row)
    commodities.sort(key=lambda x: float(x["score"]), reverse=True)

    themes: dict[str, list[dict[str, Any]]] = {}
    for rec in week_records:
        if not _cot_resolved(rec):
            continue
        sub = (rec.get("instrument_meta") or {}).get("subgroup") or "other"
        inst = rec.get("institutional_context") or {}
        align = str(inst.get("macro_alignment") or "neutral")
        themes.setdefault(sub, []).append(
            {
                "market": rec.get("market"),
                "macro_alignment": align,
                "score": _finite_float(rec.get("cot_score"), 0) * 5 + _finite_float(rec.get("macro_score"), 5),
            }
        )
    theme_summary = {
        k: sorted(v, key=lambda x: x["score"], reverse=True)[:5]
        for k, v in sorted(themes.items(), key=lambda kv: -len(kv[1]))
    }

    return {
        "calendar_week": calendar_week,
        "generated_from": "hptl.fx.relative_strength",
        "currency_leaderboard": currency_leaderboard,
        "pair_opportunities": pair_ops,
        "pair_opportunities_all_count": len([r for r in pair_audit_all if r.get("raw_differential_score") is not None]),
        "high_conviction_pairs": high_conviction,
        "pair_audit_all": pair_audit_all,
        "ranking_rules": {
            "display_board": "Sorted by |raw_differential_score| descending (leg_base − leg_quote).",
            "adjusted_opportunity_score": "|raw| × min(leg_confidence) − downgrade_penalties (crowding, low confidence, synthetic USD).",
            "not_used_for_display": "Conviction tier alone does NOT reorder the board (fixes prior high_conviction-only UI bug).",
            "g10_matrix": "All LEADERBOARD_CURRENCIES crosses included even if not in OANDA registry (e.g. GBP/JPY).",
            "registry_em_pairs": "EM crosses (TRY, ZAR, …) audited separately when in registry; may lack COT-backed legs.",
        },
        "heatmap": {
            "strongest_currencies": currency_leaderboard[:4],
            "weakest_currencies": list(reversed(currency_leaderboard[-4:])),
            "strongest_commodities": commodities[:6],
            "strongest_themes": theme_summary,
        },
        "commodity_ranks": commodities[:20],
        "limitations": [
            "USD score is synthesized from G10 COT legs, not a direct CFTC contract.",
            "Pairs not in OANDA registry appear in G10 audit but are flagged not_tradable_in_registry.",
            "EM crosses (TRY, ZAR, etc.) may lack COT-backed legs — see exclusion_reason in pair_audit_all.",
            "CHF/CAD use invert_cot for USD/XXX futures quoting; JPY uses CME 6J (yen-value) with invert_cot=False.",
            "Display board uses raw differential, not conviction-filtered subset.",
            "This layer does not generate entries, stops, or targets.",
        ],
        "audit": {
            "leg_scores": leg_scores,
            "fx_leg_flow_inputs": [
                leg_scores[c]["fx_flow_input_audit"]
                for c in LEADERBOARD_CURRENCIES
                if c in leg_scores and "fx_flow_input_audit" in leg_scores[c]
            ],
            "top_pair_differentials_by_raw": by_raw_display[:30],
            "top_pair_differentials_by_adjusted": sorted(
                [r for r in pair_audit_all if r.get("adjusted_opportunity_score") is not None],
                key=lambda x: float(x["adjusted_opportunity_score"]),
                reverse=True,
            )[:30],
            "displayed_pairs": pair_ops,
            "not_displayed_stronger_than_cutoff": [
                {
                    "pair": r["pair"],
                    "raw_differential_score": r["raw_differential_score"],
                    "rank_by_raw_differential": r.get("rank_by_raw_differential"),
                    "display_exclusion_reason": r.get("display_exclusion_reason"),
                }
                for r in by_raw_display[DISPLAY_PAIR_TOP_N : DISPLAY_PAIR_TOP_N + 10]
            ],
        },
    }


def write_relative_strength(
    payload: dict[str, Any],
    *,
    path: Path | None = None,
    public_path: Path | None = None,
) -> Path:
    out = path or RELATIVE_STRENGTH_PATH
    pub = public_path or PUBLIC_RELATIVE_STRENGTH_PATH
    out.parent.mkdir(parents=True, exist_ok=True)
    pub.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    out.write_text(text, encoding="utf-8")
    pub.write_text(text, encoding="utf-8")
    return out
