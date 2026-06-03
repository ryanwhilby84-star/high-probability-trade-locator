"""Global regime snapshot + per-instrument macro/news/sentiment *impact* copy.

Framed as contextual intelligence only — no entries, zones, or price calls.
"""
from __future__ import annotations

from typing import Any

from hptl.news.asset_sensitivity import get_profile


def _str(x: Any) -> str:
    if x is None:
        return ""
    return str(x).strip()


def _macro_signal_from_audit(audit: dict[str, Any]) -> str:
    if not audit.get("available"):
        return "unavailable"
    rr = audit.get("resolved_regime") or {}
    return _str(rr.get("macro_signal")).lower() or "unknown"


def derive_global_market_regime(macro_audit: dict[str, Any]) -> dict[str, Any]:
    """Broad environment fields for the GLOBAL MARKET REGIME panel (rates layer + placeholders)."""
    if not macro_audit.get("available"):
        return {
            "risk_regime": "source unavailable",
            "inflation_regime": "source unavailable",
            "liquidity_regime": "source unavailable",
            "usd_impulse": "source unavailable",
            "rates_pressure": "source unavailable",
            "rates_pressure_technical": "source unavailable",
            "news_intensity": "source unavailable",
            "sentiment_interference": "source unavailable",
            "summary": macro_audit.get("reason") or "Macro rates snapshot unavailable for this week.",
            "summary_technical": macro_audit.get("reason") or "source unavailable",
            "rates_snapshot_date": macro_audit.get("rates_snapshot_date"),
        }

    rr = macro_audit.get("resolved_regime") or {}
    macro_signal = _str(rr.get("macro_signal"))
    rates_bias = _str(rr.get("rates_bias"))
    policy = _str(rr.get("policy_pressure"))
    curve = _str(rr.get("curve_state") or rr.get("curve_context"))
    macro_rationale = _str(rr.get("macro_rationale"))

    if macro_signal == "risk_on":
        risk = "Leaning risk-on (rates macro score favors easing/steepening-aligned counts vs restrictive)"
    elif macro_signal == "risk_off":
        risk = "Leaning risk-off (rates macro score aligns with restrictive yields/flattening/policy pressure narratives)"
    elif macro_signal == "source unavailable":
        risk = "source unavailable"
    else:
        risk = "Mixed / neutral rates regime (risk_on vs risk_off counts tied or ambiguous)"

    d10 = macro_audit.get("one_week_deltas_pp") or {}
    dgs10 = _str(d10.get("dgs10"))
    rates_pressure_technical = (
        f"FRED rates snapshot: macro_signal={macro_signal}, rates_bias={rates_bias}, "
        f"policy_pressure={policy}, curve_state={curve} (DGS10 1w Δ {dgs10} pp where available)"
    )
    rb = rates_bias if rates_bias else "mixed"
    pol_disp = (
        policy.lower()
        if policy and str(policy).strip().lower() not in {"", "source unavailable", "n/a"}
        else "mixed"
    )
    rates_pressure = (
        f"Treasury backdrop reads {rb.lower()} with a {curve or 'unknown'} curve and "
        f"{pol_disp} policy pressure — context for risk assets, not a trade call."
    )

    out = macro_audit.get("counts") or {}
    ro = out.get("risk_on_aligned")
    rff = out.get("risk_off_aligned")
    winner = _str(macro_audit.get("winner"))
    summary_technical = (
        f"Rates winner={winner}; risk_on_aligned={ro}, risk_off_aligned={rff}. "
        f"{risk} Macro rationale (rates layer): {macro_rationale or 'source unavailable'}"
    )
    digest_bits = [risk.rstrip(".")]
    if macro_rationale and macro_rationale not in {"", "source unavailable"}:
        digest_bits.append(macro_rationale.rstrip("."))
    if dgs10 and dgs10.lower() not in {"", "nan", "none"}:
        digest_bits.append(f"10-year yield shifted about {dgs10} over the week where data exists")
    clean_bits = [b.rstrip(".") for b in digest_bits if b]
    summary = ". ".join(clean_bits) + "." if clean_bits else "Rates context is limited for this snapshot."

    return {
        "risk_regime": risk,
        "inflation_regime": "source unavailable",
        "liquidity_regime": "source unavailable",
        "usd_impulse": "source unavailable",
        "rates_pressure": rates_pressure,
        "rates_pressure_technical": rates_pressure_technical,
        "news_intensity": "source unavailable",
        "sentiment_interference": "source unavailable",
        "macro_rationale": macro_rationale or "source unavailable",
        "curve_state": curve or "source unavailable",
        "summary": summary,
        "summary_technical": summary_technical,
        "rates_snapshot_date": macro_audit.get("rates_snapshot_date"),
        "resolved_macro_signal": macro_signal,
        "resolved_macro_score": rr.get("macro_score"),
    }


def _interference_from_percentile(pct: Any) -> tuple[str, str]:
    """Heuristic distortion label from historical net percentile (context only)."""
    try:
        p = float(pct)
    except (TypeError, ValueError):
        return "UNKNOWN", "Net percentile unavailable — treat emotional crowding context as ambiguous."
    if not _finite(p):
        return "UNKNOWN", "Net percentile unavailable — treat emotional crowding context as ambiguous."
    ext = p >= 90.0 or p <= 10.0
    high = p >= 93.0 or p <= 7.0
    if high:
        return (
            "HIGH",
            "Positioning versus full loaded history sits in an extreme percentile band — narratives and fast money can distort short-term pricing quality.",
        )
    if ext:
        return (
            "MODERATE",
            "Positioning is historically stretched versus the full dataset — psychology may meaningfully compete with slower macro impulses.",
        )
    return (
        "LOW",
        "Positioning versus history is not in an extreme band — psychology appears less dominant as a distorting overlay right now.",
    )


def _finite(x: float) -> bool:
    import math

    return math.isfinite(x)


def build_instrument_intel_context(
    market: str,
    *,
    cot_bias: str,
    cot_score: Any,
    positioning_state: str,
    macro_regime: str,
    macro_score: Any,
    final_context: str,
    institutional_flow_summary: str,
    macro_audit: dict[str, Any],
    global_market_regime: dict[str, Any],
    full_loaded_net_pct: Any,
) -> dict[str, str]:
    profile = get_profile(market)
    glob_summary = global_market_regime.get("summary") or ""

    if profile is None:
        return {
            "cot_engine_summary": f"{market}: COT bias {cot_bias} (score {cot_score}); state {positioning_state}.",
            "macro_impact": "No sensitivity profile is defined for this market key yet.",
            "news_catalysts": "Add a profile to list recurring headline channels.",
            "sentiment_interference": _interference_from_percentile(full_loaded_net_pct)[1],
            "final_context_summary": f"Macro regime label: {macro_regime}; final context snapshot: {final_context}. Interpretation above is contextual — not an entry.",
        }

    sens = "; ".join(profile.sensitivities)
    mr = macro_regime if macro_regime not in {"", "N/A"} else _macro_signal_from_audit(macro_audit)

    alignment = ""
    if mr.lower().find("risk_on") >= 0:
        alignment = (
            "Broad rates macro skew is risk-on-ish; cyclical and liquidity-sensitive assets generally get a mild tailwind overlay, "
            "while pure defensive hedges rely more on idiosyncratic catalysts."
        )
    elif mr.lower().find("risk_off") >= 0:
        alignment = (
            "Broad rates macro skew is risk-off-ish; tightening and flattening impulses often pressure duration and growth narratives first, "
            "while defensive/safe-flow assets can rerate on shocks."
        )
    else:
        alignment = (
            "Macro regime label is mixed or unavailable at the snapshot; interpret asset moves through your sensitivity list rather than a single directional macro call."
        )

    commodity_note = ""
    mk = market.lower()
    if any(k in mk for k in ("gold", "silver", "oil", "gas", "copper", "coffee", "cocoa", "corn", "wheat", "soy")):
        commodity_note = " For commodities, headline supply stories can temporarily dominate the macro risk factor — sanity-check narratives against inventories and calendar risk."

    macro_impact = (
        f"{profile.market} is especially sensitive to: {sens}. {profile.stress_note} "
        f"Against the latest global regime read: {glob_summary[:320]}{'…' if len(_str(glob_summary)) > 320 else ''} "
        f"{alignment}{commodity_note} "
        f"Institutional read on this tape: positioning state «{positioning_state}», COT bias «{cot_bias}» (score {cot_score}); "
        f"these can agree or disagree with the macro impulse — divergence is informational, not a trigger."
    )

    catalysts = (
        "Recurring channels to scan (not exhaustive): "
        + ", ".join(profile.news_lens)
        + ". When the GDELT/calendar pipelines run, prioritize headlines that intersect these sensitivities "
        + "rather than unrelated micro-stories."
    )

    lvl, expl = _interference_from_percentile(full_loaded_net_pct)
    sentiment_block = (
        f"Sentiment interference (distortion heuristic, not directional sentiment): {lvl}. "
        f"{expl} Macro label is «{macro_regime}». This is secondary context — it cannot tell you entries; it warns when psychology crowds one side."
    )

    final_sum = (
        f"COT: {cot_bias}, state «{positioning_state}» — {institutional_flow_summary[:240]}"
        f"{'…' if len(_str(institutional_flow_summary)) > 240 else ''} "
        f"Macro backdrop: «{macro_regime}» (score {macro_score}); dashboard final context snapshot: «{final_context}». "
        f"Trade environment characterization only: when macro confirms positioning narratives, contexts often feel smoother; "
        f"when they conflict, expect noisier, more event-driven tapes. Prefer clean location (human discretion) over chasing narrative velocity."
    )

    cot_engine = (
        f"Institutional lean: {cot_bias} (engine score {cot_score}), tape state {positioning_state}. "
        f"Use the COT narrative below for the full scoring path — this line is only a quick anchor."
    )

    return {
        "cot_engine_summary": cot_engine,
        "macro_impact": macro_impact,
        "news_catalysts": catalysts,
        "sentiment_interference": sentiment_block,
        "final_context_summary": final_sum,
    }
