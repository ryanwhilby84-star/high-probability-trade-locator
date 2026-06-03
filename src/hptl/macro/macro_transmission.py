"""Asset-specific macro transmission — translate global rates/macro into per-market implications."""

from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.macro.macro_scoring import (
    _rates_alignment_breakdown,
    _row_has_required_scoring_inputs,
    _score_complete_row,
)
from hptl.news.asset_sensitivity import get_profile

Impact = str  # supportive | headwind | neutral | mixed | unknown

ASSET_BUCKET: dict[str, str] = {
    "Gold": "gold",
    "Silver": "silver",
    "Copper / HG": "copper",
    "NASDAQ / NQ": "equity",
    "S&P 500 / ES": "equity",
    "Dow / YM": "equity",
    "Euro FX / 6E": "fx_major",
    "British Pound / 6B": "fx_major",
    "Japanese Yen / 6J": "fx_safe_haven",
    "Swiss Franc / 6S": "fx_safe_haven",
    "Australian Dollar / 6A": "fx_commodity",
    "Canadian Dollar / 6C": "fx_commodity",
    "NZ Dollar / 6N": "fx_commodity",
    "Crude Oil / CL": "energy",
    "Natural Gas / NG": "natgas",
    "Wheat": "ag",
    "Corn": "ag",
    "Soybeans": "ag",
    "Coffee": "soft",
    "Cocoa": "soft",
}

# Primary drivers per bucket (order = display priority)
BUCKET_DRIVERS: dict[str, list[str]] = {
    "gold": ["real_yields", "usd", "fed_policy", "risk_sentiment", "inflation_channel"],
    "silver": ["real_yields", "usd", "industrial_demand", "risk_sentiment"],
    "copper": ["china_growth", "usd", "industrial_demand", "risk_sentiment", "nominal_yields"],
    "equity": ["nominal_yields", "liquidity_curve", "fed_policy", "risk_sentiment"],
    "fx_safe_haven": ["risk_sentiment", "rate_differentials", "usd"],
    "fx_major": ["rate_differentials", "usd", "risk_sentiment", "fed_policy"],
    "fx_commodity": ["usd", "risk_sentiment", "china_growth"],
    "energy": ["usd", "risk_sentiment", "industrial_demand", "supply_channel"],
    "natgas": ["weather_storage", "supply_channel", "industrial_demand"],
    "ag": ["usd", "weather_supply", "china_growth", "risk_sentiment"],
    "soft": ["usd", "weather_supply", "risk_sentiment"],
    "bond": ["nominal_yields", "fed_policy", "liquidity_curve", "inflation_channel"],
    "crypto": ["risk_sentiment", "usd", "fed_policy", "liquidity_curve"],
    "fx_cross": ["rate_differentials", "usd", "risk_sentiment", "fed_policy"],
}

# Drivers not backed by dedicated feeds (PMI, weather, inventories, etc.).
STUB_DRIVER_IDS: frozenset[str] = frozenset(
    {"china_growth", "weather_storage", "supply_channel", "rate_differentials"}
)

PROFILE_TO_BUCKET: dict[str, str] = {
    "gold": "gold",
    "silver": "silver",
    "copper": "copper",
    "equity": "equity",
    "fx": "fx_major",
    "oil": "energy",
    "natgas": "natgas",
    "ag": "ag",
    "soft": "soft",
    "bond": "bond",
    "crypto": "crypto",
    "generic": "equity",
}


def _bucket(market: str) -> str:
    try:
        from hptl.markets.instrument_registry import get_instrument

        spec = get_instrument(market)
        if spec:
            prof = spec.macro_driver_profile
            if prof == "fx":
                if spec.safe_haven_score >= 0.65:
                    return "fx_safe_haven"
                if spec.commodity_linkage >= 0.35:
                    return "fx_commodity"
                if spec.subgroup in {"fx_cross", "fx_em"}:
                    return "fx_cross"
                return "fx_major"
            return PROFILE_TO_BUCKET.get(prof, "equity")
    except Exception:
        pass
    return ASSET_BUCKET.get(market, "equity")


def _pp(val: Any) -> float | None:
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _dir_label(d: str) -> str:
    if d == "rising":
        return "Rising"
    if d == "falling":
        return "Falling"
    return "Flat"


def _momentum_label(d1w: str, d4w: str) -> str:
    if d1w == d4w and d1w in {"rising", "falling"}:
        return "Persistent" if d1w == "rising" else "Persistent easing" if d1w == "falling" else "Steady"
    if d1w == "rising":
        return "Accelerating higher"
    if d1w == "falling":
        return "Compressing"
    return "Mixed"


def build_global_macro_regime(row: pd.Series, scored: dict[str, Any], breakdown: dict[str, Any]) -> dict[str, Any]:
    """Multi-tag global regime (not a single risk_on/off label)."""
    tags: list[str] = []
    primary = str(scored.get("macro_signal") or "neutral")

    if primary == "risk_on":
        tags.append("risk_on")
    elif primary == "risk_off":
        tags.append("risk_off")

    pol = str(scored.get("policy_pressure") or "")
    if pol == "Restrictive":
        tags.append("restrictive")
    elif pol == "Easing":
        tags.append("easing")

    if breakdown.get("one_week_easing") or breakdown.get("four_week_easing"):
        tags.append("liquidity_expanding")
    if breakdown.get("one_week_restrictive"):
        tags.append("liquidity_contracting")

    curve = str(breakdown.get("curve_context") or "")
    if curve == "Steepening":
        tags.append("growth_accelerating")
    elif curve == "Flattening":
        tags.append("growth_slowing")

    if breakdown.get("one_week_restrictive") and primary == "risk_off":
        tags.append("disinflationary")

    # De-dupe preserve order
    seen: set[str] = set()
    ordered: list[str] = []
    for t in tags:
        if t not in seen:
            seen.add(t)
            ordered.append(t)

    headline = ", ".join(t.replace("_", " ") for t in ordered[:4]) if ordered else primary.replace("_", " ")

    return {
        "tags": ordered,
        "primary_signal": primary,
        "headline": headline.title() if headline else "Mixed macro",
        "policy_pressure": pol,
        "curve_context": curve,
    }


def _impact_usd(bucket: str, d10_1w: str, d2_1w: str) -> Impact:
    """USD proxy from yield dynamics (DXY series to be wired separately)."""
    if d10_1w == "rising":
        if bucket in {"gold", "silver", "copper", "fx_commodity", "ag", "energy"}:
            return "headwind"
        if bucket in {"fx_safe_haven"}:
            return "supportive"
        if bucket == "equity":
            return "mixed"
        return "headwind"
    if d10_1w == "falling":
        if bucket in {"gold", "silver", "copper", "fx_commodity", "ag", "energy"}:
            return "supportive"
        if bucket == "equity":
            return "supportive"
        return "mixed"
    return "neutral"


def _impact_real_yields(bucket: str, d10_1w: str, d10_4w: str) -> Impact:
    if bucket in {"gold", "silver"}:
        if d10_1w == "falling":
            return "supportive"
        if d10_1w == "rising":
            return "headwind"
        return "neutral"
    if bucket == "equity":
        if d10_1w == "rising":
            return "headwind"
        if d10_1w == "falling":
            return "supportive"
        return "mixed"
    if bucket == "copper":
        if d10_1w == "falling":
            return "supportive"
        if d10_1w == "rising":
            return "mixed"
        return "neutral"
    return "neutral"


def _impact_nominal_yields(bucket: str, d2_1w: str, d10_1w: str, risk_signal: str) -> Impact:
    if bucket == "equity":
        restrictive = d2_1w == "rising" and d10_1w == "rising"
        if restrictive:
            return "headwind"
        if d10_1w == "falling":
            return "supportive"
        return "mixed"
    if bucket == "fx_safe_haven":
        if risk_signal == "risk_off" and d10_1w == "falling":
            return "supportive"
        return "neutral"
    return "neutral"


def _impact_fed(bucket: str, fed_1w: str, fed_4w: str) -> Impact:
    easing = fed_1w == "falling" or fed_4w == "falling"
    restrictive = fed_1w == "rising" or fed_4w == "rising"
    if bucket in {"gold", "equity", "copper", "fx_commodity"}:
        if easing and not restrictive:
            return "supportive"
        if restrictive and not easing:
            return "headwind"
    if bucket == "fx_safe_haven" and restrictive:
        return "supportive"
    return "neutral"


def _impact_liquidity_curve(bucket: str, curve: str, b: dict[str, Any]) -> Impact:
    if bucket == "equity":
        if b.get("curve_risk_on"):
            return "supportive"
        if b.get("curve_risk_off"):
            return "headwind"
        if curve == "Steepening":
            return "supportive"
        if curve == "Flattening":
            return "headwind"
    return "neutral"


def _impact_risk_sentiment(bucket: str, risk_signal: str) -> Impact:
    if risk_signal == "risk_on":
        if bucket in {"equity", "copper", "fx_commodity", "silver"}:
            return "supportive"
        if bucket == "gold":
            return "mixed"
        if bucket == "fx_safe_haven":
            return "headwind"
    if risk_signal == "risk_off":
        if bucket in {"gold", "fx_safe_haven"}:
            return "supportive"
        if bucket in {"equity", "copper"}:
            return "headwind"
    return "neutral"


def _impact_china_growth(bucket: str, risk_signal: str, d10_1w: str) -> Impact:
    if bucket not in {"copper", "fx_commodity", "ag"}:
        return "unknown"
    if risk_signal == "risk_on" and d10_1w != "rising":
        return "supportive"
    if risk_signal == "risk_off":
        return "headwind"
    return "mixed"


def _impact_industrial(bucket: str, risk_signal: str) -> Impact:
    if bucket in {"copper", "energy", "silver"}:
        return _impact_risk_sentiment(bucket, risk_signal)
    return "unknown"


def _driver_block(
    driver_id: str,
    title: str,
    *,
    direction: str,
    momentum: str,
    market: str,
    impact: Impact,
    detail: str,
    confidence: str = "Medium",
    wired: bool = True,
    extra_assets: dict[str, Impact] | None = None,
) -> dict[str, Any]:
    bucket = _bucket(market)
    impacts: dict[str, str] = {market: impact}
    if extra_assets:
        impacts.update(extra_assets)
    return {
        "driver_id": driver_id,
        "title": title,
        "direction": direction,
        "momentum": momentum,
        "asset_impact": impact,
        "asset_impacts": impacts,
        "detail": detail,
        "confidence": confidence,
        "wired": wired,
    }


def _assess_transmission_mode(drivers: list[dict[str, Any]]) -> tuple[str, bool, str]:
    """Return (mode, generic_only, prefix_for_headline)."""
    wired_count = sum(
        1
        for b in drivers
        if b.get("wired", True) and b.get("driver_id") not in STUB_DRIVER_IDS and b.get("confidence") != "Low"
    )
    if wired_count >= 2:
        return "asset_specific", False, ""
    return (
        "generic_rates_only",
        True,
        "Macro transmission incomplete — generic rates backdrop only. ",
    )


def _build_driver_blocks(
    market: str,
    row: pd.Series,
    breakdown: dict[str, Any],
    scored: dict[str, Any],
    global_regime: dict[str, Any],
) -> list[dict[str, Any]]:
    bucket = _bucket(market)
    risk = str(scored.get("macro_signal") or "neutral")
    d2_1w = breakdown["dirs_1w"]["dgs2"]
    d10_1w = breakdown["dirs_1w"]["dgs10"]
    d30_1w = breakdown["dirs_1w"]["dgs30"]
    d10_4w = breakdown["dirs_4w"]["dgs10"]
    fed_1w = breakdown["fed_1w_dir"]
    curve = str(breakdown.get("curve_context") or "Neutral")

    blocks: list[dict[str, Any]] = []

    usd_imp = _impact_usd(bucket, d10_1w, d2_1w)
    blocks.append(
        _driver_block(
            "usd",
            "Dollar (USD proxy via yields)",
            direction=_dir_label(d10_1w),
            momentum=_momentum_label(d10_1w, breakdown["dirs_4w"]["dgs10"]),
            market=market,
            impact=usd_imp,
            detail=(
                f"10Y yield Δ {_pp_label(row.get('dgs10_1w_change'))} — used as USD pressure proxy until DXY is wired. "
                f"For {market}: {usd_imp}."
            ),
            confidence="Medium",
        )
    )

    if bucket in {"gold", "silver", "equity"}:
        ry_imp = _impact_real_yields(bucket, d10_1w, d10_4w)
        blocks.append(
            _driver_block(
                "real_yields",
                "Nominal yields (real-yield proxy)",
                direction=_dir_label(d10_1w),
                momentum=_momentum_label(d10_1w, d10_4w),
                market=market,
                impact=ry_imp,
                detail=(
                    f"DGS10 {_dir_label(d10_1w).lower()} ({_pp_label(row.get('dgs10_1w_change'))} 1w). "
                    f"Opportunity-cost channel for precious metals; discount-rate channel for equities."
                ),
                confidence="Medium",
            )
        )

    fed_imp = _impact_fed(bucket, fed_1w, breakdown["fed_4w_dir"])
    blocks.append(
        _driver_block(
            "fed_policy",
            "Fed / policy rate (DFF)",
            direction=_dir_label(fed_1w),
            momentum=_momentum_label(fed_1w, breakdown["fed_4w_dir"]),
            market=market,
            impact=fed_imp,
            detail=(
                f"Effective funds rate Δ {_pp_label(row.get('fed_funds_1w_change'))} (1w). "
                f"Policy pressure: {scored.get('policy_pressure', 'Neutral')}."
            ),
            confidence="Medium" if not breakdown.get("policy_used_dgs2_proxy") else "Low",
        )
    )

    if bucket == "equity":
        liq_imp = _impact_liquidity_curve(bucket, curve, breakdown)
        blocks.append(
            _driver_block(
                "liquidity_curve",
                "Curve / liquidity",
                direction=curve,
                momentum="Risk-on steepening" if breakdown.get("curve_risk_on") else "Flattening pressure"
                if breakdown.get("curve_risk_off")
                else "Neutral",
                market=market,
                impact=liq_imp,
                detail=(
                    f"10Y2Y 1w Δ {_pp_label(row.get('yield_curve_10y2y_1w_change'))}. "
                    "Steepening with easing front-end tends to support risk; flattening under restrictive 1w yields weighs on beta."
                ),
                confidence="Medium",
            )
        )

    if bucket in {"copper", "ag", "fx_commodity"}:
        cn_imp = _impact_china_growth(bucket, risk, d10_1w)
        blocks.append(
            _driver_block(
                "china_growth",
                "China / global growth (not wired)",
                direction="Improving" if risk == "risk_on" else "Soft" if risk == "risk_off" else "Mixed",
                momentum="PMI feed not connected",
                market=market,
                impact=cn_imp,
                detail=(
                    "China PMI / stimulus data not in feed — do not treat as live industrial signal. "
                    "Rates/risk channel only until PMI is wired."
                ),
                confidence="Low",
                wired=False,
            )
        )

    risk_imp = _impact_risk_sentiment(bucket, risk)
    blocks.append(
        _driver_block(
            "risk_sentiment",
            "Risk appetite (rates composite)",
            direction=global_regime.get("headline", risk),
            momentum=f"{scored.get('macro_strength', '')} conviction".strip(),
            market=market,
            impact=risk_imp,
            detail=scored.get("macro_summary", "")[:280],
            confidence="Medium",
        )
    )

    if bucket == "natgas":
        blocks.append(
            _driver_block(
                "weather_storage",
                "Weather / storage (not wired)",
                direction="Not in macro feed",
                momentum="—",
                market=market,
                impact="unknown",
                detail="Weather/storage/LNG feeds not connected — NG macro is rates context only.",
                confidence="Low",
                wired=False,
            )
        )

    # Order by bucket driver priority
    order = {d: i for i, d in enumerate(BUCKET_DRIVERS.get(bucket, []))}
    blocks.sort(key=lambda b: order.get(b["driver_id"], 99))
    return blocks[:6]


def _pp_label(val: Any) -> str:
    v = _pp(val)
    if v is None:
        return "N/A"
    return f"{v:+.2f} pp"


def _detect_macro_vs_price(
    market: str,
    *,
    structural_regime: str,
    flow_momentum: str,
    macro_alignment: str,
    macro_signal: str,
) -> dict[str, Any]:
    """Positioning/structure vs macro permission."""
    struct_bull = structural_regime in {"structural_bullish", "accumulation"}
    struct_bear = structural_regime in {"structural_bearish", "distribution"}
    macro_bullish = macro_alignment in {"supportive", "strong_tailwind", "liquidity_supportive"}
    macro_bearish = macro_alignment in {"headwind", "strong_contradiction", "risk_off_pressure"}
    macro_risk_on = macro_signal == "risk_on"
    macro_risk_off = macro_signal == "risk_off"

    state = "aligned"
    interpretation = "Macro transmission broadly aligns with positioning structure."

    if struct_bull and macro_bearish:
        state = "ignoring_bearish_macro"
        interpretation = (
            "Positioning remains structurally bullish while macro backdrop is restrictive — "
            "specs may be overlooking yield pressure; treat strength as fragile until macro eases."
        )
    elif struct_bull and macro_risk_off and not macro_bullish:
        state = "ignoring_risk_off"
        interpretation = (
            "Risk assets / long-biased positioning holding despite risk-off rates — "
            "possible liquidity squeeze or short-covering overpowering macro headwind."
        )
    elif struct_bear and macro_bullish:
        state = "ignoring_bearish_structure"
        interpretation = (
            "Macro improving but structure still bearish — market may be front-running policy relief "
            "before COT confirms a regime upgrade."
        )
    elif struct_bear and flow_momentum in {"improving", "short_covering"} and macro_risk_off:
        state = "covering_against_macro"
        interpretation = (
            "Short-covering rally underway against a still-restrictive macro tape — "
            "fade rallies carefully; macro has not endorsed a trend reversal."
        )
    elif struct_bull and flow_momentum in {"profit_taking", "weakening"} and macro_bullish:
        state = "aligned_pullback"
        interpretation = (
            "Macro supportive and flow shows profit-taking within bull structure — "
            "healthy pullback rather than macro-driven reversal."
        )
    elif struct_bull and macro_bullish:
        state = "aligned"
        interpretation = "Macro alignment supportive for long-biased structure."
    elif struct_bear and macro_bearish:
        state = "aligned"
        interpretation = "Macro headwinds align with bearish positioning — downside macro transmission intact."

    return {
        "state": state,
        "interpretation": interpretation,
        "label": "RESPECTING" if state == "aligned" or state == "aligned_pullback" else "IGNORING / DIVERGING",
    }


def _asset_headline(
    market: str,
    blocks: list[dict[str, Any]],
    macro_vs_price: dict[str, Any],
    global_regime: dict[str, Any],
) -> str:
    """One-line asset-specific macro read."""
    bucket = _bucket(market)
    short_name = market.split("/")[0].strip()
    dominant = [b for b in blocks if b.get("asset_impact") in {"headwind", "supportive"}][:2]
    drivers_txt = ""
    if dominant:
        parts = [f"{b['title'].split('(')[0].strip()}: {b['asset_impact']}" for b in dominant]
        drivers_txt = " · ".join(parts)

    mvp = macro_vs_price.get("interpretation", "")
    diverging = macro_vs_price.get("state", "").startswith("ignoring")

    base = ""
    if bucket == "gold":
        ry = next((b for b in blocks if b.get("driver_id") == "real_yields"), None)
        if ry and ry.get("asset_impact") == "headwind":
            base = "Real-yield and USD pressure weigh on gold — defensive bid not enough to offset opportunity cost."
        elif ry and ry.get("asset_impact") == "supportive":
            base = "Lower nominal yields supportive for gold — real-yield channel easing."
        else:
            base = "Gold macro mix neutral — watch real yields and USD proxy first."
    elif bucket == "copper":
        cn = next((b for b in blocks if b.get("driver_id") == "china_growth"), None)
        if cn and cn.get("asset_impact") == "supportive":
            base = "Industrial/growth channel supportive — China/stimulus expectations improving via risk channel."
        elif cn and cn.get("asset_impact") == "headwind":
            base = "China/growth macro headwind for copper — industrial demand narrative soft."
        else:
            base = "Copper macro read mixed — China growth proxy and USD dominate."
    elif bucket == "equity":
        if global_regime.get("primary_signal") == "risk_off":
            base = "Restrictive rates backdrop — ES/NQ need easing yields or stronger risk appetite to sustain beta."
        else:
            base = "Risk-on macro supportive for equities — watch whether yields cap upside."
    elif bucket == "fx_safe_haven":
        base = "Safe-haven macro channel active — risk sentiment and rate differentials drive CHF/JPY."
    elif bucket == "natgas":
        base = "NG macro layer is rates-agnostic — weather/storage dominate; rates block is contextual only."
    else:
        impacts = [b["asset_impact"] for b in blocks if b.get("asset_impact") not in {"unknown", "neutral"}]
        if impacts.count("supportive") > impacts.count("headwind"):
            base = f"Macro transmission net supportive for {short_name}."
        elif impacts.count("headwind") > impacts.count("supportive"):
            base = f"Macro transmission net opposing for {short_name}."
        else:
            base = f"Mixed macro transmission for {short_name}."

    if drivers_txt:
        base = f"{base} ({drivers_txt})".strip()

    if diverging:
        if bucket == "gold":
            div = "Price holding despite restrictive macro — real yields may still be capping upside."
        elif bucket == "copper":
            div = "Copper may be front-running stimulus/recovery while PMIs still soft."
        elif bucket == "equity":
            div = "Beta ignoring restrictive macro — possible trapped shorts / liquidity squeeze."
        else:
            div = mvp.split("—")[0].strip() if "—" in mvp else mvp[:120]
        return f"{base} {div}".strip()

    if base:
        return base
    return mvp or f"Mixed macro transmission — no dominant channel for {market}."


def build_macro_transmission(
    *,
    market: str,
    rates_row: pd.Series | None,
    macro_audit: dict[str, Any] | None,
    institutional_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Full transmission panel for one market-week."""
    profile = get_profile(market)
    inst = institutional_context or {}

    if rates_row is None or not _row_has_required_scoring_inputs(rates_row):
        return {
            "available": False,
            "market": market,
            "headline": "Macro transmission unavailable — incomplete FRED rates row.",
            "primary_sensitivities": list(profile.sensitivities[:4]) if profile else [],
            "global_regime": {"tags": [], "headline": "Data unavailable"},
            "drivers": [],
            "macro_vs_price": {"state": "unknown", "interpretation": "Cannot assess macro vs positioning without rates data.", "label": "N/A"},
            "asset_alignment": "unknown",
            "asset_alignment_label": "Macro data unavailable",
        }

    breakdown = _rates_alignment_breakdown(rates_row)
    scored = _score_complete_row(rates_row)
    global_regime = build_global_macro_regime(rates_row, scored, breakdown)
    drivers = _build_driver_blocks(market, rates_row, breakdown, scored, global_regime)

    macro_alignment = str(inst.get("macro_alignment") or "")
    macro_vs_price = _detect_macro_vs_price(
        market,
        structural_regime=str(inst.get("structural_regime") or ""),
        flow_momentum=str(inst.get("flow_momentum") or ""),
        macro_alignment=macro_alignment,
        macro_signal=str(scored.get("macro_signal") or ""),
    )

    supportive = sum(1 for b in drivers if b.get("asset_impact") == "supportive")
    headwind = sum(1 for b in drivers if b.get("asset_impact") == "headwind")
    if supportive > headwind + 1:
        asset_alignment = "supportive"
        asset_alignment_label = "Macro supportive for this asset"
    elif headwind > supportive + 1:
        asset_alignment = "headwind"
        asset_alignment_label = "Macro headwind for this asset"
    elif macro_vs_price.get("state", "").startswith("ignoring"):
        asset_alignment = "conflicting"
        asset_alignment_label = "Macro vs positioning diverging"
    else:
        asset_alignment = "mixed"
        asset_alignment_label = "Macro mixed / low conviction"

    mode, generic_only, generic_prefix = _assess_transmission_mode(drivers)
    headline = _asset_headline(market, drivers, macro_vs_price, global_regime)
    if generic_only:
        headline = generic_prefix + headline

    return {
        "available": True,
        "market": market,
        "transmission_mode": mode,
        "generic_rates_only": generic_only,
        "headline": headline,
        "asset_alignment": asset_alignment,
        "asset_alignment_label": asset_alignment_label,
        "primary_sensitivities": list(profile.sensitivities[:5]) if profile else [],
        "stress_note": profile.stress_note if profile else "",
        "global_regime": global_regime,
        "drivers": drivers,
        "macro_vs_price": macro_vs_price,
        "rates_snapshot": {
            "dgs10": _pp(rates_row.get("dgs10")),
            "dgs10_1w_change": _pp(rates_row.get("dgs10_1w_change")),
            "fed_funds": _pp(rates_row.get("fed_funds")),
            "macro_signal": scored.get("macro_signal"),
            "macro_score": scored.get("macro_score"),
        },
    }
