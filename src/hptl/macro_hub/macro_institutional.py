"""Institutional reads and transmission maps for Macro Hub first-class assets."""

from __future__ import annotations

from typing import Any, Final

from hptl.markets.instrument_registry import MACRO_INSTITUTIONAL_MARKETS

# instrument_id -> (macro_hub section, value key, optional history key)
MACRO_RATE_SERIES: Final[dict[str, tuple[str, str, str | None]]] = {
    "US 2-Year Treasury Yield": ("treasuries", "us_2y_yield", "us_2y_yield"),
    "US 10-Year Treasury Yield": ("treasuries", "us_10y_yield", "us_10y_yield"),
    "US 30-Year Treasury Yield": ("treasuries", "us_30y_yield", "us_30y_yield"),
    "2s10s Yield Curve": ("treasuries", "curve_2s10s", "curve_2s10s"),
    "10-Year Real Yield": ("treasuries", "real_yield_10y", "real_yield_10y"),
}

# Who each macro driver typically pressures or supports (display / transmission).
MACRO_TRANSMISSION_TARGETS: Final[dict[str, list[str]]] = {
    "US Dollar Index / DX": [
        "Euro FX / 6E",
        "British Pound / 6B",
        "Australian Dollar / 6A",
        "NZ Dollar / 6N",
        "Gold",
        "Silver",
        "Bitcoin",
    ],
    "US 2-Year Treasury Yield": [
        "NASDAQ / NQ",
        "S&P 500 / ES",
        "Gold",
        "Bitcoin",
        "Euro FX / 6E",
    ],
    "US 10-Year Treasury Yield": [
        "NASDAQ / NQ",
        "S&P 500 / ES",
        "Gold",
        "Silver",
        "US T-Bond",
    ],
    "US 30-Year Treasury Yield": [
        "US T-Bond",
        "NASDAQ / NQ",
        "Gold",
    ],
    "2s10s Yield Curve": [
        "NASDAQ / NQ",
        "S&P 500 / ES",
        "Copper / HG",
        "Australian Dollar / 6A",
    ],
    "10-Year Real Yield": [
        "Gold",
        "Silver",
        "Bitcoin",
        "NASDAQ / NQ",
    ],
}

# Scanner macro-driver panel: which drivers matter per asset bucket.
SCANNER_DRIVER_KEYS: Final[dict[str, list[tuple[str, str]]]] = {
    "gold": [
        ("usd_index", "USD Index"),
        ("real_yield", "Real Yield"),
        ("inflation", "Inflation Expectations"),
    ],
    "silver": [
        ("usd_index", "USD Index"),
        ("real_yield", "Real Yield"),
        ("risk_sentiment", "Risk Sentiment"),
    ],
    "crypto": [
        ("usd_index", "USD Index"),
        ("real_yield", "Real Yield"),
        ("risk_sentiment", "Risk Sentiment"),
    ],
    "fx_major": [
        ("usd_index", "USD Index"),
        ("real_yield", "Real Yield"),
        ("policy_differential", "Policy Differential"),
    ],
    "fx_commodity": [
        ("usd_index", "USD Index"),
        ("real_yield", "Real Yield"),
        ("risk_sentiment", "Risk Sentiment"),
    ],
    "equity": [
        ("real_yield", "Real Yield"),
        ("curve_2s10s", "2s10s Curve"),
        ("usd_index", "USD Index"),
    ],
    "bond": [
        ("us_10y", "US 10Y Yield"),
        ("curve_2s10s", "2s10s Curve"),
        ("real_yield", "Real Yield"),
    ],
}


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _closes_from_history(history: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not history:
        return []
    daily = history.get("daily_all")
    if isinstance(daily, list) and daily:
        return daily
    windows = history.get("windows") or {}
    for key in ("1y", "3y", "5y"):
        w = windows.get(key) or {}
        closes = w.get("closes")
        if isinstance(closes, list) and closes:
            return closes
    return []


def _delta_bps(closes: list[dict[str, Any]], trading_days: int) -> float | None:
    if len(closes) <= trading_days:
        return None
    cur = _num(closes[-1].get("close"))
    prev = _num(closes[-1 - trading_days].get("close"))
    if cur is None or prev is None:
        return None
    return round((cur - prev) * 100, 2)


def _direction(delta_bps: float | None, *, threshold_bps: float = 1.0) -> str:
    if delta_bps is None:
        return "flat"
    if delta_bps > threshold_bps:
        return "rising"
    if delta_bps < -threshold_bps:
        return "falling"
    return "flat"


def _stance_label(direction: str, *, yield_series: bool = True) -> str:
    if direction == "rising":
        return "Rising" if yield_series else "Bullish"
    if direction == "falling":
        return "Falling" if yield_series else "Bearish"
    return "Flat" if yield_series else "Neutral"


def _weekly_change_text(direction: str, delta_bps: float | None, *, cot: bool = False) -> str:
    if cot:
        if delta_bps is None:
            return "Weekly positioning change unavailable"
        if delta_bps > 0:
            return "Net longs increased"
        if delta_bps < 0:
            return "Net longs decreased"
        return "Net positioning unchanged"
    if delta_bps is None:
        return "Weekly change unavailable"
    sign = "+" if delta_bps > 0 else ""
    verb = "rose" if delta_bps > 0 else "fell" if delta_bps < 0 else "held"
    return f"Level {verb} {sign}{delta_bps:.0f} bps over 1 week"


def _four_week_change_text(direction_4w: str, delta_4w_bps: float | None, *, cot: bool = False) -> str:
    if cot:
        if delta_4w_bps is None:
            return "4-week positioning trend unavailable"
        if delta_4w_bps > 0:
            return "Dollar accumulation" if delta_4w_bps else "Institutional build"
        if delta_4w_bps < 0:
            return "Dollar distribution"
        return "Stable positioning"
    if direction_4w == "rising":
        return "Persistent rise over 4 weeks"
    if direction_4w == "falling":
        return "Persistent decline over 4 weeks"
    return "Range-bound over 4 weeks"


def _action_bias(market: str, direction: str, direction_4w: str) -> str:
    targets = MACRO_TRANSMISSION_TARGETS.get(market, [])
    tail = ", ".join(targets[:4])
    if market == "US Dollar Index / DX":
        if direction in {"rising", "bullish"} or direction_4w == "rising":
            return f"Watch for pressure on {tail}."
        if direction in {"falling", "bearish"} or direction_4w == "falling":
            return f"Supportive backdrop for {tail}."
        return f"Monitor {tail} for USD confirmation."
    if market == "10-Year Real Yield":
        if direction == "rising":
            return "Headwind for Gold and long-duration risk assets."
        if direction == "falling":
            return "Supportive for Gold, Silver, and Bitcoin."
        return "Neutral liquidity backdrop — watch breakouts."
    if market == "2s10s Yield Curve":
        if direction == "rising":
            return "Steepening — improving growth expectations; watch equities and cyclicals."
        if direction == "falling":
            return "Flattening — growth caution; favour defensives and duration."
        return "Curve stable — regime unchanged."
    if "Treasury Yield" in market:
        if direction == "rising":
            return f"Restrictive rates backdrop — pressure on {tail}."
        if direction == "falling":
            return f"Easing rates backdrop — supportive for {tail}."
        return f"Rates range-bound — cross-asset confirmation via {tail}."
    return f"Transmission watch: {tail}." if tail else "Monitor cross-asset confirmation."


def _trader_read(market: str, direction: str, direction_4w: str, *, cot: bool = False) -> str:
    if market == "US Dollar Index / DX":
        if cot and direction_4w == "rising":
            return "Institutions adding USD exposure."
        if cot and direction_4w == "falling":
            return "Institutions reducing USD exposure."
        if direction == "rising":
            return "Dollar strengthening on broad index."
        if direction == "falling":
            return "Dollar softening on broad index."
        return "Dollar index consolidating."
    if market == "10-Year Real Yield":
        if direction == "rising":
            return "Restrictive liquidity backdrop."
        if direction == "falling":
            return "Real yields easing — liquidity supportive."
        return "Real yields stable."
    if market == "2s10s Yield Curve":
        if direction == "rising":
            return "Curve steepening — growth expectations improving."
        if direction == "falling":
            return "Curve flattening — growth expectations fading."
        return "Curve regime unchanged."
    if "Treasury Yield" in market:
        tenor = "front-end" if "2-Year" in market else "long-end" if "30-Year" in market else "benchmark"
        if direction == "rising":
            return f"{tenor.title()} yields rising — tighter financial conditions."
        if direction == "falling":
            return f"{tenor.title()} yields falling — easing financial conditions."
        return f"{tenor.title()} yields stable."
    return "Macro institutional read from pooled Macro Hub series."


def series_momentum_from_doc(doc: dict[str, Any], market_id: str) -> dict[str, Any]:
    """Level, 1w/4w deltas and direction from macro_hub_latest.json."""
    if market_id == "US Dollar Index / DX":
        usd = doc.get("usd") or {}
        level = _num(usd.get("dxy_price"))
        history = usd.get("dxy_history")
        closes = _closes_from_history(history)
        d1 = _delta_bps(closes, 5)
        d4 = _delta_bps(closes, 20)
        cot = usd.get("cot") or {}
        cot_w1 = _num(cot.get("weekly_net_change"))
        cot_4w = _num(cot.get("four_week_net_change"))
        dir_price = _direction(d1, threshold_bps=15.0)
        dir_4w = _direction(d4, threshold_bps=30.0)
        net = _num(cot.get("net"))
        cot_dir = "bullish" if net and net > 0 else "bearish" if net and net < 0 else "neutral"
        return {
            "level": level,
            "delta_1w_bps": d1,
            "delta_4w_bps": d4,
            "direction": cot_dir if cot.get("net") is not None else dir_price,
            "direction_4w": _direction(cot_4w, threshold_bps=500.0) if cot_4w is not None else dir_4w,
            "cot_weekly": cot_w1,
            "cot_4w": cot_4w,
            "uses_cot": True,
            "as_of": usd.get("dxy_price_date") or (cot.get("report_date") or "")[:10],
        }

    spec = MACRO_RATE_SERIES.get(market_id)
    if not spec:
        return {"level": None, "direction": "flat", "direction_4w": "flat", "uses_cot": False}
    section_key, value_key, hist_key = spec
    section = doc.get(section_key) or {}
    level = _num(section.get(value_key))
    if level is None:
        detail = (section.get("series_detail") or {}).get(hist_key or value_key) or {}
        level = _num(detail.get("latest_value"))
    history_block = (section.get("series_history") or {}).get(hist_key or value_key)
    closes = _closes_from_history(history_block)
    d1 = _delta_bps(closes, 5)
    d4 = _delta_bps(closes, 20)
    return {
        "level": level,
        "delta_1w_bps": d1,
        "delta_4w_bps": d4,
        "direction": _direction(d1),
        "direction_4w": _direction(d4),
        "uses_cot": False,
        "as_of": section.get("latest_date"),
    }


def build_macro_institutional_read(market_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Current stance, weekly/4w change, trader read, action bias."""
    mom = series_momentum_from_doc(doc, market_id)
    uses_cot = bool(mom.get("uses_cot"))
    direction = str(mom.get("direction") or "flat")
    direction_4w = str(mom.get("direction_4w") or "flat")
    yield_series = market_id != "US Dollar Index / DX"
    stance = _stance_label(direction, yield_series=not uses_cot or yield_series)
    if uses_cot and direction in {"bullish", "bearish", "neutral"}:
        stance = direction.title() if direction != "neutral" else "Neutral"

    w_delta = mom.get("cot_weekly") if uses_cot else mom.get("delta_1w_bps")
    f_delta = mom.get("cot_4w") if uses_cot else mom.get("delta_4w_bps")

    return {
        "market": market_id,
        "current_stance": stance,
        "weekly_change": _weekly_change_text(direction, w_delta, cot=uses_cot),
        "four_week_change": _four_week_change_text(direction_4w, f_delta, cot=uses_cot),
        "trader_read": _trader_read(market_id, direction, direction_4w, cot=uses_cot),
        "action_bias": _action_bias(market_id, direction, direction_4w),
        "macro_interpretation": _trader_read(market_id, direction, direction_4w, cot=uses_cot),
        "level": mom.get("level"),
        "delta_1w_bps": mom.get("delta_1w_bps"),
        "delta_4w_bps": mom.get("delta_4w_bps"),
        "direction": direction,
        "direction_4w": direction_4w,
        "as_of": mom.get("as_of"),
        "transmission_targets": list(MACRO_TRANSMISSION_TARGETS.get(market_id, [])),
    }


def build_macro_driver_transmission(market_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Self-referential macro transmission panel for a macro institutional asset."""
    read = build_macro_institutional_read(market_id, doc)
    targets = read.get("transmission_targets") or []
    direction = str(read.get("direction") or "flat")
    headline = f"{read['current_stance']} — {read['trader_read']}"
    impacts = []
    for t in targets:
        if direction in {"rising", "bullish"}:
            impact = "headwind" if market_id == "US Dollar Index / DX" and "FX" in t or t in {
                "Gold",
                "Silver",
                "Bitcoin",
                "Euro FX / 6E",
                "British Pound / 6B",
                "Australian Dollar / 6A",
                "NZ Dollar / 6N",
            } else "supportive"
        elif direction in {"falling", "bearish"}:
            impact = "supportive" if market_id == "US Dollar Index / DX" else "headwind"
        else:
            impact = "neutral"
        if market_id == "10-Year Real Yield":
            impact = "headwind" if direction == "rising" and t in {"Gold", "Silver", "Bitcoin"} else (
                "supportive" if direction == "falling" and t in {"Gold", "Silver", "Bitcoin"} else impact
            )
        impacts.append({"market": t, "impact": impact})

    return {
        "available": True,
        "market": market_id,
        "headline": headline,
        "asset_alignment": "supportive" if direction in {"falling", "bearish"} and market_id != "US Dollar Index / DX" else (
            "headwind" if direction in {"rising", "bullish"} and market_id != "US Dollar Index / DX" else "mixed"
        ),
        "asset_alignment_label": read["current_stance"],
        "drivers": [
            {
                "driver_id": "macro_hub_series",
                "title": market_id,
                "direction": direction,
                "momentum": read["four_week_change"],
                "asset_impact": "neutral",
                "detail": read["trader_read"],
                "wired": True,
            }
        ],
        "transmission_targets": impacts,
        "macro_institutional_read": read,
        "generic_rates_only": False,
    }


def _driver_stance_for_asset(driver_key: str, doc: dict[str, Any]) -> tuple[str, str]:
    """Return (stance label, direction) for a scanner driver key."""
    if driver_key == "usd_index":
        read = build_macro_institutional_read("US Dollar Index / DX", doc)
        return read["current_stance"], str(read.get("direction") or "flat")
    if driver_key == "real_yield":
        read = build_macro_institutional_read("10-Year Real Yield", doc)
        return read["current_stance"], str(read.get("direction") or "flat")
    if driver_key == "curve_2s10s":
        read = build_macro_institutional_read("2s10s Yield Curve", doc)
        return read["current_stance"], str(read.get("direction") or "flat")
    if driver_key == "us_10y":
        read = build_macro_institutional_read("US 10-Year Treasury Yield", doc)
        return read["current_stance"], str(read.get("direction") or "flat")
    if driver_key == "policy_differential":
        return "Mixed", "flat"
    if driver_key == "inflation":
        y10 = build_macro_institutional_read("US 10-Year Treasury Yield", doc)
        real = build_macro_institutional_read("10-Year Real Yield", doc)
        if y10.get("direction") == "rising" and real.get("direction") != "rising":
            return "Rising", "rising"
        if y10.get("direction") == "falling":
            return "Falling", "falling"
        return "Stable", "flat"
    if driver_key == "risk_sentiment":
        curve = build_macro_institutional_read("2s10s Yield Curve", doc)
        if curve.get("direction") == "rising":
            return "Risk-on", "rising"
        if curve.get("direction") == "falling":
            return "Risk-off", "falling"
        return "Neutral", "flat"
    return "—", "flat"


def _alignment_for_driver(driver_key: str, direction: str, bucket: str) -> bool:
    """Whether driver direction is supportive for the asset bucket."""
    supportive_rising = {
        "gold": {"real_yield": False, "usd_index": False, "inflation": True},
        "silver": {"real_yield": False, "usd_index": False},
        "crypto": {"real_yield": False, "usd_index": False, "risk_sentiment": True},
        "fx_major": {"usd_index": False, "policy_differential": True},
        "fx_commodity": {"usd_index": False, "risk_sentiment": True},
        "equity": {"real_yield": False, "curve_2s10s": True, "usd_index": False},
    }
    rules = supportive_rising.get(bucket, {})
    rule = rules.get(driver_key)
    if rule is None:
        return direction == "flat"
    if direction == "flat":
        return True
    if direction in {"rising", "bullish", "risk-on"}:
        return bool(rule)
    if direction in {"falling", "bearish", "risk-off"}:
        return not rule
    return False


def build_scanner_macro_drivers(market: str, doc: dict[str, Any], *, bucket: str | None = None) -> dict[str, Any]:
    """Macro driver alignment panel for scanner rows (Phase 5)."""
    from hptl.macro.macro_transmission import _bucket

    b = bucket or _bucket(market)
    driver_defs = SCANNER_DRIVER_KEYS.get(b, SCANNER_DRIVER_KEYS.get("fx_major", []))
    drivers: list[dict[str, Any]] = []
    aligned = 0
    for key, label in driver_defs:
        stance, direction = _driver_stance_for_asset(key, doc)
        is_aligned = _alignment_for_driver(key, direction, b)
        if is_aligned:
            aligned += 1
        drivers.append(
            {
                "id": key,
                "label": label,
                "stance": stance,
                "direction": direction,
                "aligned": is_aligned,
            }
        )
    total = len(drivers)
    return {
        "drivers": drivers,
        "aligned_count": aligned,
        "total_count": total,
        "alignment_label": f"{aligned}/{total}" if total else "—",
    }


def build_macro_institutional_attention(market_id: str, doc: dict[str, Any]) -> dict[str, Any]:
    """Attention candidate scoring for macro institutional assets."""
    read = build_macro_institutional_read(market_id, doc)
    score = 0.0
    alerts: list[dict[str, str]] = []
    d4 = read.get("delta_4w_bps")
    if market_id == "US Dollar Index / DX":
        cot_4w = series_momentum_from_doc(doc, market_id).get("cot_4w")
        if cot_4w is not None and abs(cot_4w) >= 2000:
            score += 35
            alerts.append(
                {
                    "icon": "💵",
                    "text": f"Largest institutional USD build in recent weeks (4w Δ {cot_4w:,.0f})",
                    "kind": "positioning",
                }
            )
    if d4 is not None and abs(d4) >= 15:
        score += 28
        alerts.append(
            {
                "icon": "📈",
                "text": f"Structural move — 4w change {d4:+.0f} bps",
                "kind": "macro",
            }
        )
    if read.get("direction") in {"rising", "falling", "bullish", "bearish"}:
        score += 12

    tier = "watchlist"
    if score >= 40:
        tier = "high_attention"
    elif score >= 25:
        tier = "developing"

    reason = alerts[0]["text"] if alerts else read.get("trader_read") or "Macro institutional monitoring"
    return {
        "priority_tier": tier,
        "priority_score": round(score, 1),
        "dominant_narrative": read.get("trader_read"),
        "priority_headline": reason[:72],
        "alerts": alerts[:5],
        "attention_reason": reason,
    }
