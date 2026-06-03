"""Compact UI payloads for dashboard hierarchy (no scoring changes).

Level 1: executive strip labels + subtle tone hints (support/caution/warn/neutral).
Level 2: short bullet lists (max 4 each) — not long prose.
"""
from __future__ import annotations

import re
from typing import Any, Literal

Tone = Literal["support", "caution", "warn", "neutral"]


def _clip(s: str, n: int = 120) -> str:
    t = (s or "").strip()
    if len(t) <= n:
        return t
    return t[: n - 1].rstrip() + "…"


def _first_sentence(s: str) -> str:
    if not s:
        return ""
    m = re.split(r"(?<=[.!?])\s+", s.strip())
    return (m[0] if m else "").strip()


def _sentiment_level_from_text(text: str) -> str:
    u = (text or "").upper()
    if "EXTREME" in u:
        return "Extreme distortion"
    if "HIGH" in u and ("DISTORTION" in u or "INTERFERENCE" in u):
        return "High distortion"
    if "MODERATE" in u:
        return "Moderate distortion"
    if "LOW" in u and ("DISTORTION" in u or "INTERFERENCE" in u or "LOW." in u):
        return "Low distortion"
    return "Distortion unclear"


def _macro_support_label(macro_regime: str, macro_score: Any) -> str:
    mr = (macro_regime or "").strip()
    if not mr or mr.upper() == "N/A":
        return "Macro unavailable"
    if "risk_on" in mr.lower():
        return "Supportive"
    if "risk_off" in mr.lower():
        return "Restrictive"
    return "Mixed / neutral"


def _cot_executive_label(positioning_state: str, cot_bias: str) -> str:
    ps = (positioning_state or "").strip()
    if ps and ps.upper() != "N/A":
        return ps
    return (cot_bias or "N/A").strip() or "N/A"


def _impulse_display(inter: dict[str, Any] | None) -> str:
    if not inter:
        return "N/A"
    return str(inter.get("intermarket_confirmation") or "N/A")


def _regime_short(global_regime: dict[str, Any] | None, macro_regime: str) -> str:
    if global_regime and isinstance(global_regime, dict):
        rs = global_regime.get("resolved_macro_signal")
        if rs:
            s = str(rs).replace("_", " ").title()
            if "Risk" in s:
                return s
            return s
    mr = (macro_regime or "").strip()
    if mr and mr.upper() != "N/A":
        return mr.replace("_", " ").title()
    return "Unknown"


def _tones(
    positioning_state: str,
    macro_regime: str,
    sentiment_text: str,
    inter: dict[str, Any] | None,
    env: str,
) -> dict[str, Tone]:
    ps = (positioning_state or "").lower()
    mr = (macro_regime or "").lower()
    st = (sentiment_text or "").upper()
    conf = str((inter or {}).get("intermarket_confirmation") or "").upper()

    def cot_tone() -> Tone:
        if "strengthening" in ps and "bear" in ps:
            return "warn"
        if "strengthening" in ps and "bull" in ps:
            return "support"
        if "weakening" in ps or "distribution" in ps or "transition" in ps:
            return "caution"
        return "neutral"

    def macro_tone() -> Tone:
        if "risk_on" in mr:
            return "support"
        if "risk_off" in mr:
            return "caution"
        return "neutral"

    def sent_tone() -> Tone:
        if "EXTREME" in st or ("HIGH" in st and "DISTORTION" in st.replace(" ", "")):
            return "warn"
        if "MODERATE" in st:
            return "caution"
        if "LOW" in st:
            return "support"
        return "neutral"

    def imp_tone() -> Tone:
        if conf == "CONFIRMING":
            return "support"
        if conf == "MIXED":
            return "caution"
        if conf in ("DIVERGING", "WARNING"):
            return "warn"
        return "neutral"

    def regime_tone() -> Tone:
        if "risk_off" in mr:
            return "caution"
        if "risk_on" in mr:
            return "support"
        return "neutral"

    def env_tone() -> Tone:
        el = env.lower()
        if "conflicted" in el or "caution" in el:
            return "caution"
        if "hostile" in el or "diverg" in el:
            return "warn"
        if "constructive" in el or "clean" in el:
            return "support"
        return "neutral"

    return {
        "cot": cot_tone(),
        "macro": macro_tone(),
        "sentiment": sent_tone(),
        "impulse": imp_tone(),
        "regime": regime_tone(),
        "environment": env_tone(),
    }


def _environment_label(inter: dict[str, Any] | None, sentiment_text: str, macro_label: str) -> str:
    conf = str((inter or {}).get("intermarket_confirmation") or "").upper()
    st = _sentiment_level_from_text(sentiment_text)
    if conf == "CONFIRMING" and "Low" in st:
        return "Constructive"
    if conf == "MIXED" or "Moderate" in st:
        return "Mixed — monitor alignment"
    if conf in ("DIVERGING", "WARNING") or "High" in st or "Extreme" in st:
        return "Conflicted / elevated noise"
    if "Restrictive" in macro_label and conf != "CONFIRMING":
        return "Cautious backdrop"
    return "Balanced"


def build_macro_impact_bullets(
    global_regime: dict[str, Any] | None,
    macro_audit: dict[str, Any] | None,
    macro_regime: str,
) -> list[str]:
    out: list[str] = []
    if global_regime and isinstance(global_regime, dict):
        rp = global_regime.get("rates_pressure")
        if rp:
            out.append(_clip(str(rp), 72))
        liq = global_regime.get("liquidity_regime")
        if liq:
            out.append(_clip(str(liq), 72))
        usd = global_regime.get("usd_impulse")
        if usd and "not modeled" not in str(usd).lower():
            out.append(_clip(str(usd), 72))
        inf = global_regime.get("inflation_regime")
        if inf and len(out) < 4:
            out.append(_clip(str(inf), 72))
    if macro_audit and macro_audit.get("available") and len(out) < 4:
        rr = macro_audit.get("resolved_regime") or {}
        curve = rr.get("curve_context")
        if curve:
            out.append(f"Curve: {curve}")
    if len(out) < 2 and macro_regime and macro_regime.upper() != "N/A":
        out.insert(0, f"Macro label: {macro_regime}")
    return out[:4]


def build_sentiment_bullets(sentiment_paragraph: str, full_loaded_net_pct: Any) -> list[str]:
    bullets: list[str] = []
    lvl = _sentiment_level_from_text(sentiment_paragraph)
    bullets.append(f"Interference: {lvl}")
    try:
        p = float(full_loaded_net_pct)
    except (TypeError, ValueError):
        p = float("nan")
    if p == p and 0 <= p <= 100:
        if p >= 90 or p <= 10:
            bullets.append("Positioning vs full history: stretched band")
        elif p >= 75 or p <= 25:
            bullets.append("Positioning vs full history: notable tilt")
        else:
            bullets.append("Positioning vs full history: mid-range")
    if "crowding" in (sentiment_paragraph or "").lower():
        bullets.append("Crowding narrative flagged in text layer")
    if len(bullets) < 3:
        bullets.append("Use deep audit for full distortion notes")
    return bullets[:4]


def build_intermarket_bullets(inter: dict[str, Any] | None) -> tuple[list[str], list[str]]:
    if not inter:
        return ([], [])
    sup = list(inter.get("supporting_drivers") or [])[:4]
    con = list(inter.get("conflicting_drivers") or [])[:4]
    return (sup, con)


def build_final_one_liner(
    inter: dict[str, Any] | None,
    macro_label: str,
    env: str,
) -> str:
    conf = _impulse_display(inter)
    base = f"{env}. Macro: {macro_label}. Impulse: {conf}."
    return _clip(base, 160)


def build_record_ui_pack(
    *,
    positioning_state: str,
    cot_bias: str,
    macro_regime: str,
    macro_score: Any,
    global_market_regime: dict[str, Any] | None,
    macro_audit: dict[str, Any] | None,
    instrument_intel: dict[str, Any] | None,
    intermarket: dict[str, Any] | None,
    full_loaded_net_pct: Any,
) -> dict[str, Any]:
    intel = instrument_intel or {}
    sent_txt = str(intel.get("sentiment_interference") or "")
    macro_label = _macro_support_label(macro_regime, macro_score)
    env = _environment_label(intermarket, sent_txt, macro_label)
    tones = _tones(positioning_state, macro_regime, sent_txt, intermarket, env)

    executive = {
        "cot_flow": _cot_executive_label(positioning_state, cot_bias),
        "macro": macro_label,
        "sentiment": _sentiment_level_from_text(sent_txt),
        "impulse": _impulse_display(intermarket),
        "regime": _regime_short(global_market_regime, macro_regime),
        "environment": env,
        "tones": tones,
    }

    macro_bullets = build_macro_impact_bullets(global_market_regime, macro_audit, macro_regime)
    sentiment_bullets = build_sentiment_bullets(sent_txt, full_loaded_net_pct)
    sup, con = build_intermarket_bullets(intermarket)
    final_line = build_final_one_liner(intermarket, macro_label, env)

    news_bullets: list[str] = []
    nc = str(intel.get("news_catalysts") or "")
    if nc:
        raw = nc.replace("Recurring channels to scan (not exhaustive):", "").strip()
        parts = [p.strip() for p in raw.split(",") if len(p.strip()) > 3][:4]
        news_bullets = [_clip(p, 88) for p in parts]
    if not news_bullets:
        news_bullets = ["Scan calendar + GDELT when wired — no headline feed in this row"]

    return {
        "executive": executive,
        "macro_impact_bullets": macro_bullets,
        "news_catalyst_bullets": news_bullets[:4],
        "sentiment_bullets": sentiment_bullets,
        "intermarket_supporting": sup,
        "intermarket_conflicting": con,
        "intermarket_impulse_score": intermarket.get("impulse_score") if intermarket else None,
        "final_context_line": final_line,
    }
