"""Attention & priority layer — change-focused triage over institutional context."""

from __future__ import annotations

from typing import Any

import pandas as pd

from hptl.context.dominant_narrative import build_dominant_narrative
from hptl.validation import safe_float as _finite

PRIORITY_HIGH = "high_attention"
PRIORITY_DEVELOPING = "developing"
PRIORITY_WATCHLIST = "watchlist"
PRIORITY_LOW = "low_priority"

PRIORITY_LABELS = {
    PRIORITY_HIGH: "HIGH ATTENTION",
    PRIORITY_DEVELOPING: "DEVELOPING",
    PRIORITY_WATCHLIST: "WATCHLIST",
    PRIORITY_LOW: "LOW PRIORITY",
}

LOOKBACK_WEEKS = 12


def _tactical_readable(posture: str, posture_label: str, extreme: str) -> str:
    mapping = {
        "stalk_long_pullback": "Buy pullbacks only",
        "stalk_short_rally": "Fade rallies carefully",
        "avoid_chase": "Avoid chasing",
        "wait_confirmation": "Wait for confirmation",
        "stand_aside": "Sit in cash",
        "stalk_long_continuation": "Watch for continuation",
        "stalk_short_continuation": "Stalk tactical shorts",
        "watch": "Watch only",
    }
    base = mapping.get(posture, posture_label or "Watch")
    if posture == "avoid_chase" and extreme in {"euphoric_longs", "crowded_longs"}:
        return "Avoid breakout longs"
    if posture == "avoid_chase" and extreme in {"crowded_shorts", "capitulation_shorts"}:
        return "Avoid chasing downside"
    return base


def _hist_series(hist: pd.DataFrame, col: str) -> list[float]:
    if hist.empty or col not in hist.columns:
        return []
    vals = pd.to_numeric(hist[col], errors="coerce").dropna().tolist()
    return [float(v) for v in vals if _finite(v) is not None]


def _rank_extreme(current: float | None, series: list[float], *, largest: bool) -> bool:
    if current is None or len(series) < 4:
        return False
    cur = abs(current)
    pool = [abs(x) for x in series]
    if not pool:
        return False
    if largest:
        return cur >= max(pool) * 0.98 and cur > sorted(pool)[-2] if len(pool) > 1 else cur > 0
    return cur <= min(pool) * 1.02


def _detect_alerts(
    *,
    market: str,
    ctx: dict[str, Any],
    hist: pd.DataFrame,
    net: float | None,
    w1: float | None,
    long_w1: float | None,
    short_w1: float | None,
) -> list[dict[str, str]]:
    alerts: list[dict[str, str]] = []
    w1_hist = _hist_series(hist, "weekly_change")
    long_d_hist = _hist_series(hist, "long_weekly_change")
    short_d_hist = _hist_series(hist, "short_weekly_change")

    regime = str(ctx.get("structural_regime") or "")
    flow = str(ctx.get("flow_momentum") or "")
    extreme = str(ctx.get("positioning_extreme") or "none")
    weeks = int(ctx.get("weeks_in_regime") or 0)
    pending = ctx.get("pending_flip_target")

    if pending:
        alerts.append(
            {
                "icon": "⚡",
                "text": f"Regime shift brewing — watching for {str(pending).replace('_', ' ')}",
                "kind": "transition",
            }
        )
    elif weeks == 1 and regime not in {"neutral_rotation", "transitional"}:
        alerts.append(
            {
                "icon": "⚡",
                "text": f"Fresh {regime.replace('_', ' ')} — first week in new structure",
                "kind": "transition",
            }
        )

    if w1 is not None and _rank_extreme(w1, w1_hist, largest=True):
        direction = "build" if w1 > 0 else "liquidation"
        alerts.append(
            {
                "icon": "🔥",
                "text": f"Largest net {direction} in {min(len(w1_hist) + 1, LOOKBACK_WEEKS)} weeks",
                "kind": "flow_extreme",
            }
        )

    if long_w1 is not None and long_w1 < 0 and _rank_extreme(long_w1, long_d_hist, largest=True):
        alerts.append(
            {
                "icon": "🔥",
                "text": f"Largest long liquidation in {min(len(long_d_hist) + 1, LOOKBACK_WEEKS)} weeks",
                "kind": "flow_extreme",
            }
        )

    if flow == "short_covering" and w1 is not None and w1 > 0:
        alerts.append({"icon": "📈", "text": "Aggressive short covering this week", "kind": "flow"})

    if flow in {"profit_taking", "long_liquidation"} and regime in {"structural_bullish", "distribution"}:
        alerts.append(
            {"icon": "⚠️", "text": "Weak sponsorship — profit-taking into strength", "kind": "deterioration"}
        )

    if regime == "accumulation":
        alerts.append({"icon": "👀", "text": "Early accumulation behaviour emerging", "kind": "opportunity"})

    if extreme == "euphoric_longs":
        alerts.append({"icon": "💀", "text": "Euphoric long crowding — poor chase RR", "kind": "exhaustion"})
    elif extreme == "crowded_longs":
        alerts.append({"icon": "🚨", "text": "Fresh positioning extreme — crowded longs", "kind": "exhaustion"})
    elif extreme == "crowded_shorts":
        alerts.append({"icon": "🚨", "text": "Crowded shorts — squeeze risk elevated", "kind": "exhaustion"})
    elif extreme == "capitulation_shorts":
        alerts.append({"icon": "📉", "text": "Capitulation-level short positioning", "kind": "exhaustion"})

    if long_w1 is not None and short_w1 is not None and long_w1 < -500 and short_w1 < -500:
        alerts.append({"icon": "📉", "text": "Participation collapse — both sides reducing", "kind": "participation"})

    if ctx.get("flow_l1_l2_conflict"):
        alerts.append(
            {
                "icon": "⚠️",
                "text": "Structure vs flow tension — not a clean reversal signal",
                "kind": "tension",
            }
        )

    macro_align = str(ctx.get("macro_alignment") or "")
    if macro_align in {"strong_contradiction", "risk_off_pressure"} and regime in {
        "structural_bullish",
        "accumulation",
    }:
        alerts.append({"icon": "⚠️", "text": "Macro headwind into bullish structure", "kind": "macro"})

    # De-dupe by text
    seen: set[str] = set()
    out: list[dict[str, str]] = []
    for a in alerts:
        if a["text"] in seen:
            continue
        seen.add(a["text"])
        out.append(a)
    return out[:5]


def _priority_score(alerts: list[dict[str, str]], ctx: dict[str, Any]) -> float:
    score = 0.0
    kind_weights = {
        "flow_extreme": 28,
        "transition": 26,
        "exhaustion": 24,
        "deterioration": 22,
        "tension": 18,
        "opportunity": 20,
        "macro": 16,
        "flow": 14,
        "participation": 20,
    }
    for a in alerts:
        score += kind_weights.get(a.get("kind", ""), 10)
    internal = ctx.get("internal_scores") or {}
    score += min(15.0, float(internal.get("flow_intensity") or 0) * 0.12)
    if ctx.get("pending_flip_target"):
        score += 12
    weeks = int(ctx.get("weeks_in_regime") or 0)
    if weeks <= 2 and str(ctx.get("structural_regime")) not in {"neutral_rotation"}:
        score += 8
    extreme = str(ctx.get("positioning_extreme") or "")
    if extreme not in {"", "none"}:
        score += 10
    return score


def _tier_from_score(score: float, alerts: list[dict[str, str]]) -> str:
    if score >= 55 or any(a.get("kind") == "flow_extreme" for a in alerts):
        return PRIORITY_HIGH
    if score >= 32:
        return PRIORITY_DEVELOPING
    if score >= 18:
        return PRIORITY_WATCHLIST
    return PRIORITY_LOW


def build_attention_layer(
    *,
    market: str,
    ctx: dict[str, Any],
    hist: pd.DataFrame,
    net: float | None = None,
    w1: float | None = None,
    long_w1: float | None = None,
    short_w1: float | None = None,
) -> dict[str, Any]:
    """Attention metadata attached to institutional_context."""
    sd = ctx.get("scanner_display") or {}
    posture = str(ctx.get("tactical_posture") or "watch")
    extreme = str(ctx.get("positioning_extreme") or "none")
    tactical_readable = _tactical_readable(
        posture, str(ctx.get("tactical_posture_label") or ""), extreme
    )

    alerts = _detect_alerts(
        market=market,
        ctx=ctx,
        hist=hist,
        net=net,
        w1=w1,
        long_w1=long_w1,
        short_w1=short_w1,
    )
    score = _priority_score(alerts, ctx)
    tier = _tier_from_score(score, alerts)

    dominant = build_dominant_narrative(
        structural_regime=str(ctx.get("structural_regime") or ""),
        structural_short=str(sd.get("structural") or ""),
        flow_momentum=str(ctx.get("flow_momentum") or ""),
        flow_label=str(sd.get("flow") or ""),
        macro_alignment=str(ctx.get("macro_alignment") or ""),
        macro_short=str(sd.get("macro") or ""),
        positioning_extreme=extreme,
        exhaustion_short=str(sd.get("exhaustion") or ""),
        flow_conflict=bool(ctx.get("flow_l1_l2_conflict")),
        weeks_in_regime=int(ctx.get("weeks_in_regime") or 0),
        tactical_readable=tactical_readable,
        pending_flip=ctx.get("pending_flip_target"),
    )

    headline = alerts[0]["text"] if alerts else dominant.split("—")[0].strip()[:72]

    return {
        "priority_tier": tier,
        "priority_label": PRIORITY_LABELS[tier],
        "priority_score": round(score, 1),
        "dominant_narrative": dominant,
        "priority_headline": headline,
        "tactical_readable": tactical_readable,
        "alerts": alerts,
    }


def aggregate_priority_markets(
    week_records: list[dict[str, Any]],
    *,
    top_n: int = 6,
    calendar_week: str = "",
) -> dict[str, Any]:
    """Delegate to full-universe priority board (expanded registry + macro/proxy scoring)."""
    from hptl.context.priority_board import aggregate_priority_markets as _aggregate

    return _aggregate(week_records, top_n=top_n, calendar_week=calendar_week)
