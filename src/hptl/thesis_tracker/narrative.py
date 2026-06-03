"""Generate human-readable auto summaries and evolution-log notes.

These are descriptive strings built from already-computed numbers. No scoring.
"""

from __future__ import annotations

from typing import Any

from hptl.thesis_tracker.conviction import compute_trend

_TREND_WORD = {
    "improving": "improving",
    "deteriorating": "deteriorating",
    "stable": "holding steady",
}


def _fmt(v: Any, nd: int = 1) -> str | None:
    if isinstance(v, (int, float)):
        return f"{float(v):,.{nd}f}".rstrip("0").rstrip(".") if nd else f"{int(round(v)):,}"
    return None


def _signed(v: Any) -> str | None:
    if isinstance(v, (int, float)):
        n = float(v)
        return f"{'+' if n >= 0 else ''}{n:,.1f}".rstrip("0").rstrip(".")
    return None


def build_auto_summary(thesis: dict[str, Any]) -> str:
    snaps = thesis.get("snapshots") or []
    if not snaps:
        return "No weekly snapshot captured yet."
    latest = snaps[-1]
    age = len({str(s.get("week") or "") for s in snaps if s.get("week")})
    direction = str(thesis.get("direction_bias") or "neutral").upper()
    parts: list[str] = [f"Week {max(age, 1)}", f"{direction} bias"]

    conv = latest.get("conviction_score")
    trend = compute_trend(snaps)
    if isinstance(conv, (int, float)):
        parts.append(f"conviction {int(round(conv))} ({_TREND_WORD.get(trend, trend)})")

    detail: list[str] = []
    if latest.get("cot_bias"):
        c = _fmt(latest.get("cot_score"))
        detail.append(f"COT {str(latest['cot_bias']).lower()}" + (f" ({c})" if c else ""))
    if isinstance(latest.get("macro_score"), (int, float)):
        detail.append(f"macro {_fmt(latest.get('macro_score'))}")
    if isinstance(latest.get("structural_score"), (int, float)):
        detail.append(f"structural {_fmt(latest.get('structural_score'))}")
    if latest.get("positioning_state"):
        detail.append(str(latest["positioning_state"]).lower())

    summary = " · ".join(parts)
    if detail:
        summary += ". " + ", ".join(s for s in detail if s).capitalize() + "."
    summary += " Valuation/seasonality/retail pending wiring."
    return summary


def build_evolution_note(prev: dict[str, Any] | None, curr: dict[str, Any]) -> str:
    """Auto note comparing the new snapshot to the prior one (or first capture)."""
    if prev is None:
        bits = ["Thesis opened — first snapshot captured"]
        conv = curr.get("conviction_score")
        if isinstance(conv, (int, float)):
            bits.append(f"conviction {int(round(conv))}")
        if curr.get("cot_bias"):
            bits.append(f"COT {str(curr['cot_bias']).lower()}")
        return ". ".join(bits) + "."

    bits: list[str] = []
    pc, cc = prev.get("conviction_score"), curr.get("conviction_score")
    if isinstance(pc, (int, float)) and isinstance(cc, (int, float)):
        delta = int(round(cc)) - int(round(pc))
        arrow = "→"
        bits.append(f"Conviction {int(round(pc))} {arrow} {int(round(cc))} ({'+' if delta >= 0 else ''}{delta})")
    elif isinstance(cc, (int, float)):
        bits.append(f"Conviction {int(round(cc))}")

    nv = curr.get("one_week_net_change")
    if isinstance(nv, (int, float)) and abs(nv) > 0:
        bits.append(f"net positioning {_signed(nv)} w/w")

    if prev.get("cot_bias") != curr.get("cot_bias") and curr.get("cot_bias"):
        bits.append(f"COT bias → {str(curr['cot_bias']).lower()}")

    pm, cm = prev.get("macro_score"), curr.get("macro_score")
    if isinstance(pm, (int, float)) and isinstance(cm, (int, float)) and round(pm, 1) != round(cm, 1):
        bits.append(f"macro {_fmt(pm)} → {_fmt(cm)}")

    if not bits:
        return "Weekly snapshot captured — little material change."
    return ". ".join(bits) + "."
