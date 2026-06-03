"""Five-pillar alignment for the Thesis Opportunity Engine."""
from __future__ import annotations

from typing import Any, Literal

from hptl.seasonality.engine import seasonality_pass as _seasonality_pass_fn
from hptl.valuation.engine import valuation_pass as _valuation_pass_fn

Direction = Literal["long", "short", "neutral"]

PILLAR_IDS = ("valuation", "institutions", "retail", "seasonality", "location")


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def display_instrument_name(market: str) -> str:
    base = str(market or "").split("/")[0].strip()
    return base.upper() if base else str(market or "").upper()


def _bias_direction(cot_bias: str | None) -> Direction:
    b = str(cot_bias or "").lower()
    if "bull" in b:
        return "long"
    if "bear" in b:
        return "short"
    return "neutral"


def _effective_direction(thesis_direction: str, snap: dict[str, Any]) -> Direction:
    d = str(thesis_direction or "neutral").lower()
    if d in {"long", "short"}:
        return d  # type: ignore[return-value]
    return _bias_direction(snap.get("cot_bias"))


def _institutions_state(cot_bias: str | None, cot_score: float | None) -> str:
    b = str(cot_bias or "").strip()
    if not b or b.upper() == "N/A":
        return "UNAVAILABLE"
    score = _num(cot_score)
    strong = score is not None and score >= 6.0
    if "bull" in b.lower():
        return "STRONGLY BULLISH" if strong else "BULLISH"
    if "bear" in b.lower():
        return "STRONGLY BEARISH" if strong else "BEARISH"
    if "neutral" in b.lower():
        return "NEUTRAL"
    return b.upper()


def _retail_state(snap: dict[str, Any]) -> str:
    net = _num(snap.get("retail_net"))
    if net is None:
        return "UNAVAILABLE"
    if abs(net) < 500:
        return "NEUTRAL"
    return "BULLISH" if net > 0 else "BEARISH"


def _location_state(zone_focus: str | None) -> str:
    z = str(zone_focus or "").strip()
    if not z or z.upper() == "N/A":
        return "UNAVAILABLE"
    low = z.lower()
    if "demand first" in low or low.startswith("demand"):
        return "AT DEMAND"
    if "look for demand" in low or "demand watch" in low:
        return "AWAITING DEMAND ZONE"
    if "demand" in low:
        return "AT DEMAND"
    if "supply first" in low or low.startswith("supply"):
        return "AT SUPPLY"
    if "look for supply" in low or "supply watch" in low:
        return "AWAITING SUPPLY ZONE"
    if "supply" in low:
        return "AT SUPPLY"
    if "wait" in low or "mixed" in low:
        return "AWAITING DEMAND ZONE"
    return z.upper()


def _location_pass(zone_state: str, direction: Direction) -> bool | None:
    if zone_state == "UNAVAILABLE":
        return None
    if direction == "long":
        return zone_state in {"AT DEMAND", "AWAITING DEMAND ZONE"}
    if direction == "short":
        return zone_state in {"AT SUPPLY", "AWAITING SUPPLY ZONE"}
    return zone_state not in {"UNAVAILABLE"}


def _institutions_pass(state: str, direction: Direction) -> bool | None:
    if state == "UNAVAILABLE":
        return None
    if direction == "long":
        return "BULL" in state
    if direction == "short":
        return "BEAR" in state
    return state == "NEUTRAL"


def _retail_pass(state: str, direction: Direction) -> bool | None:
    if state == "UNAVAILABLE":
        return None
    if direction == "long":
        return state == "BEARISH"
    if direction == "short":
        return state == "BULLISH"
    return state == "NEUTRAL"


def _valuation_pillar(snap: dict[str, Any], direction: Direction) -> dict[str, Any]:
    wired = bool(snap.get("valuation_wired")) or str(snap.get("valuation_bias") or "") not in {"", "UNAVAILABLE", "PENDING"}
    bias = str(snap.get("valuation_bias") or "UNAVAILABLE")
    score = _num(snap.get("valuation_score"))
    if not wired:
        return {
            "pillar": "valuation",
            "label": "Valuation",
            "state": "UNAVAILABLE",
            "score_display": "—",
            "pass": False,
            "wired": False,
            "one_line": str(snap.get("valuation_reason") or "Valuation engine has no data for this week."),
        }
    passed = _valuation_pass_fn(bias, direction)
    return {
        "pillar": "valuation",
        "label": "Valuation",
        "state": bias.upper(),
        "score_display": f"{score:.1f} / 10" if score is not None else "—",
        "pass": passed,
        "wired": True,
        "one_line": str(snap.get("valuation_reason") or ""),
    }


def _seasonality_pillar(snap: dict[str, Any], direction: Direction) -> dict[str, Any]:
    wired = bool(snap.get("seasonality_wired")) or str(snap.get("seasonality_bias") or "") not in {
        "",
        "UNAVAILABLE",
        "PENDING",
    }
    bias = str(snap.get("seasonality_bias") or "UNAVAILABLE")
    score = _num(snap.get("seasonality_score"))
    if not wired:
        return {
            "pillar": "seasonality",
            "label": "Seasonality",
            "state": "UNAVAILABLE",
            "score_display": "—",
            "pass": False,
            "wired": False,
            "one_line": str(snap.get("seasonality_reason") or "Seasonality engine has no data for this week."),
        }
    passed = _seasonality_pass_fn(bias, direction)
    return {
        "pillar": "seasonality",
        "label": "Seasonality",
        "state": bias.upper(),
        "score_display": f"{score:.1f} / 10" if score is not None else "—",
        "pass": passed,
        "wired": True,
        "one_line": str(snap.get("seasonality_reason") or ""),
    }


def evaluate_pillars(
    snap: dict[str, Any],
    *,
    direction: Direction,
) -> list[dict[str, Any]]:
    """Evaluate five pillars from the latest weekly snapshot."""
    pillars: list[dict[str, Any]] = [
        _valuation_pillar(snap, direction),
    ]

    inst_state = _institutions_state(snap.get("cot_bias"), _num(snap.get("cot_score")))
    inst_pass = _institutions_pass(inst_state, direction)
    pillars.append(
        {
            "pillar": "institutions",
            "label": "Institutions",
            "state": inst_state,
            "score_display": f"{_num(snap.get('cot_score')):.1f} / 10" if _num(snap.get("cot_score")) is not None else "—",
            "pass": bool(inst_pass) if inst_pass is not None else False,
            "wired": inst_state != "UNAVAILABLE",
            "one_line": _institutions_one_line(inst_state, snap),
        }
    )

    retail_state = _retail_state(snap)
    retail_pass = _retail_pass(retail_state, direction)
    pillars.append(
        {
            "pillar": "retail",
            "label": "Retail",
            "state": retail_state,
            "score_display": "—",
            "pass": bool(retail_pass) if retail_pass is not None else False,
            "wired": retail_state != "UNAVAILABLE",
            "one_line": _retail_one_line(retail_state, snap),
        }
    )

    loc_state = _location_state(snap.get("zone_focus"))
    loc_pass = _location_pass(loc_state, direction)
    pillars.append(
        {
            "pillar": "location",
            "label": "Location",
            "state": loc_state,
            "score_display": "—",
            "pass": bool(loc_pass) if loc_pass is not None else False,
            "wired": loc_state != "UNAVAILABLE",
            "one_line": _location_one_line(loc_state, snap.get("zone_focus")),
        }
    )

    pillars.append(_seasonality_pillar(snap, direction))

    return pillars


def _institutions_one_line(state: str, snap: dict[str, Any]) -> str:
    ps = str(snap.get("positioning_state") or "").strip()
    if state == "UNAVAILABLE":
        return "Institutional positioning not available for this week."
    if ps and ps.upper() != "N/A":
        return f"Institutions read {state.lower()} ({ps})."
    return f"Institutions read {state.lower()} on the COT score scale."


def _retail_one_line(state: str, snap: dict[str, Any]) -> str:
    net = _num(snap.get("retail_net"))
    if state == "UNAVAILABLE" or net is None:
        return "Retail proxy (non-reportable) not on this snapshot."
    side = "long" if net > 0 else "short"
    return f"Retail proxy is net {side} ({int(net):+,} contracts)."


def _location_one_line(state: str, zone_raw: str | None) -> str:
    if state == "UNAVAILABLE":
        return "Location / zone tag not on this snapshot."
    if zone_raw:
        return f"HTPL location tag: {zone_raw}."
    return f"Location state: {state.replace('_', ' ')}."


def alignment_summary(pillars: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(pillars)
    passed = sum(1 for p in pillars if p.get("pass") is True)
    return {
        "pass": passed,
        "total": total,
        "label": f"{passed} / {total}",
    }
