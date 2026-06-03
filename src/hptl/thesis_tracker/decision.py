"""Decision engine: turn weekly snapshots into a plain-English thesis narrative.

Source of truth for the Thesis Tracker's narrative-first UI. Everything here is
*derived from data already captured in snapshots* — no new feeds, no fabricated
prices. Where a confirmation is not wired in HPTL (valuation, seasonality,
retail positioning, price/zone), it is reported explicitly as missing.

Output shape (attached to each thesis as ``decision``):
    priority_tier:int(1-3), priority_label, priority_reason
    confidence, confidence_reason
    headline, story:[str], interpretation
    missing_confirmations:[{label, wired:false}]
    evolution:{improved:[], deteriorated:[], unchanged:[]}
    upgrade_triggers:[str], invalidation_triggers:[str], next_trigger
    readiness:{label, met, total, checks:[{label, met}]}
    weeks_observed:int
"""

from __future__ import annotations

from typing import Any

from hptl.thesis_tracker.conviction import compute_trend, current_conviction
from hptl.thesis_tracker.models import (
    STATUS_ACTIVE,
    STATUS_COMPLETED,
    STATUS_DEVELOPING,
    STATUS_DISCOVERED,
    STATUS_INVALIDATED,
    STATUS_READY,
    norm_status,
)

# Confirmations that do not yet exist anywhere in the HPTL pipeline.
MISSING_CONFIRMATIONS = (
    {"label": "Valuation", "wired": False},
    {"label": "Seasonality", "wired": False},
    {"label": "Retail positioning", "wired": False},
    {"label": "Price / demand zone", "wired": False},
)

_CONV_HIGH = 66
_CONV_MOD = 45


# ---- small helpers -----------------------------------------------------------

def _num(v: Any) -> float | None:
    if isinstance(v, bool) or v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v) if v == v else None
    return None


def _k(v: float | None) -> str:
    """Compact contract notation, e.g. -174720 -> '-175k', 640 -> '640'."""
    if v is None:
        return "n/a"
    a = abs(v)
    if a >= 1000:
        return f"{v / 1000:,.0f}k"
    return f"{v:,.0f}"


def _signed_k(v: float | None) -> str:
    if v is None:
        return "n/a"
    s = _k(v)
    return s if (v < 0 or s.startswith("-")) else f"+{s}"


def _first_last(snaps: list[dict], field: str) -> tuple[float | None, float | None]:
    vals = [(s.get("week"), _num(s.get(field))) for s in snaps if _num(s.get(field)) is not None]
    if not vals:
        return None, None
    return vals[0][1], vals[-1][1]


def _effective_dir(direction: str, net_first: float | None, net_last: float | None) -> int:
    if direction == "long":
        return 1
    if direction == "short":
        return -1
    if net_first is not None and net_last is not None:
        if net_last > net_first:
            return 1
        if net_last < net_first:
            return -1
    return 0


def _significant(delta: float | None, base: float | None) -> bool:
    if delta is None:
        return False
    floor = max(1000.0, 0.04 * abs(base)) if base else 1000.0
    return abs(delta) >= floor


# ---- evolution ---------------------------------------------------------------

def _evolution(snaps: list[dict], direction: str) -> dict[str, list[str]]:
    improved: list[str] = []
    deteriorated: list[str] = []
    unchanged: list[str] = []
    if len(snaps) < 2:
        last = snaps[-1] if snaps else {}
        net = _num(last.get("net_value"))
        if net is not None:
            unchanged.append(f"Market net {'long' if net > 0 else 'short'} ({_signed_k(net)})")
        return {"improved": improved, "deteriorated": deteriorated, "unchanged": unchanged}

    lf, ll = _first_last(snaps, "long_value")
    sf, sl = _first_last(snaps, "short_value")
    nf, nl = _first_last(snaps, "net_value")
    eff = _effective_dir(direction, nf, nl)

    # longs
    if lf is not None and ll is not None:
        d = ll - lf
        if _significant(d, lf):
            txt = f"Long exposure {'increased' if d > 0 else 'decreased'} ({_signed_k(d)})"
            (improved if (d * (eff or 1) > 0) else deteriorated).append(txt)
        else:
            unchanged.append("Long exposure broadly flat")

    # shorts (favourable when shorts move opposite to the bias)
    if sf is not None and sl is not None:
        d = sl - sf
        if _significant(d, sf):
            txt = f"Short exposure {'increased' if d > 0 else 'decreased'} ({_signed_k(d)})"
            (improved if (d * (eff or 1) < 0) else deteriorated).append(txt)
        else:
            unchanged.append("Short exposure broadly flat")

    # net
    if nf is not None and nl is not None:
        d = nl - nf
        if _significant(d, nf):
            txt = f"Net positioning moved {_signed_k(d)} ({_signed_k(nf)} → {_signed_k(nl)})"
            (improved if (d * (eff or 1) > 0) else deteriorated).append(txt)
        unchanged.append(f"Market still net {'long' if nl > 0 else 'short'}")

    # momentum (slowing / flipping)
    mom = [_num(s.get("one_week_net_change")) for s in snaps if _num(s.get("one_week_net_change")) is not None]
    if len(mom) >= 2:
        last_m, prev_m = mom[-1], mom[-2]
        if eff and last_m * eff < 0:
            deteriorated.append("Weekly momentum turned against the thesis")
        elif abs(last_m) < abs(prev_m) * 0.6:
            deteriorated.append("Momentum slowed week-on-week")
        elif eff and last_m * eff > 0 and abs(last_m) >= abs(prev_m):
            improved.append("Weekly momentum building in favour")

    # structural alignment (only when present)
    stf, stl = _first_last(snaps, "structural_score")
    if stf is not None and stl is not None and abs(stl - stf) >= 4:
        d = stl - stf
        txt = f"Structural score {'rose' if d > 0 else 'fell'} ({stf:.0f} → {stl:.0f})"
        (improved if (d * (eff or 1) > 0) else deteriorated).append(txt)

    # conviction
    conv_series = [s.get("conviction_score") for s in snaps if isinstance(s.get("conviction_score"), (int, float))]
    if len(conv_series) >= 2:
        d = conv_series[-1] - conv_series[0]
        if d >= 3:
            improved.append(f"Composite conviction rose ({conv_series[0]} → {conv_series[-1]})")
        elif d <= -3:
            deteriorated.append(f"Composite conviction fell ({conv_series[0]} → {conv_series[-1]})")

    return {"improved": improved, "deteriorated": deteriorated, "unchanged": unchanged}


# ---- story -------------------------------------------------------------------

def _interpretation(dl: float, ds: float, dn: float) -> str:
    """``dl/ds/dn`` are significance-gated leg/net deltas (0 if not material)."""
    longs_up, longs_dn = dl > 0, dl < 0
    shorts_up, shorts_dn = ds > 0, ds < 0
    net_up, net_dn = dn > 0, dn < 0

    if net_up and longs_up and shorts_dn:
        return "Short covering and fresh accumulation — bearish pressure is unwinding."
    if net_up and longs_up:
        return "Fresh long accumulation is driving net positioning higher."
    if net_up and shorts_dn:
        return "Short covering is lifting net positioning — bears are stepping back."
    if net_dn and shorts_up and longs_dn:
        return "Fresh short selling and long liquidation — distribution underway."
    if net_dn and shorts_up:
        return "Shorts are pressing; net positioning is deteriorating."
    if net_dn and longs_dn:
        return "Longs are being liquidated — the bid is fading."
    if longs_up and shorts_up:
        return "Both legs are building — two-way conviction, not a one-sided trend yet."
    return "Positioning is broadly stable with no decisive one-sided flow."


def _story(snaps: list[dict], direction: str) -> tuple[list[str], str]:
    if not snaps:
        return (["No weekly snapshots captured yet."], "Awaiting first snapshot.")
    weeks = len({s.get("week") for s in snaps if s.get("week")})
    lf, ll = _first_last(snaps, "long_value")
    sf, sl = _first_last(snaps, "short_value")
    nf, nl = _first_last(snaps, "net_value")
    story: list[str] = []

    if nf is not None and nl is not None and weeks >= 2:
        dn = nl - nf
        verb = "improved" if dn > 0 else "deteriorated" if dn < 0 else "held flat"
        story.append(
            f"Over the last {weeks} weeks net positioning {verb} from {_signed_k(nf)} to {_signed_k(nl)} "
            f"({_signed_k(dn)})."
        )
    elif nl is not None:
        story.append(f"Net positioning is {_signed_k(nl)} ({'net long' if nl > 0 else 'net short'}).")

    if lf is not None and ll is not None and (sf is not None and sl is not None):
        story.append(
            f"Speculative longs went from {_k(lf)} to {_k(ll)} while shorts went from {_k(sf)} to {_k(sl)}."
        )

    if nl is not None:
        side = "long" if nl > 0 else "short"
        dn = (nl - nf) if (nf is not None) else 0.0
        if side == "short" and dn > 0:
            story.append("The market remains net short, but bearish pressure continues to weaken.")
        elif side == "long" and dn > 0:
            story.append("The market is net long and net-long conviction is building.")
        elif side == "short" and dn < 0:
            story.append("The market is net short and shorts are still extending.")
        elif side == "long" and dn < 0:
            story.append("The market remains net long, but long conviction is fading.")
        else:
            story.append(f"The market remains net {side} with little net change.")

    def _gated(last: float | None, first: float | None) -> float:
        if last is None or first is None:
            return 0.0
        d = last - first
        return d if _significant(d, first) else 0.0

    interpretation = _interpretation(_gated(ll, lf), _gated(sl, sf), _gated(nl, nf))
    return story, interpretation


# ---- readiness / priority / confidence --------------------------------------

def _readiness(snaps: list[dict], direction: str, status: str, conv: float | None, trend: str) -> dict[str, Any]:
    nf, nl = _first_last(snaps, "net_value")
    eff = _effective_dir(direction, nf, nl)
    net_confirms = (nf is not None and nl is not None and eff and (nl - nf) * eff > 0)
    stl = _first_last(snaps, "structural_score")[1]
    struct_aligned = stl is not None and ((stl >= 50 and eff >= 0) or (stl <= 50 and eff < 0))

    checks = [
        {"label": "Conviction trend improving", "met": trend == "improving"},
        {"label": f"Conviction ≥ {_CONV_MOD + 10}", "met": conv is not None and conv >= _CONV_MOD + 10},
        {"label": "Positioning confirming the bias", "met": bool(net_confirms)},
        {"label": "Structural regime aligned", "met": bool(struct_aligned)},
    ]
    met = sum(1 for c in checks if c["met"])

    if status == STATUS_ACTIVE:
        label = "In trade — manage the position"
    elif status == STATUS_READY:
        label = "Limit-order preparation justified"
    elif status == STATUS_INVALIDATED:
        label = "Invalidated — stand aside"
    elif status == STATUS_COMPLETED:
        label = "Completed"
    elif met >= 3:
        label = "Approaching readiness — prepare watch levels"
    elif met == 2:
        label = "Developing — track weekly"
    else:
        label = "Early — observation only"
    return {"label": label, "met": met, "total": len(checks), "checks": checks}


def _priority(status: str, readiness_met: int) -> dict[str, Any]:
    if status in (STATUS_INVALIDATED, STATUS_COMPLETED):
        return {"tier": 3, "label": "Closed / archived", "reason": "Thesis is no longer actionable."}
    if status in (STATUS_READY, STATUS_ACTIVE):
        return {"tier": 1, "label": "Ready soon — monitor closely", "reason": f"Status {status}; near or in execution."}
    if status == STATUS_DEVELOPING:
        if readiness_met >= 3:
            return {"tier": 1, "label": "Ready soon — monitor closely", "reason": "Developing with 3+ readiness checks met."}
        return {"tier": 2, "label": "Developing — track weekly", "reason": "Conditions forming; not yet aligned."}
    # DISCOVERED
    if readiness_met >= 3:
        return {"tier": 2, "label": "Developing — track weekly", "reason": "Early signal already showing alignment."}
    return {"tier": 3, "label": "Observation only", "reason": "Initial signal; monitor for development."}


def _confidence(conv: float | None, trend: str, present: list[str]) -> tuple[str, str]:
    if conv is None:
        return "Insufficient", "No scored components available yet."
    n = len(present or [])
    if conv >= _CONV_HIGH and trend != "deteriorating":
        label = "High"
    elif conv >= _CONV_MOD:
        label = "Moderate"
    else:
        label = "Low"
    if trend == "deteriorating" and label == "High":
        label = "Moderate"
    reason = (
        f"Composite {int(round(conv))}/100 from {n} wired component(s) ({', '.join(present) or 'none'}); "
        "valuation, seasonality and retail still unconfirmed."
    )
    return label, reason


def _upgrade_triggers(direction: str, evo: dict[str, list[str]], conv: float | None, present: list[str]) -> list[str]:
    out: list[str] = []
    long_bias = direction != "short"
    out.append("Another week of long accumulation" if long_bias else "Another week of short accumulation")
    out.append("Further short reduction" if long_bias else "Further long liquidation")
    if conv is not None:
        out.append(f"Composite conviction holds above {max(_CONV_MOD, int(round(conv)))}")
    if "structural" not in (present or []):
        out.append("Structural regime confirms the bias")
    out.append("Price reaches and defends a demand/supply zone (price feed not wired)")
    return out[:5]


def _invalidation_triggers(direction: str, conv: float | None) -> list[str]:
    long_bias = direction != "short"
    out = [
        "Net positioning reverses against the thesis",
        "Weekly momentum flips and sustains the other way",
    ]
    if conv is not None:
        out.append(f"Composite conviction falls below {max(20, int(round(conv)) - 15)}")
    out.append("Demand zone fails / key level breaks (price feed not wired)")
    return out


def _headline(direction: str, evo: dict[str, list[str]], priority: dict[str, Any]) -> str:
    mover = (evo.get("improved") or [None])[0] or (evo.get("deteriorated") or [None])[0]
    side = {"long": "Long", "short": "Short", "neutral": "Neutral"}.get(direction, "Neutral")
    if mover:
        return f"{side} thesis — {mover.lower()}."
    return f"{side} thesis — {priority['label'].lower()}."


# ---- public ------------------------------------------------------------------

def build_decision(thesis: dict[str, Any]) -> dict[str, Any]:
    snaps = thesis.get("snapshots") or []
    direction = str(thesis.get("direction_bias") or "neutral").lower()
    status = norm_status(thesis.get("status"))
    conv = current_conviction(snaps)
    trend = compute_trend(snaps)
    present = (snaps[-1].get("conviction_components_present") if snaps else []) or []

    evo = _evolution(snaps, direction)
    story, interpretation = _story(snaps, direction)
    readiness = _readiness(snaps, direction, status, conv, trend)
    priority = _priority(status, readiness["met"])
    confidence, confidence_reason = _confidence(conv, trend, present)
    weeks = len({s.get("week") for s in snaps if s.get("week")})

    return {
        "priority_tier": priority["tier"],
        "priority_label": priority["label"],
        "priority_reason": priority["reason"],
        "confidence": confidence,
        "confidence_reason": confidence_reason,
        "headline": _headline(direction, evo, priority),
        "story": story,
        "interpretation": interpretation,
        "missing_confirmations": [dict(m) for m in MISSING_CONFIRMATIONS],
        "evolution": evo,
        "upgrade_triggers": _upgrade_triggers(direction, evo, conv, present),
        "invalidation_triggers": _invalidation_triggers(direction, conv),
        "next_trigger": (_upgrade_triggers(direction, evo, conv, present) or ["Monitor for development."])[0],
        "readiness": readiness,
        "weeks_observed": weeks,
    }
