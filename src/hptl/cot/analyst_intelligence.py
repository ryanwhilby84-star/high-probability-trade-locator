"""Weekly Analysis — directional narrative over verified COT exports.

Reads only:
  - cot_weekly_inspector_latest (percentiles, flow, temperature, cross)
  - cot_positioning_research_latest (markers, analogues)

Does NOT recalculate percentiles, extremes, or rotations.
Does NOT predict price or emit trade signals.
Every sentence maps to fields already present in those exports.
Never hedges with buying/selling or bullish/bearish when direction is known.
"""

from __future__ import annotations

from typing import Any

from hptl.cot.weekly_inspector_export import expand_compact_market

VERSION = "cot_weekly_analysis_v2"
ENGINE = "weekly_analysis"

GROUP_LABEL = {
    "commercial": "Commercials",
    "noncommercial": "Non-Commercials",
    "nonreportable": "Non-Reportables",
}

PROGRESSION = (
    "Neutral",
    "Approaching Extreme",
    "Extreme",
    "Setup Developing",
    "Early Rotation",
    "Confirmed Rotation",
)

DISCLAIMER = (
    "Weekly Analysis organises verified COT statistics into an evidence narrative. "
    "It is not a forecast, trade signal, or substitute for your own judgement."
)

_ORDINAL = {
    1: "first",
    2: "second",
    3: "third",
    4: "fourth",
    5: "fifth",
    6: "sixth",
    7: "seventh",
    8: "eighth",
    9: "ninth",
    10: "tenth",
}


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _weeks(inspector_block: dict[str, Any]) -> list[dict[str, Any]]:
    expanded = expand_compact_market(inspector_block) if inspector_block.get("rows") else inspector_block
    return list(expanded.get("weeks") or [])


def _latest_weeks(inspector_block: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
    weeks = _weeks(inspector_block)
    if not weeks:
        return None, None
    if len(weeks) == 1:
        return weeks[-1], None
    return weeks[-1], weeks[-2]


def _active_markers(research: dict[str, Any], source_week: str | None) -> list[dict[str, Any]]:
    markers = research.get("markers") or []
    if not source_week:
        return []
    week = str(source_week)[:10]
    return [m for m in markers if str(m.get("date") or "")[:10] == week]


def _has_rotation(markers: list[dict[str, Any]], group: str | None = None) -> bool:
    for m in markers:
        et = str(m.get("event_type") or "")
        if et not in ("major_rotation", "rapid_velocity"):
            continue
        if group and str(m.get("group") or "") != group:
            continue
        return True
    return False


def _has_extreme_marker(markers: list[dict[str, Any]], group: str | None = None) -> bool:
    for m in markers:
        et = str(m.get("event_type") or "")
        if et not in ("absolute_extreme", "local_extreme"):
            continue
        if group and str(m.get("group") or "") != group:
            continue
        return True
    return False


def progression_state(group_pack: dict[str, Any] | None, *, has_rotation: bool) -> str:
    """Map existing percentile / temperature / rotation flags → progression ladder."""
    if not group_pack:
        return "Neutral"
    pct = _finite(group_pack.get("percentile"))
    temp = str(group_pack.get("temperature") or "")
    is_extreme = bool(group_pack.get("is_extreme"))
    if pct is not None and (pct <= 10 or pct >= 90):
        is_extreme = True

    if has_rotation and temp in ("recovering", "recovering_strong", "cooling_from_extreme"):
        return "Confirmed Rotation"
    if has_rotation:
        return "Early Rotation"
    if temp in ("recovering", "recovering_strong", "cooling_from_extreme") and is_extreme:
        return "Early Rotation"
    if is_extreme and temp in ("heating_rapidly", "heating", "deepening_extreme", "building"):
        return "Setup Developing"
    if is_extreme:
        return "Extreme"
    if pct is not None and (pct <= 20 or pct >= 80):
        return "Approaching Extreme"
    return "Neutral"


def _sign(v: float | None) -> int:
    if v is None or abs(v) < 1:
        return 0
    return 1 if v > 0 else -1


def _flow_noun(sign: int) -> str:
    if sign > 0:
        return "buying"
    if sign < 0:
        return "selling"
    return "positioning"


def _exposure_phrase(sign: int) -> str:
    if sign > 0:
        return "bullish exposure"
    if sign < 0:
        return "bearish exposure"
    return "exposure"


def _ordinal_week(n: int) -> str:
    if n in _ORDINAL:
        return _ORDINAL[n]
    return f"{n}th"


def _change_streak(weeks: list[dict[str, Any]], group: str) -> tuple[int, int]:
    """Return (streak_weeks, sign) counting backward from latest non-flat print."""
    if not weeks:
        return 0, 0
    streak = 0
    sign = 0
    for w in reversed(weeks):
        ch = _finite((w.get(group) or {}).get("weekly_change"))
        s = _sign(ch)
        if s == 0:
            if streak:
                break
            continue
        if sign == 0:
            sign = s
            streak = 1
            continue
        if s == sign:
            streak += 1
        else:
            break
    return streak, sign


def _pace(cur_ch: float | None, prior_ch: float | None) -> str | None:
    """accelerated | slowed | reversed | None — same-sign magnitude compare."""
    cs, ps = _sign(cur_ch), _sign(prior_ch)
    if cs == 0 or ps == 0:
        return None
    if cs != ps:
        return "reversed"
    assert cur_ch is not None and prior_ch is not None
    if abs(cur_ch) > abs(prior_ch) * 1.15:
        return "accelerated"
    if abs(cur_ch) < abs(prior_ch) * 0.85:
        return "slowed"
    return None


def _opposition_streak(weeks: list[dict[str, Any]]) -> tuple[int, str | None]:
    """Consecutive weeks of opposition widening or narrowing from the latest print."""
    if not weeks:
        return 0, None
    latest_flow = str((weeks[-1].get("cross") or {}).get("flow") or "")
    if "widening" in latest_flow:
        kind = "widening"
    elif "narrowing" in latest_flow:
        kind = "narrowing"
    else:
        return 0, None
    streak = 0
    for w in reversed(weeks):
        flow = str((w.get("cross") or {}).get("flow") or "")
        if kind in flow:
            streak += 1
        else:
            break
    return streak, kind


def _group_change_sentence(
    label: str,
    cur: dict[str, Any] | None,
    prior: dict[str, Any] | None,
    *,
    streak: int,
    streak_sign: int,
) -> str | None:
    if not cur:
        return None
    ch1 = _finite(cur.get("weekly_change"))
    prior_ch = _finite((prior or {}).get("weekly_change")) if prior else None
    sign = _sign(ch1)
    if sign == 0 and streak_sign == 0:
        return f"{label} positioning was essentially unchanged this week."

    use_sign = sign or streak_sign
    noun = _flow_noun(use_sign)
    pace = _pace(ch1, prior_ch) if sign else None

    if streak >= 2 and sign == streak_sign:
        base = (
            f"{label} have increased {_exposure_phrase(use_sign)} "
            f"for {streak} consecutive weeks"
        )
        if pace == "accelerated":
            return f"{base}; {noun} accelerated this week."
        if pace == "slowed":
            return f"{base}, though {noun} slowed this week."
        return f"{base}."

    if pace == "accelerated":
        return f"{label} {noun} accelerated this week."
    if pace == "slowed":
        return f"{label} {noun} slowed this week."
    if pace == "reversed":
        prior_noun = _flow_noun(_sign(prior_ch))
        verb = "bought" if use_sign > 0 else "sold"
        return f"{label} {verb} this week, reversing the prior week's {prior_noun}."

    if use_sign < 0:
        return f"{label} increased bearish exposure this week."
    return f"{label} increased bullish exposure this week."


def _cross_sentence(weeks: list[dict[str, Any]], cross: dict[str, Any]) -> str | None:
    rel = str(cross.get("relationship") or "")
    flow = str(cross.get("flow") or "")
    streak, kind = _opposition_streak(weeks)

    if rel in ("opposed", "strong_opposition"):
        strength = "strong opposition" if rel == "strong_opposition" else "opposition"
        if kind == "widening" and streak >= 1:
            if streak == 1:
                return f"Cross-group {strength} widened this week."
            return (
                f"Cross-group {strength} widened for the {_ordinal_week(streak)} "
                f"consecutive week."
            )
        if kind == "narrowing" and streak >= 1:
            if streak == 1:
                return f"Cross-group {strength} narrowed this week."
            return (
                f"Cross-group {strength} narrowed for the {_ordinal_week(streak)} "
                f"consecutive week."
            )
        return f"Commercials and Non-Commercials remain in {strength}."
    if rel == "aligned":
        return "Commercial and Non-Commercial positioning are aligned — opposition is not the story."
    if "widening" in flow:
        return "The Commercial–Non-Commercial percentile spread widened this week."
    if "narrowing" in flow:
        return "The Commercial–Non-Commercial percentile spread narrowed this week."
    return None


def _interpretation(
    c: dict[str, Any] | None,
    nc: dict[str, Any] | None,
    *,
    c_prog: str,
    nc_prog: str,
    c_streak: int,
    c_sign: int,
    has_c_rotation: bool,
    cross_line: str | None,
) -> list[str]:
    out: list[str] = []
    c_pct = _finite((c or {}).get("percentile"))
    nc_pct = _finite((nc or {}).get("percentile"))
    nc_ch = _finite((nc or {}).get("weekly_change"))
    nc_sign = _sign(nc_ch)

    if c_prog == "Confirmed Rotation":
        out.append(
            "Commercial rotation evidence is present. Treat this as early structural change, "
            "not a completed reversal."
        )
    elif c_prog == "Early Rotation":
        out.append("Early Commercial rotation signals are appearing; confirmation is still incomplete.")
    elif c_prog == "Setup Developing":
        if c_sign < 0:
            out.append(
                "A positioning setup is developing: Commercials sit in/near extremes and "
                "continue adding shorts."
            )
        elif c_sign > 0:
            out.append(
                "A positioning setup is developing: Commercials sit in/near extremes and "
                "continue adding longs."
            )
        else:
            out.append("A positioning setup is developing as Commercials sit in/near extremes.")
    elif c_prog == "Extreme":
        out.append("Commercials are at an extreme historical percentile without confirmed rotation yet.")
    elif c_prog == "Approaching Extreme":
        if c_pct is not None and c_pct <= 20:
            out.append(
                f"Commercials are approaching historically bearish positioning "
                f"({c_pct:.0f}th percentile) but remain above the strongest historical "
                f"extreme zone (below the 10th)."
            )
        elif c_pct is not None and c_pct >= 80:
            out.append(
                f"Commercials are approaching historically bullish positioning "
                f"({c_pct:.0f}th percentile) but remain below the strongest historical "
                f"extreme zone (above the 90th)."
            )
        else:
            out.append("Commercials are approaching historically notable positioning.")
    else:
        out.append("Commercial positioning remains in a mid-range historical regime.")

    if nc_pct is not None and (nc_pct >= 70 or nc_pct <= 30):
        if nc_sign > 0:
            out.append(
                f"Non-Commercials remain elevated ({nc_pct:.0f}th percentile) and bought this week."
            )
        elif nc_sign < 0:
            out.append(
                f"Non-Commercials remain elevated ({nc_pct:.0f}th percentile) but sold this week."
                if nc_pct >= 70
                else f"Non-Commercials remain depressed ({nc_pct:.0f}th percentile) and sold this week."
            )
        else:
            side = "elevated" if nc_pct >= 70 else "depressed"
            out.append(f"Non-Commercials remain {side} ({nc_pct:.0f}th percentile).")
    elif nc_prog == "Approaching Extreme":
        out.append("Non-Commercial positioning is becoming more crowded toward an extreme.")

    if cross_line:
        out.append(cross_line)

    if not has_c_rotation and c_prog in ("Extreme", "Setup Developing", "Approaching Extreme"):
        if c_streak >= 2 and c_sign < 0:
            out.append(
                f"Commercials have increased bearish exposure for {c_streak} consecutive weeks, "
                "but no confirmed Commercial rotation has appeared yet."
            )
        elif c_streak >= 2 and c_sign > 0:
            out.append(
                f"Commercials have increased bullish exposure for {c_streak} consecutive weeks, "
                "but no confirmed Commercial rotation has appeared yet."
            )
        else:
            out.append("No confirmed Commercial rotation has appeared yet.")

    return out


def _missing_evidence(
    c: dict[str, Any] | None,
    nc: dict[str, Any] | None,
    markers: list[dict[str, Any]],
    *,
    c_prog: str,
    c_sign: int,
    nc_sign: int,
) -> list[str]:
    missing: list[str] = []
    has_c_rot = _has_rotation(markers, "commercial")
    c_temp = str((c or {}).get("temperature") or "")
    nc_pct1 = _finite((nc or {}).get("percentile_change_1w"))

    if not has_c_rot and c_prog in ("Approaching Extreme", "Extreme", "Setup Developing", "Early Rotation"):
        missing.append("Commercial rotation")
    if has_c_rot and c_prog != "Confirmed Rotation":
        missing.append("Confirmation from next week's report")
    if c_sign < 0 and not has_c_rot:
        missing.append("First week where Commercial selling stops increasing")
    if c_sign > 0 and not has_c_rot and c_prog in ("Extreme", "Setup Developing", "Approaching Extreme"):
        missing.append("First week where Commercial buying stops increasing")
    if nc_sign > 0 and (c or {}).get("is_extreme"):
        missing.append("Non-commercial buying weakening")
    elif nc_sign < 0 and (c or {}).get("is_extreme") and _finite((c or {}).get("percentile") or 0) is not None:
        c_pct = _finite((c or {}).get("percentile"))
        if c_pct is not None and c_pct >= 80:
            missing.append("Non-commercial selling weakening")
    if nc_pct1 is not None and nc_pct1 >= 0 and nc_sign >= 0 and (c or {}).get("is_extreme"):
        if "Non-commercial buying weakening" not in missing:
            missing.append("Non-commercial buying weakening")
    if c_temp not in ("recovering", "recovering_strong", "cooling_from_extreme") and (
        c or {}
    ).get("is_extreme"):
        if c_sign < 0:
            missing.append("First positive Commercial net change from the extreme")
        elif c_sign > 0:
            missing.append("First negative Commercial net change from the extreme")
    if not missing:
        missing.append("No material evidence gaps are flagged from this COT print alone")
    return missing


def _present_checklist(
    c: dict[str, Any] | None,
    nc: dict[str, Any] | None,
    cross: dict[str, Any] | None,
    markers: list[dict[str, Any]],
    *,
    c_prog: str,
    c_sign: int,
    nc_sign: int,
) -> list[str]:
    present: list[str] = []
    c_pct = _finite((c or {}).get("percentile"))
    nc_pct = _finite((nc or {}).get("percentile"))
    if c_prog in ("Approaching Extreme", "Extreme", "Setup Developing", "Early Rotation", "Confirmed Rotation"):
        if c_pct is not None and c_pct <= 30:
            if c_sign < 0:
                present.append("Commercials increasingly bearish and still selling")
            else:
                present.append("Commercials already bearish in historical percentile terms")
        elif c_pct is not None and c_pct >= 70:
            if c_sign > 0:
                present.append("Commercials increasingly bullish and still buying")
            else:
                present.append("Commercials already bullish in historical percentile terms")
    if c_sign < 0:
        present.append("Commercial weekly flow is selling (active short addition)")
    elif c_sign > 0:
        present.append("Commercial weekly flow is buying (active long addition)")
    if nc_pct is not None and nc_pct >= 70:
        present.append("Non-Commercials elevated")
    elif nc_pct is not None and nc_pct <= 30:
        present.append("Non-Commercials depressed")
    if nc_sign > 0:
        present.append("Non-Commercials bought this week")
    elif nc_sign < 0:
        present.append("Non-Commercials sold this week")
    rel = str((cross or {}).get("relationship") or "")
    flow = str((cross or {}).get("flow") or "")
    if rel in ("opposed", "strong_opposition"):
        if "widening" in flow:
            present.append("Cross-group opposition widening")
        elif "narrowing" in flow:
            present.append("Cross-group opposition narrowing")
        else:
            present.append("Cross-group opposition present")
    if _has_rotation(markers, "commercial"):
        present.append("Commercial rotation marker on this report week")
    if _has_extreme_marker(markers, "commercial"):
        present.append("Commercial extreme marker on this report week")
    return present


def _next_week(
    c: dict[str, Any] | None,
    nc: dict[str, Any] | None,
    *,
    c_prog: str,
    c_sign: int,
    c_streak: int,
    nc_sign: int,
    has_c_rotation: bool,
    cross_line: str | None,
) -> dict[str, Any]:
    """Specific next-confirmation conditions — no hedged observation tasks."""
    current_bits: list[str] = []
    if c_streak >= 2 and c_sign < 0:
        current_bits.append(
            f"Commercials continue adding shorts ({c_streak} consecutive weeks of selling)."
        )
    elif c_streak >= 2 and c_sign > 0:
        current_bits.append(
            f"Commercials continue adding longs ({c_streak} consecutive weeks of buying)."
        )
    elif c_sign < 0:
        current_bits.append("Commercials added shorts this week.")
    elif c_sign > 0:
        current_bits.append("Commercials added longs this week.")
    else:
        current_bits.append("Commercial weekly flow was flat this week.")

    if nc_sign > 0:
        current_bits.append("Non-Commercials bought.")
    elif nc_sign < 0:
        current_bits.append("Non-Commercials sold.")

    if cross_line:
        current_bits.append(cross_line)

    confirmations: list[str] = []
    if has_c_rotation:
        confirmations.append("Next week's report confirms the Commercial rotation (not a one-week spike).")
    elif c_sign < 0:
        confirmations.append("Commercial selling stops increasing.")
        confirmations.append("First positive Commercial net change.")
    elif c_sign > 0:
        confirmations.append("Commercial buying stops increasing.")
        confirmations.append("First negative Commercial net change.")
    else:
        confirmations.append("A clear directional Commercial net change resumes.")

    if nc_sign > 0:
        confirmations.append("Non-commercial buying weakens.")
    elif nc_sign < 0:
        confirmations.append("Non-commercial selling weakens.")

    if c_prog in ("Approaching Extreme", "Extreme", "Setup Developing") and not has_c_rotation:
        confirmations.append("First Commercial rotation marker.")

    c_pct = _finite((c or {}).get("percentile"))
    if c_pct is not None and 10 < c_pct <= 25 and c_sign < 0:
        confirmations.append("Commercial percentile moves below the 10th.")
    if c_pct is not None and 75 <= c_pct < 90 and c_sign > 0:
        confirmations.append("Commercial percentile moves above the 90th.")

    # Dedup
    seen: set[str] = set()
    conf_out: list[str] = []
    for line in confirmations:
        if line not in seen:
            seen.add(line)
            conf_out.append(line)

    lead = current_bits[0] if current_bits else "Positioning is mixed this week."
    if c_sign < 0:
        bridge = (
            "The next important development would be evidence that this selling is "
            "beginning to slow or reverse."
        )
    elif c_sign > 0:
        bridge = (
            "The next important development would be evidence that this buying is "
            "beginning to slow or reverse."
        )
    else:
        bridge = "The next important development would be a clear directional Commercial print."

    return {
        "current_state": " ".join(current_bits),
        "bridge": bridge,
        "confirmations_needed": conf_out[:5],
        "lead": lead,
    }


def _stars_commercial(c: dict[str, Any] | None, *, c_prog: str) -> tuple[int, str]:
    pct = _finite((c or {}).get("percentile"))
    if pct is None:
        return 1, "Commercial percentile unavailable"
    if c_prog == "Confirmed Rotation":
        return 5, f"Confirmed rotation context at {pct:.0f}th percentile"
    if c_prog == "Early Rotation":
        return 4, f"Early rotation with Commercials at {pct:.0f}th percentile"
    if c_prog == "Setup Developing":
        return 4, f"Extreme + active flow at {pct:.0f}th percentile"
    if c_prog == "Extreme":
        return 5, f"Extreme Commercial percentile ({pct:.0f}th)"
    if c_prog == "Approaching Extreme":
        return 3, f"Approaching extreme ({pct:.0f}th) — not yet deepest zone"
    return 2, f"Mid-range Commercial percentile ({pct:.0f}th)"


def _stars_opposition(cross: dict[str, Any] | None, *, opp_streak: int, opp_kind: str | None) -> tuple[int, str]:
    rel = str((cross or {}).get("relationship") or "")
    if rel == "strong_opposition" and opp_kind == "widening" and opp_streak >= 3:
        return 5, f"Strong opposition widening for {opp_streak} consecutive weeks"
    if rel in ("opposed", "strong_opposition") and opp_kind == "widening":
        return 4, f"Opposition widening ({opp_streak} week streak)" if opp_streak else "Opposition widening"
    if rel in ("opposed", "strong_opposition") and opp_kind == "narrowing":
        return 3, f"Opposition narrowing ({opp_streak} week streak)" if opp_streak else "Opposition narrowing"
    if rel in ("opposed", "strong_opposition"):
        return 3, "Opposition present"
    if rel == "aligned":
        return 1, "Groups aligned — opposition is not supporting a conflict thesis"
    return 2, "Relationship mixed or unavailable"


def _stars_rotation(has_rot: bool, *, c_prog: str) -> tuple[int, str]:
    if c_prog == "Confirmed Rotation":
        return 5, "Confirmed rotation state"
    if c_prog == "Early Rotation" or has_rot:
        return 3, "Early / single-week rotation evidence"
    return 1, "No Commercial rotation marker on this week"


def _stars_historical(analogues: dict[str, Any] | None) -> tuple[int, str]:
    n = int((analogues or {}).get("independent_case_count") or 0)
    q = str((analogues or {}).get("sample_quality") or "")
    if n >= 15 and "INSUFFICIENT" not in q and "LOW" not in q:
        return 5, f"{n} independent historical cases"
    if n >= 8:
        return 4, f"{n} independent historical cases"
    if n >= 5:
        return 3, f"{n} independent historical cases"
    if n >= 1:
        return 2, f"Only {n} independent case(s) — thin sample"
    return 1, "No independent historical analogues available"


def _historical_context(analogues: dict[str, Any] | None) -> dict[str, Any]:
    a = analogues or {}
    n = int(a.get("independent_case_count") or 0)
    q = a.get("sample_quality")
    outcomes = a.get("outcomes_by_horizon") or {}
    o = (
        outcomes.get("12")
        or outcomes.get(12)
        or outcomes.get("8")
        or outcomes.get(8)
        or outcomes.get("4")
        or outcomes.get(4)
        or {}
    )
    higher = o.get("higher_count")
    lower = o.get("lower_count")
    total = o.get("n")
    horizon = o.get("horizon_weeks") or (
        12 if outcomes.get("12") or outcomes.get(12) else 8 if outcomes.get("8") or outcomes.get(8) else 4
    )

    if n <= 0:
        summary = "No independent historical analogues are available for this configuration."
        outcomes_note = None
    else:
        summary = (
            f"This configuration resembles {n} independent historical episode(s)"
            + (f" ({q})" if q else "")
            + "."
        )
        if total and higher is not None and lower is not None:
            outcomes_note = (
                f"Among measurable {horizon}-week outcomes in that sample: "
                f"{higher} printed higher, {lower} printed lower "
                f"(n={total}). Historical context only — not a forecast."
            )
        else:
            outcomes_note = "Forward outcome stats are insufficient for this sample."

    return {
        "independent_case_count": n,
        "sample_quality": q,
        "summary": summary,
        "outcomes_note": outcomes_note,
        "directional_tendency": a.get("directional_tendency"),
    }


def _summary_narrative(
    what_happened: list[str],
    interpretation: list[str],
    missing: list[str],
    next_week: dict[str, Any],
    hist: dict[str, Any],
) -> str:
    parts: list[str] = []
    if what_happened:
        parts.append(" ".join(what_happened[:3]))
    # Prefer the richer streak/rotation sentence from interpretation
    for line in interpretation:
        if "consecutive weeks" in line or "rotation" in line.lower():
            if line not in parts:
                parts.append(line)
            break
    else:
        if interpretation:
            parts.append(interpretation[0])

    bridge = next_week.get("bridge")
    if bridge:
        parts.append(str(bridge))

    miss = [m for m in missing if "No material evidence" not in m]
    if miss:
        parts.append(
            "Still missing: "
            + "; ".join(miss[:3])
            + ". This remains a developing watchlist situation, not a completed setup."
        )

    if hist.get("summary") and int(hist.get("independent_case_count") or 0) > 0:
        parts.append(str(hist["summary"]))

    return " ".join(p.strip() for p in parts if p).strip()


def build_market_analyst_intelligence(
    instrument_id: str,
    *,
    inspector_block: dict[str, Any] | None,
    research_block: dict[str, Any] | None,
) -> dict[str, Any]:
    if not inspector_block or not inspector_block.get("available", True):
        return {
            "instrument_id": instrument_id,
            "available": False,
            "reason": "weekly_inspector unavailable",
            "title": "Weekly Analysis",
            "disclaimer": DISCLAIMER,
        }
    research = research_block if research_block and research_block.get("available") else {}
    weeks = _weeks(inspector_block)
    cur, prior = (weeks[-1], weeks[-2] if len(weeks) > 1 else None) if weeks else (None, None)
    if not cur:
        return {
            "instrument_id": instrument_id,
            "available": False,
            "reason": "no weekly inspector weeks",
            "title": "Weekly Analysis",
            "disclaimer": DISCLAIMER,
        }

    source_week = str(cur.get("date") or research.get("source_week") or "")[:10]
    c = cur.get("commercial") or {}
    nc = cur.get("noncommercial") or {}
    nr = cur.get("nonreportable") or {}
    cross = cur.get("cross") or {}
    prior_c = (prior or {}).get("commercial") if prior else None
    prior_nc = (prior or {}).get("noncommercial") if prior else None
    prior_nr = (prior or {}).get("nonreportable") if prior else None

    markers = _active_markers(research, source_week)
    has_c_rot = _has_rotation(markers, "commercial")
    has_nc_rot = _has_rotation(markers, "noncommercial")
    has_nr_rot = _has_rotation(markers, "nonreportable")

    c_prog = progression_state(c, has_rotation=has_c_rot)
    nc_prog = progression_state(nc, has_rotation=has_nc_rot)
    nr_prog = progression_state(nr, has_rotation=has_nr_rot)

    c_streak, c_sign_streak = _change_streak(weeks, "commercial")
    nc_streak, nc_sign_streak = _change_streak(weeks, "noncommercial")
    nr_streak, nr_sign_streak = _change_streak(weeks, "nonreportable")
    c_sign = _sign(_finite(c.get("weekly_change"))) or c_sign_streak
    nc_sign = _sign(_finite(nc.get("weekly_change"))) or nc_sign_streak
    nr_sign = _sign(_finite(nr.get("weekly_change"))) or nr_sign_streak

    what_happened = [
        x
        for x in (
            _group_change_sentence("Commercials", c, prior_c, streak=c_streak, streak_sign=c_sign_streak),
            _group_change_sentence(
                "Non-Commercials", nc, prior_nc, streak=nc_streak, streak_sign=nc_sign_streak
            ),
            _group_change_sentence(
                "Non-Reportables", nr, prior_nr, streak=nr_streak, streak_sign=nr_sign_streak
            ),
        )
        if x
    ]
    cross_line = _cross_sentence(weeks, cross)
    if cross_line:
        what_happened.append(cross_line)
    if not what_happened:
        what_happened.append("No material week-to-week positioning change cleared the narrative thresholds.")

    interpretation = _interpretation(
        c,
        nc,
        c_prog=c_prog,
        nc_prog=nc_prog,
        c_streak=c_streak,
        c_sign=c_sign,
        has_c_rotation=has_c_rot,
        cross_line=None,  # already in what_happened; avoid duplicate in summary path
    )
    # Keep cross in interpretation for the panel section
    if cross_line and cross_line not in interpretation:
        interpretation.append(cross_line)

    missing = _missing_evidence(
        c, nc, markers, c_prog=c_prog, c_sign=c_sign, nc_sign=nc_sign
    )
    next_week = _next_week(
        c,
        nc,
        c_prog=c_prog,
        c_sign=c_sign,
        c_streak=c_streak,
        nc_sign=nc_sign,
        has_c_rotation=has_c_rot,
        cross_line=cross_line,
    )

    analogues = research.get("current_analogues") or {}
    if not analogues.get("independent_case_count"):
        interp = research.get("current_interpretation") or {}
        nested = interp.get("analogues") or {}
        if nested:
            analogues = {
                "independent_case_count": nested.get("independent_cases")
                or nested.get("independent_case_count")
                or 0,
                "sample_quality": nested.get("sample_quality"),
                "directional_tendency": nested.get("directional_tendency")
                or interp.get("interpretation"),
                "outcomes_by_horizon": nested.get("outcomes_by_horizon")
                or (research.get("current_analogues") or {}).get("outcomes_by_horizon")
                or {},
            }

    hist = _historical_context(analogues)
    present = _present_checklist(
        c, nc, cross, markers, c_prog=c_prog, c_sign=c_sign, nc_sign=nc_sign
    )
    missing_checklist = [m for m in missing if "No material evidence" not in m]

    opp_streak, opp_kind = _opposition_streak(weeks)
    c_stars, c_basis = _stars_commercial(c, c_prog=c_prog)
    o_stars, o_basis = _stars_opposition(cross, opp_streak=opp_streak, opp_kind=opp_kind)
    r_stars, r_basis = _stars_rotation(has_c_rot, c_prog=c_prog)
    h_stars, h_basis = _stars_historical(analogues)

    narrative = _summary_narrative(what_happened, interpretation, missing, next_week, hist)

    # Backward-compatible aliases for older consumers
    what_to_watch = list(next_week.get("confirmations_needed") or [])

    return {
        "instrument_id": instrument_id,
        "available": True,
        "title": "Weekly Analysis",
        "source_week": source_week,
        "summary": narrative,
        "narrative": narrative,
        "what_happened": what_happened,
        "what_changed": what_happened,  # alias
        "interpretation": interpretation,
        "what_it_means": interpretation,  # alias
        "missing_evidence": missing,
        "what_is_missing": missing,  # alias
        "next_week": next_week,
        "what_to_watch": what_to_watch,  # alias → confirmations
        "cross_group": cross_line,
        "progression": {
            "commercial": {
                "state": c_prog,
                "percentile": c.get("percentile"),
                "state_label": c.get("state_label"),
                "temperature": c.get("temperature"),
                "flow_sign": c_sign,
                "streak_weeks": c_streak,
            },
            "noncommercial": {
                "state": nc_prog,
                "percentile": nc.get("percentile"),
                "state_label": nc.get("state_label"),
                "temperature": nc.get("temperature"),
                "flow_sign": nc_sign,
                "streak_weeks": nc_streak,
            },
            "nonreportable": {
                "state": nr_prog,
                "percentile": nr.get("percentile"),
                "state_label": nr.get("state_label"),
                "temperature": nr.get("temperature"),
                "flow_sign": nr_sign,
                "streak_weeks": nr_streak,
            },
        },
        "checklist": {
            "present": present,
            "missing": missing_checklist,
            "status_line": (
                "Watchlist candidate — not a completed setup"
                if missing_checklist
                else "Evidence gaps not flagged from COT print alone"
            ),
        },
        "historical_context": hist,
        "confidence": [
            {
                "id": "commercial_positioning",
                "label": "Commercial positioning",
                "stars": c_stars,
                "max": 5,
                "basis": c_basis,
            },
            {
                "id": "cross_group_opposition",
                "label": "Cross-group opposition",
                "stars": o_stars,
                "max": 5,
                "basis": o_basis,
            },
            {
                "id": "rotation_evidence",
                "label": "Rotation evidence",
                "stars": r_stars,
                "max": 5,
                "basis": r_basis,
            },
            {
                "id": "historical_similarity",
                "label": "Historical similarity",
                "stars": h_stars,
                "max": 5,
                "basis": h_basis,
            },
        ],
        "active_markers": [
            {
                "event_type": m.get("event_type"),
                "group": m.get("group"),
                "label": m.get("label"),
            }
            for m in markers
        ],
        "disclaimer": DISCLAIMER,
        "sources": ["weekly_inspector", "positioning_research"],
    }


def build_analyst_intelligence(
    *,
    weekly_inspector: dict[str, Any],
    positioning_research: dict[str, Any],
) -> dict[str, Any]:
    wi_markets = weekly_inspector.get("markets") or {}
    research_markets = positioning_research.get("markets") or {}
    markets: dict[str, Any] = {}
    for mid, block in wi_markets.items():
        markets[mid] = build_market_analyst_intelligence(
            mid,
            inspector_block=block,
            research_block=research_markets.get(mid),
        )
    available = sum(1 for m in markets.values() if m.get("available"))
    return {
        "version": VERSION,
        "engine": ENGINE,
        "title": "Weekly Analysis",
        "markets": markets,
        "summary": {
            "markets_total": len(markets),
            "markets_available": available,
        },
        "disclaimer": DISCLAIMER,
    }
