"""Commercial-led COT Attention Engine V1.

Primary attention trigger = Commercial positioning events, normalized to each
instrument's own history (and OI where helpful). Non-Commercial and Non-Reportable
groups supply secondary context (flip / alignment / divergence).

This module does NOT emit a black-box 0–100 score. Ranking is an ordered evidence
ladder with explicit event labels and explainable rank reasons.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from hptl.cot.positioning_percentiles import empirical_percentile_rank

# Minimum history for percentile-based events (no look-ahead; expanding to as-of).
MIN_HISTORY_WEEKS = 52

# Statistically derived percentile thresholds (distribution tails).
SURGE_ABS_CHANGE_PCTILE = 90.0  # |1W Δ| in top decile of that market's history
EXTREME_NET_HIGH_PCTILE = 90.0
EXTREME_NET_LOW_PCTILE = 10.0
MATERIAL_BIAS_PCTILE_DIST = 10.0  # |net_pct - 50| for "established" bias
DIVERGENCE_MATERIAL_PCTILE_DIST = 15.0
FLOW_MATERIAL_ABS_PCTILE = 60.0

EVENT_COMMERCIAL_SURGE = "COMMERCIAL SURGE"
EVENT_COMMERCIAL_EXTREME = "COMMERCIAL EXTREME"
EVENT_COMMERCIAL_STRENGTHENING = "COMMERCIAL STRENGTHENING"
EVENT_COMMERCIAL_WEAKENING = "COMMERCIAL WEAKENING"
EVENT_NR_DIVERGENCE = "COMMERCIAL / NON-REPORTABLE DIVERGENCE"
EVENT_NC_FLIP = "NON-COMMERCIAL FLIP"
EVENT_ALIGN_DEVELOPING = "POSITIONING ALIGNMENT DEVELOPING"
EVENT_ALIGN_STRENGTHENING = "POSITIONING ALIGNMENT STRENGTHENING"

# Transparent evidence weights (not an opaque score — each point maps to a label).
EVIDENCE_WEIGHTS = {
    EVENT_COMMERCIAL_SURGE: 5,
    EVENT_COMMERCIAL_EXTREME: 4,
    EVENT_NR_DIVERGENCE: 3,
    EVENT_NC_FLIP: 3,
    EVENT_ALIGN_STRENGTHENING: 2,
    EVENT_ALIGN_DEVELOPING: 2,
    EVENT_COMMERCIAL_STRENGTHENING: 1,
    EVENT_COMMERCIAL_WEAKENING: 1,
}

TIER_HIGH = "high_attention"
TIER_DEVELOPING = "developing"
TIER_WATCHLIST = "watchlist"
TIER_LOW = "low_priority"

TIER_LABELS = {
    TIER_HIGH: "HIGH ATTENTION",
    TIER_DEVELOPING: "DEVELOPING",
    TIER_WATCHLIST: "WATCHLIST",
    TIER_LOW: "LOW PRIORITY",
}


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _sign(v: float | None, *, eps: float = 0.0) -> str:
    if v is None:
        return "neutral"
    if v > eps:
        return "bullish"
    if v < -eps:
        return "bearish"
    return "neutral"


def _pct(window: Sequence[float], value: float | None) -> float | None:
    if value is None or len(window) < 2:
        return None
    p = empirical_percentile_rank(window, value)
    return None if not math.isfinite(p) else round(float(p), 2)


def _change(series: list[float | None], idx: int, lag: int) -> float | None:
    if idx < lag:
        return None
    a = series[idx]
    b = series[idx - lag]
    if a is None or b is None:
        return None
    return a - b


def _extract_group_series(weeks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows = sorted(
        [w for w in weeks if w.get("report_date")],
        key=lambda w: str(w.get("report_date")),
    )
    out: list[dict[str, Any]] = []
    for w in rows:
        out.append(
            {
                "report_date": str(w.get("report_date"))[:10],
                "long": _finite(w.get("long")),
                "short": _finite(w.get("short")),
                "net": _finite(w.get("net")),
                "open_interest": _finite(w.get("open_interest")),
                "net_week_change": _finite(w.get("net_week_change")),
            }
        )
    for i in range(1, len(out)):
        if out[i]["net_week_change"] is None:
            out[i]["net_week_change"] = _change([r["net"] for r in out], i, 1)
    return out


@dataclass
class GroupSnapshot:
    long: float | None = None
    short: float | None = None
    net: float | None = None
    open_interest: float | None = None
    change_1w: float | None = None
    change_4w: float | None = None
    change_12w: float | None = None
    net_percentile: float | None = None
    change_1w_percentile: float | None = None
    change_1w_abs_percentile: float | None = None
    direction: str = "neutral"
    flow_1w: str = "neutral"
    oi_normalized_1w: float | None = None


@dataclass
class InstrumentAttention:
    instrument: str
    source_week: str
    commercial: GroupSnapshot = field(default_factory=GroupSnapshot)
    noncommercial: GroupSnapshot = field(default_factory=GroupSnapshot)
    nonreportable: GroupSnapshot = field(default_factory=GroupSnapshot)
    commercial_regime: str = "neutral"
    events: list[str] = field(default_factory=list)
    evidence_points: int = 0
    attention_tier: str = TIER_LOW
    attention_label: str = TIER_LABELS[TIER_LOW]
    rank_reasons: list[str] = field(default_factory=list)
    narratives: dict[str, str] = field(default_factory=dict)
    history_weeks_used: int = 0
    eligible: bool = False
    skip_reason: str | None = None

    def to_dict(self) -> dict[str, Any]:
        def snap(s: GroupSnapshot) -> dict[str, Any]:
            return {
                "long": s.long,
                "short": s.short,
                "net": s.net,
                "open_interest": s.open_interest,
                "change_1w": s.change_1w,
                "change_4w": s.change_4w,
                "change_12w": s.change_12w,
                "net_percentile": s.net_percentile,
                "change_1w_percentile": s.change_1w_percentile,
                "change_1w_abs_percentile": s.change_1w_abs_percentile,
                "direction": s.direction,
                "flow_1w": s.flow_1w,
                "oi_normalized_1w": s.oi_normalized_1w,
            }

        return {
            "instrument": self.instrument,
            "source_week": self.source_week,
            "commercial": snap(self.commercial),
            "noncommercial": snap(self.noncommercial),
            "nonreportable": snap(self.nonreportable),
            "commercial_regime": self.commercial_regime,
            "events": list(self.events),
            "evidence_points": self.evidence_points,
            "attention_tier": self.attention_tier,
            "attention_label": self.attention_label,
            "rank_reasons": list(self.rank_reasons),
            "narratives": dict(self.narratives),
            "history_weeks_used": self.history_weeks_used,
            "eligible": self.eligible,
            "skip_reason": self.skip_reason,
            "event_definitions_ref": "hptl.cot.commercial_attention_engine",
        }


def _build_snapshot(series: list[dict[str, Any]], idx: int) -> GroupSnapshot:
    nets = [r["net"] for r in series]
    changes = [r["net_week_change"] for r in series]
    cur = series[idx]
    snap = GroupSnapshot(
        long=cur["long"],
        short=cur["short"],
        net=cur["net"],
        open_interest=cur["open_interest"],
        change_1w=changes[idx] if idx < len(changes) else None,
        change_4w=_change(nets, idx, 4),
        change_12w=_change(nets, idx, 12),
    )
    hist_nets = [n for n in nets[: idx + 1] if n is not None]
    hist_chg = [c for c in changes[: idx + 1] if c is not None]
    hist_abs = [abs(c) for c in hist_chg]
    snap.net_percentile = _pct(hist_nets, snap.net)
    snap.change_1w_percentile = _pct(hist_chg, snap.change_1w)
    snap.change_1w_abs_percentile = _pct(
        hist_abs, abs(snap.change_1w) if snap.change_1w is not None else None
    )
    snap.direction = _sign(snap.net)
    snap.flow_1w = _sign(snap.change_1w)
    if snap.change_1w is not None and snap.open_interest and snap.open_interest > 0:
        snap.oi_normalized_1w = round(abs(snap.change_1w) / snap.open_interest, 6)
    return snap


def _prev_1w_change(series: list[dict[str, Any]], idx: int) -> float | None:
    if idx < 1:
        return None
    return series[idx - 1].get("net_week_change")


def _detect_commercial_events(
    series: list[dict[str, Any]],
    idx: int,
    snap: GroupSnapshot,
) -> tuple[list[str], str, list[str], dict[str, str]]:
    events: list[str] = []
    reasons: list[str] = []
    narr: dict[str, str] = {}

    if (
        snap.change_1w is not None
        and snap.change_1w_abs_percentile is not None
        and snap.change_1w_abs_percentile >= SURGE_ABS_CHANGE_PCTILE
        and abs(snap.change_1w) > 0
    ):
        events.append(EVENT_COMMERCIAL_SURGE)
        reasons.append(
            f"Commercial |1W Δ| at {snap.change_1w_abs_percentile:.1f}th percentile "
            f"(threshold ≥{SURGE_ABS_CHANGE_PCTILE:g})"
        )
        narr["commercials"] = (
            f"{'Bullish' if snap.change_1w > 0 else 'Bearish'} accumulation surge "
            f"(1W Δ {snap.change_1w:+.0f}, {snap.change_1w_abs_percentile:.0f}th pctile of |Δ|)"
        )

    if snap.net_percentile is not None and (
        snap.net_percentile >= EXTREME_NET_HIGH_PCTILE or snap.net_percentile <= EXTREME_NET_LOW_PCTILE
    ):
        events.append(EVENT_COMMERCIAL_EXTREME)
        reasons.append(
            f"Commercial net at {snap.net_percentile:.1f}th percentile "
            f"(tails ≤{EXTREME_NET_LOW_PCTILE:g} / ≥{EXTREME_NET_HIGH_PCTILE:g})"
        )
        side = "historically long" if snap.net_percentile >= EXTREME_NET_HIGH_PCTILE else "historically short"
        narr.setdefault("commercials", f"Commercial net extreme ({side}, {snap.net_percentile:.0f}th pctile)")

    prev_chg = _prev_1w_change(series, idx)
    same_dir = (
        snap.direction != "neutral"
        and snap.flow_1w != "neutral"
        and snap.direction == snap.flow_1w
    )
    opposite_flow = (
        snap.direction != "neutral"
        and snap.flow_1w != "neutral"
        and snap.direction != snap.flow_1w
    )
    accelerating = (
        snap.change_1w is not None
        and prev_chg is not None
        and abs(snap.change_1w) > abs(prev_chg)
        and same_dir
    )
    decelerating = (
        snap.change_1w is not None
        and prev_chg is not None
        and abs(snap.change_1w) < abs(prev_chg)
        and same_dir
    )
    four_aligned = (
        snap.change_4w is not None
        and snap.direction != "neutral"
        and _sign(snap.change_4w) == snap.direction
    )

    regime = "neutral"
    if same_dir and (accelerating or (four_aligned and (snap.change_1w_abs_percentile or 0) >= 60)):
        events.append(EVENT_COMMERCIAL_STRENGTHENING)
        regime = f"{snap.direction} / strengthening"
        reasons.append("Commercial bias continues with accelerating or confirmed multi-week flow")
        narr.setdefault("commercial_regime", f"{snap.direction.capitalize()} bias strengthening")
    elif opposite_flow or decelerating:
        events.append(EVENT_COMMERCIAL_WEAKENING)
        regime = f"{snap.direction} / weakening" if snap.direction != "neutral" else "weakening"
        reasons.append("Commercial bias losing momentum or facing opposite weekly flow")
        narr.setdefault(
            "commercial_regime",
            f"{snap.direction.capitalize()} bias weakening" if snap.direction != "neutral" else "Bias weakening",
        )
    elif snap.direction != "neutral":
        regime = snap.direction
        narr.setdefault("commercial_regime", f"{snap.direction.capitalize()} bias")

    if "commercials" not in narr and snap.net is not None:
        if snap.change_1w is not None:
            narr["commercials"] = f"Net {snap.net:+.0f} ({snap.direction}); 1W Δ {snap.change_1w:+.0f}"
        else:
            narr["commercials"] = f"Net {snap.net:+.0f} ({snap.direction})"

    return events, regime, reasons, narr


def _detect_nr_divergence(
    comm: GroupSnapshot,
    nr: GroupSnapshot,
) -> tuple[list[str], list[str], dict[str, str]]:
    events: list[str] = []
    reasons: list[str] = []
    narr: dict[str, str] = {}

    pos_disagree = (
        comm.direction != "neutral"
        and nr.direction != "neutral"
        and comm.direction != nr.direction
    )
    flow_disagree = (
        comm.flow_1w != "neutral"
        and nr.flow_1w != "neutral"
        and comm.flow_1w != nr.flow_1w
    )
    comm_extreme_enough = (
        comm.net_percentile is not None
        and abs(comm.net_percentile - 50.0) >= DIVERGENCE_MATERIAL_PCTILE_DIST
    )
    nr_extreme_enough = (
        nr.net_percentile is not None
        and abs(nr.net_percentile - 50.0) >= DIVERGENCE_MATERIAL_PCTILE_DIST
    )
    flow_material = (comm.change_1w_abs_percentile or 0) >= FLOW_MATERIAL_ABS_PCTILE or (
        nr.change_1w_abs_percentile or 0
    ) >= FLOW_MATERIAL_ABS_PCTILE

    if (pos_disagree and (comm_extreme_enough or nr_extreme_enough)) or (flow_disagree and flow_material):
        events.append(EVENT_NR_DIVERGENCE)
        if pos_disagree:
            reasons.append(
                f"NR net {nr.direction} vs Commercial {comm.direction} "
                f"(comm net pctile {comm.net_percentile}, NR net pctile {nr.net_percentile})"
            )
        if flow_disagree:
            reasons.append(f"NR 1W flow {nr.flow_1w} vs Commercial 1W flow {comm.flow_1w}")
        narr["nonreportables"] = (
            f"Remain materially {nr.direction}"
            + (" and opposing Commercial weekly flow" if flow_disagree else " versus Commercial positioning")
        )
    elif nr.direction != "neutral":
        narr["nonreportables"] = f"Net {nr.direction} (no material Commercial divergence flag)"

    return events, reasons, narr


def _nc_flow_signs(series: list[dict[str, Any]], idx: int, lookback: int = 4) -> list[str]:
    signs: list[str] = []
    start = max(0, idx - lookback + 1)
    for i in range(start, idx + 1):
        signs.append(_sign(series[i].get("net_week_change")))
    return signs


def _detect_nc_alignment(
    comm: GroupSnapshot,
    nc_series: list[dict[str, Any]],
    idx: int,
    nc_snap: GroupSnapshot,
) -> tuple[list[str], list[str], dict[str, str]]:
    events: list[str] = []
    reasons: list[str] = []
    narr: dict[str, str] = {}

    bias = comm.direction
    if bias == "neutral":
        if nc_snap.flow_1w != "neutral":
            narr["noncommercials"] = f"1W flow {nc_snap.flow_1w} (Commercial bias neutral — no flip test)"
        return events, reasons, narr

    established = (
        comm.net_percentile is not None
        and abs(comm.net_percentile - 50.0) >= MATERIAL_BIAS_PCTILE_DIST
    )
    flows = _nc_flow_signs(nc_series, idx, lookback=4)
    if len(flows) < 2:
        return events, reasons, narr

    cur = flows[-1]
    prior = flows[:-1]
    # Require sustained opposition (majority of prior lookback), not a single noisy week.
    opposed_prior = [f for f in prior if f != "neutral" and f != bias]
    aligned_prior = [f for f in prior if f == bias]
    prior_opposed = len(opposed_prior) >= 2
    # Current NC flow must be material vs its own |Δ| history to count as a flip.
    nc_flow_material = (nc_snap.change_1w_abs_percentile or 0) >= 55.0
    prior_aligned_streak = 0
    for f in reversed(flows):
        if f == bias:
            prior_aligned_streak += 1
        else:
            break

    flipped = established and prior_opposed and cur == bias and nc_flow_material

    if flipped:
        events.append(EVENT_NC_FLIP)
        reasons.append(
            f"NC 1W flow flipped to {cur} toward Commercial {bias} after "
            f"{len(opposed_prior)} opposing weeks in {prior} "
            f"(NC |1W| pctile {nc_snap.change_1w_abs_percentile})"
        )
        narr["noncommercials"] = (
            f"First material {cur} weekly shift after previous opposing flow vs Commercial {bias}"
        )

    if flipped or (cur == bias and prior_opposed and established and nc_flow_material):
        events.append(EVENT_ALIGN_DEVELOPING)
        reasons.append("NC flow now aligning with Commercial bias after sustained prior opposition")
        narr.setdefault("alignment", "Positioning alignment developing (NC toward Commercial)")

    # Strengthening: ≥3 consecutive aligned weeks (incl. current), not just 2 (noise).
    if prior_aligned_streak >= 3 and established and cur == bias and len(aligned_prior) >= 1:
        events.append(EVENT_ALIGN_STRENGTHENING)
        reasons.append(
            f"NC flow aligned with Commercial {bias} for {prior_aligned_streak} consecutive weeks"
        )
        narr["alignment"] = (
            f"Positioning alignment strengthening ({prior_aligned_streak}w NC with Commercial)"
        )
        if "noncommercials" not in narr:
            narr["noncommercials"] = f"Aligned {bias} flow continuing ({prior_aligned_streak}w)"

    if "noncommercials" not in narr:
        narr["noncommercials"] = (
            f"1W flow {nc_snap.flow_1w}; net {nc_snap.direction}"
            if nc_snap.flow_1w != "neutral" or nc_snap.direction != "neutral"
            else "No notable NC flow this week"
        )

    return events, reasons, narr


def _tier_from_events(events: list[str], evidence: int) -> str:
    if (
        evidence >= 5
        or (EVENT_COMMERCIAL_SURGE in events and EVENT_COMMERCIAL_EXTREME in events)
        or (EVENT_COMMERCIAL_SURGE in events and EVENT_NR_DIVERGENCE in events)
        or (EVENT_COMMERCIAL_SURGE in events and EVENT_NC_FLIP in events)
    ):
        return TIER_HIGH
    if evidence >= 3 or EVENT_NC_FLIP in events or EVENT_COMMERCIAL_SURGE in events:
        return TIER_DEVELOPING
    if evidence >= 1:
        return TIER_WATCHLIST
    return TIER_LOW


def analyze_instrument(
    instrument_id: str,
    inst_doc: dict[str, Any],
    *,
    as_of: str | None = None,
) -> InstrumentAttention:
    groups = inst_doc.get("groups") or {}
    comm_s = _extract_group_series((groups.get("commercials") or {}).get("weeks") or [])
    nc_s = _extract_group_series((groups.get("noncommercials") or {}).get("weeks") or [])
    nr_s = _extract_group_series((groups.get("nonreportables") or {}).get("weeks") or [])

    if not comm_s:
        return InstrumentAttention(
            instrument=instrument_id,
            source_week=as_of or "",
            eligible=False,
            skip_reason="no_commercial_weeks",
        )

    dates = [r["report_date"] for r in comm_s]
    if as_of:
        as_of = str(as_of)[:10]
        if as_of not in dates:
            return InstrumentAttention(
                instrument=instrument_id,
                source_week=as_of,
                eligible=False,
                skip_reason="as_of_week_missing",
            )
        idx = dates.index(as_of)
    else:
        idx = len(comm_s) - 1
        as_of = dates[idx]

    att = InstrumentAttention(instrument=instrument_id, source_week=as_of)
    att.history_weeks_used = idx + 1

    if idx + 1 < MIN_HISTORY_WEEKS:
        att.eligible = False
        att.skip_reason = f"insufficient_history<{MIN_HISTORY_WEEKS}"
        att.commercial = _build_snapshot(comm_s, idx)
        return att

    att.commercial = _build_snapshot(comm_s, idx)

    nc_idx = next((i for i, r in enumerate(nc_s) if r["report_date"] == as_of), None)
    nr_idx = next((i for i, r in enumerate(nr_s) if r["report_date"] == as_of), None)
    if nc_idx is not None:
        att.noncommercial = _build_snapshot(nc_s, nc_idx)
    if nr_idx is not None:
        att.nonreportable = _build_snapshot(nr_s, nr_idx)

    events, regime, reasons, narr = _detect_commercial_events(comm_s, idx, att.commercial)
    att.commercial_regime = regime
    att.narratives.update(narr)
    att.rank_reasons.extend(reasons)

    if nr_idx is not None:
        e2, r2, n2 = _detect_nr_divergence(att.commercial, att.nonreportable)
        events.extend(e2)
        att.rank_reasons.extend(r2)
        att.narratives.update(n2)

    if nc_idx is not None:
        e3, r3, n3 = _detect_nc_alignment(att.commercial, nc_s, nc_idx, att.noncommercial)
        events.extend(e3)
        att.rank_reasons.extend(r3)
        att.narratives.update(n3)

    seen: set[str] = set()
    ordered: list[str] = []
    for e in events:
        if e not in seen:
            seen.add(e)
            ordered.append(e)
    att.events = ordered
    att.evidence_points = sum(EVIDENCE_WEIGHTS.get(e, 0) for e in ordered)
    att.attention_tier = _tier_from_events(ordered, att.evidence_points)
    att.attention_label = TIER_LABELS[att.attention_tier]
    att.eligible = True

    if att.evidence_points > 0:
        att.rank_reasons.append(
            "Evidence points = "
            + " + ".join(f"{EVIDENCE_WEIGHTS[e]} ({e})" for e in ordered if e in EVIDENCE_WEIGHTS)
            + f" → {att.evidence_points}"
        )
    return att


def _rank_key(att: InstrumentAttention) -> tuple:
    tier_rank = {
        TIER_HIGH: 0,
        TIER_DEVELOPING: 1,
        TIER_WATCHLIST: 2,
        TIER_LOW: 3,
    }.get(att.attention_tier, 9)
    abs_chg_pct = att.commercial.change_1w_abs_percentile or -1.0
    net_extremity = (
        abs((att.commercial.net_percentile or 50.0) - 50.0)
        if att.commercial.net_percentile is not None
        else -1.0
    )
    oi_norm = att.commercial.oi_normalized_1w or -1.0
    return (
        tier_rank,
        -att.evidence_points,
        -abs_chg_pct,
        -net_extremity,
        -oi_norm,
        att.instrument,
    )


def build_commercial_attention(
    legacy_doc: dict[str, Any],
    *,
    as_of: str | None = None,
    instrument_ids: list[str] | None = None,
) -> dict[str, Any]:
    """Build the weekly Commercial attention board from ``legacy_cot_latest``."""
    instruments = legacy_doc.get("instruments") or {}
    ids = instrument_ids or sorted(instruments.keys())

    if not as_of:
        latest_dates: list[str] = []
        for iid in ids:
            weeks = (
                ((instruments.get(iid) or {}).get("groups") or {}).get("commercials", {}).get("weeks")
                or []
            )
            if weeks:
                d = max(str(w.get("report_date") or "")[:10] for w in weeks if w.get("report_date"))
                if d:
                    latest_dates.append(d)
        as_of = max(latest_dates) if latest_dates else None

    rows: list[InstrumentAttention] = []
    for iid in ids:
        inst = instruments.get(iid)
        if not inst:
            continue
        rows.append(analyze_instrument(iid, inst, as_of=as_of))

    ranked = sorted([r for r in rows if r.eligible and r.evidence_points > 0], key=_rank_key)
    for i, r in enumerate(ranked, start=1):
        r.rank_reasons.insert(0, f"Weekly attention rank #{i} by evidence ladder (not raw contracts)")

    board = [r.to_dict() for r in ranked]
    all_rows = [r.to_dict() for r in sorted(rows, key=lambda x: x.instrument)]

    return {
        "version": "commercial_attention_v1",
        "engine": "commercial_led_cot_attention",
        "source_week": as_of,
        "generated_note": (
            "Attention/radar only — not an execution signal. "
            "Primary trigger = Commercial events; NC/NR are context."
        ),
        "thresholds": {
            "min_history_weeks": MIN_HISTORY_WEEKS,
            "surge_abs_change_percentile": SURGE_ABS_CHANGE_PCTILE,
            "extreme_net_high_percentile": EXTREME_NET_HIGH_PCTILE,
            "extreme_net_low_percentile": EXTREME_NET_LOW_PCTILE,
            "material_bias_percentile_distance": MATERIAL_BIAS_PCTILE_DIST,
            "divergence_material_percentile_distance": DIVERGENCE_MATERIAL_PCTILE_DIST,
            "flow_material_abs_percentile": FLOW_MATERIAL_ABS_PCTILE,
            "evidence_weights": EVIDENCE_WEIGHTS,
        },
        "event_definitions": {
            EVENT_COMMERCIAL_SURGE: (
                f"|Commercial 1W net change| ≥ {SURGE_ABS_CHANGE_PCTILE:g}th percentile "
                "of that instrument's own historical |weekly net changes| (expanding, as-of)."
            ),
            EVENT_COMMERCIAL_EXTREME: (
                f"Commercial net ≤ {EXTREME_NET_LOW_PCTILE:g}th or ≥ {EXTREME_NET_HIGH_PCTILE:g}th "
                "percentile of own expanding net history."
            ),
            EVENT_COMMERCIAL_STRENGTHENING: (
                "Commercial net bias and 1W flow share direction, with acceleration vs prior 1W "
                "or confirmed 4W alignment with material |1W| percentile."
            ),
            EVENT_COMMERCIAL_WEAKENING: (
                "Commercial 1W flow opposes net bias, or same-direction flow decelerates vs prior week."
            ),
            EVENT_NR_DIVERGENCE: (
                "Non-Reportable net and/or 1W flow materially disagree with Commercial bias/flow "
                "(percentile-distance / flow-materiality gates)."
            ),
            EVENT_NC_FLIP: (
                "With established Commercial bias, NC 1W flow turns toward that bias after "
                "≥2 opposing weeks in the prior lookback, and current NC |1W| is ≥55th "
                "percentile of its own history (noise filter)."
            ),
            EVENT_ALIGN_DEVELOPING: (
                "NC flow aligning with Commercial after sustained prior opposition (includes flip week)."
            ),
            EVENT_ALIGN_STRENGTHENING: (
                "NC weekly flow aligned with Commercial bias for ≥3 consecutive weeks."
            ),
        },
        "ranking_method": (
            "Sort by attention tier, then evidence_points (sum of transparent event weights), "
            "then Commercial |1W Δ| percentile, then net extremity vs 50th percentile, "
            "then OI-normalized |1W Δ|. Raw contract size is never the primary sort key."
        ),
        "attention_board": board,
        "instruments": all_rows,
        "summary": {
            "instruments_scanned": len(rows),
            "eligible": sum(1 for r in rows if r.eligible),
            "with_events": len(ranked),
            "high_attention": sum(1 for r in ranked if r.attention_tier == TIER_HIGH),
            "developing": sum(1 for r in ranked if r.attention_tier == TIER_DEVELOPING),
            "watchlist": sum(1 for r in ranked if r.attention_tier == TIER_WATCHLIST),
        },
    }
