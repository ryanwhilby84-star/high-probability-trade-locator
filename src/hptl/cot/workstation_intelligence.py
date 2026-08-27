"""COT Workstation Intelligence V2 — extremes, multi-group events, analogues.

Precomputes explainable research evidence from ``cot_3y_series_latest``:
- per-group historical extreme ENTRY markers (episode-deduped)
- multi-group relationship events
- current-week intelligence narrative
- rule-based historical analogues + forward price outcomes

No opaque ML scoring. No look-ahead in historical event qualification.
Not an execution signal engine.
"""

from __future__ import annotations

import math
from dataclasses import dataclass, field
from typing import Any, Sequence

from hptl.cot.positioning_percentiles import empirical_percentile_rank

MIN_HISTORY = 52
EXTREME_HIGH = 90.0
EXTREME_LOW = 10.0
SEVERE_STEP = 5.0  # material deepening of an extreme (e.g. 92 → 97)
EPISODE_COOLDOWN_WEEKS = 8  # suppress consecutive weeks of same extreme episode
ANALOGUE_COOLDOWN_WEEKS = 12  # independent analogue cases

GROUP_COMMERCIAL = "commercial"
GROUP_NONCOMMERCIAL = "noncommercial"  # institutional_net in cot_3y
GROUP_NONREPORTABLE = "nonreportable"  # retail_net in cot_3y

GROUP_NET_KEY = {
    GROUP_COMMERCIAL: "commercial_net",
    GROUP_NONCOMMERCIAL: "institutional_net",
    GROUP_NONREPORTABLE: "retail_net",
}

GROUP_LABEL = {
    GROUP_COMMERCIAL: "Commercials",
    GROUP_NONCOMMERCIAL: "Non-Commercials",
    GROUP_NONREPORTABLE: "Non-Reportables",
}

FORWARD_HORIZONS = (4, 8, 12, 26)


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _sign(v: float | None) -> str:
    if v is None:
        return "neutral"
    if v > 0:
        return "bullish"
    if v < 0:
        return "bearish"
    return "neutral"


def _pct(window: Sequence[float], value: float | None) -> float | None:
    if value is None or len(window) < 2:
        return None
    p = empirical_percentile_rank(window, value)
    return None if not math.isfinite(p) else round(float(p), 2)


def _change(nets: list[float | None], idx: int, lag: int) -> float | None:
    if idx < lag:
        return None
    a, b = nets[idx], nets[idx - lag]
    if a is None or b is None:
        return None
    return a - b


def _classify_pct(pct: float | None) -> str:
    if pct is None:
        return "N/A"
    if pct <= EXTREME_LOW:
        return "Extreme Bearish"
    if pct < 30:
        return "Bearish"
    if pct <= 70:
        return "Neutral"
    if pct < EXTREME_HIGH:
        return "Bullish"
    return "Extreme Bullish"


def sample_quality(n: int) -> str:
    if n < 5:
        return "INSUFFICIENT SAMPLE"
    if n < 8:
        return "LOW CONFIDENCE"
    if n < 15:
        return "MODERATE SAMPLE"
    return "STRONGER SAMPLE"


@dataclass
class MarkerEvent:
    date: str
    group: str
    kind: str  # enters_bullish_extreme | enters_bearish_extreme | deepens_extreme | multi_group
    label: str
    net: float | None
    percentile: float | None
    change_1w: float | None
    change_4w: float | None
    change_12w: float | None
    classification: str
    meta: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "date": self.date,
            "group": self.group,
            "group_label": GROUP_LABEL.get(self.group, self.group),
            "kind": self.kind,
            "label": self.label,
            "net": self.net,
            "percentile": self.percentile,
            "change_1w": self.change_1w,
            "change_4w": self.change_4w,
            "change_12w": self.change_12w,
            "classification": self.classification,
            "meta": self.meta,
        }


def _series_nets(series: list[dict[str, Any]], key: str) -> list[float | None]:
    return [_finite(r.get(key)) for r in series]


def _detect_group_extremes(
    series: list[dict[str, Any]],
    group: str,
) -> list[MarkerEvent]:
    """Detect extreme ENTRY points with episode de-duplication (no look-ahead)."""
    key = GROUP_NET_KEY[group]
    nets = _series_nets(series, key)
    events: list[MarkerEvent] = []
    in_bull = False
    in_bear = False
    last_bull_pct: float | None = None
    last_bear_pct: float | None = None
    last_bull_idx = -10_000
    last_bear_idx = -10_000

    for i, row in enumerate(series):
        if i + 1 < MIN_HISTORY:
            continue
        hist = [n for n in nets[: i + 1] if n is not None]
        pct = _pct(hist, nets[i])
        if pct is None or nets[i] is None:
            continue
        date = str(row.get("date") or "")[:10]
        c1 = _change(nets, i, 1)
        c4 = _change(nets, i, 4)
        c12 = _change(nets, i, 12)
        cls = _classify_pct(pct)

        bull_now = pct >= EXTREME_HIGH
        bear_now = pct <= EXTREME_LOW

        if bull_now:
            entering = not in_bull and (i - last_bull_idx) >= EPISODE_COOLDOWN_WEEKS
            deepening = (
                in_bull
                and last_bull_pct is not None
                and pct >= last_bull_pct + SEVERE_STEP
                and (i - last_bull_idx) >= 2
            )
            if entering or deepening:
                events.append(
                    MarkerEvent(
                        date=date,
                        group=group,
                        kind="enters_bullish_extreme" if entering else "deepens_extreme",
                        label=(
                            f"{GROUP_LABEL[group].upper()} BULLISH EXTREME"
                            if entering
                            else f"{GROUP_LABEL[group].upper()} DEEPER BULLISH EXTREME"
                        ),
                        net=nets[i],
                        percentile=pct,
                        change_1w=c1,
                        change_4w=c4,
                        change_12w=c12,
                        classification=cls,
                    )
                )
                last_bull_idx = i
                last_bull_pct = pct
            in_bull = True
            last_bull_pct = pct if last_bull_pct is None else max(last_bull_pct, pct)
        else:
            if in_bull and pct < EXTREME_HIGH - 5:
                in_bull = False
                last_bull_pct = None

        if bear_now:
            entering = not in_bear and (i - last_bear_idx) >= EPISODE_COOLDOWN_WEEKS
            deepening = (
                in_bear
                and last_bear_pct is not None
                and pct <= last_bear_pct - SEVERE_STEP
                and (i - last_bear_idx) >= 2
            )
            if entering or deepening:
                events.append(
                    MarkerEvent(
                        date=date,
                        group=group,
                        kind="enters_bearish_extreme" if entering else "deepens_extreme",
                        label=(
                            f"{GROUP_LABEL[group].upper()} BEARISH EXTREME"
                            if entering
                            else f"{GROUP_LABEL[group].upper()} DEEPER BEARISH EXTREME"
                        ),
                        net=nets[i],
                        percentile=pct,
                        change_1w=c1,
                        change_4w=c4,
                        change_12w=c12,
                        classification=cls,
                    )
                )
                last_bear_idx = i
                last_bear_pct = pct
            in_bear = True
            last_bear_pct = pct if last_bear_pct is None else min(last_bear_pct, pct)
        else:
            if in_bear and pct > EXTREME_LOW + 5:
                in_bear = False
                last_bear_pct = None

    return events


def _snapshot_at(series: list[dict[str, Any]], idx: int) -> dict[str, Any]:
    """Feature snapshot for analogue matching (as-of idx, no future)."""
    out: dict[str, Any] = {"date": str(series[idx].get("date") or "")[:10], "index": idx}
    for group, key in GROUP_NET_KEY.items():
        nets = _series_nets(series, key)
        hist = [n for n in nets[: idx + 1] if n is not None]
        net = nets[idx]
        pct = _pct(hist, net)
        c1 = _change(nets, idx, 1)
        c4 = _change(nets, idx, 4)
        c12 = _change(nets, idx, 12)
        out[group] = {
            "net": net,
            "percentile": pct,
            "direction": _sign(net),
            "flow_1w": _sign(c1),
            "change_1w": c1,
            "change_4w": c4,
            "change_12w": c12,
            "extreme": pct is not None and (pct >= EXTREME_HIGH or pct <= EXTREME_LOW),
            "extreme_side": (
                "bullish"
                if pct is not None and pct >= EXTREME_HIGH
                else "bearish"
                if pct is not None and pct <= EXTREME_LOW
                else None
            ),
            "classification": _classify_pct(pct),
        }
    c = out[GROUP_COMMERCIAL]
    nr = out[GROUP_NONREPORTABLE]
    nc = out[GROUP_NONCOMMERCIAL]
    out["nr_divergence"] = (
        c["direction"] != "neutral"
        and nr["direction"] != "neutral"
        and c["direction"] != nr["direction"]
    )
    out["nc_opposed"] = (
        c["direction"] != "neutral"
        and nc["direction"] != "neutral"
        and c["direction"] != nc["direction"]
    )
    out["nc_aligned_flow"] = (
        c["direction"] != "neutral" and nc["flow_1w"] == c["direction"]
    )
    return out


def _detect_multi_group_events(series: list[dict[str, Any]]) -> list[MarkerEvent]:
    events: list[MarkerEvent] = []
    last_by_label: dict[str, int] = {}

    for i in range(MIN_HISTORY - 1, len(series)):
        snap = _snapshot_at(series, i)
        date = snap["date"]
        c, nc, nr = snap[GROUP_COMMERCIAL], snap[GROUP_NONCOMMERCIAL], snap[GROUP_NONREPORTABLE]
        candidates: list[tuple[str, str, dict[str, Any]]] = []

        if snap["nr_divergence"] and (c["extreme"] or nr["extreme"]):
            candidates.append(
                (
                    "COMMERCIAL / NON-REPORTABLE DIVERGENCE",
                    "multi_group",
                    {"commercial": c["direction"], "nonreportable": nr["direction"]},
                )
            )
        if c["extreme"] and snap["nr_divergence"]:
            candidates.append(
                (
                    "COMMERCIAL EXTREME + NON-REPORTABLE OPPOSITION",
                    "multi_group",
                    {"commercial_extreme": c["extreme_side"], "nr": nr["direction"]},
                )
            )
        # NC flip: prior week opposed flow, current aligns
        if i >= 1:
            prev = _snapshot_at(series, i - 1)
            if (
                c["direction"] != "neutral"
                and prev[GROUP_NONCOMMERCIAL]["flow_1w"] not in ("neutral", c["direction"])
                and nc["flow_1w"] == c["direction"]
                and (nc.get("change_1w") is not None)
            ):
                candidates.append(
                    (
                        "NON-COMMERCIAL FLIP TOWARD COMMERCIAL BIAS",
                        "multi_group",
                        {"commercial_bias": c["direction"]},
                    )
                )
        if snap["nc_aligned_flow"] and snap["nc_opposed"] is False and c["direction"] != "neutral":
            # developing if recently opposed
            if i >= 3:
                recent_opp = any(
                    _snapshot_at(series, j)[GROUP_NONCOMMERCIAL]["flow_1w"] not in ("neutral", c["direction"])
                    for j in range(i - 3, i)
                )
                if recent_opp:
                    candidates.append(
                        (
                            "POSITIONING ALIGNMENT DEVELOPING",
                            "multi_group",
                            {"bias": c["direction"]},
                        )
                    )
            streak = 0
            for j in range(i, max(-1, i - 6), -1):
                s = _snapshot_at(series, j)
                if s[GROUP_NONCOMMERCIAL]["flow_1w"] == c["direction"]:
                    streak += 1
                else:
                    break
            if streak >= 3:
                candidates.append(
                    (
                        "POSITIONING ALIGNMENT STRENGTHENING",
                        "multi_group",
                        {"weeks": streak, "bias": c["direction"]},
                    )
                )

        if c["extreme"] and nc["extreme"] and nr["extreme"]:
            if len({c["extreme_side"], nc["extreme_side"], nr["extreme_side"]}) >= 2:
                candidates.append(
                    (
                        "THREE-GROUP EXTREME / DIVERGENCE EVENT",
                        "multi_group",
                        {
                            "commercial": c["extreme_side"],
                            "noncommercial": nc["extreme_side"],
                            "nonreportable": nr["extreme_side"],
                        },
                    )
                )

        for label, kind, meta in candidates:
            prev_i = last_by_label.get(label, -10_000)
            if i - prev_i < EPISODE_COOLDOWN_WEEKS:
                continue
            last_by_label[label] = i
            events.append(
                MarkerEvent(
                    date=date,
                    group="multi",
                    kind=kind,
                    label=label,
                    net=c["net"],
                    percentile=c["percentile"],
                    change_1w=c["change_1w"],
                    change_4w=c["change_4w"],
                    change_12w=c["change_12w"],
                    classification=c["classification"],
                    meta=meta,
                )
            )
    return events


def _forward_path_stats(prices: list[float | None], idx: int, horizon: int) -> dict[str, Any] | None:
    p0 = prices[idx]
    if p0 is None or p0 == 0 or idx + horizon >= len(prices):
        return None
    window = prices[idx : idx + horizon + 1]
    if any(p is None for p in window):
        # allow sparse: require end price
        p1 = prices[idx + horizon]
        if p1 is None:
            return None
        rets = None
        fav = None
        adv = None
        end_ret = (p1 - p0) / p0 * 100.0
    else:
        end_ret = (window[-1] - p0) / p0 * 100.0
        path = [(p - p0) / p0 * 100.0 for p in window[1:]]
        fav = max(path) if path else None
        adv = min(path) if path else None
    return {
        "return_pct": round(end_ret, 4),
        "higher": end_ret > 0,
        "favourable_excursion_pct": None if fav is None else round(fav, 4),
        "adverse_excursion_pct": None if adv is None else round(adv, 4),
    }


def _summarize_outcomes(cases: list[dict[str, Any]], horizon: int) -> dict[str, Any]:
    vals = []
    favs = []
    advs = []
    for c in cases:
        o = (c.get("outcomes") or {}).get(str(horizon))
        if not o:
            continue
        vals.append(o["return_pct"])
        if o.get("favourable_excursion_pct") is not None:
            favs.append(o["favourable_excursion_pct"])
        if o.get("adverse_excursion_pct") is not None:
            advs.append(o["adverse_excursion_pct"])
    n = len(vals)
    if n == 0:
        return {
            "horizon_weeks": horizon,
            "n": 0,
            "sample_quality": sample_quality(0),
            "pct_higher": None,
            "pct_lower": None,
            "higher_count": 0,
            "lower_count": 0,
            "avg_return_pct": None,
            "median_return_pct": None,
            "avg_favourable_excursion_pct": None,
            "avg_adverse_excursion_pct": None,
            "best_return_pct": None,
            "worst_return_pct": None,
            "headline_allowed": False,
            "note": "NO RELIABLE HISTORICAL CONCLUSION — no measurable forward outcomes.",
        }
    higher = sum(1 for v in vals if v > 0)
    lower = sum(1 for v in vals if v < 0)
    sorted_v = sorted(vals)
    mid = n // 2
    median = sorted_v[mid] if n % 2 else (sorted_v[mid - 1] + sorted_v[mid]) / 2
    quality = sample_quality(n)
    return {
        "horizon_weeks": horizon,
        "n": n,
        "sample_quality": quality,
        "pct_higher": round(100.0 * higher / n, 1),
        "pct_lower": round(100.0 * lower / n, 1),
        "higher_count": higher,
        "lower_count": lower,
        "avg_return_pct": round(sum(vals) / n, 3),
        "median_return_pct": round(median, 3),
        "avg_favourable_excursion_pct": None if not favs else round(sum(favs) / len(favs), 3),
        "avg_adverse_excursion_pct": None if not advs else round(sum(advs) / len(advs), 3),
        "best_return_pct": round(max(vals), 3),
        "worst_return_pct": round(min(vals), 3),
        "headline_allowed": n >= 8,
        "note": (
            None
            if n >= 8
            else f"{higher}/{n} higher ({round(100*higher/n)}%) — {quality}. Do not treat as a reliable edge."
        ),
    }


def _analogue_match(current: dict[str, Any], hist: dict[str, Any]) -> tuple[bool, list[str]]:
    """Transparent rule-based match. Returns (ok, matched_rules)."""
    rules: list[str] = []
    c = current[GROUP_COMMERCIAL]
    h = hist[GROUP_COMMERCIAL]
    if c["extreme_side"] and c["extreme_side"] == h["extreme_side"]:
        rules.append(f"Commercial extreme side={c['extreme_side']}")
    elif c["direction"] != "neutral" and c["direction"] == h["direction"]:
        # same directional regime within 15 pctile points if both have pct
        if c["percentile"] is not None and h["percentile"] is not None:
            if abs(c["percentile"] - h["percentile"]) <= 15:
                rules.append(
                    f"Commercial regime≈pctile ({h['percentile']:.0f} vs {c['percentile']:.0f})"
                )
            else:
                return False, []
        else:
            rules.append(f"Commercial direction={c['direction']}")
    else:
        return False, []

    # Commercial recent flow direction
    if c["flow_1w"] != "neutral" and c["flow_1w"] == h["flow_1w"]:
        rules.append(f"Commercial 1W flow={c['flow_1w']}")
    elif c["flow_1w"] != "neutral" and h["flow_1w"] != c["flow_1w"]:
        # allow if 4W momentum matches
        if _sign(c["change_4w"]) == _sign(h["change_4w"]) and _sign(c["change_4w"]) != "neutral":
            rules.append("Commercial 4W momentum matches")
        else:
            return False, []

    # NR opposition state
    if current["nr_divergence"] == hist["nr_divergence"]:
        rules.append(
            "NR divergence=" + ("yes" if current["nr_divergence"] else "no")
        )
    else:
        return False, []

    # NC alignment/opposition coarse state
    if current["nc_opposed"] == hist["nc_opposed"]:
        rules.append("NC opposed=" + ("yes" if current["nc_opposed"] else "no"))
    else:
        return False, []

    return len(rules) >= 3, rules


def find_analogues(
    series: list[dict[str, Any]],
    current_idx: int,
) -> dict[str, Any]:
    prices = [_finite(r.get("price")) for r in series]
    current = _snapshot_at(series, current_idx)
    # Require enough forward room for at least 4W on historical cases
    max_i = current_idx - 1
    raw_matches: list[dict[str, Any]] = []

    for i in range(MIN_HISTORY - 1, max_i + 1):
        if i + min(FORWARD_HORIZONS) >= len(series):
            continue
        hist = _snapshot_at(series, i)
        ok, rules = _analogue_match(current, hist)
        if not ok:
            continue
        outcomes = {}
        for h in FORWARD_HORIZONS:
            fo = _forward_path_stats(prices, i, h)
            if fo:
                outcomes[str(h)] = fo
        if not outcomes:
            continue
        raw_matches.append(
            {
                "date": hist["date"],
                "index": i,
                "matched_rules": rules,
                "commercial_percentile": hist[GROUP_COMMERCIAL]["percentile"],
                "outcomes": outcomes,
            }
        )

    # De-duplicate episodes: keep earliest in each cooldown cluster
    raw_matches.sort(key=lambda m: m["index"])
    independent: list[dict[str, Any]] = []
    last_idx = -10_000
    for m in raw_matches:
        if m["index"] - last_idx < ANALOGUE_COOLDOWN_WEEKS:
            continue
        independent.append(m)
        last_idx = m["index"]

    by_horizon = {str(h): _summarize_outcomes(independent, h) for h in FORWARD_HORIZONS}

    # Best-supported horizon: prefer largest n with headline_allowed, else best n
    best = None
    for h in FORWARD_HORIZONS:
        s = by_horizon[str(h)]
        if s["n"] <= 0:
            continue
        if best is None:
            best = s
            continue
        if s["headline_allowed"] and not best["headline_allowed"]:
            best = s
        elif s["headline_allowed"] == best["headline_allowed"] and s["n"] > best["n"]:
            best = s
        elif not best["headline_allowed"] and s["n"] > best["n"]:
            best = s

    return {
        "matching_method": (
            "Rule-based: Commercial extreme/regime + 1W/4W flow, NR divergence state, "
            f"NC opposition state. Independent cases require ≥{ANALOGUE_COOLDOWN_WEEKS}w separation."
        ),
        "current_features": {
            "commercial": current[GROUP_COMMERCIAL],
            "noncommercial": current[GROUP_NONCOMMERCIAL],
            "nonreportable": current[GROUP_NONREPORTABLE],
            "nr_divergence": current["nr_divergence"],
            "nc_opposed": current["nc_opposed"],
        },
        "independent_case_count": len(independent),
        "raw_match_count_before_dedup": len(raw_matches),
        "cases": independent,
        "outcomes_by_horizon": by_horizon,
        "best_supported_horizon": best,
        "sample_quality": sample_quality(len(independent)),
    }


def _build_current_panel(snap: dict[str, Any], analogues: dict[str, Any]) -> dict[str, Any]:
    c = snap[GROUP_COMMERCIAL]
    nc = snap[GROUP_NONCOMMERCIAL]
    nr = snap[GROUP_NONREPORTABLE]

    # STATE vs CHANGE — never conflate
    state_bits = []
    if c["extreme"]:
        state_bits.append(
            f"Extreme {c['extreme_side']} positioning ({c['percentile']:.0f}th historical percentile)"
        )
    else:
        state_bits.append(
            f"{c['classification']} ({c['percentile']:.0f}th percentile)"
            if c["percentile"] is not None
            else c["classification"]
        )

    change_bits = []
    if c["change_1w"] is not None:
        change_bits.append(f"{c['change_1w']:+,.0f} this week")
    if c["change_4w"] is not None:
        change_bits.append(f"4W {c['change_4w']:+,.0f}")
    if c["change_12w"] is not None:
        change_bits.append(f"12W {c['change_12w']:+,.0f}")

    # Prefer historical-regime side (percentile extreme) over raw net sign for narrative.
    regime_side = c["extreme_side"] or c["direction"]
    if c["extreme"] and c["flow_1w"] != "neutral" and c["flow_1w"] != c["extreme_side"]:
        change_narrative = (
            f"Commercials remain at an extreme {c['extreme_side']} historical position, "
            f"but weekly flow moved {c['flow_1w']} this week (net {c['direction']})."
        )
    elif c["flow_1w"] == regime_side and regime_side != "neutral":
        change_narrative = (
            f"Commercial {regime_side} positioning is strengthening on the latest weekly flow."
        )
    elif c["flow_1w"] != "neutral" and c["flow_1w"] != regime_side:
        change_narrative = (
            f"Commercial historical regime is {regime_side} (net {c['direction']}), "
            f"but weekly flow turned {c['flow_1w']}."
        )
    else:
        change_narrative = "Commercial weekly flow is flat or unavailable."

    interpretation = (
        f"Commercials: {'; '.join(state_bits)}. "
        f"Non-Commercials remain net {nc['direction']} with 1W flow {nc['flow_1w']}. "
        f"Non-Reportables remain net {nr['direction']}"
        + (" and materially opposed to Commercials." if snap["nr_divergence"] else ".")
    )

    watch = (
        "Watch for Non-Commercial weekly flow beginning to shift toward the Commercial bias, "
        "continued Commercial accumulation/distribution, or deterioration of the Commercial extreme. "
        "Positioning evidence alone is not an execution signal."
    )

    best = analogues.get("best_supported_horizon")
    return {
        "state": {
            "commercial": " · ".join(state_bits),
            "noncommercial": f"Net {nc['direction']} ({nc['classification']})",
            "nonreportable": f"Net {nr['direction']} ({nr['classification']})",
        },
        "change": {
            "commercial_metrics": change_bits,
            "commercial_narrative": change_narrative,
            "noncommercial": f"Latest weekly flow {nc['flow_1w']}",
            "nonreportable": f"Latest weekly flow {nr['flow_1w']}",
        },
        "interpretation": interpretation,
        "what_to_watch": watch,
        "setup_summary": {
            "commercial": state_bits[0] if state_bits else "—",
            "commercial_change": change_narrative,
            "noncommercial": f"Still {nc['direction']}; 1W flow {nc['flow_1w']}",
            "nonreportable": (
                "Materially opposed" if snap["nr_divergence"] else f"Net {nr['direction']}"
            ),
            "analogue_cases": analogues.get("independent_case_count", 0),
            "sample_quality": analogues.get("sample_quality"),
            "best_horizon_weeks": None if not best else best.get("horizon_weeks"),
            "best_horizon_note": None
            if not best
            else (
                f"{best['higher_count']}/{best['n']} higher · median {best['median_return_pct']}%"
                if best.get("n")
                else "NO RELIABLE HISTORICAL CONCLUSION"
            ),
            "disclaimer": (
                "Historical tendency from positioning analogues only — not a buy/sell recommendation."
            ),
        },
    }


def build_market_intelligence(market: str, block: dict[str, Any]) -> dict[str, Any]:
    series = list(block.get("series") or [])
    if len(series) < MIN_HISTORY:
        return {
            "market": market,
            "available": False,
            "reason": f"insufficient_history<{MIN_HISTORY}",
            "weeks": len(series),
        }

    idx = len(series) - 1
    as_of = str(series[idx].get("date") or "")[:10]

    extremes = {
        GROUP_COMMERCIAL: [e.to_dict() for e in _detect_group_extremes(series, GROUP_COMMERCIAL)],
        GROUP_NONCOMMERCIAL: [e.to_dict() for e in _detect_group_extremes(series, GROUP_NONCOMMERCIAL)],
        GROUP_NONREPORTABLE: [e.to_dict() for e in _detect_group_extremes(series, GROUP_NONREPORTABLE)],
    }
    multi = [e.to_dict() for e in _detect_multi_group_events(series)]
    analogues = find_analogues(series, idx)
    snap = _snapshot_at(series, idx)
    panel = _build_current_panel(snap, analogues)

    # Flat marker list for UI
    markers = []
    for g, evs in extremes.items():
        for e in evs:
            markers.append({**e, "layer": f"{g}_extremes"})
    for e in multi:
        markers.append({**e, "layer": "multi_group"})

    return {
        "market": market,
        "available": True,
        "source_week": as_of,
        "weeks": len(series),
        "thresholds": {
            "min_history_weeks": MIN_HISTORY,
            "extreme_high_percentile": EXTREME_HIGH,
            "extreme_low_percentile": EXTREME_LOW,
            "severe_step_percentile": SEVERE_STEP,
            "episode_cooldown_weeks": EPISODE_COOLDOWN_WEEKS,
            "analogue_cooldown_weeks": ANALOGUE_COOLDOWN_WEEKS,
            "forward_horizons_weeks": list(FORWARD_HORIZONS),
        },
        "current_snapshot": snap,
        "intelligence_panel": panel,
        "extremes": extremes,
        "multi_group_events": multi,
        "markers": markers,
        "analogues": analogues,
    }


def build_workstation_intelligence(cot3y_doc: dict[str, Any]) -> dict[str, Any]:
    markets = cot3y_doc.get("markets") or {}
    out_markets: dict[str, Any] = {}
    for mid, block in markets.items():
        out_markets[mid] = build_market_intelligence(mid, block or {})

    available = sum(1 for m in out_markets.values() if m.get("available"))
    return {
        "version": "cot_workstation_intelligence_v2",
        "engine": "workstation_intelligence",
        "generated_note": (
            "Investigation / evidence only. Not buy/sell advice. "
            "Analogues are rule-based and episode-deduplicated."
        ),
        "markets": out_markets,
        "summary": {
            "markets_total": len(out_markets),
            "markets_available": available,
        },
    }
