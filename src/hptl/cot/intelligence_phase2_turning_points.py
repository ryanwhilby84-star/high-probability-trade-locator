"""COT Intelligence Engine — Phase 2 turning-point validation & outcome study.

Positioning-defined turning points A–D are evaluated for:
  1) independence / episode discipline
  2) positioning follow-through (before price)
  3) forward price outcomes on Phase-1 trustworthy markets only
  4) descriptive EX/DIV context
  5) robustness / recommendation (no production rollout)

Does NOT change EX/DIV thresholds, production detector, UI, or intelligence scores.
Copper remains excluded from return studies via the Phase-1 trustworthy gate.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.intelligence_phase1_audit import (
    ABSOLUTE_HIGH,
    ABSOLUTE_LOW,
    COT3Y_PATHS,
    EVENT_COOLDOWN_WEEKS,
    MIN_HISTORY,
    ROTATION_PCT_MOVE_26W,
    TP_CONFIRM_WEEKS,
    TP_VELOCITY_SIGN_LAG,
    _finite,
    _load_first,
    _zone,
    audit_price_series,
)
from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    GROUP_NONCOMMERCIAL,
    GROUP_NONREPORTABLE,
    _forward_path_stats,
    build_group_state_series,
    build_spread_series,
    detect_configuration_events,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
PHASE1_AUDIT_PATH = AUDIT_DIR / "cot_intelligence_phase1_audit.json"
PHASE2_JSON = AUDIT_DIR / "cot_intelligence_phase2_turning_points.json"
PHASE2_MD = AUDIT_DIR / "cot_intelligence_phase2_turning_points.md"
PHASE2_INVENTORY = (
    PROCESSED_DIR / "cot_intelligence_phase2_tp_inventory_latest.json"
)

# Independence: confirmation dates of the same (market, group, definition, direction)
# must be >= TP_COOLDOWN_WEEKS apart (mirrors production event cooldown intent).
TP_COOLDOWN_WEEKS = EVENT_COOLDOWN_WEEKS

# "Immediately after extreme" window for context study (descriptive).
AFTER_EXTREME_WEEKS = 8

# Minimum independent n to claim anything beyond "insufficient".
MIN_N_REPORT = 8
MIN_N_PROMOTE = 30

GROUPS = (GROUP_COMMERCIAL, GROUP_NONCOMMERCIAL, GROUP_NONREPORTABLE)
DEFS = ("A", "B", "C", "D")

ASSET_CLASS = {
    "Australian Dollar / 6A": "fx",
    "British Pound / 6B": "fx",
    "Canadian Dollar / 6C": "fx",
    "Euro FX / 6E": "fx",
    "Japanese Yen / 6J": "fx",
    "NZ Dollar / 6N": "fx",
    "Swiss Franc / 6S": "fx",
    "US Dollar Index / DX": "fx",
    "Gold": "metals",
    "Silver": "metals",
    "Platinum": "metals",
    "Palladium": "metals",
    "Copper / HG": "metals",
    "Crude Oil / CL": "energy",
    "Natural Gas / NG": "energy",
    "Corn": "ag",
    "Wheat": "ag",
    "Soybeans": "ag",
    "Cotton": "ag",
    "Coffee": "ag",
    "Cocoa": "ag",
    "Sugar": "ag",
    "S&P 500 / ES": "equity",
    "NASDAQ / NQ": "equity",
    "Dow / YM": "equity",
    "Bitcoin": "crypto",
}


def _median(xs: list[float]) -> float | None:
    if not xs:
        return None
    return float(statistics.median(xs))


def _mean(xs: list[float]) -> float | None:
    if not xs:
        return None
    return float(sum(xs) / len(xs))


def _stdev(xs: list[float]) -> float | None:
    if len(xs) < 2:
        return None
    return float(statistics.pstdev(xs))


def _sign(v: float | None) -> int | None:
    if v is None:
        return None
    if v > 0:
        return 1
    if v < 0:
        return -1
    return 0


def _dir_label(sign: int | None) -> str | None:
    if sign is None or sign == 0:
        return None
    return "bullish" if sign > 0 else "bearish"


def _load_trustworthy_markets() -> list[str]:
    if PHASE1_AUDIT_PATH.is_file():
        doc = json.loads(PHASE1_AUDIT_PATH.read_text(encoding="utf-8"))
        markets = (doc.get("summary") or {}).get("trustworthy_markets") or []
        if markets:
            return list(markets)
    # Fallback: recompute from cot3y via Phase-1 auditor
    cot3y = _load_first(COT3Y_PATHS)
    out = []
    for mid, block in (cot3y.get("markets") or {}).items():
        series = block.get("series") or []
        audit = audit_price_series(mid, series, None)
        if audit.get("trustworthy_for_outcome_labels"):
            out.append(mid)
    return sorted(out)


def _cluster_independent(events: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Keep earliest confirmation in each cooldown cluster per key."""
    events = sorted(events, key=lambda e: (e["confirm_index"], e["onset_index"]))
    kept: list[dict[str, Any]] = []
    last_by_key: dict[tuple[Any, ...], int] = {}
    episode_seq: dict[tuple[Any, ...], int] = defaultdict(int)

    for e in events:
        key = (e["market"], e["group"], e["definition"], e["direction"])
        prev = last_by_key.get(key, -10_000)
        if e["confirm_index"] - prev < TP_COOLDOWN_WEEKS:
            continue
        episode_seq[key] += 1
        out = dict(e)
        out["episode_id"] = f"{e['market']}|{e['group']}|{e['definition']}|{e['direction']}|{episode_seq[key]}"
        out["independent"] = True
        kept.append(out)
        last_by_key[key] = e["confirm_index"]
    return kept


def detect_turning_points_raw(
    market: str,
    states: list[dict[str, Any]],
    group: str,
) -> list[dict[str, Any]]:
    """Emit raw (pre-independence) turning points for definitions A–D.

    Confirmation date is always the research timestamp.
    Definition D is timestamped at confirmation week only (never retroactive peak).
    """
    n = len(states)
    long_pcts = [(s.get("percentiles") or {}).get("long_history") for s in states]
    nets = [s.get("net") for s in states]
    v4s = [
        ((s.get("velocity") or {}).get("4w") or {}).get("percentile_change") for s in states
    ]
    out: list[dict[str, Any]] = []

    for i in range(MIN_HISTORY - 1, n):
        s = states[i]
        date = s.get("date")
        long_pct = long_pcts[i]
        j26 = ((s.get("journey") or {}).get("26w") or {}).get("long_history_percentile")

        # --- A: production major_rotation (displacement) ---
        if long_pct is not None and j26 is not None:
            delta = long_pct - j26
            if abs(delta) >= ROTATION_PCT_MOVE_26W:
                direction = _dir_label(_sign(delta))
                if direction:
                    out.append(
                        {
                            "market": market,
                            "group": group,
                            "definition": "A",
                            "definition_name": "major_rotation_26w_displacement",
                            "direction": direction,
                            "onset_index": i,
                            "onset_date": date,
                            "confirm_index": i,
                            "confirm_date": date,
                            "detail": {
                                "delta_26w": round(delta, 2),
                                "long_pct": long_pct,
                                "pct_26w_ago": j26,
                            },
                        }
                    )

        # --- B: exit extreme zone ---
        if i > 0:
            prev_z = _zone(long_pcts[i - 1])
            cur_z = _zone(long_pct)
            if prev_z is not None and cur_z is None:
                # Exiting high extreme => positioning turning down (bearish for net)
                # Exiting low extreme => positioning turning up (bullish for net)
                direction = "bearish" if prev_z == "high" else "bullish"
                out.append(
                    {
                        "market": market,
                        "group": group,
                        "definition": "B",
                        "definition_name": "exit_extreme_zone",
                        "direction": direction,
                        "onset_index": i - 1,
                        "onset_date": states[i - 1].get("date"),
                        "confirm_index": i,
                        "confirm_date": date,
                        "detail": {
                            "prior_zone": prev_z,
                            "long_pct": long_pct,
                            "prior_long_pct": long_pcts[i - 1],
                        },
                    }
                )

        # --- C: velocity 4w sign flip + confirmation ---
        if i >= TP_VELOCITY_SIGN_LAG + TP_CONFIRM_WEEKS:
            flip_idx = i - TP_CONFIRM_WEEKS
            a = v4s[flip_idx]
            b = v4s[i]
            if a is not None and b is not None and a != 0 and b != 0 and (a > 0) != (b > 0):
                ok = True
                for k in range(flip_idx + 1, i + 1):
                    vk = v4s[k]
                    if vk is None or vk == 0 or (vk > 0) != (b > 0):
                        ok = False
                        break
                if ok:
                    direction = _dir_label(_sign(b))
                    if direction:
                        out.append(
                            {
                                "market": market,
                                "group": group,
                                "definition": "C",
                                "definition_name": "velocity_4w_sign_flip_confirmed",
                                "direction": direction,
                                "onset_index": flip_idx,
                                "onset_date": states[flip_idx].get("date"),
                                "confirm_index": i,
                                "confirm_date": date,
                                "detail": {
                                    "v4_at_onset": a,
                                    "v4_at_confirm": b,
                                    "confirm_weeks": TP_CONFIRM_WEEKS,
                                },
                            }
                        )

        # --- D: peak/trough then confirmed reversal; timestamp = confirmation week ---
        # Pivot detected at center of 3-week window ending at i; confirmation requires
        # TP_CONFIRM_WEEKS of subsequent path. Event is emitted at confirm_idx = i + TP_CONFIRM_WEEKS.
        if i >= 2 and long_pct is not None:
            window = long_pcts[i - 2 : i + 1]
            if all(x is not None for x in window):
                pivot_idx = i - 1
                pivot = window[1]
                is_peak = window[1] >= window[0] and window[1] >= window[2]  # type: ignore[operator]
                is_trough = window[1] <= window[0] and window[1] <= window[2]  # type: ignore[operator]
                confirm_idx = pivot_idx + TP_CONFIRM_WEEKS
                if confirm_idx < n and (is_peak or is_trough):
                    future = long_pcts[pivot_idx + 1 : confirm_idx + 1]
                    if len(future) == TP_CONFIRM_WEEKS and all(x is not None for x in future):
                        if is_peak and all(x < pivot for x in future):  # type: ignore[operator]
                            out.append(
                                {
                                    "market": market,
                                    "group": group,
                                    "definition": "D",
                                    "definition_name": "percentile_peak_then_reversal",
                                    "direction": "bearish",
                                    "onset_index": pivot_idx,
                                    "onset_date": states[pivot_idx].get("date"),
                                    "confirm_index": confirm_idx,
                                    "confirm_date": states[confirm_idx].get("date"),
                                    "detail": {
                                        "pivot_pct": pivot,
                                        "confirm_weeks": TP_CONFIRM_WEEKS,
                                        "timestamp_rule": "confirmation_week_only",
                                    },
                                }
                            )
                        if is_trough and all(x > pivot for x in future):  # type: ignore[operator]
                            out.append(
                                {
                                    "market": market,
                                    "group": group,
                                    "definition": "D",
                                    "definition_name": "percentile_trough_then_reversal",
                                    "direction": "bullish",
                                    "onset_index": pivot_idx,
                                    "onset_date": states[pivot_idx].get("date"),
                                    "confirm_index": confirm_idx,
                                    "confirm_date": states[confirm_idx].get("date"),
                                    "detail": {
                                        "pivot_pct": pivot,
                                        "confirm_weeks": TP_CONFIRM_WEEKS,
                                        "timestamp_rule": "confirmation_week_only",
                                    },
                                }
                            )

    # unused nets kept for clarity / future net-based defs
    _ = nets
    return out


def score_positioning_followthrough(
    event: dict[str, Any],
    states: list[dict[str, Any]],
) -> dict[str, Any]:
    """Did positioning continue in the claimed turn direction after confirmation?"""
    t = int(event["confirm_index"])
    direction = event["direction"]
    expected = 1 if direction == "bullish" else -1
    long_pcts = [(s.get("percentiles") or {}).get("long_history") for s in states]
    nets = [s.get("net") for s in states]

    def forward_delta(series: list[Any], horizon: int) -> float | None:
        if t + horizon >= len(series):
            return None
        a, b = _finite(series[t]), _finite(series[t + horizon])
        if a is None or b is None:
            return None
        return b - a

    pct_4 = forward_delta(long_pcts, 4)
    pct_8 = forward_delta(long_pcts, 8)
    net_4 = forward_delta(nets, 4)
    net_8 = forward_delta(nets, 8)

    def aligned(delta: float | None) -> bool | None:
        if delta is None:
            return None
        s = _sign(delta)
        if s is None or s == 0:
            return False
        return s == expected

    a4, a8 = aligned(pct_4), aligned(pct_8)
    n4, n8 = aligned(net_4), aligned(net_8)

    # Persistence: both 4w and 8w percentile moves agree with expected direction
    persistent = a4 is True and a8 is True
    # False turn: 4w percentile moves opposite
    false_turn = a4 is False
    # Whipsaw: correct at 4w, wrong at 8w
    whipsaw = a4 is True and a8 is False

    return {
        "pct_delta_4w": None if pct_4 is None else round(pct_4, 3),
        "pct_delta_8w": None if pct_8 is None else round(pct_8, 3),
        "net_delta_4w": None if net_4 is None else round(net_4, 2),
        "net_delta_8w": None if net_8 is None else round(net_8, 2),
        "pct_aligned_4w": a4,
        "pct_aligned_8w": a8,
        "net_aligned_4w": n4,
        "net_aligned_8w": n8,
        "persistent_reversal": persistent,
        "false_turn": false_turn,
        "whipsaw": whipsaw,
        "measurable_4w": a4 is not None,
        "measurable_8w": a8 is not None,
    }


def attach_price_outcomes(
    event: dict[str, Any],
    prices: list[float | None],
) -> dict[str, Any] | None:
    t = int(event["confirm_index"])
    out: dict[str, Any] = {}
    any_ok = False
    for h in (1, 4, 8, 12):
        fo = _forward_path_stats(prices, t, h)
        if fo is None:
            out[f"fwd_{h}w"] = None
        else:
            any_ok = True
            # Bullish positioning turn → expect higher prices if "aligned" narrative;
            # we store raw returns; signed_for_direction flips for bearish turns.
            raw = fo["return_pct"]
            signed = raw if event["direction"] == "bullish" else (-raw if raw is not None else None)
            out[f"fwd_{h}w"] = {
                "return_pct": raw,
                "signed_return_pct": signed,
                "mfe_pct": fo["favourable_excursion_pct"],
                "mae_pct": fo["adverse_excursion_pct"],
            }
    return out if any_ok else None


def attach_context(
    event: dict[str, Any],
    states: list[dict[str, Any]],
    spreads: list[dict[str, Any]],
    events_by_index: dict[int, list[dict[str, Any]]],
) -> dict[str, Any]:
    t = int(event["confirm_index"])
    long_pct = (states[t].get("percentiles") or {}).get("long_history")
    zone = _zone(long_pct)
    inside_extreme = zone is not None

    # Immediately after extreme: was in zone within prior AFTER_EXTREME_WEEKS, now out
    after_extreme = False
    if not inside_extreme:
        for j in range(max(0, t - AFTER_EXTREME_WEEKS), t):
            if _zone((states[j].get("percentiles") or {}).get("long_history")) is not None:
                after_extreme = True
                break

    sp = spreads[t] if t < len(spreads) else {}
    spread_pct = sp.get("spread_percentile")
    div_active = spread_pct is not None and (spread_pct >= 90 or spread_pct <= 10)
    # Also note if a divergence emit fires this week (cooldown-aware)
    week_evs = events_by_index.get(t) or []
    div_emit = any(e.get("event_type") == "comm_nr_divergence" for e in week_evs)
    abs_emit = any(
        e.get("event_type") in {"absolute_extreme", "local_extreme"}
        and e.get("group") == event["group"]
        for e in week_evs
    )

    return {
        "inside_extreme": inside_extreme,
        "extreme_zone": zone,
        "immediately_after_extreme": after_extreme,
        "divergence_active_band": div_active,
        "divergence_emit_this_week": div_emit,
        "group_ex_emit_this_week": abs_emit,
        "spread_percentile": spread_pct,
        "long_history_percentile": long_pct,
    }


def _rate(flags: Iterable[bool | None]) -> dict[str, Any]:
    vals = [f for f in flags if f is not None]
    if not vals:
        return {"n": 0, "rate": None}
    return {"n": len(vals), "rate": round(sum(1 for v in vals if v) / len(vals), 4)}


def summarize_positioning(events: list[dict[str, Any]]) -> dict[str, Any]:
    ft = _rate(e.get("positioning", {}).get("false_turn") for e in events)
    ws = _rate(e.get("positioning", {}).get("whipsaw") for e in events)
    pers = _rate(e.get("positioning", {}).get("persistent_reversal") for e in events)
    a4 = _rate(e.get("positioning", {}).get("pct_aligned_4w") for e in events)
    a8 = _rate(e.get("positioning", {}).get("pct_aligned_8w") for e in events)
    n4 = _rate(e.get("positioning", {}).get("net_aligned_4w") for e in events)
    n8 = _rate(e.get("positioning", {}).get("net_aligned_8w") for e in events)
    return {
        "n_events": len(events),
        "pct_aligned_4w": a4,
        "pct_aligned_8w": a8,
        "net_aligned_4w": n4,
        "net_aligned_8w": n8,
        "persistent_reversal_rate": pers,
        "false_turn_rate": ft,
        "whipsaw_rate": ws,
    }


def summarize_returns(events: list[dict[str, Any]], *, signed: bool = True) -> dict[str, Any]:
    out: dict[str, Any] = {"n_events_with_price": 0}
    keyed = [e for e in events if e.get("price_outcomes")]
    out["n_events_with_price"] = len(keyed)
    for h in (1, 4, 8, 12):
        field = "signed_return_pct" if signed else "return_pct"
        vals = []
        mfes = []
        maes = []
        for e in keyed:
            block = (e.get("price_outcomes") or {}).get(f"fwd_{h}w") or {}
            v = block.get(field)
            if v is not None:
                vals.append(float(v))
            if block.get("mfe_pct") is not None:
                mfes.append(float(block["mfe_pct"]))
            if block.get("mae_pct") is not None:
                maes.append(float(block["mae_pct"]))
        pos = sum(1 for v in vals if v > 0)
        neg = sum(1 for v in vals if v < 0)
        out[f"fwd_{h}w"] = {
            "n": len(vals),
            "median_return_pct": None if not vals else round(_median(vals) or 0, 4),
            "mean_return_pct": None if not vals else round(_mean(vals) or 0, 4),
            "stdev_return_pct": None if _stdev(vals) is None else round(_stdev(vals) or 0, 4),
            "pct_positive": None if not vals else round(100.0 * pos / len(vals), 1),
            "pct_negative": None if not vals else round(100.0 * neg / len(vals), 1),
            "median_mfe_pct": None if not mfes else round(_median(mfes) or 0, 4),
            "median_mae_pct": None if not maes else round(_median(maes) or 0, 4),
            "sample_quality": (
                "INSUFFICIENT"
                if len(vals) < MIN_N_REPORT
                else ("MODERATE" if len(vals) < MIN_N_PROMOTE else "STRONGER")
            ),
        }
    return out


def build_market_phase2(
    market: str,
    block: dict[str, Any],
    *,
    price_ok: bool,
) -> dict[str, Any]:
    series = list(block.get("series") or [])
    if len(series) < MIN_HISTORY + 12:
        return {"market": market, "available": False, "reason": "insufficient_history"}

    commercial = build_group_state_series(series, GROUP_COMMERCIAL)
    noncommercial = build_group_state_series(series, GROUP_NONCOMMERCIAL)
    nonreportable = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(commercial, nonreportable)
    detector_events = detect_configuration_events(
        commercial, noncommercial, nonreportable, spreads
    )
    events_by_index: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for e in detector_events:
        events_by_index[int(e["index"])].append(e)

    prices = [_finite(r.get("price")) for r in series]
    group_states = {
        GROUP_COMMERCIAL: commercial,
        GROUP_NONCOMMERCIAL: noncommercial,
        GROUP_NONREPORTABLE: nonreportable,
    }

    raw: list[dict[str, Any]] = []
    for g, states in group_states.items():
        raw.extend(detect_turning_points_raw(market, states, g))

    independent = _cluster_independent(raw)

    enriched: list[dict[str, Any]] = []
    for e in independent:
        states = group_states[e["group"]]
        e2 = dict(e)
        e2["asset_class"] = ASSET_CLASS.get(market, "other")
        e2["positioning"] = score_positioning_followthrough(e2, states)
        e2["context"] = attach_context(e2, states, spreads, events_by_index)
        if price_ok:
            e2["price_outcomes"] = attach_price_outcomes(e2, prices)
            e2["price_study_eligible"] = e2["price_outcomes"] is not None
        else:
            e2["price_outcomes"] = None
            e2["price_study_eligible"] = False
            e2["price_exclusion_reason"] = "market_not_in_phase1_trustworthy_set"
        enriched.append(e2)

    counts: dict[str, Any] = {
        "raw": len(raw),
        "independent": len(enriched),
        "by_definition": dict(Counter(e["definition"] for e in enriched)),
        "by_group": dict(Counter(e["group"] for e in enriched)),
        "by_direction": dict(Counter(e["direction"] for e in enriched)),
    }

    return {
        "market": market,
        "available": True,
        "price_study_eligible": price_ok,
        "weeks": len(series),
        "counts": counts,
        "events": enriched,
    }


def _slice_stats(events: list[dict[str, Any]], *, price: bool) -> dict[str, Any]:
    pos = summarize_positioning(events)
    result: dict[str, Any] = {"positioning": pos, "n": len(events)}
    if price:
        eligible = [e for e in events if e.get("price_study_eligible")]
        result["price_signed"] = summarize_returns(eligible, signed=True)
        result["price_raw"] = summarize_returns(eligible, signed=False)
        result["n_price_eligible"] = len(eligible)
    return result


def aggregate_study(all_events: list[dict[str, Any]]) -> dict[str, Any]:
    """Cross-market aggregates for positioning and (eligible) price outcomes."""
    by_def: dict[str, Any] = {}
    for d in DEFS:
        subset = [e for e in all_events if e["definition"] == d]
        by_def[d] = {
            "all": _slice_stats(subset, price=True),
            "by_group": {
                g: _slice_stats([e for e in subset if e["group"] == g], price=True)
                for g in GROUPS
            },
            "by_direction": {
                direction: _slice_stats(
                    [e for e in subset if e["direction"] == direction], price=True
                )
                for direction in ("bullish", "bearish")
            },
        }

    # Context study (descriptive) per definition
    context: dict[str, Any] = {}
    for d in DEFS:
        subset = [e for e in all_events if e["definition"] == d]
        buckets = {
            "inside_extreme": [e for e in subset if e.get("context", {}).get("inside_extreme")],
            "after_extreme": [
                e for e in subset if e.get("context", {}).get("immediately_after_extreme")
            ],
            "with_divergence": [
                e for e in subset if e.get("context", {}).get("divergence_active_band")
            ],
            "without_divergence": [
                e
                for e in subset
                if not e.get("context", {}).get("divergence_active_band")
            ],
        }
        context[d] = {
            name: _slice_stats(evs, price=True) for name, evs in buckets.items()
        }

    # Asset class (price-eligible only, sufficient n)
    by_asset: dict[str, Any] = {}
    for d in DEFS:
        by_asset[d] = {}
        for ac in sorted({e.get("asset_class") for e in all_events}):
            evs = [
                e
                for e in all_events
                if e["definition"] == d and e.get("asset_class") == ac
            ]
            if len(evs) < MIN_N_REPORT:
                continue
            by_asset[d][ac] = _slice_stats(evs, price=True)

    return {"by_definition": by_def, "context": context, "by_asset_class": by_asset}


def robustness_checks(all_events: list[dict[str, Any]]) -> dict[str, Any]:
    """Sample-size, market domination, crude regime split."""
    report: dict[str, Any] = {}
    for d in DEFS:
        subset = [e for e in all_events if e["definition"] == d and e.get("price_study_eligible")]
        by_market = Counter(e["market"] for e in subset)
        total = len(subset)
        top = by_market.most_common(5)
        top_share = (
            round(sum(n for _, n in top) / total, 4) if total else None
        )
        # Crude regime: first/second half by confirm_date
        dated = sorted(subset, key=lambda e: str(e.get("confirm_date") or ""))
        mid = len(dated) // 2
        early = dated[:mid]
        late = dated[mid:]

        def med4(evs: list[dict[str, Any]]) -> float | None:
            vals = []
            for e in evs:
                block = (e.get("price_outcomes") or {}).get("fwd_4w") or {}
                v = block.get("signed_return_pct")
                if v is not None:
                    vals.append(float(v))
            return None if not vals else round(_median(vals) or 0, 4)

        report[d] = {
            "n_price_eligible": total,
            "markets_represented": len(by_market),
            "top5_markets": [{"market": m, "n": n} for m, n in top],
            "top5_share_of_events": top_share,
            "dominated_by_few_markets": bool(top_share and top_share >= 0.5),
            "early_half_n": len(early),
            "late_half_n": len(late),
            "early_half_median_signed_4w": med4(early),
            "late_half_median_signed_4w": med4(late),
            "leave_one_top_market_out": {},
        }
        if top:
            top_m = top[0][0]
            rest = [e for e in subset if e["market"] != top_m]
            report[d]["leave_one_top_market_out"] = {
                "excluded_market": top_m,
                "n_remaining": len(rest),
                "median_signed_4w": med4(rest),
            }
    return report


def recommend_primitives(
    study: dict[str, Any],
    robustness: dict[str, Any],
) -> dict[str, Any]:
    """Recommend from positioning quality first; price is secondary/descriptive."""
    recs: dict[str, Any] = {}
    for d in DEFS:
        block = (study.get("by_definition") or {}).get(d, {}).get("all", {})
        pos = block.get("positioning") or {}
        a4 = (pos.get("pct_aligned_4w") or {}).get("rate")
        a8 = (pos.get("pct_aligned_8w") or {}).get("rate")
        ft = (pos.get("false_turn_rate") or {}).get("rate")
        ws = (pos.get("whipsaw_rate") or {}).get("rate")
        pers = (pos.get("persistent_reversal_rate") or {}).get("rate")
        n = pos.get("n_events") or 0
        rob = robustness.get(d) or {}
        price_n = (block.get("price_signed") or {}).get("n_events_with_price") or 0
        med4 = ((block.get("price_signed") or {}).get("fwd_4w") or {}).get(
            "median_return_pct"
        )

        # Positioning-first gates (not return-optimized)
        strong_pos = (
            n >= MIN_N_PROMOTE
            and a4 is not None
            and a8 is not None
            and a4 >= 0.55
            and a8 >= 0.52
            and (ft is None or ft <= 0.45)
            and (ws is None or ws <= 0.35)
        )
        moderate_pos = (
            n >= MIN_N_REPORT
            and a4 is not None
            and a4 >= 0.52
            and (ft is None or ft <= 0.48)
        )
        weak_pos = a4 is not None and a4 < 0.50

        if d == "A":
            # Displacement ≠ reversal by construction — expect weaker alignment
            verdict = "research_only"
            rationale = (
                "Production major_rotation measures large percentile displacement, not a "
                "directional reversal. Keep as migration context, not a turn primitive."
            )
        elif strong_pos and not rob.get("dominated_by_few_markets"):
            verdict = "promote"
            rationale = (
                f"Strong positioning follow-through (4w align={a4}, 8w align={a8}, "
                f"false_turn={ft}, whipsaw={ws}, persistent={pers}, n={n}). "
                "Price outcomes are descriptive only and were not used to select this verdict."
            )
        elif moderate_pos:
            verdict = "research_only"
            rationale = (
                f"Moderate positioning signal (4w align={a4}, n={n}). Useful for Phase 3 "
                "interaction studies; not yet a standalone trusted primitive."
            )
        elif weak_pos:
            verdict = "reject"
            rationale = (
                f"Positioning follow-through below coin-flip on 4w alignment ({a4}). "
                "Does not identify meaningful turns."
            )
        else:
            verdict = "research_only"
            rationale = f"Insufficient or mixed evidence (n={n}, 4w align={a4})."

        # Soft downgrade if severely market-dominated
        if verdict == "promote" and rob.get("dominated_by_few_markets"):
            verdict = "research_only"
            rationale += " Downgraded: top-5 markets dominate ≥50% of price-eligible sample."

        recs[d] = {
            "verdict": verdict,
            "rationale": rationale,
            "positioning_snapshot": {
                "n": n,
                "pct_aligned_4w": a4,
                "pct_aligned_8w": a8,
                "false_turn_rate": ft,
                "whipsaw_rate": ws,
                "persistent_reversal_rate": pers,
            },
            "price_snapshot_descriptive_only": {
                "n_price_eligible": price_n,
                "median_signed_4w": med4,
                "note": "Not used for verdict selection",
            },
        }

    promote = [d for d, r in recs.items() if r["verdict"] == "promote"]
    research = [d for d, r in recs.items() if r["verdict"] == "research_only"]
    reject = [d for d, r in recs.items() if r["verdict"] == "reject"]
    return {
        "by_definition": recs,
        "promote": promote,
        "research_only": research,
        "reject": reject,
        "phase3_guidance": (
            "Use promoted turning-point primitives as event onsets for multi-group "
            "configuration discovery. Keep definition A as displacement/migration context. "
            "Do not blend EX/DIV into a score yet — only descriptive interactions."
        ),
    }


def run_phase2_study(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    trustworthy = set(_load_trustworthy_markets())

    if markets is None:
        selected = sorted(str(k) for k in all_markets.keys())
    else:
        selected = list(markets)

    market_blocks: dict[str, Any] = {}
    all_events: list[dict[str, Any]] = []

    for mid in selected:
        block = all_markets.get(mid)
        if not block:
            continue
        price_ok = mid in trustworthy
        result = build_market_phase2(mid, block, price_ok=price_ok)
        market_blocks[mid] = {
            k: v for k, v in result.items() if k != "events"
        }
        if result.get("available"):
            all_events.extend(result["events"])
            # keep events in inventory file; slim market summary without full events
            market_blocks[mid]["event_count"] = len(result["events"])

    study = aggregate_study(all_events)
    robustness = robustness_checks(all_events)
    recommendations = recommend_primitives(study, robustness)

    inventory_counts: dict[str, Any] = {
        "independent_total": len(all_events),
        "by_definition": dict(Counter(e["definition"] for e in all_events)),
        "by_group": dict(Counter(e["group"] for e in all_events)),
        "by_direction": dict(Counter(e["direction"] for e in all_events)),
        "price_eligible": sum(1 for e in all_events if e.get("price_study_eligible")),
        "excluded_from_price_study": sum(
            1 for e in all_events if not e.get("price_study_eligible")
        ),
    }

    # Compact inventory for processed file (full events)
    inventory = {
        "version": "cot_intelligence_phase2_tp_inventory_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "independence": {
            "cooldown_weeks": TP_COOLDOWN_WEEKS,
            "rule": (
                "Independent if confirm_index gap >= cooldown for same "
                "(market, group, definition, direction). Earliest confirmation kept."
            ),
            "definition_D_timestamp": "confirmation_week_only",
        },
        "trustworthy_markets_for_price": sorted(trustworthy),
        "counts": inventory_counts,
        "events": all_events,
    }

    audit = {
        "version": "cot_intelligence_phase2_v1",
        "generated_at": inventory["generated_at"],
        "scope": "turning_point_validation_and_outcome_study",
        "constraints_honored": [
            "No pattern matching / intelligence score / ML / UI changes",
            "No EX/DIV threshold changes",
            "No return-based threshold optimization",
            "Copper excluded from price studies via Phase-1 trustworthy gate",
            "Definition D timestamped at confirmation week only",
            "Positioning follow-through evaluated before price returns",
        ],
        "definitions": {
            "A": f"|long_pct(t)-long_pct(t-26)| >= {ROTATION_PCT_MOVE_26W} (production major_rotation)",
            "B": f"Exit long-history zone >= {ABSOLUTE_HIGH} or <= {ABSOLUTE_LOW}",
            "C": f"4W velocity sign flip + {TP_CONFIRM_WEEKS}W confirmation",
            "D": f"Percentile peak/trough + {TP_CONFIRM_WEEKS}W confirmed reversal (confirm-week timestamp)",
        },
        "independence": inventory["independence"],
        "inventory_counts": inventory_counts,
        "markets": market_blocks,
        "study": study,
        "robustness": robustness,
        "recommendations": recommendations,
    }
    return {"audit": audit, "inventory": inventory}


def _fmt_rate(block: dict[str, Any] | None) -> str:
    if not block or block.get("rate") is None:
        return "—"
    return f"{100.0 * block['rate']:.1f}% (n={block.get('n')})"


def write_phase2_markdown(audit: dict[str, Any]) -> str:
    rec = audit["recommendations"]
    counts = audit["inventory_counts"]
    lines = [
        "# COT Intelligence — Phase 2 Turning-Point Validation",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "Scope: turning-point validation & outcome study only. "
        "No production rollout, no EX/DIV changes, no intelligence score.",
        "",
        "## Definitions",
        "",
    ]
    for k, v in audit["definitions"].items():
        lines.append(f"- **{k}**: {v}")
    lines += [
        "",
        "## Independence",
        "",
        f"- Cooldown: **{audit['independence']['cooldown_weeks']}w** per "
        "(market, group, definition, direction)",
        f"- D timestamp rule: **{audit['independence']['definition_D_timestamp']}**",
        "",
        "## Independent event inventory",
        "",
        f"- Total independent events: **{counts['independent_total']}**",
        f"- Price-eligible (23 trustworthy markets): **{counts['price_eligible']}**",
        f"- Excluded from price study: **{counts['excluded_from_price_study']}**",
        "",
        "### By definition",
        "",
    ]
    for d, n in sorted(counts["by_definition"].items()):
        lines.append(f"- {d}: {n}")
    lines += ["", "### By group", ""]
    for g, n in sorted(counts["by_group"].items()):
        lines.append(f"- {g}: {n}")
    lines += ["", "### By direction", ""]
    for g, n in sorted(counts["by_direction"].items()):
        lines.append(f"- {g}: {n}")

    lines += [
        "",
        "## Positioning follow-through (before price)",
        "",
        "| Def | n | Pct align 4W | Pct align 8W | False-turn | Whipsaw | Persistent |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]
    for d in DEFS:
        pos = (
            (audit.get("study") or {})
            .get("by_definition", {})
            .get(d, {})
            .get("all", {})
            .get("positioning", {})
        )
        lines.append(
            "| {d} | {n} | {a4} | {a8} | {ft} | {ws} | {pers} |".format(
                d=d,
                n=pos.get("n_events") or 0,
                a4=_fmt_rate(pos.get("pct_aligned_4w")),
                a8=_fmt_rate(pos.get("pct_aligned_8w")),
                ft=_fmt_rate(pos.get("false_turn_rate")),
                ws=_fmt_rate(pos.get("whipsaw_rate")),
                pers=_fmt_rate(pos.get("persistent_reversal_rate")),
            )
        )

    lines += [
        "",
        "## Forward price outcomes (signed for turn direction)",
        "",
        "Trustworthy markets only. Signed = raw return for bullish turns, "
        "negated for bearish turns. **Not used to choose definitions.**",
        "",
        "| Def | n | Med 4W | Mean 4W | %+ 4W | Med 12W | Sample |",
        "|---|---:|---:|---:|---:|---:|---|",
    ]
    for d in DEFS:
        price = (
            (audit.get("study") or {})
            .get("by_definition", {})
            .get(d, {})
            .get("all", {})
            .get("price_signed", {})
        )
        f4 = price.get("fwd_4w") or {}
        f12 = price.get("fwd_12w") or {}
        lines.append(
            "| {d} | {n} | {m4} | {mean4} | {pp} | {m12} | {sq} |".format(
                d=d,
                n=f4.get("n") or 0,
                m4=f4.get("median_return_pct"),
                mean4=f4.get("mean_return_pct"),
                pp=f4.get("pct_positive"),
                m12=f12.get("median_return_pct"),
                sq=f4.get("sample_quality"),
            )
        )

    lines += [
        "",
        "## Context study (descriptive)",
        "",
        "Turning points inside extreme / after extreme / with vs without Comm-NR divergence.",
        "",
    ]
    for d in DEFS:
        ctx = ((audit.get("study") or {}).get("context") or {}).get(d) or {}
        lines.append(f"### Definition {d}")
        lines.append("")
        lines.append("| Context | n | Pct align 4W | Med signed 4W |")
        lines.append("|---|---:|---:|---:|")
        for name, block in ctx.items():
            pos = block.get("positioning") or {}
            price = (block.get("price_signed") or {}).get("fwd_4w") or {}
            lines.append(
                f"| {name} | {block.get('n') or 0} | {_fmt_rate(pos.get('pct_aligned_4w'))} | "
                f"{price.get('median_return_pct')} |"
            )
        lines.append("")

    lines += [
        "## Robustness",
        "",
    ]
    for d in DEFS:
        rob = (audit.get("robustness") or {}).get(d) or {}
        lines.append(
            f"- **{d}**: n_price={rob.get('n_price_eligible')}, markets={rob.get('markets_represented')}, "
            f"top5_share={rob.get('top5_share_of_events')}, dominated={rob.get('dominated_by_few_markets')}, "
            f"early_med4w={rob.get('early_half_median_signed_4w')}, "
            f"late_med4w={rob.get('late_half_median_signed_4w')}"
        )

    lines += [
        "",
        "## Recommendations (no production rollout)",
        "",
        f"- **Promote**: {', '.join(rec.get('promote') or []) or 'none'}",
        f"- **Research-only**: {', '.join(rec.get('research_only') or []) or 'none'}",
        f"- **Reject**: {', '.join(rec.get('reject') or []) or 'none'}",
        "",
    ]
    for d, block in (rec.get("by_definition") or {}).items():
        lines.append(f"### {d} — `{block.get('verdict')}`")
        lines.append("")
        lines.append(block.get("rationale") or "")
        lines.append("")

    lines += [
        "## Phase 3 guidance",
        "",
        rec.get("phase3_guidance") or "",
        "",
        "## Constraints honored",
        "",
    ]
    for c in audit.get("constraints_honored") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines)


def write_phase2_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    audit = payload["audit"]
    inventory = payload["inventory"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    PHASE2_INVENTORY.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    PHASE2_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    PHASE2_MD.write_text(write_phase2_markdown(audit), encoding="utf-8")
    return {
        "inventory": PHASE2_INVENTORY,
        "audit_json": PHASE2_JSON,
        "audit_md": PHASE2_MD,
    }
