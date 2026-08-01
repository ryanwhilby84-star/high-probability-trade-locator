"""COT Intelligence Engine — Phase 1 research & data audit.

Read-only relative to the production positioning-research detector:
does NOT change EX/DIV thresholds, cooldown, or emit semantics.

Builds:
  * point-in-time weekly research table (features vs outcome labels separated)
  * price quality / unit-contamination audit
  * independent episode clustering (documented methodology)
  * turning-point candidate inventory (positioning-defined, not return-optimized)
  * audit summary JSON + markdown report
"""

from __future__ import annotations

import json
import math
import statistics
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.positioning_research_engine import (
    ABSOLUTE_HIGH,
    ABSOLUTE_LOW,
    ANALOGUE_COOLDOWN_WEEKS,
    EVENT_COOLDOWN_WEEKS,
    FORWARD_HORIZONS,
    GROUP_COMMERCIAL,
    GROUP_NONCOMMERCIAL,
    GROUP_NONREPORTABLE,
    LOCAL_HIGH,
    LOCAL_LOW,
    MIN_HISTORY,
    PRIMARY_BAND,
    RAPID_PCT_MOVE_4W,
    ROTATION_PCT_MOVE_26W,
    SPREAD_BANDS,
    _forward_path_stats,
    build_group_state_series,
    build_spread_series,
    detect_configuration_events,
)

COT3Y_PATHS = (
    PROCESSED_DIR / "cot_3y_series_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "cot_3y_series_latest.json",
    PROJECT_ROOT / "data" / "cot_3y_series_latest.json",
)
OHLC_PATHS = (
    PROCESSED_DIR / "workstation_ohlc_latest.json",
    PROJECT_ROOT / "web-dashboard" / "public" / "data" / "workstation_ohlc_latest.json",
    PROJECT_ROOT / "data" / "workstation_ohlc_latest.json",
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
RESEARCH_TABLE_PATH = PROCESSED_DIR / "cot_intelligence_phase1_research_table_latest.json"
AUDIT_JSON_PATH = AUDIT_DIR / "cot_intelligence_phase1_audit.json"
AUDIT_MD_PATH = AUDIT_DIR / "cot_intelligence_phase1_audit.md"

# Episode methodology (documented — not an optimization of returns).
# An extreme *regime* is active while long-history percentile is outside [10, 90].
# A new independent *episode* starts when the regime becomes active after being inactive,
# or after a gap of >= EVENT_COOLDOWN_WEEKS outside the zone (mirrors emit cooldown intent).
EPISODE_EXIT_GAP_WEEKS = EVENT_COOLDOWN_WEEKS
# Turning-point candidates: require sustained confirmation in weeks (positioning-only).
TP_CONFIRM_WEEKS = 3
TP_VELOCITY_SIGN_LAG = 4


def _load_first(paths: Sequence[Path]) -> dict[str, Any]:
    for p in paths:
        if p.is_file():
            return json.loads(p.read_text(encoding="utf-8"))
    return {}


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _zone(long_pct: float | None) -> str | None:
    if long_pct is None:
        return None
    if long_pct >= ABSOLUTE_HIGH:
        return "high"
    if long_pct <= ABSOLUTE_LOW:
        return "low"
    return None


def _slope_sign(change: float | None) -> str | None:
    if change is None:
        return None
    if change > 0:
        return "up"
    if change < 0:
        return "down"
    return "flat"


def _group_features(
    state: dict[str, Any],
    row: dict[str, Any],
    group: str,
    *,
    long_key: str | None,
    short_key: str | None,
) -> dict[str, Any]:
    pct = state.get("percentiles") or {}
    journey = state.get("journey") or {}
    velocity = state.get("velocity") or {}
    j26 = (journey.get("26w") or {}).get("long_history_percentile")
    long_pct = pct.get("long_history")
    v1 = (velocity.get("1w") or {}).get("net_change")
    v4 = (velocity.get("4w") or {}).get("net_change")
    v12 = (velocity.get("12w") or {}).get("net_change")
    vp4 = (velocity.get("4w") or {}).get("percentile_change")
    out: dict[str, Any] = {
        "net": state.get("net"),
        "long_history_percentile": long_pct,
        "percentile_1y": pct.get("1y"),
        "percentile_3y": pct.get("3y"),
        "percentile_5y": pct.get("5y"),
        "journey_26w_percentile": j26,
        "journey_26w_net": (journey.get("26w") or {}).get("net"),
        "change_1w_net": v1,
        "change_4w_net": v4,
        "change_12w_net": v12,
        "velocity_4w_percentile_change": vp4,
        "direction_1w": _slope_sign(v1),
        "direction_4w": _slope_sign(v4),
        "persistence": state.get("persistence"),
        "active_extreme_zone": _zone(long_pct),
        "active_local_extreme_zone": _zone(pct.get("3y")),
    }
    if long_key:
        out["long"] = _finite(row.get(long_key))
    if short_key:
        out["short"] = _finite(row.get(short_key))
    # Rotation displacement (same math as major_rotation detector; flag only).
    if long_pct is not None and j26 is not None:
        out["rotation_delta_26w"] = round(long_pct - j26, 2)
        out["major_rotation_condition"] = abs(long_pct - j26) >= ROTATION_PCT_MOVE_26W
    else:
        out["rotation_delta_26w"] = None
        out["major_rotation_condition"] = False
    return out


def _annotate_extreme_episodes(
    features: list[dict[str, Any]],
    group_key: str,
) -> None:
    """Mutate rows with episode fields for one participant group (PIT-safe)."""
    zone_key = "active_extreme_zone"
    episode_id = 0
    in_episode = False
    episode_start = None
    weeks_outside = 0
    last_zone: str | None = None

    for i, row in enumerate(features):
        g = row[group_key]
        zone = g.get(zone_key)
        newly = False
        exited = False

        if zone is not None:
            if not in_episode:
                # New episode: first entry, or re-entry after exit gap.
                episode_id += 1
                in_episode = True
                episode_start = i
                newly = True
                weeks_outside = 0
            elif last_zone is not None and zone != last_zone:
                # Flip high↔low without exiting — treat as new independent episode.
                episode_id += 1
                episode_start = i
                newly = True
            last_zone = zone
            weeks_in = i - (episode_start or i) + 1
        else:
            if in_episode:
                weeks_outside += 1
                if weeks_outside >= EPISODE_EXIT_GAP_WEEKS:
                    in_episode = False
                    exited = True
                    last_zone = None
                    weeks_outside = 0
            weeks_in = 0

        g["newly_entered_extreme"] = newly
        g["exited_extreme_this_week"] = exited and zone is None and weeks_outside == 0
        # clearer exit flag: first week outside after a run
        g["weeks_in_extreme_episode"] = weeks_in if zone is not None else 0
        g["extreme_episode_id"] = episode_id if (zone is not None and in_episode) else None
        g["extreme_episode_is_onset"] = newly


def _annotate_exit_flags(features: list[dict[str, Any]], group_key: str) -> None:
    """Mark the first week outside an extreme zone after an active episode."""
    prev_zone = None
    for row in features:
        g = row[group_key]
        zone = g.get("active_extreme_zone")
        g["exited_extreme_this_week"] = prev_zone is not None and zone is None
        prev_zone = zone


def _event_flags_for_index(events_by_index: dict[int, list[dict[str, Any]]], idx: int) -> dict[str, Any]:
    evs = events_by_index.get(idx) or []
    flags = {
        "event_count": len(evs),
        "event_types": sorted({e.get("event_type") for e in evs}),
        "event_groups": sorted({e.get("group") for e in evs}),
        "has_absolute_extreme": any(e.get("event_type") == "absolute_extreme" for e in evs),
        "has_local_extreme": any(e.get("event_type") == "local_extreme" for e in evs),
        "has_major_rotation": any(e.get("event_type") == "major_rotation" for e in evs),
        "has_comm_nr_divergence": any(e.get("event_type") == "comm_nr_divergence" for e in evs),
        "has_rapid_velocity": any(e.get("event_type") == "rapid_velocity" for e in evs),
        "has_sustained_persistence": any(e.get("event_type") == "sustained_persistence" for e in evs),
        "events": [
            {
                "event_type": e.get("event_type"),
                "group": e.get("group"),
                "side": e.get("side"),
                "label": e.get("label"),
            }
            for e in evs
        ],
    }
    return flags


def _outcome_labels(prices: list[float | None], idx: int) -> dict[str, Any]:
    """Forward outcomes — LABEL ONLY. Never use as features."""
    out: dict[str, Any] = {"schema": "outcome_label_only", "usable": False}
    any_ok = False
    for h in (1, 4, 8, 12):
        fo = _forward_path_stats(prices, idx, h)
        key = f"fwd_{h}w"
        if fo is None:
            out[key] = None
        else:
            any_ok = True
            out[key] = {
                "return_pct": fo["return_pct"],
                "mfe_pct": fo["favourable_excursion_pct"],
                "mae_pct": fo["adverse_excursion_pct"],
            }
    out["usable"] = any_ok
    return out


def _turning_point_candidates(
    states: list[dict[str, Any]],
    group: str,
) -> list[dict[str, Any]]:
    """Positioning-only turning-point candidates (no price in the definition).

    Candidates (documented alternatives — not a chosen production rule yet):
      A. major_rotation condition (existing |Δ26w pct| ≥ 40)
      B. extreme-zone exit (left high/low zone)
      C. velocity_4w percentile_change sign flip with TP_CONFIRM_WEEKS confirmation
      D. local peak/trough in long-history percentile with sustained reversal
    """
    cands: list[dict[str, Any]] = []
    n = len(states)
    long_pcts = [(s.get("percentiles") or {}).get("long_history") for s in states]
    v4s = [
        ((s.get("velocity") or {}).get("4w") or {}).get("percentile_change") for s in states
    ]

    for i in range(MIN_HISTORY - 1, n):
        s = states[i]
        long_pct = long_pcts[i]
        j26 = ((s.get("journey") or {}).get("26w") or {}).get("long_history_percentile")
        date = s.get("date")

        if long_pct is not None and j26 is not None and abs(long_pct - j26) >= ROTATION_PCT_MOVE_26W:
            cands.append(
                {
                    "date": date,
                    "index": i,
                    "group": group,
                    "definition": "A_major_rotation_26w_displacement",
                    "detail": {"delta_26w": round(long_pct - j26, 2), "long_pct": long_pct},
                }
            )

        if i > 0:
            prev_z = _zone(long_pcts[i - 1])
            cur_z = _zone(long_pct)
            if prev_z is not None and cur_z is None:
                cands.append(
                    {
                        "date": date,
                        "index": i,
                        "group": group,
                        "definition": "B_exit_extreme_zone",
                        "detail": {"prior_zone": prev_z, "long_pct": long_pct},
                    }
                )

        # C: velocity sign flip with confirmation
        if i >= TP_VELOCITY_SIGN_LAG + TP_CONFIRM_WEEKS:
            a = v4s[i - TP_CONFIRM_WEEKS]
            b = v4s[i]
            if a is not None and b is not None and a != 0 and b != 0 and (a > 0) != (b > 0):
                # confirm subsequent weeks keep new sign
                ok = True
                for k in range(i - TP_CONFIRM_WEEKS + 1, i + 1):
                    vk = v4s[k]
                    if vk is None or (vk > 0) != (b > 0):
                        ok = False
                        break
                if ok:
                    cands.append(
                        {
                            "date": date,
                            "index": i,
                            "group": group,
                            "definition": "C_velocity_4w_sign_flip_confirmed",
                            "detail": {"v4_before": a, "v4_after": b},
                        }
                    )

        # D: local percentile peak/trough then sustained move away (≥ confirm weeks)
        if i >= 2 and i + TP_CONFIRM_WEEKS < n and long_pct is not None:
            window = long_pcts[i - 2 : i + 1]
            if all(x is not None for x in window):
                is_peak = window[1] >= window[0] and window[1] >= window[2]  # type: ignore[operator]
                is_trough = window[1] <= window[0] and window[1] <= window[2]  # type: ignore[operator]
                if is_peak or is_trough:
                    pivot = window[1]
                    future = long_pcts[i + 1 : i + 1 + TP_CONFIRM_WEEKS]
                    if all(x is not None for x in future):
                        if is_peak and all(x < pivot for x in future):  # type: ignore[operator]
                            cands.append(
                                {
                                    "date": states[i - 1]["date"],
                                    "index": i - 1,
                                    "group": group,
                                    "definition": "D_percentile_peak_then_reversal",
                                    "detail": {"pivot_pct": pivot, "confirm_weeks": TP_CONFIRM_WEEKS},
                                }
                            )
                        if is_trough and all(x > pivot for x in future):  # type: ignore[operator]
                            cands.append(
                                {
                                    "date": states[i - 1]["date"],
                                    "index": i - 1,
                                    "group": group,
                                    "definition": "D_percentile_trough_then_reversal",
                                    "detail": {"pivot_pct": pivot, "confirm_weeks": TP_CONFIRM_WEEKS},
                                }
                            )

    # Dedup identical definition+index
    seen: set[tuple[Any, ...]] = set()
    uniq: list[dict[str, Any]] = []
    for c in cands:
        key = (c["definition"], c["index"], c["group"])
        if key in seen:
            continue
        seen.add(key)
        uniq.append(c)
    return uniq


def audit_price_series(
    market: str,
    series: list[dict[str, Any]],
    ohlc_block: dict[str, Any] | None,
) -> dict[str, Any]:
    """Flag unit contamination / gaps — do not silently repair."""
    closes = [_finite(r.get("price")) for r in series]
    present = [c for c in closes if c is not None]
    flags: list[str] = []

    if len(present) < MIN_HISTORY:
        flags.append("insufficient_cot_price_points")

    # Regime mixing: low-scale ($/lb FX metals) vs high-scale (index / HG×1000 / tonne)
    low = [c for c in present if c < 50]
    high = [c for c in present if c > 200]
    if low and high:
        flags.append("mixed_price_scale_regimes_in_cot3y_price")

    # Intra-adjacent jump: week-to-week ratio explosion (unit flip)
    jump_weeks = 0
    for a, b in zip(present, present[1:]):
        if a and b and a > 0 and (b / a > 20 or a / b > 20):
            jump_weeks += 1
    if jump_weeks:
        flags.append(f"extreme_week_to_week_jumps={jump_weeks}")

    missing = sum(1 for c in closes if c is None)
    missing_pct = round(100.0 * missing / max(len(closes), 1), 2)

    # workstation OHLC audit if present
    ohlc_flags: list[str] = []
    ohlc_meta: dict[str, Any] = {}
    if ohlc_block:
        bars = ohlc_block.get("weekly_ohlc") or []
        ohlc_closes = [_finite(b.get("close")) for b in bars]
        ohlc_present = [c for c in ohlc_closes if c is not None]
        o_low = [c for c in ohlc_present if c < 50]
        o_high = [c for c in ohlc_present if c > 200]
        if o_low and o_high:
            ohlc_flags.append("mixed_price_scale_regimes_in_workstation_ohlc")
        mixed_bars = 0
        for b in bars:
            o, h, l, c = _finite(b.get("open")), _finite(b.get("high")), _finite(b.get("low")), _finite(b.get("close"))
            if None in (o, h, l, c):
                continue
            if h / max(l, 1e-12) > 2.5:  # type: ignore[operator]
                mixed_bars += 1
        if mixed_bars:
            ohlc_flags.append(f"intra_bar_mixed_unit_weeks={mixed_bars}")
        ohlc_meta = {
            "weekly_bars": len(bars),
            "aligned_bars": len(ohlc_block.get("aligned_weekly_ohlc") or []),
            "price_source": ohlc_block.get("price_source"),
            "canonical_symbol": ohlc_block.get("canonical_symbol"),
            "incomplete_history": ohlc_block.get("incomplete_history"),
            "missing_ohlc_weeks": ohlc_block.get("missing_ohlc_weeks"),
            "common_first": ohlc_block.get("common_first_date"),
            "common_last": ohlc_block.get("common_last_date"),
            "price_quality": ohlc_block.get("price_quality"),
            "flags": ohlc_flags,
        }

    trustworthy = (
        len(present) >= MIN_HISTORY
        and "mixed_price_scale_regimes_in_cot3y_price" not in flags
        and jump_weeks == 0
        and missing_pct < 25
    )

    return {
        "market": market,
        "cot3y_weeks": len(series),
        "price_points": len(present),
        "missing_price_weeks": missing,
        "missing_price_pct": missing_pct,
        "close_min": None if not present else round(min(present), 6),
        "close_max": None if not present else round(max(present), 6),
        "close_median": None if not present else round(float(statistics.median(present)), 6),
        "flags": flags,
        "workstation_ohlc": ohlc_meta,
        "trustworthy_for_outcome_labels": trustworthy,
        "notes": (
            "cot_3y price is as-of COT date close (canonical_daily_match). "
            "Forward outcomes in the research table use this series — not workstation OHLC sanitization."
        ),
    }


def build_market_research_table(
    market: str,
    block: dict[str, Any],
    ohlc_block: dict[str, Any] | None = None,
) -> dict[str, Any]:
    series = list(block.get("series") or [])
    if len(series) < MIN_HISTORY:
        return {
            "market": market,
            "available": False,
            "reason": "insufficient_history",
            "weeks": len(series),
        }

    commercial = build_group_state_series(series, GROUP_COMMERCIAL)
    noncommercial = build_group_state_series(series, GROUP_NONCOMMERCIAL)
    nonreportable = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(commercial, nonreportable)
    events = detect_configuration_events(commercial, noncommercial, nonreportable, spreads)
    events_by_index: dict[int, list[dict[str, Any]]] = {}
    for e in events:
        events_by_index.setdefault(int(e["index"]), []).append(e)

    prices = [_finite(r.get("price")) for r in series]
    rows: list[dict[str, Any]] = []

    for i, row in enumerate(series):
        c, nc, nr, sp = commercial[i], noncommercial[i], nonreportable[i], spreads[i]
        spread_pct = sp.get("spread_percentile")
        spread_prev = spreads[i - 1].get("spread_percentile") if i > 0 else None
        expanding = None
        if spread_pct is not None and spread_prev is not None:
            if spread_pct > spread_prev:
                expanding = "expanding"
            elif spread_pct < spread_prev:
                expanding = "contracting"
            else:
                expanding = "flat"

        # PIT feature block
        feat = {
            "date": str(row.get("date") or "")[:10],
            "index": i,
            "market": market,
            "features": {
                "commercial": _group_features(
                    c, row, GROUP_COMMERCIAL, long_key=None, short_key=None
                ),
                "noncommercial": _group_features(
                    nc,
                    row,
                    GROUP_NONCOMMERCIAL,
                    long_key="institutional_long",
                    short_key="institutional_short",
                ),
                "nonreportable": _group_features(
                    nr,
                    row,
                    GROUP_NONREPORTABLE,
                    long_key="retail_long",
                    short_key="retail_short",
                ),
                "spread": {
                    "value": sp.get("spread"),
                    "percentile": spread_pct,
                    "commercial_percentile": sp.get("commercial_percentile"),
                    "nonreportable_percentile": sp.get("nonreportable_percentile"),
                    "divergence_state": expanding,
                    "formula": sp.get("formula"),
                    "primary_band_hit": (
                        None
                        if spread_pct is None
                        else (
                            "high"
                            if spread_pct >= 90
                            else ("low" if spread_pct <= 10 else None)
                        )
                    ),
                },
                "open_interest": _finite(row.get("open_interest")),
            },
            "event_flags": _event_flags_for_index(events_by_index, i),
            # SEPARATED — never feed into a signal feature matrix for the same date
            "outcome_labels": _outcome_labels(prices, i),
        }
        rows.append(feat)

    # Episode annotations (mutate feature blocks)
    for gk in ("commercial", "noncommercial", "nonreportable"):
        tmp = [{gk: r["features"][gk]} for r in rows]
        _annotate_extreme_episodes(tmp, gk)
        _annotate_exit_flags(tmp, gk)
        for r, t in zip(rows, tmp):
            r["features"][gk] = t[gk]

    # Episode counts
    def episode_count(group: str) -> int:
        ids = {
            r["features"][group].get("extreme_episode_id")
            for r in rows
            if r["features"][group].get("extreme_episode_id") is not None
        }
        return len(ids)

    tp = {
        GROUP_COMMERCIAL: _turning_point_candidates(commercial, GROUP_COMMERCIAL),
        GROUP_NONCOMMERCIAL: _turning_point_candidates(noncommercial, GROUP_NONCOMMERCIAL),
        GROUP_NONREPORTABLE: _turning_point_candidates(nonreportable, GROUP_NONREPORTABLE),
    }
    tp_counts = {
        g: {
            "total_candidates": len(cs),
            "by_definition": {
                d: sum(1 for c in cs if c["definition"] == d)
                for d in sorted({c["definition"] for c in cs})
            },
        }
        for g, cs in tp.items()
    }

    price_audit = audit_price_series(market, series, ohlc_block)

    by_type: dict[str, int] = {}
    by_group: dict[str, int] = {}
    for e in events:
        by_type[str(e.get("event_type"))] = by_type.get(str(e.get("event_type")), 0) + 1
        by_group[str(e.get("group"))] = by_group.get(str(e.get("group")), 0) + 1

    return {
        "market": market,
        "available": True,
        "weeks": len(rows),
        "source_week": rows[-1]["date"] if rows else None,
        "event_counts": {"total": len(events), "by_type": by_type, "by_group": by_group},
        "independent_extreme_episodes": {
            "methodology": (
                f"Episode onset = enter absolute extreme zone (long-history pct "
                f">={ABSOLUTE_HIGH} or <={ABSOLUTE_LOW}) after inactivity, or high/low flip. "
                f"Episode ends after {EPISODE_EXIT_GAP_WEEKS} consecutive weeks outside the zone "
                f"(aligned with EVENT_COOLDOWN_WEEKS={EVENT_COOLDOWN_WEEKS}). "
                "Ten consecutive weeks in-zone = one episode, not ten independent samples."
            ),
            "commercial": episode_count("commercial"),
            "noncommercial": episode_count("noncommercial"),
            "nonreportable": episode_count("nonreportable"),
        },
        "turning_point_candidates": {
            "note": (
                "Positioning-only definitions A–D. Not selected by forward returns. "
                "Existing production major_rotation = definition A."
            ),
            "confirm_weeks": TP_CONFIRM_WEEKS,
            "counts": tp_counts,
            # keep sample of candidates small in market summary; full list in rows not duplicated
            "sample": {
                g: cs[:5]
                for g, cs in tp.items()
            },
        },
        "price_audit": price_audit,
        "rows": rows,
        "schema": RESEARCH_TABLE_SCHEMA,
    }


RESEARCH_TABLE_SCHEMA = {
    "version": "phase1_v1",
    "point_in_time": True,
    "feature_namespace": "features.*",
    "label_namespace": "outcome_labels.*",
    "leakage_rule": (
        "outcome_labels must never be used as model/signal inputs for the same row date. "
        "Percentiles are expanding/rolling including current week only (no future weeks)."
    ),
    "groups": ["commercial", "noncommercial", "nonreportable"],
    "forward_horizons_weeks": [1, 4, 8, 12],
    "existing_detector_thresholds_unchanged": {
        "ABSOLUTE_HIGH": ABSOLUTE_HIGH,
        "ABSOLUTE_LOW": ABSOLUTE_LOW,
        "LOCAL_HIGH": LOCAL_HIGH,
        "LOCAL_LOW": LOCAL_LOW,
        "ROTATION_PCT_MOVE_26W": ROTATION_PCT_MOVE_26W,
        "RAPID_PCT_MOVE_4W": RAPID_PCT_MOVE_4W,
        "EVENT_COOLDOWN_WEEKS": EVENT_COOLDOWN_WEEKS,
        "PRIMARY_BAND": PRIMARY_BAND,
        "SPREAD_BANDS": SPREAD_BANDS,
        "ANALOGUE_COOLDOWN_WEEKS": ANALOGUE_COOLDOWN_WEEKS,
    },
}


def _detector_documentation() -> dict[str, Any]:
    return {
        "source": "src/hptl/cot/positioning_research_engine.py::detect_configuration_events",
        "pit_safe": True,
        "pit_rationale": (
            "Expanding percentiles append current then rank; rolling windows end at i; "
            "journey/velocity use lag values at i−lag; cooldown uses only past emits; "
            "loop starts at MIN_HISTORY-1. Verified by test_no_lookahead_in_expanding_percentile."
        ),
        "events": [
            {
                "name": "Commercial absolute extreme",
                "trigger": f"commercial expanding long-history percentile ≥ {ABSOLUTE_HIGH} or ≤ {ABSOLUTE_LOW}",
                "lookback": "expanding history from series start through week t (min 2 observations)",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per kind (bull/bear separate)",
                "pit_safe": True,
            },
            {
                "name": "Commercial local extreme",
                "trigger": (
                    f"commercial rolling 3Y (156w) percentile ≥ {LOCAL_HIGH} or ≤ {LOCAL_LOW}, "
                    f"and not already absolute on that side"
                ),
                "lookback": "rolling 156 weeks through t",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per kind",
                "pit_safe": True,
            },
            {
                "name": "Non-Commercial absolute extreme",
                "trigger": f"NC expanding long-history percentile ≥ {ABSOLUTE_HIGH} or ≤ {ABSOLUTE_LOW}",
                "lookback": "expanding (NC institutional_net)",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per kind",
                "pit_safe": True,
            },
            {
                "name": "Non-Commercial local extreme",
                "trigger": "NC 3Y rolling percentile extreme when not absolute (same thresholds)",
                "lookback": "rolling 156w",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per kind",
                "pit_safe": True,
            },
            {
                "name": "Non-Reportable events",
                "trigger": (
                    "No absolute/local EX emits for NR. NR participates via major_rotation "
                    f"(|Δ26w long-history pct| ≥ {ROTATION_PCT_MOVE_26W}) and as the NR leg of Comm↔NR divergence."
                ),
                "lookback": "expanding + 26w journey lag",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per kind",
                "pit_safe": True,
            },
            {
                "name": "Comm↔NR divergence",
                "trigger": (
                    "spread = comm.long_history_pct − nr.long_history_pct; "
                    f"expanding percentile of spread ≥90 or ≤10 (primary band {PRIMARY_BAND})"
                ),
                "lookback": "expanding history of spread values through t",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per high/low kind",
                "pit_safe": True,
            },
            {
                "name": "major_rotation (Comm / NC / NR)",
                "trigger": (
                    f"|long_history_pct(t) − long_history_pct(t−26)| ≥ {ROTATION_PCT_MOVE_26W}. "
                    "This is percentile displacement, not an explicit direction-reversal test."
                ),
                "lookback": "26-week lag of expanding percentile",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w per group kind",
                "pit_safe": True,
                "turning_point_assessment": (
                    "Identifies large migration in normalized positioning, not necessarily a "
                    "local peak/trough or velocity sign change. Phase 2 should compare definitions A–D."
                ),
            },
            {
                "name": "rapid_velocity (Commercial only)",
                "trigger": f"|4w percentile_change| ≥ {RAPID_PCT_MOVE_4W}",
                "lookback": "4w lag of expanding percentile",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w",
                "pit_safe": True,
                "in_ui_markers": False,
            },
            {
                "name": "sustained_persistence (Commercial only)",
                "trigger": (
                    "≥8 consecutive same-sign 1W net changes AND |long_pct − pct_26w| ≥ 15"
                ),
                "lookback": "persistence walk + 26w journey",
                "cooldown": f"{EVENT_COOLDOWN_WEEKS}w",
                "pit_safe": True,
                "in_ui_markers": False,
            },
        ],
        "lookahead_risks": [
            {
                "risk": "Forward returns / MFE / MAE in analogues and outcome_labels",
                "status": "LABEL ONLY — must not enter feature matrix for date t",
            },
            {
                "risk": "Turning-point definition D uses TP_CONFIRM_WEEKS of future percentile path",
                "status": (
                    "Research candidate only; not production. If promoted, signal date must be "
                    f"shifted forward by {TP_CONFIRM_WEEKS} weeks (confirmation lag)."
                ),
            },
            {
                "risk": "Workstation OHLC client-side unit coercion",
                "status": (
                    "Chart-only sanitization; does not alter cot_3y research prices. "
                    "Flagged contamination must be repaired upstream, not silently trusted."
                ),
            },
        ],
    }


def run_phase1_audit(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    cot3y = _load_first(COT3Y_PATHS)
    ohlc_doc = _load_first(OHLC_PATHS)
    all_markets = cot3y.get("markets") or {}
    ohlc_instruments = ohlc_doc.get("instruments") or {}

    if markets is None:
        selected = sorted(str(k) for k in all_markets.keys())
    else:
        selected = list(markets)

    market_results: dict[str, Any] = {}
    # Compact table for export: drop full rows from summary; write rows separately
    table_markets: dict[str, Any] = {}

    for mid in selected:
        block = all_markets.get(mid)
        if not block:
            market_results[mid] = {"market": mid, "available": False, "reason": "not_found"}
            continue
        ohlc_block = ohlc_instruments.get(mid) or ohlc_instruments.get(
            mid.split(" / ")[0] if " / " in mid else mid
        )
        result = build_market_research_table(mid, block, ohlc_block)
        # Store full table market (with rows)
        table_markets[mid] = result
        # Slim audit view
        slim = {k: v for k, v in result.items() if k != "rows"}
        if result.get("available"):
            slim["row_count"] = len(result.get("rows") or [])
            usable_outcomes = sum(
                1 for r in result["rows"] if (r.get("outcome_labels") or {}).get("usable")
            )
            slim["weeks_with_usable_outcome_labels"] = usable_outcomes
        market_results[mid] = slim

    available = [m for m, r in market_results.items() if r.get("available")]
    trustworthy = [
        m
        for m, r in market_results.items()
        if r.get("available") and (r.get("price_audit") or {}).get("trustworthy_for_outcome_labels")
    ]
    contaminated = [
        {
            "market": m,
            "flags": (r.get("price_audit") or {}).get("flags"),
            "ohlc_flags": ((r.get("price_audit") or {}).get("workstation_ohlc") or {}).get("flags"),
        }
        for m, r in market_results.items()
        if r.get("available")
        and (
            (r.get("price_audit") or {}).get("flags")
            or ((r.get("price_audit") or {}).get("workstation_ohlc") or {}).get("flags")
        )
    ]

    # Aggregate event / episode counts
    agg_events: dict[str, int] = {}
    agg_episodes = {"commercial": 0, "noncommercial": 0, "nonreportable": 0}
    for r in market_results.values():
        if not r.get("available"):
            continue
        for t, n in ((r.get("event_counts") or {}).get("by_type") or {}).items():
            agg_events[t] = agg_events.get(t, 0) + int(n)
        ep = r.get("independent_extreme_episodes") or {}
        for g in agg_episodes:
            agg_episodes[g] += int(ep.get(g) or 0)

    phase2_recommendations = {
        "research_table": (
            "Use features.* only for configuration discovery; hold out outcome_labels.* "
            "until after a configuration is declared from positioning alone."
        ),
        "episodes": (
            "Score configurations on independent extreme_episode_id onsets (and divergence "
            "episode onsets), not every in-zone week."
        ),
        "turning_points": (
            "Compare definitions A–D on positioning stability first; only then measure "
            "outcome_labels. Do not pick the definition with the best backtested returns."
        ),
        "price_data": (
            "Repair or quarantine markets with mixed_price_scale_regimes before trusting "
            "outcome studies. Copper is the known case; treat any flagged market similarly."
        ),
        "validation": (
            "Pre-declare bands/thresholds (already done for spreads). Use walk-forward / "
            "out-of-sample splits in Phase 4 — not in Phase 1."
        ),
    }

    audit = {
        "version": "cot_intelligence_phase1_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "research_and_data_integrity_only",
        "detector_documentation": _detector_documentation(),
        "research_table_schema": RESEARCH_TABLE_SCHEMA,
        "summary": {
            "markets_in_source": len(all_markets),
            "markets_built": len(available),
            "markets_trustworthy_price_for_outcomes": len(trustworthy),
            "trustworthy_markets": trustworthy,
            "markets_with_price_flags": contaminated,
            "aggregate_event_counts": agg_events,
            "aggregate_independent_extreme_episodes": agg_episodes,
        },
        "markets": market_results,
        "phase2_recommendations": phase2_recommendations,
        "constraints_honored": [
            "No threshold optimization",
            "No bullish/bearish intelligence score",
            "No ML model",
            "No UI redesign",
            "No change to existing EX/DIV detection logic",
            "Forward returns stored only under outcome_labels",
        ],
    }

    table_doc = {
        "version": "cot_intelligence_phase1_research_table_v1",
        "generated_at": audit["generated_at"],
        "schema": RESEARCH_TABLE_SCHEMA,
        "detector_documentation": audit["detector_documentation"],
        "markets": table_markets,
        "summary": audit["summary"],
    }

    return {"audit": audit, "research_table": table_doc}


def _write_markdown(audit: dict[str, Any]) -> str:
    det = audit["detector_documentation"]
    summary = audit["summary"]
    lines = [
        "# COT Intelligence Engine — Phase 1 Audit Report",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "Scope: **research & data integrity only**. No production intelligence score, "
        "no threshold changes, no UI redesign.",
        "",
        "## 1. Existing event system (unchanged)",
        "",
        f"Source: `{det['source']}`",
        "",
        f"Point-in-time safe: **{det['pit_safe']}** — {det['pit_rationale']}",
        "",
        "| Event | Trigger | Lookback | Cooldown | PIT-safe |",
        "|---|---|---|---|---|",
    ]
    for e in det["events"]:
        lines.append(
            f"| {e['name']} | {e['trigger']} | {e['lookback']} | {e['cooldown']} | {e['pit_safe']} |"
        )

    lines += [
        "",
        "### Look-ahead / leakage risks",
        "",
    ]
    for r in det["lookahead_risks"]:
        lines.append(f"- **{r['risk']}**: {r['status']}")

    lines += [
        "",
        "## 2. Research table",
        "",
        "Written to `data/processed/cot_intelligence_phase1_research_table_latest.json`.",
        "",
        "- Features under `features.*` (PIT)",
        "- Labels under `outcome_labels.*` (1W/4W/8W/12W return + MFE/MAE)",
        "- Event flags from the **existing** detector (cooldown-aware emits)",
        "- Extreme episode IDs for independent-sample discipline",
        "",
        "## 3. Market coverage & price quality",
        "",
        f"- Markets built: **{summary['markets_built']}** / {summary['markets_in_source']}",
        f"- Trustworthy for outcome labels: **{summary['markets_trustworthy_price_for_outcomes']}**",
        "",
        "### Trustworthy markets",
        "",
    ]
    for m in summary["trustworthy_markets"]:
        lines.append(f"- {m}")

    lines += ["", "### Markets with price-quality flags (not silently repaired)", ""]
    if not summary["markets_with_price_flags"]:
        lines.append("- None flagged.")
    else:
        for item in summary["markets_with_price_flags"]:
            lines.append(
                f"- **{item['market']}**: cot3y={item.get('flags')}, ohlc={item.get('ohlc_flags')}"
            )

    lines += [
        "",
        "## 4. Aggregate event vs independent episode counts",
        "",
        "### Detector emits (cooldown-aware)",
        "",
    ]
    for t, n in sorted(summary["aggregate_event_counts"].items(), key=lambda x: -x[1]):
        lines.append(f"- `{t}`: {n}")

    lines += [
        "",
        "### Independent absolute-extreme episodes (regime clustering)",
        "",
    ]
    for g, n in summary["aggregate_independent_extreme_episodes"].items():
        lines.append(f"- {g}: {n}")

    lines += [
        "",
        "## 5. Turning-point assessment",
        "",
        "Existing `major_rotation` = **percentile displacement** "
        f"(|Δ 26W long-history pct| ≥ {ROTATION_PCT_MOVE_26W}), available for Comm/NC/NR.",
        "",
        "It does **not** by itself prove a directional reversal. Phase 1 inventories "
        "candidate definitions A–D (rotation displacement, extreme exit, confirmed velocity "
        "sign flip, percentile peak/trough + sustained reversal) without selecting by returns.",
        "",
        "Definition D uses confirmation weeks and must be lagged if promoted to a live signal.",
        "",
        "## 6. Recommended Phase 2 methodology",
        "",
    ]
    for k, v in audit["phase2_recommendations"].items():
        lines.append(f"- **{k}**: {v}")

    lines += [
        "",
        "## Constraints honored",
        "",
    ]
    for c in audit["constraints_honored"]:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines)


def write_phase1_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    audit = payload["audit"]
    table = payload["research_table"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    RESEARCH_TABLE_PATH.write_text(json.dumps(table, indent=2), encoding="utf-8")
    AUDIT_JSON_PATH.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    md = _write_markdown(audit)
    AUDIT_MD_PATH.write_text(md, encoding="utf-8")

    # Also copy audit md/json pointers into public data notes? No — keep research-only under data/.
    return {
        "research_table": RESEARCH_TABLE_PATH,
        "audit_json": AUDIT_JSON_PATH,
        "audit_md": AUDIT_MD_PATH,
    }
