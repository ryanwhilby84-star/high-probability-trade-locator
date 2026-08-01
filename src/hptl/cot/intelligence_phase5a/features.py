"""Point-in-time pre-move behavioural features and discrete stage sequences."""

from __future__ import annotations

import math
from typing import Any

import numpy as np
import pandas as pd

from hptl.cot.intelligence_phase5a.config import (
    CHANGE_LAGS,
    EXTREME_HIGH,
    EXTREME_LOW,
    GROUPS,
    INTENSITY_TAIL_PCT,
    LOOKBACK_WEEKS,
    MIN_HISTORY_WEEKS,
    SLOPE_WINDOWS,
)
from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    GROUP_NONCOMMERCIAL,
    GROUP_NONREPORTABLE,
    GROUP_NET_KEY,
    build_group_state_series,
    _finite,
)


def _slope(ys: list[float]) -> float | None:
    if len(ys) < 2:
        return None
    x = np.arange(len(ys), dtype=float)
    y = np.asarray(ys, dtype=float)
    if not np.all(np.isfinite(y)):
        return None
    # simple OLS slope
    xm, ym = x.mean(), y.mean()
    den = float(((x - xm) ** 2).sum())
    if den <= 0:
        return None
    return float(((x - xm) * (y - ym)).sum() / den)


def _sign(v: float | None) -> int:
    if v is None or not math.isfinite(v) or v == 0:
        return 0
    return 1 if v > 0 else -1


def build_market_panel(series: list[dict[str, Any]]) -> dict[str, Any]:
    """Build PIT group state arrays for one market series."""
    commercial = build_group_state_series(series, GROUP_COMMERCIAL)
    nc = build_group_state_series(series, GROUP_NONCOMMERCIAL)
    nr = build_group_state_series(series, GROUP_NONREPORTABLE)
    dates = [str(r.get("date") or "")[:10] for r in series]
    prices = [_finite(r.get("price")) for r in series]
    nets = {
        "commercial": [_finite(r.get(GROUP_NET_KEY[GROUP_COMMERCIAL])) for r in series],
        "noncommercial": [_finite(r.get(GROUP_NET_KEY[GROUP_NONCOMMERCIAL])) for r in series],
        "nonreportable": [_finite(r.get(GROUP_NET_KEY[GROUP_NONREPORTABLE])) for r in series],
    }
    pcts = {
        "commercial": [
            (s.get("percentiles") or {}).get("long_history") for s in commercial
        ],
        "noncommercial": [(s.get("percentiles") or {}).get("long_history") for s in nc],
        "nonreportable": [(s.get("percentiles") or {}).get("long_history") for s in nr],
    }
    # Historical weekly Δnet distributions for intensity (expanding through i)
    weekly_dnet: dict[str, list[float | None]] = {g: [None] * len(series) for g in GROUPS}
    for g in GROUPS:
        for i in range(1, len(series)):
            a, b = nets[g][i - 1], nets[g][i]
            if a is None or b is None:
                continue
            weekly_dnet[g][i] = b - a

    return {
        "dates": dates,
        "prices": prices,
        "nets": nets,
        "pcts": pcts,
        "weekly_dnet": weekly_dnet,
        "states": {
            "commercial": commercial,
            "noncommercial": nc,
            "nonreportable": nr,
        },
    }


def _group_features(
    panel: dict[str, Any],
    group: str,
    onset: int,
    *,
    prefix: str,
) -> dict[str, Any]:
    nets = panel["nets"][group]
    pcts = panel["pcts"][group]
    dnets = panel["weekly_dnet"][group]
    n = len(nets)
    if onset < MIN_HISTORY_WEEKS or onset >= n:
        return {}

    cur_net = nets[onset]
    cur_pct = pcts[onset]
    out: dict[str, Any] = {
        f"{prefix}_net": cur_net,
        f"{prefix}_pct": cur_pct,
        f"{prefix}_dist_from_90": None if cur_pct is None else round(float(cur_pct) - EXTREME_HIGH, 4),
        f"{prefix}_dist_from_10": None if cur_pct is None else round(float(cur_pct) - EXTREME_LOW, 4),
    }

    # 52w high/low distance on net (PIT)
    lo = max(0, onset - 51)
    window_nets = [v for v in nets[lo : onset + 1] if v is not None]
    if window_nets and cur_net is not None:
        out[f"{prefix}_dist_52w_high"] = round(cur_net - max(window_nets), 4)
        out[f"{prefix}_dist_52w_low"] = round(cur_net - min(window_nets), 4)
    else:
        out[f"{prefix}_dist_52w_high"] = None
        out[f"{prefix}_dist_52w_low"] = None

    for lag in CHANGE_LAGS:
        j = onset - lag
        if j < 0:
            out[f"{prefix}_net_chg_{lag}w"] = None
            out[f"{prefix}_pct_chg_{lag}w"] = None
        else:
            a, b = nets[j], nets[onset]
            pa, pb = pcts[j], pcts[onset]
            out[f"{prefix}_net_chg_{lag}w"] = (
                None if a is None or b is None else round(b - a, 4)
            )
            out[f"{prefix}_pct_chg_{lag}w"] = (
                None if pa is None or pb is None else round(float(pb) - float(pa), 4)
            )

    for w in SLOPE_WINDOWS:
        seg = [pcts[i] for i in range(max(0, onset - w + 1), onset + 1)]
        seg_f = [float(v) for v in seg if v is not None]
        out[f"{prefix}_slope_{w}w"] = None if len(seg_f) < 3 else round(_slope(seg_f) or 0.0, 6)
        rises = falls = 0
        for i in range(max(1, onset - w + 1), onset + 1):
            a, b = pcts[i - 1], pcts[i]
            if a is None or b is None:
                continue
            if b > a:
                rises += 1
            elif b < a:
                falls += 1
        out[f"{prefix}_rising_weeks_{w}w"] = rises
        out[f"{prefix}_falling_weeks_{w}w"] = falls

    # Intensity vs expanding historical weekly Δnet
    hist = [v for v in dnets[: onset + 1] if v is not None]
    cur_d = dnets[onset]
    if hist and cur_d is not None and len(hist) >= 20:
        p_hi = float(np.nanpercentile(hist, 100 - INTENSITY_TAIL_PCT))
        p_lo = float(np.nanpercentile(hist, INTENSITY_TAIL_PCT))
        scale = float(np.nanstd(hist)) or 1.0
        out[f"{prefix}_dnet_z"] = round(cur_d / scale, 4)
        out[f"{prefix}_dnet_top5"] = bool(cur_d >= p_hi)
        out[f"{prefix}_dnet_bot5"] = bool(cur_d <= p_lo)
    else:
        out[f"{prefix}_dnet_z"] = None
        out[f"{prefix}_dnet_top5"] = False
        out[f"{prefix}_dnet_bot5"] = False

    for w in (4, 8, 12):
        seg = [v for v in dnets[max(1, onset - w + 1) : onset + 1] if v is not None]
        out[f"{prefix}_largest_weekly_shift_{w}w"] = (
            None if not seg else round(max(seg, key=abs), 4)
        )

    # Extreme zone behaviour over 12w
    w = 12
    start = max(0, onset - w + 1)
    above = below = 0
    entered_hi = entered_lo = exited_hi = exited_lo = False
    remained_hi = remained_lo = False
    flat_while_ext = False
    for i in range(start, onset + 1):
        p = pcts[i]
        if p is None:
            continue
        if p >= EXTREME_HIGH:
            above += 1
        if p <= EXTREME_LOW:
            below += 1
        if i > start:
            prev = pcts[i - 1]
            if prev is not None:
                if prev < EXTREME_HIGH <= p:
                    entered_hi = True
                if prev > EXTREME_LOW >= p:
                    entered_lo = True
                if prev >= EXTREME_HIGH > p:
                    exited_hi = True
                if prev <= EXTREME_LOW < p:
                    exited_lo = True
    if cur_pct is not None:
        remained_hi = cur_pct >= EXTREME_HIGH and above >= 4
        remained_lo = cur_pct <= EXTREME_LOW and below >= 4
        # flatten while extreme: |pct chg 4w| small while extreme
        ch4 = out.get(f"{prefix}_pct_chg_4w")
        if cur_pct >= EXTREME_HIGH or cur_pct <= EXTREME_LOW:
            if ch4 is not None and abs(float(ch4)) < 3.0:
                flat_while_ext = True

    out[f"{prefix}_weeks_above_90_12w"] = above
    out[f"{prefix}_weeks_below_10_12w"] = below
    out[f"{prefix}_entered_extreme_12w"] = int(entered_hi or entered_lo)
    out[f"{prefix}_exit_extreme_12w"] = int(exited_hi or exited_lo)
    out[f"{prefix}_remained_extreme_12w"] = int(remained_hi or remained_lo)
    out[f"{prefix}_flatten_while_extreme"] = int(flat_while_ext)

    # Velocity / derivative sign flip in prior 8w
    flip = 0
    signs = []
    for i in range(max(1, onset - 7), onset + 1):
        d = dnets[i]
        signs.append(_sign(d))
    for a, b in zip(signs, signs[1:]):
        if a != 0 and b != 0 and a != b:
            flip = 1
            break
    out[f"{prefix}_velocity_sign_flip_8w"] = flip

    # Acceleration: |Δnet| last 4w vs prior 4w
    recent = [v for v in dnets[max(1, onset - 3) : onset + 1] if v is not None]
    prior = [v for v in dnets[max(1, onset - 7) : max(1, onset - 3)] if v is not None]
    if recent and prior:
        out[f"{prefix}_accel_4w"] = round(
            (sum(abs(x) for x in recent) / len(recent))
            - (sum(abs(x) for x in prior) / len(prior)),
            4,
        )
    else:
        out[f"{prefix}_accel_4w"] = None

    return out


def _cross_group_features(panel: dict[str, Any], onset: int) -> dict[str, Any]:
    c_pct = panel["pcts"]["commercial"]
    nc_pct = panel["pcts"]["noncommercial"]
    nr_pct = panel["pcts"]["nonreportable"]
    c_net = panel["nets"]["commercial"]
    nc_net = panel["nets"]["noncommercial"]

    out: dict[str, Any] = {}
    if onset < 1:
        return out

    # Spreads at onset
    cp, ncp, nrp = c_pct[onset], nc_pct[onset], nr_pct[onset]
    out["c_nc_spread_pct"] = (
        None if cp is None or ncp is None else round(float(cp) - float(ncp), 4)
    )
    out["c_nr_spread_pct"] = (
        None if cp is None or nrp is None else round(float(cp) - float(nrp), 4)
    )
    j = onset - 8
    if j >= 0 and cp is not None and ncp is not None and c_pct[j] is not None and nc_pct[j] is not None:
        out["c_nc_spread_chg_8w"] = round(
            (float(cp) - float(ncp)) - (float(c_pct[j]) - float(nc_pct[j])), 4
        )
    else:
        out["c_nc_spread_chg_8w"] = None
    if j >= 0 and cp is not None and nrp is not None and c_pct[j] is not None and nr_pct[j] is not None:
        out["c_nr_spread_chg_8w"] = round(
            (float(cp) - float(nrp)) - (float(c_pct[j]) - float(nr_pct[j])), 4
        )
    else:
        out["c_nr_spread_chg_8w"] = None

    # Opposition score: C high & NC low or reverse
    if cp is not None and ncp is not None:
        if (cp >= 60 and ncp <= 40) or (cp <= 40 and ncp >= 60):
            out["c_nc_opp_score"] = round(abs(float(cp) - float(ncp)) / 100.0, 4)
        else:
            out["c_nc_opp_score"] = 0.0
    else:
        out["c_nc_opp_score"] = None

    # Direction agreement over 4w
    def dir_chg(arr: list, lag: int = 4) -> int:
        k = onset - lag
        if k < 0 or arr[onset] is None or arr[k] is None:
            return 0
        return _sign(float(arr[onset]) - float(arr[k]))

    dc, dnc, dnr = dir_chg(c_pct), dir_chg(nc_pct), dir_chg(nr_pct)
    out["c_nc_agree_4w"] = int(dc != 0 and dc == dnc)
    out["c_nr_agree_4w"] = int(dc != 0 and dc == dnr)
    out["c_nc_oppose_4w"] = int(dc != 0 and dnc != 0 and dc == -dnc)

    # Which flipped first in prior 12w (percentile direction flip)
    def first_flip(arr: list) -> int | None:
        for i in range(max(2, onset - 11), onset + 1):
            if arr[i] is None or arr[i - 1] is None or arr[i - 2] is None:
                continue
            s1 = _sign(float(arr[i - 1]) - float(arr[i - 2]))
            s2 = _sign(float(arr[i]) - float(arr[i - 1]))
            if s1 != 0 and s2 != 0 and s1 != s2:
                return i
        return None

    fc, fnc = first_flip(c_pct), first_flip(nc_pct)
    if fc is not None and fnc is not None:
        out["flip_first"] = "commercial" if fc < fnc else ("noncommercial" if fnc < fc else "tie")
        out["flip_lag_weeks"] = abs(fc - fnc)
    else:
        out["flip_first"] = None
        out["flip_lag_weeks"] = None

    # Opposition widening: |C-NC| increased over 8w
    if out.get("c_nc_spread_chg_8w") is not None and out.get("c_nc_spread_pct") is not None:
        out["opposition_widening_8w"] = int(
            abs(float(out["c_nc_spread_pct"]))
            > abs(float(out["c_nc_spread_pct"]) - float(out["c_nc_spread_chg_8w"]))
        )
    else:
        out["opposition_widening_8w"] = 0

    return out


def build_stage_sequence(panel: dict[str, Any], onset: int, lookback: int = 12) -> dict[str, Any]:
    """Discrete behavioural stages over the lookback (not forced to include all)."""
    start = max(0, onset - lookback + 1)
    group_seq: dict[str, list[str]] = {}
    for g, prefix in (
        ("commercial", "C"),
        ("noncommercial", "NC"),
        ("nonreportable", "NR"),
    ):
        pcts = panel["pcts"][g]
        stages: list[str] = []
        last = None
        for i in range(start + 1, onset + 1):
            p0, p1 = pcts[i - 1], pcts[i]
            if p0 is None or p1 is None:
                continue
            dp = float(p1) - float(p0)
            stage = None
            if p1 >= EXTREME_HIGH:
                stage = "EXTREME_HIGH"
            elif p1 <= EXTREME_LOW:
                stage = "EXTREME_LOW"
            elif abs(dp) < 1.0 and (p0 >= EXTREME_HIGH or p0 <= EXTREME_LOW):
                stage = "SATURATION"
            elif abs(dp) < 0.5:
                stage = "FLATTEN"
            elif dp >= 3.0:
                # flip up if previously falling
                if last in {"BUILD_DOWN", "EXTREME_LOW", "FLIP_DOWN"}:
                    stage = "FLIP_UP"
                else:
                    stage = "BUILD_UP"
            elif dp <= -3.0:
                if last in {"BUILD_UP", "EXTREME_HIGH", "FLIP_UP"}:
                    stage = "FLIP_DOWN"
                else:
                    stage = "BUILD_DOWN"
            if stage and stage != last:
                stages.append(stage)
                last = stage
        group_seq[g] = stages

    # Cross-group stages
    cross: list[str] = []
    c_pct, nc_pct = panel["pcts"]["commercial"], panel["pcts"]["noncommercial"]
    spreads = []
    for i in range(start, onset + 1):
        if c_pct[i] is None or nc_pct[i] is None:
            continue
        spreads.append(abs(float(c_pct[i]) - float(nc_pct[i])))
    if len(spreads) >= 4:
        mid = len(spreads) // 2
        early = float(np.mean(spreads[:mid]))
        late = float(np.mean(spreads[mid:]))
        peak = float(np.max(spreads))
        if late > early + 5:
            cross.append("OPPOSITION_WIDENING")
        if peak >= 50 and spreads[-1] >= peak - 5:
            cross.append("OPPOSITION_PEAK")
        if late + 5 < early:
            cross.append("ALIGNMENT_BEGINNING")

    return {
        "commercial_stages": group_seq["commercial"],
        "noncommercial_stages": group_seq["noncommercial"],
        "nonreportable_stages": group_seq["nonreportable"],
        "cross_stages": cross,
        "commercial_sequence": " -> ".join(group_seq["commercial"]) or "NONE",
        "noncommercial_sequence": " -> ".join(group_seq["noncommercial"]) or "NONE",
        "nonreportable_sequence": " -> ".join(group_seq["nonreportable"]) or "NONE",
        "cross_sequence": " -> ".join(cross) or "NONE",
        "lookback_weeks": lookback,
    }


def extract_case_features(
    panel: dict[str, Any],
    move: dict[str, Any],
    *,
    case_role: str = "event",
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Return (feature_row, sequence_row) for one onset. PIT-safe."""
    onset = int(move["onset_index"])
    feat: dict[str, Any] = {
        "case_id": (
            f"{move['market']}|{move['onset_date']}|h{move['horizon_weeks']}|{move['direction']}|{case_role}"
        ),
        "case_role": case_role,
        "market": move["market"],
        "asset_class": move.get("asset_class"),
        "onset_date": move["onset_date"],
        "onset_index": onset,
        "horizon_weeks": move.get("horizon_weeks"),
        "direction": move.get("direction"),
        "forward_return_pct": move.get("forward_return_pct"),
        "mfe_pct": move.get("mfe_pct"),
        "mae_pct": move.get("mae_pct"),
        "independent": move.get("independent", True),
    }
    for g, pfx in (
        ("commercial", "c"),
        ("noncommercial", "nc"),
        ("nonreportable", "nr"),
    ):
        feat.update(_group_features(panel, g, onset, prefix=pfx))
    feat.update(_cross_group_features(panel, onset))

    # Trajectory snapshots for lookbacks (numeric)
    for lb in LOOKBACK_WEEKS:
        j = onset - lb
        for g, pfx in (("commercial", "c"), ("noncommercial", "nc"), ("nonreportable", "nr")):
            key = f"{pfx}_pct_lb{lb}"
            feat[key] = None if j < 0 else panel["pcts"][g][j]

    seq = build_stage_sequence(panel, onset, lookback=12)
    seq_row = {
        "case_id": feat["case_id"],
        "case_role": case_role,
        "market": move["market"],
        "onset_date": move["onset_date"],
        "horizon_weeks": move.get("horizon_weeks"),
        "direction": move.get("direction"),
        **seq,
    }
    return feat, seq_row


def features_to_frames(
    feature_rows: list[dict[str, Any]], sequence_rows: list[dict[str, Any]]
) -> tuple[pd.DataFrame, pd.DataFrame]:
    return pd.DataFrame(feature_rows), pd.DataFrame(sequence_rows)
