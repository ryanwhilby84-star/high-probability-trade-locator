"""COT Intelligence Engine — Phase 3 multi-group configuration discovery.

Uses Phase-2 promoted Definition B (exit-from-extreme) as the turning-point
primitive. Definition A = migration context; C = research-only; D excluded.

Interpretable rule-based configuration families from point-in-time positioning
state only. Forward returns attached AFTER family assignment — never used to
define families or thresholds.

No production intelligence score, no UI deployment, no black-box ML.
"""

from __future__ import annotations

import json
import math
import statistics
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Sequence

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.intelligence_phase1_audit import (
    ABSOLUTE_HIGH,
    ABSOLUTE_LOW,
    COT3Y_PATHS,
    EVENT_COOLDOWN_WEEKS,
    MIN_HISTORY,
    ROTATION_PCT_MOVE_26W,
    _finite,
    _load_first,
    _zone,
)
from hptl.cot.intelligence_phase2_turning_points import (
    ASSET_CLASS,
    PHASE1_AUDIT_PATH,
    TP_COOLDOWN_WEEKS,
    _cluster_independent,
    _dir_label,
    _load_trustworthy_markets,
    _median,
    _mean,
    _sign,
    _stdev,
    detect_turning_points_raw,
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
PHASE3_JSON = AUDIT_DIR / "cot_intelligence_phase3_configurations.json"
PHASE3_MD = AUDIT_DIR / "cot_intelligence_phase3_configurations.md"
PHASE3_INVENTORY = (
    PROCESSED_DIR / "cot_intelligence_phase3_config_inventory_latest.json"
)

SAMPLE_COOLDOWN = EVENT_COOLDOWN_WEEKS
MIN_N_FAMILY = 8
MIN_N_CANDIDATE = 20
MIN_MARKETS_CANDIDATE = 3
MIN_ASSET_CLASSES_CANDIDATE = 2

GROUPS = (GROUP_COMMERCIAL, GROUP_NONCOMMERCIAL, GROUP_NONREPORTABLE)


def _pct_bin(pct: float | None) -> str:
    if pct is None:
        return "na"
    if pct <= ABSOLUTE_LOW:
        return "xl"  # extreme low
    if pct < 40:
        return "lo"
    if pct <= 60:
        return "mid"
    if pct < ABSOLUTE_HIGH:
        return "hi"
    return "xh"  # extreme high


def _vel_bin(v: float | None) -> str:
    if v is None:
        return "na"
    if v > 5:
        return "up"
    if v < -5:
        return "dn"
    return "flat"


def _group_snapshot(state: dict[str, Any], *, tp_b_dir: str | None) -> dict[str, Any]:
    pct = state.get("percentiles") or {}
    long_pct = pct.get("long_history")
    y3 = pct.get("3y")
    journey = state.get("journey") or {}
    velocity = state.get("velocity") or {}
    j26 = (journey.get("26w") or {}).get("long_history_percentile")
    v4 = (velocity.get("4w") or {}).get("percentile_change")
    v1 = (velocity.get("1w") or {}).get("net_change")
    v12 = (velocity.get("12w") or {}).get("percentile_change")
    zone = _zone(long_pct)
    mig_delta = None
    mig = False
    mig_dir = None
    if long_pct is not None and j26 is not None:
        mig_delta = round(long_pct - j26, 2)
        mig = abs(mig_delta) >= ROTATION_PCT_MOVE_26W
        mig_dir = _dir_label(_sign(mig_delta))
    return {
        "net": state.get("net"),
        "long_history_percentile": long_pct,
        "percentile_3y": y3,
        "pct_bin": _pct_bin(long_pct),
        "extreme_zone": zone,
        "in_extreme": zone is not None,
        "tp_b": tp_b_dir,  # bullish/bearish exit this week, else None
        "major_rotation_active": mig,
        "major_rotation_direction": mig_dir,
        "rotation_delta_26w": mig_delta,
        "velocity_1w_net": v1,
        "velocity_4w_pct": v4,
        "velocity_12w_pct": v12,
        "vel_4w_bin": _vel_bin(v4),
        "direction_4w": _dir_label(_sign(v4)),
    }


def _relative_ordering(c_pct: float | None, nc_pct: float | None, nr_pct: float | None) -> str:
    pairs = [
        ("C", c_pct),
        ("NC", nc_pct),
        ("NR", nr_pct),
    ]
    if any(p is None for _, p in pairs):
        return "na"
    ordered = sorted(pairs, key=lambda x: (-(x[1] or 0), x[0]))
    return ">".join(name for name, _ in ordered)


def _opposing_c_nc(c_bin: str, nc_bin: str) -> str | None:
    if c_bin in {"xh", "hi"} and nc_bin in {"xl", "lo"}:
        return "C_hi_NC_lo"
    if c_bin in {"xl", "lo"} and nc_bin in {"xh", "hi"}:
        return "C_lo_NC_hi"
    return None


def build_config_snapshot(
    *,
    market: str,
    index: int,
    date: str,
    commercial: dict[str, Any],
    noncommercial: dict[str, Any],
    nonreportable: dict[str, Any],
    spread: dict[str, Any],
    spread_prev: dict[str, Any] | None,
    tp_b_by_group: dict[str, str | None],
    onset_triggers: list[str],
) -> dict[str, Any]:
    c = _group_snapshot(commercial, tp_b_dir=tp_b_by_group.get(GROUP_COMMERCIAL))
    nc = _group_snapshot(noncommercial, tp_b_dir=tp_b_by_group.get(GROUP_NONCOMMERCIAL))
    nr = _group_snapshot(nonreportable, tp_b_dir=tp_b_by_group.get(GROUP_NONREPORTABLE))

    sp_pct = spread.get("spread_percentile")
    sp_val = spread.get("spread")
    div_state = None
    if sp_pct is not None:
        if sp_pct >= 90:
            div_state = "high"
        elif sp_pct <= 10:
            div_state = "low"

    expanding = None
    if sp_pct is not None and spread_prev and spread_prev.get("spread_percentile") is not None:
        prev = spread_prev["spread_percentile"]
        if sp_pct > prev:
            expanding = "expanding"
        elif sp_pct < prev:
            expanding = "contracting"
        else:
            expanding = "flat"

    ordering = _relative_ordering(
        c.get("long_history_percentile"),
        nc.get("long_history_percentile"),
        nr.get("long_history_percentile"),
    )
    opposing = _opposing_c_nc(c["pct_bin"], nc["pct_bin"])

    # Distances between group percentiles (PIT)
    dists: dict[str, float | None] = {}
    for a_name, a_pct, b_name, b_pct in (
        ("C_NC", c.get("long_history_percentile"), "NC", nc.get("long_history_percentile")),
        ("C_NR", c.get("long_history_percentile"), "NR", nr.get("long_history_percentile")),
        ("NC_NR", nc.get("long_history_percentile"), "NR", nr.get("long_history_percentile")),
    ):
        if a_pct is None or b_pct is None:
            dists[a_name] = None
        else:
            dists[a_name] = round(float(a_pct) - float(b_pct), 2)

    family = compose_family_key(
        c=c,
        nc=nc,
        nr=nr,
        div_state=div_state,
        expanding=expanding,
        ordering=ordering,
        opposing=opposing,
        onset_triggers=onset_triggers,
    )

    return {
        "market": market,
        "asset_class": ASSET_CLASS.get(market, "other"),
        "index": index,
        "date": date,
        "onset_triggers": sorted(set(onset_triggers)),
        "features": {
            "commercial": c,
            "noncommercial": nc,
            "nonreportable": nr,
            "spread": {
                "value": sp_val,
                "percentile": sp_pct,
                "divergence_state": div_state,
                "divergence_trend": expanding,
            },
            "relative": {
                "ordering": ordering,
                "opposing_c_nc": opposing,
                "percentile_distances": dists,
            },
        },
        "family_key": family["key"],
        "family_parts": family["parts"],
        "family_human": family["human"],
    }


def compose_family_key(
    *,
    c: dict[str, Any],
    nc: dict[str, Any],
    nr: dict[str, Any],
    div_state: str | None,
    expanding: str | None,
    ordering: str,
    opposing: str | None,
    onset_triggers: list[str],
) -> dict[str, Any]:
    """Transparent rule-based family signature (positioning only)."""

    def gpart(prefix: str, g: dict[str, Any]) -> str:
        tp = g.get("tp_b") or "none"
        mig = "mig" if g.get("major_rotation_active") else "nomig"
        mig_d = g.get("major_rotation_direction") or "na"
        return (
            f"{prefix}:pct={g.get('pct_bin')}|z={g.get('extreme_zone') or 'out'}"
            f"|tpB={tp}|{mig}:{mig_d}|v4={g.get('vel_4w_bin')}"
        )

    # Core structural family omits noisy velocity for primary clustering;
    # keep velocity in extended key for similarity explanations.
    parts = {
        "C": f"pct={c.get('pct_bin')}|z={c.get('extreme_zone') or 'out'}|tpB={c.get('tp_b') or 'none'}",
        "NC": f"pct={nc.get('pct_bin')}|z={nc.get('extreme_zone') or 'out'}|tpB={nc.get('tp_b') or 'none'}",
        "NR": f"pct={nr.get('pct_bin')}|z={nr.get('extreme_zone') or 'out'}|tpB={nr.get('tp_b') or 'none'}",
        "DIV": f"state={div_state or 'none'}|trend={expanding or 'na'}",
        "ORD": ordering,
        "OPP": opposing or "none",
        "ONSET": "+".join(sorted(set(onset_triggers))) or "none",
    }
    # Family identity for discovery: structural + onset class (not market)
    # Use coarse onset class to avoid splintering identical states by trigger label noise
    onset_class = _onset_class(onset_triggers)
    key_parts = [
        f"C[{parts['C']}]",
        f"NC[{parts['NC']}]",
        f"NR[{parts['NR']}]",
        f"DIV[{parts['DIV']}]",
        f"ORD[{parts['ORD']}]",
        f"OPP[{parts['OPP']}]",
        f"ONSET[{onset_class}]",
    ]
    key = "||".join(key_parts)
    human = (
        f"C {c.get('pct_bin')}/{c.get('extreme_zone') or 'out'}"
        f"{'/TP-B-' + c['tp_b'] if c.get('tp_b') else ''}; "
        f"NC {nc.get('pct_bin')}/{nc.get('extreme_zone') or 'out'}"
        f"{'/TP-B-' + nc['tp_b'] if nc.get('tp_b') else ''}; "
        f"NR {nr.get('pct_bin')}/{nr.get('extreme_zone') or 'out'}"
        f"{'/TP-B-' + nr['tp_b'] if nr.get('tp_b') else ''}; "
        f"DIV {div_state or 'none'}({expanding or 'na'}); "
        f"order {ordering}; opp {opposing or 'none'}; onset {onset_class}"
    )
    extended = {
        "C_full": gpart("C", c),
        "NC_full": gpart("NC", nc),
        "NR_full": gpart("NR", nr),
    }
    return {"key": key, "parts": parts, "human": human, "extended": extended}


def _onset_class(triggers: list[str]) -> str:
    t = set(triggers)
    has_tpb = any(x.startswith("tpB_") for x in t)
    has_div = "div_onset" in t
    has_ep = any(x.startswith("extreme_onset_") for x in t)
    labels = []
    if has_tpb:
        labels.append("tpB")
    if has_div:
        labels.append("div")
    if has_ep:
        labels.append("exOnset")
    return "+".join(labels) if labels else "other"


def _attach_outcomes(prices: list[float | None], idx: int) -> dict[str, Any] | None:
    out: dict[str, Any] = {}
    any_ok = False
    for h in (1, 4, 8, 12):
        fo = _forward_path_stats(prices, idx, h)
        if fo is None:
            out[f"fwd_{h}w"] = None
        else:
            any_ok = True
            out[f"fwd_{h}w"] = {
                "return_pct": fo["return_pct"],
                "mfe_pct": fo["favourable_excursion_pct"],
                "mae_pct": fo["adverse_excursion_pct"],
            }
    return out if any_ok else None


def _summarize_returns(samples: list[dict[str, Any]]) -> dict[str, Any]:
    keyed = [s for s in samples if s.get("outcome_labels")]
    result: dict[str, Any] = {"n_with_price": len(keyed)}
    for h in (1, 4, 8, 12):
        vals = []
        mfes = []
        maes = []
        for s in keyed:
            block = (s.get("outcome_labels") or {}).get(f"fwd_{h}w") or {}
            if block.get("return_pct") is not None:
                vals.append(float(block["return_pct"]))
            if block.get("mfe_pct") is not None:
                mfes.append(float(block["mfe_pct"]))
            if block.get("mae_pct") is not None:
                maes.append(float(block["mae_pct"]))
        pos = sum(1 for v in vals if v > 0)
        neg = sum(1 for v in vals if v < 0)
        result[f"fwd_{h}w"] = {
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
                if len(vals) < MIN_N_FAMILY
                else ("MODERATE" if len(vals) < MIN_N_CANDIDATE else "STRONGER")
            ),
        }
    return result


def collect_onset_indices(
    market: str,
    commercial: list[dict[str, Any]],
    noncommercial: list[dict[str, Any]],
    nonreportable: list[dict[str, Any]],
    spreads: list[dict[str, Any]],
) -> dict[int, dict[str, Any]]:
    """Map week index -> onset metadata (triggers, TP-B directions by group)."""
    group_states = {
        GROUP_COMMERCIAL: commercial,
        GROUP_NONCOMMERCIAL: noncommercial,
        GROUP_NONREPORTABLE: nonreportable,
    }

    # TP-B independent events
    raw_b: list[dict[str, Any]] = []
    for g, states in group_states.items():
        raw_b.extend(
            e for e in detect_turning_points_raw(market, states, g) if e["definition"] == "B"
        )
    indep_b = _cluster_independent(raw_b)

    onsets: dict[int, dict[str, Any]] = {}

    def ensure(idx: int) -> dict[str, Any]:
        if idx not in onsets:
            onsets[idx] = {
                "triggers": [],
                "tp_b_by_group": {
                    GROUP_COMMERCIAL: None,
                    GROUP_NONCOMMERCIAL: None,
                    GROUP_NONREPORTABLE: None,
                },
            }
        return onsets[idx]

    for e in indep_b:
        idx = int(e["confirm_index"])
        slot = ensure(idx)
        slot["triggers"].append(f"tpB_{e['group']}")
        slot["tp_b_by_group"][e["group"]] = e["direction"]

    # Divergence onsets: enter high/low band from outside
    last_div = None
    last_emit = -10_000
    for i, sp in enumerate(spreads):
        if i < MIN_HISTORY - 1:
            continue
        pct = sp.get("spread_percentile")
        state = None
        if pct is not None:
            if pct >= 90:
                state = "high"
            elif pct <= 10:
                state = "low"
        if state is not None and state != last_div and i - last_emit >= SAMPLE_COOLDOWN:
            slot = ensure(i)
            slot["triggers"].append("div_onset")
            last_emit = i
        if state is None:
            last_div = None
        else:
            last_div = state

    # Extreme episode onsets (enter zone) per group — independent via cooldown
    for g, states in group_states.items():
        in_zone = False
        last = -10_000
        for i, s in enumerate(states):
            if i < MIN_HISTORY - 1:
                continue
            z = _zone((s.get("percentiles") or {}).get("long_history"))
            if z is not None and not in_zone:
                if i - last >= SAMPLE_COOLDOWN:
                    slot = ensure(i)
                    slot["triggers"].append(f"extreme_onset_{g}")
                    last = i
                in_zone = True
            elif z is None:
                in_zone = False

    # Keep only weeks that have at least one trigger
    return {i: meta for i, meta in onsets.items() if meta["triggers"]}


def build_market_configurations(
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
    prices = [_finite(r.get("price")) for r in series]

    onset_map = collect_onset_indices(
        market, commercial, noncommercial, nonreportable, spreads
    )

    # Deduplicate samples: one snapshot per (market, index); then independence
    # across family with cooldown to avoid near-duplicate consecutive configs.
    raw_samples: list[dict[str, Any]] = []
    for idx, meta in sorted(onset_map.items()):
        snap = build_config_snapshot(
            market=market,
            index=idx,
            date=str(commercial[idx].get("date") or "")[:10],
            commercial=commercial[idx],
            noncommercial=noncommercial[idx],
            nonreportable=nonreportable[idx],
            spread=spreads[idx],
            spread_prev=spreads[idx - 1] if idx > 0 else None,
            tp_b_by_group=meta["tp_b_by_group"],
            onset_triggers=meta["triggers"],
        )
        if price_ok:
            snap["outcome_labels"] = _attach_outcomes(prices, idx)
            snap["price_study_eligible"] = snap["outcome_labels"] is not None
        else:
            snap["outcome_labels"] = None
            snap["price_study_eligible"] = False
            snap["price_exclusion_reason"] = "not_in_phase1_trustworthy_set"
        raw_samples.append(snap)

    # Independence: same market+family_key cooldown on index
    independent: list[dict[str, Any]] = []
    last_by_family: dict[str, int] = {}
    for s in sorted(raw_samples, key=lambda x: x["index"]):
        fam = s["family_key"]
        prev = last_by_family.get(fam, -10_000)
        if s["index"] - prev < SAMPLE_COOLDOWN:
            continue
        independent.append(s)
        last_by_family[fam] = s["index"]

    return {
        "market": market,
        "available": True,
        "price_study_eligible_market": price_ok,
        "raw_onset_weeks": len(raw_samples),
        "independent_samples": len(independent),
        "samples": independent,
    }


# --- Interaction hypotheses (examples for study, not predefined winners) ---
INTERACTION_QUERIES: list[dict[str, Any]] = [
    {
        "id": "C_tpB_plus_NC_extreme",
        "label": "Commercial TP-B + NC in extreme",
        "test": lambda f: bool(
            f["commercial"].get("tp_b") and f["noncommercial"].get("in_extreme")
        ),
    },
    {
        "id": "NC_tpB_plus_C_extreme",
        "label": "NC TP-B + Commercial in extreme",
        "test": lambda f: bool(
            f["noncommercial"].get("tp_b") and f["commercial"].get("in_extreme")
        ),
    },
    {
        "id": "C_tpB_plus_div",
        "label": "Commercial TP-B + active divergence",
        "test": lambda f: bool(
            f["commercial"].get("tp_b") and f["spread"].get("divergence_state")
        ),
    },
    {
        "id": "NC_tpB_plus_div",
        "label": "NC TP-B + active divergence",
        "test": lambda f: bool(
            f["noncommercial"].get("tp_b") and f["spread"].get("divergence_state")
        ),
    },
    {
        "id": "NC_mig_plus_NR_extreme",
        "label": "NC major_rotation + NR extreme",
        "test": lambda f: bool(
            f["noncommercial"].get("major_rotation_active")
            and f["nonreportable"].get("in_extreme")
        ),
    },
    {
        "id": "div_expanding_plus_any_tpB",
        "label": "Divergence expanding + any TP-B",
        "test": lambda f: bool(
            f["spread"].get("divergence_trend") == "expanding"
            and (
                f["commercial"].get("tp_b")
                or f["noncommercial"].get("tp_b")
                or f["nonreportable"].get("tp_b")
            )
        ),
    },
    {
        "id": "opposing_C_NC",
        "label": "Opposing Commercial/NC percentile regimes",
        "test": lambda f: bool(f["relative"].get("opposing_c_nc")),
    },
    {
        "id": "C_tpB_alone",
        "label": "Commercial TP-B (baseline single)",
        "test": lambda f: bool(f["commercial"].get("tp_b")),
    },
    {
        "id": "NC_tpB_alone",
        "label": "NC TP-B (baseline single)",
        "test": lambda f: bool(f["noncommercial"].get("tp_b")),
    },
    {
        "id": "div_alone",
        "label": "Divergence onset/active (baseline)",
        "test": lambda f: bool(f["spread"].get("divergence_state")),
    },
]


def evaluate_interactions(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    out = []
    for q in INTERACTION_QUERIES:
        matched = [s for s in samples if q["test"](s["features"])]
        price_matched = [s for s in matched if s.get("price_study_eligible")]
        out.append(
            {
                "id": q["id"],
                "label": q["label"],
                "n": len(matched),
                "n_price": len(price_matched),
                "markets": len({s["market"] for s in matched}),
                "asset_classes": sorted({s["asset_class"] for s in matched}),
                "outcomes": _summarize_returns(price_matched),
            }
        )
    return out


def analyze_families(samples: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_fam: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for s in samples:
        by_fam[s["family_key"]].append(s)

    families = []
    for key, evs in by_fam.items():
        markets = sorted({e["market"] for e in evs})
        assets = sorted({e["asset_class"] for e in evs})
        by_m = Counter(e["market"] for e in evs)
        top_m, top_n = by_m.most_common(1)[0]
        top_share = top_n / len(evs)
        price_evs = [e for e in evs if e.get("price_study_eligible")]
        human = evs[0]["family_human"]
        parts = evs[0]["family_parts"]

        # Stability / sample gates — NOT return optimization
        if (
            len(evs) >= MIN_N_CANDIDATE
            and len(markets) >= MIN_MARKETS_CANDIDATE
            and len(assets) >= MIN_ASSET_CLASSES_CANDIDATE
            and top_share < 0.5
        ):
            verdict = "candidate"
        elif len(evs) >= MIN_N_FAMILY and len(markets) >= 2:
            verdict = "weak"
        else:
            verdict = "reject"

        # Descriptive outcome flag (does not change family definition)
        outcomes = _summarize_returns(price_evs)
        f4 = outcomes.get("fwd_4w") or {}
        interesting = (
            verdict == "candidate"
            and (f4.get("n") or 0) >= MIN_N_FAMILY
            and f4.get("pct_positive") is not None
            and abs((f4.get("pct_positive") or 50) - 50) >= 8
        )

        families.append(
            {
                "family_key": key,
                "family_human": human,
                "family_parts": parts,
                "n": len(evs),
                "n_price": len(price_evs),
                "markets": markets,
                "n_markets": len(markets),
                "asset_classes": assets,
                "top_market": top_m,
                "top_market_share": round(top_share, 4),
                "verdict": verdict,
                "descriptive_outcome_asymmetric": interesting,
                "outcomes": outcomes,
                "onset_trigger_mix": dict(
                    Counter(
                        t
                        for e in evs
                        for t in e.get("onset_triggers") or []
                    )
                ),
            }
        )

    families.sort(key=lambda f: (-f["n"], f["family_key"]))
    return families


def design_similarity_framework() -> dict[str, Any]:
    """Transparent similarity model design — not deployed."""
    return {
        "status": "design_only_not_deployed",
        "goal": (
            "Explain how similar today's PIT multi-group state is to a historical "
            "configuration family — feature-level matches, not one opaque score."
        ),
        "feature_blocks": [
            {
                "block": "group_percentiles",
                "features": ["pct_bin", "extreme_zone", "long_history_percentile"],
                "match_rule": "exact bin match; optional soft distance on raw percentile",
            },
            {
                "block": "tp_states",
                "features": ["tp_b direction per group"],
                "match_rule": "exact match on which groups are exiting and direction",
            },
            {
                "block": "migration",
                "features": ["major_rotation_active", "major_rotation_direction"],
                "match_rule": "exact on active+direction (Definition A context)",
            },
            {
                "block": "divergence",
                "features": ["divergence_state", "divergence_trend"],
                "match_rule": "exact state; trend as secondary",
            },
            {
                "block": "relative_structure",
                "features": ["ordering", "opposing_c_nc", "percentile_distances"],
                "match_rule": "exact ordering/opposition; soft on distances",
            },
        ],
        "similarity_output_shape": {
            "family_key": "matched historical family",
            "block_matches": {"group_percentiles": True, "tp_states": False, "...": "..."},
            "explanations": [
                "Commercial pct_bin=xh matches",
                "NC TP-B bullish missing",
            ],
            "soft_score_optional": (
                "Equal-weight share of matched blocks — only as a diagnostic, "
                "never as a production intelligence score in Phase 3"
            ),
        },
        "distance_prototype": (
            "Hamming distance on discrete family parts "
            "(C.pct_bin, C.zone, C.tpB, NC.*, NR.*, DIV.state, ORD, OPP). "
            "Nearest neighbours = lowest Hamming; ties broken by onset_class match."
        ),
    }


def hamming_family_distance(parts_a: dict[str, str], parts_b: dict[str, str]) -> int:
    keys = ["C", "NC", "NR", "DIV", "ORD", "OPP", "ONSET"]
    return sum(1 for k in keys if parts_a.get(k) != parts_b.get(k))


def label_summary(families: list[dict[str, Any]]) -> dict[str, Any]:
    by_v = Counter(f["verdict"] for f in families)
    return {
        "n_families": len(families),
        "candidate": by_v.get("candidate", 0),
        "weak": by_v.get("weak", 0),
        "reject": by_v.get("reject", 0),
        "descriptively_asymmetric_candidates": sum(
            1 for f in families if f.get("descriptive_outcome_asymmetric")
        ),
    }


def run_phase3_discovery(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    trustworthy = set(_load_trustworthy_markets())

    if markets is None:
        selected = sorted(str(k) for k in all_markets.keys())
    else:
        selected = list(markets)

    all_samples: list[dict[str, Any]] = []
    market_summary: dict[str, Any] = {}

    for mid in selected:
        block = all_markets.get(mid)
        if not block:
            continue
        result = build_market_configurations(mid, block, price_ok=mid in trustworthy)
        market_summary[mid] = {k: v for k, v in result.items() if k != "samples"}
        if result.get("available"):
            all_samples.extend(result["samples"])

    families = analyze_families(all_samples)
    interactions = evaluate_interactions(all_samples)
    sim = design_similarity_framework()

    candidates = [f for f in families if f["verdict"] == "candidate"]
    weak = [f for f in families if f["verdict"] == "weak"]
    rejected = [f for f in families if f["verdict"] == "reject"]

    # Phase 4 shortlist: candidates with enough price samples (descriptive asymmetry optional)
    phase4 = [
        {
            "family_human": f["family_human"],
            "family_key": f["family_key"],
            "n": f["n"],
            "n_price": f["n_price"],
            "n_markets": f["n_markets"],
            "asset_classes": f["asset_classes"],
            "descriptive_outcome_asymmetric": f["descriptive_outcome_asymmetric"],
            "fwd_4w": (f.get("outcomes") or {}).get("fwd_4w"),
        }
        for f in candidates
        if (f.get("n_price") or 0) >= MIN_N_FAMILY
    ]

    inventory = {
        "version": "cot_intelligence_phase3_config_inventory_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "primitives": {
            "promoted_turning_point": "B_exit_extreme",
            "migration_context": "A_major_rotation",
            "research_only": "C_velocity_flip",
            "excluded": "D_peak_trough",
        },
        "independence": {
            "cooldown_weeks": SAMPLE_COOLDOWN,
            "rule": "One sample per (market, family_key) with confirm/index gap >= cooldown",
            "onset_sources": [
                "independent TP-B confirms (Comm/NC/NR)",
                "divergence band onsets",
                "extreme-zone episode onsets",
            ],
        },
        "counts": {
            "independent_samples": len(all_samples),
            "price_eligible_samples": sum(
                1 for s in all_samples if s.get("price_study_eligible")
            ),
            "unique_families": len(families),
        },
        "samples": all_samples,
    }

    audit = {
        "version": "cot_intelligence_phase3_v1",
        "generated_at": inventory["generated_at"],
        "scope": "multi_group_configuration_discovery",
        "constraints_honored": [
            "No production intelligence score",
            "No UI deployment",
            "No black-box ML",
            "No threshold optimization against price",
            "Families defined from PIT positioning features only",
            "Outcomes attached after family assignment",
            "Copper excluded from price studies via trustworthy gate",
            "Definition D excluded; A/C contextual only",
        ],
        "family_method": {
            "type": "interpretable_rule_based_bins",
            "description": (
                "Discrete bins for each group's long-history percentile, extreme zone, "
                "TP-B state; divergence state/trend; relative ordering; C/NC opposition; "
                "coarse onset class. Similarity = Hamming on these parts."
            ),
            "why_similar": (
                "Two snapshots share a family when all discrete structural bins match. "
                "No latent embeddings."
            ),
        },
        "summary": label_summary(families),
        "markets": market_summary,
        "families": {
            "candidate": candidates[:50],
            "weak": weak[:30],
            "reject_count": len(rejected),
            "reject_examples": rejected[:10],
            "all_count_by_verdict": label_summary(families),
        },
        "interactions": interactions,
        "similarity_framework": sim,
        "phase4_candidates": phase4[:40],
        "inventory_counts": inventory["counts"],
    }

    # Full family table (compact) for audit file — top 100 by n
    audit["family_table_top"] = [
        {
            "verdict": f["verdict"],
            "n": f["n"],
            "n_price": f["n_price"],
            "n_markets": f["n_markets"],
            "asset_classes": f["asset_classes"],
            "top_market_share": f["top_market_share"],
            "human": f["family_human"],
            "fwd4_med": ((f.get("outcomes") or {}).get("fwd_4w") or {}).get(
                "median_return_pct"
            ),
            "fwd4_pct_pos": ((f.get("outcomes") or {}).get("fwd_4w") or {}).get(
                "pct_positive"
            ),
            "asymmetric": f["descriptive_outcome_asymmetric"],
        }
        for f in families[:100]
    ]

    return {"audit": audit, "inventory": inventory, "families_all": families}


def write_phase3_markdown(audit: dict[str, Any]) -> str:
    s = audit["summary"]
    lines = [
        "# COT Intelligence — Phase 3 Multi-Group Configuration Discovery",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "Promoted TP primitive: **Definition B (exit from extreme)**. "
        "A = migration context; C = research-only; D excluded.",
        "",
        "Families defined from point-in-time positioning bins only. "
        "Outcomes attached afterward. No production score / UI.",
        "",
        "## Method",
        "",
        audit["family_method"]["description"],
        "",
        f"Why similar: {audit['family_method']['why_similar']}",
        "",
        "## Inventory",
        "",
        f"- Independent samples: **{audit['inventory_counts']['independent_samples']}**",
        f"- Price-eligible samples: **{audit['inventory_counts']['price_eligible_samples']}**",
        f"- Unique families: **{audit['inventory_counts']['unique_families']}**",
        f"- Candidate / weak / reject: "
        f"**{s['candidate']}** / **{s['weak']}** / **{s['reject']}**",
        "",
        "## Top families (by sample size)",
        "",
        "| Verdict | n | n_price | markets | assets | top_share | med4W | %+4W | Human |",
        "|---|---:|---:|---:|---|---:|---:|---:|---|",
    ]
    for row in (audit.get("family_table_top") or [])[:40]:
        lines.append(
            "| {v} | {n} | {np} | {nm} | {ac} | {ts} | {m4} | {pp} | {h} |".format(
                v=row["verdict"],
                n=row["n"],
                np=row["n_price"],
                nm=row["n_markets"],
                ac=",".join(row["asset_classes"]),
                ts=row["top_market_share"],
                m4=row.get("fwd4_med"),
                pp=row.get("fwd4_pct_pos"),
                h=(row.get("human") or "")[:80],
            )
        )

    lines += [
        "",
        "## Interaction slices (examples for study)",
        "",
        "| Id | Label | n | n_price | markets | med4W | %+4W |",
        "|---|---|---:|---:|---:|---:|---:|",
    ]
    for ix in audit.get("interactions") or []:
        f4 = (ix.get("outcomes") or {}).get("fwd_4w") or {}
        lines.append(
            f"| {ix['id']} | {ix['label']} | {ix['n']} | {ix['n_price']} | "
            f"{ix['markets']} | {f4.get('median_return_pct')} | {f4.get('pct_positive')} |"
        )

    lines += [
        "",
        "## Similarity framework (design only)",
        "",
        audit["similarity_framework"]["goal"],
        "",
        f"Distance prototype: {audit['similarity_framework']['distance_prototype']}",
        "",
        "## Phase 4 shortlist",
        "",
    ]
    for c in (audit.get("phase4_candidates") or [])[:20]:
        lines.append(
            f"- n={c['n']} markets={c['n_markets']} assets={c['asset_classes']} "
            f"asym={c['descriptive_outcome_asymmetric']}: {c['family_human'][:100]}"
        )

    lines += [
        "",
        "## Constraints honored",
        "",
    ]
    for c in audit.get("constraints_honored") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines)


def write_phase3_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    audit = payload["audit"]
    inventory = payload["inventory"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)

    # Inventory can be large — write it
    PHASE3_INVENTORY.write_text(json.dumps(inventory, indent=2), encoding="utf-8")
    PHASE3_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    PHASE3_MD.write_text(write_phase3_markdown(audit), encoding="utf-8")
    return {
        "inventory": PHASE3_INVENTORY,
        "audit_json": PHASE3_JSON,
        "audit_md": PHASE3_MD,
    }
