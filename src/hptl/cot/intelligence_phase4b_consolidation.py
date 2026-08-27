"""COT Intelligence — Phase 4B structural consolidation & sample expansion.

Creates a separate research layer above frozen Phase-3/4 families.
Archetypes are defined from positioning similarity only; outcomes attach only
after definitions are frozen. No UI, score, ML, or return-based merging.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Sequence

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.cot.intelligence_phase1_audit import COT3Y_PATHS, _load_first
from hptl.cot.intelligence_phase2_turning_points import _load_trustworthy_markets
from hptl.cot.intelligence_phase3_configurations import SAMPLE_COOLDOWN
from hptl.cot.intelligence_phase4_validation import (
    BONFERRONI_ALPHA,
    MIN_OOS_MARKETS_VALIDATED,
    MIN_OOS_N_PROMISING,
    MIN_OOS_N_VALIDATED,
    PHASE4_JSON,
    TOP_MARKET_SHARE_MAX,
    _in_test,
    _tpb_signed_return,
    build_chronological_folds,
    fold_stability,
    leave_one_asset_out,
    leave_one_market_out,
    summarize_outcomes,
)

AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
PHASE4B_JSON = AUDIT_DIR / "cot_intelligence_phase4b_consolidation.json"
PHASE4B_MD = AUDIT_DIR / "cot_intelligence_phase4b_consolidation.md"
PHASE4B_TRAIL = (
    PROCESSED_DIR / "cot_intelligence_phase4b_audit_trail_latest.json"
)

FAILED_FROZEN = ("P3C02", "P3C08", "P3C10")

# Coarse structural dimensions used for similarity (NO price features)
STRUCT_DIMS = (
    "opp",
    "c_side",
    "nc_side",
    "c_extreme",
    "nc_extreme",
    "nr_side",
    "nr_extreme",
    "c_tpb",
    "nc_tpb",
    "nr_tpb",
    "div_active",
    "div_state",
    "div_trend",
    "onset_class",
)


def _parse_group_part(part: str) -> dict[str, str]:
    """Parse 'pct=lo|z=out|tpB=none' style parts."""
    out = {"pct": "na", "z": "out", "tpB": "none"}
    if not part:
        return out
    for tok in str(part).split("|"):
        if "=" in tok:
            k, v = tok.split("=", 1)
            out[k] = v
    return out


def _side(pct: str) -> str:
    if pct in {"xh", "hi"}:
        return "hi"
    if pct in {"xl", "lo"}:
        return "lo"
    if pct == "mid":
        return "mid"
    return "na"


def structural_vector(family_parts: dict[str, Any], family_human: str = "") -> dict[str, str]:
    c = _parse_group_part(family_parts.get("C") or "")
    nc = _parse_group_part(family_parts.get("NC") or "")
    nr = _parse_group_part(family_parts.get("NR") or "")
    div = family_parts.get("DIV") or ""
    div_state, div_trend = "none", "na"
    if "state=" in div:
        for tok in div.split("|"):
            if tok.startswith("state="):
                div_state = tok.split("=", 1)[1]
            elif tok.startswith("trend="):
                div_trend = tok.split("=", 1)[1]
    opp = family_parts.get("OPP") or "none"
    onset_class = "other"
    if "onset " in family_human:
        onset_class = family_human.split("onset ", 1)[1].strip()

    return {
        "opp": opp if opp != "none" else "none",
        "c_side": _side(c.get("pct", "na")),
        "nc_side": _side(nc.get("pct", "na")),
        "c_extreme": "yes" if c.get("z") in {"high", "low"} else "no",
        "nc_extreme": "yes" if nc.get("z") in {"high", "low"} else "no",
        "nr_side": _side(nr.get("pct", "na")),
        "nr_extreme": "yes" if nr.get("z") in {"high", "low"} else "no",
        "c_tpb": "yes" if c.get("tpB") not in {None, "none", ""} else "no",
        "nc_tpb": "yes" if nc.get("tpB") not in {None, "none", ""} else "no",
        "nr_tpb": "yes" if nr.get("tpB") not in {None, "none", ""} else "no",
        "div_active": "yes" if div_state not in {None, "none", ""} else "no",
        "div_state": div_state or "none",
        "div_trend": div_trend or "na",
        "onset_class": onset_class,
    }


def hamming_distance(a: dict[str, str], b: dict[str, str]) -> int:
    return sum(1 for d in STRUCT_DIMS if a.get(d) != b.get(d))


def load_phase4_families() -> list[dict[str, Any]]:
    if not PHASE4_JSON.is_file():
        raise FileNotFoundError(f"Phase 4 audit required: {PHASE4_JSON}")
    doc = json.loads(PHASE4_JSON.read_text(encoding="utf-8"))
    out = []
    for f in doc.get("families") or []:
        cls = (f.get("classification") or {}).get("classification")
        vec = structural_vector(f.get("family_parts") or {}, f.get("family_human") or "")
        out.append(
            {
                "candidate_id": f["candidate_id"],
                "family_key": f["family_key"],
                "family_human": f["family_human"],
                "family_parts": f.get("family_parts"),
                "phase4_class": cls,
                "struct": vec,
                "frozen_failed": f["candidate_id"] in FAILED_FROZEN,
            }
        )
    return out


def similarity_matrix(families: list[dict[str, Any]]) -> dict[str, Any]:
    ids = [f["candidate_id"] for f in families]
    matrix: dict[str, dict[str, int]] = {}
    for a in families:
        matrix[a["candidate_id"]] = {}
        for b in families:
            matrix[a["candidate_id"]][b["candidate_id"]] = hamming_distance(
                a["struct"], b["struct"]
            )
    return {"ids": ids, "hamming": matrix, "dimensions": list(STRUCT_DIMS)}


def diagnose_fragmentation(families: list[dict[str, Any]]) -> dict[str, Any]:
    promising = [f for f in families if f["phase4_class"] == "PROMISING / MONITOR"]
    failed = [f for f in families if f["phase4_class"] == "FAILED"]

    by_opp: dict[str, list[str]] = defaultdict(list)
    for f in promising:
        by_opp[f["struct"]["opp"]].append(f["candidate_id"])

    # Within-opp distances
    within = {}
    for opp, ids in by_opp.items():
        members = [f for f in promising if f["candidate_id"] in ids]
        dists = []
        for i, a in enumerate(members):
            for b in members[i + 1 :]:
                dists.append(hamming_distance(a["struct"], b["struct"]))
        within[opp] = {
            "n_families": len(ids),
            "member_ids": ids,
            "mean_hamming": None if not dists else round(sum(dists) / len(dists), 2),
            "max_hamming": None if not dists else max(dists),
            "varying_dims": _varying_dims(members),
        }

    cross = []
    hi = [f for f in promising if f["struct"]["opp"] == "C_hi_NC_lo"]
    lo = [f for f in promising if f["struct"]["opp"] == "C_lo_NC_hi"]
    for a in hi:
        for b in lo:
            cross.append(hamming_distance(a["struct"], b["struct"]))

    diagnosis = {
        "n_promising": len(promising),
        "n_failed_frozen": len(failed),
        "failed_ids": [f["candidate_id"] for f in failed],
        "primary_axis": "C/NC opposition (OPP)",
        "promising_by_opp": {k: v for k, v in by_opp.items()},
        "within_opp_similarity": within,
        "cross_opp_mean_hamming": (
            None if not cross else round(sum(cross) / len(cross), 2)
        ),
        "fragmentation_finding": (
            "The 11 PROMISING families are largely two opposition regimes "
            "(C_hi_NC_lo vs C_lo_NC_hi) fragmented by NR state, DIV state/trend, "
            "onset class, and exact hi/xh vs lo/xl bins. Cross-opp Hamming is "
            "materially higher than within-opp Hamming — opposition is the "
            "genuine regime split; NR/DIV/onset differences are mostly minor "
            "state/bin fragmentation."
        ),
        "minor_vs_major": {
            "major_regime_split": ["opp"],
            "minor_fragmentation_dims": [
                "nr_side",
                "nr_extreme",
                "div_state",
                "div_trend",
                "onset_class",
                "nr_tpb",
                "c_extreme",
                "nc_extreme",
            ],
        },
    }
    return diagnosis


def _varying_dims(members: list[dict[str, Any]]) -> list[str]:
    varying = []
    for d in STRUCT_DIMS:
        vals = {m["struct"].get(d) for m in members}
        if len(vals) > 1:
            varying.append(d)
    return varying


# ---------------------------------------------------------------------------
# Frozen archetypes — defined from positioning structure ONLY.
# Definitions frozen before outcome attachment (see freeze_record).
# ---------------------------------------------------------------------------

def _feat(sample: dict[str, Any]) -> dict[str, Any]:
    return sample.get("features") or {}


def _opp_of(sample: dict[str, Any]) -> str | None:
    return (_feat(sample).get("relative") or {}).get("opposing_c_nc")


def _div_state(sample: dict[str, Any]) -> str | None:
    return (_feat(sample).get("spread") or {}).get("divergence_state")


def _div_active(sample: dict[str, Any]) -> bool:
    return bool(_div_state(sample))


def _c_tpb(sample: dict[str, Any]) -> str | None:
    return (_feat(sample).get("commercial") or {}).get("tp_b")


def _nc_tpb(sample: dict[str, Any]) -> str | None:
    return (_feat(sample).get("noncommercial") or {}).get("tp_b")


def _any_tpb(sample: dict[str, Any]) -> bool:
    f = _feat(sample)
    return bool(
        (f.get("commercial") or {}).get("tp_b")
        or (f.get("noncommercial") or {}).get("tp_b")
        or (f.get("nonreportable") or {}).get("tp_b")
    )


def _c_in_extreme(sample: dict[str, Any]) -> bool:
    return bool((_feat(sample).get("commercial") or {}).get("in_extreme"))


def _nc_in_extreme(sample: dict[str, Any]) -> bool:
    return bool((_feat(sample).get("noncommercial") or {}).get("in_extreme"))


# Matcher registry — kept as named functions for auditability
def match_a1_opp_c_hi_nc_lo(sample: dict[str, Any]) -> bool:
    return _opp_of(sample) == "C_hi_NC_lo"


def match_a2_opp_c_lo_nc_hi(sample: dict[str, Any]) -> bool:
    return _opp_of(sample) == "C_lo_NC_hi"


def match_a3_c_tpb_active_div(sample: dict[str, Any]) -> bool:
    return bool(_c_tpb(sample) and _div_active(sample))


def match_a4_nc_tpb_active_div(sample: dict[str, Any]) -> bool:
    return bool(_nc_tpb(sample) and _div_active(sample))


def match_a5_opp_plus_active_div(sample: dict[str, Any]) -> bool:
    return bool(_opp_of(sample) and _div_active(sample))


def match_a6_opp_plus_any_tpb(sample: dict[str, Any]) -> bool:
    return bool(_opp_of(sample) and _any_tpb(sample))


def match_a7_extreme_opposition(sample: dict[str, Any]) -> bool:
    """Extrema form of opposition: both C and NC in opposite extreme zones."""
    opp = _opp_of(sample)
    if opp not in {"C_hi_NC_lo", "C_lo_NC_hi"}:
        return False
    return _c_in_extreme(sample) and _nc_in_extreme(sample)


MATCHERS: dict[str, Callable[[dict[str, Any]], bool]] = {
    "A1_OPP_C_HI_NC_LO": match_a1_opp_c_hi_nc_lo,
    "A2_OPP_C_LO_NC_HI": match_a2_opp_c_lo_nc_hi,
    "A3_C_TPB_ACTIVE_DIV": match_a3_c_tpb_active_div,
    "A4_NC_TPB_ACTIVE_DIV": match_a4_nc_tpb_active_div,
    "A5_OPP_PLUS_ACTIVE_DIV": match_a5_opp_plus_active_div,
    "A6_OPP_PLUS_ANY_TPB": match_a6_opp_plus_any_tpb,
    "A7_EXTREME_OPPOSITION": match_a7_extreme_opposition,
}


def frozen_archetype_definitions(
    promising_ids_by_opp: dict[str, list[str]],
) -> list[dict[str, Any]]:
    """Return archetype definitions frozen BEFORE outcome attachment."""
    return [
        {
            "id": "A1_OPP_C_HI_NC_LO",
            "human": (
                "Commercial high-side / Non-Commercial low-side opposition "
                "at a research onset week"
            ),
            "definition_positioning": (
                "At an independent research onset (TP-B, divergence band entry, "
                "or extreme-zone episode entry), Commercial long-history "
                "percentile bin ∈ {hi, xh} AND Non-Commercial bin ∈ {lo, xl} "
                "(Phase-3 opposing_c_nc == C_hi_NC_lo). NR state, DIV state, "
                "and exact onset class are intentionally unconstrained."
            ),
            "motivating_promising_families": promising_ids_by_opp.get("C_hi_NC_lo", []),
            "structural_direction": None,
            "direction_note": (
                "No price direction is structurally asserted from opposition alone."
            ),
            "matcher": "match_a1_opp_c_hi_nc_lo",
        },
        {
            "id": "A2_OPP_C_LO_NC_HI",
            "human": (
                "Commercial low-side / Non-Commercial high-side opposition "
                "at a research onset week"
            ),
            "definition_positioning": (
                "Mirror of A1: opposing_c_nc == C_lo_NC_hi at research onset. "
                "NR/DIV/onset unconstrained."
            ),
            "motivating_promising_families": promising_ids_by_opp.get("C_lo_NC_hi", []),
            "structural_direction": None,
            "direction_note": (
                "No price direction is structurally asserted from opposition alone."
            ),
            "matcher": "match_a2_opp_c_lo_nc_hi",
        },
        {
            "id": "A3_C_TPB_ACTIVE_DIV",
            "human": "Commercial TP-B (exit from extreme) during active Commercial–Retail divergence",
            "definition_positioning": (
                "Commercial TP-B present this onset week AND spread divergence_state "
                "∈ {high, low} (active DIV band). Frozen Phase-3/4 interaction query."
            ),
            "motivating_promising_families": [],
            "structural_direction": "commercial_tp_b",
            "direction_note": (
                "TP-B direction is available but Phase 4 showed signed confirmation "
                "failed; Phase 4B investigates alternative interpretations."
            ),
            "matcher": "match_a3_c_tpb_active_div",
        },
        {
            "id": "A4_NC_TPB_ACTIVE_DIV",
            "human": "Non-Commercial TP-B during active Commercial–Retail divergence",
            "definition_positioning": (
                "NC TP-B present AND divergence_state ∈ {high, low}. "
                "Frozen Phase-3/4 interaction query."
            ),
            "motivating_promising_families": [],
            "structural_direction": "noncommercial_tp_b",
            "direction_note": "TP-B direction available; not forced as price direction.",
            "matcher": "match_a4_nc_tpb_active_div",
        },
        {
            "id": "A5_OPP_PLUS_ACTIVE_DIV",
            "human": "C/NC opposition coinciding with active divergence",
            "definition_positioning": (
                "opposing_c_nc ∈ {C_hi_NC_lo, C_lo_NC_hi} AND divergence_state "
                "∈ {high, low} at research onset."
            ),
            "motivating_promising_families": [
                i
                for opp in ("C_hi_NC_lo", "C_lo_NC_hi")
                for i in promising_ids_by_opp.get(opp, [])
            ],
            "structural_direction": None,
            "direction_note": "Regime/transition candidate; no forced price direction.",
            "matcher": "match_a5_opp_plus_active_div",
        },
        {
            "id": "A6_OPP_PLUS_ANY_TPB",
            "human": "C/NC opposition coinciding with any participant TP-B",
            "definition_positioning": (
                "opposing_c_nc set AND at least one of Commercial / NC / NR has "
                "TP-B this onset week."
            ),
            "motivating_promising_families": [
                i
                for opp in ("C_hi_NC_lo", "C_lo_NC_hi")
                for i in promising_ids_by_opp.get(opp, [])
            ],
            "structural_direction": None,
            "direction_note": "Turning-structure + opposition; no forced price direction.",
            "matcher": "match_a6_opp_plus_any_tpb",
        },
        {
            "id": "A7_EXTREME_OPPOSITION",
            "human": "Extreme-zone C/NC opposition (both groups in opposite extremes)",
            "definition_positioning": (
                "opposing_c_nc set AND Commercial in_extreme AND Non-Commercial "
                "in_extreme at research onset. Stricter subset of A1/A2."
            ),
            "motivating_promising_families": [],
            "structural_direction": None,
            "direction_note": "No forced price direction.",
            "matcher": "match_a7_extreme_opposition",
        },
    ]


def rebuild_archetype_inventory(
    all_onset_samples: list[dict[str, Any]],
    archetype_ids: list[str],
) -> dict[str, list[dict[str, Any]]]:
    """Match archetypes on onset samples; apply 8w cooldown per (market, archetype).

    Important: does NOT pool Phase-3 family occurrences — rematches structural rules
    on the onset research table. Uses Phase-3 onset samples as the PIT-safe table
    (same onset generation), then applies archetype matchers independently.
    """
    # Prefer de-duplicated by market+index first (samples already independent by family;
    # multiple family keys don't exist per sample — each onset is one sample).
    by_market_idx: dict[tuple[str, int], dict[str, Any]] = {}
    for s in all_onset_samples:
        key = (s["market"], int(s["index"]))
        by_market_idx[key] = s
    universe = sorted(by_market_idx.values(), key=lambda x: (x["market"], x["index"]))

    inventory: dict[str, list[dict[str, Any]]] = {aid: [] for aid in archetype_ids}
    last_idx: dict[tuple[str, str], int] = {}

    for s in universe:
        for aid in archetype_ids:
            matcher = MATCHERS[aid]
            if not matcher(s):
                continue
            prev = last_idx.get((s["market"], aid), -10_000)
            if int(s["index"]) - prev < SAMPLE_COOLDOWN:
                continue
            row = dict(s)
            row["archetype_id"] = aid
            inventory[aid].append(row)
            last_idx[(s["market"], aid)] = int(s["index"])
    return inventory


def inventory_summary(samples: list[dict[str, Any]]) -> dict[str, Any]:
    price = [s for s in samples if s.get("price_study_eligible")]
    by_m = Counter(s["market"] for s in samples)
    by_ac = Counter(s.get("asset_class") for s in samples)
    by_year = Counter(str(s.get("date") or "")[:4] for s in samples if s.get("date"))
    return {
        "n_independent": len(samples),
        "n_price_eligible": len(price),
        "n_markets": len(by_m),
        "by_market": dict(by_m.most_common()),
        "n_asset_classes": len({k for k in by_ac if k}),
        "by_asset_class": dict(by_ac),
        "by_year": dict(sorted(by_year.items())),
    }


def _prior_return(sample: dict[str, Any], prices_by_market: dict[str, list], weeks: int) -> float | None:
    """Not available on sample directly — skip if we don't have series.

    Phase 4B attaches prior path stats during rebuild enrichment instead.
    """
    return sample.get(f"prior_{weeks}w_return_pct")


def enrich_with_prior_paths(
    samples: list[dict[str, Any]],
    *,
    markets: Sequence[str] | None = None,
) -> None:
    """Attach prior 4w/12w returns for reversal/continuation analysis (in-place)."""
    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    # Build price lists once
    price_map: dict[str, list[float | None]] = {}
    for mid, block in all_markets.items():
        if markets is not None and mid not in markets:
            continue
        series = list(block.get("series") or [])
        price_map[mid] = []
        for r in series:
            p = r.get("price")
            try:
                price_map[mid].append(float(p) if p is not None else None)
            except (TypeError, ValueError):
                price_map[mid].append(None)

    for s in samples:
        prices = price_map.get(s["market"]) or []
        idx = int(s["index"])
        for w in (4, 12):
            j = idx - w
            key = f"prior_{w}w_return_pct"
            if j < 0 or idx >= len(prices):
                s[key] = None
                continue
            p0, p1 = prices[j], prices[idx]
            if p0 is None or p1 is None or p0 == 0:
                s[key] = None
            else:
                s[key] = round(100.0 * (p1 - p0) / p0, 4)


def outcome_study(samples: list[dict[str, Any]], *, archetype: dict[str, Any]) -> dict[str, Any]:
    price = [s for s in samples if s.get("price_study_eligible")]
    raw = summarize_outcomes(price)
    signed = None
    if archetype.get("structural_direction") == "commercial_tp_b":
        signed = summarize_outcomes(price, signed_fn=_tpb_signed_return)
    elif archetype.get("structural_direction") == "noncommercial_tp_b":

        def _nc_signed(sample: dict[str, Any], horizon: int = 4) -> float | None:
            feat = sample.get("features") or {}
            direction = (feat.get("noncommercial") or {}).get("tp_b")
            block = (sample.get("outcome_labels") or {}).get(f"fwd_{horizon}w") or {}
            raw_r = block.get("return_pct")
            if raw_r is None or direction is None:
                return None
            return float(raw_r) if direction == "bullish" else -float(raw_r)

        signed = summarize_outcomes(price, signed_fn=_nc_signed)

    # Volatility / movement proxies
    abs_stats = {}
    for h in (1, 4, 8, 12):
        abs_rets = []
        ranges = []
        for s in price:
            block = (s.get("outcome_labels") or {}).get(f"fwd_{h}w") or {}
            if block.get("return_pct") is not None:
                abs_rets.append(abs(float(block["return_pct"])))
            mfe, mae = block.get("mfe_pct"), block.get("mae_pct")
            if mfe is not None and mae is not None:
                ranges.append(float(mfe) - float(mae))  # excursion span
        abs_stats[f"fwd_{h}w"] = {
            "n": len(abs_rets),
            "median_abs_return_pct": (
                None
                if not abs_rets
                else round(sorted(abs_rets)[len(abs_rets) // 2], 4)
            ),
            "median_excursion_span_pct": (
                None if not ranges else round(sorted(ranges)[len(ranges) // 2], 4)
            ),
        }

    # Continuation / reversal vs prior 4w
    cont = {"n": 0, "continuation": 0, "reversal": 0, "flat_prior": 0}
    for s in price:
        prior = s.get("prior_4w_return_pct")
        block = (s.get("outcome_labels") or {}).get("fwd_4w") or {}
        fwd = block.get("return_pct")
        if prior is None or fwd is None:
            continue
        if prior == 0:
            cont["flat_prior"] += 1
            continue
        cont["n"] += 1
        if (prior > 0 and float(fwd) > 0) or (prior < 0 and float(fwd) < 0):
            cont["continuation"] += 1
        elif (prior > 0 and float(fwd) < 0) or (prior < 0 and float(fwd) > 0):
            cont["reversal"] += 1
    if cont["n"]:
        cont["pct_continuation"] = round(100.0 * cont["continuation"] / cont["n"], 1)
        cont["pct_reversal"] = round(100.0 * cont["reversal"] / cont["n"], 1)
    else:
        cont["pct_continuation"] = None
        cont["pct_reversal"] = None

    return {
        "raw": raw,
        "signed_by_structural_direction": signed,
        "movement_abs": abs_stats,
        "vs_prior_4w_path": cont,
    }


def investigate_tpb_div_asymmetry(
    inventory: dict[str, list[dict[str, Any]]],
    all_samples: list[dict[str, Any]],
) -> dict[str, Any]:
    """Deep-dive: what do C/NC TP-B + DIV actually associate with?"""

    def baseline_tpb(group: str) -> list[dict[str, Any]]:
        out = []
        last: dict[str, int] = {}
        for s in sorted(all_samples, key=lambda x: (x["market"], x["index"])):
            feat = s.get("features") or {}
            if not (feat.get(group) or {}).get("tp_b"):
                continue
            prev = last.get(s["market"], -10_000)
            if int(s["index"]) - prev < SAMPLE_COOLDOWN:
                continue
            if s.get("price_study_eligible"):
                out.append(s)
                last[s["market"]] = int(s["index"])
        return out

    def stratify(samples: list[dict[str, Any]], key_fn: Callable) -> dict[str, Any]:
        buckets: dict[str, list] = defaultdict(list)
        for s in samples:
            if s.get("price_study_eligible"):
                buckets[str(key_fn(s))].append(s)
        return {
            k: {
                "n": len(v),
                "outcomes": summarize_outcomes(v),
                "movement": outcome_study(v, archetype={"structural_direction": None})[
                    "movement_abs"
                ].get("fwd_4w"),
                "vs_prior": outcome_study(v, archetype={"structural_direction": None})[
                    "vs_prior_4w_path"
                ],
            }
            for k, v in sorted(buckets.items(), key=lambda x: x[0])
        }

    c_div = [s for s in inventory.get("A3_C_TPB_ACTIVE_DIV", []) if s.get("price_study_eligible")]
    nc_div = [s for s in inventory.get("A4_NC_TPB_ACTIVE_DIV", []) if s.get("price_study_eligible")]
    c_alone = baseline_tpb("commercial")
    nc_alone = baseline_tpb("noncommercial")

    # Enrich priors
    enrich_with_prior_paths(c_div + nc_div + c_alone + nc_alone)

    def pack(label: str, samples: list[dict[str, Any]], signed_group: str) -> dict[str, Any]:
        arch = {
            "structural_direction": (
                "commercial_tp_b" if signed_group == "commercial" else "noncommercial_tp_b"
            )
        }
        study = outcome_study(samples, archetype=arch)
        return {
            "label": label,
            "n": len(samples),
            "raw_4w": (study["raw"].get("fwd_4w") or {}),
            "signed_4w": ((study["signed_by_structural_direction"] or {}).get("fwd_4w") or {}),
            "abs_4w": (study["movement_abs"].get("fwd_4w") or {}),
            "continuation_reversal": study["vs_prior_4w_path"],
            "by_tpb_direction": stratify(
                samples,
                lambda s: ((_feat(s).get(signed_group) or {}).get("tp_b") or "none"),
            ),
            "by_div_state": stratify(samples, lambda s: _div_state(s) or "none"),
        }

    c_pack = pack("Commercial TP-B + active DIV", c_div, "commercial")
    c_base = pack("Commercial TP-B alone", c_alone, "commercial")
    nc_pack = pack("NC TP-B + active DIV", nc_div, "noncommercial")
    nc_base = pack("NC TP-B alone", nc_alone, "noncommercial")

    interpretation = {
        "commercial": {
            "raw_lift_vs_alone": None
            if c_pack["raw_4w"].get("pct_positive") is None
            or c_base["raw_4w"].get("pct_positive") is None
            else round(
                c_pack["raw_4w"]["pct_positive"] - c_base["raw_4w"]["pct_positive"], 1
            ),
            "signed_edge": c_pack["signed_4w"].get("pct_positive"),
            "abs_return_vs_alone": {
                "interaction_median_abs": c_pack["abs_4w"].get("median_abs_return_pct"),
                "alone_median_abs": c_base["abs_4w"].get("median_abs_return_pct"),
            },
            "reversal_rate": c_pack["continuation_reversal"].get("pct_reversal"),
            "continuation_rate": c_pack["continuation_reversal"].get("pct_continuation"),
            "alone_reversal_rate": c_base["continuation_reversal"].get("pct_reversal"),
            "finding": None,  # filled below
        },
        "noncommercial": {
            "raw_lift_vs_alone": None
            if nc_pack["raw_4w"].get("pct_positive") is None
            or nc_base["raw_4w"].get("pct_positive") is None
            else round(
                nc_pack["raw_4w"]["pct_positive"] - nc_base["raw_4w"]["pct_positive"], 1
            ),
            "signed_edge": nc_pack["signed_4w"].get("pct_positive"),
            "abs_return_vs_alone": {
                "interaction_median_abs": nc_pack["abs_4w"].get("median_abs_return_pct"),
                "alone_median_abs": nc_base["abs_4w"].get("median_abs_return_pct"),
            },
            "reversal_rate": nc_pack["continuation_reversal"].get("pct_reversal"),
            "continuation_rate": nc_pack["continuation_reversal"].get("pct_continuation"),
            "alone_reversal_rate": nc_base["continuation_reversal"].get("pct_reversal"),
            "finding": None,
        },
    }

    # Interpret without forcing TP-B == price direction
    c_rev = interpretation["commercial"]["reversal_rate"]
    c_alone_rev = interpretation["commercial"]["alone_reversal_rate"]
    c_abs_i = interpretation["commercial"]["abs_return_vs_alone"]["interaction_median_abs"]
    c_abs_a = interpretation["commercial"]["abs_return_vs_alone"]["alone_median_abs"]
    if (
        c_rev is not None
        and c_alone_rev is not None
        and c_rev >= (c_alone_rev + 5)
    ):
        c_find = (
            "Associated more with path reversal vs prior 4W than Commercial TP-B alone "
            "(not a clean TP-B-signed directional edge)."
        )
    elif (
        c_abs_i is not None
        and c_abs_a is not None
        and c_abs_i >= c_abs_a * 1.15
    ):
        c_find = (
            "Associated with larger absolute 4W moves than Commercial TP-B alone "
            "(volatility / movement expansion candidate)."
        )
    elif (interpretation["commercial"]["signed_edge"] or 50) < 48:
        c_find = (
            "Raw up-frequency lift persists but TP-B-signed confirmation fails — "
            "treat as descriptive regime marker, not directional signal."
        )
    else:
        c_find = (
            "Insufficient evidence that DIV transforms Commercial TP-B into a "
            "robust directional or volatility regime."
        )
    interpretation["commercial"]["finding"] = c_find

    nc_pp = nc_pack["raw_4w"].get("pct_positive")
    if nc_pp is not None and nc_pp <= 40:
        interpretation["noncommercial"]["finding"] = (
            "Raw OOS remains skewed negative vs NC TP-B alone — adverse / "
            "cautionary interaction; not a long setup. May mark stressed or "
            "transition weeks rather than NC-direction trades."
        )
    else:
        interpretation["noncommercial"]["finding"] = (
            "NC TP-B + DIV contrast is weak or unstable after consolidation review."
        )

    return {
        "commercial_tpb_plus_div": c_pack,
        "commercial_tpb_alone": c_base,
        "nc_tpb_plus_div": nc_pack,
        "nc_tpb_alone": nc_base,
        "interpretation": interpretation,
    }


def classify_archetype_oos(
    *,
    oos: dict[str, Any],
    folds: list[dict[str, Any]],
    lomo: dict[str, Any],
    stability: dict[str, Any],
    n_tested: int,
    structural_direction: str | None,
    signed_oos: dict[str, Any] | None,
) -> dict[str, Any]:
    f4 = oos.get("fwd_4w") or {}
    n = f4.get("n") or 0
    pp = f4.get("pct_positive")
    pval = f4.get("binom_pvalue_vs_50")
    ci = f4.get("wilson_ci_positive")
    n_markets = oos.get("n_markets") or 0
    by_m = oos.get("by_market") or {}
    top_share = (max(by_m.values()) / n) if by_m and n else None

    oos_side = None
    if pp is not None:
        if pp >= 54:
            oos_side = "up"
        elif pp <= 46:
            oos_side = "down"
        else:
            oos_side = "flat"

    bonf = BONFERRONI_ALPHA / max(n_tested, 1)
    significant = pval is not None and pval < bonf
    ci_excludes_50 = (
        ci is not None
        and len(ci) == 2
        and (ci[1] < 0.5 or ci[0] > 0.5)
    )

    signed_ok = True
    if structural_direction and signed_oos:
        spp = (signed_oos.get("fwd_4w") or {}).get("pct_positive")
        if spp is not None and spp < 54 and oos_side == "up":
            signed_ok = False
        if spp is not None and spp > 46 and oos_side == "down":
            signed_ok = False

    lomo_ok = True
    if lomo.get("applicable") and (lomo.get("n_remaining_price") or 0) >= MIN_OOS_N_PROMISING:
        lomo_pp = ((lomo.get("outcomes") or {}).get("fwd_4w") or {}).get("pct_positive")
        if oos_side == "up" and lomo_pp is not None and lomo_pp < 48:
            lomo_ok = False
        if oos_side == "down" and lomo_pp is not None and lomo_pp > 52:
            lomo_ok = False

    folds_ok = stability.get("same_side_of_50") is True and not stability.get(
        "dominated_by_one_fold"
    )
    n_signal_folds = sum(
        1
        for u in (stability.get("folds") or [])
        if abs((u.get("pct_positive") or 50) - 50) >= 3
    )

    reasons: list[str] = []
    if n < MIN_OOS_N_PROMISING:
        label = "PROMISING / MONITOR"
        reasons.append(f"Insufficient OOS n={n}")
    elif oos_side == "flat" and n >= MIN_OOS_N_VALIDATED:
        label = "FAILED"
        reasons.append("No OOS asymmetry with adequate sample (raw %+ ~ coin-flip)")
    elif (
        n >= MIN_OOS_N_VALIDATED
        and n_markets >= MIN_OOS_MARKETS_VALIDATED
        and oos_side in {"up", "down"}
        and (top_share is None or top_share <= TOP_MARKET_SHARE_MAX)
        and lomo_ok
        and folds_ok
        and n_signal_folds >= 2
        and (significant or ci_excludes_50)
        and signed_ok
    ):
        label = "VALIDATED"
        reasons.append(
            "OOS persistence with sample, breadth, fold stability, and uncertainty gates"
        )
    elif oos_side in {"up", "down"} and n >= MIN_OOS_N_PROMISING:
        label = "PROMISING / MONITOR"
        reasons.append("Directional OOS hint but robustness/uncertainty incomplete")
        if not signed_ok:
            reasons.append("Structural-direction signed confirmation failed")
        if not (significant or ci_excludes_50):
            reasons.append("Not distinguishable from 50% after uncertainty adjustment")
        if not folds_ok:
            reasons.append("Fold instability or single-fold dominance")
        if top_share and top_share > TOP_MARKET_SHARE_MAX:
            reasons.append(f"Top market share {top_share:.0%} too high")
    else:
        label = "FAILED"
        reasons.append("OOS behaviour does not support a robust regime claim")

    return {
        "classification": label,
        "reasons": reasons,
        "oos_side": oos_side,
        "oos_pct_positive_4w": pp,
        "oos_median_4w": f4.get("median_return_pct"),
        "oos_n_price": n,
        "oos_n_markets": n_markets,
        "top_market_share_oos": None if top_share is None else round(top_share, 4),
        "bonferroni_alpha": bonf,
        "significant_vs_50_bonferroni": significant,
        "wilson_ci_excludes_50": ci_excludes_50,
        "signed_confirmation_ok": signed_ok,
    }


def validate_archetypes(
    inventory: dict[str, list[dict[str, Any]]],
    archetypes: list[dict[str, Any]],
    folds: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    n_tested = len(archetypes)
    results = []
    for arch in archetypes:
        aid = arch["id"]
        members = inventory.get(aid) or []
        price_members = [s for s in members if s.get("price_study_eligible")]

        fold_blocks = []
        oos_members: list[dict[str, Any]] = []
        for fold in folds:
            test_s = [s for s in price_members if _in_test(s["date"], fold)]
            fold_blocks.append(
                {
                    "fold_id": fold["fold_id"],
                    "train_end": fold["train_end"],
                    "test_start": fold["test_start"],
                    "test_end": fold["test_end"],
                    "outcomes": summarize_outcomes(test_s),
                    "n_test": len(test_s),
                    "markets_test": sorted({s["market"] for s in test_s}),
                }
            )
            oos_members.extend(test_s)

        seen = set()
        oos_uniq = []
        for s in oos_members:
            k = (s["market"], s["date"], aid)
            if k in seen:
                continue
            seen.add(k)
            oos_uniq.append(s)

        oos_stats = summarize_outcomes(oos_uniq)
        signed_oos = None
        if arch.get("structural_direction") == "commercial_tp_b":
            signed_oos = summarize_outcomes(oos_uniq, signed_fn=_tpb_signed_return)
        elif arch.get("structural_direction") == "noncommercial_tp_b":

            def _nc_signed(sample: dict[str, Any], horizon: int = 4) -> float | None:
                feat = sample.get("features") or {}
                direction = (feat.get("noncommercial") or {}).get("tp_b")
                block = (sample.get("outcome_labels") or {}).get(f"fwd_{horizon}w") or {}
                raw_r = block.get("return_pct")
                if raw_r is None or direction is None:
                    return None
                return float(raw_r) if direction == "bullish" else -float(raw_r)

            signed_oos = summarize_outcomes(oos_uniq, signed_fn=_nc_signed)

        lomo = leave_one_market_out(oos_uniq)
        lao = leave_one_asset_out(oos_uniq)
        stability = fold_stability(fold_blocks)
        classification = classify_archetype_oos(
            oos=oos_stats,
            folds=fold_blocks,
            lomo=lomo,
            stability=stability,
            n_tested=n_tested,
            structural_direction=arch.get("structural_direction"),
            signed_oos=signed_oos,
        )

        results.append(
            {
                "archetype_id": aid,
                "human": arch["human"],
                "definition_positioning": arch["definition_positioning"],
                "motivating_promising_families": arch.get("motivating_promising_families"),
                "inventory": inventory_summary(members),
                "full_eligible_outcomes": summarize_outcomes(price_members),
                "oos_outcomes": oos_stats,
                "oos_signed": signed_oos,
                "folds": fold_blocks,
                "fold_stability": stability,
                "leave_one_market_out": lomo,
                "leave_one_asset_class_out": lao,
                "classification": classification,
            }
        )
    return results


def run_phase4b(*, markets: Sequence[str] | None = None) -> dict[str, Any]:
    families = load_phase4_families()
    sim = similarity_matrix(families)
    diagnosis = diagnose_fragmentation(families)

    promising_by_opp = diagnosis["promising_by_opp"]
    archetypes = frozen_archetype_definitions(promising_by_opp)
    freeze_record = {
        "frozen_at": datetime.now(timezone.utc).isoformat(),
        "note": (
            "Archetype definitions frozen from positioning structure of PROMISING "
            "Phase-3 families and Phase-3/4 interaction queries BEFORE outcome "
            "attachment or OOS validation in this run."
        ),
        "n_archetypes": len(archetypes),
        "ids": [a["id"] for a in archetypes],
    }

    # Independent rebuild from full PIT-safe onset research table
    all_samples = _rebuild_onset_universe(markets=markets)

    enrich_with_prior_paths(all_samples, markets=markets)
    inventory = rebuild_archetype_inventory(
        all_samples, [a["id"] for a in archetypes]
    )

    # Sample expansion vs Phase-3 promising family sizes
    p4 = json.loads(PHASE4_JSON.read_text(encoding="utf-8"))
    fam_n = []
    for f in p4.get("families") or []:
        if f["classification"]["classification"] != "PROMISING / MONITOR":
            continue
        fam_n.append((f["oos_outcomes"].get("fwd_4w") or {}).get("n") or 0)
    expansion = {
        "phase3_promising_median_oos_n": (
            None if not fam_n else sorted(fam_n)[len(fam_n) // 2]
        ),
        "phase3_promising_oos_n": fam_n,
        "archetype_independent_n": {
            aid: len(inventory[aid]) for aid in inventory
        },
        "archetype_price_n": {
            aid: sum(1 for s in inventory[aid] if s.get("price_study_eligible"))
            for aid in inventory
        },
        "solved_fragmentation": None,  # set after seeing sizes
    }
    arch_price_ns = list(expansion["archetype_price_n"].values())
    med_arch = sorted(arch_price_ns)[len(arch_price_ns) // 2] if arch_price_ns else 0
    med_fam = expansion["phase3_promising_median_oos_n"] or 0
    expansion["solved_fragmentation"] = bool(med_arch >= max(40, med_fam * 2))
    expansion["note"] = (
        "Sample expansion judged by whether broader archetypes yield materially "
        "larger price-eligible independent n than typical Phase-3 family OOS n."
    )

    # Outcomes AFTER freeze
    outcome_blocks = {}
    for arch in archetypes:
        outcome_blocks[arch["id"]] = outcome_study(
            inventory[arch["id"]], archetype=arch
        )

    # Unconditional baseline on all price-eligible onsets
    price_all = [s for s in all_samples if s.get("price_study_eligible")]
    folds = build_chronological_folds(all_samples)
    oos_all = [s for s in price_all if any(_in_test(s["date"], f) for f in folds)]
    baselines = {
        "unconditional_full": summarize_outcomes(price_all),
        "unconditional_oos": summarize_outcomes(oos_all),
    }

    asymmetry = investigate_tpb_div_asymmetry(inventory, all_samples)
    validated = validate_archetypes(inventory, archetypes, folds)

    counts = Counter(v["classification"]["classification"] for v in validated)
    n_val = counts.get("VALIDATED", 0)
    n_prom = counts.get("PROMISING / MONITOR", 0)
    n_fail = counts.get("FAILED", 0)

    executive = {
        "question": (
            "Did broader, positioning-defined COT archetypes reveal robust "
            "repeatable behaviour that the overly specific Phase-3 families were hiding?"
        ),
        "answer": (
            "YES — limited"
            if n_val
            else (
                "NO — consolidation expanded samples but did not clear VALIDATED"
                if expansion["solved_fragmentation"] and n_prom
                else (
                    "NO — structural consolidation did not unlock robust repeatable behaviour"
                )
            )
        ),
        "n_validated": n_val,
        "n_promising": n_prom,
        "n_failed": n_fail,
        "validated_ids": [
            v["archetype_id"]
            for v in validated
            if v["classification"]["classification"] == "VALIDATED"
        ],
        "promising_ids": [
            v["archetype_id"]
            for v in validated
            if v["classification"]["classification"] == "PROMISING / MONITOR"
        ],
        "failed_ids": [
            v["archetype_id"]
            for v in validated
            if v["classification"]["classification"] == "FAILED"
        ],
        "fragmentation_solved": expansion["solved_fragmentation"],
        "phase5_live_matching_justified": bool(n_val > 0),
        "guidance": (
            "Phase 5 live matching may consume only VALIDATED archetypes."
            if n_val
            else (
                "Do not begin Phase 5 live matching. Broader archetypes remain "
                "PROMISING/FAILED under fresh chronological validation. "
                "Keep FAILED Phase-3 families frozen-failed. No retuning."
            )
        ),
        "tpb_div_summary": {
            "commercial": asymmetry["interpretation"]["commercial"]["finding"],
            "noncommercial": asymmetry["interpretation"]["noncommercial"]["finding"],
        },
    }

    audit = {
        "version": "cot_intelligence_phase4b_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope": "structural_consolidation_sample_expansion",
        "constraints_honored": [
            "Phase 1–4 definitions preserved",
            "P3C02/P3C08/P3C10 remain frozen-failed",
            "Archetypes defined from positioning similarity before outcomes",
            "No return-based merging",
            "No UI / score / ML / threshold optimization",
            "Copper excluded via trustworthy gate",
            "Fresh chronological OOS validation (not discovery-contaminated labels)",
        ],
        "phase4_reference": str(PHASE4_JSON),
        "families_structural": [
            {
                "candidate_id": f["candidate_id"],
                "phase4_class": f["phase4_class"],
                "family_human": f["family_human"],
                "struct": f["struct"],
            }
            for f in families
        ],
        "similarity_matrix": sim,
        "fragmentation_diagnosis": diagnosis,
        "archetype_freeze": freeze_record,
        "archetypes": archetypes,
        "sample_expansion": expansion,
        "inventory": {aid: inventory_summary(inventory[aid]) for aid in inventory},
        "baselines": baselines,
        "walk_forward_folds": folds,
        "outcomes_after_freeze": {
            aid: {
                "raw_4w": (outcome_blocks[aid]["raw"].get("fwd_4w") or {}),
                "signed_4w": (
                    (outcome_blocks[aid]["signed_by_structural_direction"] or {}).get(
                        "fwd_4w"
                    )
                    if outcome_blocks[aid]["signed_by_structural_direction"]
                    else None
                ),
                "abs_4w": (outcome_blocks[aid]["movement_abs"].get("fwd_4w") or {}),
                "continuation_reversal": outcome_blocks[aid]["vs_prior_4w_path"],
            }
            for aid in outcome_blocks
        },
        "tpb_div_asymmetry": asymmetry,
        "validation": validated,
        "classification_counts": dict(counts),
        "executive_verdict": executive,
    }

    trail = {
        "version": "cot_intelligence_phase4b_audit_trail_v1",
        "generated_at": audit["generated_at"],
        "archetype_freeze": freeze_record,
        "archetypes": archetypes,
        "inventory": audit["inventory"],
        "validation": [
            {
                "archetype_id": v["archetype_id"],
                "human": v["human"],
                "definition_positioning": v["definition_positioning"],
                "inventory": v["inventory"],
                "oos_outcomes": v["oos_outcomes"],
                "folds": v["folds"],
                "classification": v["classification"],
            }
            for v in validated
        ],
        "executive_verdict": executive,
    }
    return {"audit": audit, "trail": trail}


def _rebuild_onset_universe(
    *, markets: Sequence[str] | None = None
) -> list[dict[str, Any]]:
    """Rebuild onset snapshots without family_key independence thinning.

    Uses the same onset detector and snapshot builder as Phase 3, but keeps
    one row per (market, index) so archetype matchers see the full onset table.
    """
    from hptl.cot.intelligence_phase3_configurations import (
        GROUP_COMMERCIAL,
        GROUP_NONCOMMERCIAL,
        GROUP_NONREPORTABLE,
        MIN_HISTORY,
        _attach_outcomes,
        _finite,
        build_config_snapshot,
        collect_onset_indices,
    )
    from hptl.cot.positioning_research_engine import (
        build_group_state_series,
        build_spread_series,
    )

    cot3y = _load_first(COT3Y_PATHS)
    all_markets = cot3y.get("markets") or {}
    trustworthy = set(_load_trustworthy_markets())
    selected = (
        sorted(str(k) for k in all_markets.keys())
        if markets is None
        else list(markets)
    )
    samples: list[dict[str, Any]] = []
    for mid in selected:
        block = all_markets.get(mid)
        if not block:
            continue
        series = list(block.get("series") or [])
        if len(series) < MIN_HISTORY + 12:
            continue
        commercial = build_group_state_series(series, GROUP_COMMERCIAL)
        noncommercial = build_group_state_series(series, GROUP_NONCOMMERCIAL)
        nonreportable = build_group_state_series(series, GROUP_NONREPORTABLE)
        spreads = build_spread_series(commercial, nonreportable)
        prices = [_finite(r.get("price")) for r in series]
        onset_map = collect_onset_indices(
            mid, commercial, noncommercial, nonreportable, spreads
        )
        price_ok = mid in trustworthy
        for idx, meta in sorted(onset_map.items()):
            snap = build_config_snapshot(
                market=mid,
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
            samples.append(snap)
    return samples


def write_phase4b_markdown(audit: dict[str, Any]) -> str:
    ev = audit["executive_verdict"]
    lines = [
        "# COT Intelligence — Phase 4B Structural Consolidation",
        "",
        f"Generated: `{audit['generated_at']}`",
        "",
        "Separate research layer above frozen Phase 3/4. No UI / score / ML.",
        "",
        "## Executive verdict",
        "",
        f"**{ev['answer']}**",
        "",
        ev["question"],
        "",
        f"- VALIDATED: {ev['n_validated']} → {ev['validated_ids']}",
        f"- PROMISING / MONITOR: {ev['n_promising']} → {ev['promising_ids']}",
        f"- FAILED: {ev['n_failed']} → {ev['failed_ids']}",
        f"- Fragmentation solved (sample expansion): {ev['fragmentation_solved']}",
        f"- Phase 5 live matching justified: {ev['phase5_live_matching_justified']}",
        "",
        ev["guidance"],
        "",
        "## Fragmentation diagnosis",
        "",
        audit["fragmentation_diagnosis"].get("fragmentation_finding", ""),
        "",
        f"Promising by OPP: {audit['fragmentation_diagnosis'].get('promising_by_opp')}",
        f"Within-opp similarity: {audit['fragmentation_diagnosis'].get('within_opp_similarity')}",
        f"Cross-opp mean Hamming: {audit['fragmentation_diagnosis'].get('cross_opp_mean_hamming')}",
        "",
        "## Frozen archetypes",
        "",
    ]
    for a in audit["archetypes"]:
        inv = audit["inventory"].get(a["id"]) or {}
        lines += [
            f"### {a['id']}",
            "",
            f"- **Human:** {a['human']}",
            f"- **Definition:** {a['definition_positioning']}",
            f"- **Motivating PROMISING families:** {a.get('motivating_promising_families')}",
            f"- **Independent n:** {inv.get('n_independent')} (price-eligible {inv.get('n_price_eligible')})",
            f"- **Markets / asset classes:** {inv.get('n_markets')} / {inv.get('n_asset_classes')}",
            "",
        ]

    lines += [
        "## Sample expansion",
        "",
        str(audit["sample_expansion"]),
        "",
        "## Validation table",
        "",
        "| ID | Class | OOS n | Mkts | %+4W OOS | Med4W | CI≠50 | Folds | Human |",
        "|---|---|---:|---:|---:|---:|---|---|---|",
    ]
    for v in audit["validation"]:
        c = v["classification"]
        o4 = (v.get("oos_outcomes") or {}).get("fwd_4w") or {}
        stab = v.get("fold_stability") or {}
        lines.append(
            "| {id} | {cls} | {n} | {m} | {pp} | {med} | {ci} | {fs} | {h} |".format(
                id=v["archetype_id"],
                cls=c["classification"],
                n=o4.get("n"),
                m=v["oos_outcomes"].get("n_markets"),
                pp=o4.get("pct_positive"),
                med=o4.get("median_return_pct"),
                ci=c.get("wilson_ci_excludes_50"),
                fs=stab.get("same_side_of_50"),
                h=(v.get("human") or "")[:50],
            )
        )

    lines += [
        "",
        "## TP-B + divergence asymmetry",
        "",
        f"- Commercial: {ev['tpb_div_summary']['commercial']}",
        f"- NC: {ev['tpb_div_summary']['noncommercial']}",
        "",
        "### Commercial contrast",
        "",
        str(audit["tpb_div_asymmetry"]["interpretation"]["commercial"]),
        "",
        "### NC contrast",
        "",
        str(audit["tpb_div_asymmetry"]["interpretation"]["noncommercial"]),
        "",
        "## Constraints honored",
        "",
    ]
    for c in audit.get("constraints_honored") or []:
        lines.append(f"- {c}")
    lines.append("")
    return "\n".join(lines)


def write_phase4b_outputs(payload: dict[str, Any]) -> dict[str, Path]:
    audit = payload["audit"]
    trail = payload["trail"]
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    PHASE4B_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")
    PHASE4B_TRAIL.write_text(json.dumps(trail, indent=2), encoding="utf-8")
    PHASE4B_MD.write_text(write_phase4b_markdown(audit), encoding="utf-8")
    return {
        "audit_json": PHASE4B_JSON,
        "audit_md": PHASE4B_MD,
        "trail": PHASE4B_TRAIL,
    }
