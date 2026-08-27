"""Phase 4B structural consolidation tests."""

from __future__ import annotations

from hptl.cot.intelligence_phase4b_consolidation import (
    FAILED_FROZEN,
    MATCHERS,
    hamming_distance,
    match_a1_opp_c_hi_nc_lo,
    match_a3_c_tpb_active_div,
    structural_vector,
)


def test_structural_vector_ignores_returns():
    parts = {
        "C": "pct=xh|z=high|tpB=none",
        "NC": "pct=xl|z=low|tpB=none",
        "NR": "pct=lo|z=out|tpB=bullish",
        "DIV": "state=high|trend=contracting",
        "ORD": "C>NR>NC",
        "OPP": "C_hi_NC_lo",
        "ONSET": "tpB_nonreportable",
    }
    human = (
        "C xh/high; NC xl/low; NR lo/out/TP-B-bullish; DIV high(contracting); "
        "order C>NR>NC; opp C_hi_NC_lo; onset tpB"
    )
    v = structural_vector(parts, human)
    assert v["opp"] == "C_hi_NC_lo"
    assert v["c_side"] == "hi"
    assert v["nc_side"] == "lo"
    assert v["div_active"] == "yes"
    assert "return" not in v


def test_hamming_separates_opposition_regimes():
    # Same opposition, only onset class differs → small distance
    a = structural_vector(
        {
            "C": "pct=hi|z=out|tpB=none",
            "NC": "pct=lo|z=out|tpB=none",
            "NR": "pct=mid|z=out|tpB=none",
            "DIV": "state=none|trend=contracting",
            "OPP": "C_hi_NC_lo",
            "ONSET": "x",
        },
        "onset tpB",
    )
    same_opp = structural_vector(
        {
            "C": "pct=hi|z=out|tpB=none",
            "NC": "pct=lo|z=out|tpB=none",
            "NR": "pct=mid|z=out|tpB=none",
            "DIV": "state=none|trend=contracting",
            "OPP": "C_hi_NC_lo",
            "ONSET": "x",
        },
        "onset exOnset",
    )
    # Opposite opposition regime
    mirror = structural_vector(
        {
            "C": "pct=lo|z=out|tpB=none",
            "NC": "pct=hi|z=out|tpB=none",
            "NR": "pct=mid|z=out|tpB=none",
            "DIV": "state=none|trend=contracting",
            "OPP": "C_lo_NC_hi",
            "ONSET": "x",
        },
        "onset tpB",
    )
    assert hamming_distance(a, same_opp) == 1
    assert hamming_distance(a, mirror) >= 3


def test_archetype_matchers_positioning_only():
    sample = {
        "features": {
            "commercial": {"tp_b": "bullish", "in_extreme": False},
            "noncommercial": {"tp_b": None, "in_extreme": False},
            "nonreportable": {"tp_b": None},
            "spread": {"divergence_state": "high", "divergence_trend": "expanding"},
            "relative": {"opposing_c_nc": "C_hi_NC_lo"},
        }
    }
    assert match_a1_opp_c_hi_nc_lo(sample)
    assert match_a3_c_tpb_active_div(sample)
    assert MATCHERS["A5_OPP_PLUS_ACTIVE_DIV"](sample)
    assert not MATCHERS["A2_OPP_C_LO_NC_HI"](sample)


def test_failed_ids_frozen():
    assert set(FAILED_FROZEN) == {"P3C02", "P3C08", "P3C10"}
