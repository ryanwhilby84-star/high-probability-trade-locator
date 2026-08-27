"""Phase 3 configuration discovery tests."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.intelligence_phase3_configurations import (
    SAMPLE_COOLDOWN,
    analyze_families,
    build_config_snapshot,
    compose_family_key,
    hamming_family_distance,
)
from hptl.cot.positioning_research_engine import (
    GROUP_COMMERCIAL,
    MIN_HISTORY,
    build_group_state_series,
    build_spread_series,
)


def _series(n: int = MIN_HISTORY + 120):
    start = date(2016, 1, 5)
    rows = []
    price = 100.0
    for i in range(n):
        phase = (i % 40) / 40.0
        c = -40_000 + phase * 80_000
        nr = 20_000 - phase * 50_000
        nc = -c * 0.6
        price *= 1.0 + ((i % 7) - 3) * 0.002
        rows.append(
            {
                "date": (start + timedelta(weeks=i)).isoformat(),
                "commercial_net": c,
                "institutional_net": nc,
                "retail_net": nr,
                "price": price,
            }
        )
    return rows


def test_family_key_is_interpretable_and_stable():
    series = _series()
    c = build_group_state_series(series, GROUP_COMMERCIAL)
    nc = build_group_state_series(series, "noncommercial")
    nr = build_group_state_series(series, "nonreportable")
    spreads = build_spread_series(c, nr)
    i = MIN_HISTORY + 20
    snap = build_config_snapshot(
        market="Test",
        index=i,
        date=c[i]["date"],
        commercial=c[i],
        noncommercial=nc[i],
        nonreportable=nr[i],
        spread=spreads[i],
        spread_prev=spreads[i - 1],
        tp_b_by_group={"commercial": "bullish", "noncommercial": None, "nonreportable": None},
        onset_triggers=["tpB_commercial"],
    )
    assert "family_key" in snap
    assert "C[" in snap["family_key"]
    assert "tpB=bullish" in snap["family_key"] or "tpB=bullish" in snap["family_human"]
    assert "outcome_labels" not in snap.get("features", {})


def test_hamming_distance_explains_dissimilarity():
    a = compose_family_key(
        c={"pct_bin": "xh", "extreme_zone": "high", "tp_b": "bearish", "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "dn"},
        nc={"pct_bin": "xl", "extreme_zone": "low", "tp_b": None, "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "up"},
        nr={"pct_bin": "mid", "extreme_zone": None, "tp_b": None, "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "flat"},
        div_state="high",
        expanding="expanding",
        ordering="C>NR>NC",
        opposing="C_hi_NC_lo",
        onset_triggers=["tpB_commercial"],
    )
    b = dict(a)
    # identical parts -> distance 0
    assert hamming_family_distance(a["parts"], a["parts"]) == 0
    other = compose_family_key(
        c={"pct_bin": "mid", "extreme_zone": None, "tp_b": None, "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "flat"},
        nc={"pct_bin": "mid", "extreme_zone": None, "tp_b": None, "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "flat"},
        nr={"pct_bin": "mid", "extreme_zone": None, "tp_b": None, "major_rotation_active": False, "major_rotation_direction": None, "vel_4w_bin": "flat"},
        div_state=None,
        expanding=None,
        ordering="C>NC>NR",
        opposing=None,
        onset_triggers=["div_onset"],
    )
    assert hamming_family_distance(a["parts"], other["parts"]) >= 3


def test_analyze_families_labels_without_using_returns_for_identity():
    samples = []
    for i in range(25):
        samples.append(
            {
                "market": f"M{i % 5}",
                "asset_class": "fx" if i % 2 == 0 else "metals",
                "index": i * SAMPLE_COOLDOWN,
                "family_key": "FAM_STABLE",
                "family_human": "stable demo",
                "family_parts": {},
                "onset_triggers": ["tpB_commercial"],
                "price_study_eligible": True,
                "outcome_labels": {
                    "fwd_4w": {"return_pct": 1.0, "mfe_pct": 2.0, "mae_pct": -0.5}
                },
            }
        )
    # tiny family
    samples.append(
        {
            "market": "Only",
            "asset_class": "ag",
            "index": 0,
            "family_key": "FAM_TINY",
            "family_human": "tiny",
            "family_parts": {},
            "onset_triggers": ["div_onset"],
            "price_study_eligible": False,
            "outcome_labels": None,
        }
    )
    fams = analyze_families(samples)
    by = {f["family_key"]: f["verdict"] for f in fams}
    assert by["FAM_STABLE"] == "candidate"
    assert by["FAM_TINY"] == "reject"
