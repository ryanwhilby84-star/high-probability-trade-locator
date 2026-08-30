from __future__ import annotations

from hptl.seasonality_workstation.production_roadmap import (
    METHOD_VERSION,
    NO_EDGE,
    apply_production_seasonality,
    build_production_roadmap,
)
from hptl.seasonality_workstation.returns import weekly_return_rows


def _research(*, wf_hit: float = 0.70, wf_n: int = 10, agreement: float = 0.8):
    week_stats = {}
    aligned = {}
    index_path = {}
    level = 100.0
    for w in range(1, 53):
        r = 0.002 if w % 3 else -0.001
        level *= 1 + r
        week_stats[str(w)] = {
            "n": 12,
            "mean": r,
            "median": r * 0.9,
            "trimmed_mean": r,
            "positive_frequency": 0.67 if r > 0 else 0.33,
            "dispersion": 0.018,
        }
        index_path[str(w)] = level
        aligned[str(w)] = level * 10

    block = {
        "sample_years": list(range(2013, 2025)),
        "sample_size": 12,
        "week_stats": week_stats,
        "index_paths": {"trimmed_mean": index_path},
        "price_aligned": {"trimmed_mean": aligned},
        "forward_horizons": {
            "4w": {
                "mean_return": 0.012,
                "median_return": 0.010,
                "positive_frequency": 0.67,
                "n": 12,
                "dispersion": 0.025,
            },
            "8w": {
                "mean_return": 0.022,
                "median_return": 0.019,
                "positive_frequency": 0.67,
                "n": 12,
                "dispersion": 0.03,
            },
            "12w": {
                "mean_return": 0.028,
                "median_return": 0.024,
                "positive_frequency": 0.67,
                "n": 12,
                "dispersion": 0.035,
            },
        },
    }
    return {
        "status": "ok",
        "selected_lookback": "15Y",
        "lookbacks": {"15Y": block},
        "anchor": {
            "date": "2026-08-28",
            "price": 1012.5,
            "iso_year": 2026,
            "iso_week": 35,
        },
        "integrity": {"status": "PASS"},
        "lookback_agreement": {"score": agreement},
        "walk_forward": {"hit_rate": wf_hit, "n": wf_n},
        "seasonality": {"primary": "indexed_year_path"},
        "display_defaults": {},
        "weekly_roadmap": {
            "comparison": {
                "monthly_roadmap_direction": "Neutral",
                "monthly_label": "Monthly Roadmap",
            }
        },
    }


def test_production_roadmap_is_unsmoothed_weekly_return_path():
    roadmap = build_production_roadmap(_research())
    assert roadmap["available"] is True
    assert roadmap["method"]["version"] == METHOD_VERSION
    assert roadmap["method"]["aggregation"] == "10pct_trimmed_mean_weekly_return"
    assert roadmap["method"]["interpolation"] == "none_in_payload"
    assert roadmap["method"]["smooth"] is None
    assert roadmap["smoothed"] is None
    assert len(roadmap["unsmoothed"]["full_year"]) == 52
    assert roadmap["unsmoothed"]["full_year"][34]["segment"] == "today"
    assert roadmap["unsmoothed"]["full_year"][34]["price"] == 1012.5


def test_production_adapter_replaces_legacy_primary_without_touching_cot():
    research = _research()
    research["cot"] = {"sentinel": "must_remain_independent"}
    out = apply_production_seasonality(research)
    assert out["seasonality"]["primary"] == METHOD_VERSION
    assert out["seasonal_roadmap"]["method"]["version"] == METHOD_VERSION
    assert out["monthly_roadmap"]["method"]["version"] == METHOD_VERSION
    assert out["display_defaults"]["roadmap_smoothed"] is False
    assert out["cot"] == {"sentinel": "must_remain_independent"}


def test_reliability_refuses_weak_out_of_sample_evidence():
    roadmap = build_production_roadmap(_research(wf_hit=0.40, wf_n=10))
    reliability = roadmap["reliability"]
    assert reliability["reliable"] is False
    assert reliability["verdict"] == NO_EDGE
    assert any("walk_forward" in reason for reason in reliability["reasons"])


def test_missing_week_is_not_bridged_into_one_week_return():
    weekly = [
        ("2026-01-09", 100.0),
        ("2026-01-16", 102.0),
        # 2026-01-23 intentionally missing
        ("2026-01-30", 110.0),
    ]
    rows = weekly_return_rows(weekly)
    assert rows[1]["return"] == 0.02
    assert rows[2]["return"] is None
