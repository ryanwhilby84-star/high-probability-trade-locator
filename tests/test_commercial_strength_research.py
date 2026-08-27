"""Commercial strength research layer tests."""

from __future__ import annotations

from hptl.fx.commercial_strength_research import (
    build_commercial_spec_divergence,
    build_commercial_strength,
    build_research_table_rows,
    compute_commercial_currency_metrics,
)


def _weeks(nets: list[float]) -> list[dict]:
    return [{"report_date": f"2024-01-{i+1:02d}", "net": v, "net_week_change": 100.0} for i, v in enumerate(nets)]


def test_commercial_score_from_percentile():
    # Mid history rising to top of window -> high percentile / positive score
    nets = [float(i) for i in range(160)]
    metrics = compute_commercial_currency_metrics(_weeks(nets), invert_cot=False)
    assert metrics is not None
    assert metrics["percentile"] is not None
    assert metrics["percentile"] >= 90.0
    assert metrics["commercial_score"] == 82 or metrics["commercial_score"] >= 80
    assert metrics["extreme"] == "HIGH"


def test_invert_cot_flips_orientation():
    nets = [10.0, 20.0, 30.0]
    plain = compute_commercial_currency_metrics(_weeks(nets), invert_cot=False)
    inv = compute_commercial_currency_metrics(_weeks(nets), invert_cot=True)
    assert plain["commercial_net_oriented"] == 30.0
    assert inv["commercial_net_oriented"] == -30.0


def test_divergence_is_commercial_minus_spec():
    commercial_doc = {
        "calendar_week": "2026-01-01",
        "currencies": {
            "CHF": {"commercial_score": 82, "cot_market": "Swiss Franc / 6S"},
            "EUR": {"commercial_score": 10, "cot_market": "Euro FX / 6E"},
        },
    }
    rs_doc = {
        "relative_strength": {
            "leaderboard": [
                {"currency": "CHF", "positioning_score": -35.0},
                {"currency": "EUR", "positioning_score": 5.0},
            ]
        }
    }
    div = build_commercial_spec_divergence(commercial_doc, rs_doc)
    assert div["currencies"]["CHF"]["divergence"] == 117.0
    assert div["currencies"]["EUR"]["divergence"] == 5.0


def test_research_table_sorted_by_abs_divergence():
    commercial_doc = {"currencies": {}}
    divergence_doc = {
        "currencies": {
            "CHF": {"spec_score": -35, "commercial_score": 82, "divergence": 117},
            "JPY": {"spec_score": -40, "commercial_score": 65, "divergence": 105},
            "AUD": {"spec_score": 70, "commercial_score": -20, "divergence": -90},
        }
    }
    rows = build_research_table_rows(commercial_doc, divergence_doc)
    codes = [r["currency"] for r in rows if r["currency"] in {"CHF", "JPY", "AUD"}]
    assert codes[0] == "CHF"
    assert codes[1] == "JPY"
    assert codes[2] == "AUD"


def test_build_commercial_strength_all_g10_currencies():
    legacy = {
        "instruments": {
            "Euro FX / 6E": {
                "groups": {
                    "commercials": {
                        "weeks": _weeks([float(i) for i in range(20)]),
                    }
                }
            }
        }
    }
    doc = build_commercial_strength(legacy, calendar_week="2026-01-01")
    assert doc["research_only"] is True
    assert len(doc["currencies"]) == 7
    assert doc["currencies"]["EUR"]["commercial_score"] is not None
