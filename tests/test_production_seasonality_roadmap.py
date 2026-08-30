from __future__ import annotations

from datetime import date, timedelta

from hptl.seasonality_workstation.production_roadmap import (
    METHOD_VERSION,
    NO_EDGE,
    apply_production_seasonality,
    build_production_roadmap,
)


def _synthetic_daily() -> list[tuple[str, float]]:
    out: list[tuple[str, float]] = []
    px = 100.0
    d = date(2010, 1, 1)
    end = date(2026, 8, 24)
    i = 0
    while d <= end:
        if d.weekday() < 5:
            # Deterministic but deliberately textured daily movement.
            r = 0.0018 if i % 11 in (1, 2, 7) else -0.0012 if i % 11 in (4, 8) else 0.0002
            px *= 1.0 + r
            out.append((d.isoformat(), px))
            i += 1
        d += timedelta(days=1)
    return out


def _research(*, wf_hit: float = 0.70, wf_n: int = 10, agreement: float = 0.8):
    block = {
        "sample_years": list(range(2011, 2026)),
        "sample_size": 15,
        "forward_horizons": {
            "4w": {"mean_return": 0.012, "median_return": 0.010, "positive_frequency": 0.67, "n": 15, "dispersion": 0.025},
            "8w": {"mean_return": 0.022, "median_return": 0.019, "positive_frequency": 0.67, "n": 15, "dispersion": 0.03},
            "12w": {"mean_return": 0.028, "median_return": 0.024, "positive_frequency": 0.67, "n": 15, "dispersion": 0.035},
        },
    }
    daily = _synthetic_daily()
    anchor_price = next(c for d, c in reversed(daily) if d <= "2026-08-24")
    return {
        "status": "ok",
        "instrument_id": "Synthetic",
        "selected_lookback": "15Y",
        "lookbacks": {"15Y": block},
        "anchor": {"date": "2026-08-24", "price": anchor_price, "iso_year": 2026, "iso_week": 35},
        "integrity": {"status": "PASS"},
        "lookback_agreement": {"score": agreement},
        "walk_forward": {"hit_rate": wf_hit, "n": wf_n},
        "seasonality": {"primary": "legacy"},
        "display_defaults": {},
        "weekly_roadmap": {"comparison": {}},
        "_daily_closes": daily,
    }


def test_production_roadmap_is_unsmoothed_daily_return_path():
    roadmap = build_production_roadmap(_research())
    assert roadmap["available"] is True
    assert roadmap["method"]["version"] == METHOD_VERSION
    assert roadmap["method"]["aggregation"] == "10pct_trimmed_mean_daily_close_to_close_return"
    assert roadmap["method"]["interpolation"] == "none_in_payload"
    assert roadmap["method"]["smooth"] is None
    assert roadmap["smoothed"] is None
    points = roadmap["unsmoothed"]["full_year"]
    assert len(points) >= 250
    assert len(points) > 52
    today = next(p for p in points if p["segment"] == "today")
    assert today["date"] == "2026-08-24"
    assert abs(today["price"] - roadmap["anchor_price"]) < 1e-9
    # Daily path must contain actual day-to-day directional changes.
    returns = [p["trimmed_mean_return"] for p in points[1:80] if p["trimmed_mean_return"] is not None]
    assert any(r > 0 for r in returns)
    assert any(r < 0 for r in returns)


def test_production_adapter_replaces_legacy_primary_without_touching_cot():
    research = _research()
    research["cot"] = {"sentinel": "must_remain_independent"}
    out = apply_production_seasonality(research)
    assert out["seasonality"]["primary"] == METHOD_VERSION
    assert out["seasonal_roadmap"]["method"]["version"] == METHOD_VERSION
    assert out["display_defaults"]["roadmap_smoothed"] is False
    assert out["cot"] == {"sentinel": "must_remain_independent"}


def test_reliability_refuses_weak_out_of_sample_evidence():
    roadmap = build_production_roadmap(_research(wf_hit=0.40, wf_n=10))
    reliability = roadmap["reliability"]
    assert reliability["reliable"] is False
    assert reliability["verdict"] == NO_EDGE
    assert any("walk_forward" in reason for reason in reliability["reasons"])
