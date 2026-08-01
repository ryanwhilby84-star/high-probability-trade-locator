"""Tests for positioning research engine (state / spread / analogues)."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.cot.positioning_research_engine import (
    ANALOGUE_COOLDOWN_WEEKS,
    EVENT_COOLDOWN_WEEKS,
    MIN_HISTORY,
    PRIMARY_BAND,
    SPREAD_BANDS,
    build_group_state_series,
    build_market_positioning_research,
    build_spread_series,
    detect_configuration_events,
    find_configuration_analogues,
    sample_quality,
    GROUP_COMMERCIAL,
    GROUP_NONCOMMERCIAL,
    GROUP_NONREPORTABLE,
)


def _row(d: str, c: float, nc: float, nr: float, price: float) -> dict:
    return {
        "date": d,
        "commercial_net": c,
        "institutional_net": nc,
        "retail_net": nr,
        "price": price,
    }


def _series(n: int = MIN_HISTORY + 120):
    start = date(2016, 1, 5)
    rows = []
    price = 1200.0
    for i in range(n):
        # Opposing oscillation between commercials and non-reportables
        phase = (i % 40) / 40.0
        c = -40_000 + phase * 80_000 + (i % 7) * 200
        nr = 20_000 - phase * 50_000 + (i % 5) * 100
        nc = -c * 0.6
        price = price * (1.0 + ((i % 11) - 5) * 0.0015)
        rows.append(_row((start + timedelta(weeks=i)).isoformat(), c, nc, nr, price))
    return rows


def test_sample_quality_tiers():
    assert sample_quality(2) == "INSUFFICIENT SAMPLE"
    assert sample_quality(6) == "LOW CONFIDENCE"
    assert sample_quality(10) == "MODERATE SAMPLE"
    assert sample_quality(20) == "STRONGER SAMPLE"


def test_no_lookahead_in_expanding_percentile():
    series = _series()
    states = build_group_state_series(series, GROUP_COMMERCIAL)
    mid = MIN_HISTORY + 10
    series[-1]["commercial_net"] = 10_000_000  # future spike
    states_full = build_group_state_series(series, GROUP_COMMERCIAL)
    states_trunc = build_group_state_series(series[: mid + 1], GROUP_COMMERCIAL)
    assert (
        states_full[mid]["percentiles"]["long_history"]
        == states_trunc[mid]["percentiles"]["long_history"]
    )
    # sanity: mid state existed before mutation path
    assert states[mid]["percentiles"]["long_history"] is not None


def test_spread_formula_is_comm_minus_nr_percentile():
    series = _series()
    c = build_group_state_series(series, GROUP_COMMERCIAL)
    nr = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(c, nr)
    i = MIN_HISTORY + 5
    cp = c[i]["percentiles"]["long_history"]
    np_ = nr[i]["percentiles"]["long_history"]
    assert spreads[i]["spread"] == round(cp - np_, 2)


def test_divergence_events_respect_cooldown():
    series = _series()
    # Force prolonged high-spread regime near end
    for i in range(-30, 0):
        series[i]["commercial_net"] = 90_000 + i
        series[i]["retail_net"] = -60_000 + i
    c = build_group_state_series(series, GROUP_COMMERCIAL)
    nc = build_group_state_series(series, "noncommercial")
    nr = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(c, nr)
    events = detect_configuration_events(c, nc, nr, spreads)
    div = [e for e in events if e["event_type"] == "comm_nr_divergence"]
    assert div, "expected divergence events in forced regime"
    by_side: dict[str, list[int]] = {}
    for e in div:
        by_side.setdefault(str(e["side"]), []).append(int(e["index"]))
    for idxs in by_side.values():
        for a, b in zip(idxs, idxs[1:]):
            assert b - a >= EVENT_COOLDOWN_WEEKS


def test_analogue_independence_cooldown():
    series = _series(MIN_HISTORY + 160)
    # Create repeating high-spread windows
    for i in range(MIN_HISTORY, len(series) - 40):
        if (i // 3) % 2 == 0:
            series[i]["commercial_net"] = 85_000
            series[i]["retail_net"] = -55_000
        else:
            series[i]["commercial_net"] = 0
            series[i]["retail_net"] = 0
    series[-1]["commercial_net"] = 86_000
    series[-1]["retail_net"] = -56_000

    c = build_group_state_series(series, GROUP_COMMERCIAL)
    nr = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(c, nr)
    prices = [r["price"] for r in series]
    analogues = find_configuration_analogues(
        spreads, c, nr, prices, len(series) - 1, side="high"
    )
    cases = analogues["cases"]
    for a, b in zip(cases, cases[1:]):
        assert b["index"] - a["index"] >= ANALOGUE_COOLDOWN_WEEKS


def test_band_audit_includes_all_predeclared_bands():
    series = _series()
    doc = build_market_positioning_research("Test", {"series": series})
    assert doc["available"]
    assert doc["normalization"]["primary_marker_band"] == PRIMARY_BAND
    for band in SPREAD_BANDS:
        assert band["name"] in doc["band_audit"]


def test_market_research_builds_from_fixture_shape():
    series = _series()
    out = build_market_positioning_research("Any Market", {"series": series})
    assert out["available"]
    assert "spread_series" in out
    assert "markers" in out
    assert "current_interpretation" in out
    assert out["normalization"]["spread_formula"]


def test_research_doc_defaults_to_full_source_universe():
    from hptl.cot.positioning_research_engine import build_positioning_research_doc

    series = _series()
    cot3y = {
        "markets": {
            "Alpha / AA": {"series": series},
            "Beta / BB": {"series": series[:40]},  # insufficient
            "Gamma / GG": {"series": series},
        }
    }
    doc = build_positioning_research_doc(cot3y, markets=None)
    assert doc["scope"] == "full_cot3y_universe"
    assert set(doc["markets"]) == {"Alpha / AA", "Beta / BB", "Gamma / GG"}
    assert doc["markets"]["Alpha / AA"]["available"] is True
    assert doc["markets"]["Gamma / GG"]["available"] is True
    assert doc["markets"]["Beta / BB"]["available"] is False
    assert doc["summary"]["markets_in_source"] == 3
    assert doc["summary"]["markets_available"] == 2


def test_noncommercial_extremes_and_rotations_emit_nc_group():
    """NC absolute/local extremes + 26W rotations use established thresholds on NC only."""
    series = _series(MIN_HISTORY + 180)
    # Force NC absolute extremes near the end (independent of commercial).
    for i in range(-40, 0):
        series[i]["institutional_net"] = 120_000 + i
        series[i]["commercial_net"] = 1_000  # keep commercial mid-range
    # Earlier window: pull NC to low end so a later 26W journey can rotate.
    for i in range(-(40 + 30), -40):
        series[i]["institutional_net"] = -90_000 + i

    c = build_group_state_series(series, GROUP_COMMERCIAL)
    nc = build_group_state_series(series, GROUP_NONCOMMERCIAL)
    nr = build_group_state_series(series, GROUP_NONREPORTABLE)
    spreads = build_spread_series(c, nr)
    events = detect_configuration_events(c, nc, nr, spreads)

    nc_events = [e for e in events if e["group"] == GROUP_NONCOMMERCIAL]
    assert nc_events, "expected Non-Commercial configuration events"

    types = {e["event_type"] for e in nc_events}
    assert "absolute_extreme" in types or "local_extreme" in types

    for e in nc_events:
        assert e["group"] == GROUP_NONCOMMERCIAL
        assert e["event_type"] in {
            "absolute_extreme",
            "local_extreme",
            "major_rotation",
        }
        # NC events must be driven by NC percentiles, not commercial.
        nc_pct = (e.get("noncommercial") or {}).get("long_history_percentile")
        assert nc_pct is not None

    # Existing commercial / multi / NR groups remain intact in the same pass.
    groups = {e["group"] for e in events}
    assert GROUP_COMMERCIAL in groups or "multi" in groups
