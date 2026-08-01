"""Tests for COT Workstation Intelligence V2."""

from __future__ import annotations

from hptl.cot.workstation_intelligence import (
    ANALOGUE_COOLDOWN_WEEKS,
    EPISODE_COOLDOWN_WEEKS,
    EXTREME_HIGH,
    EXTREME_LOW,
    MIN_HISTORY,
    _detect_group_extremes,
    _snapshot_at,
    build_market_intelligence,
    find_analogues,
    sample_quality,
    GROUP_COMMERCIAL,
)


def _row(date: str, c: float, nc: float, nr: float, price: float) -> dict:
    return {
        "date": date,
        "commercial_net": c,
        "institutional_net": nc,
        "retail_net": nr,
        "price": price,
        "open_interest": 100000,
    }


def _build_series(n: int = MIN_HISTORY + 40):
    from datetime import date, timedelta

    start = date(2018, 1, 2)
    rows = []
    price = 100.0
    for i in range(n):
        # Mild commercial oscillation then late extreme
        c = 1000 + (i % 11 - 5) * 80
        nc = -800 + (i % 7 - 3) * 40
        nr = -200 + (i % 5 - 2) * 30
        price = price * (1.0 + ((i % 9) - 4) * 0.002)
        rows.append(_row((start + timedelta(weeks=i)).isoformat(), c, nc, nr, price))
    return rows


def test_sample_quality_tiers():
    assert sample_quality(3) == "INSUFFICIENT SAMPLE"
    assert sample_quality(6) == "LOW CONFIDENCE"
    assert sample_quality(10) == "MODERATE SAMPLE"
    assert sample_quality(20) == "STRONGER SAMPLE"


def test_extreme_entry_dedup_not_every_week():
    series = _build_series()
    # Force a prolonged bullish extreme episode at the end
    for i in range(-20, 0):
        series[i]["commercial_net"] = 50_000 + abs(i) * 10
    # Rebuild with higher baseline so last 20 are extreme vs expanding history
    # Simpler: amplify last 15 nets far above history
    base_max = max(r["commercial_net"] for r in series[:-20])
    for i in range(-15, 0):
        series[i]["commercial_net"] = base_max + 20_000 + (15 + i) * 500

    events = _detect_group_extremes(series, GROUP_COMMERCIAL)
    bull = [e for e in events if "BULLISH" in e.label]
    assert bull, "expected at least one bullish extreme entry"
    # Must not mark every consecutive extreme week
    assert len(bull) < 12
    # Gaps between entries should respect cooldown for enters (deepens allowed closer)
    enter_idxs = []
    dates = [r["date"] for r in series]
    for e in bull:
        if e.kind == "enters_bullish_extreme":
            enter_idxs.append(dates.index(e.date))
    for a, b in zip(enter_idxs, enter_idxs[1:]):
        assert b - a >= EPISODE_COOLDOWN_WEEKS


def test_no_lookahead_in_snapshot_percentile():
    series = _build_series()
    # Future spike should not affect mid-series percentile
    mid = MIN_HISTORY + 5
    series[-1]["commercial_net"] = 1_000_000
    snap_mid = _snapshot_at(series, mid)
    # Recompute without future by truncating
    snap_trunc = _snapshot_at(series[: mid + 1], mid)
    assert snap_mid[GROUP_COMMERCIAL]["percentile"] == snap_trunc[GROUP_COMMERCIAL]["percentile"]


def test_analogue_cooldown_independent_cases():
    series = _build_series(MIN_HISTORY + 80)
    # Create repeating similar regimes every 3 weeks near the end — should collapse
    for i in range(MIN_HISTORY, len(series) - 30):
        series[i]["commercial_net"] = 8000
        series[i]["institutional_net"] = -3000
        series[i]["retail_net"] = -2000
    # Current similar
    series[-1]["commercial_net"] = 8200
    series[-1]["institutional_net"] = -3100
    series[-1]["retail_net"] = -2100
    # Make current look extreme-ish vs early history
    for i in range(0, MIN_HISTORY):
        series[i]["commercial_net"] = 500 + (i % 5) * 20

    result = find_analogues(series, len(series) - 1)
    cases = result["cases"]
    if len(cases) >= 2:
        idxs = [c["index"] for c in cases]
        for a, b in zip(idxs, idxs[1:]):
            assert b - a >= ANALOGUE_COOLDOWN_WEEKS


def test_forward_outcomes_present_when_price_available():
    series = _build_series(MIN_HISTORY + 60)
    # Push current into a distinctive bullish commercial extreme
    for i in range(0, len(series) - 40):
        series[i]["commercial_net"] = 1000 + (i % 6) * 50
        series[i]["institutional_net"] = -500
        series[i]["retail_net"] = 200
    for i in range(len(series) - 40, len(series)):
        series[i]["commercial_net"] = 20_000
        series[i]["institutional_net"] = -8_000
        series[i]["retail_net"] = -5_000
        series[i]["price"] = 100 + (i - (len(series) - 40)) * 0.5

    intel = build_market_intelligence("Test", {"series": series})
    assert intel["available"] is True
    assert "analogues" in intel
    assert "intelligence_panel" in intel
    assert intel["intelligence_panel"]["state"]["commercial"]
    # Thresholds exposed for explainability
    assert intel["thresholds"]["extreme_high_percentile"] == EXTREME_HIGH
    assert intel["thresholds"]["extreme_low_percentile"] == EXTREME_LOW


def test_live_markets_smoke():
    import json
    from pathlib import Path

    path = Path("web-dashboard/public/data/cot_3y_series_latest.json")
    if not path.is_file():
        return
    doc = json.loads(path.read_text(encoding="utf-8"))
    for mid in ("Natural Gas / NG", "Gold", "Crude Oil / CL"):
        block = (doc.get("markets") or {}).get(mid)
        if not block:
            continue
        intel = build_market_intelligence(mid, block)
        assert intel["available"] is True
        assert intel["source_week"]
        assert isinstance(intel["markers"], list)
