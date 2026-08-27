"""Phase 1 correlation matrix — mathematical and integrity validation."""

from __future__ import annotations

from datetime import date, timedelta

from hptl.correlation_matrix.alignment import align_returns_pairwise, take_last_n
from hptl.correlation_matrix.engine import (
    build_correlation_matrix,
    pair_correlation,
    validate_matrix_payload,
)
from hptl.correlation_matrix.methods import PearsonMethod, get_method
from hptl.correlation_matrix.returns import percentage_returns, returns_for_frequency
from hptl.correlation_matrix.service import build_correlation_matrix_payload


def _daily_closes(
    start: str,
    n: int,
    *,
    base: float = 100.0,
    seed: int = 1,
):
    """Synthetic closes with *varying* returns (non-zero variance)."""
    d0 = date.fromisoformat(start)
    out = []
    px = base
    i = 0
    k = 0
    while len(out) < n:
        d = d0 + timedelta(days=i)
        i += 1
        if d.weekday() >= 5:
            continue
        # Deterministic oscillating drift — not constant.
        drift = 0.001 * ((seed * 3 + k * 7) % 11 - 5) / 5.0
        px *= 1.0 + drift
        out.append((d.isoformat(), px))
        k += 1
    return out


def _returns_from_closes(closes):
    return percentage_returns(closes)


def test_pearson_perfect_positive():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [2.0, 4.0, 6.0, 8.0, 10.0]
    r = PearsonMethod().correlate(x, y)
    assert r is not None
    assert abs(r - 1.0) < 1e-12


def test_pearson_perfect_negative():
    x = [1.0, 2.0, 3.0, 4.0, 5.0]
    y = [10.0, 8.0, 6.0, 4.0, 2.0]
    r = PearsonMethod().correlate(x, y)
    assert r is not None
    assert abs(r + 1.0) < 1e-12


def test_never_correlate_price_levels_directly():
    # Engine path always uses percentage returns.
    closes_a = _daily_closes("2020-01-01", 80, base=100, seed=3)
    closes_b = [(d, c * 2) for d, c in closes_a]  # perfect price co-movement
    ra = returns_for_frequency(closes_a, "daily")
    rb = returns_for_frequency(closes_b, "daily")
    # Perfect co-movement in prices ⇒ perfect return correlation
    r, meta = pair_correlation(ra, rb, lookback=60)
    assert meta["status"] == "ok"
    assert abs(r - 1.0) < 1e-9


def test_date_alignment_inner_join_only():
    a = [("2020-01-02", 0.01), ("2020-01-03", 0.02), ("2020-01-06", 0.03)]
    b = [("2020-01-03", 0.05), ("2020-01-06", -0.01), ("2020-01-07", 0.02)]
    xa, xb, dates = align_returns_pairwise(a, b)
    assert dates == ["2020-01-03", "2020-01-06"]
    assert xa == [0.02, 0.03]
    assert xb == [0.05, -0.01]


def test_no_forward_fill_on_missing_dates():
    a = [("2020-01-02", 0.01), ("2020-01-03", 0.02)]
    b = [("2020-01-02", 0.01), ("2020-01-05", 0.02)]  # gap — 01-03 missing in b
    xa, xb, dates = align_returns_pairwise(a, b)
    assert dates == ["2020-01-02"]
    assert "2020-01-03" not in dates


def test_insufficient_overlap_rejected():
    a = [("2020-01-%02d" % d, 0.01 * (d % 3 - 1)) for d in range(1, 21)]
    b = [("2020-01-%02d" % d, 0.02 * (d % 5 - 2)) for d in range(10, 21)]  # 11 overlap
    r, meta = pair_correlation(a, b, lookback=20)
    assert r is None
    assert meta["status"] == "insufficient_overlap"


def test_nan_ignored_in_alignment():
    a = [("2020-01-02", 0.01), ("2020-01-03", float("nan")), ("2020-01-06", 0.02)]
    b = [("2020-01-02", 0.01), ("2020-01-03", 0.02), ("2020-01-06", 0.03)]
    xa, xb, dates = align_returns_pairwise(a, b)
    assert dates == ["2020-01-02", "2020-01-06"]


def test_matrix_symmetry_diagonal_bounds():
    ids = ["A", "B", "C"]
    base = _daily_closes("2019-01-01", 300, seed=2)
    ra = _returns_from_closes(base)
    series = {
        "A": ra,
        "B": [(d, r * 0.8 + 0.0001 * ((i % 5) - 2)) for i, (d, r) in enumerate(ra)],
        "C": [(d, -r * 0.5) for d, r in ra],
    }

    payload = build_correlation_matrix(
        instrument_ids=ids,
        frequency="daily",
        lookback=60,
        return_series=series,
    )
    errors = validate_matrix_payload(payload)
    assert errors == []
    m = payload["matrix"]
    for i in range(3):
        assert m[i][i] == 1.0
        for j in range(3):
            v = m[i][j]
            assert v is not None
            assert -1.0 <= v <= 1.0
            assert abs(v - m[j][i]) < 1e-12


def test_missing_series_handled_safely():
    ids = ["A", "B"]
    series = {
        "A": _returns_from_closes(_daily_closes("2019-01-01", 100, seed=4)),
        "B": [],
    }
    payload = build_correlation_matrix(
        instrument_ids=ids,
        lookback=20,
        return_series=series,
    )
    assert payload["matrix"][0][0] == 1.0
    assert payload["matrix"][1][1] is None
    assert payload["matrix"][0][1] is None
    assert payload["matrix"][1][0] is None
    assert any("no_return_series" in w for w in payload["warnings"])


def test_lookback_extensible_without_code_change():
    # Engine accepts non-preset lookback 40
    closes = _daily_closes("2018-01-01", 200, seed=5)
    rets = _returns_from_closes(closes)
    payload = build_correlation_matrix(
        instrument_ids=["A", "B"],
        lookback=40,
        return_series={
            "A": rets,
            "B": [(d, r * 0.9 + 0.0002 * (i % 3)) for i, (d, r) in enumerate(rets)],
        },
    )
    assert payload["lookback"] == 40
    assert payload["matrix"][0][1] is not None


def test_run_stability_deterministic():
    closes_a = _daily_closes("2018-01-01", 200, seed=6)
    closes_b = _daily_closes("2018-01-01", 200, seed=7)
    series = {
        "A": _returns_from_closes(closes_a),
        "B": _returns_from_closes(closes_b),
    }
    p1 = build_correlation_matrix(
        instrument_ids=["A", "B"], lookback=60, return_series=series
    )
    p2 = build_correlation_matrix(
        instrument_ids=["A", "B"], lookback=60, return_series=series
    )
    assert p1["matrix"] == p2["matrix"]


def test_weekly_frequency_uses_fewer_bars():
    closes = _daily_closes("2018-01-01", 400, seed=8)
    daily = returns_for_frequency(closes, "daily")
    weekly = returns_for_frequency(closes, "weekly")
    assert len(weekly) < len(daily)
    assert len(weekly) > 20


def test_take_last_n():
    x = [1, 2, 3, 4, 5]
    y = [5, 4, 3, 2, 1]
    d = ["a", "b", "c", "d", "e"]
    xx, yy, dd = take_last_n(x, y, d, 3)
    assert xx == [3, 4, 5]
    assert yy == [3, 2, 1]
    assert dd == ["c", "d", "e"]


def test_method_registry_pearson_only_phase1():
    assert get_method("pearson").name == "pearson"
    try:
        get_method("spearman")
        assert False, "spearman should not be registered in Phase 1"
    except ValueError:
        pass


def test_service_payload_shape():
    closes = _daily_closes("2018-01-01", 200, seed=9)
    rets = _returns_from_closes(closes)
    bad = build_correlation_matrix_payload(frequency="monthly", lookback=60)
    assert bad["status"] == "error"
    bad2 = build_correlation_matrix_payload(frequency="daily", lookback=0)
    assert bad2["status"] == "error"

    eng = build_correlation_matrix(
        instrument_ids=["X", "Y"],
        lookback=30,
        return_series={"X": rets, "Y": [(d, -r) for d, r in rets]},
    )
    assert validate_matrix_payload(eng) == []
    assert eng["matrix"][0][1] is not None
    assert eng["matrix"][0][1] < 0
