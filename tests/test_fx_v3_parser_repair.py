"""Tests for Phase 3B FX V3 parser/data repairs."""
from __future__ import annotations

import csv
import io
from unittest.mock import patch

import pytest

from hptl.fx.fx_macro_history import (
    MIN_FOUNDATION_OBS,
    load_bis_policy_history,
    load_jpy_y2_history,
    load_nzd_y2_history,
)
from hptl.fx.fx_spot_history import _best_instrument_record, get_daily_spot_series
from hptl.valuation.fx_carry_real_yield_v3 import (
    MIN_R_SQUARED,
    MIN_WEEKLY_OBS,
    apply_pillar_canonical_gate,
    compute_fx_pair_v3,
)


def _bis_csv(rows: list[tuple[str, float]]) -> str:
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["TIME_PERIOD", "OBS_VALUE"])
    for d, v in rows:
        w.writerow([d, v])
    return buf.getvalue()


def test_bis_policy_prefers_deep_history_over_shallow(tmp_path, monkeypatch):
    from hptl.fx import fx_macro_history as mod

    cache = tmp_path
    monkeypatch.setattr(mod, "CACHE_DIR", cache)

    shallow = _bis_csv([("2026-06-02", 0.75)])
    deep = _bis_csv([(f"2016-01-{d:02d}", 0.1) for d in range(1, 32)])
    (cache / "bis_cbpol_jp.txt").write_text(shallow, encoding="utf-8")
    (cache / "bis_cbpol_jp_history.txt").write_text(deep, encoding="utf-8")

    loaded = load_bis_policy_history("jp")
    assert len(loaded) == 31
    assert len(loaded) > 1
    assert loaded.get("2016-01-01") == 0.1


def test_bis_policy_shallow_only_when_no_deep(tmp_path, monkeypatch):
    from hptl.fx import fx_macro_history as mod

    cache = tmp_path
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    (cache / "bis_cbpol_jp.txt").write_text(_bis_csv([("2026-06-02", 0.5)]), encoding="utf-8")

    loaded = load_bis_policy_history("jp")
    assert len(loaded) == 1


def test_jpy_y2_uses_fred_fallback_when_mof_shallow(tmp_path, monkeypatch):
    from hptl.fx import fx_macro_history as mod

    cache = tmp_path
    monkeypatch.setattr(mod, "CACHE_DIR", cache)
    monkeypatch.setattr(mod, "offline_mode", lambda: True)
    (cache / "jpy_jgb.txt").write_text(
        "Date,2Y,10Y\n2026/6/1,0.5,1.0\n",
        encoding="utf-8",
    )
    fred_map = {f"2016-{m:02d}-01": 0.2 + m * 0.01 for m in range(1, 60)}

    with patch.object(mod, "load_fred_daily_map", return_value=fred_map):
        y2, src = load_jpy_y2_history()
    assert len(y2) >= MIN_FOUNDATION_OBS
    assert "FRED OECD" in src


def test_spot_history_prefers_cot_major_over_shallow_alias():
    shallow_daily = [{"date": f"2025-06-{d:02d}", "close": 0.7} for d in range(1, 11)]
    deep_daily = [{"date": f"2016-06-{d:02d}", "close": 0.7} for d in range(1, 21)]

    instruments = {
        "AUD/USD": {"daily": shallow_daily},
        "Australian Dollar / 6A": {"daily": deep_daily},
    }

    with patch("hptl.fx.fx_spot_history.load_price_store", return_value={"instruments": instruments}):
        series, meta = get_daily_spot_series("AUD/USD")
    assert len(series) == 20
    assert "6A" in meta["source"]

    instruments_nzd = {
        "NZD/USD": {"daily": shallow_daily},
        "NZ Dollar / 6N": {"daily": deep_daily},
    }
    with patch("hptl.fx.fx_spot_history.load_price_store", return_value={"instruments": instruments_nzd}):
        series, meta = get_daily_spot_series("NZD/USD")
    assert len(series) == 20
    assert "6N" in meta["source"]


def test_aud_usd_alias_does_not_duplicate_pillar_row():
    wired = {"pair": "AUD/USD", "wired": True, "valuation_state": "Fair Value"}
    alias = apply_pillar_canonical_gate("AUD/USD", wired)
    assert alias["wired"] is False
    assert alias["valuation_pillar_role"] == "alias"


def test_missing_history_fails_without_crash():
    with patch("hptl.valuation.fx_carry_real_yield_v3._align_daily_panel", return_value=[]):
        with patch("hptl.valuation.fx_carry_real_yield_v3._spot_and_percentile", return_value=(1.0, None)):
            result = compute_fx_pair_v3("NZD/USD")
    assert result.audit_status == "FAIL"
    assert result.valuation_state == "Unavailable"
    assert result.fair_value is None


def test_nzd_y2_empty_without_fred_cache(tmp_path, monkeypatch):
    from hptl.fx import fx_macro_history as mod

    monkeypatch.setattr(mod, "CACHE_DIR", tmp_path)
    monkeypatch.setattr(mod, "offline_mode", lambda: True)
    with patch.object(mod, "load_fred_daily_map", return_value={}):
        y2, _src = load_nzd_y2_history()
    assert y2 == {}


def test_valuation_gates_unchanged():
    assert MIN_WEEKLY_OBS == 52
    assert MIN_R_SQUARED == 0.08
