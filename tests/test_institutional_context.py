"""Institutional context engine guardrails."""

from __future__ import annotations

import pandas as pd

from hptl.context.flow_momentum import l1_l2_conflict
from hptl.context.institutional_context import build_institutional_context_for_row
from hptl.context.regime_store import RegimeStore
from hptl.context.structural_regime import _block_one_week_flip


def test_no_one_week_bull_bear_flip():
    assert _block_one_week_flip("structural_bullish", "structural_bearish", 1000.0, -500.0) == "transitional"
    assert _block_one_week_flip("structural_bearish", "structural_bullish", -1000.0, 500.0) == "transitional"


def test_l1_l2_conflict_not_reversal_narrative():
    assert l1_l2_conflict("structural_bullish", "weakening") is True
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "state.json"
        st = RegimeStore(path=p)
        st.get("Gold").structural_regime = "structural_bullish"
        st.get("Gold").structural_score_ema = 40.0
        st.get("Gold").regime_since_cot_week = "2026-04-01"
        st.get("Gold").weeks_in_regime = 5

        ctx = build_institutional_context_for_row(
            market="Gold",
            net=50000.0,
            w1=-8000.0,
            w4=2000.0,
            long_w1=-5000.0,
            short_w1=3000.0,
            hist=pd.DataFrame(),
            store=st,
            cot_week="2026-05-19",
            macro_signal="risk_on",
            macro_score=5.0,
            full_loaded_ctx={"current_net_percentile": 75.0},
        )
        assert ctx["structural_regime"] == "structural_bullish"
        assert ctx["flow_l1_l2_conflict"] is True
        assert "pullback" in (ctx.get("flow_conflict_narrative") or "").lower()
        assert "stalk_long_pullback" in ctx["tactical_posture"] or ctx["tactical_posture"] == "avoid_chase"


def test_regime_flip_requires_two_weeks():
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as td:
        p = Path(td) / "state.json"
        st = RegimeStore(path=p)
        st.get("Wheat").structural_regime = "structural_bullish"
        st.get("Wheat").structural_score_ema = 35.0
        st.get("Wheat").weeks_in_regime = 4

        hist_rows = []
        for i, (net, w1) in enumerate([(-5000.0, -3000.0), (-15000.0, -8000.0), (-25000.0, -9000.0)]):
            hist_rows.append(
                {
                    "cot_report_date": pd.Timestamp(f"2026-04-{10 + i * 7}"),
                    "net_value": net,
                    "weekly_change": w1,
                    "long_weekly_change": -2000,
                    "short_weekly_change": 1000,
                }
            )
        hist = pd.DataFrame(hist_rows)

        build_institutional_context_for_row(
            market="Wheat",
            net=-35000.0,
            w1=-10000.0,
            w4=-20000.0,
            long_w1=-3000.0,
            short_w1=2000.0,
            hist=hist,
            store=st,
            cot_week="2026-05-05",
            macro_signal="risk_off",
            macro_score=8.0,
        )
        assert st.get("Wheat").structural_regime == "structural_bullish"

        build_institutional_context_for_row(
            market="Wheat",
            net=-40000.0,
            w1=-12000.0,
            w4=-25000.0,
            long_w1=-4000.0,
            short_w1=3000.0,
            hist=hist,
            store=st,
            cot_week="2026-05-12",
            macro_signal="risk_off",
            macro_score=8.0,
        )
        assert st.get("Wheat").structural_regime in {"structural_bearish", "structural_bullish"}
        if st.get("Wheat").structural_regime == "structural_bullish":
            assert st.get("Wheat").pending_flip is not None
        else:
            assert st.get("Wheat").weeks_in_regime >= 1
