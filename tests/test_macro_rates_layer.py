from __future__ import annotations

import pandas as pd

from hptl.macro.macro_scoring import REQUIRED_SCORING_INPUTS, score_macro
from hptl.macro.run_macro_update import _latest_core_yield_date, _select_dashboard_row, DASHBOARD_COLS


def _base_rows(periods: int = 40) -> pd.DataFrame:
    dates = pd.date_range("2026-03-26", periods=periods, freq="D")
    df = pd.DataFrame(
        {
            "date": dates,
            "dgs2": [4.50 - i * 0.02 for i in range(periods)],
            "dgs10": [4.70 - i * 0.02 for i in range(periods)],
            "dgs30": [4.90 - i * 0.02 for i in range(periods)],
            "fed_funds": [4.33 for _ in range(periods)],
        }
    )
    df["yield_curve_10y2y"] = df["dgs10"] - df["dgs2"]
    for col in ["dgs2", "dgs10", "dgs30", "fed_funds", "yield_curve_10y2y"]:
        df[f"{col}_1w_change"] = df[col] - df[col].shift(5)
        df[f"{col}_4w_change"] = df[col] - df[col].shift(20)
    return df


def test_valid_row_has_zero_data_lag() -> None:
    scored = score_macro(_base_rows())
    valid = scored[scored["macro_score"].notna()].iloc[-1]

    assert valid["macro_snapshot_date"] == valid["date"]
    assert valid["data_lag_days"] == 0
    assert bool(valid["macro_valid_for_trading"]) is True


def test_invalid_row_after_latest_valid_carries_prior_snapshot_and_correct_lag() -> None:
    df = _base_rows()
    # 2026-04-29 is valid; later rows have missing yields.
    df.loc[df["date"] > pd.Timestamp("2026-04-29"), ["dgs2", "dgs10", "dgs30"]] = pd.NA
    scored = score_macro(df)

    row = scored.loc[scored["date"] == pd.Timestamp("2026-05-04")].iloc[0]

    assert row["macro_snapshot_date"] == pd.Timestamp("2026-04-29")
    assert row["data_lag_days"] == 5
    assert pd.isna(row["macro_score"])
    assert row["macro_signal"] == "insufficient_data"
    assert bool(row["macro_valid_for_trading"]) is False


def test_latest_available_date_ignores_all_missing_current_rows() -> None:
    df = _base_rows()
    df.loc[df["date"] > pd.Timestamp("2026-04-29"), ["dgs2", "dgs10", "dgs30"]] = pd.NA
    scored = score_macro(df)

    assert _latest_core_yield_date(scored) == pd.Timestamp("2026-04-29")


def test_dashboard_uses_latest_complete_scoring_snapshot() -> None:
    df = _base_rows()
    df.loc[df["date"] > pd.Timestamp("2026-04-29"), ["dgs2", "dgs10", "dgs30"]] = pd.NA
    scored = score_macro(df)
    dashboard = _select_dashboard_row(scored)

    assert dashboard["latest_available_date"].iloc[0] == pd.Timestamp("2026-04-29")
    assert dashboard["macro_snapshot_date"].iloc[0] == pd.Timestamp("2026-04-29")
    assert dashboard["data_lag_days"].iloc[0] == 0
    assert pd.notna(dashboard["macro_score"].iloc[0])


def test_rates_history_no_longer_has_same_data_lag_for_every_row() -> None:
    df = _base_rows()
    df.loc[df["date"] > pd.Timestamp("2026-04-29"), ["dgs2", "dgs10", "dgs30"]] = pd.NA
    scored = score_macro(df)
    lags = scored["data_lag_days"].dropna().unique().tolist()

    assert 0 in lags
    assert 5 in lags
    assert len(lags) > 1


def test_no_complete_snapshot_returns_insufficient_data_and_blank_score() -> None:
    df = _base_rows()
    df[["dgs2", "dgs10", "dgs30"]] = pd.NA
    scored = score_macro(df)

    assert scored["macro_score"].isna().all()
    assert set(scored["macro_signal"]) == {"insufficient_data"}
    assert not scored["macro_valid_for_trading"].any()
    assert scored["macro_snapshot_date"].isna().all()
    assert scored["data_lag_days"].isna().all()


def test_core_yields_without_directional_inputs_fail_closed() -> None:
    df = _base_rows().head(10).copy()
    for col in [c for c in REQUIRED_SCORING_INPUTS if c.endswith("_4w_change")]:
        df[col] = pd.NA
    scored = score_macro(df)

    assert scored["macro_score"].isna().all()
    assert set(scored["macro_signal"]) == {"insufficient_data"}


def test_macro_valid_for_trading_matches_score_presence() -> None:
    scored = score_macro(_base_rows())

    assert (scored["macro_valid_for_trading"] == scored["macro_score"].notna()).all()


def test_dashboard_includes_snapshot_and_lag_columns() -> None:
    scored = score_macro(_base_rows())
    dashboard = _select_dashboard_row(scored)

    for col in ["macro_snapshot_date", "data_lag_days", "macro_valid_for_trading"]:
        assert col in dashboard.columns
    for col in DASHBOARD_COLS:
        assert col in dashboard.columns


def test_no_score_can_appear_beside_blank_required_directional_inputs() -> None:
    df = _base_rows()
    df.loc[df.index[-1], "dgs10_1w_change"] = pd.NA
    scored = score_macro(df)
    scored_rows = scored[scored["macro_score"].notna()]

    assert not scored_rows[REQUIRED_SCORING_INPUTS].isna().any(axis=1).any()
    assert pd.isna(scored.loc[scored.index[-1], "macro_score"])
    assert scored.loc[scored.index[-1], "macro_signal"] == "insufficient_data"


def test_fed_funds_limitation_is_documented() -> None:
    import inspect
    import hptl.macro.rates_downloader as downloader
    import hptl.macro.macro_scoring as scoring

    combined = inspect.getsource(downloader) + inspect.getsource(scoring)
    assert "historical/effective" in combined
    assert "not real-time policy expectations" in combined or "not a forward-looking" in combined
