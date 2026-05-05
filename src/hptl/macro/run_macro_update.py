from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .macro_scoring import REQUIRED_SCORING_INPUTS, score_macro
from .rates_downloader import START_DATE, download_all
from .rates_parser import process_rates

EXPORT_DIR = Path("data/exports")

DASHBOARD_COLS = [
    "macro_snapshot_date",
    "data_lag_days",
    "macro_valid_for_trading",
    "latest_available_date",
    "dgs2",
    "dgs10",
    "dgs30",
    "fed_funds",
    "yield_curve_10y2y",
    "dgs2_1w_change",
    "dgs10_1w_change",
    "dgs30_1w_change",
    "dgs2_4w_change",
    "dgs10_4w_change",
    "dgs30_4w_change",
    "yield_curve_10y2y_1w_change",
    "macro_signal",
    "rates_bias",
    "curve_context",
    "policy_pressure",
    "macro_score",
    "macro_strength",
    "macro_context_for_trades",
    "technical_trade_filter",
    "macro_summary",
]


def _latest_core_yield_date(scored: pd.DataFrame) -> pd.Timestamp:
    if scored.empty:
        return pd.NaT
    core_complete = scored[["dgs2", "dgs10", "dgs30"]].notna().all(axis=1)
    complete_rows = scored[core_complete]
    return complete_rows["date"].max() if not complete_rows.empty else pd.NaT


def _select_dashboard_row(scored: pd.DataFrame) -> pd.DataFrame:
    latest_available = _latest_core_yield_date(scored)
    usable = scored[scored["macro_valid_for_trading"] == True].copy()

    if usable.empty:
        dashboard = scored.tail(1).copy() if not scored.empty else pd.DataFrame([{}])
        dashboard["macro_snapshot_date"] = pd.NaT
        dashboard["data_lag_days"] = pd.NA
        dashboard["macro_signal"] = "insufficient_data"
        dashboard["macro_valid_for_trading"] = False
        dashboard["macro_score"] = pd.NA
        dashboard["macro_strength"] = pd.NA
        dashboard["rates_bias"] = "Neutral"
        dashboard["macro_context_for_trades"] = "Neutral/Unclear"
        dashboard["technical_trade_filter"] = "Do not use macro layer; required yield data is incomplete."
        dashboard["macro_summary"] = "Missing required yield data"
    else:
        dashboard = usable.tail(1).copy()

    macro_snapshot_date = dashboard["macro_snapshot_date"].iloc[0] if "macro_snapshot_date" in dashboard else pd.NaT
    if pd.notna(latest_available) and pd.notna(macro_snapshot_date):
        dashboard["data_lag_days"] = (
            pd.Timestamp(latest_available).normalize() - pd.Timestamp(macro_snapshot_date).normalize()
        ).days
    else:
        dashboard["data_lag_days"] = pd.NA

    dashboard["latest_available_date"] = latest_available
    return dashboard


def run() -> Path:
    print("=" * 70)
    print("Macro update started")
    print("=" * 70)

    try:
        raw = download_all()
        print(f"Raw row count: {len(raw)}")

        clean = process_rates()
        print(f"Clean row count: {len(clean)}")

        scored = score_macro(clean)
        print(f"Scored row count: {scored['macro_score'].notna().sum()}")

        # Fail-closed invariant: required directional inputs must be present wherever score exists.
        scored_rows = scored[scored["macro_score"].notna()]
        if not scored_rows.empty and scored_rows[REQUIRED_SCORING_INPUTS].isna().any(axis=1).any():
            raise ValueError("Fail-closed check failed: score exists beside blank required directional inputs")

        latest_available = _latest_core_yield_date(scored)
        dashboard = _select_dashboard_row(scored)
        macro_snapshot_date = dashboard["macro_snapshot_date"].iloc[0] if "macro_snapshot_date" in dashboard else pd.NaT
        data_lag_days = dashboard["data_lag_days"].iloc[0] if "data_lag_days" in dashboard else pd.NA
        macro_score = dashboard["macro_score"].iloc[0]
        macro_signal = dashboard["macro_signal"].iloc[0]

        print(f"Pulled date range: {START_DATE} -> latest available")
        print(f"Latest available date: {latest_available.date() if pd.notna(latest_available) else 'UNKNOWN'}")
        print(
            "Macro snapshot date used: "
            f"{pd.Timestamp(macro_snapshot_date).date() if pd.notna(macro_snapshot_date) else 'NONE'}"
        )
        print(f"Data lag days: {data_lag_days if pd.notna(data_lag_days) else 'UNKNOWN'}")
        print(f"Macro score / signal: {macro_score if pd.notna(macro_score) else 'blank'} / {macro_signal}")

        EXPORT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = EXPORT_DIR / f"macro_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        for col in DASHBOARD_COLS:
            if col not in dashboard.columns:
                dashboard[col] = pd.NA

        score_cols = [
            "date",
            "macro_snapshot_date",
            "data_lag_days",
            "macro_valid_for_trading",
            "rates_bias",
            "curve_context",
            "policy_pressure",
            "macro_signal",
            "macro_score",
            "macro_strength",
            "macro_context_for_trades",
            "technical_trade_filter",
            "macro_summary",
        ]

        notes = pd.DataFrame(
            {
                "Notes": [
                    "Macro/rates layer is a regime/confluence filter only, not a standalone buy/sell signal engine.",
                    "Technicals locate the trade; macro rates context filters or weights trade quality.",
                    "Data source: FRED / Federal Reserve H.15 selected interest rates.",
                    f"Pulled date range: {START_DATE} to latest available.",
                    "Required core series for valid scoring: DGS2, DGS10, DGS30.",
                    "Fail-closed behaviour: no macro_score is produced unless all required yield and directional fields are present.",
                    "DFF/fed_funds is historical effective federal funds rate data only; it is not real-time policy expectations data.",
                    "Directional thresholds: yields > +10bps restrictive/rising; yields < -10bps easing/falling.",
                    "Curve threshold: > +5bps steepening; < -5bps flattening.",
                    "Price/rates ratio layer placeholder added in src/hptl/macro/ratio_context.py; no fake ratio signals are emitted.",
                    "Red-folder news/event-risk placeholder added in src/hptl/macro/news_risk.py; it does not affect macro_score yet.",
                    "Timestamped exports avoid Excel file-lock PermissionError issues.",
                ]
            }
        )

        print(f"Saving timestamped Excel file: {output_path}")
        with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
            scored.to_excel(writer, sheet_name="Rates_History", index=False)
            dashboard[DASHBOARD_COLS].to_excel(writer, sheet_name="Macro_Dashboard", index=False)
            scored[score_cols].to_excel(writer, sheet_name="Macro_Score", index=False)
            notes.to_excel(writer, sheet_name="Macro_Source_Notes", index=False)

        print("=" * 70)
        print("Macro update complete")
        print(f"Saved output path: {output_path}")
        print("=" * 70)
        return output_path

    except Exception as exc:
        print("=" * 70)
        print("MACRO UPDATE FAILED")
        print(f"Error: {type(exc).__name__}: {exc}")
        print("=" * 70)
        raise


if __name__ == "__main__":
    run()
