from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

from .macro_scoring import REQUIRED_SCORING_INPUTS, score_macro
from .rates_downloader import download_all
from .rates_parser import process_rates

EXPORT_DIR = Path("data/exports")

MACRO_HISTORY_COLS = [
    "macro_snapshot_date",
    "latest_available_date",
    "data_lag_days",
    "dgs2",
    "dgs10",
    "dgs30",
    "fed_funds",
    "yield_curve_10y2y",
    "dgs2_1w_change",
    "dgs10_1w_change",
    "dgs30_1w_change",
    "yield_curve_10y2y_1w_change",
    "dgs2_4w_change",
    "dgs10_4w_change",
    "dgs30_4w_change",
    "yield_curve_10y2y_4w_change",
    "macro_signal",
    "rates_bias",
    "curve_context",
    "policy_pressure",
    "macro_score",
    "macro_strength",
    "macro_context_for_trades",
    "macro_summary",
]


def run() -> Path:
    print("=" * 70)
    print("Macro history build started")
    print("=" * 70)

    raw = download_all()
    print(f"Raw rates rows downloaded: {len(raw)}")

    clean = process_rates()
    print(f"Clean rates rows processed: {len(clean)}")

    scored = score_macro(clean)
    latest_available_date = scored[scored[["dgs2", "dgs10", "dgs30"]].notna().all(axis=1)]["date"].max()

    scored_rows = scored[scored["macro_score"].notna()].copy()
    if scored_rows.empty:
        raise ValueError("No historical macro rows could be scored from available rates data.")

    if scored_rows[REQUIRED_SCORING_INPUTS].isna().any(axis=1).any():
        raise ValueError("Fail-closed check failed: score exists beside blank required directional inputs")

    scored_rows["macro_snapshot_date"] = pd.to_datetime(scored_rows["macro_snapshot_date"], errors="coerce").dt.normalize()
    scored_rows["latest_available_date"] = latest_available_date

    out = scored_rows[MACRO_HISTORY_COLS].copy()
    out = out.sort_values("macro_snapshot_date").drop_duplicates(subset=["macro_snapshot_date"], keep="last")

    if out[["dgs2", "dgs10", "dgs30"]].isna().any(axis=1).any():
        raise ValueError("Fail-closed check failed: missing required DGS2/DGS10/DGS30 values in output rows")

    EXPORT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = EXPORT_DIR / f"macro_history_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        out.to_excel(writer, sheet_name="Macro_History", index=False)

    print("=" * 70)
    print("Input rates files used:")
    print("  - data/raw/macro/rates_raw.csv")
    print("  - data/processed/macro/rates_clean.csv")
    print(f"Date range covered: {out['macro_snapshot_date'].min()} -> {out['macro_snapshot_date'].max()}")
    print(f"Row count: {len(out)}")
    print(f"Output path: {output_path}")
    print("=" * 70)

    return output_path


if __name__ == "__main__":
    run()
