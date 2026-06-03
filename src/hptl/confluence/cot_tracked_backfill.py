"""Build validated tracked-market COT master CSV and refresh dashboard JSON.

Steps:
  1) Inventory ``data/processed/cot_cleaned_*.csv`` (rows, date span).
  2) Ensure annual CFTC commodity ZIPs exist for recent years (default 2025–current).
  3) Merge Legacy COT from ``data/legacy_cot_latest.json`` (same logic as ``build_decision_table``).
  4) Keep only ``TARGET_MARKETS``, dedupe ``(market, cot_report_date)``, sort ascending.
  5) Write ``cot_tracked_master_normalized.csv`` and print a coverage report.
  6) Run ``build_decision_table.run()`` to rebuild ``confluence_history_latest.json``.

Commodities: annual ``dea_cot`` style files resolved via ``Settings.cot_source_url`` pattern.
Positioning: run ``python -m hptl.cot.run_legacy_cot`` before backfill (Legacy Futures Only).
"""
from __future__ import annotations

import argparse
from dataclasses import replace
from datetime import datetime, timezone

import pandas as pd

from hptl.config import get_settings
from hptl.cot.downloader import download_latest_cot
from hptl.cot.parser import parse_cot_file

from hptl.confluence.build_decision_table import (
    PROCESSED_DIR,
    TARGET_MARKETS,
    TRACKED_MASTER_FILENAME,
    tracked_master_csv_path,
    _merge_cot_from_cleaned_csvs,
    _finalize_cot_pipeline,
)


FLOOR_DATE = pd.Timestamp("2025-01-01")
GAP_THRESHOLD_DAYS = 10


def _gap_events(dates: pd.Series) -> list[str]:
    d = pd.to_datetime(dates, errors="coerce").dropna().sort_values().reset_index(drop=True)
    out: list[str] = []
    for i in range(1, len(d)):
        delta = (d.iloc[i] - d.iloc[i - 1]).days
        if delta > GAP_THRESHOLD_DAYS:
            out.append(f"{d.iloc[i - 1].date()}→{d.iloc[i].date()} ({delta} days)")
    return out


def inspect_processed_cot_files() -> None:
    """Task 1–2: inventory current ``cot_cleaned_*.csv`` inputs."""
    files = sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime)
    print("=" * 72)
    print("COT_PROCESSED_FILE_INVENTORY (cot_cleaned_*.csv)")
    print("=" * 72)
    if not files:
        print("  (none found under data/processed/)")
        return
    for path in files:
        try:
            df = pd.read_csv(path, low_memory=False)
        except Exception as exc:
            print(f"  {path.name}: ERROR reading file: {exc}")
            continue
        dc = _find_date_col(df)
        if dc is None:
            print(f"  {path.name}: rows={len(df)} date_col=MISSING")
            continue
        dt = pd.to_datetime(df[dc], errors="coerce").dropna()
        if dt.empty:
            print(f"  {path.name}: rows={len(df)} dates=NONE")
            continue
        print(
            f"  {path.name}: rows={len(df)} "
            f"earliest={pd.Timestamp(dt.min()).date()} latest={pd.Timestamp(dt.max()).date()}"
        )


def _find_date_col(df: pd.DataFrame) -> str | None:
    for c in ("report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date"):
        if c in df.columns:
            return c
    return None


def ensure_annual_cot_downloads(start_year: int = 2025) -> None:
    """Download + write ``cot_cleaned_{year}_backfill.csv`` for missing commodity years."""
    settings = get_settings()
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    current_year = datetime.now(timezone.utc).year
    for year in range(start_year, current_year + 1):
        existing = list(PROCESSED_DIR.glob(f"cot_cleaned_{year}*.csv"))
        if existing:
            print(f"COT_BACKFILL_SKIP year={year} (already have {len(existing)} file(s))")
            continue
        year_settings = replace(settings, cot_year=year)
        print(f"COT_BACKFILL_DOWNLOAD year={year} …")
        try:
            dl = download_latest_cot(year_settings)
            parsed = parse_cot_file(dl.raw_file_path)
            out = PROCESSED_DIR / f"cot_cleaned_{year}_backfill.csv"
            parsed.to_csv(out, index=False)
            print(f"COT_BACKFILL_WROTE {out} rows={len(parsed)} url={dl.source_url}")
        except Exception as exc:
            print(f"COT_BACKFILL_FAIL year={year}: {exc}")


def print_per_market_validation(cot: pd.DataFrame) -> None:
    """Tasks 2 & 8: per tracked market stats + gap list + 2025+ floor check."""
    print("=" * 72)
    print("COT_TRACKED_COVERAGE_REPORT (merged, TARGET_MARKETS only, deduped)")
    print("=" * 72)
    for m in TARGET_MARKETS:
        sub = cot.loc[cot["market"] == m].sort_values("cot_report_date")
        if sub.empty:
            print(f"market={m!r} rows=0 earliest=N/A latest=N/A gaps=n/a floor2025=MISSING")
            continue
        earliest = pd.Timestamp(sub["cot_report_date"].min())
        latest = pd.Timestamp(sub["cot_report_date"].max())
        gaps = _gap_events(sub["cot_report_date"])
        post = sub.loc[sub["cot_report_date"] >= FLOOR_DATE]
        floor_ok = "ok" if not post.empty else "MISSING_ON_OR_AFTER_2025-01-01"
        gap_note = f"{len(gaps)} gap(s) >{GAP_THRESHOLD_DAYS}d" if gaps else "none"
        print(
            f"market={m!r} rows={len(sub)} earliest={earliest.date()} latest={latest.date()} "
            f"gaps={gap_note} floor2025={floor_ok}"
        )
        if gaps:
            for g in gaps[:20]:
                print(f"    gap: {g}")
            if len(gaps) > 20:
                print(f"    … ({len(gaps) - 20} more)")


def print_floor_summary(cot: pd.DataFrame) -> None:
    """Rows on/after 2025-01-01 per market (acceptance helper)."""
    print("-" * 72)
    print("COT_FLOOR_2025_ROW_COUNTS (reports with cot_report_date >= 2025-01-01)")
    print("-" * 72)
    for m in TARGET_MARKETS:
        sub = cot.loc[(cot["market"] == m) & (cot["cot_report_date"] >= FLOOR_DATE)]
        print(f"  {m}: n={len(sub)}")


def run_backfill_master(*, ensure_years_from: int = 2025) -> Path:
    ensure_annual_cot_downloads(start_year=ensure_years_from)
    cot = _merge_cot_from_cleaned_csvs()
    if cot.empty:
        raise RuntimeError("Merged COT is empty — check data/processed/cot_cleaned_*.csv and financial caches.")
    cot = cot.loc[cot["market"].isin(TARGET_MARKETS)].copy()
    cot = cot.sort_values(["market", "cot_report_date", "quality_score"], ascending=[True, True, False]).drop_duplicates(
        ["market", "cot_report_date"], keep="first"
    )
    cot = cot.sort_values(["market", "cot_report_date"]).reset_index(drop=True)
    print_per_market_validation(cot)
    print_floor_summary(cot)

    cot_scored = _finalize_cot_pipeline(cot)
    out = tracked_master_csv_path()
    out.parent.mkdir(parents=True, exist_ok=True)
    cot_scored.to_csv(out, index=False)
    print("=" * 72)
    print(f"COT_TRACKED_MASTER_WROTE path={out} rows={len(cot_scored)} filename={TRACKED_MASTER_FILENAME}")
    print("=" * 72)
    return out


def run(*, ensure_years_from: int = 2025) -> None:
    inspect_processed_cot_files()
    run_backfill_master(ensure_years_from=ensure_years_from)
    from hptl.confluence import build_decision_table as bdt_module

    bdt_module.run()


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="COT tracked backfill + confluence JSON rebuild")
    p.add_argument(
        "--ensure-from-year",
        type=int,
        default=2025,
        help="First commodity annual year to ensure on disk (default 2025).",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    run(ensure_years_from=args.ensure_from_year)
