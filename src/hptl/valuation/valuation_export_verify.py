"""Inspect and log FX valuation dashboard export freshness."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.valuation.currency_futures_ive_v1 import PUBLIC_JSON as FUTURES_PUBLIC
from hptl.valuation.export import PUBLIC_OUT as VALUATION_PUBLIC
from hptl.valuation.fx_v3_audit import PUBLIC_JSON as FX_V3_PUBLIC

# Scanner ValuationCell reads currency_futures_ive_latest.json (not valuation_latest).
SCANNER_FX_INSTRUMENTS: tuple[tuple[str, str], ...] = (
    ("AUD", "Australian Dollar / 6A"),
    ("GBP", "British Pound / 6B"),
    ("CAD", "Canadian Dollar / 6C"),
    ("JPY", "Japanese Yen / 6J"),
    ("CHF", "Swiss Franc / 6S"),
    ("EUR", "Euro FX / 6E"),
    ("NZD", "NZ Dollar / 6N"),
    ("DX", "US Dollar Index / DX"),
)


def dashboard_export_paths() -> dict[str, Path]:
    return {
        "scanner_primary": FUTURES_PUBLIC,
        "fx_v3_pairs": FX_V3_PUBLIC,
        "valuation_pillar": VALUATION_PUBLIC,
    }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def file_mtime_iso(path: Path) -> str:
    if not path.exists():
        return "—"
    ts = path.stat().st_mtime
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat(timespec="seconds")


def summarize_export_file(path: Path) -> dict[str, Any]:
    doc = _read_json(path)
    return {
        "path": str(path),
        "exists": path.exists(),
        "mtime_utc": file_mtime_iso(path),
        "generated_at": str(doc.get("generated_at") or "—"),
        "size_bytes": path.stat().st_size if path.exists() else 0,
    }


def summarize_scanner_fx_rows(path: Path | None = None) -> list[dict[str, Any]]:
    """Rows matching scanner ValuationCell (currency futures IVE)."""
    path = path or FUTURES_PUBLIC
    doc = _read_json(path)
    instruments = doc.get("instruments") or {}
    rows: list[dict[str, Any]] = []
    for label, market_id in SCANNER_FX_INSTRUMENTS:
        block = instruments.get(market_id) or {}
        diag = block.get("valuation_diagnostics") or {}
        input_dates = diag.get("input_latest_dates") or {}
        rows.append(
            {
                "label": label,
                "market_id": market_id,
                "valuation_pct": block.get("valuation_pct"),
                "valuation_pct_raw": block.get("valuation_pct_raw"),
                "model_status": block.get("model_status"),
                "publishable": block.get("publishable"),
                "inputs_stale": block.get("inputs_stale") or diag.get("inputs_stale"),
                "freshness_status": diag.get("freshness_status"),
                "spot_date": diag.get("spot_date"),
                "input_latest_dates": input_dates,
                "stale_reason": diag.get("stale_reason"),
            }
        )
    return rows


def print_valuation_export_banner(title: str) -> None:
    print(f"\n{'=' * 72}")
    print(title)
    print(f"{'=' * 72}")


def log_fx_input_refresh_start() -> None:
    print_valuation_export_banner("FX VALUATION — refreshing price/macro inputs")
    skip = __import__("os").environ.get("HPTL_SKIP_LIVE_FEEDS", "unset")
    print(f"  HPTL_SKIP_LIVE_FEEDS={skip}  (1=cache-only macro/rates; prices may still fetch)")


def log_fx_input_refresh_done(report: dict[str, Any]) -> None:
    print(f"  FX input refresh completed: {str(report.get('generated_at', '—'))[:19]}")
    if report.get("fatal_error"):
        print(f"  WARNING: {report['fatal_error']}")
    prices = report.get("prices") or {}
    if prices:
        print("  Price refresh summary:")
        for sym, rec in sorted(prices.items()):
            if isinstance(rec, dict):
                print(
                    f"    {sym}: status={rec.get('status')} "
                    f"last_date={rec.get('last_date')} bars={rec.get('daily_bars')}"
                )
    macro = report.get("macro") or {}
    if isinstance(macro, dict) and macro.get("currency_rates"):
        print("  Macro/rates ingest statuses:", macro.get("currency_rates"))


def log_valuation_exports_start() -> None:
    print_valuation_export_banner("FX VALUATION — writing dashboard exports")
    print("  Scanner reads: web-dashboard/public/data/currency_futures_ive_latest.json")
    print("  Also writes:   fx_valuation_v3_latest.json, valuation_latest.json")


def log_valuation_exports_done(paths: dict[str, Path]) -> None:
    print("  Files written:")
    for key in ("currency_futures_public", "fx_v3_public", "fx_v3_audit", "public", "data"):
        p = paths.get(key)
        if p is None:
            continue
        path = Path(p)
        if not path.exists():
            print(f"    [{key}] MISSING {path}")
            continue
        doc = _read_json(path)
        gen = str(doc.get("generated_at") or "—")[:19]
        print(f"    [{key}] {path}")
        print(f"             mtime_utc={file_mtime_iso(path)}  generated_at={gen}")

    print("\n  Scanner FX valuation % (currency_futures_ive_latest.json):")
    futures_path = Path(paths.get("currency_futures_public") or FUTURES_PUBLIC)
    for row in summarize_scanner_fx_rows(futures_path):
        pct = row["valuation_pct"]
        raw = row["valuation_pct_raw"]
        pct_s = f"{pct:+.2f}%" if pct is not None else "—"
        raw_s = f" (raw {raw:+.4f}%)" if raw is not None and pct is not None and raw != pct else ""
        stale = row.get("freshness_status") or "—"
        spot = row.get("spot_date") or "—"
        print(
            f"    {row['label']:3}  {pct_s}{raw_s}  "
            f"status={row.get('model_status')}  fresh={stale}  spot={spot}"
        )
        if row.get("stale_reason"):
            print(f"         stale: {row['stale_reason']}")
    print(f"{'=' * 72}\n")


def print_verification_report() -> int:
    """Print full verification report; return 0 if primary scanner export exists."""
    paths = dashboard_export_paths()
    print_valuation_export_banner("VALUATION EXPORT VERIFICATION")
    print("\nDashboard data sources:")
    print("  Scanner ValuationCell  -> currency_futures_ive_latest.json  (PRIMARY for FX futures)")
    print("  FxValuationV3Panel     -> fx_valuation_v3_latest.json         (pair V3 detail)")
    print("  useValuationLatest     -> valuation_latest.json               (pillar/metals/agri)")
    print("\nWriters:")
    print("  write_valuation_exports() in hptl.valuation.export")
    print("  Called from rebuild_pillar_exports() in hptl.dashboard.weekly_refresh")
    print("  Entry CLI: scripts/weekly_dashboard_refresh.py -> run_weekly_refresh()")

    skip = __import__("os").environ.get("HPTL_SKIP_LIVE_FEEDS", "unset")
    print(f"\nHPTL_SKIP_LIVE_FEEDS={skip}")

    for name, path in paths.items():
        info = summarize_export_file(path)
        print(f"\n--- {name} ---")
        print(f"  path:         {info['path']}")
        print(f"  exists:       {info['exists']}")
        print(f"  mtime_utc:    {info['mtime_utc']}")
        print(f"  generated_at: {info['generated_at']}")
        print(f"  size_bytes:   {info['size_bytes']}")

    print("\n--- Scanner FX % (matches dashboard ValuationCell) ---")
    futures = paths["scanner_primary"]
    if not futures.exists():
        print("  MISSING — run: python scripts/weekly_dashboard_refresh.py")
        return 1

    doc = _read_json(futures)
    print(f"  export generated_at: {doc.get('generated_at', '—')}")
    for row in summarize_scanner_fx_rows(futures):
        pct = row["valuation_pct"]
        pct_s = f"{pct:+.2f}%" if pct is not None else "—"
        dates = row.get("input_latest_dates") or {}
        y2 = dates.get("AUD.y2") or dates.get("USD.y2") or dates.get(f"{row['label']}.y2")
        pol = dates.get("AUD.policy_rate") or dates.get("USD.policy_rate")
        print(
            f"  {row['label']:3}  {pct_s:>8}  spot={row.get('spot_date') or '—':10}  "
            f"fresh={row.get('freshness_status') or '—':7}  status={row.get('model_status')}"
        )
        if dates:
            spot_d = dates.get("spot")
            print(f"       inputs: spot={spot_d}  sample_y2={y2}  sample_policy={pol}")
        if row.get("stale_reason"):
            print(f"       stale_reason: {row['stale_reason']}")

    dist = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "currency_futures_ive_latest.json"
    if dist.exists():
        pub_mtime = futures.stat().st_mtime
        dist_mtime = dist.stat().st_mtime
        if dist_mtime < pub_mtime - 1:
            print(f"\n  WARNING: dist copy older than public ({file_mtime_iso(dist)} vs {file_mtime_iso(futures)})")
            print("  Run weekly refresh or copy public/data -> dist/data for production builds.")
        else:
            print(f"\n  dist/data copy OK (mtime {file_mtime_iso(dist)})")

    print(f"\n{'=' * 72}")
    return 0
