#!/usr/bin/env python3
"""One-command HPTL data pipeline refresh — prices → COT → exports → dist sync."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")

# Max seconds for the isolated FX/instrument valuation-history subprocess before it
# is killed. A hung RBA workbook read (pandas.read_excel) inside currency_histories()
# must not block the pipeline. Override with HPTL_VAL_HISTORY_TIMEOUT.
try:
    VAL_HISTORY_TIMEOUT_S = int(os.environ.get("HPTL_VAL_HISTORY_TIMEOUT", "600"))
except (TypeError, ValueError):
    VAL_HISTORY_TIMEOUT_S = 600


def _step(label: str) -> None:
    print(f"\n--- {label} ---")


def refresh_oanda_commodity_backfill() -> None:
    from hptl.prices.cot_fail_backfill import OANDA_COT_FAIL_PAIRS, run_oanda_backfill
    from hptl.prices.promote_price_backfill import promote_staging_backfill

    summary = run_oanda_backfill(years=3)
    keys = [pair[2] for pair in OANDA_COT_FAIL_PAIRS]
    promotion = promote_staging_backfill(keys)
    print(f"OANDA commodity backfill: {summary}")
    print(f"Promoted {promotion.get('count', 0)} instrument(s)")


def refresh_workstation_index_history() -> None:
    from hptl.prices.workstation_index_ohlc_history import backfill_workstation_index

    for instrument_id in ("NASDAQ / NQ",):
        result = backfill_workstation_index(instrument_id, refresh=True)
        print(f"Workstation index OHLC {instrument_id}: {result.get('daily_rows')} daily rows")


def refresh_prices(*, live: bool, skip_validation: bool) -> int:
    if live:
        from hptl.prices.run_price_refresh import main as run_price_refresh

        argv: list[str] = []
        if skip_validation:
            argv.append("--skip-validation")
        return int(run_price_refresh(argv) or 0)

    from hptl.prices.price_store import PUBLIC_PATH, rebuild_price_store_from_disk

    path = rebuild_price_store_from_disk()
    print(f"Rebuilt price store from disk -> {path}")
    print(f"Dashboard export: {PUBLIC_PATH}")
    return 0


def refresh_valuation_history(*, markets: list[str] | None, max_weeks: int | None) -> None:
    from hptl.valuation.instrument_valuation_history_viz_export import (
        export_instrument_valuation_history,
        write_export,
    )

    doc = export_instrument_valuation_history(markets=markets or None, max_weeks=max_weeks)
    _, pub = write_export(doc)
    print(f"instrument_valuation_history_latest.json — {len(doc.get('instruments') or {})} markets → {pub}")


def run_valuation_history_isolated(max_weeks: int | None) -> tuple[str, str]:
    """Run the FX/instrument valuation-history export in a timeout-bounded subprocess.

    The export reaches ``currency_histories()`` → ``load_aud_rba_history()`` →
    ``pandas.read_excel()``, which can hang indefinitely. A plain try/except cannot
    interrupt a hung C/IO call, so the export runs as a separate process that is
    killed on timeout. Valuation logic and the RBA parser are unchanged — this only
    isolates the dependency. The child writes the export to disk itself.

    Returns ``(status, detail)`` where status is ``"ok" | "timeout" | "error"``.
    """
    cmd = [sys.executable, "-m", "hptl.valuation.instrument_valuation_history_viz_export"]
    if max_weeks is not None:
        cmd += ["--max-weeks", str(max_weeks)]

    env = dict(os.environ)
    env["PYTHONPATH"] = str(ROOT / "src") + os.pathsep + env.get("PYTHONPATH", "")

    try:
        proc = subprocess.run(cmd, cwd=str(ROOT), env=env, timeout=VAL_HISTORY_TIMEOUT_S)
    except subprocess.TimeoutExpired:
        print(
            f"WARNING (FX valuation history, non-fatal): timed out after {VAL_HISTORY_TIMEOUT_S}s "
            "(likely RBA workbook read_excel hang) — skipped, pipeline continues.",
            file=sys.stderr,
        )
        return "timeout", f"exceeded {VAL_HISTORY_TIMEOUT_S}s (RBA workbook load)"
    except Exception as exc:  # noqa: BLE001 — isolated stage, never fatal
        print(f"WARNING (FX valuation history, non-fatal): {exc}", file=sys.stderr)
        return "error", str(exc)

    if proc.returncode != 0:
        print(
            f"WARNING (FX valuation history, non-fatal): subprocess exited {proc.returncode}",
            file=sys.stderr,
        )
        return "error", f"subprocess exited {proc.returncode}"
    return "ok", ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Refresh the full HPTL workstation data pipeline in dependency order.",
    )
    parser.add_argument("--skip-prices", action="store_true", help="Skip price store refresh/rebuild.")
    parser.add_argument(
        "--cached-prices",
        action="store_true",
        help="Rebuild price exports from on-disk records only. Default is live price refresh.",
    )
    parser.add_argument(
        "--live-prices",
        action="store_true",
        help="Deprecated no-op; live price refresh is now the default.",
    )
    parser.add_argument("--skip-cot-pull", action="store_true", help="Skip live CFTC pull; use local master CSV.")
    parser.add_argument("--force-cot", action="store_true", help="Force CFTC re-download.")
    parser.add_argument(
        "--skip-valuation-history",
        action="store_true",
        help="Skip instrument_valuation_history_latest.json (slow viz export).",
    )
    parser.add_argument("--valuation-history-weeks", type=int, default=None, help="Limit weeks for valuation history.")
    parser.add_argument("--verify", action="store_true", help="Run freshness verification after refresh.")
    parser.add_argument("--json-report", type=str, default="", help="Write verification JSON to this path.")
    args = parser.parse_args(argv)

    errors: list[str] = []
    fx_failures: list[str] = []

    if not args.skip_prices:
        _step("1/8 Market prices")
        # Each price source is guarded independently. A single unavailable or
        # rate-limited source (e.g. Alpha Vantage informational/quota response,
        # which run_price_refresh may surface as SystemExit) must NOT terminate
        # the pipeline — record it and continue to COT/workstation/etc.
        try:
            rc_prices = refresh_prices(live=not args.cached_prices, skip_validation=args.cached_prices)
            if rc_prices:
                price_warnings.append(f"price refresh returned {rc_prices} (source unavailable or key missing)")
        except SystemExit as exc:
            price_warnings.append(f"price refresh aborted (source unavailable): {exc}")
            print(f"WARNING (price source, non-fatal): {exc}", file=sys.stderr)
        except Exception as exc:
            price_warnings.append(f"price refresh: {exc}")
            print(f"WARNING (price source, non-fatal): {exc}", file=sys.stderr)

        if not args.cached_prices:
            try:
                refresh_oanda_commodity_backfill()
            except Exception as exc:
                price_warnings.append(f"OANDA commodity backfill: {exc}")
                print(f"WARNING (price source, non-fatal): {exc}", file=sys.stderr)
            try:
                refresh_workstation_index_history()
            except Exception as exc:
                price_warnings.append(f"workstation index history: {exc}")
                print(f"WARNING (price source, non-fatal): {exc}", file=sys.stderr)

    _step("2/8 COT + master + pillar exports + confluence + workstation exports")
    from hptl.dashboard.weekly_refresh import print_weekly_report, run_weekly_refresh

    report = run_weekly_refresh(force_cot=args.force_cot, skip_cot_pull=args.skip_cot_pull)
    print_weekly_report(report)
    if report.errors:
        errors.extend(report.errors)
    # FX valuation is isolated inside run_weekly_refresh: surface its failure here
    # as a non-fatal FX issue so the dashboard refresh is not marked failed.
    if getattr(report, "fx_valuation_error", None):
        fx_failures.append(f"pillar FX valuation: {report.fx_valuation_error}")

    valuation_history_status = "skipped"
    if not args.skip_valuation_history:
        _step("3/8 FX / instrument valuation history (viz export — isolated subprocess)")
        # Timeout-bounded subprocess: a hung RBA workbook read must not block the
        # pipeline. Failures/timeouts are recorded as non-fatal FX warnings.
        valuation_history_status, vh_detail = run_valuation_history_isolated(args.valuation_history_weeks)
        if valuation_history_status != "ok":
            fx_failures.append(f"valuation history ({valuation_history_status}): {vh_detail}")
    else:
        _step("3/8 Instrument valuation history — skipped")

    _step("4/8 Verification")
    try:
        from hptl.prices.live_quotes_export import write_live_quotes_exports
        from hptl.prices.price_integrity_audit import write_price_integrity_audit

        write_live_quotes_exports()
        write_price_integrity_audit(fetch_live=False)
    except Exception as exc:
        errors.append(f"price visibility exports: {exc}")
        print(f"ERROR: {exc}", file=sys.stderr)

    from hptl.dashboard.pipeline_freshness import build_pipeline_freshness_report, print_freshness_report

    freshness = build_pipeline_freshness_report()
    if args.json_report:
        out = Path(args.json_report)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(json.dumps(freshness.as_dict(), indent=2), encoding="utf-8")
        print(f"Wrote report → {out}")

    rc = print_freshness_report(freshness)

    _step("HPTL refresh summary")
    dashboard_ok = report.passed and not errors
    print(f"  Dashboard refresh:      {'PASS' if dashboard_ok else 'FAIL'}")
    print(f"  Price-source warnings:  {len(price_warnings)} (non-fatal; verification reports staleness)")
    for item in price_warnings:
        print(f"    - {item}")
    print(f"  FX valuation warnings:  {len(fx_failures)} (non-fatal; FX stage isolated)")
    for item in fx_failures:
        print(f"    - {item}")
    print(f"  FX valuation history:   {valuation_history_status} (isolated subprocess, timeout {VAL_HISTORY_TIMEOUT_S}s)")
    print(f"  Fatal failures:         {len(errors)}")
    for item in errors:
        print(f"    - {item}")

    # Price-source and FX valuation problems are intentionally excluded from the
    # fatal exit code: a single unavailable/rate-limited source or a broken FX
    # workbook must not fail the whole HPTL dashboard refresh.
    if errors or not report.passed:
        return 1
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
