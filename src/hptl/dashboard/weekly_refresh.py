"""Weekly dashboard refresh — COT/master → pillars → confluence → chart series (no full enrichment loop)."""
from __future__ import annotations

import json
import shutil
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROJECT_ROOT, PROCESSED_DIR
from hptl.confluence.dashboard_export import DIST_CONFLUENCE_PATH, OUT_PATH, sync_dist_exports
from hptl.cot.report_dates import get_latest_local_report_date

MASTER_CSV = PROCESSED_DIR / "cot_tracked_master_normalized.csv"
PUBLIC_DATA = PROJECT_ROOT / "web-dashboard" / "public" / "data"
COT_3Y_PUBLIC = PUBLIC_DATA / "cot_3y_series_latest.json"
COT_3Y_PROCESSED = PROCESSED_DIR / "cot_3y_series_latest.json"
LEGACY_PUBLIC = PUBLIC_DATA / "legacy_cot_latest.json"
CHART_PROBE_MARKET = "Gold"


@dataclass
class WeeklyRefreshReport:
    master_latest: str = "—"
    cot_bundle_latest: str = "—"
    confluence_latest: str = "—"
    graph_latest: str = "—"
    markets_updated: int = 0
    chart_series_updated: int = 0
    stale_cleared: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    fx_valuation_status: str = "—"
    fx_valuation_error: str | None = None
    passed: bool = False

    def as_dict(self) -> dict[str, Any]:
        return {
            "master_latest": self.master_latest,
            "cot_bundle_latest": self.cot_bundle_latest,
            "confluence_latest": self.confluence_latest,
            "graph_latest": self.graph_latest,
            "markets_updated": self.markets_updated,
            "chart_series_updated": self.chart_series_updated,
            "stale_cleared": self.stale_cleared,
            "errors": self.errors,
            "fx_valuation_status": self.fx_valuation_status,
            "fx_valuation_error": self.fx_valuation_error,
            "passed": self.passed,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _master_max() -> str:
    if not MASTER_CSV.exists():
        return "—"
    df = pd.read_csv(MASTER_CSV, usecols=["cot_report_date"], low_memory=False)
    return str(df["cot_report_date"].astype(str).str[:10].max())


def _legacy_max() -> str:
    doc = _read_json(PROJECT_ROOT / "data" / "legacy_cot_latest.json")
    maxd = ""
    for block in (doc.get("instruments") or {}).values():
        weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
        if weeks:
            d = str(weeks[-1].get("report_date") or "")[:10]
            if d > maxd:
                maxd = d
    return maxd or "—"


def _confluence_latest() -> str:
    p = OUT_PATH if OUT_PATH.is_absolute() else (PROJECT_ROOT / OUT_PATH)
    return str(_read_json(p).get("latest_cot_report_date") or "—")[:10]


def _cot3y_latest(market: str = CHART_PROBE_MARKET) -> str:
    doc = _read_json(COT_3Y_PUBLIC)
    blk = (doc.get("markets") or {}).get(market) or {}
    return str(blk.get("latest_date") or "—")[:10]


def _cot3y_market_count() -> int:
    doc = _read_json(COT_3Y_PUBLIC)
    return len(doc.get("markets") or {})


def clear_stale_dashboard_copies() -> list[str]:
    """Mirror fresh public JSON into dist/ and drop stale backup copies."""
    cleared: list[str] = []
    dist_dir = PROJECT_ROOT / "web-dashboard" / "dist" / "data"
    if dist_dir.exists():
        for src in PUBLIC_DATA.glob("*.json"):
            dest = dist_dir / src.name
            if dest.exists() and dest.stat().st_mtime >= src.stat().st_mtime:
                continue
            shutil.copy2(src, dest)
            cleared.append(f"synced dist/data/{src.name}")

    bak = (PROJECT_ROOT / OUT_PATH if not OUT_PATH.is_absolute() else OUT_PATH).with_suffix(".json.bak")
    if bak.exists():
        bak.unlink()
        cleared.append(f"removed {bak.name}")

    stale_probe = PROCESSED_DIR / "cot_probe_cache.json"
    if stale_probe.exists():
        try:
            cache = _read_json(stale_probe)
            trusted_urls = cache.get("source_urls") or []
            if any("example.com" in str(u) for u in trusted_urls):
                stale_probe.unlink()
                cleared.append("removed untrusted cot_probe_cache.json")
        except OSError:
            pass

    return cleared


def rebuild_chart_series_exports() -> Path:
    """Rebuild cot_3y_series_latest.json from master CSV (positioning chart source)."""
    from hptl.cot.cot_3y_series_export import run as run_cot_3y

    return run_cot_3y()


def refresh_fx_valuation_inputs() -> dict[str, Any]:
    """Refresh FX futures price + macro inputs used by IVE / V3 valuation exports."""
    from hptl.valuation.fx_futures_data_refresh import refresh_fx_futures_data

    return refresh_fx_futures_data()


def rebuild_fx_valuation_exports() -> dict[str, Any]:
    """ISOLATED FX valuation stage — refresh FX macro inputs + write FX/valuation exports.

    Raises on a hard failure (e.g. a broken FX macro workbook reaching
    ``_parse_rba_workbook`` / ``pandas.read_excel``) so the caller can record it,
    skip this stage, and continue the rest of the dashboard refresh. Valuation
    logic is unchanged — this only isolates the dependency.
    """
    from hptl.valuation.export import write_valuation_exports

    meta: dict[str, Any] = {}
    fx_report = refresh_fx_valuation_inputs()
    meta["fx_refresh_at"] = str(fx_report.get("generated_at") or datetime.now(timezone.utc).isoformat())

    write_valuation_exports()
    meta["fx_valuation_exports_at"] = datetime.now(timezone.utc).isoformat()

    fx_warnings: list[str] = []
    try:
        from hptl.valuation.currency_futures_ive_v1 import write_currency_futures_ive_export

        write_currency_futures_ive_export()
    except Exception as exc:
        fx_warnings.append(f"currency_futures_ive export: {exc}")

    try:
        from hptl.valuation.fx_v3_audit import write_fx_v3_audit_artifacts

        write_fx_v3_audit_artifacts()
    except Exception as exc:
        fx_warnings.append(f"fx_valuation_v3 export: {exc}")

    if fx_warnings:
        meta["fx_warnings"] = fx_warnings
    return meta


def rebuild_pillar_exports() -> dict[str, Any]:
    """Rebuild valuation/location/seasonality pillar exports.

    FX valuation runs as an isolated sub-stage: a broken FX macro workbook must
    not stop the independent location and seasonality exports (or the wider
    dashboard refresh). Any FX failure is recorded under ``fx_valuation_error``
    and reported at the end instead of aborting the run.
    """
    from hptl.location.export import write_location_exports
    from hptl.seasonality.export import build_seasonality_latest, write_seasonality_exports

    meta: dict[str, Any] = {}

    try:
        meta.update(rebuild_fx_valuation_exports())
    except Exception as exc:
        meta["fx_valuation_error"] = f"{type(exc).__name__}: {exc}"

    # Independent pillars — always run regardless of the FX valuation outcome.
    try:
        write_location_exports()
    except Exception as exc:
        meta.setdefault("warnings", []).append(f"location export: {exc}")

    try:
        write_seasonality_exports(build_seasonality_latest())
    except Exception as exc:
        meta.setdefault("warnings", []).append(f"seasonality export: {exc}")

    return meta


def rebuild_workstation_exports() -> Path:
    from hptl.prices.workstation_ohlc_export import write_workstation_ohlc_exports

    return write_workstation_ohlc_exports()


def refresh_legacy_cot_if_stale() -> str:
    """Rebuild legacy_cot_latest.json when it trails the tracked master CSV."""
    import pandas as pd

    from hptl.cot.pipeline import _legacy_latest_report_date, _refresh_legacy_positioning
    from hptl.cot.report_dates import get_latest_local_report_date

    master_ts = get_latest_local_report_date()
    legacy_ts = _legacy_latest_report_date()
    if master_ts is None or pd.isna(master_ts):
        return "—"
    if legacy_ts is not None and not pd.isna(legacy_ts) and legacy_ts >= master_ts.normalize():
        return str(legacy_ts)[:10]
    _refresh_legacy_positioning(cftc_max=pd.Timestamp(master_ts).normalize())
    refreshed = _legacy_latest_report_date()
    return str(refreshed)[:10] if refreshed is not None and not pd.isna(refreshed) else "—"


def pull_cot_and_master(*, force: bool = False) -> int:
    """Live CFTC probe + legacy/master refresh; skips full confluence enrichment."""
    import os

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    from hptl.cot.pipeline import run_full_pipeline

    result = run_full_pipeline(force=force, skip_confluence=True)
    return int(result.exit_code or 0)


def validate_alignment(*, probe_market: str = CHART_PROBE_MARKET) -> tuple[bool, list[str]]:
    """Fail when header/confluence/master/chart weeks diverge."""
    master = _master_max()
    legacy = _legacy_max()
    conf = _confluence_latest()
    graph = _cot3y_latest(probe_market)
    errors: list[str] = []

    if master == "—":
        errors.append("master CSV has no cot_report_date")
    if conf == "—":
        errors.append("confluence export missing latest_cot_report_date")
    if graph == "—":
        errors.append(f"cot_3y_series missing latest_date for {probe_market}")

    if master != "—":
        if legacy != "—" and legacy < master:
            errors.append(
                f"legacy COT bundle stale: legacy={legacy} master={master} "
                "(run refresh without --skip-cot-pull)"
            )
        if conf != "—" and conf < master:
            errors.append(f"confluence export stale: confluence={conf} master={master}")
        if graph != "—" and graph != master:
            errors.append(f"graph latest ({graph}) != master latest ({master}) for {probe_market}")
        if conf != "—" and graph != "—" and conf != graph:
            errors.append(f"graph ({graph}) != confluence ({conf}) for {probe_market}")

    return (len(errors) == 0, errors)


def run_weekly_refresh(*, force_cot: bool = False, skip_cot_pull: bool = False) -> WeeklyRefreshReport:
    import os

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")

    report = WeeklyRefreshReport()

    if not skip_cot_pull:
        rc = pull_cot_and_master(force=force_cot)
        if rc != 0:
            report.errors.append(f"COT/master pull exited {rc}")
    try:
        refresh_legacy_cot_if_stale()
    except Exception as exc:
        report.errors.append(f"legacy COT refresh failed: {exc}")

    try:
        meta = rebuild_pillar_exports()
        if meta.get("warnings"):
            report.errors.extend(meta["warnings"])
        # FX valuation is isolated: record its outcome but do NOT fail the
        # dashboard refresh when only the FX macro workbook is broken.
        if meta.get("fx_valuation_error"):
            report.fx_valuation_status = "FAILED"
            report.fx_valuation_error = str(meta["fx_valuation_error"])
        elif meta.get("fx_warnings"):
            report.fx_valuation_status = "PARTIAL"
            report.fx_valuation_error = "; ".join(meta["fx_warnings"])
        else:
            report.fx_valuation_status = "OK"
    except Exception as exc:
        # Non-FX pillar failure (location/seasonality) — preserve prior strictness.
        report.errors.append(f"pillar export failed: {exc}")

    try:
        from hptl.confluence.export_from_masters import catch_up_confluence_export

        catch = catch_up_confluence_export()
        report.markets_updated = catch.markets_exported
        if catch.error:
            report.errors.append(f"confluence catch-up: {catch.error}")
    except Exception as exc:
        report.errors.append(f"confluence catch-up failed: {exc}")

    try:
        rebuild_chart_series_exports()
        report.chart_series_updated = _cot3y_market_count()
    except Exception as exc:
        report.errors.append(f"chart series export failed: {exc}")

    try:
        rebuild_workstation_exports()
    except Exception as exc:
        report.errors.append(f"workstation OHLC export failed: {exc}")

    try:
        from hptl.thesis_tracker.opportunity_distribution_report import write_scanner_latest

        write_scanner_latest()
    except Exception as exc:
        report.errors.append(f"scanner export failed: {exc}")

    report.stale_cleared = clear_stale_dashboard_copies()
    sync_dist_exports()

    local_ts = get_latest_local_report_date()
    report.master_latest = _master_max() if _master_max() != "—" else (
        str(local_ts)[:10] if local_ts is not None and not pd.isna(local_ts) else "—"
    )
    report.cot_bundle_latest = _legacy_max()
    report.confluence_latest = _confluence_latest()
    report.graph_latest = _cot3y_latest(CHART_PROBE_MARKET)

    ok, val_errors = validate_alignment(probe_market=CHART_PROBE_MARKET)
    report.errors.extend(val_errors)
    report.passed = ok and not any(
        e for e in report.errors if not e.startswith("confluence catch-up") or "No master weeks" not in e
    )
    # Strict pass: no errors at all
    report.passed = ok and len(report.errors) == 0
    return report


def print_weekly_report(report: WeeklyRefreshReport) -> None:
    print("| Field | Value |")
    print("| --- | --- |")
    print(f"| latest master date | {report.master_latest} |")
    print(f"| latest COT bundle date | {report.cot_bundle_latest} |")
    print(f"| latest confluence date | {report.confluence_latest} |")
    print(f"| latest graph plotted date ({CHART_PROBE_MARKET}) | {report.graph_latest} |")
    print(f"| markets updated | {report.markets_updated} |")
    print(f"| chart series updated | {report.chart_series_updated} |")
    print(f"| stale files cleared | {len(report.stale_cleared)} |")
    for item in report.stale_cleared:
        print(f"  - {item}")
    print(f"| FX valuation stage | {report.fx_valuation_status} |")
    if report.fx_valuation_error:
        print(f"  - FX valuation issue (non-fatal, dashboard still refreshed): {report.fx_valuation_error}")
    if report.errors:
        print("| errors | |")
        for err in report.errors:
            print(f"  - {err}")
    print(f"| final status | {'PASS' if report.passed else 'FAIL'} |")
