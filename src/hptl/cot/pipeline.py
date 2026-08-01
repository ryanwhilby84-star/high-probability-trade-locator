"""End-to-end COT refresh: CFTC download → processed cache → master → confluence → dashboard JSON."""
from __future__ import annotations

import argparse
import json
import logging
import os
import shutil
import sys
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.confluence.build_decision_table import OUT_PATH, TARGET_MARKETS, tracked_master_csv_path
from hptl.confluence.dashboard_export import DIST_CONFLUENCE_PATH, sync_dist_exports
from hptl.confluence.cot_tracked_backfill import run_backfill_master
from hptl.cot.contracts import GOOD_WORKBOOK_DISPLAY_NAMES
from hptl.cot.report_dates import (
    get_latest_local_report_date,
    missing_workbook_markets,
    probe_cftc_latest_report_date,
    tracked_market_week_keys,
)
from hptl.cot.update_log import log_kv, log_step
from hptl.cot.workbook_export import run_workbook_export
from hptl.cot.weekly_run_log import persist_weekly_run
from hptl.logging_setup import setup_logging

logger = logging.getLogger(__name__)

PROBE_CACHE_PATH = Path("data/processed/cot_probe_cache.json")

# The probe cache lets repeated same-day runs skip the (slow) live CFTC download.
# It MUST expire, otherwise once local data matches a cached week the pipeline
# trusts that cache forever and never notices a newly published CFTC report.
# Override with HPTL_PROBE_CACHE_TTL_HOURS (0 = always re-check the live source).
try:
    PROBE_CACHE_TTL_HOURS = float(os.environ.get("HPTL_PROBE_CACHE_TTL_HOURS", "12"))
except (TypeError, ValueError):
    PROBE_CACHE_TTL_HOURS = 12.0


@dataclass
class CotPipelineResult:
    run_timestamp_utc: str = ""
    latest_local_report_date: str | None = None
    latest_cftc_report_date: str | None = None
    update_needed: bool = False
    update_performed: bool = False
    rows_fetched: int = 0
    rows_added: int = 0
    rows_skipped_duplicates: int = 0
    markets_updated: list[str] = field(default_factory=list)
    markets_missing: list[str] = field(default_factory=list)
    markets_skipped_no_change: list[str] = field(default_factory=list)
    export_workbook_path: str | None = None
    export_processed_csv: str | None = None
    export_master_csv: str | None = None
    export_confluence_path: str | None = None
    export_latest_cot_week: str | None = None
    cot_data_stale: bool = False
    environment_feeds_ran: bool = False
    integrity_gate_checked: int = 0
    integrity_gate_passed: int = 0
    integrity_gate_failed: int = 0
    integrity_gate_failed_instruments: list[str] = field(default_factory=list)
    error: str | None = None
    exit_code: int = 0

    def to_log_dict(self) -> dict[str, Any]:
        return {k: getattr(self, k) for k in self.__dataclass_fields__}


def _iso(d: pd.Timestamp | date | None) -> str | None:
    if d is None:
        return None
    if isinstance(d, pd.Timestamp):
        if pd.isna(d):
            return None
        return d.strftime("%Y-%m-%d")
    return d.isoformat()


def _markets_with_new_keys(before: set[tuple[str, str]], after: set[tuple[str, str]]) -> list[str]:
    return sorted({m for m, _ in (after - before)})


def _print_banner(title: str) -> None:
    print("=" * 72)
    print(title)
    print("=" * 72)


def _human_lines(result: CotPipelineResult) -> list[str]:
    lines = [
        f"latest local COT week:     {result.latest_local_report_date or '(none)'}",
        f"latest CFTC report week:   {result.latest_cftc_report_date or '(unknown)'}",
        f"COT data stale (export):   {result.cot_data_stale}",
        f"update needed:             {result.update_needed}",
        f"update performed:        {result.update_performed}",
    ]
    if result.update_needed or result.update_performed:
        lines.extend(
            [
                f"rows fetched (probe):      {result.rows_fetched}",
                f"new market-week keys:      {result.rows_added}",
                f"duplicate keys skipped:  {result.rows_skipped_duplicates}",
                f"markets with new data:     {', '.join(result.markets_updated) or '(none)'}",
            ]
        )
    if result.markets_missing:
        lines.append(f"markets missing on CFTC week: {', '.join(result.markets_missing)}")
    if result.markets_skipped_no_change:
        lines.append(f"TARGET_MARKETS unchanged:  {', '.join(result.markets_skipped_no_change)}")
    lines.extend(
        [
            f"workbook export:           {result.export_workbook_path or '—'}",
            f"processed CSV:             {result.export_processed_csv or '—'}",
            f"tracked master CSV:        {result.export_master_csv or '—'}",
            f"confluence JSON:           {result.export_confluence_path or '—'}",
            f"dashboard latest COT week: {result.export_latest_cot_week or '—'}",
            f"environment feeds:         {'yes' if result.environment_feeds_ran else 'no'}",
            (
                "integrity gate:            "
                f"{result.integrity_gate_passed}/{result.integrity_gate_checked} passed"
                + (
                    f" — quarantined: {', '.join(result.integrity_gate_failed_instruments)}"
                    if result.integrity_gate_failed_instruments
                    else ""
                )
            ),
        ]
    )
    if result.error:
        lines.append(f"ERROR: {result.error}")
    return lines


def _write_probe_cache(cftc_max: pd.Timestamp, urls: tuple[str, ...]) -> None:
    PROBE_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PROBE_CACHE_PATH.write_text(
        json.dumps(
            {
                "probed_at_utc": datetime.now(timezone.utc).isoformat(),
                "latest_cftc_report_date": _iso(cftc_max),
                "source_urls": list(urls),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


def _confluence_export_latest_week() -> str | None:
    out = Path(OUT_PATH)
    if not out.exists():
        return None
    try:
        payload = json.loads(out.read_text(encoding="utf-8"))
        v = payload.get("latest_cot_report_date")
        return str(v) if v else None
    except (OSError, json.JSONDecodeError):
        return None



def _downstream_export_stale(local_iso: str | None, export_week: str | None) -> bool:
    """True when dashboard/confluence export trails the local/master COT week."""
    if not local_iso:
        return False
    if not export_week:
        return True
    return str(export_week)[:10] < str(local_iso)[:10]


def _scanner_export_week() -> str | None:
    """Latest week advertised by scanner_latest.json (public or data copy)."""
    for path in (
        Path("web-dashboard/public/data/scanner_latest.json"),
        Path("data/scanner_latest.json"),
    ):
        if not path.exists():
            continue
        try:
            doc = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        week = doc.get("latest_week")
        if not week:
            att = doc.get("scanner_attention_week") or {}
            if isinstance(att, dict):
                week = att.get("calendar_week")
        if week:
            return str(week)[:10]
    return None


def _publish_scanner_latest(*, confluence_path: str | Path | None = None) -> Path:
    """Write scanner_latest.json from confluence and mirror into dist/data."""
    from hptl.thesis_tracker.opportunity_distribution_report import write_scanner_latest

    scanner_path = write_scanner_latest(Path(confluence_path or OUT_PATH))
    dist_scanner = Path("web-dashboard/dist/data/scanner_latest.json")
    if scanner_path.exists():
        dist_scanner.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(scanner_path, dist_scanner)
    return scanner_path


def _republish_downstream_exports(
    result: CotPipelineResult,
    *,
    export_week: str | None,
    cftc_week: str | None,
) -> None:
    """Republish confluence + cot_3y + mirrors when master is ahead of dashboard.

    Prefers incremental catch-up (missing weeks only); falls back to a full
    confluence rebuild if catch-up cannot advance the export week.
    """
    log_step(
        f"Downstream stale: export ({export_week or '—'}) < master "
        f"({result.latest_local_report_date}) — republishing dashboard JSON."
    )
    from hptl.confluence.export_from_masters import catch_up_confluence_export

    new_week: str | None = None
    try:
        catch = catch_up_confluence_export(
            cot_feed_meta={
                "latest_cftc_report_date": cftc_week or result.latest_local_report_date,
                "cot_data_stale": False,
            }
        )
        new_week = catch.confluence_after
        if catch.error or _downstream_export_stale(result.latest_local_report_date, new_week):
            raise RuntimeError(
                catch.error
                or f"catch-up left export at {new_week}, master={result.latest_local_report_date}"
            )
        result.export_confluence_path = catch.export_path or str(Path(OUT_PATH).resolve())
        log_step(f"Incremental catch-up advanced dashboard to {new_week}")
    except Exception as exc:
        log_step(f"Incremental catch-up insufficient ({exc}); full confluence rebuild.")
        conf_path, new_week = _safe_rebuild_confluence(
            previous_latest=export_week,
            cftc_week=cftc_week or result.latest_local_report_date,
        )
        result.export_confluence_path = str(conf_path.resolve())

    result.export_latest_cot_week = new_week
    result.cot_data_stale = False
    result.update_performed = True
    log_kv("confluence export path", result.export_confluence_path)
    log_kv("latest week in JSON", new_week)
    try:
        cot3_path = _export_cot_workstation_series()
        log_kv("cot_3y series path", str(cot3_path.resolve()))
        synced = _sync_confluence_dashboard_exports()
        if synced:
            log_step("Synced public JSON → dist/data.")
    except Exception as exc:
        logger.warning("cot_3y export failed (confluence OK): %s", exc)
    # Scanner slice is a separate publish artifact; confluence alone does not refresh it.
    try:
        scanner_path = _publish_scanner_latest(
            confluence_path=result.export_confluence_path or OUT_PATH
        )
        log_kv("scanner_latest path", str(scanner_path.resolve()))
        log_kv("scanner_latest week", _scanner_export_week())
    except Exception as exc:
        logger.warning("scanner_latest export failed (confluence OK): %s", exc)
    try:
        from hptl.cot.commercial_attention_export import run_commercial_attention_export

        attn = run_commercial_attention_export(as_of=result.latest_local_report_date)
        log_kv(
            "commercial attention board",
            f"week={attn.get('source_week')} events={(attn.get('summary') or {}).get('with_events')}",
        )
    except Exception as exc:
        logger.warning("commercial attention export failed (confluence OK): %s", exc)
    try:
        from hptl.cot.workstation_intelligence_export import run_workstation_intelligence_export

        intel = run_workstation_intelligence_export()
        log_kv(
            "workstation intelligence",
            f"available={(intel.get('summary') or {}).get('markets_available')}",
        )
    except Exception as exc:
        logger.warning("workstation intelligence export failed (confluence OK): %s", exc)
    try:
        from hptl.cot.positioning_research_export import run_positioning_research_export

        research = run_positioning_research_export(markets=None)
        log_kv(
            "positioning research",
            f"available={(research.get('summary') or {}).get('markets_available')}/"
            f"{(research.get('summary') or {}).get('markets_requested')}",
        )
    except Exception as exc:
        logger.warning("positioning research export failed (confluence OK): %s", exc)

def _read_probe_cache() -> dict[str, Any] | None:
    if not PROBE_CACHE_PATH.exists():
        return None
    try:
        return json.loads(PROBE_CACHE_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _probe_cache_age_hours(cache: dict[str, Any] | None) -> float | None:
    """Age of the probe cache in hours (None when timestamp missing/unparseable)."""
    if not cache:
        return None
    ts = pd.to_datetime(cache.get("probed_at_utc"), errors="coerce", utc=True)
    if pd.isna(ts):
        return None
    return max(0.0, (pd.Timestamp.now(tz="UTC") - ts).total_seconds() / 3600.0)


def _probe_cache_is_trusted(cache: dict[str, Any] | None, local_max: pd.Timestamp | None) -> bool:
    """Trust cache only when it is CFTC-sourced, matches local data, and is fresh.

    Rejects test fixtures, caches older than the local processed week, and caches
    older than ``PROBE_CACHE_TTL_HOURS`` so a newly published CFTC report is
    detected on the next run without requiring ``--force``.
    """
    if not cache:
        return False
    week = cache.get("latest_cftc_report_date")
    urls = cache.get("source_urls") or []
    if not week:
        return False
    if not urls or not all("cftc.gov" in str(u).lower() for u in urls):
        log_step("Ignoring probe cache (missing or non-CFTC source URLs).")
        return False
    if local_max is not None and pd.Timestamp(week) != pd.Timestamp(local_max).normalize():
        log_step(
            f"Ignoring probe cache (cached {week} != local processed max {_iso(local_max)})."
        )
        return False
    age_hours = _probe_cache_age_hours(cache)
    if age_hours is None or age_hours > PROBE_CACHE_TTL_HOURS:
        age_txt = "unknown" if age_hours is None else f"{age_hours:.1f}h"
        log_step(
            f"Ignoring probe cache (age {age_txt} > TTL {PROBE_CACHE_TTL_HOURS:g}h) — "
            "re-checking live CFTC source for a newer weekly report."
        )
        return False
    return True


def _clear_probe_cache() -> None:
    if PROBE_CACHE_PATH.exists():
        PROBE_CACHE_PATH.unlink(missing_ok=True)
        log_step(f"Removed stale probe cache: {PROBE_CACHE_PATH.resolve()}")


def _legacy_latest_report_date() -> pd.Timestamp | None:
    """Max report date across instruments in ``legacy_cot_latest.json``."""
    from hptl.cot.legacy_cot_loader import legacy_cot_latest_path, load_legacy_cot_document

    path = legacy_cot_latest_path()
    if not path.exists():
        return None
    try:
        doc = load_legacy_cot_document(str(path))
    except (OSError, json.JSONDecodeError, TypeError, ValueError):
        return None
    max_dt: pd.Timestamp | None = None
    for block in (doc.get("instruments") or {}).values():
        weeks = ((block.get("groups") or {}).get("noncommercials") or {}).get("weeks") or []
        if not weeks:
            continue
        last = weeks[-1].get("report_date")
        ts = pd.to_datetime(last, errors="coerce")
        if pd.isna(ts):
            continue
        ts = pd.Timestamp(ts).normalize()
        if max_dt is None or ts > max_dt:
            max_dt = ts
    return max_dt


def _refresh_legacy_positioning(*, cftc_max: pd.Timestamp | None = None) -> None:
    """Re-download Legacy Futures-Only ZIPs and rebuild ``legacy_cot_latest.json``.

    Confluence/master positioning reads exclusively from legacy exports — workbook
    disaggregated/TFF downloads do not feed the dashboard bundle.
    """
    from datetime import datetime as dt, timezone as tz

    from hptl.cot.legacy_cot import default_history_years, ensure_legacy_futures_only_year, run_legacy_cot_reset

    year = dt.now(tz.utc).year
    legacy_max = _legacy_latest_report_date()
    needs_zip = (
        cftc_max is not None
        and not pd.isna(cftc_max)
        and (legacy_max is None or legacy_max < cftc_max.normalize())
    )
    if needs_zip:
        log_step(f"Refreshing Legacy COT year {year} ZIP (CFTC {_iso(cftc_max)} > legacy {_iso(legacy_max)})…")
        ensure_legacy_futures_only_year(year, download=True, force_refresh=True)
    elif legacy_max is not None and cftc_max is not None and legacy_max >= cftc_max.normalize():
        log_step(f"Legacy COT already at {_iso(legacy_max)} — skipping ZIP re-download.")
    else:
        log_step(f"Ensuring Legacy COT year {year} ZIP is present…")
        ensure_legacy_futures_only_year(year, download=True, force_refresh=bool(needs_zip))

    if legacy_max is not None and cftc_max is not None and legacy_max >= cftc_max.normalize():
        log_step("Legacy COT JSON current — skipping full legacy rebuild.")
        return

    log_step("Rebuilding legacy_cot_latest.json (positioning source for master + confluence)…")
    run_legacy_cot_reset(years=default_history_years())


def _export_cot_workstation_series() -> Path:
    """Write ``cot_3y_series_latest.json`` to processed + public dashboard paths."""
    from hptl.cot.cot_3y_series_export import run as run_cot_3y_export

    log_step("Writing cot_3y_series_latest.json (COT workstation charts)…")
    return run_cot_3y_export()


def _sync_confluence_dashboard_exports(source: Path | None = None) -> list[Path]:
    """Copy canonical JSON to ``dist/data`` so ``vite preview`` / built assets are not stale."""
    if source and Path(source).exists() and Path(source).resolve() != Path(OUT_PATH).resolve():
        shutil.copy2(source, OUT_PATH)
    return sync_dist_exports()


def _safe_rebuild_confluence(*, previous_latest: str | None, cftc_week: str | None) -> tuple[Path, str]:
    from hptl.confluence import build_decision_table as bdt

    out = Path(OUT_PATH)
    backup = out.with_suffix(".json.bak")
    if out.exists():
        shutil.copy2(out, backup)

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    # Weekly COT refresh legitimately exceeds the 120s stage watchdog during confluence export.
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    # Restore Jul-15 fast-export gate: Stage 4 must not re-enter FX V3 / RBA Excel parsing
    # for every market-week. Valuation exports run as a separate pillar stage.
    os.environ.setdefault("HPTL_SKIP_VALUATION", "1")
    bdt.run(
        cot_feed_meta={
            "latest_cftc_report_date": cftc_week,
            "cot_data_stale": False,
        }
    )

    if not out.exists():
        if backup.exists():
            shutil.copy2(backup, out)
        raise RuntimeError(f"Confluence export missing: {out}")

    payload = json.loads(out.read_text(encoding="utf-8"))
    records = payload.get("records")
    if not isinstance(records, list) or not records:
        if backup.exists():
            shutil.copy2(backup, out)
        raise RuntimeError("Confluence export has no records.")

    new_latest = str(payload.get("latest_cot_report_date") or "")
    if previous_latest and new_latest and new_latest < previous_latest:
        if backup.exists():
            shutil.copy2(backup, out)
        raise RuntimeError(f"Confluence week regressed ({previous_latest} → {new_latest}).")

    synced = _sync_confluence_dashboard_exports(out)
    for p in synced:
        log_kv("synced dashboard JSON copy", p)

    return out, new_latest


def _run_environment_feeds() -> None:
    from hptl.intelligence.run_environment_feed_update import main as env_main

    prev = os.environ.get("HPTL_SKIP_LIVE_FEEDS")
    os.environ["HPTL_SKIP_LIVE_FEEDS"] = "0"
    try:
        rc = env_main()
        if rc != 0:
            raise RuntimeError(f"environment feed update exited {rc}")
    finally:
        if prev is None:
            os.environ.pop("HPTL_SKIP_LIVE_FEEDS", None)
        else:
            os.environ["HPTL_SKIP_LIVE_FEEDS"] = prev


def run_full_pipeline(
    *,
    force: bool = False,
    with_live_feeds: bool = False,
    skip_confluence: bool = False,
    probe_only: bool = False,
    ensure_years_from: int = 2025,
) -> CotPipelineResult:
    """Single entry for weekly COT refresh used by ``hptl.cot.run_update``."""
    setup_logging()
    result = CotPipelineResult(run_timestamp_utc=datetime.now(timezone.utc).isoformat())

    local_max = get_latest_local_report_date()
    result.latest_local_report_date = _iso(local_max)
    export_week_start = _confluence_export_latest_week()

    log_step("HPTL COT update starting")
    log_kv("local processed max week", result.latest_local_report_date)
    log_kv("confluence export week (before)", export_week_start)
    log_kv("confluence export path", Path(OUT_PATH).resolve())
    dist_week = None
    if DIST_CONFLUENCE_PATH.exists():
        try:
            dist_week = json.loads(DIST_CONFLUENCE_PATH.read_text(encoding="utf-8")).get("latest_cot_report_date")
        except (OSError, json.JSONDecodeError):
            pass
    if dist_week and export_week_start and str(dist_week) != str(export_week_start):
        log_kv("WARNING dist/data JSON week (stale copy?)", dist_week)

    probe = None
    try:
        if not force and local_max is not None:
            cache = _read_probe_cache()
            cached_week = cache.get("latest_cftc_report_date") if cache else None
            if cached_week and _probe_cache_is_trusted(cache, local_max):
                result.latest_cftc_report_date = cached_week
                result.rows_fetched = 0
                export_week = _confluence_export_latest_week()
                local_iso = result.latest_local_report_date
                needs_json = _downstream_export_stale(local_iso, export_week)
                if probe_only:
                    result.update_needed = False
                    result.export_latest_cot_week = export_week or local_iso
                    result.cot_data_stale = needs_json
                    _print_banner("HPTL COT PIPELINE — PROBE ONLY (cached)")
                    for line in _human_lines(result):
                        print(line)
                    return result
                # Upstream current (CFTC == local) is independent of dashboard freshness.
                result.update_needed = False
                if needs_json and not skip_confluence:
                    try:
                        _republish_downstream_exports(
                            result, export_week=export_week, cftc_week=cached_week
                        )
                    except Exception as exc:
                        result.error = f"Confluence rebuild failed: {exc}"
                        result.exit_code = 1
                        result.cot_data_stale = True
                    _print_banner("HPTL COT PIPELINE — DOWNSTREAM REPUBLISHED")
                    for line in _human_lines(result):
                        print(line)
                    print("=" * 72)
                    return result
                if needs_json and skip_confluence:
                    result.cot_data_stale = True
                    result.export_latest_cot_week = export_week or local_iso
                    _mark_confluence_stale_flag(
                        is_stale=True,
                        export_week=result.export_latest_cot_week,
                        cftc_week=cached_week,
                    )
                    _print_banner("HPTL COT PIPELINE — UPSTREAM CURRENT, DASHBOARD STALE")
                    for line in _human_lines(result):
                        print(line)
                    print("=" * 72)
                    return result
                log_step(
                    f"No new CFTC week (cached probe {cached_week} matches local {local_iso}); "
                    "dashboard export is current."
                )
                result.cot_data_stale = False
                result.export_latest_cot_week = export_week or local_iso
                synced = _sync_confluence_dashboard_exports()
                if synced:
                    log_step("Synced public JSON → dist/data (preview build was behind).")
                # Confluence can be current while scanner_latest.json is still stale.
                if _downstream_export_stale(local_iso, _scanner_export_week()):
                    try:
                        scanner_path = _publish_scanner_latest(confluence_path=OUT_PATH)
                        log_step(
                            f"Refreshed stale scanner_latest → {_scanner_export_week()} "
                            f"({scanner_path})"
                        )
                        result.update_performed = True
                    except Exception as exc:
                        logger.warning("scanner_latest refresh failed: %s", exc)
                _mark_confluence_stale_flag(
                    is_stale=False,
                    export_week=result.export_latest_cot_week,
                    cftc_week=cached_week,
                )
                _print_banner("HPTL COT PIPELINE — UP TO DATE")
                for line in _human_lines(result):
                    print(line)
                print("=" * 72)
                return result
            elif cached_week and not _probe_cache_is_trusted(cache, local_max):
                _clear_probe_cache()

        probe = probe_cftc_latest_report_date()
        result.latest_cftc_report_date = _iso(probe.latest_report_date)
        result.rows_fetched = probe.rows_fetched
        log_kv("latest detected CFTC week (combined)", result.latest_cftc_report_date)
        if probe.commodity_max_report_date is not None:
            log_kv("parsed max (commodity)", _iso(probe.commodity_max_report_date))
        if probe.financial_max_report_date is not None:
            log_kv("parsed max (financial)", _iso(probe.financial_max_report_date))
        if probe.latest_report_date is not None and not pd.isna(probe.latest_report_date):
            _write_probe_cache(probe.latest_report_date, probe.source_urls)

    except Exception as exc:
        result.error = f"CFTC probe failed: {exc}"
        logger.exception(result.error)
        result.exit_code = 1
        _print_banner("HPTL COT PIPELINE — FAILED")
        for line in _human_lines(result):
            print(line)
        return result

    cftc_max = probe.latest_report_date if probe is not None else None
    if cftc_max is None or pd.isna(cftc_max):
        result.error = "Could not determine latest CFTC report date."
        result.exit_code = 1
        _print_banner("HPTL COT PIPELINE — FAILED")
        for line in _human_lines(result):
            print(line)
        return result

    if probe_only:
        _print_banner("HPTL COT PIPELINE — PROBE ONLY")
        for line in _human_lines(result):
            print(line)
        return result

    export_week = _confluence_export_latest_week()
    local_iso = result.latest_local_report_date
    cftc_iso = result.latest_cftc_report_date

    if not force and local_max is not None and cftc_max <= local_max:
        # Upstream ingestion not needed — but dashboard may still be behind master.
        result.update_needed = False
        needs_json_rebuild = _downstream_export_stale(local_iso, export_week)
        result.cot_data_stale = needs_json_rebuild or bool(
            cftc_iso and export_week and str(export_week)[:10] < str(cftc_iso)[:10]
        )
        result.export_latest_cot_week = export_week or local_iso

        if needs_json_rebuild and not skip_confluence:
            _print_banner("HPTL COT PIPELINE — DOWNSTREAM REPUBLISH (master newer than dashboard)")
            try:
                _republish_downstream_exports(
                    result, export_week=export_week, cftc_week=cftc_iso
                )
                print(f"Wrote confluence: {result.export_confluence_path}")
            except Exception as exc:
                result.error = f"Confluence rebuild failed: {exc}"
                result.exit_code = 1
                _mark_confluence_stale_flag(is_stale=True, export_week=export_week, cftc_week=cftc_iso)
        elif needs_json_rebuild and skip_confluence:
            result.cot_data_stale = True
            _mark_confluence_stale_flag(is_stale=True, export_week=export_week, cftc_week=cftc_iso)
            _print_banner("HPTL COT PIPELINE — UPSTREAM CURRENT, DASHBOARD STALE")
            for line in _human_lines(result):
                print(line)
            print("=" * 72)
            return result
        else:
            _mark_confluence_stale_flag(
                is_stale=False, export_week=result.export_latest_cot_week, cftc_week=cftc_iso
            )

        _print_banner(
            "HPTL COT PIPELINE — DOWNSTREAM REPUBLISHED"
            if result.update_performed
            else "HPTL COT PIPELINE — NO NEW CFTC WEEK"
        )
        for line in _human_lines(result):
            print(line)
        print("=" * 72)
        return result

    result.update_needed = True
    before_keys = tracked_market_week_keys()
    previous_confluence_latest = result.latest_local_report_date

    _print_banner("HPTL COT PIPELINE — DOWNLOAD & PARSE")
    print("Sources: Disaggregated commodities + Traders in Financial Futures (indices/FX)")
    try:
        export_paths = run_workbook_export()
        result.export_workbook_path = str(export_paths.workbook_path.resolve())
        result.export_processed_csv = str(export_paths.processed_csv_path.resolve())
        print(f"Wrote workbook:   {result.export_workbook_path}")
        print(f"Wrote processed:  {result.export_processed_csv}")
    except Exception as exc:
        result.error = f"COT download/export failed: {exc}"
        logger.exception(result.error)
        result.exit_code = 1
        _print_banner("HPTL COT PIPELINE — FAILED")
        for line in _human_lines(result):
            print(line)
        return result

    _print_banner("HPTL COT PIPELINE — LEGACY POSITIONING REFRESH")
    try:
        _refresh_legacy_positioning(cftc_max=cftc_max)
    except Exception as exc:
        result.error = f"Legacy COT refresh failed: {exc}"
        logger.exception(result.error)
        result.exit_code = 1
        _print_banner("HPTL COT PIPELINE — FAILED")
        for line in _human_lines(result):
            print(line)
        return result

    _print_banner("HPTL COT PIPELINE — MERGE TRACKED MASTER")
    try:
        master_path = run_backfill_master(ensure_years_from=ensure_years_from)
        result.export_master_csv = str(master_path.resolve())
        print(f"Wrote master:     {result.export_master_csv}")
    except Exception as exc:
        result.error = f"Tracked master rebuild failed: {exc}"
        logger.exception(result.error)
        result.exit_code = 1
        _print_banner("HPTL COT PIPELINE — FAILED")
        for line in _human_lines(result):
            print(line)
        return result

    after_keys = tracked_market_week_keys()
    added_keys = after_keys - before_keys
    result.rows_added = len(added_keys)
    new_date_str = _iso(cftc_max)
    result.rows_skipped_duplicates = sum(1 for _m, d in before_keys if d == new_date_str)
    result.markets_updated = _markets_with_new_keys(before_keys, after_keys)

    if probe is not None and not probe.dashboard_rows.empty:
        result.markets_missing = [
            GOOD_WORKBOOK_DISPLAY_NAMES.get(m, m)
            for m in missing_workbook_markets(probe.dashboard_rows, cftc_max)
        ]

    if result.rows_added == 0:
        result.markets_skipped_no_change = [m for m in TARGET_MARKETS if m not in result.markets_updated]

    if not skip_confluence:
        _print_banner("HPTL COT PIPELINE — REBUILD CONFLUENCE / SCANNER JSON")
        log_step("Rebuilding confluence JSON (scans all cot_cleaned_*.csv — often 1–3 min, not frozen)…")
        try:
            conf_path, new_week = _safe_rebuild_confluence(
                previous_latest=previous_confluence_latest,
                cftc_week=result.latest_cftc_report_date,
            )
            result.export_confluence_path = str(conf_path.resolve())
            result.export_latest_cot_week = new_week
            result.cot_data_stale = False
            n_inst = len(result.markets_updated) if result.markets_updated else len(TARGET_MARKETS)
            log_kv("instruments with new week keys", len(result.markets_updated))
            log_kv("TARGET_MARKETS count", len(TARGET_MARKETS))
            log_kv("confluence export path", result.export_confluence_path)
            log_kv("latest week in JSON", result.export_latest_cot_week)
            print(f"Wrote confluence: {result.export_confluence_path}")
            print(f"Latest COT week in JSON: {result.export_latest_cot_week}")
            print(f"Instruments updated this run: {n_inst} ({', '.join(result.markets_updated) or 'see TARGET_MARKETS'})")
        except Exception as exc:
            result.error = f"Confluence rebuild failed: {exc}"
            logger.exception(result.error)
            result.cot_data_stale = True
            result.exit_code = 1
            _mark_confluence_stale_flag(is_stale=True, export_week=previous_confluence_latest, cftc_week=result.latest_cftc_report_date)
            _print_banner("HPTL COT PIPELINE — FAILED")
            for line in _human_lines(result):
                print(line)
            return result

        try:
            cot3_path = _export_cot_workstation_series()
            print(f"Wrote COT workstation: {cot3_path.resolve()}")
        except Exception as exc:
            logger.warning("cot_3y export failed (confluence OK): %s", exc)

        if with_live_feeds:
            _print_banner("HPTL COT PIPELINE — LIVE ENVIRONMENT FEEDS")
            try:
                _run_environment_feeds()
                result.environment_feeds_ran = True
            except Exception as exc:
                logger.warning("Environment feeds failed (COT export OK): %s", exc)

        _print_banner("HPTL COT PIPELINE — WEEKLY INTEGRITY GATE")
        try:
            from hptl.cot.weekly_integrity_gate import run_weekly_integrity_gate

            gate = run_weekly_integrity_gate(
                force_download=False,
                seed_thesis=True,
                republish_on_quarantine=True,
                skip_deliverable_markdown=True,
            )
            result.integrity_gate_checked = gate.checked_count
            result.integrity_gate_passed = gate.passed_count
            result.integrity_gate_failed = gate.failed_count
            result.integrity_gate_failed_instruments = list(gate.failed_instruments)
            if gate.error:
                logger.warning("Integrity gate warning: %s", gate.error)
            if gate.failed_count and gate.exit_code:
                result.exit_code = max(result.exit_code, gate.exit_code)
        except Exception as exc:
            result.error = result.error or f"Weekly integrity gate failed: {exc}"
            logger.exception(result.error)
            result.exit_code = 1

    result.update_performed = result.exit_code == 0 and not result.error
    _print_banner("HPTL COT PIPELINE — COMPLETE")
    for line in _human_lines(result):
        print(line)
    print("=" * 72)

    try:
        persist_weekly_run(result.to_log_dict(), human_lines=_human_lines(result))
    except OSError as exc:
        logger.warning("Could not persist run log: %s", exc)

    return result


def _mark_confluence_stale_flag(*, is_stale: bool, export_week: str | None, cftc_week: str | None) -> None:
    """Patch ``cot_feed_status`` on existing confluence JSON without full rebuild."""
    out = Path(OUT_PATH)
    if not out.exists():
        return
    try:
        payload = json.loads(out.read_text(encoding="utf-8"))
        payload["cot_feed_status"] = {
            "latest_export_cot_week": export_week,
            "latest_cftc_report_date": cftc_week,
            "is_stale": is_stale,
            "checked_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        out.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    except (OSError, json.JSONDecodeError, TypeError) as exc:
        logger.warning("Could not patch cot_feed_status: %s", exc)


def _parse_cli(argv: list[str] | None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="HPTL full COT update (download → master → confluence JSON)")
    p.add_argument("--force", action="store_true", help="Re-download and rebuild even if local week is current")
    p.add_argument("--with-live-feeds", action="store_true", help="Run environment feed update after confluence rebuild")
    p.add_argument("--skip-confluence", action="store_true", help="Stop after tracked master (no JSON rebuild)")
    p.add_argument("--probe-only", action="store_true", help="Only probe CFTC latest week (no download)")
    p.add_argument("--ensure-from-year", type=int, default=2025, help="Earliest commodity backfill year")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = _parse_cli(argv)
    result = run_full_pipeline(
        force=args.force,
        with_live_feeds=args.with_live_feeds,
        skip_confluence=args.skip_confluence,
        probe_only=args.probe_only,
        ensure_years_from=args.ensure_from_year,
    )
    result.exit_code = 0 if result.error is None else (result.exit_code or 1)
    return result.exit_code


if __name__ == "__main__":
    raise SystemExit(main())
