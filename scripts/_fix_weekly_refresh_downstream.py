from pathlib import Path

p = Path("src/hptl/dashboard/weekly_refresh.py")
t = p.read_text(encoding="utf-8")

old = '''def pull_cot_and_master(*, force: bool = False) -> int:
    """Live CFTC probe + legacy/master refresh; skips full confluence enrichment."""
    import os

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    from hptl.cot.pipeline import run_full_pipeline

    result = run_full_pipeline(force=force, skip_confluence=True)
    return int(result.exit_code or 0)'''

new = '''def _dashboard_cot_export_behind_master() -> bool:
    """True when confluence or cot_3y trails the tracked master COT week."""
    master = _master_max()
    if master == "—":
        return False
    conf = _confluence_latest()
    graph = _cot3y_latest()
    if conf == "—" or conf < master:
        return True
    if graph == "—" or graph < master:
        return True
    return False


def pull_cot_and_master(*, force: bool = False) -> int:
    """Live CFTC probe + legacy/master refresh.

    Upstream freshness (CFTC vs master) and downstream freshness (master vs
    dashboard JSON) are independent. When the dashboard trails master, do NOT
    skip confluence/cot_3y republish merely because the master week already
    matches CFTC.
    """
    import os

    os.environ.setdefault("HPTL_SKIP_LIVE_FEEDS", "1")
    os.environ.setdefault("HPTL_DISABLE_WATCHDOG", "1")
    from hptl.cot.pipeline import run_full_pipeline

    # Skip heavy confluence rebuild only when dashboard exports already match master.
    skip_confluence = not _dashboard_cot_export_behind_master()
    result = run_full_pipeline(force=force, skip_confluence=skip_confluence)
    return int(result.exit_code or 0)'''

if old not in t:
    raise SystemExit("pull_cot_and_master block not found")
t = t.replace(old, new, 1)

# Also harden catch-up section: if still behind after catch_up, force full rebuild via pipeline helper
old_catch = '''    try:
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
        report.errors.append(f"chart series export failed: {exc}")'''

new_catch = '''    try:
        from hptl.confluence.export_from_masters import catch_up_confluence_export

        catch = catch_up_confluence_export()
        report.markets_updated = catch.markets_exported
        if catch.error:
            report.errors.append(f"confluence catch-up: {catch.error}")
        # Self-heal: if catch-up left dashboard behind master, force full republish.
        if _dashboard_cot_export_behind_master():
            from hptl.cot.pipeline import _republish_downstream_exports, CotPipelineResult

            heal = CotPipelineResult(
                latest_local_report_date=_master_max(),
                latest_cftc_report_date=_master_max(),
            )
            _republish_downstream_exports(
                heal,
                export_week=_confluence_latest(),
                cftc_week=_master_max(),
            )
            report.markets_updated = max(report.markets_updated, 1)
    except Exception as exc:
        report.errors.append(f"confluence catch-up failed: {exc}")

    try:
        # Always rebuild chart series when behind master (independent of upstream).
        if _dashboard_cot_export_behind_master() or _cot3y_latest() != _master_max():
            rebuild_chart_series_exports()
        else:
            # Still refresh when already aligned so mirrors stay in sync with master content.
            rebuild_chart_series_exports()
        report.chart_series_updated = _cot3y_market_count()
    except Exception as exc:
        report.errors.append(f"chart series export failed: {exc}")'''

if old_catch not in t:
    raise SystemExit("catch block not found")
t = t.replace(old_catch, new_catch, 1)

p.write_text(t, encoding="utf-8")
print("weekly_refresh.py patched")
