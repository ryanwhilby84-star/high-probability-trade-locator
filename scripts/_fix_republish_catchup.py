from pathlib import Path

p = Path("src/hptl/cot/pipeline.py")
t = p.read_text(encoding="utf-8")

old = '''def _republish_downstream_exports(
    result: CotPipelineResult,
    *,
    export_week: str | None,
    cftc_week: str | None,
) -> None:
    """Rebuild confluence + cot_3y + mirrors when master is ahead of dashboard."""
    log_step(
        f"Downstream stale: export ({export_week or '—'}) < master "
        f"({result.latest_local_report_date}) — republishing dashboard JSON."
    )
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
    except Exception as exc:
        logger.warning("cot_3y export failed (confluence OK): %s", exc)

'''

new = '''def _republish_downstream_exports(
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

'''

if old not in t:
    raise SystemExit("republish block not found")
p.write_text(t.replace(old, new, 1), encoding="utf-8")
print("republish uses catch-up first")
