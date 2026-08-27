from pathlib import Path

# ========== pipeline.py ==========
p = Path("src/hptl/cot/pipeline.py")
t = p.read_text(encoding="utf-8")

# Add helper after _confluence_export_latest_week
helper = '''
def _downstream_export_stale(local_iso: str | None, export_week: str | None) -> bool:
    """True when dashboard/confluence export trails the local/master COT week."""
    if not local_iso:
        return False
    if not export_week:
        return True
    return str(export_week)[:10] < str(local_iso)[:10]


def _republish_downstream_exports(
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

if "_downstream_export_stale" not in t:
    anchor = "def _read_probe_cache()"
    i = t.find(anchor)
    if i < 0:
        raise SystemExit("anchor _read_probe_cache missing")
    t = t[:i] + helper + t[i:]
    print("inserted helpers")
else:
    print("helpers already present")

# Replace the cached-probe early-exit block
old_cache = '''                export_week = _confluence_export_latest_week()
                local_iso = result.latest_local_report_date
                needs_json = bool(local_iso and export_week and export_week < local_iso)
                if probe_only:
                    result.update_needed = False
                    result.export_latest_cot_week = export_week or local_iso
                    _print_banner("HPTL COT PIPELINE — PROBE ONLY (cached)")
                    for line in _human_lines(result):
                        print(line)
                    return result
                if needs_json and not skip_confluence:
                    log_step(
                        f"Export ({export_week}) < local ({local_iso}) — rebuilding confluence JSON only."
                    )
                    try:
                        conf_path, new_week = _safe_rebuild_confluence(
                            previous_latest=export_week,
                            cftc_week=cached_week,
                        )
                        result.export_confluence_path = str(conf_path.resolve())
                        result.export_latest_cot_week = new_week
                        result.cot_data_stale = False
                        result.update_performed = True
                        log_kv("confluence export path", result.export_confluence_path)
                        log_kv("latest week in JSON", new_week)
                    except Exception as exc:
                        result.error = f"Confluence rebuild failed: {exc}"
                        result.exit_code = 1
                    _print_banner("HPTL COT PIPELINE — COMPLETE")
                    for line in _human_lines(result):
                        print(line)
                    print("=" * 72)
                    return result
                elif not needs_json:
                    log_step(
                        f"No new CFTC week (cached probe {cached_week} matches local {local_iso}). "
                        "Use --force to re-download."
                    )
                    result.update_needed = False
                    result.cot_data_stale = bool(export_week and cached_week and export_week < cached_week)
                    result.export_latest_cot_week = export_week or local_iso
                    if export_week and local_iso and export_week >= local_iso:
                        synced = _sync_confluence_dashboard_exports()
                        if synced:
                            log_step("Synced public JSON → dist/data (preview build was behind).")
                    _mark_confluence_stale_flag(
                        is_stale=result.cot_data_stale,
                        export_week=result.export_latest_cot_week,
                        cftc_week=cached_week,
                    )
                    _print_banner("HPTL COT PIPELINE — UP TO DATE")
                    for line in _human_lines(result):
                        print(line)
                    print("=" * 72)
                    return result'''

new_cache = '''                export_week = _confluence_export_latest_week()
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
                _mark_confluence_stale_flag(
                    is_stale=False,
                    export_week=result.export_latest_cot_week,
                    cftc_week=cached_week,
                )
                _print_banner("HPTL COT PIPELINE — UP TO DATE")
                for line in _human_lines(result):
                    print(line)
                print("=" * 72)
                return result'''

if old_cache not in t:
    raise SystemExit("old_cache block not found")
t = t.replace(old_cache, new_cache, 1)
print("patched cache early-exit")

# Replace the live-probe "no new CFTC week" block
old_live = '''    if not force and local_max is not None and cftc_max <= local_max:
        result.update_needed = False
        needs_json_rebuild = bool(
            local_iso
            and export_week
            and export_week < local_iso
        )
        result.cot_data_stale = bool(
            (cftc_iso and export_week and export_week < cftc_iso)
            or needs_json_rebuild
        )
        result.export_latest_cot_week = export_week or local_iso

        if needs_json_rebuild and not skip_confluence:
            _print_banner("HPTL COT PIPELINE — REBUILD CONFLUENCE (local data newer than export)")
            log_step("Rebuilding confluence JSON (export behind local master)…")
            try:
                conf_path, new_week = _safe_rebuild_confluence(
                    previous_latest=export_week,
                    cftc_week=cftc_iso,
                )
                result.export_confluence_path = str(conf_path.resolve())
                result.export_latest_cot_week = new_week
                result.cot_data_stale = False
                result.update_performed = True
                print(f"Wrote confluence: {result.export_confluence_path}")
                try:
                    cot3_path = _export_cot_workstation_series()
                    print(f"Wrote COT workstation: {cot3_path.resolve()}")
                except Exception as exc:
                    logger.warning("cot_3y export failed (confluence OK): %s", exc)
            except Exception as exc:
                result.error = f"Confluence rebuild failed: {exc}"
                result.exit_code = 1
                _mark_confluence_stale_flag(is_stale=True, export_week=export_week, cftc_week=cftc_iso)

        elif result.cot_data_stale:
            _mark_confluence_stale_flag(is_stale=True, export_week=export_week, cftc_week=cftc_iso)
        else:
            _mark_confluence_stale_flag(is_stale=False, export_week=result.export_latest_cot_week, cftc_week=cftc_iso)

        _print_banner("HPTL COT PIPELINE — NO NEW CFTC WEEK")
        for line in _human_lines(result):
            print(line)
        if result.cot_data_stale and not needs_json_rebuild:
            print("TIP: run with --force to re-download, or rebuild confluence if master CSV is current.")
        print("=" * 72)
        return result'''

new_live = '''    if not force and local_max is not None and cftc_max <= local_max:
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
        return result'''

if old_live not in t:
    raise SystemExit("old_live block not found")
t = t.replace(old_live, new_live, 1)
print("patched live no-new-week path")

p.write_text(t, encoding="utf-8")
print("pipeline.py written")
