"""Full HTPL COT data lineage — Source Truth through Dashboard → Scanner → Thesis → Scoring."""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.markets.instrument_registry import cot_mapped_ids

AUDIT_PARSER = "hptl.cot.data_lineage_audit"
DATA_OUT = Path("data/cot_data_lineage_latest.json")
PUBLIC_OUT = Path("web-dashboard/public/data/cot_data_lineage_latest.json")
DELIVERABLE_MD = Path("data/exports/cot_data_lineage_deliverable.md")

SOURCE_TRUTH_PATH = Path("data/cot_source_truth_audit_latest.json")
CONFLUENCE_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")
THESIS_PATH = Path("web-dashboard/public/data/thesis_tracker_latest.json")
LEGACY_LATEST_PATH = Path("data/legacy_cot_latest.json")

CHAIN_ORDER = [
    "source_truth",
    "dashboard",
    "scanner",
    "thesis",
    "scoring",
]


def _file_meta(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"file": str(path), "exists": False, "generated_at": None, "mtime_utc": None}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
        gen = doc.get("generated_at") if isinstance(doc, dict) else None
    except (OSError, json.JSONDecodeError):
        gen = None
    mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()
    return {"file": str(path.resolve()), "exists": True, "generated_at": gen, "mtime_utc": mtime}


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, str):
        s = v.strip().lower()
        if not s or s in {"n/a", "nan", "null", "none", "—"}:
            return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _values_equal(a: float | None, b: float | None) -> bool:
    if a is None and b is None:
        return True
    if a is None or b is None:
        return False
    return abs(a - b) < 0.5


def _bundle(
    nc_long: float | None = None,
    nc_short: float | None = None,
    nc_net: float | None = None,
    nr_long: float | None = None,
    nr_short: float | None = None,
    nr_net: float | None = None,
    cot_score: float | None = None,
) -> dict[str, float | None]:
    if nc_net is None and nc_long is not None and nc_short is not None:
        nc_net = nc_long - nc_short
    if nr_net is None and nr_long is not None and nr_short is not None:
        nr_net = nr_long - nr_short
    return {
        "nc_long": nc_long,
        "nc_short": nc_short,
        "nc_net": nc_net,
        "nr_long": nr_long,
        "nr_short": nr_short,
        "nr_net": nr_net,
        "cot_score": cot_score,
    }


def _layer(
    *,
    layer_id: str,
    dataset_name: str,
    file_path: Path,
    row_source: str,
    values: dict[str, float | None],
    report_date: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    meta = _file_meta(file_path)
    return {
        "layer": layer_id,
        "dataset_name": dataset_name,
        "file_name": file_path.name,
        "file_path": meta["file"],
        "file_exists": meta["exists"],
        "generated_at": meta["generated_at"],
        "mtime_utc": meta["mtime_utc"],
        "row_source": row_source,
        "report_date": report_date,
        "values": values,
        **(extra or {}),
    }


def _compare_layers(
    base: dict[str, float | None],
    other: dict[str, float | None],
    *,
    fields: tuple[str, ...] | None = None,
) -> dict[str, Any]:
    keys = fields or ("nc_long", "nc_short", "nc_net", "nr_long", "nr_short", "nr_net", "cot_score")
    diffs: list[dict[str, Any]] = []
    for key in keys:
        a, b = base.get(key), other.get(key)
        if not _values_equal(a, b):
            diffs.append({"field": key, "expected": a, "actual": b, "delta": (b - a) if a is not None and b is not None else None})
    return {"match": len(diffs) == 0, "differences": diffs}


def _load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _confluence_row_for_date(records: list[dict], instrument_id: str, report_date: str) -> dict[str, Any] | None:
    hits = [
        r
        for r in records
        if r.get("market") == instrument_id
        and str(r.get("cot_report_date") or r.get("date") or "")[:10] == report_date
    ]
    if not hits:
        return None
    hits.sort(key=lambda r: str(r.get("date") or ""))
    return hits[-1]


def _thesis_snapshot_for_date(theses: list[dict], instrument_id: str, report_date: str) -> dict[str, Any] | None:
    for th in theses:
        if str(th.get("market") or "").strip() != instrument_id:
            continue
        snaps = th.get("snapshots") or []
        for s in reversed(snaps):
            w = str(s.get("cot_report_date") or s.get("week") or "")[:10]
            if w == report_date:
                return s
    return None


def audit_instrument_lineage(
    instrument_id: str,
    *,
    truth_doc: dict[str, Any],
    confluence_doc: dict[str, Any],
    thesis_doc: dict[str, Any],
    report_date: str,
) -> dict[str, Any]:
    truth_inst = (truth_doc.get("instruments") or {}).get(instrument_id) or {}
    records = confluence_doc.get("records") or []
    conf_row = _confluence_row_for_date(records, instrument_id, report_date)
    thesis_snap = _thesis_snapshot_for_date(thesis_doc.get("theses") or [], instrument_id, report_date)

    off_nc = (truth_inst.get("official_raw_values") or {}).get("noncommercials") or {}
    off_nr = (truth_inst.get("official_raw_values") or {}).get("nonreportables") or {}

    source_truth = _layer(
        layer_id="source_truth",
        dataset_name="cot_source_truth_audit_latest",
        file_path=SOURCE_TRUTH_PATH,
        row_source=f"instruments.{instrument_id} official_raw_values (fresh CFTC deacot parse)",
        report_date=report_date,
        values=_bundle(
            nc_long=_num(off_nc.get("long")),
            nc_short=_num(off_nc.get("short")),
            nc_net=_num(off_nc.get("net")),
            nr_long=_num(off_nr.get("long")),
            nr_short=_num(off_nr.get("short")),
            nr_net=_num(off_nr.get("net")),
        ),
        extra={
            "cftc_code": truth_inst.get("selected_cftc_code"),
            "market_name": truth_inst.get("selected_market_name"),
            "official_raw_source_file": truth_inst.get("official_raw_source_file"),
            "official_raw_row_index": truth_inst.get("official_raw_row_index"),
        },
    )

    if not conf_row:
        dashboard = _layer(
            layer_id="dashboard",
            dataset_name="confluence_history_latest (missing row)",
            file_path=CONFLUENCE_PATH,
            row_source=f"records[market={instrument_id!r} cot_report_date={report_date}] MISSING",
            report_date=report_date,
            values=_bundle(),
        )
    else:
        groups = conf_row.get("cot_positioning_groups") or {}
        mm = groups.get("managed_money") or {}
        nr = groups.get("nonreportable") or {}
        dashboard = _layer(
            layer_id="dashboard",
            dataset_name="confluence_history_latest",
            file_path=CONFLUENCE_PATH,
            row_source=f"records[market={instrument_id!r} cot_report_date={report_date}]",
            report_date=report_date,
            values=_bundle(
                nc_long=_num(conf_row.get("long_value")),
                nc_short=_num(conf_row.get("short_value")),
                nc_net=_num(conf_row.get("net_value")),
                nr_long=_num(nr.get("long")),
                nr_short=_num(nr.get("short")),
                nr_net=_num(nr.get("net")),
                cot_score=_num(conf_row.get("cot_score")),
            ),
            extra={
                "calendar_week": conf_row.get("date"),
                "positioning_source": conf_row.get("positioning_source"),
                "trader_group_used": conf_row.get("trader_group_used"),
            },
        )

    # Scanner uses the same confluence row as dashboard (useConfluenceData → marketRows)
    scanner = _layer(
        layer_id="scanner",
        dataset_name="confluence_history_latest (scanner marketRows)",
        file_path=CONFLUENCE_PATH,
        row_source=f"same as dashboard — useConfluenceData.marketRows for week {report_date}",
        report_date=report_date,
        values=dashboard["values"].copy(),
        extra={"note": "Scanner table reads identical confluence record; divergence here indicates a UI bug."},
    )

    if not thesis_snap:
        thesis = _layer(
            layer_id="thesis",
            dataset_name="thesis_tracker_latest (no snapshot for date)",
            file_path=THESIS_PATH,
            row_source=f"theses[market={instrument_id!r}].snapshots[cot_report_date={report_date}] MISSING",
            report_date=report_date,
            values=_bundle(),
        )
    else:
        thesis = _layer(
            layer_id="thesis",
            dataset_name="thesis_tracker_latest",
            file_path=THESIS_PATH,
            row_source=f"theses[market={instrument_id!r}].snapshots[cot_report_date={report_date}]",
            report_date=report_date,
            values=_bundle(
                nc_long=_num(thesis_snap.get("long_value")),
                nc_short=_num(thesis_snap.get("short_value")),
                nc_net=_num(thesis_snap.get("net_value")),
                cot_score=_num(thesis_snap.get("cot_score")),
            ),
            extra={"thesis_id": next((t.get("thesis_id") for t in (thesis_doc.get("theses") or []) if t.get("market") == instrument_id), None)},
        )

    scoring = _layer(
        layer_id="scoring",
        dataset_name="confluence_history_latest (cot_score from build_decision_table)",
        file_path=CONFLUENCE_PATH,
        row_source=f"records[market={instrument_id!r}].cot_score — same row as dashboard",
        report_date=report_date,
        values=_bundle(
            nc_long=dashboard["values"].get("nc_long"),
            nc_short=dashboard["values"].get("nc_short"),
            nc_net=dashboard["values"].get("nc_net"),
            nr_long=dashboard["values"].get("nr_long"),
            nr_short=dashboard["values"].get("nr_short"),
            nr_net=dashboard["values"].get("nr_net"),
            cot_score=_num(conf_row.get("cot_score")) if conf_row else None,
        ),
        extra={"final_calculated_cot_score": _num((conf_row or {}).get("final_calculated_cot_score"))},
    )

    layers = {
        "source_truth": source_truth,
        "dashboard": dashboard,
        "scanner": scanner,
        "thesis": thesis,
        "scoring": scoring,
    }

    chain_checks: list[dict[str, Any]] = []
    first_fail: str | None = None
    failure_reasons: list[str] = []

    pairs: list[tuple[str, str, tuple[str, ...] | None]] = [
        ("source_truth", "dashboard", ("nc_long", "nc_short", "nc_net", "nr_long", "nr_short", "nr_net")),
        ("dashboard", "scanner", ("nc_long", "nc_short", "nc_net", "nr_long", "nr_short", "nr_net")),
        ("scanner", "thesis", ("nc_long", "nc_short", "nc_net", "cot_score")),
        ("thesis", "scoring", ("cot_score",)),
    ]
    for a_id, b_id, fields in pairs:
        cmp = _compare_layers(layers[a_id]["values"], layers[b_id]["values"], fields=fields)
        entry = {
            "from_layer": a_id,
            "to_layer": b_id,
            "match": cmp["match"],
            "differences": cmp["differences"],
        }
        chain_checks.append(entry)
        if not cmp["match"] and first_fail is None:
            first_fail = b_id
            for d in cmp["differences"]:
                failure_reasons.append(
                    f"{a_id}→{b_id} {d['field']}: expected={d['expected']} actual={d['actual']}"
                )

    overall = "PASS" if first_fail is None else "FAIL"

    # NC/NR positioning match across source_truth → dashboard (exclude cot_score from that gate)
    pos_match = _compare_layers(
        source_truth["values"],
        dashboard["values"],
        fields=("nc_long", "nc_short", "nc_net", "nr_long", "nr_short", "nr_net"),
    )["match"]

    return {
        "instrument": instrument_id,
        "report_date": report_date,
        "mapping": {
            "cftc_code": truth_inst.get("selected_cftc_code"),
            "market_name": truth_inst.get("selected_market_name"),
            "exchange": truth_inst.get("exchange"),
            "source_truth_status": truth_inst.get("status"),
        },
        "raw_source": {
            "official_raw_source_url": truth_inst.get("official_raw_source_url"),
            "official_raw_source_file": truth_inst.get("official_raw_source_file"),
            "official_raw_row_index": truth_inst.get("official_raw_row_index"),
            "report_type": "legacy_futures_only",
        },
        "layers": layers,
        "chain_checks": chain_checks,
        "overall_status": overall,
        "positioning_matches_source_truth": pos_match,
        "first_divergence_layer": first_fail,
        "failure_reasons": failure_reasons,
    }


def build_data_lineage_audit() -> dict[str, Any]:
    truth_doc = _load_json(SOURCE_TRUTH_PATH)
    confluence_doc = _load_json(CONFLUENCE_PATH)
    thesis_doc = _load_json(THESIS_PATH)

    report_date = str(truth_doc.get("latest_report_date") or confluence_doc.get("latest_cot_report_date") or "")[:10]
    if not report_date:
        raise RuntimeError("Cannot determine latest COT report date for lineage audit.")

    instruments: dict[str, Any] = {}
    counts = {"PASS": 0, "FAIL": 0}
    failed: list[dict[str, Any]] = []

    for iid in cot_mapped_ids():
        row = audit_instrument_lineage(
            iid,
            truth_doc=truth_doc,
            confluence_doc=confluence_doc,
            thesis_doc=thesis_doc,
            report_date=report_date,
        )
        instruments[iid] = row
        counts[row["overall_status"]] += 1
        if row["overall_status"] == "FAIL":
            failed.append(
                {
                    "instrument": iid,
                    "first_divergence_layer": row.get("first_divergence_layer"),
                    "failure_reasons": row.get("failure_reasons"),
                }
            )

    return {
        "version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": AUDIT_PARSER,
        "latest_report_date": report_date,
        "chain": [
            "Instrument",
            "Raw Source (official CFTC deacot)",
            "Mapping (canonical code)",
            "Source Truth Audit",
            "Dashboard (confluence_history_latest)",
            "Scanner (same confluence row)",
            "Thesis (thesis_tracker_latest snapshots)",
            "Scoring (cot_score on confluence row)",
        ],
        "input_files": {
            "source_truth": _file_meta(SOURCE_TRUTH_PATH),
            "confluence": _file_meta(CONFLUENCE_PATH),
            "thesis": _file_meta(THESIS_PATH),
            "legacy_latest": _file_meta(LEGACY_LATEST_PATH),
        },
        "summary": {
            "total_instruments_checked": len(cot_mapped_ids()),
            "pass_count": counts["PASS"],
            "fail_count": counts["FAIL"],
            "all_layers_identical": counts["FAIL"] == 0,
            "failed_instruments": failed,
        },
        "instruments": instruments,
    }


def write_data_lineage_exports(
    payload: dict[str, Any],
    *,
    skip_deliverable: bool = False,
) -> dict[str, Path]:
    for path in (DATA_OUT, PUBLIC_OUT):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    if skip_deliverable:
        return {"lineage": DATA_OUT, "public": PUBLIC_OUT}

    s = payload["summary"]
    lines = [
        "# COT Data Lineage Deliverable",
        "",
        f"Generated: {payload.get('generated_at')}",
        f"Report date: {payload.get('latest_report_date')}",
        "",
        f"- Checked: {s.get('total_instruments_checked')}",
        f"- PASS (all layers identical): {s.get('pass_count')}",
        f"- FAIL: {s.get('fail_count')}",
        "",
    ]
    if s.get("failed_instruments"):
        lines.append("## FAIL — first divergence layer")
        for f in s["failed_instruments"]:
            lines.append(f"### {f['instrument']}")
            lines.append(f"- First divergence: **{f.get('first_divergence_layer')}**")
            for r in f.get("failure_reasons") or []:
                lines.append(f"  - {r}")
            inst = payload["instruments"][f["instrument"]]
            layers = inst["layers"]
            lines.append(
                f"- Source Truth NC: {layers['source_truth']['values'].get('nc_long')} / "
                f"{layers['source_truth']['values'].get('nc_short')}"
            )
            lines.append(
                f"- Dashboard NC: {layers['dashboard']['values'].get('nc_long')} / "
                f"{layers['dashboard']['values'].get('nc_short')}"
            )
            lines.append(
                f"- Thesis NC: {layers['thesis']['values'].get('nc_long')} / "
                f"{layers['thesis']['values'].get('nc_short')}"
            )
            lines.append("")
    lines.append("## Remediation")
    lines.append("- Dashboard≠Source Truth: `python -m hptl.confluence.build_decision_table`")
    lines.append("- Thesis stale: `python -m hptl.thesis_tracker.run_thesis_seed --reset`")
    DELIVERABLE_MD.parent.mkdir(parents=True, exist_ok=True)
    DELIVERABLE_MD.write_text("\n".join(lines), encoding="utf-8")

    return {"lineage": DATA_OUT, "public": PUBLIC_OUT, "deliverable": DELIVERABLE_MD}
