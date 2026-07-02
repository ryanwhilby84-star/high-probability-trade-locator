"""Per-instrument data pipeline freshness audit for workstation exports."""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.confluence.build_decision_table import TARGET_MARKETS
from hptl.cot.report_dates import get_latest_local_report_date

PUBLIC_DATA = PROJECT_ROOT / "web-dashboard" / "public" / "data"
MASTER_CSV = PROCESSED_DIR / "cot_tracked_master_normalized.csv"


@dataclass
class InstrumentFreshnessRow:
    instrument: str
    cot_latest: str = "—"
    ohlc_latest: str = "—"
    valuation_status: str = "—"
    seasonality_status: str = "—"
    scanner_status: str = "—"
    workstation_status: str = "—"
    passed: bool = False
    reason: str = ""

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PipelineFreshnessReport:
    checked_at: str = ""
    master_cot_latest: str = "—"
    confluence_latest: str = "—"
    export_files: dict[str, str] = field(default_factory=dict)
    instruments: list[InstrumentFreshnessRow] = field(default_factory=list)
    summary: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "checked_at": self.checked_at,
            "master_cot_latest": self.master_cot_latest,
            "confluence_latest": self.confluence_latest,
            "export_files": self.export_files,
            "summary": self.summary,
            "instruments": [r.as_dict() for r in self.instruments],
        }


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _resolve_cot_block(doc: dict[str, Any], market: str) -> dict[str, Any] | None:
    markets = doc.get("markets") or {}
    if market in markets:
        return markets[market]
    norm = lambda s: str(s or "").lower().replace("/", " ").strip()
    target = norm(market)
    for key, block in markets.items():
        if norm(key) == target:
            return block
    return None


def _master_max_date() -> str:
    if not MASTER_CSV.exists():
        ts = get_latest_local_report_date()
        if ts is not None and not pd.isna(ts):
            return str(ts)[:10]
        return "—"
    df = pd.read_csv(MASTER_CSV, usecols=["cot_report_date"], low_memory=False)
    return str(df["cot_report_date"].astype(str).str[:10].max())


def _cot_mapped_instruments(registry_doc: dict[str, Any]) -> list[str]:
    markets = registry_doc.get("markets") or []
    if markets:
        return [m["id"] for m in markets if m.get("has_cot_mapping")]
    return list(TARGET_MARKETS)


def _export_file_status() -> dict[str, str]:
    names = [
        "cot_3y_series_latest.json",
        "workstation_ohlc_latest.json",
        "instrument_valuation_history_latest.json",
        "valuation_latest.json",
        "currency_futures_ive_latest.json",
        "fx_valuation_v3_latest.json",
        "seasonality_latest.json",
        "scanner_latest.json",
        "confluence_history_latest.json",
        "instrument_registry.json",
        "prices_latest.json",
    ]
    out: dict[str, str] = {}
    for name in names:
        path = PUBLIC_DATA / name
        if not path.exists():
            out[name] = "missing"
            continue
        doc = _read_json(path)
        gen = str(doc.get("generated_at") or doc.get("latest_cot_report_date") or "")[:19]
        out[name] = f"ok ({gen or 'no timestamp'})"
    return out


def _confluence_row_for(doc: dict[str, Any], market: str) -> dict[str, Any] | None:
    by_market = doc.get("latest_cot_report_date_by_market") or {}
    if market in by_market:
        return {"market": market, "cot_report_date": by_market[market]}
    rows = doc.get("rows") or doc.get("instruments") or []
    for row in rows:
        if row.get("market") == market:
            return row
    history = doc.get("history") or {}
    weeks = history.get("weeks") or []
    if weeks:
        latest = weeks[-1]
        for row in latest.get("rows") or []:
            if row.get("market") == market:
                return row
    return None


def build_pipeline_freshness_report(*, include_non_cot: bool = False) -> PipelineFreshnessReport:
    cot_doc = _read_json(PUBLIC_DATA / "cot_3y_series_latest.json")
    ohlc_doc = _read_json(PUBLIC_DATA / "workstation_ohlc_latest.json")
    val_doc = _read_json(PUBLIC_DATA / "valuation_latest.json")
    sea_doc = _read_json(PUBLIC_DATA / "seasonality_latest.json")
    conf_doc = _read_json(PUBLIC_DATA / "confluence_history_latest.json")
    reg_doc = _read_json(PUBLIC_DATA / "instrument_registry.json")

    master_latest = _master_max_date()
    conf_latest = str(conf_doc.get("latest_cot_report_date") or "—")[:10]
    instruments = _cot_mapped_instruments(reg_doc)
    if include_non_cot:
        for m in TARGET_MARKETS:
            if m not in instruments:
                instruments.append(m)

    rows: list[InstrumentFreshnessRow] = []
    passed = failed = warned = 0

    for market in instruments:
        row = InstrumentFreshnessRow(instrument=market)
        reasons: list[str] = []

        cot_block = _resolve_cot_block(cot_doc, market)
        if not cot_block:
            reasons.append("missing_cot_series")
            row.cot_latest = "—"
        else:
            row.cot_latest = str(cot_block.get("latest_date") or "—")[:10]
            series = cot_block.get("series") or cot_block.get("rows") or []
            if not series:
                reasons.append("empty_cot_series")
            elif master_latest != "—" and row.cot_latest != "—" and row.cot_latest < master_latest:
                reasons.append(f"cot_stale({row.cot_latest}<{master_latest})")

        ohlc_block = (ohlc_doc.get("instruments") or {}).get(market)
        if not ohlc_block:
            row.ohlc_latest = "—"
            if market in _cot_mapped_instruments(reg_doc):
                reasons.append("missing_ohlc_block")
        else:
            row.ohlc_latest = str(ohlc_block.get("ohlc_last_date") or "—")[:10]
            weekly = ohlc_block.get("weekly_ohlc") or []
            if not weekly:
                reasons.append("empty_ohlc")
            elif row.cot_latest != "—" and row.ohlc_latest != "—" and row.ohlc_latest < row.cot_latest:
                reasons.append(f"ohlc_behind_cot({row.ohlc_latest}<{row.cot_latest})")

        val_block = (val_doc.get("instruments") or {}).get(market) or {}
        if val_block.get("wired"):
            row.valuation_status = "wired"
        elif val_block:
            row.valuation_status = str(val_block.get("model_status") or val_block.get("valuation_state") or "unwired")
        else:
            row.valuation_status = "missing"

        sea_block = (sea_doc.get("instruments") or {}).get(market) or {}
        if sea_block.get("wired"):
            row.seasonality_status = "wired"
        elif sea_block:
            row.seasonality_status = str(sea_block.get("status") or "unwired")
        else:
            row.seasonality_status = "missing"

        conf_row = _confluence_row_for(conf_doc, market)
        if conf_row:
            conf_date = str(conf_row.get("cot_report_date") or conf_latest)[:10]
            row.scanner_status = f"present({conf_date})"
            if master_latest != "—" and conf_date != "—" and conf_date < master_latest:
                reasons.append(f"scanner_stale({conf_date}<{master_latest})")
        elif conf_latest == "—":
            row.scanner_status = "no_confluence_export"
            reasons.append("confluence_export_missing")
        else:
            row.scanner_status = "missing_row"
            reasons.append("missing_scanner_row")

        ws_issues: list[str] = []
        if not cot_block:
            ws_issues.append("no_cot")
        if not ohlc_block:
            ws_issues.append("no_ohlc")
        elif not (ohlc_block.get("weekly_ohlc") or []):
            ws_issues.append("empty_ohlc")
        meta = next((m for m in (reg_doc.get("markets") or []) if m.get("id") == market), None)
        if meta and meta.get("has_cot_mapping") and not cot_block:
            ws_issues.append("registry_cot_unresolved")
        row.workstation_status = "ok" if not ws_issues else ",".join(ws_issues)
        if ws_issues and "empty_ohlc" not in ws_issues:
            reasons.extend(ws_issues)

        critical = [
            r
            for r in reasons
            if not r.startswith("ohlc_behind_cot") or (
                row.ohlc_latest != "—"
                and row.cot_latest != "—"
                and (pd.Timestamp(row.cot_latest) - pd.Timestamp(row.ohlc_latest)).days > 14
            )
        ]
        row.passed = len(critical) == 0
        row.reason = "; ".join(reasons) if reasons else "ok"
        if row.passed:
            passed += 1
        elif any(
            r.startswith(("missing_cot", "empty_cot", "cot_stale", "missing_ohlc", "confluence", "scanner_stale"))
            for r in critical
        ):
            failed += 1
        else:
            warned += 1
        rows.append(row)

    return PipelineFreshnessReport(
        checked_at=datetime.now(timezone.utc).isoformat(),
        master_cot_latest=master_latest,
        confluence_latest=conf_latest,
        export_files=_export_file_status(),
        instruments=rows,
        summary={"passed": passed, "failed": failed, "warned": warned, "total": len(rows)},
    )


def print_freshness_report(report: PipelineFreshnessReport, *, show_passing: bool = False) -> int:
    print(f"\n{'=' * 88}")
    print("HPTL DATA PIPELINE FRESHNESS")
    print(f"{'=' * 88}")
    print(f"Checked: {report.checked_at}")
    print(f"Master COT latest: {report.master_cot_latest}")
    print(f"Confluence latest: {report.confluence_latest}")
    print("\nExport files:")
    for name, status in report.export_files.items():
        print(f"  {name}: {status}")

    print(f"\nSummary: {report.summary['passed']} pass / {report.summary['failed']} fail / "
          f"{report.summary['warned']} warn / {report.summary['total']} total")

    print(f"\n{'Instrument':<28} {'COT':<12} {'OHLC':<12} {'Val':<10} {'Sea':<10} {'Scanner':<12} {'WS':<12} {'Status'}")
    print("-" * 110)
    for row in report.instruments:
        if row.passed and not show_passing:
            continue
        status = "PASS" if row.passed else "FAIL"
        print(
            f"{row.instrument:<28} {row.cot_latest:<12} {row.ohlc_latest:<12} "
            f"{row.valuation_status:<10} {row.seasonality_status:<10} {row.scanner_status:<12} "
            f"{row.workstation_status:<12} {status} {row.reason}"
        )

    stale = [r for r in report.instruments if not r.passed]
    if stale:
        print(f"\nStale/missing ({len(stale)}):")
        for r in stale:
            print(f"  - {r.instrument}: {r.reason}")

    return 0 if report.summary["failed"] == 0 else 1
