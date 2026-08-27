"""Ingest USDA PSD balance sheets for agriculture valuation."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

from hptl.config import PROJECT_ROOT
from hptl.valuation.agri_fundamental_valuation import BALANCE_SHEET_DIR, PRIORITY_MARKETS, discover_instrument_data
from hptl.valuation.usda_psd_client import UsdaPsdError, fetch_psd_commodity_xml, parse_psd_rows

CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "usda_psd_commodity_map.json"
INGEST_AUDIT_MD = Path("data/audits/agri_balance_sheet_ingest.md")
MIN_OBS_RECOMMENDED = 12

SOURCE_LABEL = "USDA WASDE/PSD"


def _load_map() -> dict[str, Any]:
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))


def _finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _marketing_year_label(market_year: str) -> str:
    my = str(market_year).strip()
    if not my.isdigit():
        return my
    n = int(my)
    return f"{n}/{str(n + 1)[-2:]}"


def _release_date(calendar_year: str, month: str) -> str:
    cy = str(calendar_year).strip()
    mo = str(month).strip().zfill(2)
    return f"{cy}-{mo}-01"


def _attr_ids(cfg: dict[str, Any]) -> dict[str, str]:
    return {str(k): str(v) for k, v in (cfg.get("attribute_ids") or {}).items()}


def _pivot_rows(rows: list[dict[str, Any]]) -> dict[tuple[str, str, str], dict[str, float]]:
    grouped: dict[tuple[str, str, str], dict[str, float]] = {}
    for row in rows:
        key = (row["calendar_year"], row["month"], row["market_year"])
        aid = row["attribute_id"]
        val = _finite(row.get("value"))
        if val is None:
            continue
        grouped.setdefault(key, {})[aid] = val
    return grouped


def _total_use(values: dict[str, float], inst_cfg: dict[str, Any], attrs: dict[str, str]) -> float | None:
    mode = inst_cfg.get("total_use_mode") or "domestic_plus_exports"
    ending = values.get(attrs["ending_stocks"])
    supply = values.get(attrs["total_supply"])

    if mode == "direct":
        direct_key = inst_cfg.get("total_use_attr") or "total_use_direct"
        direct_id = attrs.get(direct_key.replace("_direct", "_direct")) or attrs["total_use_direct"]
        direct = values.get(direct_id)
        if direct is not None and direct > 0:
            return direct

    if supply is not None and ending is not None and supply > ending:
        return supply - ending

    dom = values.get(attrs["total_domestic_consumption"])
    exp_key = inst_cfg.get("exports_attr") or "ty_exports"
    exp_id = attrs.get(exp_key) or attrs.get("ty_exports") or attrs.get("exports") or ""
    exports = values.get(exp_id) if exp_id else None
    if dom is not None and exports is not None and (dom + exports) > 0:
        return dom + exports
    return None


def build_balance_sheet_series(
    market: str,
    *,
    force_refresh: bool = False,
) -> dict[str, Any]:
    """Fetch PSD data and build normalized balance-sheet series for one market."""
    cfg = _load_map()
    inst = (cfg.get("instruments") or {}).get(market)
    if not inst:
        raise KeyError(f"No USDA PSD mapping for {market}")

    code = str(inst["psd_code"])
    country = str(inst.get("country") or "US")
    attrs = _attr_ids(cfg)

    try:
        xml_bytes = fetch_psd_commodity_xml(code, force_refresh=force_refresh)
        rows = parse_psd_rows(xml_bytes, country_code=country)
    except (UsdaPsdError, requests.RequestException) as exc:
        return {
            "market": market,
            "source": SOURCE_LABEL,
            "source_detail": f"USDA FAS PSD SOAP commodity {code}",
            "ingest_status": "failed",
            "error": str(exc),
            "series": [],
        }

    grouped = _pivot_rows(rows)
    series: list[dict[str, Any]] = []
    for (cy, mo, my), values in sorted(grouped.items()):
        ending = values.get(attrs["ending_stocks"])
        production = values.get(attrs["production"])
        total_use = _total_use(values, inst, attrs)
        stu: float | None = None
        if ending is not None and total_use is not None and total_use > 0:
            stu = ending / total_use
        series.append(
            {
                "date": _release_date(cy, mo),
                "marketing_year": _marketing_year_label(my),
                "production": production,
                "ending_stocks": ending,
                "total_use": total_use,
                "stocks_to_use": round(stu, 6) if stu is not None else None,
            }
        )

    series.sort(key=lambda r: (r["date"], r["marketing_year"]))
    return {
        "market": market,
        "source": SOURCE_LABEL,
        "source_detail": f"USDA FAS PSD SOAP getDatabyCommodity ({code}, country={country})",
        "psd_commodity_code": code,
        "ingest_status": "ok" if series else "empty",
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "series": series,
    }


def validate_balance_sheet(doc: dict[str, Any]) -> dict[str, Any]:
    """Validate ingested balance sheet file."""
    series = doc.get("series") or []
    stu_rows = [r for r in series if r.get("stocks_to_use") is not None]
    dates = [r.get("date") for r in stu_rows if r.get("date")]
    fields = ("production", "ending_stocks", "total_use", "stocks_to_use")
    missing_counts = {f: sum(1 for r in series if r.get(f) is None) for f in fields}
    total = len(series) or 1
    missing_rate = {f: round(missing_counts[f] / total, 4) for f in fields}

    market = str(doc.get("market") or "")
    inv = discover_instrument_data(market) if market else {}
    aligned = inv.get("aligned_price_stu_pairs") or 0

    return {
        "market": market,
        "source": doc.get("source"),
        "ingest_status": doc.get("ingest_status"),
        "observation_count": len(series),
        "stu_observation_count": len(stu_rows),
        "date_start": min(dates) if dates else None,
        "date_end": max(dates) if dates else None,
        "latest_observation_date": max(dates) if dates else None,
        "missing_value_rate": missing_rate,
        "meets_minimum_observations": len(stu_rows) >= MIN_OBS_RECOMMENDED,
        "aligned_price_stu_pairs": aligned,
        "valuation_ready": aligned >= MIN_OBS_RECOMMENDED,
        "error": doc.get("error"),
    }


def write_balance_sheet(market: str, doc: dict[str, Any]) -> Path:
    out = BALANCE_SHEET_DIR / f"{market}.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return out


def ingest_priority_balance_sheets(*, force_refresh: bool = False) -> dict[str, Any]:
    """Ingest all priority markets and write processed JSON files."""
    results: dict[str, Any] = {}
    written: list[str] = []
    failed: list[str] = []

    for market in PRIORITY_MARKETS:
        doc = build_balance_sheet_series(market, force_refresh=force_refresh)
        if doc.get("ingest_status") == "ok" and doc.get("series"):
            write_balance_sheet(market, doc)
            written.append(market)
            validation = validate_balance_sheet(doc)
        else:
            failed.append(market)
            validation = validate_balance_sheet(doc)
        results[market] = {**validation, "path": str(BALANCE_SHEET_DIR / f"{market}.json")}

    return {
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "source": SOURCE_LABEL,
        "source_adapter": "usda_psd_soap",
        "config": str(CONFIG_PATH.relative_to(PROJECT_ROOT)),
        "raw_cache_dir": str(_load_map().get("raw_cache_dir") or "data/raw/usda_psd"),
        "markets_written": written,
        "markets_failed": failed,
        "instruments": results,
    }


def render_ingest_audit_md(report: dict[str, Any], *, valuation_payload: dict[str, Any] | None = None) -> str:
    lines = [
        "# Agriculture Balance Sheet Ingest Audit",
        "",
        f"- Generated: `{report.get('generated_at')}`",
        f"- Source: **{report.get('source')}**",
        f"- Adapter: `{report.get('source_adapter')}`",
        f"- Config: `{report.get('config')}`",
        f"- Raw cache: `{report.get('raw_cache_dir')}`",
        "",
        "## Source found",
        "",
        "USDA FAS **Production, Supply & Distribution (PSD)** via public SOAP endpoint "
        "`getDatabyCommodity` (WASDE-aligned US balance sheets). "
        "No API key required. Raw XML cached under `data/raw/usda_psd/`.",
        "",
        "## Ingest summary",
        "",
        "| Market | Status | Rows | S/U rows | Date range | Aligned w/ price | Valuation ready |",
        "| --- | --- | ---: | ---: | --- | ---: | --- |",
    ]
    for market in PRIORITY_MARKETS:
        row = (report.get("instruments") or {}).get(market) or {}
        dr = "—"
        if row.get("date_start") and row.get("date_end"):
            dr = f"{row['date_start']} → {row['date_end']}"
        lines.append(
            f"| {market} | {row.get('ingest_status', '—')} | {row.get('observation_count', 0)} | "
            f"{row.get('stu_observation_count', 0)} | {dr} | {row.get('aligned_price_stu_pairs', 0)} | "
            f"{'yes' if row.get('valuation_ready') else 'no'} |"
        )

    lines.extend(["", "## Missing value rates", ""])
    for market in PRIORITY_MARKETS:
        row = (report.get("instruments") or {}).get(market) or {}
        rates = row.get("missing_value_rate") or {}
        lines.append(
            f"- **{market}**: ending_stocks {rates.get('ending_stocks', '—')}, "
            f"total_use {rates.get('total_use', '—')}, stocks_to_use {rates.get('stocks_to_use', '—')}"
        )

    if report.get("markets_failed"):
        lines.extend(["", "## Blockers", ""])
        for market in report["markets_failed"]:
            row = (report.get("instruments") or {}).get(market) or {}
            lines.append(f"- **{market}**: {row.get('error') or 'ingest returned no series'}")

    if valuation_payload:
        lines.extend(["", "## Valuation outcome (post-ingest)", ""])
        wired = [
            m
            for m in PRIORITY_MARKETS
            if (valuation_payload.get("instruments") or {}).get(m, {}).get("wired")
        ]
        if wired:
            lines.append("### Markets valued")
            for m in wired:
                v = valuation_payload["instruments"][m]
                lines.append(
                    f"- **{m}**: fair {v.get('fair_value')} · dev {v.get('deviation_pct')}% · "
                    f"{v.get('model_id')} · confidence {v.get('confidence')}"
                )
        else:
            lines.append("- None wired yet.")
        lines.extend(["", "### Markets still unavailable"])
        for m in PRIORITY_MARKETS:
            v = (valuation_payload.get("instruments") or {}).get(m) or {}
            if v.get("wired"):
                continue
            lines.append(f"- **{m}**: {v.get('unavailable_reason') or v.get('valuation_reason') or '—'}")

    lines.append("")
    return "\n".join(lines)
