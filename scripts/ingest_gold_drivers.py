#!/usr/bin/env python3
"""GOLD-FIX-B — ingest Gold ETF (automated) + CB purchases (manual CSV bridge)."""

from __future__ import annotations

import json
import math
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from hptl.config import PROJECT_ROOT
from hptl.data_sources.metals_driver_ingest import ingest_gold_etf_holdings
from hptl.valuation.export import PUBLIC_OUT, _sanitize_withheld_export_block
from hptl.valuation.ive_adapter import attach_ive_to_export_block
from hptl.valuation.metals_institutional_drivers import build_driver_bundle
from hptl.valuation.metals_institutional_fair_value_v1 import compute_metals_institutional_valuation
from hptl.valuation.export import METALS_PILLAR_ENGINE

MANUAL_CB_CSV = PROJECT_ROOT / "data" / "manual" / "metals" / "gold_cb_purchases.csv"
ETF_CACHE = PROJECT_ROOT / "data" / "cache" / "metals_drivers" / "gold_etf_holdings.json"
CB_CACHE = PROJECT_ROOT / "data" / "cache" / "metals_drivers" / "wgc_cb_gold_net_purchases.json"
MIN_OBS = 52
MAX_STALE_DAYS = 45


@dataclass
class DriverRow:
    driver: str
    status: str
    latest_date: str
    obs_count: str
    source: str
    cache_written: str


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _infer_frequency(dates: list[str]) -> str:
    if len(dates) < 2:
        return "unknown"
    gaps = []
    for a, b in zip(dates[:-1], dates[1:]):
        try:
            gaps.append(
                (
                    datetime.strptime(b[:10], "%Y-%m-%d")
                    - datetime.strptime(a[:10], "%Y-%m-%d")
                ).days
            )
        except ValueError:
            continue
    if not gaps:
        return "unknown"
    med = sorted(gaps)[len(gaps) // 2]
    if med <= 7:
        return "daily"
    if med <= 35:
        return "monthly"
    if med <= 100:
        return "quarterly"
    return "annual"


def _validate_observations(obs: list[dict[str, Any]]) -> tuple[bool, str | None]:
    if not obs:
        return False, "no observations parsed"
    if len(obs) < MIN_OBS:
        return False, f"insufficient observations ({len(obs)} < {MIN_OBS})"
    for row in obs:
        if not row.get("date") or row.get("value") is None:
            return False, "empty date or value present"
        try:
            float(row["value"])
        except (TypeError, ValueError):
            return False, "non-numeric value present"
    latest = obs[-1]["date"]
    try:
        delta = (
            datetime.now(timezone.utc).date()
            - datetime.strptime(str(latest)[:10], "%Y-%m-%d").date()
        ).days
    except ValueError:
        return False, f"invalid latest date {latest}"
    if delta > MAX_STALE_DAYS:
        return False, f"latest date {latest} stale ({delta} days old)"
    return True, None


def _write_cb_cache(manual_path: Path, observations: list[dict[str, Any]]) -> None:
    rel = str(manual_path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    dates = [o["date"] for o in observations]
    payload = {
        "driver_id": "cb_net_purchases",
        "source_name": "Manual WGC export",
        "source_file": rel,
        "source_url": None,
        "refresh_date": _now_iso(),
        "frequency": _infer_frequency(dates),
        "unit": "tonnes",
        "notes": (
            "Central bank net gold purchases from manual WGC "
            "'Changes in World Official Gold Reserves' export."
        ),
        "observation_count": len(observations),
        "latest_date": dates[-1],
        "observations": observations,
    }
    CB_CACHE.parent.mkdir(parents=True, exist_ok=True)
    CB_CACHE.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")


def _load_manual_cb_csv(path: Path) -> list[dict[str, Any]]:
    df = pd.read_csv(path)
    cols = {str(c).strip().lower(): c for c in df.columns}
    if "date" not in cols or "value" not in cols:
        raise ValueError("required columns missing: date,value")
    obs: list[dict[str, Any]] = []
    for _, row in df.iterrows():
        dt = pd.to_datetime(row[cols["date"]], errors="coerce")
        if pd.isna(dt):
            continue
        val = pd.to_numeric(row[cols["value"]], errors="coerce")
        if pd.isna(val) or not math.isfinite(float(val)):
            continue
        obs.append({"date": dt.strftime("%Y-%m-%d"), "value": float(val)})
    obs.sort(key=lambda x: x["date"])
    return obs


def ingest_etf_driver() -> DriverRow:
    result = ingest_gold_etf_holdings()
    if result.status != "ok":
        return DriverRow(
            "etf_holdings_or_flows",
            "Blocked",
            "—",
            str(result.observation_count or 0),
            result.source_name,
            "No",
        )
    # Normalise cache metadata fields for dashboard lineage.
    if ETF_CACHE.exists():
        doc = json.loads(ETF_CACHE.read_text(encoding="utf-8"))
        doc["source_file"] = None
        doc["source_url"] = (
            "https://www.ssga.com/library-content/products/fund-data/etfs/us/navhist-us-en-gld.xlsx"
        )
        doc["refresh_date"] = _now_iso()
        dates = [o["date"] for o in doc.get("observations", [])]
        doc["frequency"] = _infer_frequency(dates)
        ETF_CACHE.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    return DriverRow(
        "etf_holdings_or_flows",
        "Present",
        result.latest_date or "—",
        str(result.observation_count),
        f"{result.source_name} ({result.source_id})",
        "Yes",
    )


def ingest_cb_driver() -> tuple[DriverRow | None, str | None]:
    if not MANUAL_CB_CSV.is_file():
        return None, "data/manual/metals/gold_cb_purchases.csv"
    try:
        obs = _load_manual_cb_csv(MANUAL_CB_CSV)
        ok, reason = _validate_observations(obs)
        if not ok:
            return (
                DriverRow(
                    "central_bank_net_purchases",
                    "Blocked",
                    obs[-1]["date"] if obs else "—",
                    str(len(obs)),
                    "Manual WGC export",
                    "No",
                ),
                reason,
            )
        _write_cb_cache(MANUAL_CB_CSV, obs)
        return (
            DriverRow(
                "central_bank_net_purchases",
                "Present",
                obs[-1]["date"],
                str(len(obs)),
                f"Manual WGC export ({MANUAL_CB_CSV.name})",
                "Yes",
            ),
            None,
        )
    except Exception as exc:
        return (
            DriverRow(
                "central_bank_net_purchases",
                "Error",
                "—",
                "0",
                "Manual WGC export",
                "No",
            ),
            str(exc),
        )


def _fred_driver_row(bundle) -> DriverRow:
    lin = bundle.lineage.get("real_yield", {})
    return DriverRow(
        "real_yield_10y",
        "Present" if "real_yield" in bundle.features else "Missing",
        lin.get("source_date", "—"),
        str(bundle.n) if bundle.n else "—",
        f"FRED {lin.get('source_id', 'DFII10')}",
        "n/a",
    )


def _dxy_driver_row(bundle) -> DriverRow:
    lin = bundle.lineage.get("log_dxy", {})
    return DriverRow(
        "dxy_broad",
        "Present" if "log_dxy" in bundle.features else "Missing",
        lin.get("source_date", "—"),
        str(bundle.n) if bundle.n else "—",
        f"FRED {lin.get('source_id', 'DTWEXBGS')}",
        "n/a",
    )


def export_gold_only() -> dict[str, Any]:
    generated_at = datetime.now(timezone.utc).isoformat()
    block = compute_metals_institutional_valuation(market="Gold")
    block["valuation_pillar"] = METALS_PILLAR_ENGINE
    enriched = attach_ive_to_export_block(dict(block), "Gold", generated_at=generated_at)
    gold_block = _sanitize_withheld_export_block(enriched)

    path = ROOT / PUBLIC_OUT if not PUBLIC_OUT.is_absolute() else PUBLIC_OUT
    doc = {}
    if path.exists():
        doc = json.loads(path.read_text(encoding="utf-8"))
    instruments = dict(doc.get("instruments") or {})
    instruments["Gold"] = gold_block
    doc["instruments"] = instruments
    doc["generated_at"] = generated_at
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(doc, indent=2, ensure_ascii=False), encoding="utf-8")
    print(f"Wrote {path}")
    return gold_block


def _print_table(rows: list[DriverRow], gold: dict[str, Any] | None) -> None:
    print("| Driver | Status | Latest date | Obs count | Source | Cache written? |")
    print("| --- | --- | --- | ---: | --- | --- |")
    for r in rows:
        print(
            f"| {r.driver} | {r.status} | {r.latest_date} | {r.obs_count} | {r.source} | {r.cache_written} |"
        )
    print()
    if gold:
        dev = gold.get("deviation_pct")
        dev_s = f"{dev:+.2f}%" if dev is not None else "—"
        status = gold.get("model_status") or "—"
        publish = "Yes" if gold.get("publish") else "No"
        blocker = gold.get("blocker_reason") or "—"
        print(f"Gold valuation %: {dev_s}")
        print(f"Gold status: {status}")
        print(f"Publish?: {publish}")
        print(f"Blocker if no: {blocker}")


def main() -> int:
    etf_row = ingest_etf_driver()
    cb_row, cb_error = ingest_cb_driver()

    if cb_row is None:
        bundle = build_driver_bundle("Gold")
        rows = [_fred_driver_row(bundle), _dxy_driver_row(bundle), etf_row]
        rows.append(
            DriverRow(
                "central_bank_net_purchases",
                "Missing",
                "—",
                "—",
                "Manual WGC export",
                "No",
            )
        )
        print()
        print("GOLD WITHHELD — add:")
        print("data/manual/metals/gold_cb_purchases.csv")
        print()
        _print_table(rows, None)
        return 1

    if cb_error:
        bundle = build_driver_bundle("Gold")
        rows = [_fred_driver_row(bundle), _dxy_driver_row(bundle), etf_row, cb_row]
        print()
        print(f"GOLD WITHHELD — {cb_error}")
        print()
        _print_table(rows, None)
        return 1

    if not ETF_CACHE.is_file() or not CB_CACHE.is_file():
        print("GOLD WITHHELD — driver caches incomplete")
        return 1

    gold = export_gold_only()
    bundle = build_driver_bundle("Gold")
    rows = [_fred_driver_row(bundle), _dxy_driver_row(bundle), etf_row, cb_row]
    _print_table(rows, gold)
    return 0 if gold.get("publish") else 1


if __name__ == "__main__":
    raise SystemExit(main())
