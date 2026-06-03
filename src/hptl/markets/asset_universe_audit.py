"""Asset universe + data-coverage audit (READ-ONLY).

This module does NOT change scoring, confluence, or the UI. It only inspects what is
already loaded so we can answer, per instrument:

    * name / asset class / canonical asset id
    * price source available?  (OANDA symbol configured)  + whether price *data* is ingested
    * macro data available?    (FRED-backed relationship map)
    * COT data available?      (direct / leg-derived / proxy, from the COT coverage audit)
    * currently displayed on radar?  (present in the latest confluence week)

It also flags DERIVED / DUPLICATE / NO DATA / ORPHANED instruments, defines a canonical
"primary asset" universe, and reports missing data coverage for those primaries.

Outputs:
    data/asset_universe_audit.json   (machine-readable)
    data/asset_universe_audit.md     (human-readable report)
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from hptl.macro.macro_relationship_maps import MACRO_RELATIONSHIP_MARKETS
from hptl.markets.instrument_registry import (
    all_instrument_ids,
    canonical_priority_group,
    load_registry,
)

COT_AUDIT_PATH = Path("data/cot_coverage_audit_latest.json")
CONFLUENCE_LATEST_PATH = Path("web-dashboard/public/data/confluence_history_latest.json")
OUT_JSON = Path("data/asset_universe_audit.json")
OUT_MD = Path("data/asset_universe_audit.md")

# Classification buckets.
PRIMARY = "PRIMARY"
DERIVED = "DERIVED"
DUPLICATE = "DUPLICATE"
MACRO_ONLY = "MACRO_ONLY"
NO_DATA = "NO_DATA"
ORPHANED = "ORPHANED"

# --- Canonical "primary asset" universe (the only assets we want to surface) ---
# Maps the requested canonical asset -> the registry instrument id that represents it
# (or None when no instrument currently represents it).
CANONICAL_UNIVERSE: dict[str, list[tuple[str, str | None]]] = {
    "Currencies": [
        ("USD", None),  # numeraire — no single registry instrument (synthesized leg)
        ("EUR", "Euro FX / 6E"),
        ("GBP", "British Pound / 6B"),
        ("JPY", "Japanese Yen / 6J"),
        ("CHF", "Swiss Franc / 6S"),
        ("AUD", "Australian Dollar / 6A"),
        ("NZD", "NZ Dollar / 6N"),
        ("CAD", "Canadian Dollar / 6C"),
    ],
    "Metals": [
        ("Gold", "Gold"),
        ("Silver", "Silver"),
        ("Copper", "Copper / HG"),
        ("Platinum", "Platinum"),
    ],
    "Energy": [
        ("WTI", "Crude Oil / CL"),
        ("Brent", "Brent Crude Oil"),
        ("Natural Gas", "Natural Gas / NG"),
    ],
    "Agriculture": [
        ("Wheat", "Wheat"),
        ("Corn", "Corn"),
        ("Soybeans", "Soybeans"),
        ("Coffee", "Coffee"),
        ("Cocoa", "Cocoa"),
    ],
    "Rates": [
        ("US 2Y", "US 2Y T-Note"),
        ("US 10Y", "US 10Y T-Note"),
        ("US 30Y", "US T-Bond"),
        ("Bund", "Bund"),
        ("Gilt", "UK 10Y Gilt"),
    ],
    "Indices": [
        ("SPX", "S&P 500 / ES"),
        ("NDX", "NASDAQ / NQ"),
        ("Dow", "Dow / YM"),
        ("Russell", "US Russ 2000"),
        ("DAX", "Germany 30"),
        ("FTSE", "UK 100"),
        ("Nikkei", "Japan 225"),
    ],
}


def _load_cot_audit() -> dict[str, dict[str, Any]]:
    if not COT_AUDIT_PATH.exists():
        return {}
    data = json.loads(COT_AUDIT_PATH.read_text(encoding="utf-8"))
    return {x["instrument_id"]: x for x in data.get("instruments", [])}


def _load_radar_markets() -> set[str]:
    """Instruments present in the latest confluence week == displayed on the radar."""
    if not CONFLUENCE_LATEST_PATH.exists():
        return set()
    data = json.loads(CONFLUENCE_LATEST_PATH.read_text(encoding="utf-8"))
    records = data.get("records", [])
    latest = data.get("latest_cot_report_date")
    # The radar shows the latest *calendar* week; use the max 'date' present.
    weeks = {str(r.get("date") or "") for r in records}
    latest_week = max(weeks) if weeks else ""
    on_radar = {str(r.get("market") or "") for r in records if str(r.get("date") or "") == latest_week}
    on_radar.discard("")
    _ = latest  # latest_cot_report_date kept for reference only
    return on_radar


def _classify(
    *,
    cot_status: str,
    duplicate_of: str | None,
    has_macro: bool,
    subgroup: str,
    in_canonical: bool,
) -> str:
    if duplicate_of or cot_status == "proxy_cot":
        return DUPLICATE
    if cot_status == "leg_derived_cot" or subgroup.endswith("_cross"):
        return DERIVED
    if cot_status == "direct_cot":
        return PRIMARY
    # No COT below this point.
    if has_macro:
        return MACRO_ONLY if not in_canonical else PRIMARY
    # No COT and no FRED macro relationship.
    return ORPHANED if not in_canonical else NO_DATA


def build_audit() -> dict[str, Any]:
    reg = load_registry()
    cot_audit = _load_cot_audit()
    on_radar = _load_radar_markets()
    macro_set = set(MACRO_RELATIONSHIP_MARKETS)

    canonical_ids = {rid for entries in CANONICAL_UNIVERSE.values() for _, rid in entries if rid}

    instruments: list[dict[str, Any]] = []
    for iid in all_instrument_ids(tradeable_only=False):
        spec = reg[iid]
        cca = cot_audit.get(iid, {})
        cot_status = str(cca.get("cot_status") or "no_cot_available")
        dq = str(cca.get("data_quality_status") or "missing")
        duplicate_of = cca.get("duplicate_of")
        canonical = canonical_priority_group(spec, iid)
        has_macro = iid in macro_set
        has_price_symbol = bool(spec.oanda_symbol)
        has_cot = cot_status in {"direct_cot", "leg_derived_cot", "proxy_cot"}
        in_canonical = iid in canonical_ids

        classification = _classify(
            cot_status=cot_status,
            duplicate_of=duplicate_of,
            has_macro=has_macro,
            subgroup=spec.subgroup,
            in_canonical=in_canonical,
        )

        instruments.append(
            {
                "instrument_id": iid,
                "display_name": spec.display_name,
                "asset_class": spec.asset_class,
                "subgroup": spec.subgroup,
                "canonical_asset_id": canonical,
                "price_symbol": spec.oanda_symbol,
                "price_source_available": has_price_symbol,
                "macro_data_available": has_macro,
                "macro_driver_profile": spec.macro_driver_profile,
                "cot_data_available": has_cot,
                "cot_status": cot_status,
                "data_quality_status": dq,
                "duplicate_of": duplicate_of or (spec.cot_proxy_of if classification == DUPLICATE else None),
                "displayed_on_radar": iid in on_radar,
                "in_canonical_universe": in_canonical,
                "classification": classification,
            }
        )

    # --- Canonical universe coverage (Task 3 + 4) ---
    by_id = {x["instrument_id"]: x for x in instruments}
    canonical_rows: list[dict[str, Any]] = []
    for asset_class, entries in CANONICAL_UNIVERSE.items():
        for canon_name, rid in entries:
            row: dict[str, Any] = {
                "asset_class": asset_class,
                "canonical_asset": canon_name,
                "registry_instrument": rid,
                "exists_in_registry": bool(rid and rid in by_id),
                "price_source": False,
                "macro_data": False,
                "cot_data": False,
                "cot_status": None,
                "missing": [],
            }
            if rid and rid in by_id:
                inst = by_id[rid]
                row["price_source"] = bool(inst["price_source_available"])
                row["macro_data"] = bool(inst["macro_data_available"])
                row["cot_data"] = bool(inst["cot_data_available"])
                row["cot_status"] = inst["cot_status"]
            missing = []
            # Price *data* is never ingested system-wide (no candle store); treat price
            # "source" as the configured symbol only.
            if not row["price_source"]:
                missing.append("price_symbol")
            if not row["macro_data"]:
                missing.append("fred_macro_map")
            # COT only expected where a real futures market exists (skip rates / foreign indices / USD).
            cot_expected = asset_class in {"Metals", "Energy", "Agriculture"} or (
                asset_class == "Currencies" and canon_name != "USD"
            ) or canon_name in {"SPX", "NDX", "Dow"}
            if cot_expected and not row["cot_data"]:
                missing.append("cot")
            if not row["exists_in_registry"]:
                missing.append("registry_instrument")
            row["cot_expected"] = cot_expected
            row["missing"] = missing
            canonical_rows.append(row)

    # --- Summary tallies ---
    from collections import Counter

    cls_counts = Counter(x["classification"] for x in instruments)
    radar_count = sum(1 for x in instruments if x["displayed_on_radar"])
    price_no_data_total = len(instruments)  # no candle store exists at all

    summary = {
        "instruments_total": len(instruments),
        "displayed_on_radar": radar_count,
        "classification": dict(cls_counts),
        "price_data_store_exists": False,
        "price_symbols_configured": sum(1 for x in instruments if x["price_source_available"]),
        "instruments_without_price_candles": price_no_data_total,
        "fred_macro_maps": len(macro_set),
        "with_any_cot": sum(1 for x in instruments if x["cot_data_available"]),
        "canonical_universe_total": len(canonical_rows),
        "canonical_fully_covered": sum(1 for r in canonical_rows if not r["missing"]),
        "canonical_with_gaps": sum(1 for r in canonical_rows if r["missing"]),
    }

    return {
        "note": (
            "READ-ONLY audit. No price candle/OHLC data is ingested anywhere in the system; "
            "'price_source_available' means an OANDA symbol is configured in the registry only. "
            "Macro = FRED-backed relationship map. COT from cot_coverage_audit_latest.json."
        ),
        "summary": summary,
        "instruments": instruments,
        "canonical_universe": canonical_rows,
    }


def _md_table(headers: list[str], rows: list[list[str]]) -> str:
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    for r in rows:
        out.append("| " + " | ".join(r) + " |")
    return "\n".join(out)


def _yn(v: bool) -> str:
    return "yes" if v else "no"


def write_reports(audit: dict[str, Any]) -> tuple[Path, Path]:
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(audit, indent=2), encoding="utf-8")

    s = audit["summary"]
    lines: list[str] = []
    lines.append("# HPTL Asset Universe & Data Coverage Audit")
    lines.append("")
    lines.append(f"- Instruments in registry: **{s['instruments_total']}**")
    lines.append(f"- Displayed on radar (latest week): **{s['displayed_on_radar']}**")
    lines.append(f"- Price candle/OHLC data store exists: **{_yn(s['price_data_store_exists'])}** "
                 f"(price symbols configured: {s['price_symbols_configured']}, "
                 f"instruments with no price candles: {s['instruments_without_price_candles']})")
    lines.append(f"- FRED-backed macro relationship maps: **{s['fred_macro_maps']}**")
    lines.append(f"- Instruments with any COT (direct/leg/proxy): **{s['with_any_cot']}**")
    lines.append(f"- Classification: {s['classification']}")
    lines.append("")

    # Task 1 inventory
    lines.append("## Task 1 — Full instrument inventory")
    lines.append("")
    headers = ["Instrument", "Asset class", "Canonical id", "Price src", "Macro", "COT", "COT status", "Radar", "Class"]
    rows = []
    for x in sorted(audit["instruments"], key=lambda r: (r["asset_class"], r["instrument_id"])):
        rows.append([
            x["instrument_id"],
            x["asset_class"],
            x["canonical_asset_id"],
            _yn(x["price_source_available"]),
            _yn(x["macro_data_available"]),
            _yn(x["cot_data_available"]),
            x["cot_status"],
            _yn(x["displayed_on_radar"]),
            x["classification"],
        ])
    lines.append(_md_table(headers, rows))
    lines.append("")

    # Task 2 dup/derived
    lines.append("## Task 2 — Duplicate / derived / no-data / orphaned")
    lines.append("")
    for bucket in (DUPLICATE, DERIVED, MACRO_ONLY, NO_DATA, ORPHANED):
        members = [x for x in audit["instruments"] if x["classification"] == bucket]
        lines.append(f"### {bucket} ({len(members)})")
        for x in sorted(members, key=lambda r: r["instrument_id"]):
            extra = f" -> {x['duplicate_of']}" if x["duplicate_of"] else ""
            lines.append(f"- {x['instrument_id']} ({x['asset_class']}){extra}")
        lines.append("")

    # Task 3+4 canonical coverage
    lines.append("## Task 3 + 4 — Canonical universe coverage")
    lines.append("")
    headers = ["Canonical", "Class", "Registry instrument", "Price", "Macro", "COT", "COT status", "Missing"]
    rows = []
    for r in audit["canonical_universe"]:
        rows.append([
            r["canonical_asset"],
            r["asset_class"],
            r["registry_instrument"] or "—",
            _yn(r["price_source"]),
            _yn(r["macro_data"]),
            _yn(r["cot_data"]) if r["cot_expected"] else "n/a",
            r["cot_status"] or "—",
            ", ".join(r["missing"]) or "—",
        ])
    lines.append(_md_table(headers, rows))
    lines.append("")

    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    return OUT_JSON, OUT_MD


def main() -> None:
    audit = build_audit()
    j, m = write_reports(audit)
    s = audit["summary"]
    print(f"Wrote {j}")
    print(f"Wrote {m}")
    print(json.dumps(s, indent=2))


if __name__ == "__main__":
    main()
