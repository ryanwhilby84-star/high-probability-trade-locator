"""Official World Gold Council GDT XLSX ingestion (research data pipeline).

Replaces HTML table scraping. Accepts authenticated download (optional cookie)
or manually supplied workbooks. Produces canonical quarterly supply/demand.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import os
import re
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from html import unescape
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import unquote

import pandas as pd
import requests

from hptl.config import PROJECT_ROOT

RAW_DIR = PROJECT_ROOT / "data" / "raw" / "wgc_gdt"
PROCESSED_DIR = PROJECT_ROOT / "data" / "processed"
AUDIT_DIR = PROJECT_ROOT / "data" / "audits"
MANIFEST_PATH = RAW_DIR / "source_manifest.json"
QUARTERLY_CSV = PROCESSED_DIR / "wgc_gold_supply_demand_quarterly.csv"
VINTAGES_CSV = PROCESSED_DIR / "wgc_gold_supply_demand_vintages.csv"
CB_RESERVES_CSV = PROCESSED_DIR / "wgc_central_bank_reserves_quarterly.csv"
REPORT_MD = AUDIT_DIR / "wgc_gdt_bootstrap_report.md"
REVISIONS_CSV = AUDIT_DIR / "wgc_gdt_revisions.csv"
CACHE_JSON = PROJECT_ROOT / "data" / "cache" / "gold_market_clearing" / "wgc_gdt_sectors.json"

GOLDHUB_DEMAND_PAGE = "https://www.gold.org/goldhub/data/gold-demand-by-country"
GOLDHUB_RESERVES_PAGE = "https://www.gold.org/goldhub/data/gold-reserves-by-country"
USER_AGENT = "Mozilla/5.0 (compatible; HPTL/wgc-gdt-xlsx)"

MIN_QUARTERS_HARD = 40
MIN_QUARTERS_TARGET = 60
RECON_TOLERANCE_TONNES = 25.0  # residual / rounding tolerance

CANONICAL_FIELDS = [
    "quarter",
    "jewellery_tonnes",
    "jewellery_consumption_tonnes",
    "technology_tonnes",
    "bar_coin_tonnes",
    "etf_tonnes",
    "central_bank_tonnes",
    "other_investment_tonnes",
    "total_demand_tonnes",
    "mine_production_tonnes",
    "recycling_tonnes",
    "producer_hedging_tonnes",
    "total_supply_tonnes",
    "otc_other_tonnes",
    "gold_price_usd_oz",
    "publication_date",
    "available_date",
    "source_file",
    "source_schema",
]

# Label aliases → canonical field (fabrication preferred for jewellery)
LABEL_ALIASES: dict[str, str] = {
    # jewellery (fabrication ≠ consumption)
    "jewellery fabrication": "jewellery_tonnes",
    "jewelry fabrication": "jewellery_tonnes",
    "jewellery consumption": "jewellery_consumption_tonnes",
    "jewelry consumption": "jewellery_consumption_tonnes",
    "jewellery": "jewellery_tonnes",
    "jewelry": "jewellery_tonnes",
    # tech
    "technology": "technology_tonnes",
    # investment
    "total bar and coin": "bar_coin_tonnes",
    "bar and coin": "bar_coin_tonnes",
    "bars and coins": "bar_coin_tonnes",
    "etfs and similar products": "etf_tonnes",
    "etfs & similar products": "etf_tonnes",
    "etf and similar products": "etf_tonnes",
    "etfs": "etf_tonnes",
    "central bank and other institutions": "central_bank_tonnes",
    "central banks and other institutions": "central_bank_tonnes",
    "central banks & other inst.": "central_bank_tonnes",
    "central banks & other institutions": "central_bank_tonnes",
    "central banks": "central_bank_tonnes",
    "other investment": "other_investment_tonnes",
    "otc investment": "other_investment_tonnes",
    # totals / residual — prefer "gold demand" (ex-OTC) over accounting "total demand"
    "gold demand": "total_demand_tonnes",
    "total gold demand": "total_demand_tonnes",
    "total demand": "total_demand_tonnes",
    "mine production": "mine_production_tonnes",
    "recycled gold": "recycling_tonnes",
    "recycling": "recycling_tonnes",
    "net producer hedging": "producer_hedging_tonnes",
    "producer hedging": "producer_hedging_tonnes",
    "total supply": "total_supply_tonnes",
    "total gold supply": "total_supply_tonnes",
    "otc and other": "otc_other_tonnes",
    "otc & other": "otc_other_tonnes",
    "surplus/deficit": "otc_other_tonnes",
    "surplus / deficit": "otc_other_tonnes",
    # price
    "lbma gold price": "gold_price_usd_oz",
    "lbma gold price (us$/oz)": "gold_price_usd_oz",
    "average gold price": "gold_price_usd_oz",
    "gold price": "gold_price_usd_oz",
    "gold price (us$/oz)": "gold_price_usd_oz",
    "us$/oz": "gold_price_usd_oz",
}

# Do not map these (sub-components / accounting duplicates / junk)
LABEL_SKIP: set[str] = {
    "jewellery inventory",
    "jewelry inventory",
    "total mine supply",
    "investment",
    "electronics",
    "other industrial",
    "dentistry",
    "bars",
    "official coins",
    "medals imitation coins",
    "year-on-year % change",
    "taxonomy",
}

# Short aliases that must match exactly (avoid "jewellery inventory" → jewellery)
LABEL_EXACT_ONLY: set[str] = {
    "jewellery",
    "jewelry",
    "etfs",
    "central banks",
    "us$/oz",
    "gold price",
    "technology",
    "recycling",
}

PREFERRED_SHEETS = (
    "gold balance",
    "supply and demand",
    "table 1",
    "gdt",
)


@dataclass
class SourceEntry:
    path: str
    report_date: str | None = None
    publication_date: str | None = None
    covered_period: str | None = None
    workbook_url: str | None = None
    schema_version: str = "auto"
    priority: int = 100  # higher = preferred for conflicts
    notes: str = ""


@dataclass
class IngestResult:
    ok: bool
    n_quarters: int = 0
    earliest: str | None = None
    latest: str | None = None
    populated: dict[str, int] = field(default_factory=dict)
    n_revisions: int = 0
    n_recon_failures: int = 0
    auth_required: bool = False
    error: str | None = None
    sources_used: list[str] = field(default_factory=list)
    paths: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "n_quarters": self.n_quarters,
            "earliest": self.earliest,
            "latest": self.latest,
            "populated": self.populated,
            "n_revisions": self.n_revisions,
            "n_recon_failures": self.n_recon_failures,
            "auth_required": self.auth_required,
            "error": self.error,
            "sources_used": self.sources_used,
            "paths": self.paths,
        }


# ---------------------------------------------------------------------------
# XLSX detection / download
# ---------------------------------------------------------------------------


def is_real_xlsx(content: bytes) -> bool:
    """ZIP/OOXML magic; reject HTML login pages."""
    if not content or len(content) < 4:
        return False
    if content[:4] != b"PK\x03\x04":
        return False
    head = content[:800].lower()
    if b"<html" in head or b"<!doctype" in head:
        return False
    return True


def detect_bad_download(content: bytes, content_type: str | None = None) -> str | None:
    ctype = (content_type or "").lower()
    if "html" in ctype:
        return f"Incorrect MIME type (HTML): {content_type}"
    if not content:
        return "Empty download"
    if content[:4] != b"PK\x03\x04":
        head = content[:200].lower()
        if b"<html" in head or b"<!doctype" in head or b"login" in head:
            return "Goldhub authentication required (HTML/login response, not XLSX)"
        return f"Not an XLSX workbook (magic={content[:4]!r})"
    return None


def _now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _relpath(path: Path) -> str:
    try:
        return str(path.relative_to(PROJECT_ROOT)).replace("\\", "/")
    except ValueError:
        return str(path).replace("\\", "/")


def discover_gdt_xlsx_url(page_url: str = GOLDHUB_DEMAND_PAGE) -> str | None:
    """Find the official GDT Tables XLSX link on the Goldhub page."""
    r = requests.get(page_url, headers={"User-Agent": USER_AGENT}, timeout=60)
    r.raise_for_status()
    hrefs = re.findall(r'href=["\']([^"\']+)["\']', r.text, flags=re.I)
    for h in hrefs:
        hu = unescape(unquote(h))
        if ".xlsx" in hu.lower() and ("gdt" in hu.lower() or "table" in hu.lower()):
            if hu.startswith("http"):
                return hu
            return "https://www.gold.org" + hu
    return None


def try_download_gdt_xlsx(*, dest_dir: Path | None = None) -> tuple[Path | None, str | None]:
    """Attempt authenticated/unauthenticated download. Returns (path, error)."""
    dest_dir = dest_dir or RAW_DIR
    dest_dir.mkdir(parents=True, exist_ok=True)
    url = discover_gdt_xlsx_url()
    if not url:
        return None, "No GDT XLSX download link found on Goldhub demand page"
    headers = {"User-Agent": USER_AGENT}
    cookie = os.environ.get("WGC_GOLDHUB_COOKIE", "").strip()
    if cookie:
        headers["Cookie"] = cookie
    resp = requests.get(url, headers=headers, timeout=120, allow_redirects=True)
    bad = detect_bad_download(resp.content, resp.headers.get("content-type"))
    if bad:
        if resp.status_code in {401, 403} or "authentication" in bad.lower() or "html" in bad.lower():
            return None, (
                "Goldhub authentication required. "
                "Set WGC_GOLDHUB_COOKIE or pass --xlsx /path/to/GDT_Tables.xlsx "
                f"(HTTP {resp.status_code}; {bad})"
            )
        return None, bad
    fname = unquote(url.rstrip("/").split("/")[-1]).replace("'", "_")
    if not fname.lower().endswith(".xlsx"):
        fname = "GDT_Tables_latest_EN.xlsx"
    path = dest_dir / fname
    path.write_bytes(resp.content)
    return path, None


# ---------------------------------------------------------------------------
# Quarter / label parsing
# ---------------------------------------------------------------------------


def parse_quarter_label(raw: Any) -> str | None:
    """Return quarter-end ISO date from assorted WGC labels."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return None
    if isinstance(raw, (datetime, pd.Timestamp)):
        ts = pd.Timestamp(raw)
        q = (ts.month - 1) // 3 + 1
        month = q * 3
        end = pd.Timestamp(year=ts.year, month=month, day=1) + pd.offsets.MonthEnd(0)
        return end.strftime("%Y-%m-%d")
    s = str(raw).strip()
    # Normalize curly / typographic apostrophes to ASCII
    s = s.replace("\u2019", "'").replace("\u2018", "'").replace("`", "'")
    s = re.sub(r"\s+", " ", s)

    # (year, quarter) extractors — order matters
    extractors = [
        (r"^Q([1-4])['\s/\-]*(\d{2,4})$", lambda m: (int(m.group(2)), int(m.group(1)))),
        (r"^Q([1-4])\s+(\d{4})$", lambda m: (int(m.group(2)), int(m.group(1)))),
        (r"^(\d{4})\s*Q([1-4])$", lambda m: (int(m.group(1)), int(m.group(2)))),
        (r"^([1-4])Q(\d{2,4})$", lambda m: (int(m.group(2)), int(m.group(1)))),
    ]
    for pat, pick in extractors:
        m = re.match(pat, s, re.I)
        if not m:
            continue
        y, q = pick(m)
        if y < 100:
            y += 2000
        month = q * 3
        end = pd.Timestamp(year=y, month=month, day=1) + pd.offsets.MonthEnd(0)
        return end.strftime("%Y-%m-%d")
    # Annual-only labels are skipped for quarterly series
    if re.fullmatch(r"\d{4}", s):
        return None
    dt = pd.to_datetime(raw, errors="coerce")
    if pd.isna(dt):
        return None
    # Only accept if looks like quarter-end-ish
    ts = pd.Timestamp(dt)
    if ts.month in {3, 6, 9, 12} and ts.day >= 28:
        return ts.strftime("%Y-%m-%d")
    return None


def normalize_label(raw: Any) -> str:
    s = str(raw or "").strip().lower()
    s = s.replace("&", "and")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("–", "-").replace("—", "-")
    return s


def map_label(raw: Any) -> str | None:
    lab = normalize_label(raw)
    if not lab or lab in {"nan", "none"}:
        return None
    if lab in LABEL_SKIP:
        return None
    if lab in LABEL_ALIASES:
        return LABEL_ALIASES[lab]
    for key, field in LABEL_ALIASES.items():
        if key in LABEL_EXACT_ONLY:
            continue
        if lab.startswith(key + " ") or lab.startswith(key + "("):
            return field
    return None


def _row_label(row: list[Any]) -> Any:
    """First non-empty cell in the leading columns (WGC often uses col B)."""
    for cell in row[:4]:
        if cell is None or (isinstance(cell, float) and pd.isna(cell)):
            continue
        s = str(cell).strip()
        if not s or s.lower() in {"nan", "none"}:
            continue
        # Skip pure numeric period headers used as duplicate row tags
        if parse_quarter_label(cell) is not None:
            continue
        try:
            float(s.replace(",", ""))
            continue
        except ValueError:
            return cell
    return None


def parse_data_as_of(matrix: list[list[Any]]) -> str | None:
    """Extract 'Data as of 30 June, 2026' → ISO date."""
    for row in matrix:
        for cell in row[:6]:
            if cell is None:
                continue
            m = re.search(
                r"Data as of\s+(\d{1,2}\s+[A-Za-z]+,?\s+\d{4})",
                str(cell),
                flags=re.I,
            )
            if not m:
                continue
            dt = pd.to_datetime(m.group(1), errors="coerce")
            if pd.notna(dt):
                return pd.Timestamp(dt).strftime("%Y-%m-%d")
    return None


def default_publication_date(quarter_end: str) -> str:
    """Conservative fallback: ~45 days after quarter-end (not quarter-end itself)."""
    return (date.fromisoformat(quarter_end[:10]) + timedelta(days=45)).isoformat()


# Known publication dates for recent GDT releases (extend via manifest).
KNOWN_PUBLICATION_DATES: dict[str, str] = {
    "2026-06-30": "2026-07-30",
    "2026-03-31": "2026-04-30",
    "2025-12-31": "2026-01-29",
    "2025-09-30": "2025-10-30",
    "2025-06-30": "2025-07-31",
    "2025-03-31": "2025-04-30",
}


# ---------------------------------------------------------------------------
# Parsers
# ---------------------------------------------------------------------------


def _sheet_to_matrix(df: pd.DataFrame) -> list[list[Any]]:
    return df.where(pd.notna(df), None).values.tolist()


def _find_header_row(matrix: list[list[Any]], max_scan: int = 15) -> tuple[int, list[str | None]] | None:
    for i, row in enumerate(matrix[:max_scan]):
        quarters = [parse_quarter_label(c) for c in row]
        n_q = sum(1 for q in quarters if q is not None)
        if n_q >= 4:
            return i, quarters
    return None


def _sheet_priority(name: str) -> int:
    n = normalize_label(name)
    for i, pref in enumerate(PREFERRED_SHEETS):
        if pref in n:
            return i
    return 100


def parse_gdt_workbook(path: Path) -> tuple[list[dict[str, Any]], str, dict[str, Any]]:
    """Parse a GDT XLSX into long vintage rows.

    Returns (rows, schema_name, meta).
    Prefers the official 'Gold Balance' global quarterly block.
    """
    raw = path.read_bytes()
    bad = detect_bad_download(raw)
    if bad:
        raise ValueError(f"{path.name}: {bad}")

    xl = pd.ExcelFile(BytesIO(raw))
    all_rows: list[dict[str, Any]] = []
    schema_votes = {"legacy": 0, "consolidated": 0}
    sheets_used: list[str] = []
    publication_date: str | None = None
    # Prefer Gold Balance; skip country/detail sheets that also have quarter headers
    sheet_names = sorted(xl.sheet_names, key=_sheet_priority)
    preferred_only = [s for s in sheet_names if _sheet_priority(s) < 100]
    scan_sheets = preferred_only or sheet_names

    for sheet in scan_sheets:
        df = pd.read_excel(xl, sheet_name=sheet, header=None)
        if df.empty or df.shape[1] < 3:
            continue
        matrix = _sheet_to_matrix(df)
        if publication_date is None:
            publication_date = parse_data_as_of(matrix)
        found = _find_header_row(matrix, max_scan=20)
        if not found:
            continue
        header_i, quarters = found
        n_q = sum(1 for q in quarters if q is not None)
        if n_q < 4:
            continue
        sheets_used.append(sheet)

        labels = []
        for r in range(header_i + 1, len(matrix)):
            lab_raw = _row_label(matrix[r]) if matrix[r] else None
            if lab_raw is not None:
                labels.append(normalize_label(lab_raw))
        has_fab = any("fabrication" in (lab or "") for lab in labels)
        has_cons = any(
            "consumption" in (lab or "") and ("jewell" in (lab or "") or "jewelr" in (lab or ""))
            for lab in labels
        )
        has_surplus = any("surplus" in (lab or "") and "deficit" in (lab or "") for lab in labels)
        has_otc = any((lab or "").startswith("otc") for lab in labels)
        has_lbma = any("lbma" in (lab or "") for lab in labels)
        # Current Gold Balance has fabrication + consumption + OTC → consolidated/current
        if has_otc or has_lbma or (has_fab and "gold balance" in normalize_label(sheet)):
            schema = "consolidated"
        elif has_cons or (has_surplus and not has_otc):
            schema = "legacy"
        else:
            schema = "consolidated"
        schema_votes[schema] += 1

        # Track whether gold demand was seen (preferred over accounting total demand)
        seen_gold_demand = False
        pending_total_demand: list[tuple[str, float]] = []

        for r in range(header_i + 1, len(matrix)):
            row = matrix[r]
            if not row:
                continue
            lab_raw = _row_label(row)
            if lab_raw is None:
                continue
            lab_norm = normalize_label(lab_raw)
            field = map_label(lab_raw)
            if field is None:
                continue
            is_gold_demand = lab_norm == "gold demand"
            is_total_demand = lab_norm in {"total demand", "total gold demand"}
            for c, q in enumerate(quarters):
                if q is None or c >= len(row):
                    continue
                val = row[c]
                if val is None or (isinstance(val, float) and pd.isna(val)):
                    continue
                try:
                    num = float(str(val).replace(",", "").replace("—", "").replace("–", ""))
                except (TypeError, ValueError):
                    continue
                if not math.isfinite(num):
                    continue
                if is_total_demand and not is_gold_demand:
                    pending_total_demand.append((q, num))
                    continue
                if is_gold_demand:
                    seen_gold_demand = True
                all_rows.append(
                    {
                        "quarter": q,
                        "field": field,
                        "value": num,
                        "source_file": path.name,
                        "source_schema": schema,
                        "sheet": sheet,
                    }
                )

        if not seen_gold_demand:
            for q, num in pending_total_demand:
                all_rows.append(
                    {
                        "quarter": q,
                        "field": "total_demand_tonnes",
                        "value": num,
                        "source_file": path.name,
                        "source_schema": schema,
                        "sheet": sheet,
                    }
                )

        # Official Gold Balance is sufficient; stop after first preferred sheet with data
        if all_rows and _sheet_priority(sheet) < 100:
            break

    if not all_rows:
        raise ValueError(f"No sector/quarter cells parsed from {path.name}")

    schema_name = (
        "legacy" if schema_votes["legacy"] > schema_votes["consolidated"] else "consolidated"
    )
    meta = {
        "file": path.name,
        "sheets": xl.sheet_names,
        "sheets_used": sheets_used,
        "schema_votes": schema_votes,
        "n_cells": len(all_rows),
        "sha256": hashlib.sha256(raw).hexdigest()[:16],
        "publication_date": publication_date,
    }
    return all_rows, schema_name, meta


def wide_from_long(
    long_rows: list[dict[str, Any]],
    *,
    source_file: str,
    source_schema: str,
    publication_date_by_quarter: dict[str, str] | None = None,
    priority: int = 100,
) -> list[dict[str, Any]]:
    """Pivot long cells to one row per quarter."""
    by_q: dict[str, dict[str, Any]] = {}
    for cell in long_rows:
        q = cell["quarter"]
        rec = by_q.setdefault(
            q,
            {
                "quarter": q,
                "source_file": source_file,
                "source_schema": source_schema,
                "priority": priority,
            },
        )
        # Prefer first non-null; jewellery_tonnes prefers fabrication mapping already
        field = cell["field"]
        if field not in rec or rec[field] is None:
            rec[field] = cell["value"]

    pub_map = publication_date_by_quarter or {}
    out = []
    for q in sorted(by_q):
        rec = by_q[q]
        pub = pub_map.get(q) or KNOWN_PUBLICATION_DATES.get(q) or default_publication_date(q)
        rec["publication_date"] = pub
        rec["available_date"] = pub
        # Ensure jewellery uses fabrication when both present
        if rec.get("jewellery_tonnes") is None and rec.get("jewellery_consumption_tonnes") is not None:
            # do not silently substitute consumption into fabrication field
            pass
        out.append(rec)
    return out


# ---------------------------------------------------------------------------
# Vintage merge / reconciliation
# ---------------------------------------------------------------------------


def merge_vintages(
    vintage_tables: list[list[dict[str, Any]]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    """Merge vintages: highest priority wins; record revisions.

    Returns (canonical_rows, all_vintage_rows, revisions).
    """
    # Flatten with priority
    flat: list[dict[str, Any]] = []
    for table in vintage_tables:
        flat.extend(table)
    flat.sort(key=lambda r: (r["quarter"], int(r.get("priority") or 0)))

    canonical: dict[str, dict[str, Any]] = {}
    revisions: list[dict[str, Any]] = []
    for rec in flat:
        q = rec["quarter"]
        if q not in canonical:
            canonical[q] = dict(rec)
            continue
        prev = canonical[q]
        # Higher priority replaces field-wise
        if int(rec.get("priority") or 0) < int(prev.get("priority") or 0):
            # older/lower priority: only fill missing
            for k, v in rec.items():
                if k in {"quarter", "priority", "source_file", "source_schema"}:
                    continue
                if prev.get(k) is None and v is not None:
                    prev[k] = v
            continue
        for k, v in rec.items():
            if k in {"quarter", "priority"}:
                continue
            if k in {"source_file", "source_schema", "publication_date", "available_date"}:
                if int(rec.get("priority") or 0) >= int(prev.get("priority") or 0):
                    prev[k] = v
                continue
            old = prev.get(k)
            if old is None:
                prev[k] = v
            elif v is not None and abs(float(old) - float(v)) > 1e-9:
                revisions.append(
                    {
                        "quarter": q,
                        "field": k,
                        "old_value": old,
                        "new_value": v,
                        "old_source": prev.get("source_file"),
                        "new_source": rec.get("source_file"),
                        "revision_magnitude": round(float(v) - float(old), 6),
                    }
                )
                prev[k] = v
        prev["priority"] = rec.get("priority")
        prev["source_file"] = rec.get("source_file")
        prev["source_schema"] = rec.get("source_schema")

    canon_rows = [canonical[q] for q in sorted(canonical)]
    return canon_rows, flat, revisions


def reconcile_row(row: dict[str, Any]) -> list[str]:
    """Return list of reconciliation failure messages."""
    fails = []
    # Demand components vs total (fabrication preferred)
    jew = row.get("jewellery_tonnes")
    tech = row.get("technology_tonnes")
    bar = row.get("bar_coin_tonnes")
    etf = row.get("etf_tonnes")
    cb = row.get("central_bank_tonnes")
    other = row.get("other_investment_tonnes") or 0.0
    td = row.get("total_demand_tonnes")
    parts = [jew, tech, bar, etf, cb]
    if td is not None and all(v is not None for v in parts):
        s = float(jew) + float(tech) + float(bar) + float(etf) + float(cb) + float(other)
        # OTC often bridges demand/supply identity; allow larger residual vs printed total
        if abs(s - float(td)) > RECON_TOLERANCE_TONNES + abs(float(row.get("otc_other_tonnes") or 0)):
            # Soft fail only if residual huge
            if abs(s - float(td)) > 150.0:
                fails.append(
                    f"{row['quarter']}: demand parts sum {s:.1f} vs total_demand {td}"
                )
    mine = row.get("mine_production_tonnes")
    recy = row.get("recycling_tonnes")
    hedge = row.get("producer_hedging_tonnes")
    ts = row.get("total_supply_tonnes")
    if ts is not None and mine is not None and recy is not None:
        hs = float(hedge) if hedge is not None else 0.0
        ssup = float(mine) + float(recy) + hs
        if abs(ssup - float(ts)) > RECON_TOLERANCE_TONNES:
            if abs(ssup - float(ts)) > 50.0:
                fails.append(
                    f"{row['quarter']}: supply parts sum {ssup:.1f} vs total_supply {ts}"
                )
    return fails


def validate_canonical(rows: list[dict[str, Any]]) -> tuple[bool, str, dict[str, Any]]:
    if not rows:
        return False, "No quarterly rows", {}
    quarters = [r["quarter"] for r in rows]
    if len(quarters) != len(set(quarters)):
        return False, "Duplicate canonical quarters", {}
    earliest, latest = quarters[0], quarters[-1]
    if len(rows) < MIN_QUARTERS_HARD:
        return (
            False,
            f"Fewer than {MIN_QUARTERS_HARD} quarters loaded (n={len(rows)}; earliest={earliest}, latest={latest})",
            {"earliest": earliest, "latest": latest, "n": len(rows)},
        )
    if len(rows) < MIN_QUARTERS_TARGET:
        return (
            False,
            f"History short of target ({MIN_QUARTERS_TARGET}+ quarters required; n={len(rows)}; earliest={earliest}, latest={latest})",
            {"earliest": earliest, "latest": latest, "n": len(rows)},
        )
    if earliest > "2011-03-31":
        return (
            False,
            f"Earliest quarter too late for WGC GDT history (got {earliest}; expect ~2010 or earlier)",
            {"earliest": earliest, "latest": latest, "n": len(rows)},
        )
    # Units sanity: mine production quarterly ~700-1000 tonnes
    mines = [float(r["mine_production_tonnes"]) for r in rows if r.get("mine_production_tonnes") is not None]
    if mines and (min(mines) < 100 or max(mines) > 2000):
        return False, f"Mine production units look wrong (min={min(mines)}, max={max(mines)})", {}
    # No future publication leakage relative to today
    today = date.today().isoformat()
    for r in rows:
        if r.get("available_date") and r["available_date"] > today:
            # allowed only if explicitly future-dated publication; flag
            pass
    recon_fails = []
    for r in rows:
        recon_fails.extend(reconcile_row(r))
    populated = {}
    for f in CANONICAL_FIELDS:
        if f in {"quarter", "publication_date", "available_date", "source_file", "source_schema"}:
            continue
        populated[f] = sum(1 for r in rows if r.get(f) is not None)
    meta = {
        "earliest": earliest,
        "latest": latest,
        "n": len(rows),
        "populated": populated,
        "recon_failures": recon_fails,
        "n_recon_failures": len(recon_fails),
    }
    return True, "ok", meta


# ---------------------------------------------------------------------------
# Central bank reserves (separate)
# ---------------------------------------------------------------------------


def try_ingest_cb_reserves_xlsx(paths: list[Path]) -> list[dict[str, Any]]:
    """Best-effort parse of WGC official reserves / changes workbooks."""
    rows: list[dict[str, Any]] = []
    for path in paths:
        if not path.is_file():
            continue
        raw = path.read_bytes()
        if not is_real_xlsx(raw):
            continue
        name = path.name.lower()
        if not any(k in name for k in ("reserve", "changes", "official")):
            continue
        xl = pd.ExcelFile(BytesIO(raw))
        for sheet in xl.sheet_names:
            df = pd.read_excel(xl, sheet_name=sheet, header=None)
            matrix = _sheet_to_matrix(df)
            found = _find_header_row(matrix)
            if not found:
                continue
            header_i, quarters = found
            for r in range(header_i + 1, min(header_i + 40, len(matrix))):
                lab = normalize_label(matrix[r][0] if matrix[r] else "")
                if not lab:
                    continue
                # World total / changes rows
                if not any(k in lab for k in ("world", "total", "change", "net")):
                    continue
                for c, q in enumerate(quarters):
                    if q is None or c >= len(matrix[r]):
                        continue
                    try:
                        val = float(str(matrix[r][c]).replace(",", ""))
                    except (TypeError, ValueError):
                        continue
                    rows.append(
                        {
                            "quarter": q,
                            "value_tonnes": val,
                            "label": lab,
                            "source_file": path.name,
                            "series_type": "reserves_or_changes",
                            "note": (
                                "Separate from GDT central-bank demand; "
                                "reserve stock/changes ≠ GDT CB demand estimate"
                            ),
                        }
                    )
    # Dedup by quarter+label keeping last
    by_key: dict[tuple[str, str], dict[str, Any]] = {}
    for r in rows:
        by_key[(r["quarter"], r["label"])] = r
    return [by_key[k] for k in sorted(by_key)]


# ---------------------------------------------------------------------------
# Manifest / IO
# ---------------------------------------------------------------------------


def load_or_init_manifest() -> dict[str, Any]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    if MANIFEST_PATH.exists():
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    doc = {
        "description": "WGC GDT XLSX source manifest",
        "updated_at": _now_iso(),
        "sources": [],
        "notes": (
            "Place official GDT Tables XLSX files under data/raw/wgc_gdt/. "
            "Latest comprehensive workbook should have highest priority."
        ),
    }
    MANIFEST_PATH.write_text(json.dumps(doc, indent=2), encoding="utf-8")
    return doc


def collect_xlsx_paths(
    *,
    xlsx: Path | None,
    xlsx_dir: Path | None,
    include_raw_dir: bool = True,
) -> list[Path]:
    paths: list[Path] = []
    if xlsx is not None:
        paths.append(xlsx)
    dirs = []
    if xlsx_dir is not None:
        dirs.append(xlsx_dir)
    if include_raw_dir:
        dirs.append(RAW_DIR)
    for d in dirs:
        if d.is_dir():
            paths.extend(sorted(d.glob("*.xlsx")))
            paths.extend(sorted(d.glob("*.xls")))
    # unique preserve order
    seen = set()
    out = []
    for p in paths:
        rp = p.resolve()
        if rp in seen or not p.is_file():
            continue
        # skip probe leftovers
        if p.name.startswith("_"):
            continue
        seen.add(rp)
        out.append(p)
    return out


def _priority_for_file(
    path: Path,
    manifest: dict[str, Any],
    *,
    primary: Path | None = None,
) -> tuple[int, str | None, str | None]:
    if primary is not None and path.resolve() == primary.resolve():
        return 300, KNOWN_PUBLICATION_DATES.get("2026-06-30"), "auto"
    name = path.name
    for src in manifest.get("sources") or []:
        if src.get("path") and Path(src["path"]).name == name:
            return (
                int(src.get("priority") or 100),
                src.get("publication_date"),
                src.get("schema_version"),
            )
        if src.get("workbook_url") and name in str(src.get("workbook_url")):
            return (
                int(src.get("priority") or 100),
                src.get("publication_date"),
                src.get("schema_version"),
            )
    # Heuristic: "Q2'26" / latest tables get high priority; FY2021 lower
    n = name.lower()
    if "2026" in n or ("q2" in n and "26" in n):
        return 200, KNOWN_PUBLICATION_DATES.get("2026-06-30"), "auto"
    if "2025" in n:
        return 180, None, "auto"
    if "2021" in n:
        return 120, None, "legacy"
    return 100, None, "auto"


def write_csv(path: Path, rows: list[dict[str, Any]], fields: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    if fields is None:
        fields = []
        seen = set()
        for r in rows:
            for k in r:
                if k not in seen:
                    seen.add(k)
                    fields.append(k)
    with path.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k) for k in fields})


def write_sector_cache(canon: list[dict[str, Any]]) -> Path:
    """Maintain compatibility cache for market-clearing loader."""
    series = {
        "jewellery": {},
        "jewellery_consumption": {},
        "technology": {},
        "bar_coin": {},
        "etf": {},
        "cb": {},
        "mine": {},
        "recycling": {},
        "hedging": {},
        "total_demand": {},
        "total_supply": {},
        "otc_other": {},
        "gold_price": {},
        "available_date": {},
    }
    for r in canon:
        q = r["quarter"]
        mapping = {
            "jewellery": r.get("jewellery_tonnes"),
            "jewellery_consumption": r.get("jewellery_consumption_tonnes"),
            "technology": r.get("technology_tonnes"),
            "bar_coin": r.get("bar_coin_tonnes"),
            "etf": r.get("etf_tonnes"),
            "cb": r.get("central_bank_tonnes"),
            "mine": r.get("mine_production_tonnes"),
            "recycling": r.get("recycling_tonnes"),
            "hedging": r.get("producer_hedging_tonnes"),
            "total_demand": r.get("total_demand_tonnes"),
            "total_supply": r.get("total_supply_tonnes"),
            "otc_other": r.get("otc_other_tonnes"),
            "gold_price": r.get("gold_price_usd_oz"),
        }
        for k, v in mapping.items():
            if v is not None:
                series[k][q] = float(v)
        series["available_date"][q] = r.get("available_date") or r.get("publication_date")
    payload = {
        "source": "WGC GDT official XLSX (canonical merged vintages)",
        "unit": "tonnes",
        "generated_at": _now_iso(),
        "n_quarters": len(canon),
        "earliest": canon[0]["quarter"] if canon else None,
        "latest": canon[-1]["quarter"] if canon else None,
        "series": series,
        "counts": {k: len(v) for k, v in series.items() if k != "available_date"},
        "note": (
            "jewellery series uses jewellery fabrication where available; "
            "jewellery_consumption kept separately. "
            "available_date is GDT publication date for as-of joins."
        ),
    }
    CACHE_JSON.parent.mkdir(parents=True, exist_ok=True)
    CACHE_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return CACHE_JSON


def render_report(result: IngestResult, meta: dict[str, Any]) -> str:
    lines = [
        "# WGC GDT XLSX Bootstrap Report",
        "",
        f"Generated: `{_now_iso()}`",
        "",
        f"**Status:** `{'PASS' if result.ok else 'FAIL'}`",
        "",
    ]
    if result.error:
        lines.extend([f"**Error:** {result.error}", ""])
    if result.auth_required:
        lines.extend(
            [
                "## Authentication required",
                "",
                "Goldhub returned HTML/login instead of XLSX.",
                "",
                "Options:",
                "",
                "1. Download **GDT Tables XLSX** from "
                f"[Historical demand and supply]({GOLDHUB_DEMAND_PAGE}) while logged in",
                "2. Save under `data/raw/wgc_gdt/`",
                "3. Or run:",
                "",
                "```bash",
                "python scripts/_bootstrap_wgc_gdt_sectors.py --xlsx /path/to/GDT_Tables.xlsx",
                "```",
                "",
                "Optional cookie automation: set `WGC_GOLDHUB_COOKIE`.",
                "",
            ]
        )
    lines.extend(
        [
            "## Summary",
            "",
            f"- Earliest quarter: **{result.earliest}**",
            f"- Latest quarter: **{result.latest}**",
            f"- Number of quarters: **{result.n_quarters}**",
            f"- Revisions recorded: **{result.n_revisions}**",
            f"- Reconciliation failures: **{result.n_recon_failures}**",
            f"- Sources used: `{result.sources_used}`",
            "",
            "### Populated values by sector",
            "",
        ]
    )
    for k, n in sorted((result.populated or {}).items()):
        lines.append(f"- `{k}`: {n}")
    lines.extend(
        [
            "",
            "## Outputs",
            "",
        ]
    )
    for k, p in (result.paths or {}).items():
        lines.append(f"- `{k}`: `{p}`")
    lines.extend(
        [
            "",
            "## Policy",
            "",
            "- Latest comprehensive workbook is canonical for conflicts",
            "- Prior vintages retained in `wgc_gold_supply_demand_vintages.csv`",
            "- `available_date` = official publication date (not quarter-end)",
            "- Jewellery fabrication preferred over consumption for clearing models",
            "- Central-bank reserves XLSX kept separate from GDT CB demand",
            "",
            f"Extra meta: `{meta}`",
            "",
        ]
    )
    return "\n".join(lines)


def run_bootstrap(
    *,
    xlsx: Path | None = None,
    xlsx_dir: Path | None = None,
    try_download: bool = True,
) -> IngestResult:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    AUDIT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = load_or_init_manifest()

    auth_required = False
    download_error = None

    # Explicit --xlsx: validate locally and never touch the network.
    primary_xlsx: Path | None = None
    if xlsx is not None:
        primary_xlsx = Path(xlsx)
        if not primary_xlsx.is_file():
            result = IngestResult(ok=False, error=f"Workbook not found: {primary_xlsx}")
            REPORT_MD.write_text(render_report(result, {}), encoding="utf-8")
            result.paths = {"report": _relpath(REPORT_MD)}
            return result
        bad = detect_bad_download(primary_xlsx.read_bytes())
        if bad:
            result = IngestResult(
                ok=False,
                error=f"Unreadable workbook ({primary_xlsx.name}): {bad}",
            )
            REPORT_MD.write_text(render_report(result, {}), encoding="utf-8")
            result.paths = {"report": _relpath(REPORT_MD)}
            return result
        try_download = False

    if try_download:
        path, err = try_download_gdt_xlsx()
        if err:
            download_error = err
            if "authentication" in err.lower() or "cookie" in err.lower():
                auth_required = True
        elif path is not None:
            # register in manifest (download path only — never called when --xlsx set)
            sources = list(manifest.get("sources") or [])
            rel = _relpath(path)
            if not any(s.get("path") == rel for s in sources):
                sources.append(
                    {
                        "path": rel,
                        "workbook_url": None,
                        "publication_date": KNOWN_PUBLICATION_DATES.get("2026-06-30"),
                        "covered_period": "2010Q1–latest",
                        "schema_version": "auto",
                        "priority": 200,
                        "report_date": "2026-06-30",
                        "notes": "Auto-downloaded latest GDT Tables EN",
                    }
                )
                manifest["sources"] = sources
                manifest["updated_at"] = _now_iso()
                MANIFEST_PATH.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    # When --xlsx is supplied and valid: that workbook is the sole default source.
    # Extra local vintages only if --xlsx-dir is explicit. Never HTTP.
    if primary_xlsx is not None:
        if xlsx_dir is not None:
            paths = collect_xlsx_paths(
                xlsx=primary_xlsx,
                xlsx_dir=xlsx_dir,
                include_raw_dir=False,
            )
            paths = [primary_xlsx] + [
                p for p in paths if p.resolve() != primary_xlsx.resolve()
            ]
        else:
            paths = [primary_xlsx]
    else:
        paths = collect_xlsx_paths(xlsx=xlsx, xlsx_dir=xlsx_dir, include_raw_dir=True)

    if not paths:
        msg = download_error or (
            "No GDT XLSX files found. Place official workbooks in data/raw/wgc_gdt/ "
            "or pass --xlsx / --xlsx-dir."
        )
        result = IngestResult(ok=False, auth_required=auth_required, error=msg)
        REPORT_MD.write_text(render_report(result, {}), encoding="utf-8")
        result.paths = {"report": _relpath(REPORT_MD)}
        return result

    # Copy primary workbook into raw drop folder for provenance (local-only).
    if primary_xlsx is not None:
        dest = RAW_DIR / primary_xlsx.name.replace("'", "_")
        if primary_xlsx.resolve() != dest.resolve():
            dest.write_bytes(primary_xlsx.read_bytes())

    vintage_tables: list[list[dict[str, Any]]] = []
    sources_used: list[str] = []
    parse_meta: list[dict[str, Any]] = []
    for path in paths:
        try:
            long_rows, schema, meta = parse_gdt_workbook(path)
        except ValueError as exc:
            parse_meta.append({"file": path.name, "error": str(exc)})
            continue
        priority, pub_override, schema_override = _priority_for_file(
            path, manifest, primary=primary_xlsx
        )
        if schema_override and schema_override != "auto":
            schema = schema_override
        # Publication: workbook "Data as of" applies to the latest quarter;
        # known map + conservative defaults for earlier quarters.
        pub_map = dict(KNOWN_PUBLICATION_DATES)
        workbook_pub = meta.get("publication_date") or pub_override
        if workbook_pub and long_rows:
            latest_q = max(cell["quarter"] for cell in long_rows)
            pub_map[latest_q] = workbook_pub
        wide = wide_from_long(
            long_rows,
            source_file=path.name,
            source_schema=schema,
            publication_date_by_quarter=pub_map,
            priority=priority,
        )
        vintage_tables.append(wide)
        sources_used.append(path.name)
        parse_meta.append(meta)

    if not vintage_tables:
        result = IngestResult(
            ok=False,
            auth_required=auth_required,
            error="XLSX files present but no parseable GDT sector matrices",
            sources_used=[p.name for p in paths],
        )
        REPORT_MD.write_text(render_report(result, {"parse_meta": parse_meta}), encoding="utf-8")
        result.paths = {"report": _relpath(REPORT_MD)}
        return result

    canon, all_vintages, revisions = merge_vintages(vintage_tables)
    ok, reason, vmeta = validate_canonical(canon)
    populated = vmeta.get("populated") or {}
    n_recon = int(vmeta.get("n_recon_failures") or 0)

    # Write outputs even on soft recon warnings if history gate passes
    write_csv(QUARTERLY_CSV, canon, CANONICAL_FIELDS + ["priority"])
    # vintages long format
    vint_long = []
    for r in all_vintages:
        base = {
            "quarter": r["quarter"],
            "source_file": r.get("source_file"),
            "source_schema": r.get("source_schema"),
            "priority": r.get("priority"),
            "publication_date": r.get("publication_date"),
        }
        for f in CANONICAL_FIELDS:
            if f in {"quarter", "publication_date", "available_date", "source_file", "source_schema"}:
                continue
            if r.get(f) is not None:
                vint_long.append({**base, "field": f, "value": r.get(f)})
    write_csv(
        VINTAGES_CSV,
        vint_long,
        ["quarter", "field", "value", "source_file", "source_schema", "priority", "publication_date"],
    )
    write_csv(
        REVISIONS_CSV,
        revisions,
        ["quarter", "field", "old_value", "new_value", "old_source", "new_source", "revision_magnitude"],
    )

    cb_rows = try_ingest_cb_reserves_xlsx(paths)
    write_csv(
        CB_RESERVES_CSV,
        cb_rows,
        ["quarter", "value_tonnes", "label", "source_file", "series_type", "note"],
    )
    cache_path = write_sector_cache(canon) if ok else None

    result = IngestResult(
        ok=ok,
        n_quarters=len(canon),
        earliest=vmeta.get("earliest"),
        latest=vmeta.get("latest"),
        populated=populated,
        n_revisions=len(revisions),
        n_recon_failures=n_recon,
        auth_required=auth_required,
        error=None if ok else reason,
        sources_used=sources_used,
        paths={
            "quarterly_csv": _relpath(QUARTERLY_CSV),
            "vintages_csv": _relpath(VINTAGES_CSV),
            "revisions_csv": _relpath(REVISIONS_CSV),
            "cb_reserves_csv": _relpath(CB_RESERVES_CSV),
            "report": _relpath(REPORT_MD),
            "manifest": _relpath(MANIFEST_PATH),
            **({"sector_cache": _relpath(cache_path)} if cache_path else {}),
        },
    )
    REPORT_MD.write_text(
        render_report(result, {"parse_meta": parse_meta, "download_error": download_error}),
        encoding="utf-8",
    )
    return result
