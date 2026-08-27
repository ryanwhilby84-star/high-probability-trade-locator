"""Tests for official WGC GDT XLSX ingestion (no HTML scraping)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from hptl.data_sources.wgc_gdt_xlsx_ingest import (
    MIN_QUARTERS_HARD,
    detect_bad_download,
    is_real_xlsx,
    map_label,
    merge_vintages,
    parse_gdt_workbook,
    parse_quarter_label,
    reconcile_row,
    run_bootstrap,
    validate_canonical,
    wide_from_long,
)


def _quarter_labels(n: int = 64, start_year: int = 2010) -> list[str]:
    labels = []
    y, q = start_year, 1
    for _ in range(n):
        labels.append(f"Q{q}'{str(y)[2:]}")
        q += 1
        if q > 4:
            q = 1
            y += 1
    return labels


def _write_consolidated_workbook(path: Path, n: int = 64) -> Path:
    labels = _quarter_labels(n)
    # header row
    header = [None] + labels
    rows = [
        header,
        ["Jewellery fabrication"] + [500.0 + i * 0.1 for i in range(n)],
        ["Technology"] + [80.0 + (i % 5) for i in range(n)],
        ["Total bar and coin"] + [300.0 for _ in range(n)],
        ["ETFs and similar products"] + [10.0 - (i % 7) for i in range(n)],
        ["Central banks and other institutions"] + [100.0 for _ in range(n)],
        ["Total Demand"]
        + [
            500.0 + i * 0.1 + 80.0 + (i % 5) + 300.0 + (10.0 - (i % 7)) + 100.0
            for i in range(n)
        ],
        ["Mine production"] + [900.0 for _ in range(n)],
        ["Net producer hedging"] + [5.0 for _ in range(n)],
        ["Recycled gold"] + [300.0 for _ in range(n)],
        ["Total Supply"] + [1205.0 for _ in range(n)],
        ["OTC and other"] + [0.0 for _ in range(n)],
        ["LBMA gold price"] + [1200.0 + i * 10 for i in range(n)],
    ]
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        df.to_excel(xl, sheet_name="Supply and Demand", header=False, index=False)
    return path


def _write_legacy_workbook(path: Path, n: int = 48) -> Path:
    labels = _quarter_labels(n, start_year=2010)
    header = ["Sector"] + labels
    rows = [
        header,
        ["Jewellery fabrication"] + [480.0 for _ in range(n)],
        ["Jewellery consumption"] + [470.0 for _ in range(n)],
        ["Technology"] + [75.0 for _ in range(n)],
        ["Total bar and coin"] + [280.0 for _ in range(n)],
        ["ETFs & similar products"] + [20.0 for _ in range(n)],
        ["Central banks & other inst."] + [50.0 for _ in range(n)],
        ["Mine production"] + [850.0 for _ in range(n)],
        ["Recycling"] + [250.0 for _ in range(n)],
        ["Net producer hedging"] + [-5.0 for _ in range(n)],
        ["Surplus / Deficit"] + [15.0 for _ in range(n)],
        ["Average gold price"] + [1100.0 for _ in range(n)],
    ]
    df = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    with pd.ExcelWriter(path, engine="openpyxl") as xl:
        df.to_excel(xl, sheet_name="Table 1", header=False, index=False)
    return path


def test_real_xlsx_detection(tmp_path: Path):
    xlsx = _write_consolidated_workbook(tmp_path / "ok.xlsx", n=8)
    assert is_real_xlsx(xlsx.read_bytes())
    assert detect_bad_download(xlsx.read_bytes()) is None


def test_html_login_response_rejection():
    html = b"<!DOCTYPE html><html><body>Please login to Goldhub</body></html>"
    assert is_real_xlsx(html) is False
    err = detect_bad_download(html, "text/html; charset=UTF-8")
    assert err is not None
    assert "HTML" in err or "authentication" in err.lower() or "login" in err.lower()


def test_html_with_fake_zip_magic_still_rejected_if_html_body():
    # Pure HTML must fail
    assert detect_bad_download(b"<html>login</html>", "text/html") is not None


def test_quarter_parsing():
    assert parse_quarter_label("Q1'10") == "2010-03-31"
    assert parse_quarter_label("Q2'26") == "2026-06-30"
    assert parse_quarter_label("2021 Q3") == "2021-09-30"
    assert parse_quarter_label("2020") is None  # annual skipped


def test_legacy_and_consolidated_schema_mapping(tmp_path: Path):
    legacy = _write_legacy_workbook(tmp_path / "legacy.xlsx", n=48)
    cons = _write_consolidated_workbook(tmp_path / "cons.xlsx", n=64)

    long_l, schema_l, _ = parse_gdt_workbook(legacy)
    long_c, schema_c, _ = parse_gdt_workbook(cons)
    assert schema_l == "legacy"
    assert schema_c == "consolidated"
    assert map_label("Jewellery fabrication") == "jewellery_tonnes"
    assert map_label("Jewellery consumption") == "jewellery_consumption_tonnes"
    assert map_label("ETFs & similar products") == "etf_tonnes"

    wide_l = wide_from_long(long_l, source_file=legacy.name, source_schema=schema_l, priority=120)
    wide_c = wide_from_long(long_c, source_file=cons.name, source_schema=schema_c, priority=200)
    assert len(wide_l) >= 48
    assert len(wide_c) >= 60
    # Fabrication and consumption both present and distinct in legacy
    assert wide_l[0].get("jewellery_tonnes") == 480.0
    assert wide_l[0].get("jewellery_consumption_tonnes") == 470.0


def test_publication_date_alignment(tmp_path: Path):
    path = _write_consolidated_workbook(tmp_path / "pub.xlsx", n=8)
    long_rows, schema, _ = parse_gdt_workbook(path)
    wide = wide_from_long(
        long_rows,
        source_file=path.name,
        source_schema=schema,
        publication_date_by_quarter={"2010-03-31": "2010-05-15"},
        priority=100,
    )
    first = next(r for r in wide if r["quarter"] == "2010-03-31")
    assert first["publication_date"] == "2010-05-15"
    assert first["available_date"] == "2010-05-15"
    # Not equal to quarter-end
    assert first["available_date"] != first["quarter"]


def test_vintage_precedence(tmp_path: Path):
    a = _write_consolidated_workbook(tmp_path / "old.xlsx", n=8)
    b = _write_consolidated_workbook(tmp_path / "new.xlsx", n=8)
    la, sa, _ = parse_gdt_workbook(a)
    lb, sb, _ = parse_gdt_workbook(b)
    wide_a = wide_from_long(la, source_file=a.name, source_schema=sa, priority=100)
    wide_b = wide_from_long(lb, source_file=b.name, source_schema=sb, priority=200)
    # Alter old jewellery for Q1'10
    wide_a[0]["jewellery_tonnes"] = 111.0
    wide_b[0]["jewellery_tonnes"] = 222.0
    canon, _, revisions = merge_vintages([wide_a, wide_b])
    q0 = canon[0]
    assert q0["jewellery_tonnes"] == 222.0
    assert q0["source_file"] == b.name
    assert any(r["field"] == "jewellery_tonnes" for r in revisions)


def test_unit_validation_and_reconciliation():
    row = {
        "quarter": "2020-03-31",
        "jewellery_tonnes": 500.0,
        "technology_tonnes": 80.0,
        "bar_coin_tonnes": 300.0,
        "etf_tonnes": 20.0,
        "central_bank_tonnes": 100.0,
        "other_investment_tonnes": 0.0,
        "total_demand_tonnes": 1000.0,
        "mine_production_tonnes": 900.0,
        "recycling_tonnes": 300.0,
        "producer_hedging_tonnes": 5.0,
        "total_supply_tonnes": 1205.0,
    }
    assert reconcile_row(row) == []


def test_minimum_history_gate(tmp_path: Path):
    short = _write_consolidated_workbook(tmp_path / "short.xlsx", n=20)
    long_rows, schema, _ = parse_gdt_workbook(short)
    wide = wide_from_long(long_rows, source_file=short.name, source_schema=schema, priority=100)
    ok, reason, meta = validate_canonical(wide)
    assert ok is False
    assert meta["n"] < MIN_QUARTERS_HARD
    assert "40" in reason or "Fewer" in reason


def _isolate_bootstrap_paths(monkeypatch, tmp_path: Path) -> None:
    raw = tmp_path / "raw"
    proc = tmp_path / "processed"
    audit = tmp_path / "audits"
    cache = tmp_path / "cache" / "wgc_gdt_sectors.json"
    raw.mkdir()
    proc.mkdir()
    audit.mkdir()
    cache.parent.mkdir(parents=True)
    monkeypatch.setattr("hptl.data_sources.wgc_gdt_xlsx_ingest.RAW_DIR", raw)
    monkeypatch.setattr("hptl.data_sources.wgc_gdt_xlsx_ingest.PROCESSED_DIR", proc)
    monkeypatch.setattr("hptl.data_sources.wgc_gdt_xlsx_ingest.AUDIT_DIR", audit)
    monkeypatch.setattr("hptl.data_sources.wgc_gdt_xlsx_ingest.MANIFEST_PATH", raw / "source_manifest.json")
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.QUARTERLY_CSV",
        proc / "wgc_gold_supply_demand_quarterly.csv",
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.VINTAGES_CSV",
        proc / "wgc_gold_supply_demand_vintages.csv",
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.CB_RESERVES_CSV",
        proc / "wgc_central_bank_reserves_quarterly.csv",
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.REPORT_MD",
        audit / "wgc_gdt_bootstrap_report.md",
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.REVISIONS_CSV",
        audit / "wgc_gdt_revisions.csv",
    )
    monkeypatch.setattr("hptl.data_sources.wgc_gdt_xlsx_ingest.CACHE_JSON", cache)


def test_bootstrap_with_fixture_dir_passes_gate(tmp_path: Path, monkeypatch):
    _isolate_bootstrap_paths(monkeypatch, tmp_path)
    d = tmp_path / "xlsx"
    d.mkdir()
    _write_consolidated_workbook(d / "GDT_Tables_fixture.xlsx", n=64)
    result = run_bootstrap(xlsx_dir=d, try_download=False)
    assert result.ok is True
    assert result.n_quarters >= 60
    assert result.earliest.startswith("2010")
    assert (tmp_path / "processed" / "wgc_gold_supply_demand_quarterly.csv").exists()


def test_bootstrap_fails_loudly_without_xlsx(tmp_path: Path, monkeypatch):
    _isolate_bootstrap_paths(monkeypatch, tmp_path)
    empty = tmp_path / "empty"
    empty.mkdir()
    result = run_bootstrap(xlsx_dir=empty, try_download=False)
    assert result.ok is False
    assert result.error


def test_explicit_xlsx_skips_network(tmp_path: Path, monkeypatch):
    """Valid --xlsx must never attempt Goldhub HTTP."""
    _isolate_bootstrap_paths(monkeypatch, tmp_path)
    xlsx = _write_consolidated_workbook(tmp_path / "local.xlsx", n=64)

    def _boom(*_a, **_k):
        raise AssertionError("HTTP should not be called when --xlsx is valid")

    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.try_download_gdt_xlsx",
        _boom,
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.discover_gdt_xlsx_url",
        _boom,
    )
    monkeypatch.setattr(
        "hptl.data_sources.wgc_gdt_xlsx_ingest.requests.get",
        _boom,
    )
    result = run_bootstrap(xlsx=xlsx, try_download=True)
    assert result.ok is True
    assert result.auth_required is False
    assert result.n_quarters >= 60
