"""Run USDA balance sheet ingest + agri valuation export."""

from __future__ import annotations

from hptl.valuation.agri_balance_sheet_ingest import (
    INGEST_AUDIT_MD,
    ingest_priority_balance_sheets,
    render_ingest_audit_md,
)
from hptl.valuation.agri_fundamental_valuation import build_all_agri_valuations
from hptl.valuation.agri_valuation_export import merge_agri_into_valuation_latest, write_agri_valuation_exports
from hptl.valuation.export import build_valuation_latest, write_valuation_exports


def main() -> None:
    ingest_report = ingest_priority_balance_sheets()
    print(f"Ingest written: {', '.join(ingest_report.get('markets_written') or []) or 'none'}")
    if ingest_report.get("markets_failed"):
        print(f"Ingest failed: {', '.join(ingest_report['markets_failed'])}")

    agri_doc = build_all_agri_valuations()
    write_agri_valuation_exports()
    merged = merge_agri_into_valuation_latest(build_valuation_latest(), agri=agri_doc)
    write_valuation_exports(merged)

    INGEST_AUDIT_MD.parent.mkdir(parents=True, exist_ok=True)
    INGEST_AUDIT_MD.write_text(
        render_ingest_audit_md(ingest_report, valuation_payload=agri_doc),
        encoding="utf-8",
    )
    print(f"Wrote ingest audit: {INGEST_AUDIT_MD}")
    print(f"Agri wired: {agri_doc.get('summary', {}).get('wired_count', 0)}/{len(agri_doc.get('instruments') or {})}")


if __name__ == "__main__":
    main()
