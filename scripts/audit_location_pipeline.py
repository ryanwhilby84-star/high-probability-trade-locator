#!/usr/bin/env python3
"""Audit Location pipeline: snapshot vs chart history per instrument."""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PUBLIC = ROOT / "web-dashboard" / "public" / "data"


def price_percentile(closes: list[float], window: int = 52) -> float | None:
    if len(closes) < 12:
        return None
    window_closes = closes[-window:] if len(closes) >= window else closes
    current = window_closes[-1]
    rank = sum(1 for c in window_closes if c <= current) / len(window_closes)
    return rank * 100.0


def score_from_pct(pct: float) -> float:
    return round(min(10.0, max(0.0, abs(pct - 50.0) / 5.0)), 1)


def main() -> None:
    loc_doc = json.loads((PUBLIC / "location_latest.json").read_text(encoding="utf-8"))
    cot_doc = json.loads((PUBLIC / "cot_3y_series_latest.json").read_text(encoding="utf-8"))
    conf_doc = json.loads((PUBLIC / "confluence_history_latest.json").read_text(encoding="utf-8"))
    conf_rows = conf_doc.get("records") or conf_doc.get("rows") or []
    markets = cot_doc.get("markets") or {}

    lines = [
        "# Location pipeline audit",
        "",
        "| Market | Snapshot source | Snapshot pct/score | Price weeks (COT) | "
        "Location history pts | First hist date | Last hist date | "
        "Confluence hist scores | Chart would render |",
        "|--------|-----------------|--------------------|--------------------|"
        "----------------------|-----------------|----------------|"
        "------------------------|-------------------|",
    ]

    for market, block in sorted(markets.items()):
        snap = (loc_doc.get("instruments") or {}).get(market) or {}
        series = block.get("series") or []
        closes: list[float] = []
        hist_pts = 0
        first_d = last_d = None
        for pt in series:
            p = pt.get("price")
            if isinstance(p, (int, float)) and p == p:
                closes.append(float(p))
            pct = price_percentile(closes)
            if pct is not None:
                hist_pts += 1
                d = str(pt.get("date") or "")[:10]
                if not first_d:
                    first_d = d
                last_d = d

        mrows = [r for r in conf_rows if r.get("market") == market]
        conf_scores = sum(
            1
            for r in mrows
            if r.get("location_score") is not None
            or (r.get("valuation_wired") and r.get("valuation_score") is not None)
        )

        snap_pct = snap.get("price_percentile_52w")
        snap_score = snap.get("location_score")
        snap_src = "location_latest.json" if snap.get("wired") else "—"
        price_weeks = len(closes)
        renders = hist_pts > 0

        lines.append(
            f"| {market} | {snap_src} | "
            f"{snap_pct if snap_pct is not None else '—'}/{snap_score if snap_score is not None else '—'} | "
            f"{price_weeks} ({series[0]['date'][:7] if series else '—'}→{series[-1]['date'][:7] if series else '—'}) | "
            f"{hist_pts} | {first_d or '—'} | {last_d or '—'} | {conf_scores} | "
            f"{'YES' if renders else 'NO'} |"
        )

    out = ROOT / "web-dashboard" / "LOCATION_PIPELINE_AUDIT.md"
    out.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {out} ({len(markets)} instruments)")


if __name__ == "__main__":
    main()
