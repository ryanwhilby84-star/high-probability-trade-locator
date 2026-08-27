"""Audit seasonality confidence labels for all instruments (mirrors UI interpret layer)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
EXPORT = ROOT / "web-dashboard" / "public" / "data" / "seasonality_price_latest.json"
OUT_JSON = ROOT / "data" / "audits" / "seasonality_confidence_clarity_audit.json"
OUT_MD = ROOT / "data" / "audits" / "seasonality_confidence_clarity_audit.md"

RELIABILITY_LABELS = frozenset({"Low sample reliability", "Insufficient history"})


def _is_num(v: Any) -> bool:
    return isinstance(v, (int, float)) and v == v


def _fmt_pct(v: Any) -> str:
    if not _is_num(v):
        return "—"
    x = float(v)
    return f"{x:+.2f}%"


def _path_agreement_level(confidence: dict[str, Any] | None) -> str:
    level = str((confidence or {}).get("level") or "")
    if level == "Strong":
        return "Strong"
    if level == "Medium":
        return "Medium"
    if level in {"Low sample reliability", "Insufficient history"}:
        return "Low"
    return "Weak"


def _data_quality(block: dict[str, Any]) -> tuple[str, str]:
    grade = block.get("trust_grade") or "C"
    years = block.get("years_used") or block.get("years_of_history") or "—"
    sample = (block.get("forward_read") or {}).get("next_8w", {}).get("sample_years")
    if sample is None:
        sample = block.get("sample_size")
    warning = block.get("data_quality_warning")

    if grade == "C":
        return "Low", f"Low — {block.get('trust_notes') or block.get('reason') or 'Insufficient history'}."

    level = "Low"
    if grade == "A" and not warning:
        level = "High"
    elif grade in {"A", "B"}:
        level = "Medium"

    parts = [f"{years} years", f"trust grade {grade}"]
    if sample is not None:
        parts.append(f"n={sample}")
    summary = f"{level} — {', '.join(str(p) for p in parts)}."
    if warning:
        summary += f" Warning: {warning}."
    return level, summary


def _path_agreement(block: dict[str, Any]) -> tuple[str, str]:
    cb = block.get("confidence") or {}
    raw = _path_agreement_level(cb)
    display = {"Strong": "Strong", "Medium": "Medium", "Weak": "Weak", "Low": "Low"}.get(raw, "Weak")
    detail = cb.get("detail") or "Path agreement unavailable."
    return display, f"{display} — {detail}"


def _horizon_confidence(row: dict[str, Any] | None, path_raw: str) -> str:
    if not row or not row.get("available"):
        return "—"
    n = int(row.get("sample_years") or 0)
    if n < 5 or str(row.get("direction") or "") in RELIABILITY_LABELS:
        return "Low"
    overall = "High" if path_raw == "Strong" else "Medium" if path_raw == "Medium" else "Low"
    wr = row.get("win_rate_pct")
    avg = row.get("avg_return_pct")
    if overall == "High" and n >= 8 and _is_num(wr) and (wr >= 60 or wr <= 40):
        return "High"
    if n >= 7 and _is_num(avg) and abs(float(avg)) >= 0.3:
        return "Medium"
    return overall if overall != "High" else "Low"


def _forward_window(block: dict[str, Any], weeks: int, path_raw: str) -> tuple[str, str]:
    key = f"{weeks}W"
    grade = block.get("trust_grade") or "C"
    row = ((block.get("forward_read") or {}).get(f"next_{weeks}w")) or {}
    if grade == "C" or not row.get("available") or int(row.get("sample_years") or 0) < 5:
        return "Low", f"{key}: Unreliable — insufficient sample (n={row.get('sample_years', 0)})."
    conf = _horizon_confidence(row, path_raw)
    direction = str(row.get("direction") or "Neutral")
    wr = row.get("win_rate_pct")
    wr_s = f"{wr}%" if _is_num(wr) else "—"
    n = row.get("sample_years", "—")
    return conf, (
        f"{key}: {direction}, {conf} confidence — "
        f"avg {_fmt_pct(row.get('avg_return_pct'))}, win rate {wr_s}, n={n}."
    )


def _trade_usefulness(block: dict[str, Any], path_raw: str) -> str:
    grade = block.get("trust_grade") or "C"
    f8 = ((block.get("forward_read") or {}).get("next_8w")) or {}
    f4 = ((block.get("forward_read") or {}).get("next_4w")) or {}
    f12 = ((block.get("forward_read") or {}).get("next_12w")) or {}

    if not block.get("available") or grade == "C":
        return "Seasonality not reliable enough to support a trade thesis."
    if not f8.get("available") or int(f8.get("sample_years") or 0) < 5:
        return "Forward sample too thin — seasonality does not support a directional thesis."
    if grade == "B":
        return "Context only — sparse curve; use as background, not a primary thesis driver."

    d8 = str(f8.get("direction") or "Neutral")
    dirs = []
    for row in (f4, f8, f12):
        if row.get("available") and str(row.get("direction") or "") not in RELIABILITY_LABELS:
            dirs.append(str(row.get("direction")))
    unique = list(dict.fromkeys(dirs))

    if len(unique) > 1:
        thesis = "Seasonality mixed across forward windows"
    elif d8 == "Bullish":
        thesis = "Seasonality supports longs"
    elif d8 == "Bearish":
        thesis = "Seasonality supports shorts"
    else:
        thesis = "Seasonality neutral"

    if path_raw == "Strong":
        qual = "Path agreement is strong."
    elif path_raw == "Medium":
        qual = "Path agreement is moderate — not all windows align."
    else:
        qual = "Path agreement is weak — windows disagree even if one read looks usable."
    return f"{thesis}; {qual}"


def audit_market(block: dict[str, Any]) -> dict[str, Any]:
    path_raw = _path_agreement_level(block.get("confidence"))
    data_level, data_summary = _data_quality(block)
    path_level, path_summary = _path_agreement(block)
    w4_level, w4_summary = _forward_window(block, 4, path_raw)
    w8_level, w8_summary = _forward_window(block, 8, path_raw)
    w12_level, w12_summary = _forward_window(block, 12, path_raw)
    usefulness = _trade_usefulness(block, path_raw)
    return {
        "instrument": block.get("market"),
        "available": bool(block.get("available")),
        "data_confidence": data_level,
        "data_summary": data_summary,
        "path_agreement_confidence": path_level,
        "path_agreement_summary": path_summary,
        "forward_4w_confidence": w4_level,
        "forward_4w_summary": w4_summary,
        "forward_8w_confidence": w8_level,
        "forward_8w_summary": w8_summary,
        "forward_12w_confidence": w12_level,
        "forward_12w_summary": w12_summary,
        "trade_usefulness": usefulness,
        "trust_grade": block.get("trust_grade"),
        "legacy_top_confidence": (block.get("confidence") or {}).get("level"),
    }


def main() -> None:
    doc = json.loads(EXPORT.read_text(encoding="utf-8"))
    markets = doc.get("markets") or {}
    rows = [audit_market(block) for block in markets.values()]
    rows.sort(key=lambda r: str(r.get("instrument") or ""))

    payload = {
        "generated_at": doc.get("generated_at"),
        "source": str(EXPORT.relative_to(ROOT)),
        "instrument_count": len(rows),
        "definitions": {
            "data_confidence": "Source depth, cleanliness, trust grade (A/B/C).",
            "path_agreement_confidence": "Agreement between 3Y/5Y/10Y seasonal forward directions (Strong/Medium/Weak).",
            "forward_Nw_confidence": "Reliability of each forward return window (sample, win rate, avg move).",
            "trade_usefulness": "Whether seasonality supports longs/shorts/mixed/neutral plus path agreement qualifier.",
        },
        "instruments": rows,
    }
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# Seasonality confidence clarity audit",
        "",
        f"Generated: {payload['generated_at']}",
        f"Source: `{payload['source']}`",
        "",
        "## Label definitions",
        "",
        "- **Data confidence** — price history depth, bar density, trust grade, data-quality warnings.",
        "- **Path agreement** — do 3Y / 5Y / 10Y seasonal paths agree on forward direction?",
        "- **Forward window confidence** — reliability of each 4W / 8W / 12W historical return read.",
        "- **Trade usefulness** — directional support for thesis + path agreement qualifier.",
        "",
        "| Instrument | Data | Path agreement | 4W | 8W | 12W | Trade usefulness |",
        "|---|---|---|---|---|---|---|",
    ]
    for r in rows:
        use = r["trade_usefulness"]
        if len(use) > 72:
            use = use[:69] + "..."
        lines.append(
            f"| {r['instrument']} | {r['data_confidence']} | {r['path_agreement_confidence']} | "
            f"{r['forward_4w_confidence']} | {r['forward_8w_confidence']} | {r['forward_12w_confidence']} | {use} |"
        )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(f"Wrote {OUT_JSON} ({len(rows)} instruments)")
    print(f"Wrote {OUT_MD}")


if __name__ == "__main__":
    main()
