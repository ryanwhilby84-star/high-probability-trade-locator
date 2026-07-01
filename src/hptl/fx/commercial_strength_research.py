"""Commercial strength research layer — independent of Relative Strength scoring.

Reads Legacy COT commercial positioning and existing RS exports for side-by-side
research. Does not modify scanner, confluence, or RS formulas.
"""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.cot.legacy_cot_loader import load_legacy_cot_document
from hptl.cot.positioning_percentiles import (
    WINDOW_WEEKS_3Y,
    empirical_percentile_rank,
)
from hptl.fx.currency_map import COT_CURRENCY_SOURCES
from hptl.fx.relative_strength import RELATIVE_STRENGTH_PATH

COMMERCIAL_STRENGTH_PATH = Path("data/commercial_strength_latest.json")
PUBLIC_COMMERCIAL_STRENGTH_PATH = Path("web-dashboard/public/data/commercial_strength_latest.json")
DIVERGENCE_PATH = Path("data/commercial_spec_divergence_latest.json")
PUBLIC_DIVERGENCE_PATH = Path("web-dashboard/public/data/commercial_spec_divergence_latest.json")
AUDIT_PATH = Path("data/audits/commercial_strength_research.md")

TREND_WEEKS = 13
EXTREME_HIGH_PCT = 90.0
EXTREME_LOW_PCT = 10.0

RESEARCH_CURRENCIES: tuple[str, ...] = tuple(COT_CURRENCY_SOURCES.keys())


def _finite(v: Any) -> float | None:
    try:
        x = float(v)
        return x if math.isfinite(x) else None
    except (TypeError, ValueError):
        return None


def _orient(value: float | None, invert_cot: bool) -> float | None:
    if value is None:
        return None
    return -value if invert_cot else value


def _extreme_label(percentile: float | None) -> str:
    if percentile is None or not math.isfinite(percentile):
        return "UNAVAILABLE"
    if percentile >= EXTREME_HIGH_PCT:
        return "HIGH"
    if percentile <= EXTREME_LOW_PCT:
        return "LOW"
    return "NEUTRAL"


def _commercial_score(percentile: float | None) -> int | None:
    if percentile is None or not math.isfinite(percentile):
        return None
    raw = (float(percentile) - 50.0) * 2.0
    return int(round(max(-100.0, min(100.0, raw))))


def _sorted_commercial_weeks(instrument: dict[str, Any] | None) -> list[dict[str, Any]]:
    if not instrument:
        return []
    weeks = (instrument.get("groups") or {}).get("commercials", {}).get("weeks") or []
    if not isinstance(weeks, list):
        return []
    return sorted(weeks, key=lambda w: str(w.get("report_date") or ""))


def compute_commercial_currency_metrics(
    weeks: list[dict[str, Any]],
    *,
    invert_cot: bool,
) -> dict[str, Any] | None:
    if not weeks:
        return None

    oriented_nets: list[float] = []
    for w in weeks:
        net = _orient(_finite(w.get("net")), invert_cot)
        if net is not None:
            oriented_nets.append(net)
    if not oriented_nets:
        return None

    latest_week = weeks[-1]
    current_net = _orient(_finite(latest_week.get("net")), invert_cot)
    if current_net is None:
        return None

    window = oriented_nets[-WINDOW_WEEKS_3Y:]
    percentile = empirical_percentile_rank(window, current_net)
    pct = None if percentile != percentile else round(float(percentile), 1)

    trend_13w: float | None = None
    if len(oriented_nets) > TREND_WEEKS:
        prior = oriented_nets[-(TREND_WEEKS + 1)]
        trend_13w = round(current_net - prior, 1)

    weekly_raw = _finite(latest_week.get("net_week_change"))
    if weekly_raw is None:
        weekly_raw = _finite(latest_week.get("long_week_change"))
    weekly_change = _orient(weekly_raw, invert_cot)
    weekly_change_out = None if weekly_change is None else int(round(weekly_change))

    score = _commercial_score(pct)

    return {
        "commercial_score": score,
        "percentile": pct,
        "trend_13w": trend_13w,
        "weekly_change": weekly_change_out,
        "extreme": _extreme_label(pct),
        "report_date": str(latest_week.get("report_date") or "")[:10],
        "commercial_net_oriented": round(current_net, 1),
    }


def build_commercial_strength(
    legacy_doc: dict[str, Any] | None = None,
    *,
    calendar_week: str = "",
) -> dict[str, Any]:
    doc = legacy_doc or load_legacy_cot_document()
    instruments = doc.get("instruments") or {}
    out: dict[str, Any] = {}

    for code, meta in COT_CURRENCY_SOURCES.items():
        market = str(meta["market"])
        invert = bool(meta.get("invert_cot"))
        weeks = _sorted_commercial_weeks(instruments.get(market))
        metrics = compute_commercial_currency_metrics(weeks, invert_cot=invert)
        if metrics:
            out[code] = {
                **metrics,
                "cot_market": market,
                "invert_cot_applied": invert,
            }
        else:
            out[code] = {
                "commercial_score": None,
                "percentile": None,
                "trend_13w": None,
                "weekly_change": None,
                "extreme": "UNAVAILABLE",
                "cot_market": market,
                "invert_cot_applied": invert,
                "reason": "No commercial COT weeks for this currency.",
            }

    return {
        "calendar_week": calendar_week,
        "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        "generated_from": "hptl.fx.commercial_strength_research",
        "research_only": True,
        "score_formula": "(percentile - 50) * 2, clamped [-100, 100]",
        "percentile_window_weeks": WINDOW_WEEKS_3Y,
        "currencies": out,
    }


def _spec_scores_from_relative_strength(rs_doc: dict[str, Any] | None) -> dict[str, float | None]:
    scores: dict[str, float | None] = {code: None for code in RESEARCH_CURRENCIES}
    if not rs_doc:
        return scores

    for row in (rs_doc.get("relative_strength") or {}).get("leaderboard") or []:
        code = str(row.get("currency") or "").upper()
        if code not in scores:
            continue
        # Positioning-only leg score (COT + flow + anomaly) for positioning-vs-positioning research.
        val = _finite(row.get("positioning_score"))
        if val is None:
            val = _finite(row.get("raw_rs"))
        if val is None:
            val = _finite(row.get("final_score"))
        scores[code] = round(val, 1) if val is not None else None
    return scores


def build_commercial_spec_divergence(
    commercial_doc: dict[str, Any],
    rs_doc: dict[str, Any] | None = None,
) -> dict[str, Any]:
    spec_scores = _spec_scores_from_relative_strength(rs_doc)
    out: dict[str, Any] = {}

    for code in RESEARCH_CURRENCIES:
        comm = (commercial_doc.get("currencies") or {}).get(code) or {}
        commercial_score = comm.get("commercial_score")
        spec_score = spec_scores.get(code)
        divergence: float | None = None
        if commercial_score is not None and spec_score is not None:
            divergence = round(float(commercial_score) - float(spec_score), 1)

        out[code] = {
            "spec_score": spec_score,
            "commercial_score": commercial_score,
            "divergence": divergence,
            "cot_market": comm.get("cot_market"),
        }

    return {
        "calendar_week": commercial_doc.get("calendar_week") or (rs_doc or {}).get("calendar_week") or "",
        "generated_at": commercial_doc.get("generated_at"),
        "generated_from": "hptl.fx.commercial_strength_research",
        "research_only": True,
        "spec_score_source": "relative_strength.positioning_score",
        "divergence_formula": "commercial_score - spec_score",
        "currencies": out,
    }


def build_research_table_rows(
    commercial_doc: dict[str, Any],
    divergence_doc: dict[str, Any],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for code in RESEARCH_CURRENCIES:
        div = (divergence_doc.get("currencies") or {}).get(code) or {}
        divergence = div.get("divergence")
        rows.append(
            {
                "currency": code,
                "spec_strength": div.get("spec_score"),
                "commercial_strength": div.get("commercial_score"),
                "divergence": divergence,
                "abs_divergence": abs(float(divergence)) if divergence is not None else -1.0,
            }
        )
    rows.sort(key=lambda r: r["abs_divergence"], reverse=True)
    return rows


def _load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def write_commercial_strength_research(
    *,
    legacy_doc: dict[str, Any] | None = None,
    rs_doc: dict[str, Any] | None = None,
    calendar_week: str = "",
) -> dict[str, Path]:
    rs = rs_doc or _load_json(RELATIVE_STRENGTH_PATH)
    week = calendar_week or (rs or {}).get("calendar_week") or ""

    commercial_doc = build_commercial_strength(legacy_doc, calendar_week=week)
    divergence_doc = build_commercial_spec_divergence(commercial_doc, rs)

    COMMERCIAL_STRENGTH_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_COMMERCIAL_STRENGTH_PATH.parent.mkdir(parents=True, exist_ok=True)
    DIVERGENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_DIVERGENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    AUDIT_PATH.parent.mkdir(parents=True, exist_ok=True)

    commercial_text = json.dumps(commercial_doc, indent=2, ensure_ascii=False)
    divergence_text = json.dumps(divergence_doc, indent=2, ensure_ascii=False)
    COMMERCIAL_STRENGTH_PATH.write_text(commercial_text, encoding="utf-8")
    PUBLIC_COMMERCIAL_STRENGTH_PATH.write_text(commercial_text, encoding="utf-8")
    DIVERGENCE_PATH.write_text(divergence_text, encoding="utf-8")
    PUBLIC_DIVERGENCE_PATH.write_text(divergence_text, encoding="utf-8")

    audit_md = render_research_audit(commercial_doc, divergence_doc)
    AUDIT_PATH.write_text(audit_md, encoding="utf-8")

    return {
        "commercial_strength": COMMERCIAL_STRENGTH_PATH,
        "divergence": DIVERGENCE_PATH,
        "audit": AUDIT_PATH,
    }


def render_research_audit(
    commercial_doc: dict[str, Any],
    divergence_doc: dict[str, Any],
) -> str:
    currencies = commercial_doc.get("currencies") or {}
    div_currencies = divergence_doc.get("currencies") or {}

    scored = [
        (code, currencies[code])
        for code in RESEARCH_CURRENCIES
        if code in currencies and currencies[code].get("commercial_score") is not None
    ]
    strongest = sorted(scored, key=lambda x: x[1]["commercial_score"], reverse=True)[:3]
    weakest = sorted(scored, key=lambda x: x[1]["commercial_score"])[:3]

    div_rows = [
        (code, div_currencies.get(code) or {})
        for code in RESEARCH_CURRENCIES
        if (div_currencies.get(code) or {}).get("divergence") is not None
    ]
    pos_div = sorted(div_rows, key=lambda x: x[1]["divergence"], reverse=True)[:3]
    neg_div = sorted(div_rows, key=lambda x: x[1]["divergence"])[:3]

    lines = [
        "# Commercial Strength Research Audit",
        "",
        f"- Calendar week: **{commercial_doc.get('calendar_week') or '—'}**",
        f"- Generated: `{commercial_doc.get('generated_at') or '—'}`",
        "- Layer: **research only** — no scanner / RS / confluence changes",
        "",
        "## Strongest commercial currencies (by commercial_score)",
        "",
    ]
    for code, row in strongest:
        lines.append(
            f"- **{code}**: score {row.get('commercial_score')}, "
            f"percentile {row.get('percentile')}, extreme {row.get('extreme')}"
        )

    lines.extend(["", "## Weakest commercial currencies", ""])
    for code, row in weakest:
        lines.append(
            f"- **{code}**: score {row.get('commercial_score')}, "
            f"percentile {row.get('percentile')}, extreme {row.get('extreme')}"
        )

    lines.extend(["", "## Largest positive divergences (commercial stronger than spec)", ""])
    for code, row in pos_div:
        lines.append(
            f"- **{code}**: divergence {row.get('divergence'):+.1f} "
            f"(commercial {row.get('commercial_score')}, spec {row.get('spec_score')})"
        )

    lines.extend(["", "## Largest negative divergences (spec stronger than commercial)", ""])
    for code, row in neg_div:
        lines.append(
            f"- **{code}**: divergence {row.get('divergence'):+.1f} "
            f"(commercial {row.get('commercial_score')}, spec {row.get('spec_score')})"
        )

    lines.extend(
        [
            "",
            "## Research table (|divergence| desc)",
            "",
            "| Currency | Spec Strength | Commercial Strength | Divergence |",
            "| --- | ---: | ---: | ---: |",
        ]
    )
    for row in build_research_table_rows(commercial_doc, divergence_doc):
        spec = row["spec_strength"]
        comm = row["commercial_strength"]
        div = row["divergence"]
        lines.append(
            f"| {row['currency']} | {spec if spec is not None else '—'} | "
            f"{comm if comm is not None else '—'} | "
            f"{f'{div:+.1f}' if div is not None else '—'} |"
        )

    lines.append("")
    return "\n".join(lines)
