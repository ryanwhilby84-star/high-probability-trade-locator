"""Export Seasonality Workstation research packs + audit reports."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.seasonality_workstation.engine import build_seasonality_research
from hptl.seasonality_workstation.models import DEFAULT_LOOKBACK, ENGINE_VERSION

CANONICAL = PROCESSED_DIR / "seasonality_workstation_latest.json"
PUBLIC = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_workstation_latest.json"
DATA = PROJECT_ROOT / "data" / "seasonality_workstation_latest.json"
AUDIT_JSON = PROJECT_ROOT / "data" / "audits" / "seasonality_workstation_v1_audit.json"
AUDIT_MD = PROJECT_ROOT / "data" / "audits" / "seasonality_workstation_v1_audit.md"

DEFAULT_AUDIT_INSTRUMENTS = (
    "Gold",
    "Crude Oil / CL",
    "Copper / HG",
    "Soybeans",
    "Corn",
    "Natural Gas / NG",
    "Euro FX / 6E",
    "Coffee",
)


def _write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def run_seasonality_workstation_export(
    instruments: list[str] | None = None,
    *,
    lookback: str = DEFAULT_LOOKBACK,
) -> dict[str, Any]:
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS

    ids = list(instruments) if instruments else list(LEGACY_COT_MARKETS)
    markets: dict[str, Any] = {}
    for mid in ids:
        markets[mid] = build_seasonality_research(
            mid, lookback=lookback, fail_on_integrity=True
        )

    payload = {
        "version": ENGINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "default_lookback": lookback,
        "markets": markets,
        "summary": {
            "markets_total": len(markets),
            "markets_ok": sum(1 for m in markets.values() if m.get("status") == "ok"),
            "markets_fail": sum(1 for m in markets.values() if m.get("status") != "ok"),
        },
    }
    for path in (CANONICAL, PUBLIC, DATA):
        _write(path, payload)
    return payload


def run_seasonality_workstation_audit(
    instruments: tuple[str, ...] | list[str] | None = None,
) -> dict[str, Any]:
    ids = list(instruments) if instruments else list(DEFAULT_AUDIT_INSTRUMENTS)
    rows: list[dict[str, Any]] = []
    for mid in ids:
        # Allow audit to capture FAIL details (still fail loudly in status)
        research = build_seasonality_research(mid, lookback="10Y", fail_on_integrity=True)
        if research.get("status") != "ok":
            # Also try without hard fail to surface integrity diagnostics
            soft = build_seasonality_research(mid, lookback="10Y", fail_on_integrity=False)
            rows.append(
                {
                    "instrument_id": mid,
                    "status": research.get("status"),
                    "error": research.get("error"),
                    "integrity": research.get("integrity") or soft.get("integrity"),
                    "message": research.get("message"),
                }
            )
            continue

        lb = research.get("lookbacks") or {}
        comparison = {}
        for key in ("5Y", "10Y", "20Y", "FULL"):
            pack = lb.get(key) or {}
            h8 = (pack.get("forward_horizons") or {}).get("8w") or {}
            comparison[key] = {
                "sample_size": pack.get("sample_size"),
                "avg_8w_return_pct": None
                if h8.get("mean_return") is None
                else round(h8["mean_return"] * 100, 2),
                "positive_years_pct": None
                if h8.get("positive_frequency") is None
                else round(h8["positive_frequency"] * 100, 1),
                "dispersion": h8.get("dispersion"),
            }

        seas = research.get("seasonality") or {}
        rows.append(
            {
                "instrument_id": mid,
                "status": "ok",
                "report_date": research.get("report_date"),
                "data_quality": research.get("data_quality"),
                "integrity": {
                    "status": (research.get("integrity") or {}).get("status"),
                    "available_history_years": (research.get("integrity") or {}).get(
                        "available_history_years"
                    ),
                    "usable_year_count": (research.get("integrity") or {}).get(
                        "usable_year_count"
                    ),
                    "source": (research.get("integrity") or {}).get("source"),
                    "issues": (research.get("integrity") or {}).get("issues"),
                    "warnings": (research.get("integrity") or {}).get("warnings"),
                },
                "sample_size": research.get("sample_size"),
                "confidence": research.get("confidence"),
                "stats_panel": research.get("stats_panel"),
                "projection_tail": (seas.get("projection") or [])[:3]
                + (seas.get("projection") or [])[-2:],
                "turning_windows": research.get("turning_windows"),
                "lookback_comparison": comparison,
                "lookback_agreement": research.get("lookback_agreement"),
            }
        )

    report = {
        "version": ENGINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "instruments": rows,
        "summary": {
            "total": len(rows),
            "ok": sum(1 for r in rows if r.get("status") == "ok"),
            "fail": sum(1 for r in rows if r.get("status") != "ok"),
        },
    }
    _write(AUDIT_JSON, report)
    AUDIT_MD.write_text(_render_audit_md(report), encoding="utf-8")
    return report


def _render_audit_md(report: dict[str, Any]) -> str:
    lines = [
        "# Seasonality Workstation V1 — Engine Audit",
        "",
        f"Generated: `{report.get('generated_at')}`",
        f"Engine: `{report.get('version')}`",
        f"Summary: ok={report['summary']['ok']} / fail={report['summary']['fail']} / total={report['summary']['total']}",
        "",
    ]
    for r in report.get("instruments") or []:
        lines.append(f"## {r['instrument_id']}")
        lines.append("")
        if r.get("status") != "ok":
            lines.append(f"- **STATUS: FAIL** — `{r.get('error')}`")
            lines.append(f"- Integrity: `{json.dumps(r.get('integrity'), indent=2)[:800]}`")
            lines.append("")
            continue
        integ = r.get("integrity") or {}
        conf = r.get("confidence") or {}
        panel = r.get("stats_panel") or {}
        lines.append(f"- Report date: `{r.get('report_date')}`")
        lines.append(
            f"- History: `{integ.get('available_history_years')}y` usable years "
            f"`{integ.get('usable_year_count')}` source `{integ.get('source')}` quality `{r.get('data_quality')}`"
        )
        lines.append(
            f"- Confidence: **{conf.get('label')}** (composite `{conf.get('composite')}`) "
            f"sample `{conf.get('sample_size')}` dispersion `{conf.get('dispersion')}`"
        )
        lines.append(
            f"- Bias: **{panel.get('current_seasonal_bias')}** | "
            f"4W `{panel.get('average_4w_return_pct')}%` | "
            f"8W `{panel.get('average_8w_return_pct')}%` | "
            f"12W `{panel.get('average_12w_return_pct')}%` | "
            f"+years `{panel.get('positive_years_pct')}%`"
        )
        lines.append("- Lookback comparison (avg 8W return):")
        for k, pack in (r.get("lookback_comparison") or {}).items():
            lines.append(
                f"  - `{k}`: n={pack.get('sample_size')} "
                f"8W={pack.get('avg_8w_return_pct')}% "
                f"+%={pack.get('positive_years_pct')}"
            )
        turns = r.get("turning_windows") or []
        lines.append(f"- Turning windows: {len(turns)}")
        for t in turns[:4]:
            lines.append(
                f"  - {t.get('kind')} {t.get('window', {}).get('label')} "
                f"hit={t.get('hit_rate')} avg8W={t.get('average_follow_return_pct')}% "
                f"conf={t.get('confidence')} n={t.get('sample_years')}"
            )
        lines.append("")
    lines.append("## Gate")
    lines.append("")
    lines.append(
        "Engine validation is numerical. UI polish should proceed only after FAIL instruments "
        "are understood (integrity issues) and OK instruments show stable lookback agreement."
    )
    lines.append("")
    return "\n".join(lines)
