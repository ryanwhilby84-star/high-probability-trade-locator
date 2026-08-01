"""Full-universe integrity audit (Phases 1–7) for LEGACY_COT_MARKETS.

Gate: no new feature work until every instrument PASSes.
"""

from __future__ import annotations

import json
import random
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.cot.legacy_cot import CANONICAL_LEGACY_CODE
from hptl.markets.canonical_identity import (
    BY_ID,
    CANONICAL_INSTRUMENTS,
    assert_universe_complete,
    canonical_cftc_codes,
)
from hptl.markets.instrument_registry import (
    LEGACY_COT_MARKETS,
    load_registry,
)
from hptl.prices.canonical_timeline import OANDA_PRICE_FALLBACK

ROOT = Path(__file__).resolve().parents[3]
DATA = ROOT / "data"
PUBLIC = ROOT / "web-dashboard" / "public" / "data"
OUT_JSON = DATA / "audits" / "universe_integrity_audit.json"
OUT_MD = DATA / "audits" / "universe_integrity_audit.md"
PUBLIC_JSON = PUBLIC / "universe_integrity_audit.json"

# Maximum permitted calendar days between latest COT report date and the matched
# price bar used for that week. Covers a long weekend; beyond this the history
# is stale and must FAIL (never warn).
MAX_PRICE_COT_LAG_DAYS = 5

# Legacy OANDA forms still seen in registry / stores (must converge to canonical).
LEGACY_OANDA_ALIASES: dict[str, str] = {
    "NAS100USD": "NAS100_USD",
    "SPX500USD": "SPX500_USD",
    "US30USD": "US30_USD",
    "BTCUSD": "BTC_USD",
}


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _finite(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


@dataclass
class PhaseResult:
    status: str  # pass | warn | fail
    issues: list[str] = field(default_factory=list)
    details: dict[str, Any] = field(default_factory=dict)

    def bump(self, level: str, msg: str) -> None:
        self.issues.append(msg)
        order = {"pass": 0, "warn": 1, "fail": 2}
        if order[level] > order[self.status]:
            self.status = level


def _mark(ok: bool, warn: bool = False) -> str:
    if ok and not warn:
        return "pass"
    if warn and ok:
        return "warn"
    return "fail"


def audit_identity(instrument_id: str) -> PhaseResult:
    res = PhaseResult(status="pass")
    canon = BY_ID.get(instrument_id)
    if not canon:
        res.bump("fail", "missing from CANONICAL_INSTRUMENTS")
        return res

    reg = load_registry().get(instrument_id)
    if not reg:
        res.bump("fail", "missing from instrument registry")
        return res

    if reg.display_name != canon.display_name:
        res.bump("fail", f"display_name mismatch registry={reg.display_name!r}")
    if reg.id != canon.instrument_id:
        res.bump("fail", f"id mismatch registry={reg.id!r}")
    if reg.asset_class != canon.asset_class:
        res.bump("fail", f"asset_class mismatch registry={reg.asset_class!r}")

    # CFTC code must be the real contract code — never an OANDA symbol.
    expected_code = canon.cftc_market_code
    legacy_code = CANONICAL_LEGACY_CODE.get(instrument_id)
    if not legacy_code:
        res.bump("fail", "missing from CANONICAL_LEGACY_CODE")
    elif legacy_code != expected_code:
        res.bump("fail", f"CANONICAL_LEGACY_CODE={legacy_code} != identity={expected_code}")

    reg_code = str(reg.cot_market_code or "")
    if reg_code != expected_code:
        res.bump(
            "fail",
            f"registry.cot_market_code={reg_code!r} must equal CFTC code {expected_code}",
        )

    # Price provider symbol
    expected_px = canon.price_provider_symbol
    reg_oanda = reg.oanda_symbol
    fallback = OANDA_PRICE_FALLBACK.get(instrument_id)
    if canon.price_provider == "yahoo":
        if not expected_px:
            res.bump("fail", "yahoo provider missing futures symbol")
        elif reg_oanda:
            res.bump("fail", f"soft/yahoo instrument must not carry oanda_symbol={reg_oanda!r}")
    elif expected_px:
        candidates = {expected_px, LEGACY_OANDA_ALIASES.get(expected_px, expected_px)}
        if reg_oanda and reg_oanda not in candidates and LEGACY_OANDA_ALIASES.get(reg_oanda) != expected_px:
            if LEGACY_OANDA_ALIASES.get(reg_oanda) == expected_px or reg_oanda.replace(
                "_", ""
            ) == expected_px.replace("_", ""):
                res.bump(
                    "warn",
                    f"registry.oanda_symbol legacy form {reg_oanda!r} (want {expected_px})",
                )
            else:
                res.bump("fail", f"registry.oanda_symbol={reg_oanda!r} want {expected_px}")
        elif not reg_oanda and fallback != expected_px:
            if fallback == expected_px:
                res.bump("warn", "registry.oanda_symbol null; relying on OANDA_PRICE_FALLBACK")
            else:
                res.bump("fail", f"no price symbol wired (want {expected_px})")
    else:
        if reg_oanda:
            res.bump("warn", f"unexpected oanda_symbol={reg_oanda!r} for fred/none instrument")

    # Proxy duplicates must not be in the primary universe
    if reg.cot_proxy_of:
        res.bump("fail", f"primary universe instrument is a proxy of {reg.cot_proxy_of}")

    res.details = {
        "instrument_id": canon.instrument_id,
        "display_name": canon.display_name,
        "exchange_symbol": canon.exchange_symbol,
        "price_provider_symbol": canon.price_provider_symbol,
        "cftc_market_code": canon.cftc_market_code,
        "cftc_market_name": canon.cftc_market_name,
        "asset_class": canon.asset_class,
    }
    return res


def audit_price(instrument_id: str, cot3y: dict[str, Any], prices_doc: dict[str, Any]) -> PhaseResult:
    res = PhaseResult(status="pass")
    canon = BY_ID[instrument_id]
    block = (cot3y.get("markets") or {}).get(instrument_id) or {}
    if not block:
        res.bump("fail", "missing from cot_3y_series_latest.json")
        return res

    audit = block.get("price_audit") or {}
    series = block.get("series") or []
    has_price = bool(block.get("has_price"))
    priced = [r for r in series if _finite(r.get("price")) is not None]

    if not has_price or not priced:
        res.bump("fail", "no matched price series on COT weeks")
        return res

    store_key = audit.get("price_store_key")
    if store_key and store_key != instrument_id:
        # Copper historically aliased — fail hard (no accidental joins)
        res.bump("fail", f"price_store_key={store_key!r} != instrument_id")

    canon_sym = str(audit.get("canonical_symbol") or "")
    expected = canon.price_provider_symbol
    if expected and canon_sym:
        if canon_sym.startswith("alpha_vantage:"):
            res.bump(
                "fail",
                f"price source is ETF/AV proxy {canon_sym!r}; "
                f"want continuous provider symbol {expected}",
            )
        elif canon.price_provider == "yahoo":
            # Accept yahoo:KC=F or bare KC=F
            ok = (
                canon_sym == expected
                or canon_sym.endswith(expected)
                or canon_sym.replace("yahoo:", "") == expected
            )
            if not ok:
                res.bump("fail", f"canonical_symbol={canon_sym!r} want yahoo:{expected}")
        elif canon_sym != expected and LEGACY_OANDA_ALIASES.get(canon_sym) != expected:
            if canon_sym.replace("_", "") != expected.replace("_", ""):
                res.bump("fail", f"canonical_symbol={canon_sym!r} want {expected}")

    lag = audit.get("latest_price_lag_days")
    if isinstance(lag, (int, float)) and lag > MAX_PRICE_COT_LAG_DAYS:
        res.bump(
            "fail",
            f"latest price lag {lag}d exceeds max permitted {MAX_PRICE_COT_LAG_DAYS}d "
            f"(latest_price_bar={audit.get('latest_price_bar_date')}, "
            f"latest_cot={audit.get('latest_cot_date')})",
        )

    missing = audit.get("missing_price_weeks")
    if isinstance(missing, int) and missing > 0:
        res.bump("fail", f"{missing} COT weeks missing price")

    # Sanity: prices positive / finite on recent weeks
    for row in priced[-8:]:
        px = _finite(row.get("price"))
        if px is None or px <= 0:
            res.bump("fail", f"non-positive price on {row.get('date')}")
            break

    # Price store presence
    instruments = prices_doc.get("instruments") or prices_doc
    if isinstance(instruments, dict) and instrument_id not in instruments:
        # may be under store key
        if store_key and store_key in instruments:
            res.bump("warn", f"prices_latest keyed by store_key {store_key}")
        else:
            res.bump("warn", "instrument absent from prices_latest.json instruments map")

    res.details = {
        "price_store_key": store_key,
        "canonical_symbol": canon_sym,
        "bar_count": audit.get("price_bar_count"),
        "latest_price_bar_date": audit.get("latest_price_bar_date"),
        "latest_price_lag_days": lag,
        "priced_weeks": len(priced),
    }
    return res


def audit_cot(instrument_id: str, cot3y: dict[str, Any], legacy_doc: dict[str, Any]) -> PhaseResult:
    res = PhaseResult(status="pass")
    block = (cot3y.get("markets") or {}).get(instrument_id) or {}
    series = block.get("series") or []
    if len(series) < 52:
        res.bump("fail", f"only {len(series)} COT weeks (need ≥52)")
        return res

    required = (
        "commercial_net",
        "institutional_net",
        "institutional_long",
        "institutional_short",
        "retail_net",
        "retail_long",
        "retail_short",
        "date",
    )
    sample = random.sample(series, min(5, len(series)))
    for row in sample:
        for key in required:
            if row.get(key) is None and key != "date":
                # commercial long/short may be absent in cot_3y — check net
                if key.startswith("commercial_") and key != "commercial_net":
                    continue
                res.bump("fail", f"week {row.get('date')}: missing {key}")
        cn = _finite(row.get("commercial_net"))
        il = _finite(row.get("institutional_long"))
        ish = _finite(row.get("institutional_short"))
        inet = _finite(row.get("institutional_net"))
        if il is not None and ish is not None and inet is not None:
            if abs((il - ish) - inet) > 1.0:
                res.bump("fail", f"week {row.get('date')}: NC net != long-short")
        if cn is None:
            res.bump("fail", f"week {row.get('date')}: commercial_net null")

    # Weekly change fields present on latest
    latest = series[-1]
    for key in ("one_week_net_change", "four_week_net_change"):
        if latest.get(key) is None:
            res.bump("warn", f"latest missing {key}")

    # Legacy latest presence when available
    legacy_markets = legacy_doc.get("markets") or legacy_doc.get("instruments") or {}
    if isinstance(legacy_markets, dict) and instrument_id in legacy_markets:
        leg = legacy_markets[instrument_id]
        code = str(leg.get("cftc_market_code") or leg.get("contract_code") or "")
        expected = BY_ID[instrument_id].cftc_market_code
        if code and code != expected:
            res.bump("fail", f"legacy export CFTC code {code} != {expected}")
    else:
        res.bump("warn", "instrument not found in legacy_cot_latest.json")

    res.details = {
        "weeks": len(series),
        "latest_date": latest.get("date"),
        "latest_commercial_net": latest.get("commercial_net"),
        "latest_nc_net": latest.get("institutional_net"),
        "latest_nr_net": latest.get("retail_net"),
    }
    return res


def audit_alignment(instrument_id: str, cot3y: dict[str, Any], research: dict[str, Any], inspector: dict[str, Any]) -> PhaseResult:
    res = PhaseResult(status="pass")
    keys = {
        "cot_3y": instrument_id in (cot3y.get("markets") or {}),
        "research": instrument_id in (research.get("markets") or {}),
        "weekly_inspector": instrument_id in (inspector.get("markets") or {}),
    }
    for name, ok in keys.items():
        if not ok:
            res.bump("fail", f"missing from {name} under exact instrument_id")

    block = (cot3y.get("markets") or {}).get(instrument_id) or {}
    if block.get("market") not in (None, instrument_id):
        res.bump("fail", f"cot_3y.market field={block.get('market')!r} != id")

    # Detector / research must not be keyed under a proxy/alias
    rblock = (research.get("markets") or {}).get(instrument_id) or {}
    if rblock and rblock.get("instrument") not in (None, instrument_id):
        if rblock.get("instrument") != instrument_id:
            res.bump("fail", f"research.instrument={rblock.get('instrument')!r}")

    res.details = keys
    return res


def audit_derived(instrument_id: str, research: dict[str, Any], inspector: dict[str, Any]) -> PhaseResult:
    res = PhaseResult(status="pass")
    rblock = (research.get("markets") or {}).get(instrument_id) or {}
    if not rblock.get("available"):
        res.bump("fail", "positioning research unavailable")
    else:
        markers = rblock.get("markers") or []
        if not markers:
            res.bump("warn", "research available but zero markers")
        types = {str(m.get("event_type") or "") for m in markers}
        if markers and not (types & {"major_rotation", "absolute_extreme", "local_extreme", "rapid_velocity", "comm_nr_divergence"}):
            res.bump("warn", f"unexpected marker types only: {sorted(types)[:6]}")

    iblock = (inspector.get("markets") or {}).get(instrument_id) or {}
    if not iblock.get("available"):
        res.bump("fail", "weekly_inspector unavailable")
    else:
        rows = iblock.get("rows") or []
        weeks = iblock.get("weeks") or []
        n = len(rows) if rows else len(weeks)
        if n < 52:
            res.bump("fail", f"weekly_inspector only {n} weeks")
        # Spot-check percentiles in compact rows
        if rows:
            sample = rows[-1]
            # [date, c, nc, nr, cross] — group arrays index 4 = percentile
            for gi, label in ((1, "commercial"), (2, "noncommercial"), (3, "nonreportable")):
                g = sample[gi] if len(sample) > gi else None
                if not isinstance(g, list) or len(g) < 5:
                    res.bump("fail", f"compact group {label} malformed")
                    continue
                net, pct = g[0], g[4]
                if net is not None and pct is None:
                    res.bump("fail", f"{label} net present but percentile null on latest week")

    res.details = {
        "marker_count": len(rblock.get("markers") or []),
        "inspector_weeks": len(iblock.get("rows") or iblock.get("weeks") or []),
    }
    return res


def audit_ui(instrument_id: str, cot3y: dict[str, Any], research: dict[str, Any], inspector: dict[str, Any]) -> PhaseResult:
    """Static UI data-plane checks (no browser)."""
    res = PhaseResult(status="pass")
    # Frontend tracked set is a subset — instruments outside it still need public data.
    tracked_path = ROOT / "web-dashboard" / "src" / "marketResolution.js"
    tracked_src = tracked_path.read_text(encoding="utf-8") if tracked_path.exists() else ""
    in_tracked = f"'{instrument_id}'" in tracked_src or f'"{instrument_id}"' in tracked_src
    if not in_tracked:
        res.bump("warn", "not listed in TRACKED_MARKET_IDS (may be hidden from scanner)")

    block = (cot3y.get("markets") or {}).get(instrument_id) or {}
    series = block.get("series") or []
    if not series:
        res.bump("fail", "no series for charts")
    else:
        last = series[-1]
        if last.get("price") is None:
            res.bump("fail", "latest chart week has no price")
        if last.get("commercial_net") is None:
            res.bump("fail", "latest chart week has no commercial_net")

    if instrument_id not in (research.get("markets") or {}):
        res.bump("fail", "research JSON missing — markers/inspector chrome will break")
    if instrument_id not in (inspector.get("markets") or {}):
        res.bump("fail", "weekly_inspector JSON missing — percentiles unavailable")

    res.details = {"in_tracked_market_ids": in_tracked}
    return res


def _phase_emoji(status: str) -> str:
    return {"pass": "✅", "warn": "⚠️", "fail": "❌"}.get(status, "❓")


def _overall(phases: dict[str, PhaseResult]) -> str:
    if any(p.status == "fail" for p in phases.values()):
        return "FAIL"
    if any(p.status == "warn" for p in phases.values()):
        return "WARN"
    return "PASS"


def audit_instrument(
    instrument_id: str,
    *,
    cot3y: dict[str, Any],
    prices_doc: dict[str, Any],
    legacy_doc: dict[str, Any],
    research: dict[str, Any],
    inspector: dict[str, Any],
) -> dict[str, Any]:
    phases = {
        "identity": audit_identity(instrument_id),
        "price": audit_price(instrument_id, cot3y, prices_doc),
        "cot": audit_cot(instrument_id, cot3y, legacy_doc),
        "alignment": audit_alignment(instrument_id, cot3y, research, inspector),
        "derived": audit_derived(instrument_id, research, inspector),
        "ui": audit_ui(instrument_id, cot3y, research, inspector),
    }
    status = _overall(phases)
    return {
        "instrument_id": instrument_id,
        "status": status,
        "phases": {
            k: {"status": v.status, "issues": v.issues, "details": v.details}
            for k, v in phases.items()
        },
    }


def run_universe_integrity_audit(*, seed: int = 7) -> dict[str, Any]:
    random.seed(seed)
    assert_universe_complete()

    # Duplicate identity field checks (universe-level)
    universe_issues: list[str] = []
    codes = [c.cftc_market_code for c in CANONICAL_INSTRUMENTS]
    if len(codes) != len(set(codes)):
        universe_issues.append("duplicate CFTC market codes in canonical identity")
    exch = [c.exchange_symbol for c in CANONICAL_INSTRUMENTS]
    if len(exch) != len(set(exch)):
        universe_issues.append("duplicate exchange symbols in canonical identity")
    ids = list(LEGACY_COT_MARKETS)
    if len(ids) != len(set(ids)):
        universe_issues.append("duplicate LEGACY_COT_MARKETS ids")

    cot3y = _read_json(PUBLIC / "cot_3y_series_latest.json") or _read_json(DATA / "cot_3y_series_latest.json")
    prices_doc = _read_json(PUBLIC / "prices_latest.json") or _read_json(DATA / "processed" / "prices_latest.json")
    legacy_doc = _read_json(PUBLIC / "legacy_cot_latest.json") or _read_json(DATA / "legacy_cot_latest.json")
    research = _read_json(PUBLIC / "cot_positioning_research_latest.json") or _read_json(
        DATA / "cot_positioning_research_latest.json"
    )
    inspector = _read_json(PUBLIC / "cot_weekly_inspector_latest.json") or _read_json(
        DATA / "cot_weekly_inspector_latest.json"
    )

    rows = [
        audit_instrument(
            mid,
            cot3y=cot3y,
            prices_doc=prices_doc,
            legacy_doc=legacy_doc,
            research=research,
            inspector=inspector,
        )
        for mid in LEGACY_COT_MARKETS
    ]

    passed = sum(1 for r in rows if r["status"] == "PASS")
    warned = sum(1 for r in rows if r["status"] == "WARN")
    failed = sum(1 for r in rows if r["status"] == "FAIL")
    manual = [r["instrument_id"] for r in rows if r["status"] == "FAIL"]

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "universe": list(LEGACY_COT_MARKETS),
        "canonical_cftc_codes": canonical_cftc_codes(),
        "universe_issues": universe_issues,
        "summary": {
            "total_markets": len(rows),
            "passed": passed,
            "warnings": warned,
            "failed": failed,
            "manual_review_required": manual,
            "gate_open": failed == 0 and not universe_issues,
        },
        "instruments": rows,
    }
    return report


def render_markdown(report: dict[str, Any]) -> str:
    lines = [
        "# Universe Integrity Audit",
        "",
        f"Generated: `{report.get('generated_at')}`",
        "",
        "| Instrument | Identity | Price | COT | Alignment | Derived | UI | Status |",
        "|---|---|---|---|---|---|---|---|",
    ]
    for row in report["instruments"]:
        p = row["phases"]
        lines.append(
            "| {id} | {identity} | {price} | {cot} | {alignment} | {derived} | {ui} | {status} |".format(
                id=row["instrument_id"],
                identity=_phase_emoji(p["identity"]["status"]),
                price=_phase_emoji(p["price"]["status"]),
                cot=_phase_emoji(p["cot"]["status"]),
                alignment=_phase_emoji(p["alignment"]["status"]),
                derived=_phase_emoji(p["derived"]["status"]),
                ui=_phase_emoji(p["ui"]["status"]),
                status=row["status"],
            )
        )
    s = report["summary"]
    lines += [
        "",
        f"**Total markets:** {s['total_markets']}",
        "",
        f"**Passed:** {s['passed']}",
        "",
        f"**Warnings:** {s['warnings']}",
        "",
        f"**Failed:** {s['failed']}",
        "",
    ]
    if s["manual_review_required"]:
        lines.append(
            "**Manual review required:** " + ", ".join(s["manual_review_required"])
        )
        lines.append("")
    lines.append(
        f"**Feature gate open:** {'YES' if s['gate_open'] else 'NO — fix failures before new features'}"
    )
    lines.append("")
    lines.append("## Failure details")
    lines.append("")
    for row in report["instruments"]:
        if row["status"] == "PASS":
            continue
        lines.append(f"### {row['instrument_id']} — {row['status']}")
        for phase, body in row["phases"].items():
            if body["status"] == "pass":
                continue
            for issue in body["issues"]:
                lines.append(f"- **{phase}** ({body['status']}): {issue}")
        lines.append("")
    if report.get("universe_issues"):
        lines.append("## Universe-level issues")
        lines.append("")
        for u in report["universe_issues"]:
            lines.append(f"- {u}")
        lines.append("")
    return "\n".join(lines)


def write_report(report: dict[str, Any] | None = None) -> dict[str, Any]:
    report = report or run_universe_integrity_audit()
    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(render_markdown(report), encoding="utf-8")
    PUBLIC_JSON.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    return report
