#!/usr/bin/env python3
"""Phase 3 FX V3 parser/data repair audit — bounded, cache-first.

Never blocks on live network in default (offline) mode. Per-source probes are
capped at SOURCE_CHECK_TIMEOUT_S. Failed or timed-out probes are recorded as
DATA_SOURCE_UNAVAILABLE and the audit continues.

Usage:
    python scripts/audit_fx_v3_parser_repair.py --offline
    python scripts/audit_fx_v3_parser_repair.py --offline --pairs EUR/USD GBP/USD
    python scripts/audit_fx_v3_parser_repair.py --offline --max-pairs 5
"""
from __future__ import annotations

import argparse
import csv
import io
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

DATA = ROOT / "data"
OUT_JSON = DATA / "audits" / "fx_v3_parser_repair_plan.json"
OUT_MD = DATA / "audits" / "fx_v3_parser_repair_plan.md"

SOURCE_CHECK_TIMEOUT_S = 5.0
AUDIT_BUDGET_S = 120.0

DEFAULT_SAMPLE_PAIRS = (
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "NZD/USD",
    "USD/CAD",
)

MIN_ALIGNED_OBS = 52
MIN_R_SQUARED = 0.08


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _bounded_call(fn: Callable[..., Any], *args: Any, timeout: float = SOURCE_CHECK_TIMEOUT_S) -> dict[str, Any]:
    """Run fn with a hard timeout; never raise to caller."""
    with ThreadPoolExecutor(max_workers=1) as pool:
        fut = pool.submit(fn, *args)
        try:
            result = fut.result(timeout=timeout)
            if isinstance(result, dict):
                return result
            return {"status": "OK", "value": result}
        except FuturesTimeoutError:
            return {"status": "DATA_SOURCE_UNAVAILABLE", "reason": f"timeout after {timeout}s"}
        except Exception as exc:  # noqa: BLE001
            return {"status": "DATA_SOURCE_UNAVAILABLE", "reason": f"{type(exc).__name__}: {exc}"}


def _count_csv_data_rows(path: Path) -> int:
    if not path.exists():
        return 0
    raw = path.read_text(encoding="utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(raw))
    if reader.fieldnames and "TIME_PERIOD" in reader.fieldnames:
        return sum(1 for row in reader if (row.get("TIME_PERIOD") or "").strip())
    return sum(1 for line in raw.splitlines() if line.strip() and line[0].isdigit())


def _count_jgb_rows(path: Path) -> int:
    if not path.exists():
        return 0
    raw = path.read_text(encoding="utf-8", errors="replace")
    return sum(1 for line in raw.splitlines() if line.strip() and line[0].isdigit())


def _probe_cache_sources() -> dict[str, dict[str, Any]]:
    from hptl.fx.rate_adapter_base import CACHE_DIR

    probes: dict[str, Callable[[], dict[str, Any]]] = {
        "bis_cbpol_jp": lambda: _probe_file(CACHE_DIR / "bis_cbpol_jp.txt", _count_csv_data_rows, min_obs=52),
        "bis_cbpol_jp_history": lambda: _probe_file(
            CACHE_DIR / "bis_cbpol_jp_history.txt", _count_csv_data_rows, min_obs=52
        ),
        "bis_cbpol_nz": lambda: _probe_file(CACHE_DIR / "bis_cbpol_nz.txt", _count_csv_data_rows, min_obs=52),
        "bis_cbpol_nz_history": lambda: _probe_file(
            CACHE_DIR / "bis_cbpol_nz_history.txt", _count_csv_data_rows, min_obs=52
        ),
        "jpy_jgb": lambda: _probe_file(CACHE_DIR / "jpy_jgb.txt", _count_jgb_rows, min_obs=52),
        "cad_valet": lambda: _probe_file(CACHE_DIR / "cad_valet.txt", lambda p: p.stat().st_size if p.exists() else 0, min_obs=1000),
        "eur_2y": lambda: _probe_file(CACHE_DIR / "eur_2y.txt", _count_csv_data_rows, min_obs=52),
        "gbp_boe_glc": lambda: _probe_file(CACHE_DIR / "boe_glc_nominal_archive.bin", lambda p: 1 if p.exists() else 0, min_obs=1),
    }

    out: dict[str, dict[str, Any]] = {}
    for key, fn in probes.items():
        out[key] = _bounded_call(fn, timeout=SOURCE_CHECK_TIMEOUT_S)
    return out


def _probe_file(path: Path, counter: Callable[[Path], int], *, min_obs: int) -> dict[str, Any]:
    if not path.exists():
        return {
            "status": "DATA_SOURCE_UNAVAILABLE",
            "path": str(path),
            "reason": "cache file missing",
            "observation_count": 0,
            "min_required": min_obs,
        }
    count = counter(path)
    if count < min_obs:
        return {
            "status": "DATA_SOURCE_UNAVAILABLE",
            "path": str(path),
            "reason": f"insufficient cached rows ({count} < {min_obs})",
            "observation_count": count,
            "min_required": min_obs,
        }
    return {
        "status": "OK",
        "path": str(path),
        "observation_count": count,
        "min_required": min_obs,
    }


def _spot_key_audit(pair_id: str) -> dict[str, Any]:
    from hptl.fx.currency_map import COT_CURRENCY_SOURCES
    from hptl.fx.fx_valuation import resolve_pair_currencies
    from hptl.fx.fx_spot_history import get_daily_spot_series
    from hptl.prices.price_store import load_price_store

    resolved = resolve_pair_currencies(pair_id)
    if not resolved:
        return {"status": "DATA_SOURCE_UNAVAILABLE", "reason": "unsupported pair mapping"}
    _base, _quote, canonical = resolved
    instruments = load_price_store().get("instruments") or {}

    candidates: list[tuple[str, int]] = []
    keys = [canonical, pair_id]
    for _code, spec in COT_CURRENCY_SOURCES.items():
        if str(spec.get("quote")) == canonical:
            keys.append(str(spec.get("market")))
    seen: set[str] = set()
    for key in keys:
        if not key or key in seen:
            continue
        seen.add(key)
        rec = instruments.get(key) or {}
        daily = rec.get("daily") or []
        candidates.append((key, len(daily)))

    series, meta = get_daily_spot_series(pair_id)
    best = max(candidates, key=lambda x: x[1]) if candidates else (None, 0)
    used = meta.get("source", "")
    issue = None
    if best[0] and best[1] > len(series) + 50:
        issue = "parser_alias"
    return {
        "status": "OK" if series else "DATA_SOURCE_UNAVAILABLE",
        "canonical_pair": canonical,
        "spot_obs_used": len(series),
        "spot_source": used,
        "candidate_keys": [{"key": k, "daily_obs": n} for k, n in candidates],
        "deepest_key": best[0],
        "deepest_obs": best[1],
        "parser_issue": issue,
    }


def _pair_from_v3_audit(pair_id: str, v3_audit: dict[str, Any]) -> dict[str, Any] | None:
    for row in v3_audit.get("rows") or []:
        if row.get("pair") == pair_id:
            return row
    return v3_audit.get("pairs", {}).get(pair_id)


def _classify_failure(
    pair_id: str,
    v3_row: dict[str, Any] | None,
    spot_audit: dict[str, Any],
    currency_legs: dict[str, Any],
) -> dict[str, Any]:
    if not v3_row:
        return {
            "failure_class": "DATA_SOURCE_UNAVAILABLE",
            "exact_reason": "No row in cached fx_valuation_v3_audit.json",
            "repairable_parser_data": True,
        }

    aligned = int(v3_row.get("aligned_obs") or 0)
    r2 = v3_row.get("r_squared")
    audit_status = v3_row.get("audit_status")
    spot_obs = int(v3_row.get("spot_obs") or 0)

    missing: list[str] = list(v3_row.get("missing_inputs") or [])
    reasons: list[str] = []

    if spot_audit.get("parser_issue") == "parser_alias":
        missing.append("spot_history_shallow_alias")
        reasons.append(
            f"Price store alias {spot_audit.get('spot_source')} has {spot_obs} obs; "
            f"COT key {spot_audit.get('deepest_key')} has {spot_audit.get('deepest_obs')} obs"
        )

    base, quote = pair_id.split("/", 1)
    for ccy in (base, quote):
        leg = currency_legs.get(ccy) or {}
        for field in ("policy", "y2"):
            meta = leg.get(field) or {}
            if meta.get("status") == "DATA_SOURCE_UNAVAILABLE":
                missing.append(f"{ccy}_{field}")
                reasons.append(meta.get("reason") or f"{ccy} {field} cache unavailable")

    if aligned < MIN_ALIGNED_OBS:
        reasons.append(f"aligned_obs={aligned} < required {MIN_ALIGNED_OBS}")
        failure_class = "parser_data" if missing or spot_audit.get("parser_issue") else "data_gap"
    elif r2 is None:
        reasons.append("R² not computed (insufficient aligned panel)")
        failure_class = "parser_data" if missing or spot_audit.get("parser_issue") else "data_gap"
    elif float(r2) < MIN_R_SQUARED:
        reasons.append(f"R²={r2} < required {MIN_R_SQUARED}")
        failure_class = "model_weakness" if not missing and not spot_audit.get("parser_issue") else "parser_data"
    elif audit_status == "PASS":
        return {
            "failure_class": "none",
            "exact_reason": "PASS",
            "repairable_parser_data": False,
        }
    else:
        reasons.append(f"audit_status={audit_status}")
        failure_class = "parser_data"

    repairable = failure_class in ("parser_data", "data_gap") and (
        bool(missing) or spot_audit.get("parser_issue") or aligned < MIN_ALIGNED_OBS
    )
    if failure_class == "model_weakness":
        repairable = False

    return {
        "failure_class": failure_class,
        "exact_reason": "; ".join(reasons) or "unknown",
        "missing_inputs": missing,
        "repairable_parser_data": repairable,
        "available_observations": aligned,
        "required_observations": MIN_ALIGNED_OBS,
        "r_squared": r2,
        "required_r_squared": MIN_R_SQUARED,
    }


def _currency_legs_from_foundation(foundation: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """Read G10 leg counts from exported foundation audit (no heavy loaders)."""
    out: dict[str, dict[str, Any]] = {}
    for row in foundation.get("g10_currency_table") or []:
        ccy = row.get("currency")
        if not ccy:
            continue
        detail = row.get("detail") or {}
        leg: dict[str, Any] = {}
        for field, key in (("policy", "policy"), ("y2", "yield_2y"), ("y10", "yield_10y")):
            meta = detail.get(key) or {}
            count = int(meta.get("observation_count") or 0)
            leg[field] = {
                "observation_count": count,
                "status": meta.get("audit_status") if count >= MIN_ALIGNED_OBS else "DATA_SOURCE_UNAVAILABLE",
                "reason": None
                if count >= MIN_ALIGNED_OBS
                else f"only {count} cached obs (need {MIN_ALIGNED_OBS})",
                "source": meta.get("source"),
            }
        out[str(ccy)] = leg
    return out


def _currency_legs_from_pair_foundation(foundation: dict[str, Any], pair_id: str) -> dict[str, dict[str, Any]]:
    """Per-pair leg detail from foundation audit pairs block."""
    block = (foundation.get("pairs") or {}).get(pair_id) or {}
    legs: dict[str, dict[str, Any]] = {}
    for side in ("base", "quote"):
        for field_key, field in (("policy_history", "policy"), ("yield_history", "y2")):
            section = block.get(field_key) or {}
            sub = section.get(side) or {}
            ccy = sub.get("currency")
            if not ccy:
                continue
            count = int(sub.get("observation_count") or 0)
            legs.setdefault(str(ccy), {})[field] = {
                "observation_count": count,
                "status": sub.get("audit_status") if count >= MIN_ALIGNED_OBS else "DATA_SOURCE_UNAVAILABLE",
                "reason": (sub.get("missing_periods") or [None])[0]
                if count < MIN_ALIGNED_OBS
                else None,
                "source": sub.get("source"),
            }
    return legs


def _select_pairs(args: argparse.Namespace) -> list[str]:
    if args.pairs:
        return list(args.pairs)
    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_PAIRS

    if args.max_pairs is not None:
        pool = list(DEFAULT_SAMPLE_PAIRS) + [p for p in FX_V3_PAIRS if p not in DEFAULT_SAMPLE_PAIRS]
        return pool[: args.max_pairs]
    # Default: V3 PARTIAL majors from inventory + sample set
    inv = _read_json(DATA / "audits" / "valuation_inventory.json")
    partial_fx: list[str] = []
    for inst in inv.get("instruments") or []:
        if inst.get("asset_class") != "fx":
            continue
        if inst.get("valuation_status") != "PARTIAL":
            continue
        if inst.get("valuation_engine_used") != "fx_carry_real_yield_v3":
            continue
        pid = inst.get("fx_pair_id")
        if pid and pid not in partial_fx:
            partial_fx.append(pid)
    return partial_fx or list(DEFAULT_SAMPLE_PAIRS)


def _canonical_market(pair_id: str) -> str | None:
    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_CANONICAL_MARKET_BY_PAIR, FX_V3_PILLAR_ALIAS_OF

    return FX_V3_CANONICAL_MARKET_BY_PAIR.get(pair_id) or FX_V3_PILLAR_ALIAS_OF.get(pair_id)


def run_audit(*, offline: bool, pairs: list[str]) -> dict[str, Any]:
    started = time.monotonic()
    if offline:
        os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"
    else:
        os.environ.pop("HPTL_SKIP_LIVE_FEEDS", None)

    v3_audit = _read_json(DATA / "audits" / "fx_valuation_v3_audit.json")
    foundation = _read_json(DATA / "audits" / "fx_valuation_data_foundation_audit.json")

    cache_probes = _probe_cache_sources()
    currency_legs = _currency_legs_from_foundation(foundation)

    pair_rows: list[dict[str, Any]] = []
    repairs: list[dict[str, Any]] = []

    for pair_id in pairs:
        if time.monotonic() - started > AUDIT_BUDGET_S:
            pair_rows.append(
                {
                    "pair": pair_id,
                    "status": "SKIPPED",
                    "reason": f"audit budget {AUDIT_BUDGET_S}s exceeded",
                }
            )
            break

        v3_row = _pair_from_v3_audit(pair_id, v3_audit)
        spot_audit = _bounded_call(_spot_key_audit, pair_id)
        pair_legs = _currency_legs_from_pair_foundation(foundation, pair_id)
        merged_legs = {**currency_legs, **pair_legs}
        classification = _classify_failure(pair_id, v3_row, spot_audit, merged_legs)

        instrument_id = _canonical_market(pair_id) or pair_id
        cached = v3_row or {}
        row = {
            "pair": pair_id,
            "instrument_id": instrument_id,
            "canonical_market": instrument_id,
            "v3_status": cached.get("audit_status", "UNKNOWN"),
            "wired": cached.get("wired"),
            "exact_failure_reason": classification.get("exact_reason"),
            "failure_class": classification.get("failure_class"),
            "parser_or_model": (
                "pass"
                if cached.get("audit_status") == "PASS"
                else "parser_data"
                if classification.get("repairable_parser_data")
                else "model_weakness"
                if classification.get("failure_class") == "model_weakness"
                else "data_gap"
            ),
            "missing_inputs": classification.get("missing_inputs") or [],
            "available_observations": classification.get("available_observations")
            or cached.get("aligned_obs"),
            "required_observations": MIN_ALIGNED_OBS,
            "r_squared": classification.get("r_squared") if classification.get("r_squared") is not None else cached.get("r_squared"),
            "required_r_squared": MIN_R_SQUARED,
            "spot_audit": spot_audit,
            "cached_v3_row": v3_row,
            "foundation_pair": (foundation.get("pairs") or {}).get(pair_id),
        }
        pair_rows.append(row)

        if classification.get("repairable_parser_data"):
            action = []
            if spot_audit.get("parser_issue") == "parser_alias":
                action.append("Prefer COT major price_store key over OANDA cross alias in fx_spot_history")
            if any("JPY" in m for m in row["missing_inputs"]):
                action.append("Wire load_bis_policy_history to bis_cbpol_*_history.txt; add JPY 2Y fallback (MoF cache is 9 rows)")
            if any("NZD" in m for m in row["missing_inputs"]):
                action.append("Fetch bis_cbpol_nz_history.txt; add NZD 2Y history loader (FRED OECD fallback)")
            if not action:
                action.append("Review cached macro leg coverage in fx_macro_history")
            repairs.append({"pair": pair_id, "actions": action, "priority": "high" if action else "low"})

    elapsed = round(time.monotonic() - started, 2)
    report = {
        "audit_type": "fx_v3_parser_repair_plan",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "mode": "offline" if offline else "online_bounded",
        "source_check_timeout_s": SOURCE_CHECK_TIMEOUT_S,
        "audit_budget_s": AUDIT_BUDGET_S,
        "elapsed_s": elapsed,
        "gates": {"min_aligned_obs": MIN_ALIGNED_OBS, "min_r_squared": MIN_R_SQUARED},
        "pairs_audited": len(pair_rows),
        "cache_probes": cache_probes,
        "currency_legs": currency_legs,
        "pairs": pair_rows,
        "repair_actions": repairs,
        "summary": {
            "pass": sum(1 for r in pair_rows if r.get("v3_status") == "PASS"),
            "fail": sum(1 for r in pair_rows if r.get("v3_status") == "FAIL"),
            "parser_repairable": sum(1 for r in pair_rows if r.get("parser_or_model") == "parser_data"),
            "model_weakness": sum(1 for r in pair_rows if r.get("parser_or_model") == "model_weakness"),
        },
        "data_sources": {
            "v3_audit": str(DATA / "audits" / "fx_valuation_v3_audit.json"),
            "foundation_audit": str(DATA / "audits" / "fx_valuation_data_foundation_audit.json"),
            "valuation_inventory": str(DATA / "audits" / "valuation_inventory.json"),
        },
    }
    return report


def _markdown(report: dict[str, Any]) -> str:
    lines = [
        "# FX V3 Parser / Data Repair Plan",
        "",
        f"- Generated: {report.get('generated_at')}",
        f"- Mode: **{report.get('mode')}** (no blocking network in offline mode)",
        f"- Elapsed: {report.get('elapsed_s')}s (budget {report.get('audit_budget_s')}s)",
        f"- Per-source timeout: {report.get('source_check_timeout_s')}s",
        "",
        "## Summary",
        "",
        f"- Pairs audited: {report.get('pairs_audited')}",
        f"- V3 PASS (cached): {report['summary']['pass']}",
        f"- V3 FAIL (cached): {report['summary']['fail']}",
        f"- Parser/data repairable: {report['summary']['parser_repairable']}",
        f"- Model weakness (do not promote): {report['summary']['model_weakness']}",
        "",
        "## Pair diagnosis",
        "",
        "| Pair | V3 | Obs | R² | Class | Failure reason |",
        "|---|---|---:|---:|---|---|",
    ]
    for r in report.get("pairs") or []:
        if r.get("status") == "SKIPPED":
            lines.append(f"| {r['pair']} | SKIPPED | — | — | — | {r.get('reason')} |")
            continue
        r2 = r.get("r_squared")
        r2s = f"{r2:.4f}" if isinstance(r2, (int, float)) else "—"
        obs = r.get("available_observations")
        lines.append(
            f"| {r['pair']} | {r.get('v3_status')} | {obs} | {r2s} | {r.get('parser_or_model')} | "
            f"{r.get('exact_failure_reason', '')[:80]} |"
        )

    lines.extend(["", "## Cache probes", ""])
    for name, probe in (report.get("cache_probes") or {}).items():
        lines.append(f"- **{name}**: {probe.get('status')} — {probe.get('reason') or probe.get('observation_count', 'OK')}")

    lines.extend(["", "## Repair actions", ""])
    for rep in report.get("repair_actions") or []:
        lines.append(f"### {rep['pair']}")
        for act in rep.get("actions") or []:
            lines.append(f"- {act}")
        lines.append("")

    return "\n".join(lines)


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Bounded FX V3 parser/data repair audit.")
    ap.add_argument(
        "--offline",
        action="store_true",
        help="Cache-only mode (default). Sets HPTL_SKIP_LIVE_FEEDS=1.",
    )
    ap.add_argument(
        "--online",
        action="store_true",
        help="Allow bounded live probes (still capped at 5s per source). Default is offline.",
    )
    ap.add_argument(
        "--pairs",
        nargs="+",
        metavar="PAIR",
        help="Explicit pair list, e.g. EUR/USD GBP/USD",
    )
    ap.add_argument(
        "--max-pairs",
        type=int,
        metavar="N",
        help="Audit first N pairs (sample defaults first).",
    )
    args = ap.parse_args(argv)
    offline = not args.online  # default: offline/cache-first

    pairs = list(args.pairs) if args.pairs else _select_pairs(args)

    report = run_audit(offline=offline, pairs=pairs)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(report, indent=2), encoding="utf-8")
    OUT_MD.write_text(_markdown(report), encoding="utf-8")

    print(f"Wrote {OUT_JSON}")
    print(f"Wrote {OUT_MD}")
    print(
        f"Done in {report['elapsed_s']}s — "
        f"{report['summary']['pass']} PASS, {report['summary']['fail']} FAIL, "
        f"{report['summary']['parser_repairable']} parser-repairable"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
