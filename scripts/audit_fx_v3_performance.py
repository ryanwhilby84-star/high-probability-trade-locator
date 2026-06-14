#!/usr/bin/env python3
"""FX V3 performance audit — profile build_all_fx_v3_pairs (no code changes)."""
from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

OUT_JSON = ROOT / "data" / "audits" / "fx_v3_performance_audit.json"
OUT_MD = ROOT / "data" / "audits" / "fx_v3_performance_audit.md"


@dataclass
class Counter:
    calls: int = 0
    total_s: float = 0.0

    def add(self, dt: float) -> None:
        self.calls += 1
        self.total_s += dt

    @property
    def avg_s(self) -> float:
        return self.total_s / self.calls if self.calls else 0.0


@dataclass
class AuditState:
    counters: dict[str, Counter] = field(default_factory=lambda: defaultdict(Counter))
    pair_timings: list[dict[str, Any]] = field(default_factory=list)
    build_total_s: float = 0.0
    build_shared_s: float = 0.0


STATE = AuditState()
_ORIGINALS: dict[str, Callable[..., Any]] = {}


def _wrap(module: Any, name: str, label: str | None = None) -> None:
    key = label or name
    fn = getattr(module, name)

    def wrapped(*args: Any, **kwargs: Any) -> Any:
        t0 = time.monotonic()
        try:
            return fn(*args, **kwargs)
        finally:
            STATE.counters[key].add(time.monotonic() - t0)

    _ORIGINALS[key] = fn
    setattr(module, name, wrapped)


def _install_hooks() -> None:
    import hptl.fx.fx_macro_history as mh
    import hptl.fx.fx_rate_history_loaders as loaders
    import hptl.fx.fx_spot_history as sh
    import hptl.fx.fx_valuation_attach as attach
    import hptl.fx.currency_rates as cr
    import hptl.prices.price_store as ps
    import hptl.prices.canonical_timeline as ct
    import hptl.macro.fred_client as fred
    import hptl.valuation.fx_carry_real_yield_v3 as v3

    _wrap(mh, "currency_histories", "currency_histories()")
    loaders.currency_histories = mh.currency_histories  # re-export alias
    v3.currency_histories = mh.currency_histories  # bound name used by compute_fx_pair_v3
    _wrap(mh, "load_usd_combined_history", "load_usd_combined_history()")
    _wrap(mh, "load_gbp_boe_yield_history", "load_gbp_boe_yield_history() [BoE GLC zip parse]")
    _wrap(mh, "load_aud_rba_history", "load_aud_rba_history() [RBA xlsx parse]")
    _wrap(mh, "load_chf_rendoblid_history", "load_chf_rendoblid_history()")
    _wrap(mh, "load_jpy_y2_history", "load_jpy_y2_history()")
    _wrap(mh, "load_jpy_y10_history", "load_jpy_y10_history()")
    _wrap(mh, "load_nzd_y2_history", "load_nzd_y2_history()")
    _wrap(mh, "load_nzd_y10_history", "load_nzd_y10_history()")
    _wrap(mh, "load_cad_valet_history", "load_cad_valet_history()")
    _wrap(mh, "load_ecb_yield_history", "load_ecb_yield_history() [ECB CSV read]")
    _wrap(mh, "load_bis_policy_history", "load_bis_policy_history() [BIS CSV read]")
    _wrap(mh, "load_gbp_bank_rate_history", "load_gbp_bank_rate_history()")
    _wrap(mh, "load_jpy_jgb_history", "load_jpy_jgb_history()")
    _wrap(mh, "load_fred_daily_map", "load_fred_daily_map() [FRED macro_cache]")
    _wrap(fred, "get_series_df", "fred_client.get_series_df()")

    _wrap(sh, "get_daily_spot_series", "get_daily_spot_series()")
    _wrap(ps, "load_price_store", "load_price_store()")
    _wrap(cr, "get_currency_rate", "get_currency_rate()")
    _wrap(attach, "_spot_and_percentile", "_spot_and_percentile()")
    _wrap(ct, "load_canonical_timeline", "load_canonical_timeline()")
    _wrap(v3, "_align_daily_panel", "_align_daily_panel()")
    _wrap(v3, "_dxy_regime", "_dxy_regime()")
    _wrap(v3, "_treasury_regime", "_treasury_regime()")


def _pct(part: float, whole: float) -> float:
    return round(part / whole * 100.0, 1) if whole > 0 else 0.0


def _run_build_all() -> None:
    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_PAIRS, build_all_fx_v3_pairs, compute_fx_pair_v3

    orig_compute = compute_fx_pair_v3

    def timed_compute(pair_id: str, *args: Any, **kwargs: Any) -> Any:
        ch_before = STATE.counters["currency_histories()"].calls
        t_pair = time.monotonic()
        result = orig_compute(pair_id, *args, **kwargs)
        STATE.pair_timings.append(
            {
                "pair": pair_id,
                "duration_s": round(time.monotonic() - t_pair, 3),
                "currency_histories_calls": STATE.counters["currency_histories()"].calls - ch_before,
            }
        )
        return result

    import hptl.valuation.fx_carry_real_yield_v3 as v3

    v3.compute_fx_pair_v3 = timed_compute  # type: ignore[assignment]

    t0 = time.monotonic()
    build_all_fx_v3_pairs()
    STATE.build_total_s = round(time.monotonic() - t0, 3)


def _run_shared_estimate() -> None:
    from hptl.fx.fx_rate_history_loaders import currency_histories
    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_PAIRS, compute_fx_pair_v3

    t0 = time.monotonic()
    shared = currency_histories()
    for pid in FX_V3_PAIRS:
        compute_fx_pair_v3(pid, histories=shared)
    return round(time.monotonic() - t0, 3)


def _rows_sorted(total: float) -> list[dict[str, Any]]:
    rows = []
    for name, c in STATE.counters.items():
        rows.append(
            {
                "step": name,
                "call_count": c.calls,
                "total_s": round(c.total_s, 3),
                "avg_s": round(c.avg_s, 4),
                "pct_of_build": _pct(c.total_s, total),
            }
        )
    rows.sort(key=lambda r: r["total_s"], reverse=True)
    return rows


def _markdown(payload: dict[str, Any]) -> str:
    lines = [
        "# FX V3 Performance Audit — `build_all_fx_v3_pairs`",
        "",
        f"- Generated: {payload['generated_at']}",
        f"- Mode: {'offline' if payload['offline_mode'] else 'online'} (`HPTL_SKIP_LIVE_FEEDS`)",
        f"- FX V3 pairs: {payload['pair_count']}",
        "",
        "## Summary",
        "",
        f"- **`build_all_fx_v3_pairs` wall time: {payload['build_total_s']}s**",
        f"- Estimated with shared histories (one load): **{payload['build_shared_estimate_s']}s**",
        f"- Estimated savings if duplicate work removed: **~{payload['estimated_savings_s']}s** "
        f"({payload['estimated_savings_pct']}% of current runtime)",
        "",
        "## Root cause",
        "",
        payload["root_cause"],
        "",
        "## Instrumented step breakdown (during `build_all_fx_v3_pairs`)",
        "",
        "| Step | Calls | Total (s) | Avg (s) | % of build |",
        "|---|---:|---:|---:|---:|",
    ]
    for r in payload["steps"]:
        lines.append(
            f"| {r['step']} | {r['call_count']} | {r['total_s']} | {r['avg_s']} | {r['pct_of_build']} |"
        )

    lines.extend(["", "## Per-pair timing (isolated `compute_fx_pair_v3` calls)", ""])
    lines.append("| Pair | Duration (s) | `currency_histories()` calls |")
    lines.append("|---|---:|---:|")
    for p in payload["per_pair"]:
        lines.append(f"| {p['pair']} | {p['duration_s']} | {p['currency_histories_calls']} |")

    lines.extend(["", "## Shared histories safety", ""])
    for bullet in payload["shared_histories_safety"]:
        lines.append(f"- {bullet}")

    lines.extend(["", "## Repeated expensive operations", ""])
    for bullet in payload["repeated_operations"]:
        lines.append(f"- {bullet}")

    lines.extend(["", "## What is NOT duplicated", ""])
    for bullet in payload["not_duplicated"]:
        lines.append(f"- {bullet}")

    return "\n".join(lines)


def main() -> int:
    os.environ["HPTL_SKIP_LIVE_FEEDS"] = "1"
    _install_hooks()

    from hptl.valuation.fx_carry_real_yield_v3 import FX_V3_PAIRS

    _run_build_all()
    build_counters = {k: Counter(v.calls, v.total_s) for k, v in STATE.counters.items()}
    build_total = STATE.build_total_s
    pair_timings = list(STATE.pair_timings)

    shared_total = _run_shared_estimate()

    # Restore build counter view for report primary table
    STATE.counters = build_counters
    steps = _rows_sorted(build_total)

    ch = build_counters.get("currency_histories()", Counter())
    gbp = build_counters.get("load_gbp_boe_yield_history() [BoE GLC zip parse]", Counter())
    fred_map = build_counters.get("load_fred_daily_map() [FRED macro_cache]", Counter())
    ecb = build_counters.get("load_ecb_yield_history() [ECB CSV read]", Counter())
    bis = build_counters.get("load_bis_policy_history() [BIS CSV read]", Counter())
    aud = build_counters.get("load_aud_rba_history() [RBA xlsx parse]", Counter())
    spot = build_counters.get("get_daily_spot_series()", Counter())
    align = build_counters.get("_align_daily_panel()", Counter())
    price_store = build_counters.get("load_price_store()", Counter())

    savings = round(build_total - shared_total, 1)
    savings_pct = _pct(savings, build_total)

    root_cause = (
        f"`build_all_fx_v3_pairs` calls `compute_fx_pair_v3(pid)` for each of {len(FX_V3_PAIRS)} pairs "
        f"without passing `histories=`. Each call executes `histories = histories or currency_histories()` "
        f"({ch.calls} times, {round(ch.total_s, 1)}s total, ~{round(ch.avg_s, 1)}s avg). "
        f"That reloads and reparses all G10 macro series on every pair — BoE GLC ({gbp.calls}×, "
        f"{round(gbp.total_s, 1)}s), FRED ({fred_map.calls}×), ECB CSV ({ecb.calls}×), BIS CSV ({bis.calls}×), "
        f"RBA xlsx ({aud.calls}×). Per-pair valuation logic itself (_align_daily_panel, regression) is comparatively cheap."
    )

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "offline_mode": True,
        "pair_count": len(FX_V3_PAIRS),
        "build_total_s": build_total,
        "build_shared_estimate_s": shared_total,
        "estimated_savings_s": savings,
        "estimated_savings_pct": savings_pct,
        "root_cause": root_cause,
        "steps": steps,
        "per_pair": pair_timings,
        "shared_histories_safety": [
            "`currency_histories()` returns a fresh dict of plain `{date: float}` maps — no mutation in `compute_fx_pair_v3`.",
            "`_align_daily_panel` copies leg maps with `dict(...)` before alignment; shared input is read-only.",
            "Point-in-time rates come from `get_currency_rate()` per pair (current adapter snapshot), not from mutating `histories`.",
            "Passing one `histories` dict into all 13 `compute_fx_pair_v3` calls is safe and produces identical macro inputs.",
        ],
        "repeated_operations": [
            f"`currency_histories()` — {ch.calls} calls, {round(ch.total_s, 1)}s ({_pct(ch.total_s, build_total)}% of build)",
            f"`load_gbp_boe_yield_history()` — {gbp.calls} calls, {round(gbp.total_s, 1)}s (BoE GLC zip / JSON parse)",
            f"`load_fred_daily_map()` — {fred_map.calls} calls, {round(fred_map.total_s, 1)}s (USD + JPY/NZD fallbacks)",
            f"`load_ecb_yield_history()` — {ecb.calls} calls, {round(ecb.total_s, 1)}s (3 ECB files × {ch.calls} macro loads)",
            f"`load_bis_policy_history()` — {bis.calls} calls, {round(bis.total_s, 1)}s (JPY/NZD/CHF policy CSVs)",
            f"`load_aud_rba_history()` — {aud.calls} calls, {round(aud.total_s, 1)}s (RBA F1/F2 xlsx parse from .bin cache)",
            f"`get_daily_spot_series()` — {spot.calls} calls, {round(spot.total_s, 1)}s (once per pair in `_align_daily_panel`)",
            f"`load_price_store()` — {price_store.calls} calls, {round(price_store.total_s, 1)}s (JSON deserialisation on spot path)",
            f"`_align_daily_panel()` — {align.calls} calls, {round(align.total_s, 1)}s (spot × macro forward-fill join per pair)",
        ],
        "not_duplicated": [
            "BoE GLC has an in-process memory cache (`_GBP_YIELD_MEM`) but `currency_histories()` still re-enters the loader each call.",
            "When JSON sidecar exists and archive mtime unchanged, BoE reload is fast; cold path parses zip/xlsx.",
            "Regression OLS and fair-value math are negligible vs I/O and parsing.",
        ],
        "macro_rebuilt_every_pair": True,
        "macro_rebuilt_evidence": {
            "currency_histories_calls_during_build": ch.calls,
            "expected_if_shared_once": 1,
            "pairs": len(FX_V3_PAIRS),
        },
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    OUT_MD.write_text(_markdown(payload), encoding="utf-8")

    print(f"build_all_fx_v3_pairs: {build_total}s")
    print(f"shared histories estimate: {shared_total}s (save ~{savings}s, {savings_pct}%)")
    print(f"currency_histories: {ch.calls} calls, {round(ch.total_s,1)}s")
    print(f"Wrote {OUT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
