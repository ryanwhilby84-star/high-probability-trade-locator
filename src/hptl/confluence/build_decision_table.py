from __future__ import annotations

import io
import json
import math
import os
import zipfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import requests

from hptl.confluence.build_confluence_history import _build_confluence
from hptl.cot.contracts import FINANCIAL_FUTURES_ONLY_URL_TEMPLATE, FINANCIAL_INDEX_CODE_TO_TARGET
from hptl.cot.scoring_engine import apply_probabilistic_cot_scoring
from hptl.cot.parser import clean_columns
from hptl.cot.positioning_percentiles import (
    WINDOW_WEEKS_3Y,
    classification_line,
    classify_percentile,
    compute_absolute_positioning,
    empirical_percentile_rank as _pct_rank_window,
    interpret_metric,
    METRIC_LONG,
    METRIC_NET,
    METRIC_OI,
    METRIC_SHORT,
)
from hptl.confluence.run_confluence_update import _find_column
from hptl.macro.macro_scoring import build_macro_audit_payload, _row_has_required_scoring_inputs
from hptl.macro.macro_transmission import build_macro_transmission
from hptl.news.instrument_intel import build_instrument_intel_context, derive_global_market_regime
from hptl.intermarket.intermarket_scoring import build_intermarket_impulse_context
from hptl.confluence.ui_pack import build_record_ui_pack
from hptl.intelligence.market_environment_feed import attach_feeds_to_latest_records
from hptl.macro.macro_relationship_maps import build_all_macro_relationship_maps
from hptl.confluence.dashboard_export import (
    MACRO_MAPS_PATH,
    OUT_PATH,
    write_dashboard_exports,
)
from hptl.context.priority_board import (
    aggregate_priority_markets,
    build_priority_debug,
    write_priority_debug,
)
from hptl.fx.relative_strength import build_relative_strength, write_relative_strength
from hptl.context.institutional_context import precompute_institutional_context_index
from hptl.context.macro_only_context import build_macro_only_institutional_context
from hptl.markets.instrument_registry import (
    LEGACY_COT_MARKETS,
    MARKET_ALIASES,
    TARGET_MARKETS,
    cot_mapped_ids,
    export_registry_json,
    get_instrument,
    instrument_meta_for_record,
)
from hptl.markets.coverage_audit import run_coverage_audit, write_coverage_audit
from hptl.pillars.confluence_attach import pillar_fields_for_market_week
from hptl.fx.fx_valuation_attach import fx_valuation_fields_for_market
from hptl.confluence.macro_hub_cot_attach import apply_macro_hub_cot_fallback

PROCESSED_DIR = Path("data/processed")
RATES_CLEAN_PATH = PROCESSED_DIR / "macro" / "rates_clean.csv"
EXPORT_DIR = Path("data/exports")
AUDIT_CSV_PATH = Path("data/exports/decision_table_audit.csv")
TRACKED_MASTER_FILENAME = "cot_tracked_master_normalized.csv"

# --- Stage progress + timing + stall watchdog --------------------------------
import threading
import time

_PROGRESS_LOCK = threading.Lock()
_PROGRESS: dict[str, Any] = {"ts": time.monotonic(), "stage": "init", "detail": ""}
_WATCHDOG_TIMEOUT_S = float(os.environ.get("HPTL_STAGE_TIMEOUT_S", "120"))
_WATCHDOG_ON = os.environ.get("HPTL_DISABLE_WATCHDOG", "").strip().lower() not in {"1", "true", "yes"}


def _heartbeat(stage: str | None = None, detail: str = "") -> None:
    """Mark progress so the stall watchdog does not abort a healthy stage."""
    with _PROGRESS_LOCK:
        _PROGRESS["ts"] = time.monotonic()
        if stage is not None:
            _PROGRESS["stage"] = stage
        _PROGRESS["detail"] = detail


def _watchdog_loop() -> None:
    while True:
        time.sleep(5)
        with _PROGRESS_LOCK:
            idle = time.monotonic() - _PROGRESS["ts"]
            stage = _PROGRESS["stage"]
            detail = _PROGRESS["detail"]
        if idle > _WATCHDOG_TIMEOUT_S:
            print(
                f"\n[WATCHDOG] ABORT: stage '{stage}' made no progress for {idle:.0f}s "
                f"(limit {_WATCHDOG_TIMEOUT_S:.0f}s). Last detail: {detail}",
                flush=True,
            )
            os._exit(2)


def _start_watchdog() -> None:
    if not _WATCHDOG_ON:
        print("[WATCHDOG] disabled (HPTL_DISABLE_WATCHDOG set).", flush=True)
        return
    threading.Thread(target=_watchdog_loop, daemon=True, name="hptl-stage-watchdog").start()
    print(f"[WATCHDOG] armed — aborts if any stage stalls >{_WATCHDOG_TIMEOUT_S:.0f}s.", flush=True)


class _Stage:
    """Context manager: prints start/end + elapsed, and heartbeats the watchdog."""

    def __init__(self, name: str):
        self.name = name
        self.t0 = 0.0

    def __enter__(self) -> "_Stage":
        self.t0 = time.monotonic()
        _heartbeat(self.name, "start")
        print(f"[STAGE START] {self.name}", flush=True)
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        dt = time.monotonic() - self.t0
        status = "OK" if exc_type is None else f"FAILED ({getattr(exc_type, '__name__', exc_type)})"
        print(f"[STAGE END]   {self.name} — {status} in {dt:.1f}s", flush=True)
        _heartbeat(self.name, f"end {dt:.1f}s")
        return False


# Futures-code aliases for HPTL_ONLY_MARKETS (small-scope test rebuilds).
_MARKET_CODE_ALIASES: dict[str, str] = {
    "GC": "Gold",
    "SI": "Silver",
    "HG": "Copper / HG",
    "CL": "Crude Oil / CL",
    "NG": "Natural Gas / NG",
}


def _selected_build_markets() -> list[str]:
    """TARGET_MARKETS, optionally narrowed via HPTL_ONLY_MARKETS=GC,SI,HG,CL,NG (codes or ids)."""
    from hptl.cot.cot_quarantine import quarantined_instrument_ids

    raw = os.environ.get("HPTL_ONLY_MARKETS", "").strip()
    if not raw:
        base = list(TARGET_MARKETS)
    else:
        base = _resolve_only_markets(raw)
    blocked = quarantined_instrument_ids()
    if blocked:
        active = [m for m in base if m not in blocked]
        print(
            f"COT quarantine: excluding {len(blocked)} instrument(s) from confluence build: "
            f"{', '.join(sorted(blocked))}",
            flush=True,
        )
        return active
    return base


def _resolve_only_markets(raw: str) -> list[str]:
    """Parse HPTL_ONLY_MARKETS env value into market ids."""
    if not raw.strip():
        return list(TARGET_MARKETS)
    wanted: list[str] = []
    for tok in raw.split(","):
        tok = tok.strip()
        if not tok:
            continue
        mapped = _MARKET_CODE_ALIASES.get(tok.upper(), tok)
        if mapped in TARGET_MARKETS:
            if mapped not in wanted:
                wanted.append(mapped)
        else:
            print(f"[ONLY_MARKETS] '{tok}' -> '{mapped}' not in TARGET_MARKETS; skipping.", flush=True)
    if not wanted:
        print("[ONLY_MARKETS] no valid markets resolved; falling back to full TARGET_MARKETS.", flush=True)
        return list(TARGET_MARKETS)
    print(f"[ONLY_MARKETS] restricting build to {len(wanted)} markets: {', '.join(wanted)}", flush=True)
    return wanted  # only reached via _resolve_only_markets


def tracked_master_csv_path() -> Path:
    return PROCESSED_DIR / TRACKED_MASTER_FILENAME

def _cftc_contract_code_str(raw: Any) -> str:
    """Normalize CFTC contract codes (numeric floats from Excel/CSV, trailing .0)."""
    if raw is None:
        return ""
    try:
        if pd.isna(raw):
            return ""
    except (TypeError, ValueError):
        pass
    s = str(raw).strip().upper()
    if len(s) > 2 and s.endswith(".0") and s[:-2].lstrip("-").isdigit():
        s = s[:-2]
    if s.isdigit():
        try:
            s = str(int(s))
        except ValueError:
            pass
        if 0 < len(s) <= 6:
            return s.zfill(6)
        return s
    try:
        f = float(s)
        if math.isfinite(f) and f == int(f):
            return str(int(f))
    except ValueError:
        pass
    return s


def _normalize_market_text(value: str) -> str:
    return " ".join(str(value).upper().replace("_", " ").replace("/", " ").replace("-", " ").split())


def _map_market(raw_market: str) -> str | None:
    normalized = _normalize_market_text(raw_market)
    for canonical, aliases in MARKET_ALIASES.items():
        if any(_normalize_market_text(alias) in normalized for alias in aliases):
            return canonical
    return None


def _finite_num(x: Any) -> bool:
    try:
        return x is not None and not pd.isna(x) and math.isfinite(float(x))
    except (TypeError, ValueError):
        return False


def _strict_pos(x: Any) -> bool:
    return _finite_num(x) and float(x) > 0


def _strict_neg(x: Any) -> bool:
    return _finite_num(x) and float(x) < 0


def _compute_positioning_state(
    net: Any,
    one_week_net: Any,
    four_week_net: Any,
    long_weekly: Any,
    short_weekly: Any,
) -> str:
    """Human-readable positioning regime (does not replace cot_bias / cot_score)."""
    if not _finite_num(net):
        return "N/A"
    n = float(net)
    if n == 0:
        return "Neutral"

    # Leg-flow signals take precedence when both legs move in the classic pattern.
    if n < 0 and _strict_pos(long_weekly) and _strict_neg(short_weekly):
        return "Accumulation"
    if n > 0 and _strict_neg(long_weekly) and _strict_pos(short_weekly):
        return "Distribution"

    w1, w4 = one_week_net, four_week_net
    if n < 0 and _strict_neg(w1) and _strict_neg(w4):
        return "Bearish Strengthening"
    if n < 0 and _strict_pos(w1) and _strict_pos(w4):
        return "Short Covering"
    if n < 0 and (_strict_pos(w1) or _strict_pos(w4)):
        return "Bearish Improving"
    if n > 0 and _strict_pos(w1) and _strict_pos(w4):
        return "Bullish Strengthening"
    if n > 0 and _strict_neg(w1) and _strict_neg(w4):
        return "Bullish Weakening"
    if n > 0 and (_strict_neg(w1) or _strict_neg(w4)):
        return "Bullish Softening"

    # Partial / transitional when strict quadrants do not apply
    bits: list[str] = []
    if _finite_num(w1):
        bits.append("1w net " + ("up" if float(w1) > 0 else "down" if float(w1) < 0 else "flat"))
    if _finite_num(w4):
        bits.append("4w net " + ("up" if float(w4) > 0 else "down" if float(w4) < 0 else "flat"))
    if bits:
        side = "Bearish" if n < 0 else "Bullish" if n > 0 else "Neutral"
        return f"Transition ({side}; {'; '.join(bits)})"
    return "Transition (momentum window incomplete)"


def _positioning_interpretation_detail(
    market: str,
    state: str,
    net: Any,
    long_v: Any,
    short_v: Any,
    one_week_net: Any,
    four_week_net: Any,
    long_weekly: Any,
    short_weekly: Any,
) -> str:
    """Explain how the state was inferred from net, momentum, and leg deltas."""
    parts: list[str] = []
    parts.append(
        f"{market}: labelled «{state}». Raw book: long {int(long_v) if _finite_num(long_v) else 'N/A'}, "
        f"short {int(short_v) if _finite_num(short_v) else 'N/A'}, "
        f"net {int(net) if _finite_num(net) else 'N/A'} (net = long − short)."
    )
    if _finite_num(one_week_net):
        leg_bits = ""
        if _finite_num(long_weekly) and _finite_num(short_weekly):
            leg_bits = (
                f" Leg deltas vs last report: longs {int(float(long_weekly)):+d}, "
                f"shorts {int(float(short_weekly)):+d}."
            )
        elif _finite_num(long_weekly):
            leg_bits = f" Longs vs last report: {int(float(long_weekly)):+d} (short leg delta N/A)."
        elif _finite_num(short_weekly):
            leg_bits = f" Shorts vs last report: {int(float(short_weekly)):+d} (long leg delta N/A)."
        parts.append(
            f"Versus the prior COT report, net changed by {int(float(one_week_net)):+d} contracts.{leg_bits}"
        )
    else:
        parts.append("Prior-week comparison is missing, so 1w net and leg deltas are not available for this row.")
    if _finite_num(four_week_net):
        parts.append(
            f"Versus four reports back, net changed by {int(float(four_week_net)):+d}—this is the same basis as the published 4w net change column."
        )
    else:
        parts.append("Four-report net change is not available yet (insufficient history).")
    if state == "Accumulation":
        parts.append(
            "Interpretation: still net-short, but longs added and shorts reduced—classic short-cover / dip-buying into a bearish net."
        )
    elif state == "Distribution":
        parts.append(
            "Interpretation: still net-long, but longs were cut and shorts grew—profit-taking / hedging into a bullish net."
        )
    elif state == "Short Covering":
        parts.append("Interpretation: net remains short, but both 1w and 4w net improved—pressure easing on the short side.")
    elif state == "Bearish Improving":
        parts.append("Interpretation: net short with at least one horizon showing net improvement—pressure easing selectively.")
    elif state.startswith("Bearish Strengthening"):
        parts.append("Interpretation: net short and deepening on both horizons—speculative bearish conviction building.")
    elif state.startswith("Bullish Strengthening"):
        parts.append("Interpretation: net long and building on both horizons—speculative bullish conviction building.")
    elif state.startswith("Bullish Weakening"):
        parts.append("Interpretation: net still long, but net deteriorated on both horizons—longs losing control of the tape.")
    elif state.startswith("Bullish Softening"):
        parts.append("Interpretation: net long but one horizon already cooling—early sign of profit-taking or two-way trade.")
    elif state.startswith("Neutral"):
        parts.append("Interpretation: positioning is effectively balanced after netting longs vs shorts.")
    else:
        parts.append(
            "Interpretation: 1w and 4w net changes do not line up in one clean box—treat as a regime shift or noisy week."
        )
    return " ".join(parts)


def _four_week_positioning_story(cot: pd.DataFrame, market: str, week_date: pd.Timestamp) -> str:
    """Plain-English flow over recent reports (no week-by-week contract arithmetic on the surface)."""
    cols = ["cot_report_date", "long_value", "short_value", "net_value"]
    g = cot.loc[cot["market"] == market, cols].dropna(subset=["cot_report_date"]).sort_values("cot_report_date")
    g = g.loc[g["cot_report_date"] <= week_date]
    if g.shape[0] < 2:
        return "Not enough consecutive reports yet for a monthly flow read."
    hist = g.tail(min(5, len(g))).copy()
    first, last = hist.iloc[0], hist.iloc[-1]
    sd = pd.Timestamp(first["cot_report_date"]).strftime("%Y-%m-%d")
    ed = pd.Timestamp(last["cot_report_date"]).strftime("%Y-%m-%d")

    def fv(x: Any) -> float | None:
        if not _finite_num(x):
            return None
        return float(x)

    n0, n1 = fv(first["net_value"]), fv(last["net_value"])
    l0, l1 = fv(first["long_value"]), fv(last["long_value"])
    s0, s1 = fv(first["short_value"]), fv(last["short_value"])

    bits: list[str] = [
        f"Over the last several COT prints (about a month, {sd} through {ed}), here's how participation evolved.",
    ]
    if all(v is not None for v in (n0, n1, l0, l1, s0, s1)):
        dn = n1 - n0
        dl = l1 - l0
        ds = s1 - s0
        long_side = n1 > 0 and n0 > 0
        short_side = n1 < 0 and n0 < 0
        if long_side:
            if dn > 500:
                bits.append("Specs stayed net long and added exposure through the window.")
            elif dn < -500:
                bits.append("The book stayed net long, but length was trimmed—upside participation cooled.")
            else:
                bits.append("Net long exposure was little changed—no major conviction shift on the net line.")
        elif short_side:
            if dn < -500:
                bits.append("Bearish positioning deepened—sellers stayed in control and pressed harder.")
            elif dn > 500:
                bits.append("The market remained net short, but pressure eased—often a sign of covering or two-way balance improving.")
            else:
                bits.append("Net short exposure was broadly stable—no fresh acceleration in either direction.")
        else:
            bits.append("Net positioning crossed or hugged flat versus the start of the window—treat as a transitional read.")

        if dl > 500 and ds > 500:
            bits.append("Both longs and shorts grew—choppy two-way trade, not a one-sided trend book.")
        elif dl > 500 and ds < -500:
            bits.append("Longs lifted while shorts contracted—classic demand-with-cover tone.")
        elif dl < -500 and ds > 500:
            bits.append("Longs were cut while shorts built—supply leaning into strength.")
        elif dl < -500 and ds < -500:
            bits.append("Both sides shrank—open interest likely leaking as conviction fades.")

    bits.append("Use the history table below if you need exact contract deltas.")
    return " ".join(bits)


def _institutional_flow_summary(
    market: str,
    state: str,
    net: Any,
    long_v: Any,
    short_v: Any,
    one_week_net: Any,
    four_week_net: Any,
    long_w1: Any,
    short_w1: Any,
    cot: pd.DataFrame,
    week_date: pd.Timestamp,
) -> str:
    """Trader-readable institutional commentary (does not alter COT/macro scores)."""
    if not _finite_num(net) or not _finite_num(long_v) or not _finite_num(short_v):
        return "Positioning data is incomplete for this report; institutional flow cannot be summarised."
    n = int(float(net))
    lg = int(float(long_v))
    sh = int(float(short_v))
    absn = abs(n)
    lean = "net long" if n > 0 else "net short" if n < 0 else "balanced"
    intensity = "heavily" if absn > 50000 else "materially" if absn > 10000 else "modestly"

    hist = (
        cot.loc[cot["market"] == market, ["cot_report_date", "long_value", "short_value", "net_value"]]
        .dropna(subset=["cot_report_date"])
        .sort_values("cot_report_date")
    )
    hist = hist.loc[hist["cot_report_date"] <= week_date].tail(5)
    long_up_weeks = 0
    short_down_weeks = 0
    weeks_pairs = 0
    for i in range(1, len(hist)):
        l0, l1 = hist.iloc[i - 1]["long_value"], hist.iloc[i]["long_value"]
        s0, s1 = hist.iloc[i - 1]["short_value"], hist.iloc[i]["short_value"]
        if pd.notna(l0) and pd.notna(l1):
            weeks_pairs += 1
            if float(l1) > float(l0):
                long_up_weeks += 1
        if pd.notna(s0) and pd.notna(s1) and float(s1) < float(s0):
            short_down_weeks += 1

    leg_sentence = ""
    if weeks_pairs >= 2 and long_up_weeks > 0:
        leg_sentence = f" Longs strengthened in {long_up_weeks} of the last {weeks_pairs} week-to-week steps in this window."
    if state in {"Accumulation", "Short Covering", "Bearish Improving"} and short_down_weeks >= 2:
        leg_sentence += f" Shorts were trimmed in {short_down_weeks} of those steps—consistent with two-way de-risking or cover."

    mom_parts: list[str] = []
    if _finite_num(one_week_net):
        w1 = float(one_week_net)
        if n < 0 and w1 > 0:
            mom_parts.append("The latest week worked against the bearish bias—net drifted back toward neutral.")
        elif n < 0 and w1 < 0:
            mom_parts.append("The latest week added to the bearish side—pressure still building.")
        elif n > 0 and w1 > 0:
            mom_parts.append("The latest week reinforced the long side.")
        elif n > 0 and w1 < 0:
            mom_parts.append("The latest week trimmed length—early sign of profit-taking or balance.")
        else:
            mom_parts.append("The latest week was quiet versus the prior print.")
    if _finite_num(four_week_net):
        w4 = float(four_week_net)
        if w4 > 500:
            mom_parts.append("Over four reports, net drifted supportive for longs.")
        elif w4 < -500:
            mom_parts.append("Over four reports, net drifted supportive for shorts.")
        else:
            mom_parts.append("The four-report net drift was small—no strong multi-week thrust.")

    state_sentence = {
        "Bullish Strengthening": "Bullish conviction is strengthening—both recent horizons add to the long bias.",
        "Bullish Weakening": "The book is still bullish, but momentum is fading on both 1w and 4w net changes.",
        "Bullish Softening": "Still net long, but at least one horizon is cooling—watch for a shift toward two-way trade.",
        "Bearish Strengthening": "Bearish conviction is strengthening—specs are pressing shorts on both horizons.",
        "Short Covering": "Exposure remains bearish, but short pressure is easing on both 1w and 4w—classic cover sequence.",
        "Bearish Improving": "Net positioning is still bearish, but the bleed is slowing or reversing on at least one horizon.",
        "Accumulation": "Despite a bearish net, institutions are lifting longs and cutting shorts—accumulation under the surface.",
        "Distribution": "Despite a bullish net, longs are being distributed and shorts are building—distribution under the surface.",
        "Neutral": "Positioning is effectively flat after netting longs versus shorts.",
    }.get(
        state,
        (
            f"Flow is labelled «{state}»—weekly history may still be building, or one-week and four-week effects are offsetting; use the trail table below."
            if str(state).startswith("Transition")
            else f"The current regime label is «{state}»—see detail panel for leg-level mechanics."
        ),
    )

    open_line = (
        f"{market}: institutions show a {intensity} {lean} lean. "
        f"{state_sentence}"
    )
    return (open_line + " " + " ".join(mom_parts) + leg_sentence).strip()


def _finite_scalar(x: Any) -> bool:
    try:
        return bool(pd.notna(float(x)))
    except (TypeError, ValueError):
        return False


def _trader_pressure_summary(
    positioning_state: str,
    net: Any,
    weekly: Any,
    four: Any,
    long_w1: Any,
    short_w1: Any,
) -> str:
    """One-line dominant-pressure read for the trader layer (does not replace scoring)."""
    st = str(positioning_state or "Unknown")
    if not _finite_scalar(net):
        return "N/A: net positioning unavailable."
    n = float(net)
    side = "bullish" if n > 0 else "bearish" if n < 0 else "neutral"
    mag = abs(n)
    if mag >= 50_000:
        book = f"Large {side} book"
    elif mag >= 10_000:
        book = f"Meaningful {side} lean"
    else:
        book = "Relatively small net lean"
    wk = ""
    if _finite_scalar(weekly):
        wv = float(weekly)
        if wv > 0:
            wk = "This week leaned long versus the prior report."
        elif wv < 0:
            wk = "This week leaned short versus the prior report."
        else:
            wk = "This week was flat versus the prior report."
    fw = ""
    if _finite_scalar(four):
        fv = float(four)
        if fv > 0:
            fw = "The last four reports built a mild long drift overall."
        elif fv < 0:
            fw = "The last four reports built a mild short drift overall."
        else:
            fw = "The last four reports showed little net drift."
    legs = ""
    if _finite_scalar(long_w1) and _finite_scalar(short_w1):
        lw = float(long_w1)
        sw = float(short_w1)
        if lw > 0 and sw < 0:
            legs = "Long adds with short cover dominated the week."
        elif lw < 0 and sw > 0:
            legs = "Long cuts with short adds dominated the week."
        elif lw > 0 and sw > 0:
            legs = "Both sides grew—two-way expansion."
        elif lw < 0 and sw < 0:
            legs = "Both sides shrank—open interest likely leaked out."
        else:
            legs = "Leg flows were mixed week-to-week."
    return " ".join(
        s
        for s in (
            f"{book}. Positioning reads as {st}.",
            wk,
            fw,
            legs,
        )
        if s
    ).strip()


def _trader_flow_change_summary(
    net: Any,
    weekly: Any,
    four: Any,
    prev_net: Any,
    cot_bias: str,
    positioning_state: str,
) -> str:
    """Plain-English week-over-week / multi-week flow narrative (interpretation only)."""
    if not _finite_scalar(net):
        return "N/A: insufficient history to compare weeks yet."
    n = float(net)
    lines: list[str] = []
    if _finite_scalar(weekly) and _finite_scalar(prev_net):
        w = float(weekly)
        pn = float(prev_net)
        if w > 0:
            if n >= 0:
                lines.append(
                    f"Compared with last week, managed money added {w:,.0f} contracts to net long lean "
                    f"(prior net {pn:+,.0f})."
                )
            else:
                lines.append(
                    f"Compared with last week, managed money eased the bearish book by {w:,.0f} contracts "
                    f"(prior net {pn:+,.0f})."
                )
        elif w < 0:
            if n > 0:
                lines.append(
                    f"Compared with last week, managed money trimmed the bullish book by {abs(w):,.0f} contracts "
                    f"(prior net {pn:+,.0f})."
                )
            else:
                lines.append(
                    f"Compared with last week, managed money pressed the bearish side by an additional "
                    f"{abs(w):,.0f} contracts (prior net {pn:+,.0f})."
                )
        else:
            lines.append("Compared with last week, managed-money net was unchanged.")
    elif _finite_scalar(weekly):
        lines.append(f"This week’s net change in managed-money positioning is {float(weekly):+,.0f} contracts.")
    if n != 0:
        lines.append("Longs remain dominant on net." if n > 0 else "Shorts remain dominant on net.")
    if _finite_scalar(four):
        f4 = float(four)
        if f4 > 0 and n < 0:
            lines.append(
                "The four-week trail still shows net buying of the bearish side easing—"
                "demand zones can matter more than chasing fresh supply breakdowns."
            )
        elif f4 < 0 and n > 0:
            lines.append(
                "The four-week trail shows some give-back of the bullish lean—"
                "watch supply for exhaustion rather than assuming trend continuation."
            )
        elif f4 > 0 and n > 0:
            lines.append("Four-week flow reinforces the bullish lean.")
        elif f4 < 0 and n < 0:
            lines.append("Four-week flow reinforces the bearish lean.")
    st = str(positioning_state or "")
    if "Improving" in st or "Covering" in st or "Accumulation" in st:
        lines.append(
            "Positioning is shifting toward a less one-sided bearish profile; "
            "favour mapping demand and reversal context over new breakdown shorts."
        )
    if "Distribution" in st or "Weakening" in st or "Softening" in st:
        lines.append(
            "Bullish urgency is cooling in the data; supply and distribution-style fades deserve more weight."
        )
    lines.append(f"COT bias label: «{cot_bias}» (interpretation layer; raw columns remain in audit).")
    return " ".join(lines)


def _trader_action_fields(
    positioning_state: str,
    net: Any,
    weekly: Any,
    four: Any,
    prev_net: Any,
    long_w1: Any,
    short_w1: Any,
    macro_regime: Any,
    macro_score: Any,
    cot_bias: str,
    has_positions: bool,
) -> dict[str, str]:
    """Interpretation-only: zone focus and trader notes (rules per product spec)."""
    wait_note = "Positioning is conflicted. Wait for price to reach a major HTF zone before acting."
    incomplete = "COT positioning row incomplete—wait for managed-money long/short before applying zone rules."
    if not has_positions:
        return {
            "pressure_summary": "N/A",
            "flow_change_summary": "N/A",
            "zone_to_watch": "Both / Wait",
            "trader_action_note": incomplete,
            "zone_focus": "Wait / Mixed",
        }
    if not _finite_scalar(net):
        return {
            "pressure_summary": "N/A",
            "flow_change_summary": "N/A",
            "zone_to_watch": "Both / Wait",
            "trader_action_note": incomplete,
            "zone_focus": "Wait / Mixed",
        }

    st = str(positioning_state or "Unknown")
    action_map: dict[str, tuple[str, str]] = {
        "Bullish Strengthening": (
            "Demand",
            "Look for demand zones for potential long setups.",
        ),
        "Bearish Strengthening": (
            "Supply",
            "Look for supply zones for potential short setups.",
        ),
        "Short Covering": (
            "Demand",
            "Bearish pressure is fading. Start watching demand zones or short-covering reversal setups.",
        ),
        "Bearish Improving": (
            "Demand",
            "Bearish pressure is fading. Start watching demand zones or short-covering reversal setups.",
        ),
        "Accumulation": (
            "Demand",
            "Bearish pressure is fading. Start watching demand zones or short-covering reversal setups.",
        ),
        "Bullish Softening": (
            "Supply",
            "Bullish pressure is fading. Start watching supply zones or long-exhaustion setups.",
        ),
        "Bullish Weakening": (
            "Supply",
            "Bullish pressure is fading. Start watching supply zones or long-exhaustion setups.",
        ),
        "Distribution": (
            "Supply",
            "Bullish pressure is fading. Start watching supply zones or long-exhaustion setups.",
        ),
    }
    zone, note = action_map.get(st, ("Both / Wait", wait_note))

    pressure_summary = _trader_pressure_summary(st, net, weekly, four, long_w1, short_w1)
    flow_change_summary = _trader_flow_change_summary(
        net, weekly, four, prev_net, str(cot_bias or ""), st
    )

    if zone == "Demand":
        zone_focus = "Look for Demand"
    elif zone == "Supply":
        zone_focus = "Look for Supply"
    else:
        zone_focus = "Wait / Mixed"

    try:
        ms = float(macro_score) if macro_score not in (None, "N/A") and pd.notna(macro_score) else None
    except (TypeError, ValueError):
        ms = None
    mr = str(macro_regime or "").lower()
    risk_offish = "risk_off" in mr or "risk-off" in mr or "risk off" in mr
    if zone_focus == "Look for Demand" and risk_offish and ms is not None and ms >= 4.0:
        zone_focus = "Demand watch, but macro headwind"
    if (
        zone_focus == "Look for Supply"
        and _finite_scalar(weekly)
        and float(weekly) > 0
        and float(net) < 0
    ):
        zone_focus = "Supply watch, but COT improving against shorts"

    return {
        "pressure_summary": pressure_summary,
        "flow_change_summary": flow_change_summary,
        "zone_to_watch": zone,
        "trader_action_note": note,
        "zone_focus": zone_focus,
    }


def _zone_decision_layer_fields(positioning_state: str, has_real_positions: bool) -> dict[str, str]:
    """Interpretation-only: next chart-zone focus based on institutional positioning regime."""
    wait_rule = {
        "zone_focus": "Wait",
        "setup_type": "No clean institutional edge",
        "confidence_label": "Low",
        "invalidation_note": "N/A",
        "next_data_watch": "Wait for clearer 1W/4W positioning direction.",
    }
    if not has_real_positions:
        return wait_rule

    st = str(positioning_state or "N/A")
    if st == "Bullish Strengthening":
        return {
            "zone_focus": "Demand",
            "setup_type": "Long continuation / demand reaction",
            "confidence_label": "Medium to High",
            "invalidation_note": "N/A",
            "next_data_watch": "Longs must stay stable or increase; warning if longs reduce sharply.",
        }
    if st == "Bearish Strengthening":
        return {
            "zone_focus": "Supply",
            "setup_type": "Short continuation / supply reaction",
            "confidence_label": "Medium to High",
            "invalidation_note": "N/A",
            "next_data_watch": "Shorts must stay stable or increase; warning if shorts reduce sharply.",
        }

    # Still net short, but bearish pressure is improving / covering / accumulating.
    if st in {"Bearish Improving", "Short Covering", "Accumulation"}:
        return {
            "zone_focus": "Demand first, Supply only at major HTF premium zones",
            "setup_type": "Short-covering rally / transition",
            "confidence_label": "Medium",
            "invalidation_note": "Fresh shorts are lower quality unless price is inside major HTF supply and new COT data shows selling pressure returning.",
            "next_data_watch": "Watch for shorts increasing again or net positioning deteriorating near supply.",
        }

    # Still net long, but bullish pressure is fading / distributing.
    if st in {"Bullish Weakening", "Distribution", "Bullish Softening"}:
        return {
            "zone_focus": "Supply first, Demand only at major HTF discount zones",
            "setup_type": "Long exhaustion / distribution watch",
            "confidence_label": "Medium",
            "invalidation_note": "Fresh longs are lower quality unless price is inside major HTF discount zones and new COT data shows buying pressure returning.",
            "next_data_watch": "Watch for longs reducing or shorts increasing near supply.",
        }

    # Mixed / conflicted / incomplete.
    return wait_rule


def _print_equity_index_candidates_in_processed() -> None:
    """Search commodity-style processed files; equity index rows are normally absent there."""
    needles = (
        "NASDAQ MINI",
        "NASDAQ-100",
        "E-MINI NASDAQ",
        "E-MINI S&P 500",
        "S&P 500 STOCK",
        "DJIA",
        "MINI DOW",
        "E-MINI DOW",
    )
    print("Equity index keyword search in cot_cleaned_*.csv (commodity pipeline):")
    for path in sorted(PROCESSED_DIR.glob("cot_cleaned_*.csv"), key=lambda p: p.stat().st_mtime):
        try:
            df = pd.read_csv(path, usecols=["market_and_exchange_names"], low_memory=False)
        except (ValueError, KeyError):
            df = pd.read_csv(path, low_memory=False)
            if "market_and_exchange_names" not in df.columns:
                continue
        names = df["market_and_exchange_names"].dropna().astype(str)
        hits = names[names.apply(lambda s: any(n.upper() in s.upper() for n in needles))]
        uniq = sorted(hits.unique().tolist())
        print(f"  {path.name}: {len(uniq)} candidate rows (sample up to 12)")
        for u in uniq[:12]:
            print(f"    - {u}")
    print("Financial index mapping (locked to CFTC financial futures disclosures):")
    for code, target in FINANCIAL_INDEX_CODE_TO_TARGET.items():
        print(f"  {code} -> {target}")


def _resolve_position_columns(df: pd.DataFrame, long_candidates: list[str], short_candidates: list[str]) -> tuple[str | None, str | None, str]:
    """Resolve long/short source columns from raw COT CSV fields.

    Returns (long_col, short_col, source_family) where source_family is one of
    managed_money | noncommercial | unknown.
    """
    long_col = _find_column(df, *long_candidates)
    short_col = _find_column(df, *short_candidates)
    if long_col is None or short_col is None:
        return long_col, short_col, "unknown"

    managed_hints = {
        "managed_money",
        "m_money",
        "money_manager",
        "lev_money",
        "leveraged_money",
    }
    normalized_pair = f"{long_col} {short_col}".lower()
    source_family = "managed_money" if any(hint in normalized_pair for hint in managed_hints) else "noncommercial"
    return long_col, short_col, source_family


def _parse_cot_report_dates(raw_dates: pd.Series, source_name: str) -> pd.Series:
    """Parse COT report dates explicitly from known formats only."""
    raw = raw_dates.astype(str).str.strip()
    parsed = pd.Series(pd.NaT, index=raw.index, dtype="datetime64[ns]")

    slash_date_mask = raw.str.match(r"^\d{1,2}/\d{1,2}/\d{4}$", na=False)
    slash_first = pd.to_numeric(raw.loc[slash_date_mask].str.split("/").str[0], errors="coerce")
    slash_second = pd.to_numeric(raw.loc[slash_date_mask].str.split("/").str[1], errors="coerce")
    # Determine slash format explicitly:
    # - if first token ever > 12 => DD/MM/YYYY
    # - elif second token ever > 12 => MM/DD/YYYY
    # - else ambiguous; default to MM/DD/YYYY
    slash_format = "%m/%d/%Y"
    slash_format_label = "MM/DD/YYYY"
    if (slash_first > 12).any():
        slash_format = "%d/%m/%Y"
        slash_format_label = "DD/MM/YYYY"
    elif (slash_second > 12).any():
        slash_format = "%m/%d/%Y"
        slash_format_label = "MM/DD/YYYY"

    fmt_counts = {
        "YYYY-MM-DD": int(raw.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False).sum()),
        "slash_dates": int(slash_date_mask.sum()),
        "slash_detected_as": slash_format_label,
        "YYYYMMDD": int(raw.str.match(r"^\d{8}$", na=False).sum()),
    }
    # prefer unambiguous ISO, then compact ISO, then US slash dates
    iso_mask = raw.str.match(r"^\d{4}-\d{2}-\d{2}$", na=False)
    compact_iso_mask = raw.str.match(r"^\d{8}$", na=False)
    slash_mask = slash_date_mask

    parsed.loc[iso_mask] = pd.to_datetime(raw.loc[iso_mask], format="%Y-%m-%d", errors="coerce")
    parsed.loc[compact_iso_mask] = pd.to_datetime(raw.loc[compact_iso_mask], format="%Y%m%d", errors="coerce")
    parsed.loc[slash_mask] = pd.to_datetime(raw.loc[slash_mask], format=slash_format, errors="coerce")

    print(f"Date format inspection ({source_name}): {fmt_counts}")
    print(f"Raw report_date sample ({source_name}): {raw.head(12).tolist()}")
    print(f"Parsed report_date sample ({source_name}): {parsed.dt.strftime('%Y-%m-%d').head(12).tolist()}")
    return parsed.dt.normalize()


def _apply_net_anchored_cot_scoring(cot: pd.DataFrame) -> pd.DataFrame:
    """Probabilistic COT scoring (signal strength vs confidence, persistence, price response)."""
    return apply_probabilistic_cot_scoring(cot)


def _load_financial_index_year(
    year: int,
    candidate_long_cols: list[str],
    candidate_short_cols: list[str],
) -> pd.DataFrame | None:
    """Load CME/CBOT equity index rows from CFTC financial futures annual file."""
    from hptl.cot.financial_index_loader import ensure_financial_index_year_cache

    cache_path = ensure_financial_index_year_cache(year)
    if cache_path is None or not cache_path.exists():
        return None
    filtered = pd.read_csv(cache_path, low_memory=False)

    if filtered.empty:
        return None

    market_col = _find_column(filtered, "market_and_exchange_names")
    date_col = _find_column(filtered, "report_date_as_yyyy_mm_dd", "cot_report_date", "report_date", "date")
    long_col, short_col, source_family = _resolve_position_columns(filtered, candidate_long_cols, candidate_short_cols)
    if market_col is None or date_col is None or long_col is None or short_col is None:
        print(
            "WARNING: could not resolve required columns on financial index frame:",
            {"market_col": market_col, "date_col": date_col, "long_col": long_col, "short_col": short_col},
        )
        return None

    code_col2 = _find_column(filtered, "cftc_contract_market_code", "cftc_market_code")
    if code_col2 is None:
        return None

    x = pd.DataFrame()
    code_series = filtered[code_col2].map(_cftc_contract_code_str)
    x["market"] = code_series.map(FINANCIAL_INDEX_CODE_TO_TARGET)
    x["raw_cftc_market_name"] = filtered[market_col].astype(str).str.strip()
    x["cot_report_date"] = _parse_cot_report_dates(filtered[date_col], source_name=f"fut_fin_txt_{year}.csv")
    x["long_value"] = pd.to_numeric(filtered[long_col], errors="coerce")
    x["short_value"] = pd.to_numeric(filtered[short_col], errors="coerce")
    x["long_col_used"] = long_col
    x["short_col_used"] = short_col
    x["position_source_family"] = source_family
    x["missing_reason"] = pd.NA
    x = x.dropna(subset=["market", "cot_report_date"]).copy()
    x["net_value"] = x["long_value"] - x["short_value"]
    missing_positions = x["long_value"].isna() | x["short_value"].isna()
    x.loc[missing_positions, "net_value"] = pd.NA
    x.loc[missing_positions, "missing_reason"] = "missing long/short values in resolved source columns"
    zero_pair = x["long_value"].eq(0) & x["short_value"].eq(0)
    x.loc[zero_pair, "net_value"] = pd.NA
    x.loc[zero_pair, "missing_reason"] = "long and short are both 0 in source row (treated as invalid/stale)"
    x["quality_score"] = (
        x["long_value"].notna().astype(int) * 5
        + x["short_value"].notna().astype(int) * 5
        + (~zero_pair).astype(int) * 5
        + x["net_value"].notna().astype(int) * 5
    )
    sample = (
        x[x["cot_report_date"] == x["cot_report_date"].max()]
        .sort_values("market")[["market", "raw_cftc_market_name", "long_value", "short_value", "net_value"]]
        .head(6)
        if not x.empty
        else pd.DataFrame()
    )
    print(f"Financial index confirmation sample (latest date in {year} file):\n{sample.to_string(index=False)}")
    return x


def _merge_cot_from_cleaned_csvs() -> pd.DataFrame:
    """Legacy COT only — ``data/legacy_cot_latest.json`` (no TFF / disaggregated / fut_fin)."""
    from hptl.cot.legacy_cot_loader import legacy_cot_latest_path, load_legacy_positioning_decision_rows

    path = legacy_cot_latest_path()
    print(f"LEGACY_COT_MERGE: loading positioning from {path}")
    cot = load_legacy_positioning_decision_rows(eligible_only=True)
    if cot.empty:
        print("LEGACY_COT_MERGE: empty — run python -m hptl.cot.run_legacy_cot")
        return cot

    today_utc = pd.Timestamp(datetime.now(timezone.utc).date())
    future_mask = cot["cot_report_date"] > today_utc
    if future_mask.any():
        cot = cot.loc[~future_mask].copy()
    print(
        f"LEGACY_COT_MERGE: rows={len(cot)} markets={cot['market'].nunique()} "
        f"latest={cot['cot_report_date'].max()}"
    )
    return cot.sort_values(["market", "cot_report_date"]).reset_index(drop=True)


def _finalize_cot_pipeline(cot: pd.DataFrame) -> pd.DataFrame:
    """Weekly / 4W diffs and COT scoring (unchanged math)."""
    cot = cot.sort_values(["market", "cot_report_date"]).reset_index(drop=True)
    cot["weekly_change"] = cot.groupby("market")["net_value"].diff(1)
    cot["four_week_change"] = cot.groupby("market")["net_value"].diff(4)
    cot["long_weekly_change"] = cot.groupby("market")["long_value"].diff(1)
    cot["short_weekly_change"] = cot.groupby("market")["short_value"].diff(1)
    cot["managed_money_net"] = cot["net_value"]
    cot["noncommercial_net"] = cot["net_value"]
    cot["commercial_net"] = cot["net_value"]
    cot["mm_weekly_change"] = cot["weekly_change"]
    cot["mm_four_week_change"] = cot["four_week_change"]
    return _apply_net_anchored_cot_scoring(cot)


def _maybe_print_natural_gas_integrity_report(cot: pd.DataFrame, *, data_source: str) -> None:
    """Optional validation for NYMEX Henry Hub (023651) mapping — set HPTL_DEBUG_NATURAL_GAS=1."""
    flag = os.environ.get("HPTL_DEBUG_NATURAL_GAS", "").strip().lower()
    if flag not in ("1", "true", "yes"):
        return
    if cot.empty or "market" not in cot.columns:
        print("NAT_GAS_DEBUG: empty COT frame — nothing to report.")
        return
    ng = cot.loc[cot["market"] == "Natural Gas / NG"].sort_values("cot_report_date")
    print("=" * 72)
    print(f"NAT_GAS_INTEGRITY_REPORT source={data_source} rows={len(ng)}")
    print("=" * 72)
    if ng.empty:
        print("  (no rows mapped to 'Natural Gas / NG' — check MARKET_ALIASES vs CFTC labels)")
        print("=" * 72)
        return
    raws = sorted({str(x) for x in ng["raw_cftc_market_name"].dropna().unique().tolist()})
    print(f"  raw_cftc_market_name variants ({len(raws)}): {raws}")
    lg = pd.to_numeric(ng["long_value"], errors="coerce")
    sh = pd.to_numeric(ng["short_value"], errors="coerce")
    print(f"  managed_money_long: min={lg.min()} max={lg.max()} zero_weeks={int((lg == 0).sum())} na={int(lg.isna().sum())}")
    print(f"  managed_money_short: min={sh.min()} max={sh.max()} na={int(sh.isna().sum())}")
    if "long_col_used" in ng.columns:
        print(f"  long_col_used: {sorted({str(x) for x in ng['long_col_used'].dropna().unique()})}")
    if "short_col_used" in ng.columns:
        print(f"  short_col_used: {sorted({str(x) for x in ng['short_col_used'].dropna().unique()})}")
    tail_cols = [
        "cot_report_date",
        "raw_cftc_market_name",
        "long_col_used",
        "long_value",
        "short_value",
        "net_value",
        "weekly_change",
    ]
    have = [c for c in tail_cols if c in ng.columns]
    print("  last 12 weeks (wide):")
    print(ng[have].tail(12).to_string(index=False))
    print("=" * 72)


def _refresh_equity_index_on_cot(cot: pd.DataFrame) -> pd.DataFrame:
    """Disabled — Legacy COT is the sole positioning source (no fut_fin overlay)."""
    return cot


def _tracked_master_is_legacy_positioning(cot: pd.DataFrame) -> bool:
    """Reject cached master still on TFF/disaggregated columns (pre–Legacy COT reset)."""
    if cot.empty:
        return False
    if "position_source_family" in cot.columns:
        fam = cot["position_source_family"].astype(str)
        if fam.str.contains("legacy", case=False, na=False).all():
            return True
        if fam.str.contains("lev_money|financial|fut_fin", case=False, na=False).any():
            return False
    if "positioning_source" in cot.columns:
        src = cot["positioning_source"].astype(str)
        if src.str.contains("legacy_cot", case=False, na=False).all():
            return True
    for col in ("long_col_used", "trader_group_used"):
        if col in cot.columns:
            if cot[col].astype(str).str.contains("lev_money|leveraged_money", case=False, na=False).any():
                return False
            if cot[col].astype(str).str.contains("Noncommercial", case=False, na=False).any():
                return True
    return False


def _load_cot_history_from_master() -> pd.DataFrame | None:
    """Load pre-merged tracked COT master CSV if present and Legacy-sourced."""
    path = tracked_master_csv_path()
    if not path.exists():
        return None
    cot = pd.read_csv(path, low_memory=False)
    if "cot_report_date" not in cot.columns:
        print(f"WARNING: {path.name} missing cot_report_date — ignoring master.")
        return None
    cot["cot_report_date"] = pd.to_datetime(cot["cot_report_date"], errors="coerce").dt.normalize()
    cot = cot.sort_values(["market", "cot_report_date", "quality_score"], ascending=[True, True, False]).drop_duplicates(
        ["market", "cot_report_date"], keep="first"
    )
    if not _tracked_master_is_legacy_positioning(cot):
        print(
            f"COT_TRACKED_MASTER: ignoring {path.name} — not Legacy NC "
            f"(rebuild: python -m hptl.confluence.cot_tracked_backfill)"
        )
        return None
    if "cot_bias" not in cot.columns or "weekly_change" not in cot.columns:
        print(f"COT_TRACKED_MASTER: recomputing diffs/scores from {path.name} (legacy or partial columns).")
        return _finalize_cot_pipeline(cot)
    print(f"COT_TRACKED_MASTER: loaded legacy master {path.name} rows={len(cot)} markets={cot['market'].nunique()}")
    return cot


def _load_cot_history() -> pd.DataFrame:
    master = _load_cot_history_from_master()
    if master is not None and not master.empty:
        master = _refresh_equity_index_on_cot(master)
        today_utc = pd.Timestamp(datetime.now(timezone.utc).date())
        future_mask = master["cot_report_date"] > today_utc
        if future_mask.any():
            master = master.loc[~future_mask].copy()
        _print_hist_floor_coverage(master)
        _print_matched_raw_names(master)
        _print_latest_traced_by_market(master)
        _maybe_print_natural_gas_integrity_report(master, data_source=tracked_master_csv_path().name)
        print("LEGACY_COT: tracked master loaded — fut_fin overlay disabled")
        return master

    cot = _merge_cot_from_cleaned_csvs()
    if cot.empty:
        _maybe_print_natural_gas_integrity_report(cot, data_source="legacy_cot(empty)")
        return pd.DataFrame()
    _print_hist_floor_coverage(cot)
    _print_matched_raw_names(cot)
    _print_latest_traced_by_market(cot)
    finalized = _finalize_cot_pipeline(cot)
    _maybe_print_natural_gas_integrity_report(finalized, data_source="legacy_cot+finalized")
    return finalized


def _print_matched_raw_names(cot: pd.DataFrame) -> None:
    matched = cot.groupby("market")["raw_cftc_market_name"].apply(lambda s: sorted(set(s.dropna().astype(str).tolist()))).to_dict()
    col_trace = (
        cot.groupby("market")[["long_col_used", "short_col_used", "position_source_family"]]
        .agg(lambda s: sorted(set(s.astype(str).tolist())))
        .to_dict("index")
    )
    print("Matched raw market names by tracked market:")
    for m in TARGET_MARKETS:
        print(f"  {m}: {matched.get(m, [])}")
        print(f"    columns: {col_trace.get(m, {})}")


def _print_latest_traced_by_market(cot: pd.DataFrame) -> None:
    latest_date = cot["cot_report_date"].max()
    latest_rows = cot[cot["cot_report_date"] == latest_date] if pd.notna(latest_date) else pd.DataFrame()
    print(f"Latest traced values by tracked market (date={latest_date.date() if pd.notna(latest_date) else 'N/A'}):")
    for market in TARGET_MARKETS:
        r = latest_rows[latest_rows["market"] == market]
        if r.empty:
            print(
                f"  {market}: raw_market=N/A | long_col_used=N/A | short_col_used=N/A | long=N/A | short=N/A | net=N/A | missing_reason=no mapped raw COT row"
            )
            continue
        row = r.iloc[-1]
        print(
            f"  {market}: raw_market={row.get('raw_cftc_market_name')} | "
            f"long_col_used={row.get('long_col_used')} | short_col_used={row.get('short_col_used')} | "
            f"long={None if pd.isna(row.get('long_value')) else float(row.get('long_value'))} | "
            f"short={None if pd.isna(row.get('short_value')) else float(row.get('short_value'))} | "
            f"net={None if pd.isna(row.get('net_value')) else float(row.get('net_value'))} | "
            f"missing_reason={row.get('missing_reason')}"
        )


def _load_macro_history() -> pd.DataFrame:
    files = sorted(EXPORT_DIR.glob("macro_history_*.xlsx"), key=lambda p: p.stat().st_mtime)
    if not files:
        files = sorted(EXPORT_DIR.glob("macro_output_*.xlsx"), key=lambda p: p.stat().st_mtime)
    frames = []
    for path in files:
        for sheet in ["Macro_History", "Macro_Dashboard"]:
            try:
                m = pd.read_excel(path, sheet_name=sheet)
                break
            except Exception:
                m = None
        if m is None or m.empty or "macro_snapshot_date" not in m.columns:
            continue
        y = m.copy()
        y["macro_snapshot_date"] = pd.to_datetime(y["macro_snapshot_date"], errors="coerce").dt.normalize()
        y["macro_signal"] = y.get("macro_signal", "").astype(str).str.strip().str.lower()
        y["macro_score"] = pd.to_numeric(y.get("macro_score"), errors="coerce")
        y = y[y["macro_snapshot_date"].notna()].copy()
        frames.append(y)
    if not frames:
        return pd.DataFrame(columns=["macro_snapshot_date", "macro_signal", "macro_score"])
    return pd.concat(frames, ignore_index=True).sort_values("macro_snapshot_date").drop_duplicates("macro_snapshot_date", keep="last")


def _load_rates_clean_for_macro_audit() -> pd.DataFrame:
    if not RATES_CLEAN_PATH.exists():
        return pd.DataFrame()
    r = pd.read_csv(RATES_CLEAN_PATH, parse_dates=["date"], low_memory=False)
    r["date"] = pd.to_datetime(r["date"], errors="coerce").dt.normalize()
    return r.sort_values("date").reset_index(drop=True)


def _rates_row_for_macro_audit(macro_row: pd.Series | None, rates: pd.DataFrame) -> pd.Series | None:
    """Prefer processed rates_clean row on macro_snapshot_date (has fed_funds_* deltas)."""
    if macro_row is None:
        return None
    snap = macro_row.get("macro_snapshot_date")
    if snap is None or pd.isna(snap) or rates.empty:
        return None
    snap_ts = pd.Timestamp(snap).normalize()
    hit = rates.loc[rates["date"] == snap_ts]
    if not hit.empty:
        return hit.iloc[-1]
    try:
        if _row_has_required_scoring_inputs(macro_row):
            return macro_row
    except (TypeError, ValueError, KeyError):
        pass
    return None


def _empirical_percentile_rank(window: np.ndarray, value: float) -> float:
    """Percentile in [0, 100]: tie-aware rank / n over finite ``window``."""
    w = window[np.isfinite(window)]
    if w.size == 0 or not math.isfinite(value):
        return float("nan")
    if w.size == 1:
        return 50.0
    below = int(np.sum(w < value))
    eq = int(np.sum(w == value))
    return 100.0 * (below + 0.5 * eq) / w.size


def _net_rank_label(net: float, pct: float) -> str:
    if not math.isfinite(net) or not math.isfinite(pct):
        return "N/A"
    if net > 0:
        if pct >= 90:
            return "Historically very elevated net long positioning."
        if pct >= 75:
            return "Historically elevated net long positioning."
        if pct >= 50:
            return "Above the median historically for net long readings."
        if pct >= 25:
            return "Below the median historically for net long readings."
        return "Historically light net long positioning."
    if net < 0:
        if pct <= 10:
            return "Historically extreme net bearish positioning."
        if pct <= 25:
            return "Historically deep net bearish positioning."
        if pct <= 40:
            return "Historically net bearish but not at extreme lows."
        if pct <= 60:
            return "Mid-range historically for net bearish readings."
        return "Historically modest net short positioning."
    return "Near flat net versus historical readings."


def _json_safe_num(x: Any) -> float | None:
    if x is None:
        return None
    try:
        if isinstance(x, (float, int)) and not math.isfinite(float(x)):
            return None
        if pd.isna(x):
            return None
    except (TypeError, ValueError):
        return None
    try:
        v = float(x)
        return None if not math.isfinite(v) else round(v, 4)
    except (TypeError, ValueError):
        return None


def _json_safe_contract_int(x: Any) -> int | None:
    v = _json_safe_num(x)
    if v is None:
        return None
    return int(round(v))


def _hist_context_block_from_row(row: pd.Series, prefix: str) -> dict[str, Any]:
    """Build one nested historical context object (expanding_* or full_loaded_* columns)."""
    pl = _json_safe_num(row.get(f"{prefix}_current_long_percentile"))
    ps = _json_safe_num(row.get(f"{prefix}_current_short_percentile"))
    pn = _json_safe_num(row.get(f"{prefix}_current_net_percentile"))
    ru = row.get(f"{prefix}_rows_used")
    rows_used: int | None
    try:
        if ru is None or pd.isna(ru):
            rows_used = None
        else:
            rows_used = int(ru)
    except (TypeError, ValueError):
        rows_used = None
    return {
        "rows_used": rows_used,
        "earliest_report_date": None if row.get(f"{prefix}_earliest_report_date") is None else str(row.get(f"{prefix}_earliest_report_date")),
        "latest_report_date": None if row.get(f"{prefix}_latest_report_date") is None else str(row.get(f"{prefix}_latest_report_date")),
        "long_max": _json_safe_contract_int(row.get(f"{prefix}_long_max")),
        "long_min": _json_safe_contract_int(row.get(f"{prefix}_long_min")),
        "short_max": _json_safe_contract_int(row.get(f"{prefix}_short_max")),
        "short_min": _json_safe_contract_int(row.get(f"{prefix}_short_min")),
        "net_max": _json_safe_contract_int(row.get(f"{prefix}_net_max")),
        "net_min": _json_safe_contract_int(row.get(f"{prefix}_net_min")),
        "current_long_percentile": None if pl is None else round(pl, 1),
        "current_short_percentile": None if ps is None else round(ps, 1),
        "current_net_percentile": None if pn is None else round(pn, 1),
        "current_net_rank_label": None if row.get(f"{prefix}_current_net_rank_label") is None else str(row.get(f"{prefix}_current_net_rank_label")),
        "summary": None if row.get(f"{prefix}_context_summary") is None else str(row.get(f"{prefix}_context_summary")),
    }


def _rolling_3y_context_block_from_row(row: pd.Series) -> dict[str, Any]:
    """Serialize the rolling 3-year (156-week) positioning context block."""
    pl = _json_safe_num(row.get("rolling_3y_long_percentile"))
    ps = _json_safe_num(row.get("rolling_3y_short_percentile"))
    pn = _json_safe_num(row.get("rolling_3y_net_percentile"))
    po = _json_safe_num(row.get("rolling_3y_oi_percentile"))
    pl = None if pl is None else round(pl, 1)
    ps = None if ps is None else round(ps, 1)
    pn = None if pn is None else round(pn, 1)
    po = None if po is None else round(po, 1)

    ru = row.get("rolling_3y_rows_used")
    try:
        rows_used = None if ru is None or pd.isna(ru) else int(ru)
    except (TypeError, ValueError):
        rows_used = None
    ww = row.get("rolling_3y_window_weeks")
    try:
        window_weeks = WINDOW_WEEKS_3Y if ww is None or pd.isna(ww) else int(ww)
    except (TypeError, ValueError):
        window_weeks = WINDOW_WEEKS_3Y

    classification_lines = [
        line
        for line in (
            classification_line(METRIC_NET, pn),
            classification_line(METRIC_LONG, pl),
            classification_line(METRIC_SHORT, ps),
            classification_line(METRIC_OI, po),
        )
        if line
    ]
    long_vs_max = _json_safe_num(row.get("rolling_3y_long_vs_max_pct"))
    short_vs_max = _json_safe_num(row.get("rolling_3y_short_vs_max_pct"))
    net_range = _json_safe_num(row.get("rolling_3y_net_range_pct"))
    oi_vs_max = _json_safe_num(row.get("rolling_3y_oi_vs_max_pct"))
    long_vs_max = None if long_vs_max is None else round(long_vs_max, 1)
    short_vs_max = None if short_vs_max is None else round(short_vs_max, 1)
    net_range = None if net_range is None else round(net_range, 1)
    oi_vs_max = None if oi_vs_max is None else round(oi_vs_max, 1)
    long_crowding = str(row.get("rolling_3y_long_crowding") or "N/A")
    short_crowding = str(row.get("rolling_3y_short_crowding") or "N/A")
    oi_participation = str(row.get("rolling_3y_oi_participation") or "N/A")
    crowding_classification_lines = [
        line for line in (long_crowding, short_crowding, oi_participation) if line and line != "N/A"
    ]
    summary_parts = []
    if pn is not None:
        summary_parts.append(f"Net {pn:.0f}th pct — {interpret_metric(METRIC_NET, pn)}")
    if pl is not None:
        summary_parts.append(f"Long {pl:.0f}th pct — {interpret_metric(METRIC_LONG, pl)}")
    if ps is not None:
        summary_parts.append(f"Short {ps:.0f}th pct — {interpret_metric(METRIC_SHORT, ps)}")
    if po is not None:
        summary_parts.append(f"OI {po:.0f}th pct — {interpret_metric(METRIC_OI, po)}")
    summary = (
        "\n".join(
            [
                (
                    f"Rolling {window_weeks}-week (3Y) positioning context using the trailing "
                    f"{rows_used if rows_used is not None else 'N/A'} reports "
                    f"({row.get('rolling_3y_earliest_report_date')} → "
                    f"{row.get('rolling_3y_latest_report_date')})."
                ),
                *summary_parts,
            ]
        )
        if summary_parts
        else (
            "N/A: insufficient multi-year history loaded for rolling 3Y positioning context."
        )
    )

    return {
        "window_weeks": window_weeks,
        "rows_used": rows_used,
        "earliest_report_date": (
            None
            if row.get("rolling_3y_earliest_report_date") is None
            else str(row.get("rolling_3y_earliest_report_date"))
        ),
        "latest_report_date": (
            None
            if row.get("rolling_3y_latest_report_date") is None
            else str(row.get("rolling_3y_latest_report_date"))
        ),
        "long_min": _json_safe_contract_int(row.get("rolling_3y_long_min")),
        "long_max": _json_safe_contract_int(row.get("rolling_3y_long_max")),
        "long_avg": _json_safe_contract_int(row.get("rolling_3y_long_avg")),
        "short_min": _json_safe_contract_int(row.get("rolling_3y_short_min")),
        "short_max": _json_safe_contract_int(row.get("rolling_3y_short_max")),
        "short_avg": _json_safe_contract_int(row.get("rolling_3y_short_avg")),
        "net_min": _json_safe_contract_int(row.get("rolling_3y_net_min")),
        "net_max": _json_safe_contract_int(row.get("rolling_3y_net_max")),
        "net_avg": _json_safe_contract_int(row.get("rolling_3y_net_avg")),
        "oi_min": _json_safe_contract_int(row.get("rolling_3y_oi_min")),
        "oi_max": _json_safe_contract_int(row.get("rolling_3y_oi_max")),
        "oi_avg": _json_safe_contract_int(row.get("rolling_3y_oi_avg")),
        "long_percentile": pl,
        "short_percentile": ps,
        "net_percentile": pn,
        "oi_percentile": po,
        "long_class": classify_percentile(pl),
        "short_class": classify_percentile(ps),
        "net_class": classify_percentile(pn),
        "oi_class": classify_percentile(po),
        "net_interpretation": interpret_metric(METRIC_NET, pn),
        "long_interpretation": interpret_metric(METRIC_LONG, pl),
        "short_interpretation": interpret_metric(METRIC_SHORT, ps),
        "oi_interpretation": interpret_metric(METRIC_OI, po),
        "classification_lines": classification_lines,
        "current_long": _json_safe_contract_int(row.get("rolling_3y_current_long")),
        "current_short": _json_safe_contract_int(row.get("rolling_3y_current_short")),
        "current_net": _json_safe_contract_int(row.get("rolling_3y_current_net")),
        "current_oi": _json_safe_contract_int(row.get("rolling_3y_current_oi")),
        "long_vs_3y_max_pct": long_vs_max,
        "short_vs_3y_max_pct": short_vs_max,
        "net_range_pct": net_range,
        "oi_vs_3y_max_pct": oi_vs_max,
        "long_crowding": long_crowding,
        "short_crowding": short_crowding,
        "oi_participation": oi_participation,
        "crowding_classification_lines": crowding_classification_lines,
        "summary": summary,
    }


def _rolling_3y_context_missing() -> dict[str, Any]:
    return {
        "window_weeks": WINDOW_WEEKS_3Y,
        "rows_used": None,
        "earliest_report_date": None,
        "latest_report_date": None,
        "long_min": None,
        "long_max": None,
        "long_avg": None,
        "short_min": None,
        "short_max": None,
        "short_avg": None,
        "net_min": None,
        "net_max": None,
        "net_avg": None,
        "oi_min": None,
        "oi_max": None,
        "oi_avg": None,
        "long_percentile": None,
        "short_percentile": None,
        "net_percentile": None,
        "oi_percentile": None,
        "long_class": "N/A",
        "short_class": "N/A",
        "net_class": "N/A",
        "oi_class": "N/A",
        "net_interpretation": "N/A",
        "long_interpretation": "N/A",
        "short_interpretation": "N/A",
        "oi_interpretation": "N/A",
        "classification_lines": [],
        "current_long": None,
        "current_short": None,
        "current_net": None,
        "current_oi": None,
        "long_vs_3y_max_pct": None,
        "short_vs_3y_max_pct": None,
        "net_range_pct": None,
        "oi_vs_3y_max_pct": None,
        "long_crowding": "N/A",
        "short_crowding": "N/A",
        "oi_participation": "N/A",
        "crowding_classification_lines": [],
        "summary": "N/A: no COT row for this market and date — 3Y positioning context unavailable.",
    }


def _historical_json_fields_from_row(row: pd.Series) -> dict[str, Any]:
    """Serialize dual-mode historical positioning for JSON (no flat legacy keys)."""
    return {
        "expanding_history_context": _hist_context_block_from_row(row, "expanding"),
        "full_loaded_history_context": _hist_context_block_from_row(row, "full_loaded"),
        "rolling_3y_history_context": _rolling_3y_context_block_from_row(row),
    }


def _historical_json_fields_missing() -> dict[str, Any]:
    empty = {
        "rows_used": None,
        "earliest_report_date": None,
        "latest_report_date": None,
        "long_max": None,
        "long_min": None,
        "short_max": None,
        "short_min": None,
        "net_max": None,
        "net_min": None,
        "current_long_percentile": None,
        "current_short_percentile": None,
        "current_net_percentile": None,
        "current_net_rank_label": "N/A",
        "summary": "N/A: no COT row for this market and date — historical context unavailable.",
    }
    return {
        "expanding_history_context": dict(empty),
        "full_loaded_history_context": dict(empty),
        "rolling_3y_history_context": _rolling_3y_context_missing(),
    }


def _build_expanding_historical_stats(cot: pd.DataFrame) -> pd.DataFrame:
    """Expanding-window stats through each report date only (no look-ahead / backtest-safe)."""
    out_rows: list[dict[str, Any]] = []
    for market in TARGET_MARKETS:
        m = cot.loc[cot["market"] == market, ["cot_report_date", "long_value", "short_value", "net_value"]].sort_values(
            "cot_report_date"
        )
        if m.empty:
            continue
        dates = m["cot_report_date"].to_numpy()
        longs = m["long_value"].to_numpy(dtype=float)
        shorts = m["short_value"].to_numpy(dtype=float)
        nets = m["net_value"].to_numpy(dtype=float)
        series_start = pd.Timestamp(dates[0]).strftime("%Y-%m-%d")
        n_m = len(m)
        for i in range(n_m):
            lg = longs[: i + 1]
            sh = shorts[: i + 1]
            nt = nets[: i + 1]
            cur_dt = pd.Timestamp(dates[i]).strftime("%Y-%m-%d")
            n_long = int(np.sum(np.isfinite(lg)))
            n_short = int(np.sum(np.isfinite(sh)))
            n_net = int(np.sum(np.isfinite(nt)))
            joint = np.isfinite(lg) & np.isfinite(sh) & np.isfinite(nt)
            n_joint = int(np.sum(joint))

            row_out: dict[str, Any] = {
                "market": market,
                "cot_report_date": dates[i],
                "expanding_rows_used": i + 1,
                "expanding_earliest_report_date": series_start,
                "expanding_latest_report_date": cur_dt,
            }

            if not (math.isfinite(longs[i]) and math.isfinite(shorts[i]) and math.isfinite(nets[i])):
                row_out.update(
                    {
                        "expanding_long_max": None,
                        "expanding_long_min": None,
                        "expanding_short_max": None,
                        "expanding_short_min": None,
                        "expanding_net_max": None,
                        "expanding_net_min": None,
                        "expanding_current_long_percentile": None,
                        "expanding_current_short_percentile": None,
                        "expanding_current_net_percentile": None,
                        "expanding_current_net_rank_label": "N/A",
                        "expanding_context_summary": (
                            "N/A: missing long/short/net on this report — Known-at-the-time percentiles not computed."
                        ),
                    }
                )
                out_rows.append(row_out)
                continue

            w_l = lg[np.isfinite(lg)]
            w_s = sh[np.isfinite(sh)]
            w_n = nt[np.isfinite(nt)]
            row_out["expanding_long_max"] = float(np.max(w_l)) if w_l.size else None
            row_out["expanding_long_min"] = float(np.min(w_l)) if w_l.size else None
            row_out["expanding_short_max"] = float(np.max(w_s)) if w_s.size else None
            row_out["expanding_short_min"] = float(np.min(w_s)) if w_s.size else None
            row_out["expanding_net_max"] = float(np.max(w_n)) if w_n.size else None
            row_out["expanding_net_min"] = float(np.min(w_n)) if w_n.size else None

            pl = _empirical_percentile_rank(lg, float(longs[i]))
            ps = _empirical_percentile_rank(sh, float(shorts[i]))
            pn = _empirical_percentile_rank(nt, float(nets[i]))
            row_out["expanding_current_long_percentile"] = pl
            row_out["expanding_current_short_percentile"] = ps
            row_out["expanding_current_net_percentile"] = pn
            row_out["expanding_current_net_rank_label"] = _net_rank_label(float(nets[i]), pn)

            pls = "N/A" if not math.isfinite(pl) else f"{pl:.0f}"
            pss = "N/A" if not math.isfinite(ps) else f"{ps:.0f}"
            pns = "N/A" if not math.isfinite(pn) else f"{pn:.0f}"
            summary_lines = [
                (
                    f"Known-at-the-time history (no look-ahead): percentiles and extremes use only reports on or before "
                    f"{cur_dt} ({i + 1} rows from series start {series_start}). "
                    f"Joint-valid reports in this window: {n_joint} (long-only: {n_long}, short-only: {n_short}, net-only: {n_net})."
                ),
                f"Current longs are in the {pls}th percentile of long readings known through this report.",
                f"Current shorts are in the {pss}th percentile of short readings known through this report.",
                f"Current net is in the {pns}th percentile of net readings known through this report — {row_out['expanding_current_net_rank_label']}",
            ]
            row_out["expanding_context_summary"] = "\n".join(summary_lines)
            out_rows.append(row_out)

    if not out_rows:
        return pd.DataFrame()
    return pd.DataFrame(out_rows)


def _build_full_loaded_historical_stats(cot: pd.DataFrame) -> pd.DataFrame:
    """Full loaded-series min/max and percentiles vs entire dataset (same for every report date)."""
    out_rows: list[dict[str, Any]] = []
    for market in TARGET_MARKETS:
        m = cot.loc[cot["market"] == market, ["cot_report_date", "long_value", "short_value", "net_value"]].sort_values(
            "cot_report_date"
        )
        if m.empty:
            continue
        dates = m["cot_report_date"].to_numpy()
        longs = m["long_value"].to_numpy(dtype=float)
        shorts = m["short_value"].to_numpy(dtype=float)
        nets = m["net_value"].to_numpy(dtype=float)
        full_er = pd.Timestamp(dates[0]).strftime("%Y-%m-%d")
        full_lr = pd.Timestamp(dates[-1]).strftime("%Y-%m-%d")
        rows_used = int(len(m))
        w_l = longs[np.isfinite(longs)]
        w_s = shorts[np.isfinite(shorts)]
        w_n = nets[np.isfinite(nets)]
        fl_long_max = float(np.max(w_l)) if w_l.size else None
        fl_long_min = float(np.min(w_l)) if w_l.size else None
        fl_short_max = float(np.max(w_s)) if w_s.size else None
        fl_short_min = float(np.min(w_s)) if w_s.size else None
        fl_net_max = float(np.max(w_n)) if w_n.size else None
        fl_net_min = float(np.min(w_n)) if w_n.size else None

        for i in range(len(m)):
            row_out: dict[str, Any] = {
                "market": market,
                "cot_report_date": dates[i],
                "full_loaded_rows_used": rows_used,
                "full_loaded_earliest_report_date": full_er,
                "full_loaded_latest_report_date": full_lr,
                "full_loaded_long_max": fl_long_max,
                "full_loaded_long_min": fl_long_min,
                "full_loaded_short_max": fl_short_max,
                "full_loaded_short_min": fl_short_min,
                "full_loaded_net_max": fl_net_max,
                "full_loaded_net_min": fl_net_min,
            }
            if not (math.isfinite(longs[i]) and math.isfinite(shorts[i]) and math.isfinite(nets[i])):
                row_out.update(
                    {
                        "full_loaded_current_long_percentile": None,
                        "full_loaded_current_short_percentile": None,
                        "full_loaded_current_net_percentile": None,
                        "full_loaded_current_net_rank_label": "N/A",
                        "full_loaded_context_summary": (
                            "N/A: missing long/short/net on this report — full loaded dataset percentiles not computed."
                        ),
                    }
                )
                out_rows.append(row_out)
                continue

            pl = _empirical_percentile_rank(w_l, float(longs[i]))
            ps = _empirical_percentile_rank(w_s, float(shorts[i]))
            pn = _empirical_percentile_rank(w_n, float(nets[i]))
            row_out["full_loaded_current_long_percentile"] = pl
            row_out["full_loaded_current_short_percentile"] = ps
            row_out["full_loaded_current_net_percentile"] = pn
            row_out["full_loaded_current_net_rank_label"] = _net_rank_label(float(nets[i]), pn)
            pls = "N/A" if not math.isfinite(pl) else f"{pl:.0f}"
            pss = "N/A" if not math.isfinite(ps) else f"{ps:.0f}"
            pns = "N/A" if not math.isfinite(pn) else f"{pn:.0f}"
            row_out["full_loaded_context_summary"] = "\n".join(
                [
                    (
                        f"Full loaded dataset extremes: min/max and percentiles use all {rows_used} reports in the "
                        f"loaded history for this market ({full_er} through {full_lr}), regardless of selected date."
                    ),
                    f"Current longs are in the {pls}th percentile of all loaded long readings.",
                    f"Current shorts are in the {pss}th percentile of all loaded short readings.",
                    f"Current net is in the {pns}th percentile of all loaded net readings — {row_out['full_loaded_current_net_rank_label']}",
                ]
            )
            out_rows.append(row_out)

    if not out_rows:
        return pd.DataFrame()
    return pd.DataFrame(out_rows)


def _build_rolling_3y_historical_stats(cot: pd.DataFrame, window: int = WINDOW_WEEKS_3Y) -> pd.DataFrame:
    """Trailing rolling-window (default 156wk / 3Y) extremes + percentiles per report.

    Backtest-safe: each row uses only the prior ``window`` reports up to and
    including itself. Covers long / short / net / open interest. When fewer than
    ``window`` reports exist, the full available trailing history is used and
    ``rolling_3y_rows_used`` reflects the actual depth.
    """
    out_rows: list[dict[str, Any]] = []
    for market in TARGET_MARKETS:
        cols = ["cot_report_date", "long_value", "short_value", "net_value", "open_interest"]
        present = [c for c in cols if c in cot.columns]
        m = cot.loc[cot["market"] == market, present].sort_values("cot_report_date")
        if m.empty:
            continue
        dates = m["cot_report_date"].to_numpy()
        longs = m["long_value"].to_numpy(dtype=float) if "long_value" in m else np.full(len(m), np.nan)
        shorts = m["short_value"].to_numpy(dtype=float) if "short_value" in m else np.full(len(m), np.nan)
        nets = m["net_value"].to_numpy(dtype=float) if "net_value" in m else np.full(len(m), np.nan)
        ois = m["open_interest"].to_numpy(dtype=float) if "open_interest" in m else np.full(len(m), np.nan)
        n_m = len(m)
        for i in range(n_m):
            lo = max(0, i + 1 - window)
            w_l = longs[lo : i + 1]
            w_s = shorts[lo : i + 1]
            w_n = nets[lo : i + 1]
            w_o = ois[lo : i + 1]
            fin_n = w_n[np.isfinite(w_n)]
            rows_used = int(fin_n.size) if fin_n.size else int(np.sum(np.isfinite(w_l)))
            earliest = pd.Timestamp(dates[lo]).strftime("%Y-%m-%d")
            cur_dt = pd.Timestamp(dates[i]).strftime("%Y-%m-%d")

            def _extrema(arr: np.ndarray) -> tuple[float | None, float | None, float | None]:
                fin = arr[np.isfinite(arr)]
                if not fin.size:
                    return None, None, None
                return float(np.min(fin)), float(np.max(fin)), float(np.mean(fin))

            l_min, l_max, l_avg = _extrema(w_l)
            s_min, s_max, s_avg = _extrema(w_s)
            n_min, n_max, n_avg = _extrema(w_n)
            o_min, o_max, o_avg = _extrema(w_o)

            row_out: dict[str, Any] = {
                "market": market,
                "cot_report_date": dates[i],
                "rolling_3y_window_weeks": int(window),
                "rolling_3y_rows_used": rows_used,
                "rolling_3y_earliest_report_date": earliest,
                "rolling_3y_latest_report_date": cur_dt,
                "rolling_3y_long_min": l_min,
                "rolling_3y_long_max": l_max,
                "rolling_3y_long_avg": l_avg,
                "rolling_3y_short_min": s_min,
                "rolling_3y_short_max": s_max,
                "rolling_3y_short_avg": s_avg,
                "rolling_3y_net_min": n_min,
                "rolling_3y_net_max": n_max,
                "rolling_3y_net_avg": n_avg,
                "rolling_3y_oi_min": o_min,
                "rolling_3y_oi_max": o_max,
                "rolling_3y_oi_avg": o_avg,
            }
            row_out["rolling_3y_long_percentile"] = _pct_rank_window(w_l.tolist(), longs[i])
            row_out["rolling_3y_short_percentile"] = _pct_rank_window(w_s.tolist(), shorts[i])
            row_out["rolling_3y_net_percentile"] = _pct_rank_window(w_n.tolist(), nets[i])
            row_out["rolling_3y_oi_percentile"] = _pct_rank_window(w_o.tolist(), ois[i])

            cur_long = float(longs[i]) if math.isfinite(longs[i]) else None
            cur_short = float(shorts[i]) if math.isfinite(shorts[i]) else None
            cur_net = float(nets[i]) if math.isfinite(nets[i]) else None
            cur_oi = float(ois[i]) if math.isfinite(ois[i]) else None
            abs_ctx = compute_absolute_positioning(
                current_long=cur_long,
                long_max=l_max,
                current_short=cur_short,
                short_max=s_max,
                current_net=cur_net,
                net_min=n_min,
                net_max=n_max,
                current_oi=cur_oi,
                oi_max=o_max,
            )
            row_out["rolling_3y_current_long"] = cur_long
            row_out["rolling_3y_current_short"] = cur_short
            row_out["rolling_3y_current_net"] = cur_net
            row_out["rolling_3y_current_oi"] = cur_oi
            row_out["rolling_3y_long_vs_max_pct"] = abs_ctx.long_vs_3y_max_pct
            row_out["rolling_3y_short_vs_max_pct"] = abs_ctx.short_vs_3y_max_pct
            row_out["rolling_3y_net_range_pct"] = abs_ctx.net_range_pct
            row_out["rolling_3y_oi_vs_max_pct"] = abs_ctx.oi_vs_3y_max_pct
            row_out["rolling_3y_long_crowding"] = abs_ctx.long_crowding
            row_out["rolling_3y_short_crowding"] = abs_ctx.short_crowding
            row_out["rolling_3y_oi_participation"] = abs_ctx.oi_participation
            out_rows.append(row_out)

    if not out_rows:
        return pd.DataFrame()
    return pd.DataFrame(out_rows)


def _print_hist_floor_coverage(cot: pd.DataFrame) -> None:
    """Warn if we lack coverage from 2025-01-01 onward (processed files are source of truth)."""
    floor = pd.Timestamp("2025-01-01")
    print("HIST_COVERAGE_FLOOR check (expect data on/after 2025-01-01 when sources include it):")
    for market in TARGET_MARKETS:
        sub = cot.loc[cot["market"] == market, "cot_report_date"]
        if sub.empty:
            print(f"  market={market!r} status=no_rows")
            continue
        earliest = pd.Timestamp(sub.min())
        latest = pd.Timestamp(sub.max())
        post = cot.loc[(cot["market"] == market) & (cot["cot_report_date"] >= floor)]
        if post.empty:
            print(
                f"  market={market!r} WARNING=no_rows_on_or_after_{floor.date()} "
                f"(earliest={earliest.date()} latest={latest.date()})"
            )
        elif earliest > floor:
            print(
                f"  market={market!r} WARNING=earliest_after_floor "
                f"earliest={earliest.date()} latest={latest.date()} rows={len(sub)}"
            )
        else:
            print(f"  market={market!r} ok earliest={earliest.date()} latest={latest.date()} rows={len(sub)}")


def _print_dual_hist_context_console(cot: pd.DataFrame) -> None:
    """Console: per tracked market, full-loaded vs expanding rows and date ranges (tail row = latest report)."""
    print("HIST_CONTEXT_DUAL (full loaded = entire dataset; expanding = cumulative through that row's report date):")
    for market in TARGET_MARKETS:
        sub = cot.loc[cot["market"] == market].sort_values("cot_report_date")
        if sub.empty:
            print(
                f"  market={market!r} full_loaded_rows_used=N/A full_loaded_date_range=N/A "
                f"expanding_rows_used=N/A expanding_date_range=N/A"
            )
            continue
        tail = sub.iloc[-1]

        def _cell(v: Any) -> str:
            if v is None:
                return "N/A"
            try:
                if pd.isna(v):
                    return "N/A"
            except TypeError:
                pass
            return str(v)

        fl_ru = tail.get("full_loaded_rows_used")
        fl_er = tail.get("full_loaded_earliest_report_date")
        fl_lr = tail.get("full_loaded_latest_report_date")
        ex_ru = tail.get("expanding_rows_used")
        ex_er = tail.get("expanding_earliest_report_date")
        ex_lr = tail.get("expanding_latest_report_date")
        fl_range = (
            "N/A"
            if fl_er is None or fl_lr is None or pd.isna(fl_er) or pd.isna(fl_lr)
            else f"{_cell(fl_er)}..{_cell(fl_lr)}"
        )
        ex_range = (
            "N/A"
            if ex_er is None or ex_lr is None or pd.isna(ex_er) or pd.isna(ex_lr)
            else f"{_cell(ex_er)}..{_cell(ex_lr)}"
        )
        print(
            f"  market={market!r} full_loaded_rows_used={_cell(fl_ru)} full_loaded_date_range={fl_range} "
            f"expanding_rows_used={_cell(ex_ru)} expanding_date_range={ex_range}"
        )


def _normalize_cot_report_dates_naive(cot: pd.DataFrame) -> pd.DataFrame:
    """UTC-normalize then strip tz so merges (COT vs hist stats) match on calendar dates."""
    if cot.empty or "cot_report_date" not in cot.columns:
        return cot
    out = cot.copy()
    s = pd.to_datetime(out["cot_report_date"], errors="coerce", utc=True)
    out["cot_report_date"] = s.dt.normalize().dt.tz_localize(None)
    return out


def _debug_hist_context_wheat_2026_05_05(cot: pd.DataFrame) -> None:
    """Task 2: backend debug for Wheat on 2026-05-05 (merge + JSON-shaped snapshot)."""
    market = "Wheat"
    target = pd.Timestamp("2026-05-05")
    print("HIST_DEBUG_Wheat_2026-05-05 (merged cot row + JSON hist blocks):")
    hit = cot.loc[(cot["market"] == market) & (cot["cot_report_date"] == target)]
    if hit.empty:
        w = cot.loc[cot["market"] == market, "cot_report_date"].dropna().sort_values()
        tail = w.tail(5).dt.strftime("%Y-%m-%d").tolist() if len(w) else []
        print(f"  status=NO_EXACT_ROW n_wheat_rows={len(w)} last_dates={tail}")
        return
    r = hit.iloc[-1]
    hist_cols = sorted(c for c in r.index if str(c).startswith("expanding_") or str(c).startswith("full_loaded_"))

    def _cell(v: Any) -> Any:
        try:
            if v is None or (isinstance(v, float) and pd.isna(v)):
                return None
        except TypeError:
            pass
        return v

    snap = {k: _cell(r.get(k)) for k in hist_cols}
    print(f"  merged_column_snapshot={snap}")
    try:
        hj = _historical_json_fields_from_row(r)
        print(f"  json_expanding={hj.get('expanding_history_context')}")
        print(f"  json_full_loaded={hj.get('full_loaded_history_context')}")
    except Exception as exc:
        print(f"  json_SERIALIZE_FAIL: {exc}")


def _sanitize_audit_for_json(obj: Any) -> Any:
    if obj is None:
        return None
    if isinstance(obj, dict):
        return {str(k): _sanitize_audit_for_json(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_sanitize_audit_for_json(v) for v in obj]
    if isinstance(obj, (str, bool)):
        return obj
    if isinstance(obj, (int, float)):
        return obj
    if isinstance(obj, (datetime, pd.Timestamp)):
        return pd.Timestamp(obj).isoformat()
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    try:
        return float(obj)
    except (TypeError, ValueError):
        return str(obj)


def _build_by_market_as_of(cot: pd.DataFrame, week_date: pd.Timestamp) -> dict[str, pd.Series]:
    """Latest COT row per market on or before ``week_date`` (handles staggered report dates)."""
    by_market: dict[str, pd.Series] = {}
    if cot.empty or pd.isna(week_date):
        return by_market
    for market in TARGET_MARKETS:
        m = cot.loc[(cot["market"] == market) & (cot["cot_report_date"] <= week_date)]
        if m.empty:
            continue
        by_market[market] = m.iloc[-1]
    return by_market


def _cot_report_date_str(row: pd.Series) -> str:
    ts = row.get("cot_report_date")
    if ts is None or (isinstance(ts, float) and pd.isna(ts)):
        return ""
    return pd.Timestamp(ts).strftime("%Y-%m-%d")


def _rates_macro_payload(macro_audit: dict[str, Any] | None) -> dict[str, Any]:
    """Dashboard-facing FRED/macro strip (aligned with ``macro_audit.resolved_regime``)."""
    rr = (macro_audit or {}).get("resolved_regime") or {}
    ms = rr.get("macro_score")
    if ms is None:
        macro_score_rates: Any = "source unavailable"
    else:
        try:
            macro_score_rates = float(ms) if pd.notna(ms) else "source unavailable"
        except (TypeError, ValueError):
            macro_score_rates = "source unavailable"
    return {
        "macro_signal": str(rr.get("macro_signal") or "source unavailable"),
        "macro_score": macro_score_rates,
        "macro_rationale": str(rr.get("macro_rationale") or "source unavailable"),
        "rates_bias": str(rr.get("rates_bias") or "source unavailable"),
        "curve_state": str(rr.get("curve_state") or rr.get("curve_context") or "source unavailable"),
        "liquidity_regime": str(rr.get("liquidity_regime") or "source unavailable"),
    }


def _build_cot_feed_status(
    *,
    latest_cot_report_date: str | None,
    cot_feed_meta: dict[str, Any] | None,
) -> dict[str, Any]:
    meta = cot_feed_meta or {}
    cftc_week = meta.get("latest_cftc_report_date")
    explicit_stale = meta.get("cot_data_stale")
    is_stale = bool(explicit_stale) if explicit_stale is not None else False
    if cftc_week and latest_cot_report_date and str(latest_cot_report_date) < str(cftc_week):
        is_stale = True
    return {
        "latest_export_cot_week": latest_cot_report_date,
        "latest_cftc_report_date": cftc_week,
        "is_stale": is_stale,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _build_no_cot_record(
    *,
    market: str,
    date_str: str,
    macro_row_backward: pd.Series | None,
    rates_audit_df: pd.DataFrame,
    global_market_regime: dict[str, Any],
    macro_sig_week: str,
    by_market: dict[str, pd.Series],
    rates_snap_week: pd.Series | None,
) -> dict[str, Any]:
    """Record for instruments without a COT row this week — macro + metadata only."""
    spec = get_instrument(market)
    rates_snap = _rates_row_for_macro_audit(macro_row_backward, rates_audit_df)
    if rates_snap is None and rates_snap_week is not None:
        try:
            if _row_has_required_scoring_inputs(rates_snap_week):
                rates_snap = rates_snap_week
        except (TypeError, ValueError, KeyError):
            pass
    macro_audit = _sanitize_audit_for_json(build_macro_audit_payload(rates_snap))
    rates_macro = _rates_macro_payload(macro_audit)
    missing_macro_regime = (
        "N/A" if macro_row_backward is None else str(macro_row_backward.get("macro_signal") or "N/A")
    )
    macro_score_out: Any = "N/A"
    if macro_row_backward is not None and pd.notna(macro_row_backward.get("macro_score")):
        macro_score_out = float(macro_row_backward.get("macro_score"))

    inst_meta = instrument_meta_for_record(market)  # data_status added after record dict built
    positioning_status = inst_meta.get("positioning_status") or "no_direct_pair_cot"
    proxy_of = inst_meta.get("cot_proxy_of")

    macro_transmission = build_macro_transmission(
        market=market,
        rates_row=rates_snap,
        macro_audit=dict(macro_audit) if macro_audit else None,
        institutional_context=None,
    )
    inst_ctx: dict[str, Any] | None = None
    if spec and macro_transmission.get("available"):
        inst_ctx = build_macro_only_institutional_context(
            market=market,
            spec=spec,
            macro_transmission=macro_transmission,
            macro_signal=missing_macro_regime if missing_macro_regime != "N/A" else None,
            macro_score=macro_score_out if macro_score_out != "N/A" else None,
        )
        inst_ctx["macro_transmission"] = macro_transmission

    cot_status_label = "COT unavailable"
    if positioning_status == "proxy_required" and proxy_of:
        cot_status_label = f"Proxy required (see {proxy_of})"
    elif positioning_status == "no_direct_pair_cot":
        cot_status_label = "No direct pair COT"

    flow_summary = (
        f"N/A: {cot_status_label} for {market} on {date_str}. "
        "Macro transmission and driver profile remain active."
    )

    intel_missing = build_instrument_intel_context(
        market,
        cot_bias="N/A",
        cot_score="N/A",
        positioning_state="N/A",
        macro_regime=missing_macro_regime,
        macro_score=macro_score_out,
        final_context="N/A",
        institutional_flow_summary=flow_summary,
        macro_audit=dict(macro_audit),
        global_market_regime=dict(global_market_regime),
        full_loaded_net_pct=None,
    )
    inter_missing = build_intermarket_impulse_context(
        market,
        target_cot_bias="N/A",
        target_positioning_state="N/A",
        by_market_rows=by_market,
        rates_row=rates_snap_week,
        macro_signal=macro_sig_week,
    )
    ui_pack_missing = build_record_ui_pack(
        positioning_state="N/A",
        cot_bias="N/A",
        macro_regime=missing_macro_regime,
        macro_score=macro_score_out,
        global_market_regime=dict(global_market_regime) if global_market_regime else None,
        macro_audit=dict(macro_audit),
        instrument_intel=intel_missing,
        intermarket=inter_missing,
        full_loaded_net_pct=None,
    )

    zone = {
        "zone_focus": inst_ctx.get("zone_focus", "Macro / Drivers") if inst_ctx else "Macro / Drivers",
        "setup_type": inst_ctx.get("setup_type", "No direct COT mapping yet") if inst_ctx else "No direct COT mapping yet",
        "confidence_label": "Low",
        "invalidation_note": "N/A",
        "next_data_watch": "Await COT mapping or approved proxy wiring.",
    }

    return {
        "date": date_str,
        "market": market,
        "latest_report_date": "N/A",
        "cot_bias": "N/A",
        "cot_score": "N/A",
        "cot_reason": f"N/A: missing raw COT row for {market} on {date_str}.",
        "missing_reason": f"no mapped raw COT row for {market} on {date_str}",
        "positioning_status": positioning_status,
        "cot_status_label": cot_status_label,
        "instrument_meta": inst_meta,
        "macro_regime": missing_macro_regime,
        "macro_score": macro_score_out,
        "final_context": "N/A",
        "technical_action_note": cot_status_label,
        "final_context_reason": "Cannot score positioning without COT — macro layer only.",
        "positioning_state": "N/A",
        "four_week_positioning_story": flow_summary,
        "positioning_interpretation": flow_summary,
        "one_week_long_change": None,
        "one_week_short_change": None,
        "macro_audit": macro_audit,
        "rates_macro": rates_macro,
        "macro_transmission": macro_transmission,
        "institutional_flow_summary": flow_summary,
        "pressure_summary": "N/A",
        "flow_change_summary": "N/A",
        "zone_to_watch": "Macro drivers",
        "trader_action_note": "No direct positioning data — use macro transmission and related COT markets.",
        **zone,
        **_historical_json_fields_missing(),
        "global_market_regime": global_market_regime,
        "instrument_intel_context": intel_missing,
        "intermarket_impulse_context": inter_missing,
        "ui_pack": ui_pack_missing,
        "institutional_context": inst_ctx,
        **fx_valuation_fields_for_market(market),
    }


def run(*, cot_feed_meta: dict[str, Any] | None = None) -> Path:
    _run_t0 = time.monotonic()
    _start_watchdog()
    with _Stage("1/6 load COT master + financial index"):
        cot = _load_cot_history()
        print(f"  loaded COT rows={0 if cot is None else len(cot)}", flush=True)
    macro = _load_macro_history()
    rates_audit_df = _load_rates_clean_for_macro_audit()
    records: list[dict[str, Any]] = []
    if cot.empty:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "latest_cot_report_date": None,
            "cot_feed_status": _build_cot_feed_status(latest_cot_report_date=None, cot_feed_meta=cot_feed_meta),
            "records": [],
        }
        write_dashboard_exports(payload)
        print("VERIFY: latest_cot_report_date in dataset = None (empty COT history)")
        return OUT_PATH

    cot = _normalize_cot_report_dates_naive(cot)

    # --- Hard COT integrity validation: quarantine invalid / placeholder rows BEFORE scoring ---
    from hptl.cot.data_integrity import frame_integrity_summary, validate_cot_frame

    cot = validate_cot_frame(cot)
    _integ = frame_integrity_summary(cot)
    _n_invalid = int(_integ.get("invalid_rows", 0))
    if _n_invalid:
        print(
            f"COT integrity: quarantining {_n_invalid}/{_integ.get('total_rows')} invalid rows "
            f"(reasons: {_integ.get('reason_tally')})"
        )
    cot = (
        cot[cot["cot_valid"].astype(bool)]
        .drop(columns=["cot_valid", "cot_invalid_reasons", "reported_positions"], errors="ignore")
        .reset_index(drop=True)
    )

    from hptl.cot.cot_quarantine import quarantined_instrument_ids

    _blocked = quarantined_instrument_ids()
    if _blocked:
        cot = cot[~cot["market"].isin(_blocked)].reset_index(drop=True)

    _stage2 = _Stage("2/6 historical stats + institutional context precompute")
    _stage2.__enter__()
    hist_exp = _build_expanding_historical_stats(cot)
    _heartbeat("2/6 historical stats", "expanding stats built")
    hist_full = _build_full_loaded_historical_stats(cot)
    _heartbeat("2/6 historical stats", "full-loaded stats built")
    hist_3y = _build_rolling_3y_historical_stats(cot)
    _heartbeat("2/6 historical stats", "rolling 3Y stats built")
    if hist_exp.empty:
        print("WARNING: expanding historical stats frame is empty — check TARGET_MARKETS vs COT market mapping.")
    else:
        hist_exp = _normalize_cot_report_dates_naive(hist_exp)
        cot = cot.merge(hist_exp, on=["market", "cot_report_date"], how="left")
        if "expanding_rows_used" in cot.columns:
            n_miss = int(cot["expanding_rows_used"].isna().sum())
            if n_miss:
                print(
                    f"WARNING: expanding hist merge missed {n_miss}/{len(cot)} COT rows "
                    "(cot_report_date / market alignment)."
                )
    if hist_full.empty:
        print("WARNING: full-loaded historical stats frame is empty — check TARGET_MARKETS vs COT market mapping.")
    else:
        hist_full = _normalize_cot_report_dates_naive(hist_full)
        cot = cot.merge(hist_full, on=["market", "cot_report_date"], how="left")
        if "full_loaded_rows_used" in cot.columns:
            n_miss = int(cot["full_loaded_rows_used"].isna().sum())
            if n_miss:
                print(
                    f"WARNING: full-loaded hist merge missed {n_miss}/{len(cot)} COT rows "
                    "(cot_report_date / market alignment)."
                )
    if hist_3y.empty:
        print("WARNING: rolling 3Y historical stats frame is empty — check TARGET_MARKETS vs COT market mapping.")
    else:
        hist_3y = _normalize_cot_report_dates_naive(hist_3y)
        cot = cot.merge(hist_3y, on=["market", "cot_report_date"], how="left")
        if "rolling_3y_rows_used" in cot.columns:
            n_miss = int(cot["rolling_3y_rows_used"].isna().sum())
            if n_miss:
                print(
                    f"WARNING: rolling 3Y hist merge missed {n_miss}/{len(cot)} COT rows "
                    "(cot_report_date / market alignment)."
                )
    from hptl.cot.legacy_cot_loader import legacy_trader_groups_payload
    from hptl.cot.trader_positioning import merge_trader_positioning_into_cot

    cot = merge_trader_positioning_into_cot(cot, map_market_fn=_map_market, parse_dates_fn=_parse_cot_report_dates)
    trader_groups_payload = legacy_trader_groups_payload
    if "comm_long" in cot.columns:
        n_comm = int(cot["comm_long"].notna().sum())
        n_nr = int(cot["nrept_long"].notna().sum()) if "nrept_long" in cot.columns else 0
        print(f"TRADER_POSITIONING merge: commercial={n_comm}/{len(cot)} nonreportable={n_nr}/{len(cot)} rows")

    _print_dual_hist_context_console(cot)
    _debug_hist_context_wheat_2026_05_05(cot)

    _heartbeat("2/6 institutional context precompute", "starting precompute (LEGACY markets)")
    institutional_ctx_index, _regime_store = precompute_institutional_context_index(
        cot,
        markets=list(LEGACY_COT_MARKETS),
        macro=macro,
        save_store=True,
    )
    print(f"Institutional context index: {len(institutional_ctx_index)} market-weeks (L1–L5)")
    _stage2.__exit__(None, None, None)

    all_dates = sorted(cot["cot_report_date"].dropna().dt.strftime("%Y-%m-%d").unique())

    _only_env = os.environ.get("HPTL_CONFLUENCE_ONLY_DATES", "").strip()
    _incremental = os.environ.get("HPTL_CONFLUENCE_INCREMENTAL", "").strip().lower() in {"1", "true", "yes"}
    if _only_env:
        only_set = {d.strip()[:10] for d in _only_env.split(",") if d.strip()}
        all_dates = [d for d in all_dates if d in only_set]
        print(
            f"[INCREMENTAL] restricting build to {len(all_dates)} week(s): {', '.join(all_dates)}",
            flush=True,
        )

    build_markets = _selected_build_markets()
    _stage4 = _Stage(f"4/6 build confluence rows ({len(build_markets)} markets x {len(all_dates)} weeks)")
    _stage4.__enter__()
    _n_dates = len(all_dates)
    for _di, date_str in enumerate(all_dates, start=1):
        if _di == 1 or _di % 25 == 0 or _di == _n_dates:
            print(
                f"[BUILD ROWS] week {_di}/{_n_dates} {date_str} "
                f"(rows so far={len(records)}, elapsed={time.monotonic() - _stage4.t0:.1f}s)",
                flush=True,
            )
        _heartbeat("4/6 build confluence rows", f"week {_di}/{_n_dates} {date_str}")
        week_date = pd.Timestamp(date_str)
        by_market = _build_by_market_as_of(cot, week_date)
        macro_row_backward = None
        if not macro.empty:
            avail = macro[macro["macro_snapshot_date"] <= week_date]
            if not avail.empty:
                macro_row_backward = avail.iloc[-1]

        rates_snap_week = _rates_row_for_macro_audit(macro_row_backward, rates_audit_df)
        audit_week_plain = build_macro_audit_payload(rates_snap_week)
        global_market_regime = derive_global_market_regime(_sanitize_audit_for_json(audit_week_plain))
        macro_sig_week = (
            str(macro_row_backward.get("macro_signal") or "")
            if macro_row_backward is not None
            else ""
        )

        for market in build_markets:
            _heartbeat("4/6 build confluence rows", f"week {_di}/{_n_dates} {date_str} :: {market}")
            row = by_market.get(market)
            if row is None:
                records.append(
                    _build_no_cot_record(
                        market=market,
                        date_str=date_str,
                        macro_row_backward=macro_row_backward,
                        rates_audit_df=rates_audit_df,
                        global_market_regime=global_market_regime,
                        macro_sig_week=macro_sig_week,
                        by_market=by_market,
                        rates_snap_week=rates_snap_week,
                    )
                )
                continue

            has_real_positions = pd.notna(row.get("long_value")) and pd.notna(row.get("short_value")) and pd.notna(row.get("net_value"))
            cot_bias = str(row["cot_bias"]) if has_real_positions else "N/A"
            cot_score = float(row["cot_score"]) if has_real_positions and pd.notna(row.get("cot_score")) else None
            weekly = row.get("weekly_change")
            four = row.get("four_week_change")
            net = row.get("net_value")
            if pd.isna(weekly):
                weekly = None
            if pd.isna(four):
                four = None

            long_w1 = row.get("long_weekly_change")
            short_w1 = row.get("short_weekly_change")
            if pd.isna(long_w1):
                long_w1 = None
            else:
                long_w1 = float(long_w1)
            if pd.isna(short_w1):
                short_w1 = None
            else:
                short_w1 = float(short_w1)

            positioning_state = (
                _compute_positioning_state(net, weekly, four, long_w1, short_w1) if has_real_positions else "N/A"
            )
            institutional_flow_summary = (
                str(row.get("cot_summary"))
                if has_real_positions and row.get("cot_summary")
                else _institutional_flow_summary(
                    market,
                    positioning_state,
                    net,
                    row.get("long_value"),
                    row.get("short_value"),
                    weekly,
                    four,
                    long_w1,
                    short_w1,
                    cot,
                    week_date,
                )
                if has_real_positions
                else "N/A: missing positions."
            )
            story_4w = (
                _four_week_positioning_story(cot, market, week_date) if has_real_positions else "N/A: missing positions."
            )
            positioning_interpretation = (
                _positioning_interpretation_detail(
                    market,
                    positioning_state,
                    net,
                    row.get("long_value"),
                    row.get("short_value"),
                    weekly,
                    four,
                    long_w1,
                    short_w1,
                )
                if has_real_positions
                else "N/A: missing positions."
            )

            if has_real_positions:
                cot_interp = str(row.get("cot_interpretation") or "")
                cot_reason = (
                    f"Managed money net is {int(net)} (long {int(row['long_value'])}, short {int(row['short_value'])}); "
                    + (f"1w net change {int(weekly)}; " if weekly is not None else "1w net change N/A; ")
                    + (f"4w net change {int(four)}." if four is not None else "4w net change N/A.")
                )
                if cot_interp:
                    cot_reason = f"{cot_reason} {cot_interp}"
            else:
                cot_reason = "N/A: missing long/short values in source COT row; score suppressed."

            macro_row = macro_row_backward
            # Preserve known-good Cocoa alignment by allowing nearest forward macro snapshot.
            if market == "Cocoa" and not macro.empty:
                forward = macro[(macro["macro_snapshot_date"] > week_date) & (macro["macro_snapshot_date"] <= week_date + pd.Timedelta(days=7))]
                if not forward.empty:
                    macro_row = forward.iloc[0]

            macro_signal = None if macro_row is None else str(macro_row.get("macro_signal") or "")
            macro_score = None if macro_row is None else pd.to_numeric(pd.Series([macro_row.get("macro_score")]), errors="coerce").iloc[0]
            has_macro = macro_signal not in {None, "", "nan"} and pd.notna(macro_score)
            confluence_cot_bias = str(row.get("cot_directional_bias") or cot_bias)

            if has_macro and cot_score is not None:
                conf = _build_confluence(confluence_cot_bias, cot_score, macro_signal, float(macro_score))
                final_context = f"{conf['confluence_bias']} {conf['confluence_score']:.0f}"
                technical_note = conf["trade_readiness"]
                final_reason = conf["summary"]
                macro_regime = macro_signal
                macro_score_out = float(macro_score)
            else:
                final_context = "N/A"
                technical_note = "N/A: macro input unavailable."
                final_reason = "Cannot calculate final context because macro input is missing." if cot_score is not None else "Cannot calculate final context because COT long/short data is missing."
                macro_regime = "N/A"
                macro_score_out = "N/A"

            rates_snap = _rates_row_for_macro_audit(macro_row, rates_audit_df)
            macro_audit = _sanitize_audit_for_json(build_macro_audit_payload(rates_snap))
            rates_macro = _rates_macro_payload(macro_audit)

            prev_net_for_trader = (
                float(net) - float(weekly)
                if weekly is not None and pd.notna(weekly) and pd.notna(net)
                else None
            )
            trader_fields = _trader_action_fields(
                positioning_state,
                net,
                weekly,
                four,
                prev_net_for_trader,
                long_w1,
                short_w1,
                macro_regime,
                macro_score_out,
                cot_bias,
                has_real_positions,
            )
            cot_asof_pre = _cot_report_date_str(row) or date_str
            inst_ctx = institutional_ctx_index.get((market, cot_asof_pre)) if has_real_positions else None
            if inst_ctx:
                zone_decision_fields = {
                    "zone_focus": inst_ctx.get("zone_focus", "Wait"),
                    "setup_type": inst_ctx.get("setup_type", "No clean institutional edge"),
                    "confidence_label": inst_ctx.get("confidence_label", "Low"),
                    "invalidation_note": "N/A",
                    "next_data_watch": inst_ctx.get("flow_conflict_narrative")
                    or "Monitor structural regime persistence and weekly flow.",
                }
            else:
                zone_decision_fields = _zone_decision_layer_fields(positioning_state, has_real_positions)
            hist_json = _historical_json_fields_from_row(row)
            fl_ctx = hist_json.get("full_loaded_history_context") or {}
            intel_ctx = build_instrument_intel_context(
                market,
                cot_bias=cot_bias,
                cot_score=cot_score if cot_score is not None else "N/A",
                positioning_state=positioning_state,
                macro_regime=macro_regime,
                macro_score=macro_score_out,
                final_context=final_context,
                institutional_flow_summary=institutional_flow_summary,
                macro_audit=dict(macro_audit),
                global_market_regime=dict(global_market_regime),
                full_loaded_net_pct=fl_ctx.get("current_net_percentile"),
            )
            inter_ctx = build_intermarket_impulse_context(
                market,
                target_cot_bias=cot_bias,
                target_positioning_state=str(positioning_state),
                by_market_rows=by_market,
                rates_row=rates_snap_week,
                macro_signal=macro_sig_week,
            )
            ui_pack_row = build_record_ui_pack(
                positioning_state=str(positioning_state),
                cot_bias=cot_bias,
                macro_regime=macro_regime,
                macro_score=macro_score_out,
                global_market_regime=dict(global_market_regime) if global_market_regime else None,
                macro_audit=dict(macro_audit),
                instrument_intel=intel_ctx,
                intermarket=inter_ctx,
                full_loaded_net_pct=fl_ctx.get("current_net_percentile"),
            )

            macro_transmission = build_macro_transmission(
                market=market,
                rates_row=rates_snap,
                macro_audit=dict(macro_audit) if macro_audit else None,
                institutional_context=inst_ctx if inst_ctx else None,
            )
            if inst_ctx and macro_transmission.get("available"):
                inst_ctx = dict(inst_ctx)
                inst_ctx["macro_transmission"] = macro_transmission
                sd = dict(inst_ctx.get("scanner_display") or {})
                sd["macro"] = macro_transmission.get("headline") or sd.get("macro")
                inst_ctx["scanner_display"] = sd

            cot_asof = _cot_report_date_str(row) or date_str
            inst_meta_cot = instrument_meta_for_record(market)
            pillar_week = pillar_fields_for_market_week(market, date_str)
            records.append({
                "date": date_str,
                "market": market,
                "instrument_meta": inst_meta_cot,
                "positioning_status": "cot_available",
                "cot_status_label": "COT mapped",
                "cot_report_date": cot_asof,
                "latest_report_date": cot_asof,
                "cot_bias": cot_bias,
                "cot_score": round(float(cot_score), 1) if cot_score is not None else "N/A",
                "signal_strength": round(float(row["signal_strength"]), 1)
                if has_real_positions and pd.notna(row.get("signal_strength"))
                else "N/A",
                "score_confidence": round(float(row["score_confidence"]), 2)
                if has_real_positions and pd.notna(row.get("score_confidence"))
                else "N/A",
                "market_state": str(row.get("market_state") or "")
                if has_real_positions and row.get("market_state")
                else "N/A",
                "cot_summary": institutional_flow_summary if has_real_positions else "N/A",
                "cot_reason": cot_reason,
                "macro_regime": macro_regime,
                "macro_score": macro_score_out,
                "final_context": final_context,
                "technical_action_note": technical_note,
                "final_context_reason": final_reason,
                "raw_cftc_market_name": str(row.get("raw_cftc_market_name", "")),
                "trader_group_used": f"{row.get('long_col_used','N/A')} / {row.get('short_col_used','N/A')}",
                "positioning_source": str(row.get("positioning_source") or "legacy_cot_latest.json"),
                "position_source_family": str(row.get("position_source_family") or "legacy_noncommercial"),
                "long_value": float(row["long_value"]) if pd.notna(row.get("long_value")) else None,
                "short_value": float(row["short_value"]) if pd.notna(row.get("short_value")) else None,
                "net_value": float(net) if pd.notna(net) else None,
                "open_interest": float(row["open_interest"]) if pd.notna(row.get("open_interest")) else None,
                "cot_positioning_groups": trader_groups_payload(row),
                "missing_reason": None if pd.isna(row.get("missing_reason")) else str(row.get("missing_reason")),
                "previous_week_net": float(net - weekly) if weekly is not None else None,
                "one_week_net_change": weekly,
                "four_week_net_change": four,
                "bias_rule_used": "net>0 => Bullish; net<0 => Bearish; net==0 => Neutral",
                "score_rule_used": "probabilistic_cot_scoring (signal_strength + score_confidence)",
                "final_calculated_cot_bias": cot_bias,
                "final_calculated_cot_score": round(float(cot_score), 1) if cot_score is not None else "N/A",
                "positioning_state": positioning_state,
                "four_week_positioning_story": story_4w,
                "positioning_interpretation": positioning_interpretation,
                "one_week_long_change": long_w1,
                "one_week_short_change": short_w1,
                "macro_audit": macro_audit,
                "rates_macro": rates_macro,
                "institutional_flow_summary": institutional_flow_summary,
                **trader_fields,
                **zone_decision_fields,
                **hist_json,
                "global_market_regime": global_market_regime,
                "instrument_intel_context": intel_ctx,
                "intermarket_impulse_context": inter_ctx,
                "ui_pack": ui_pack_row,
                "institutional_context": inst_ctx if inst_ctx else None,
                "macro_transmission": macro_transmission,
                **pillar_week,
                **fx_valuation_fields_for_market(market),
            })
            if market == "Cocoa":
                print(
                    "COCOA DEBUG:",
                    {
                        "report_date": date_str,
                        "raw_market_name": str(row.get("raw_cftc_market_name", "")),
                        "long_column_used": row.get("long_col_used"),
                        "short_column_used": row.get("short_col_used"),
                        "long_value": None if pd.isna(row.get("long_value")) else float(row.get("long_value")),
                        "short_value": None if pd.isna(row.get("short_value")) else float(row.get("short_value")),
                        "net": None if pd.isna(net) else float(net),
                        "1w_change": weekly,
                        "4w_change": four,
                        "cot_bias": cot_bias,
                        "cot_score": cot_score,
                    },
                )

    _stage4.__exit__(None, None, None)
    print(f"[BUILD ROWS] complete — {len(records)} records from {_n_dates} weeks.", flush=True)

    if _incremental and OUT_PATH.exists():
        try:
            existing = json.loads(OUT_PATH.read_text(encoding="utf-8"))
            old_records = existing.get("records") or []
            new_keys = {(str(r.get("market")), str(r.get("date") or "")[:10]) for r in records}
            merged = [
                r
                for r in old_records
                if (str(r.get("market")), str(r.get("date") or "")[:10]) not in new_keys
            ]
            merged.extend(records)
            records = merged
            print(
                f"[INCREMENTAL] merged {len(new_keys)} new market-week keys into "
                f"{len(records)} total records",
                flush=True,
            )
        except (OSError, json.JSONDecodeError) as exc:
            print(f"[INCREMENTAL] merge skipped — could not read existing export: {exc}", flush=True)

    _mh_patched = apply_macro_hub_cot_fallback(records)
    if _mh_patched:
        print(f"MACRO_HUB_COT: patched {_mh_patched} latest-week row(s) from macro_hub_latest.json", flush=True)

    from hptl.confluence.macro_hub_institutional_attach import apply_macro_hub_institutional_fallback

    _mh_inst = apply_macro_hub_institutional_fallback(records)
    if _mh_inst:
        print(
            f"MACRO_HUB_INSTITUTIONAL: patched {_mh_inst} macro asset row(s) + scanner drivers on latest week",
            flush=True,
        )

    for rec in records:
        if rec.get("market") == "Wheat" and str(rec.get("date")) == "2026-05-05":
            exp = rec.get("expanding_history_context") or {}
            fl = rec.get("full_loaded_history_context") or {}
            print(
                "HIST_VERIFY_JSON_Wheat_2026-05-05:",
                {
                    "has_expanding_block": isinstance(rec.get("expanding_history_context"), dict),
                    "has_full_loaded_block": isinstance(rec.get("full_loaded_history_context"), dict),
                    "expanding_rows_used": exp.get("rows_used"),
                    "expanding_long_max": exp.get("long_max"),
                    "expanding_date_range": f"{exp.get('earliest_report_date')}->{exp.get('latest_report_date')}",
                    "full_loaded_rows_used": fl.get("rows_used"),
                    "full_loaded_long_max": fl.get("long_max"),
                    "full_loaded_date_range": f"{fl.get('earliest_report_date')}->{fl.get('latest_report_date')}",
                },
            )
            break

    latest_cot_report_date = (
        sorted(cot["cot_report_date"].dropna().dt.strftime("%Y-%m-%d").unique())[-1]
        if not cot.empty
        else None
    )
    latest_cot_report_date_by_market: dict[str, str] = {}
    if not cot.empty:
        for market in TARGET_MARKETS:
            m = cot.loc[cot["market"] == market, "cot_report_date"].dropna()
            if not m.empty:
                latest_cot_report_date_by_market[market] = pd.Timestamp(m.max()).strftime("%Y-%m-%d")
    latest_week_regime: dict[str, Any] | None = None
    if records:
        tail = sorted(records, key=lambda r: str(r.get("date") or ""))[-1]
        latest_week_regime = tail.get("global_market_regime")

    with _Stage("3/6 load macro relationships"):
        macro_relationship_maps = build_all_macro_relationship_maps()

    with _Stage(f"3b/6 attach feeds + instrument metadata ({len(records)} records)"):
        attach_feeds_to_latest_records(records, markets=list(TARGET_MARKETS))
        _heartbeat("3b/6 instrument metadata", "feeds attached")

        latest_calendar_week = (
            max((str(r.get("date") or "")[:10] for r in records), default="")
            or (latest_cot_report_date or "")
        )
        week_slice = (
            [r for r in records if str(r.get("date") or "") == latest_calendar_week]
            if latest_calendar_week
            else []
        )

        reg_path = export_registry_json()
        from hptl.markets.instrument_registry import load_registry

        _n_rec = len(records)
        for _ri, rec in enumerate(records, start=1):
            meta = instrument_meta_for_record(str(rec.get("market") or ""), rec)
            rec["instrument_meta"] = meta
            rec["data_status"] = meta.get("data_status", "unknown")
            if _ri % 5000 == 0 or _ri == _n_rec:
                _heartbeat("3b/6 instrument metadata", f"meta {_ri}/{_n_rec}")

    _stage6 = _Stage("6/6 write radar / scanner outputs")
    _stage6.__enter__()
    priority_debug = build_priority_debug(week_slice, calendar_week=latest_calendar_week or "", top_n=6)
    priority_debug_path = write_priority_debug(priority_debug)
    print(f"Wrote priority debug: {priority_debug_path} ({priority_debug.get('candidates_above_floor')} candidates)")

    relative_strength = build_relative_strength(week_slice, calendar_week=latest_calendar_week or "")
    relative_strength_path = write_relative_strength(relative_strength)
    print(
        f"Wrote relative strength: {relative_strength_path} "
        f"({len(relative_strength.get('pair_opportunities', []))} pair opportunities)"
    )

    from hptl.fx.usd_anchor import sync_usd_dxy_price_to_store, write_usd_anchor_document

    _usd_px = sync_usd_dxy_price_to_store()
    _usd_anchor_path = write_usd_anchor_document()
    print(
        f"Wrote USD anchor: {_usd_anchor_path} "
        f"(price_sync={_usd_px.get('written')}, mode={_usd_px.get('mode')})"
    )

    from hptl.cot.tff_macro_export import run as run_tff_macro_positioning

    _tff_path = run_tff_macro_positioning()
    print(f"Wrote TFF macro positioning: {_tff_path}")

    from hptl.setup_ranking.export import run as run_fx_setup_ranking

    _setup_path = run_fx_setup_ranking(confluence_records=records)
    print(f"Wrote FX setup ranking: {_setup_path}")

    scanner_attention_week = aggregate_priority_markets(
        week_slice, top_n=6, calendar_week=latest_calendar_week or ""
    )
    coverage_audit = run_coverage_audit(records, latest_calendar_week=latest_calendar_week)
    audit_path = write_coverage_audit(coverage_audit)
    print(f"Wrote instrument coverage audit: {audit_path}")

    from hptl.markets.cot_coverage_audit import build_cot_coverage_audit, write_cot_coverage_audit

    cot_coverage_audit = build_cot_coverage_audit()
    cot_coverage_path = write_cot_coverage_audit(cot_coverage_audit)
    _cca_sum = cot_coverage_audit.get("summary", {})
    print(
        f"Wrote COT coverage audit: {cot_coverage_path} "
        f"(direct={_cca_sum.get('direct_cot')} leg={_cca_sum.get('leg_derived_cot')} "
        f"proxy={_cca_sum.get('proxy_cot')} macro={_cca_sum.get('macro_only')} "
        f"broken={_cca_sum.get('broken_mapping')} invalid_rows={_cca_sum.get('invalid_cot_rows_detected')})"
    )

    _stage6.__exit__(None, None, None)

    _cca_by_id = {x["instrument_id"]: x for x in cot_coverage_audit.get("instruments", [])}
    for rec in records:
        cca = _cca_by_id.get(str(rec.get("market") or ""))
        if not cca:
            continue
        rec["cot_status"] = cca["cot_status"]
        rec["data_quality_status"] = cca["data_quality_status"]
        rec["leg_cot_markets"] = cca["leg_cot_markets"]
        rec["proxy_cot_markets"] = cca["proxy_cot_markets"]
        rec["duplicate_of"] = cca["duplicate_of"]
        meta = rec.get("instrument_meta")
        if isinstance(meta, dict):
            meta["cot_status"] = cca["cot_status"]
            meta["data_quality_status"] = cca["data_quality_status"]
            meta["leg_cot_markets"] = cca["leg_cot_markets"]
            meta["duplicate_of"] = cca["duplicate_of"]

    from hptl.cot.cot_quarantine import load_quarantine_doc

    reg = load_registry()
    _gate_doc = load_quarantine_doc()
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "latest_cot_report_date": latest_cot_report_date,
        "cot_integrity_gate": {
            "report_date": _gate_doc.get("report_date"),
            "checked_count": _gate_doc.get("checked_count"),
            "passed_count": _gate_doc.get("passed_count"),
            "failed_count": _gate_doc.get("failed_count"),
            "quarantined_instruments": _gate_doc.get("quarantined_instruments") or [],
            "generated_at": _gate_doc.get("generated_at"),
        },
        "latest_cot_report_date_by_market": latest_cot_report_date_by_market,
        "global_market_regime_latest_week": latest_week_regime,
        "macro_relationship_maps": macro_relationship_maps,
        "instrument_registry": {
            "version": 1,
            "path": str(reg_path),
            "total": len(reg),
            "legacy_cot_markets": list(LEGACY_COT_MARKETS),
            "markets": [reg[k].to_dict() for k in TARGET_MARKETS],
        },
        "cot_feed_status": _build_cot_feed_status(
            latest_cot_report_date=latest_cot_report_date,
            cot_feed_meta=cot_feed_meta,
        ),
        "records": records,
        "scanner_attention_week": scanner_attention_week,
        "instrument_coverage_audit_summary": coverage_audit.get("summary"),
        "instrument_coverage_audit_path": str(audit_path),
        "cot_coverage_audit_summary": cot_coverage_audit.get("summary"),
        "cot_coverage_audit_path": str(cot_coverage_path),
        "priority_debug_path": str(priority_debug_path),
        "relative_strength_path": str(relative_strength_path),
    }
    with _Stage("5/6 write confluence JSON"):
        out_path, maps_path = write_dashboard_exports(payload)
        AUDIT_CSV_PATH.parent.mkdir(parents=True, exist_ok=True)
        pd.DataFrame(records).to_csv(AUDIT_CSV_PATH, index=False)
    print(f"Wrote {out_path} with {len(records)} rows (slim dashboard export)")
    print(f"Wrote {maps_path} (macro relationship maps)")
    print(f"[BUILD] total elapsed {time.monotonic() - _run_t0:.1f}s", flush=True)
    print(f"VERIFY: latest_cot_report_date in dataset = {latest_cot_report_date}")
    from hptl.cot.legacy_cot_loader import legacy_cot_latest_path, scoring_eligible_markets

    print(f"LEGACY_COT_JSON source={legacy_cot_latest_path()}")
    for dash_name in scoring_eligible_markets():
        sub = [r for r in records if r.get("market") == dash_name and r.get("cot_report_date")]
        if not sub:
            print(f"LEGACY_COT_JSON {dash_name}: no cot_report_date rows")
            continue
        dates = sorted(str(r.get("cot_report_date")) for r in sub)
        src = next((r.get("positioning_source") for r in sub if r.get("positioning_source")), "legacy_cot_latest.json")
        print(
            f"LEGACY_COT_JSON {dash_name}: latest_cot_report_date={dates[-1]} "
            f"positioning_source={src} calendar_week_latest={max(str(r.get('date') or '') for r in sub)}"
        )
    return OUT_PATH


if __name__ == "__main__":
    run()
