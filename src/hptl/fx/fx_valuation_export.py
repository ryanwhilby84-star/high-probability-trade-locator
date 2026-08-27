"""Build a standalone FX institutional valuation artifact (SECONDARY / PARALLEL).

.. warning::
    **Not the valuation pillar export.** Output goes to ``fx_valuation_latest.json``
    for setup ranking and ``FxValuationPanel`` only. Dashboard pillar valuation
    uses ``hptl.valuation.fx_carry_real_yield_v3`` via ``valuation_latest.json``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.fx.currency_rates import all_currency_rates, config_meta
from hptl.fx.fx_institutional_valuation import (
    VALUATION_MODEL_TYPE,
    score_currencies,
    value_fx_pair_institutional,
)
from hptl.fx.fx_macro_positioning import apply_macro_positioning_to_pair, build_macro_positioning_document
from hptl.fx.fx_valuation import resolve_pair_currencies
from hptl.fx.fx_valuation_attach import _spot_and_percentile
from hptl.cot.tff_macro_loader import latest_tff_macro_snapshot, load_tff_macro_weeks
from hptl.fx.fx_valuation_history import append_valuation_snapshot
from hptl.fx.fx_valuation_panel import build_all_valuation_panels

CANONICAL_PATH = PROCESSED_DIR / "fx_valuation_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "fx_valuation_latest.json"

DEFAULT_PAIRS: tuple[str, ...] = (
    "EUR/USD",
    "GBP/USD",
    "USD/JPY",
    "AUD/USD",
    "NZD/USD",
    "USD/CAD",
    "USD/CHF",
    "GBP/NZD",
    "EUR/JPY",
    "EUR/AUD",
    "EUR/CAD",
    "EUR/GBP",
    "EUR/CHF",
    "EUR/NZD",
    "NZD/JPY",
    "CHF/JPY",
    "NZD/AUD",
    "NZD/CAD",
    "GBP/JPY",
    "AUD/JPY",
    "CAD/JPY",
    "GBP/AUD",
    "GBP/CAD",
    "AUD/NZD",
    "AUD/CAD",
    "AUD/CHF",
    "CAD/CHF",
    "NZD/CHF",
)


def build_fx_valuation_payload(pairs: tuple[str, ...] = DEFAULT_PAIRS) -> dict[str, Any]:
    rates = all_currency_rates()
    currency_scores = score_currencies(rates)
    tff_weeks = load_tff_macro_weeks()
    tff_snapshot = latest_tff_macro_snapshot(tff_weeks)
    macro_positioning = build_macro_positioning_document(tff_snapshot)

    currencies_out: dict[str, Any] = {}
    for code, rec in rates.items():
        cv = currency_scores[code]
        block = rec.as_dict()
        block.update(cv.as_dict())
        currencies_out[code] = block

    out_pairs: list[dict[str, Any]] = []
    for pid in pairs:
        resolved = resolve_pair_currencies(pid)
        if not resolved:
            out_pairs.append({"pair": pid, "supported": False})
            continue
        base, quote, canonical = resolved
        spot, _pctl = _spot_and_percentile(canonical)
        val = value_fx_pair_institutional(
            base,
            quote,
            spot=spot,
            currency_scores=currency_scores,
        )
        block = val.as_block()
        overlay = apply_macro_positioning_to_pair(
            base,
            quote,
            institutional_score_diff=val.pair_score_differential,
            tff_snapshot=tff_snapshot,
        )
        block["macro_positioning_overlay"] = overlay
        block["positioning_adjusted_score_differential"] = overlay.get("adjusted_pair_score_differential")
        block["positioning_bias"] = overlay.get("positioning_bias")
        block["supported"] = True
        out_pairs.append(block)

    from hptl.fx.usd_anchor import build_usd_anchor_document

    usd_anchor = build_usd_anchor_document()

    payload_without_panels = {
        "schema_version": 2,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "valuation_model_type": f"{VALUATION_MODEL_TYPE} + TFF Positioning",
        "config": config_meta(),
        "currencies": currencies_out,
        "currency_scores": {c: currency_scores[c].as_dict() for c in currency_scores},
        "pairs": out_pairs,
        "usd_anchor": usd_anchor,
        "macro_positioning": macro_positioning,
        "tff_positioning_source": tff_snapshot.get("source"),
        "tff_trader_group": tff_snapshot.get("trader_group"),
    }
    valuation_panels = build_all_valuation_panels(payload_without_panels)
    payload = {**payload_without_panels, "valuation_panels": valuation_panels}
    append_valuation_snapshot(payload)
    return payload


def write_fx_valuation_exports(payload: dict[str, Any]) -> Path:
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run() -> Path:
    payload = build_fx_valuation_payload()
    path = write_fx_valuation_exports(payload)
    supported = sum(1 for p in payload["pairs"] if p.get("supported"))
    print(f"Wrote {path} ({supported}/{len(payload['pairs'])} pairs valued, V2 institutional).")
    return path


if __name__ == "__main__":
    run()
