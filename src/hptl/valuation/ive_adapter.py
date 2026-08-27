"""Adapt legacy valuation blocks to IVEOutput without changing fair-value math."""
from __future__ import annotations

from typing import Any

from hptl.valuation.ive_schema import (
    CalculationStep,
    IVEOutput,
    SourceLineage,
    model_status_from_block,
    strip_confidence_fields,
    valuation_grade_from_pct,
    valuation_label_from_block,
)

# Known series metadata for lineage display (Phase 0 — static map; expand per model phase).
_FX_SERIES = {
    "policy_rate": ("Central bank policy", "policy_rate"),
    "yield_2y": ("Government bond yield", "y2"),
    "real_yield": ("Real yield", "real_yield"),
    "cpi_yoy": ("Consumer price index YoY", "cpi_yoy"),
}
_METALS_SERIES = {
    "real_yield_10y": ("Federal Reserve", "DFII10"),
    "dxy_broad": ("FRED", "DTWEXBGS"),
}


def _fmt(v: Any) -> str:
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.4f}" if abs(v) < 1000 else f"{v:.2f}"
    return str(v)


def _fx_lineage(block: dict[str, Any], generated_at: str) -> list[SourceLineage]:
    fresh = block.get("input_freshness") or {}
    drivers = block.get("drivers") or {}
    base = block.get("base") or ""
    quote = block.get("quote") or ""
    rows: list[SourceLineage] = []

    def leg_lineage(leg: str, field_key: str, driver_key: str, series_suffix: str) -> None:
        name, sid = _FX_SERIES.get(field_key, ("Macro input", field_key))
        rows.append(
            SourceLineage(
                source_name=f"{leg} — {name}",
                source_id=f"{leg}.{series_suffix}",
                source_date=str(fresh.get(f"{leg.lower()}_rates_as_of") or fresh.get("quote_rates_as_of") or "—")[:10],
                last_refresh=generated_at[:10],
                field=driver_key,
            )
        )

    if base and quote:
        for label, key, drv in (
            (base, "policy_rate", "base_policy_rate"),
            (quote, "policy_rate", "quote_policy_rate"),
            (base, "yield_2y", "base_yield_2y"),
            (quote, "yield_2y", "quote_yield_2y"),
        ):
            rows.append(
                SourceLineage(
                    source_name=f"{label} — {_FX_SERIES.get(key, ('', ''))[0]}",
                    source_id=f"{label}.{key}",
                    source_date=str(fresh.get("base_rates_as_of" if label == base else "quote_rates_as_of") or "—")[:10],
                    last_refresh=generated_at[:10],
                    field=drv,
                )
            )

    spot_as_of = fresh.get("spot_as_of") or block.get("pair") or "—"
    rows.append(
        SourceLineage(
            source_name="Spot FX",
            source_id=str(spot_as_of),
            source_date=str(fresh.get("quote_rates_as_of") or generated_at)[:10],
            last_refresh=generated_at[:10],
            field="spot_price",
        )
    )
    dxy = block.get("dxy_regime") or {}
    if dxy.get("available"):
        rows.append(
            SourceLineage(
                source_name="Broad USD (DXY)",
                source_id="DTWEXBGS",
                source_date=str(dxy.get("as_of") or "—")[:10],
                last_refresh=generated_at[:10],
                field="dxy_regime",
            )
        )
  treas = block.get("treasury_regime") or {}
    if treas.get("available"):
        rows.append(
            SourceLineage(
                source_name="US Treasury curve",
                source_id="DGS2/DGS10",
                source_date=str(treas.get("as_of") or "—")[:10],
                last_refresh=generated_at[:10],
                field="treasury_regime",
            )
        )
    for stale in block.get("stale_inputs") or []:
        rows.append(
            SourceLineage(
                source_name=f"STALE — {stale}",
                source_id=stale,
                source_date="stale",
                last_refresh=generated_at[:10],
                field=stale,
            )
        )
    return rows


def _fx_breakdown(block: dict[str, Any]) -> list[CalculationStep]:
    d = block.get("drivers") or {}
    reg = block.get("regression") or {}
    steps: list[CalculationStep] = []
    n = 1
    for desc, key in (
        ("Policy rate differential (pp)", "policy_rate_diff"),
        ("2Y yield differential (pp)", "yield_2y_diff"),
        ("Real yield differential (pp)", "real_yield_diff"),
        ("Inflation (CPI YoY) differential (pp)", "inflation_diff"),
    ):
        if d.get(key) is not None:
            steps.append(CalculationStep(n, desc, d[key]))
            n += 1
    if reg.get("r_squared") is not None:
        steps.append(CalculationStep(n, "Regression R²", reg["r_squared"]))
        n += 1
    if reg.get("n") is not None:
        steps.append(CalculationStep(n, "Aligned observations (n)", reg["n"]))
        n += 1
    if block.get("fair_value") is not None:
        steps.append(CalculationStep(n, "Fair value (model output)", block["fair_value"]))
        n += 1
    if block.get("spot_price") is not None and block.get("fair_value"):
        steps.append(
            CalculationStep(
                n,
                "Deviation % = (spot − fair) / fair × 100",
                block.get("deviation_pct"),
            )
        )
    return steps


def _metals_lineage(block: dict[str, Any], generated_at: str) -> list[SourceLineage]:
    fresh = block.get("input_freshness") or {}
    rows = [
        SourceLineage(
            source_name="Federal Reserve",
            source_id=_METALS_SERIES["real_yield_10y"][1],
            source_date=str(fresh.get("price_as_of") or "—")[:10],
            last_refresh=generated_at[:10],
            field="real_yield_10y",
        ),
        SourceLineage(
            source_name="FRED",
            source_id=_METALS_SERIES["dxy_broad"][1],
            source_date=str(fresh.get("price_as_of") or "—")[:10],
            last_refresh=generated_at[:10],
            field="dxy_broad",
        ),
        SourceLineage(
            source_name="Canonical metal price",
            source_id=str(fresh.get("real_yield_series") or "price_store"),
            source_date=str(fresh.get("price_as_of") or "—")[:10],
            last_refresh=generated_at[:10],
            field="spot_price",
        ),
    ]
    return rows


def _metals_breakdown(block: dict[str, Any]) -> list[CalculationStep]:
    d = block.get("drivers") or {}
    reg = block.get("regression") or {}
    feats = reg.get("features") or {}
    steps: list[CalculationStep] = []
    n = 1
    if d.get("real_yield_10y") is not None:
        steps.append(CalculationStep(n, "10Y real yield (DFII10)", d["real_yield_10y"]))
        n += 1
    if d.get("dxy_broad") is not None:
        steps.append(CalculationStep(n, "Broad USD index (DXY)", d["dxy_broad"]))
        n += 1
    if feats.get("real_yield") is not None:
        steps.append(CalculationStep(n, "Regression β — real yield", feats["real_yield"]))
        n += 1
    if feats.get("log_dxy") is not None:
        steps.append(CalculationStep(n, "Regression β — log(DXY)", feats["log_dxy"]))
        n += 1
    if reg.get("r_squared") is not None:
        steps.append(CalculationStep(n, "Regression R²", reg["r_squared"]))
        n += 1
    if block.get("fair_value") is not None:
        steps.append(CalculationStep(n, "Fair value = exp(log-linear macro model)", block["fair_value"]))
        n += 1
    if block.get("deviation_pct") is not None:
        steps.append(CalculationStep(n, "Deviation %", block["deviation_pct"]))
    return steps


def _agri_lineage(block: dict[str, Any], generated_at: str) -> list[SourceLineage]:
    return [
        SourceLineage(
            source_name="USDA WASDE / PSD",
            source_id=block.get("model_id") or "balance_sheet",
            source_date=str(block.get("as_of_week") or "—")[:10],
            last_refresh=generated_at[:10],
            field="stocks_to_use",
        ),
        SourceLineage(
            source_name=str(block.get("price_source") or "Canonical price"),
            source_id="price_store",
            source_date=str(block.get("as_of_week") or "—")[:10],
            last_refresh=generated_at[:10],
            field="spot_price",
        ),
    ]


def _agri_breakdown(block: dict[str, Any]) -> list[CalculationStep]:
    steps: list[CalculationStep] = []
    n = 1
    if block.get("stocks_to_use") is not None:
        steps.append(CalculationStep(n, "Stocks-to-use ratio", block["stocks_to_use"]))
        n += 1
    depth = block.get("data_depth") or block.get("balance_sheet_observations")
    if depth is not None:
        steps.append(CalculationStep(n, "Aligned balance-sheet observations", depth))
        n += 1
    steps.append(CalculationStep(n, "Model path", block.get("model_note") or block.get("model_id")))
    n += 1
    if block.get("fair_value") is not None:
        steps.append(CalculationStep(n, "Fair value", block["fair_value"]))
        n += 1
    if block.get("deviation_pct") is not None:
        steps.append(CalculationStep(n, "Deviation %", block["deviation_pct"]))
    return steps


def _macro_breakdown(block: dict[str, Any]) -> list[CalculationStep]:
    reg = block.get("regression") or {}
    steps: list[CalculationStep] = []
    n = 1
    for feat, val in (reg.get("features") or {}).items():
        steps.append(CalculationStep(n, f"Driver — {feat}", val))
        n += 1
    if reg.get("r_squared") is not None:
        steps.append(CalculationStep(n, "Regression R²", reg["r_squared"]))
        n += 1
    if block.get("fair_value") is not None:
        steps.append(CalculationStep(n, "Fair value", block["fair_value"]))
        n += 1
    if block.get("deviation_pct") is not None:
        steps.append(CalculationStep(n, "Deviation %", block["deviation_pct"]))
    return steps


def legacy_block_to_ive(
    block: dict[str, Any],
    instrument: str,
    *,
    generated_at: str,
) -> IVEOutput:
    """Map existing valuation export block → IVE contract (no math changes)."""
    model_id = str(block.get("model_id") or block.get("valuation_model_id") or "unknown")
    current = block.get("spot_price") if block.get("spot_price") is not None else block.get("current_price")
    fair = block.get("fair_value")
    dev = block.get("deviation_pct") if block.get("deviation_pct") is not None else block.get("valuation_pct")
    label = valuation_label_from_block(block)
    grade = valuation_grade_from_pct(float(dev) if dev is not None else None)
    status = model_status_from_block(block)

    if model_id == "fx_carry_real_yield_v3":
        lineage = _fx_lineage(block, generated_at)
        breakdown = _fx_breakdown(block)
        inputs = dict(block.get("drivers") or {})
    elif model_id == "metals_real_yield_v1":
        lineage = _metals_lineage(block, generated_at)
        breakdown = _metals_breakdown(block)
        inputs = dict(block.get("drivers") or {})
    elif "agri" in model_id:
        lineage = _agri_lineage(block, generated_at)
        breakdown = _agri_breakdown(block)
        inputs = {
            "stocks_to_use": block.get("stocks_to_use"),
            "balance_sheet_observations": block.get("balance_sheet_observations") or block.get("data_depth"),
            "model_path": block.get("model_id"),
        }
    else:
        lineage = []
        breakdown = _macro_breakdown(block)
        inputs = dict(block.get("drivers") or {})

    inputs["_regression"] = block.get("regression")
    inputs["_missing_inputs"] = block.get("missing_inputs") or []
    inputs["_stale_inputs"] = block.get("stale_inputs") or []

    source_names = [ln.source_name for ln in lineage]
    source_dates = [ln.source_date for ln in lineage]

    return IVEOutput(
        instrument=instrument,
        current_price=float(current) if current is not None else None,
        fair_value=float(fair) if fair is not None else None,
        valuation_pct=float(dev) if dev is not None else None,
        valuation_label=label,
        valuation_grade=grade,
        model_name=model_id,
        source_names=source_names,
        source_dates=source_dates,
        inputs=inputs,
        calculation_breakdown=[s.to_dict() for s in breakdown],
        last_updated=str(block.get("as_of_week") or generated_at)[:10],
        model_status=status,
        source_lineage=[ln.to_dict() for ln in lineage],
    )


def attach_ive_to_export_block(
    block: dict[str, Any],
    instrument: str,
    *,
    generated_at: str,
) -> dict[str, Any]:
    """Merge IVE contract onto export block; strip confidence fields."""
    out = strip_confidence_fields(block)
    ive = legacy_block_to_ive(out, instrument, generated_at=generated_at)
    ive_dict = ive.to_dict()
    out["ive"] = ive_dict
    # Promote IVE contract fields for API / scanner (Phase 0 canonical names).
    for key, val in ive_dict.items():
        if key != "ive":
            out[key] = val
    # Legacy aliases retained for confluence attach (not valuation UI).
    if out.get("current_price") is not None:
        out["spot_price"] = out["current_price"]
    if out.get("valuation_pct") is not None:
        out["deviation_pct"] = out["valuation_pct"]
    if out.get("valuation_label"):
        out["valuation_state"] = out["valuation_label"]
        out["valuation_bias"] = out["valuation_label"]
    return out
