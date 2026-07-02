"""Ingestion orchestrator: run every currency adapter -> ``fx_currency_rates.json``.

This is the single place that knows about all eight source adapters. It runs
each one, normalizes the result, and merges it into the master config that the
valuation engine reads (:mod:`hptl.fx.currency_rates`).

Merge policy (non-destructive + auditable)
------------------------------------------
* A field fetched live this run is written as ``live`` with its real source +
  observation date.
* A field the adapter could not fetch is **carried forward** from the prior
  config (so valuation keeps a usable, if degraded, input) but explicitly
  marked ``live=False`` and tagged ``[carried]`` in its source, and is never
  allowed to report ``PASS``.
* A field with no live value and no prior value is left ``null`` (-> FAIL).

The resulting ``status`` per currency is the worst of its three field statuses
(FAIL > WARN > PASS) and is recomputed, not trusted, by the audit script.
"""

from __future__ import annotations

import json
from datetime import date
from typing import Any, Callable

from hptl.fx.currency_rates import CONFIG_PATH, clear_cache
from hptl.fx.rate_adapter_base import (
    FAIL,
    PASS,
    WARN,
    FieldValue,
    NormalizedRate,
    field_status,
    now_iso,
    offline_mode,
    today_iso,
)
from hptl.fx.fred_inflation_adapter import fetch_all_cpi
from hptl.fx import (
    boc_adapter,
    boe_adapter,
    boj_adapter,
    ecb_adapter,
    fed_adapter,
    rba_adapter,
    rbnz_adapter,
    snb_adapter,
)

# Ordered registry — currency -> adapter module exposing ``fetch() -> NormalizedRate``.
ADAPTERS: dict[str, Callable[[], NormalizedRate]] = {
    "USD": fed_adapter.fetch,
    "EUR": ecb_adapter.fetch,
    "GBP": boe_adapter.fetch,
    "JPY": boj_adapter.fetch,
    "AUD": rba_adapter.fetch,
    "NZD": rbnz_adapter.fetch,
    "CAD": boc_adapter.fetch,
    "CHF": snb_adapter.fetch,
}

SCHEMA_VERSION = 3


def _load_prior() -> dict[str, Any]:
    if not CONFIG_PATH.exists():
        return {}
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}


def _round(value: float | None, ndigits: int = 3) -> float | None:
    return None if value is None else round(float(value), ndigits)


def _base_source(label: str | None) -> str:
    """Strip any prior ' [carried]' tags so provenance does not compound."""
    if not label:
        return "prior"
    return label.split(" [carried]")[0].strip() or "prior"


def _merge_field(
    name: str,
    fv: FieldValue,
    prior: dict[str, Any],
    *,
    kind: str,
    reference: date,
) -> dict[str, Any]:
    """Resolve one field (live / carried / missing) into a provenance record."""
    prior_value = prior.get(name)
    prior_as_of = prior.get(f"{name}_as_of")
    prior_source = (prior.get("field_sources") or {}).get(name) or prior.get("source")

    if fv.value is not None:
        rounded = _round(fv.value)
        status = field_status(rounded, fv.as_of, kind=kind, reference=reference)
        return {
            "value": rounded,
            "as_of": fv.as_of,
            "source": fv.source,
            "live": True,
            "status": status,
            "error": None,
        }

    if prior_value is not None:
        # Carry last-good forward but never let it claim PASS.
        carried_source = f"{_base_source(prior_source)} [carried]"
        return {
            "value": _round(prior_value),
            "as_of": prior_as_of,
            "source": carried_source,
            "live": False,
            "status": WARN,
            "error": fv.error,
        }

    return {
        "value": None,
        "as_of": None,
        "source": None,
        "live": False,
        "status": FAIL,
        "error": fv.error or "no value",
    }


def _currency_status(field_records: list[dict[str, Any]]) -> str:
    statuses = [r["status"] for r in field_records]
    if FAIL in statuses:
        return FAIL
    if WARN in statuses:
        return WARN
    return PASS


def build_currency_block(rate: NormalizedRate, prior_block: dict[str, Any], reference: date) -> dict[str, Any]:
    pol = _merge_field("policy_rate", rate.policy, prior_block, kind="policy", reference=reference)
    y2 = _merge_field("y2", rate.y2, prior_block, kind="yield", reference=reference)
    y10 = _merge_field("y10", rate.y10, prior_block, kind="yield", reference=reference)
    records = [pol, y2, y10]

    live_count = sum(1 for r in records if r["live"])
    if live_count == 3:
        data_quality = "live"
    elif live_count > 0:
        data_quality = "partial"
    elif any(r["value"] is not None for r in records):
        data_quality = "carried"
    else:
        data_quality = "missing"

    live_sources = [r["source"] for r in records if r["live"] and r["source"]]
    source_label = "/".join(dict.fromkeys(live_sources)) if live_sources else (
        "carried" if data_quality == "carried" else "missing"
    )
    as_of_dates = [r["as_of"] for r in records if r["as_of"]]

    errors = list(rate.errors)
    return {
        "central_bank": rate.central_bank,
        "policy_rate": pol["value"],
        "y2": y2["value"],
        "y10": y10["value"],
        "policy_rate_as_of": pol["as_of"],
        "y2_as_of": y2["as_of"],
        "y10_as_of": y10["as_of"],
        "source": source_label,
        "data_quality": data_quality,
        "status": _currency_status(records),
        "as_of": max(as_of_dates) if as_of_dates else None,
        "fetched_at": rate.fetched_at,
        "field_sources": {"policy_rate": pol["source"], "y2": y2["source"], "y10": y10["source"]},
        "field_live": {"policy_rate": pol["live"], "y2": y2["live"], "y10": y10["live"]},
        "field_status": {"policy_rate": pol["status"], "y2": y2["status"], "y10": y10["status"]},
        "errors": errors,
        "notes": list(rate.notes),
        # CPI merged in ingest() second pass (FRED inflation adapter).
        "cpi_yoy": prior_block.get("cpi_yoy"),
        "cpi_yoy_as_of": prior_block.get("cpi_yoy_as_of"),
    }


def _merge_cpi_into_block(
    block: dict[str, Any],
    prior_block: dict[str, Any],
    cpi_fv: FieldValue,
    reference: date,
) -> None:
    """Attach CPI YoY inflation to an existing currency block (in-place)."""
    cpi = _merge_field("cpi_yoy", cpi_fv, prior_block, kind="cpi", reference=reference)
    block["cpi_yoy"] = cpi["value"]
    block["cpi_yoy_as_of"] = cpi["as_of"]
    block.setdefault("field_sources", {})["cpi_yoy"] = cpi["source"]
    block.setdefault("field_live", {})["cpi_yoy"] = cpi["live"]
    block.setdefault("field_status", {})["cpi_yoy"] = cpi["status"]
    if cpi["error"] and cpi["value"] is None:
        block.setdefault("errors", [])
        if cpi["error"] not in block["errors"]:
            block["errors"].append(f"cpi_yoy: {cpi['error']}")
    statuses = list((block.get("field_status") or {}).values())
    block["status"] = _currency_status([{"status": s} for s in statuses])
    as_of_dates = [
        block.get("policy_rate_as_of"),
        block.get("y2_as_of"),
        block.get("y10_as_of"),
        block.get("cpi_yoy_as_of"),
    ]
    block["as_of"] = max(d for d in as_of_dates if d) if any(as_of_dates) else block.get("as_of")


def ingest(*, write: bool = True, verbose: bool = True) -> dict[str, Any]:
    """Run all adapters and (optionally) write the merged master config."""
    prior = _load_prior()
    prior_currencies = prior.get("currencies") or {}
    reference = date.today()

    currencies: dict[str, Any] = {}
    central_banks: dict[str, str] = {}
    for code, fetch in ADAPTERS.items():
        if verbose:
            mode = "cache-only" if offline_mode() else "live"
            print(f"[{mode}] fetching {code} ...", flush=True)
        try:
            rate = fetch()
        except Exception as exc:  # noqa: BLE001 - one adapter must not sink the run
            rate = NormalizedRate(
                currency=code,
                central_bank=prior_currencies.get(code, {}).get("central_bank", code),
                policy=FieldValue(error=f"adapter crashed: {type(exc).__name__}: {exc}"),
                y2=FieldValue(error="adapter crashed"),
                y10=FieldValue(error="adapter crashed"),
            )
        block = build_currency_block(rate, prior_currencies.get(code, {}), reference)
        currencies[code] = block
        central_banks[code] = rate.central_bank
        if verbose:
            print(f"    {code}: {block['status']} ({block['data_quality']}) src={block['source']}", flush=True)

    if verbose:
        mode = "cache-only" if offline_mode() else "live"
        print(f"[{mode}] fetching CPI (FRED) ...", flush=True)
    cpi_by_code = fetch_all_cpi()
    for code, cpi_fv in cpi_by_code.items():
        if code not in currencies:
            continue
        _merge_cpi_into_block(currencies[code], prior_currencies.get(code, {}), cpi_fv, reference)
        if verbose:
            cpi_val = currencies[code].get("cpi_yoy")
            print(f"    {code} CPI: {cpi_val if cpi_val is not None else 'missing'}", flush=True)

    config = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": today_iso(),
        "generated_at_utc": now_iso(),
        "note": (
            "FX Institutional Macro currency inputs: policy rates + sovereign yields from "
            "official central-bank adapters; CPI YoY from FRED OECD harmonized series. "
            "Fields marked live=False are carried forward. "
            "Run scripts/audit_fx_data_sources.py for PASS/WARN/FAIL."
        ),
        "max_staleness_days": 10,
        "max_staleness_days_cpi": 400,
        "central_banks": central_banks,
        "currencies": currencies,
    }

    if write:
        CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
        CONFIG_PATH.write_text(json.dumps(config, indent=2) + "\n", encoding="utf-8")
        clear_cache()
        if verbose:
            print(f"\nWrote {CONFIG_PATH}")
    return config
