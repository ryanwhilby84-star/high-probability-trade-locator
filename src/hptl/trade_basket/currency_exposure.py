"""Phase 4 currency exposure engine.

Method
------
For each populated trade with risk_percent ``r``:

* FX pair: allocate ``r / 2`` to each currency leg, signed by decomposition.
* Non-FX: no currency-leg contribution (instrument remains a single exposure
  unit for portfolio correlation only).

Net exposure for currency ``c``:

    net_c = Σ (sign_{trade,c} × r_trade / 2)

Display units are these risk-weighted half-trade exposures (not re-normalised
to sum 1 across currencies). Dominant share uses:

    gross = Σ |net_c|
    share = |net_dominant| / gross   (0 if gross = 0)

Statements are deterministic functions of the signed legs only.
"""

from __future__ import annotations

from typing import Any

from hptl.trade_basket.fx_decomposition import (
    currency_label,
    decompose_fx_pair,
    is_fx_pair_id,
    trade_display_label,
)

ENGINE_VERSION = "currency_exposure_v4"
NEAR_ZERO = 1e-12
# Dominant concentration diagnostic threshold (share of gross).
DOMINANT_CONCENTRATION_SHARE = 0.45


def _risk(trade: dict[str, Any]) -> float:
    try:
        v = float(trade.get("risk_percent") or 0.0)
    except (TypeError, ValueError):
        v = 0.0
    if v != v or v < 0:  # NaN / negative
        return 0.0
    return v


def _direction_label(net: float) -> str:
    if abs(net) < NEAR_ZERO:
        return "Neutral"
    return "Long" if net > 0 else "Short"


def compute_currency_exposure(trades: list[dict[str, Any]]) -> dict[str, Any]:
    """Compute net currency exposure + diagnostics from basket trade dicts."""
    # currency → accumulator
    nets: dict[str, float] = {}
    # currency → list of (trade_label, signed_contrib)
    contrib: dict[str, list[dict[str, Any]]] = {}
    trade_legs_out: list[dict[str, Any]] = []
    fx_trade_count = 0

    for t in trades:
        iid = str(t.get("instrument_id") or t.get("instrument_pair") or "").strip()
        direction = str(t.get("direction") or "").strip().upper()
        risk = _risk(t)
        label = trade_display_label(iid, direction)
        legs = decompose_fx_pair(iid, direction)
        if legs is None:
            trade_legs_out.append(
                {
                    "instrument_id": iid,
                    "instrument_pair": iid if is_fx_pair_id(iid) else None,
                    "direction": direction,
                    "risk_percent": risk,
                    "is_fx_pair": False,
                    "currency_legs": [],
                }
            )
            continue

        fx_trade_count += 1
        half = risk / 2.0
        leg_rows = []
        for leg in legs:
            ccy = leg["currency"]
            signed = float(leg["sign"]) * half
            nets[ccy] = nets.get(ccy, 0.0) + signed
            contrib.setdefault(ccy, []).append(
                {
                    "trade": label,
                    "signed_exposure": round(signed, 8),
                    "sign": int(leg["sign"]),
                }
            )
            leg_rows.append(
                {
                    "currency": ccy,
                    "sign": int(leg["sign"]),
                    "allocated_risk": round(half, 8),
                    "signed_exposure": round(signed, 8),
                }
            )
        trade_legs_out.append(
            {
                "instrument_id": iid,
                "instrument_pair": iid,
                "direction": direction,
                "risk_percent": risk,
                "is_fx_pair": True,
                "currency_legs": leg_rows,
            }
        )

    currency_rows: list[dict[str, Any]] = []
    for ccy, net in nets.items():
        trade_labels = []
        seen: set[str] = set()
        for row in contrib.get(ccy, []):
            if row["trade"] not in seen:
                seen.add(row["trade"])
                trade_labels.append(row["trade"])
        currency_rows.append(
            {
                "currency": ccy,
                "net_exposure": round(net, 8),
                "direction": _direction_label(net),
                "contributing_trades": trade_labels,
            }
        )

    # Tie-break: larger |net| first, then alphabetical currency code.
    currency_rows.sort(
        key=lambda r: (-abs(float(r["net_exposure"])), r["currency"])
    )

    gross = sum(abs(float(r["net_exposure"])) for r in currency_rows)
    dominant = None
    if currency_rows and gross > NEAR_ZERO:
        top = currency_rows[0]
        net = float(top["net_exposure"])
        side = "LONG" if net > NEAR_ZERO else ("SHORT" if net < -NEAR_ZERO else "NEUTRAL")
        dominant = {
            "currency": top["currency"],
            "direction": side,
            "net_exposure": round(net, 8),
            "share_of_gross": round(abs(net) / gross, 8),
            "contributing_trades": list(top["contributing_trades"]),
            "display": f"{top['currency']} {side}",
            "tie_break": "max_abs_net_then_alphabetical_currency",
        }

    diagnostics = _build_diagnostics(
        currency_rows=currency_rows,
        contrib=contrib,
        nets=nets,
        dominant=dominant,
        trade_legs=trade_legs_out,
        fx_trade_count=fx_trade_count,
    )

    return {
        "status": "ok",
        "engine": ENGINE_VERSION,
        "phase": "4",
        "has_fx_trades": fx_trade_count > 0,
        "currencies": currency_rows,
        "dominant_currency_exposure": dominant,
        "diagnostics": diagnostics,
        "trade_decompositions": trade_legs_out,
        "gross_currency_exposure": round(gross, 8),
        "method": {
            "fx_leg_allocation": "equal_split_of_trade_risk_percent",
            "leg_weight": "risk_percent / 2",
            "net_exposure": "sum of signed leg allocations",
            "gross_exposure": "sum of absolute net exposures",
            "dominant_share": "|net_dominant| / sum(|net_c|)",
            "dominant_tie_break": "max_abs_net_then_alphabetical_currency",
            "near_zero": NEAR_ZERO,
            "non_fx": "excluded_from_currency_panel",
        },
    }


def _build_diagnostics(
    *,
    currency_rows: list[dict[str, Any]],
    contrib: dict[str, list[dict[str, Any]]],
    nets: dict[str, float],
    dominant: dict[str, Any] | None,
    trade_legs: list[dict[str, Any]],
    fx_trade_count: int,
) -> list[str]:
    lines: list[str] = []

    # Same pair entered LONG and SHORT.
    dirs_by_pair: dict[str, set[str]] = {}
    for t in trade_legs:
        if not t.get("is_fx_pair"):
            continue
        iid = str(t.get("instrument_id") or "")
        dirs_by_pair.setdefault(iid, set()).add(str(t.get("direction") or ""))
    for iid in sorted(dirs_by_pair):
        if {"LONG", "SHORT"} <= dirs_by_pair[iid]:
            lines.append(
                f"{iid} appears as both LONG and SHORT — opposing pair trades."
            )

    # Shared signed exposure across two+ distinct trades
    shared_any = False
    for ccy in sorted(contrib.keys()):
        by_sign: dict[int, set[str]] = {1: set(), -1: set()}
        for row in contrib[ccy]:
            s = int(row["sign"])
            if s in by_sign:
                by_sign[s].add(row["trade"])
        for sign, trade_set in by_sign.items():
            if len(trade_set) >= 2:
                shared_any = True
                side = "long" if sign > 0 else "short"
                lines.append(
                    f"{len(trade_set)} trades share {side} {currency_label(ccy)} exposure."
                )

    # Partial / full offsets from opposing leg signs
    for ccy in sorted(contrib.keys()):
        signs = {int(r["sign"]) for r in contrib[ccy]}
        if 1 not in signs or -1 not in signs:
            continue
        net = float(nets.get(ccy, 0.0))
        if abs(net) < NEAR_ZERO:
            lines.append(f"{currency_label(ccy)} exposure is fully offset.")
        else:
            lines.append(f"{currency_label(ccy)} exposure is partially offset.")

    if dominant and float(dominant.get("share_of_gross") or 0) >= DOMINANT_CONCENTRATION_SHARE:
        ccy = dominant["currency"]
        if dominant["direction"] == "LONG":
            lines.append(
                f"Most basket risk is concentrated in {currency_label(ccy)} strength."
            )
        elif dominant["direction"] == "SHORT":
            lines.append(
                f"Most basket risk is concentrated in {currency_label(ccy)} weakness."
            )

    # When FX trades exist but no shared-leg / offset / opposing-pair signal.
    structural = [
        ln
        for ln in lines
        if ("share" in ln) or ("offset" in ln) or ("opposing pair" in ln)
    ]
    if fx_trade_count > 0 and not structural and not shared_any:
        lines.append("No meaningful shared currency exposure detected across trades.")

    # Deduplicate while preserving order
    seen: set[str] = set()
    out: list[str] = []
    for line in lines:
        if line not in seen:
            seen.add(line)
            out.append(line)
    return out


def enrich_basket_with_currency_exposure(
    basket_payload: dict[str, Any],
) -> dict[str, Any]:
    """Attach ``currency_exposure`` without altering Phase 2A/3 maths fields."""
    out = dict(basket_payload)
    if out.get("status") != "ok":
        out["currency_exposure"] = {
            "status": "skipped",
            "reason": "basket_not_ok",
            "engine": ENGINE_VERSION,
        }
        return out

    trades = list(out.get("trades") or [])
    # Annotate trades with instrument_pair + legs for API consumers
    annotated: list[dict[str, Any]] = []
    for t in trades:
        row = dict(t)
        iid = str(row.get("instrument_id") or "").strip()
        direction = str(row.get("direction") or "").strip().upper()
        row["instrument_pair"] = iid
        legs = decompose_fx_pair(iid, direction)
        row["is_fx_pair"] = legs is not None
        row["currency_legs"] = legs or []
        annotated.append(row)
    out["trades"] = annotated

    exposure = compute_currency_exposure(annotated)
    out["currency_exposure"] = exposure
    out["workstation_phase"] = "4"
    return out
