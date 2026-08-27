"""Trade basket validation — Phase 2A + Phase 4 FX pair rules."""

from __future__ import annotations

from typing import Any

from hptl.fx.currency_map import parse_fx_pair
from hptl.trade_basket.models import (
    DEFAULT_RISK_PERCENT,
    DIRECTION_SIGN,
    MAX_BASKET_TRADES,
    TradeBasketInput,
    TradeEntry,
)


def _known_instrument_ids() -> set[str]:
    from hptl.markets.instrument_registry import LEGACY_COT_MARKETS

    known = set(LEGACY_COT_MARKETS)
    # Registry FX crosses / tradeable IDs (AUD/NZD, AUD/CHF, …)
    try:
        from hptl.markets.instrument_registry import all_instrument_ids

        for iid in all_instrument_ids(tradeable_only=True):
            known.add(iid)
    except Exception:  # noqa: BLE001
        pass
    return known


def _is_known_instrument(iid: str) -> bool:
    if iid in _known_instrument_ids():
        return True
    # Accept well-formed FX pair identifiers (BASE/QUOTE).
    if parse_fx_pair(iid) is not None:
        return True
    from hptl.markets.instrument_registry import get_instrument

    return get_instrument(iid) is not None


def _trade_id_from_raw(raw: dict[str, Any]) -> str:
    """Prefer instrument_pair (Phase 4), fall back to instrument_id."""
    return str(
        raw.get("instrument_pair")
        or raw.get("instrument_id")
        or raw.get("instrument")
        or ""
    ).strip()


def _is_empty_slot(raw: Any) -> bool:
    if raw is None:
        return True
    if isinstance(raw, TradeEntry):
        return not str(raw.instrument_id or "").strip()
    if isinstance(raw, dict):
        return not _trade_id_from_raw(raw)
    return True


def _parse_entry(raw: Any, *, index: int) -> tuple[TradeEntry | None, list[str]]:
    """Parse one slot. Empty → (None, []). Invalid → (None, errors)."""
    if _is_empty_slot(raw):
        return None, []

    errors: list[str] = []
    if isinstance(raw, TradeEntry):
        entry = raw
        iid = str(entry.instrument_id or "").strip()
        direction = str(entry.direction or "").strip().upper()
        risk = entry.risk_percent
    elif isinstance(raw, dict):
        iid = _trade_id_from_raw(raw)
        direction = str(raw.get("direction") or "").strip().upper()
        risk_raw = raw.get("risk_percent", DEFAULT_RISK_PERCENT)
        try:
            risk = float(risk_raw)
        except (TypeError, ValueError):
            errors.append(f"slot[{index}]: invalid_risk_percent={risk_raw!r}")
            risk = DEFAULT_RISK_PERCENT
    else:
        return None, [f"slot[{index}]: invalid_trade_entry_type"]

    if not iid:
        return None, []

    if direction not in DIRECTION_SIGN:
        errors.append(
            f"slot[{index}]: invalid_direction={direction!r} "
            f"(allowed: LONG, SHORT)"
        )

    if not _is_known_instrument(iid):
        errors.append(f"slot[{index}]: unknown_instrument_id={iid!r}")

    if errors:
        return None, errors

    return (
        TradeEntry(instrument_id=iid, direction=direction, risk_percent=float(risk)),  # type: ignore[arg-type]
        [],
    )


class TradeBasketValidator:
    """Validate and normalise basket inputs. Does not compute correlations."""

    def __init__(self) -> None:
        self.last_warnings: list[str] = []

    def validate(
        self, basket: TradeBasketInput | dict[str, Any]
    ) -> tuple[list[TradeEntry], list[str]]:
        if isinstance(basket, dict):
            raw_trades = basket.get("trades") or []
            frequency = str(basket.get("frequency") or "daily").strip().lower()
            lookback = basket.get("lookback", 60)
        else:
            raw_trades = list(basket.trades or [])
            frequency = str(basket.frequency or "daily").strip().lower()
            lookback = basket.lookback

        errors: list[str] = []
        warnings: list[str] = []
        if frequency not in ("daily", "weekly"):
            errors.append(f"invalid_frequency={frequency!r}")
        try:
            lb = int(lookback)
            if lb <= 0:
                errors.append(f"invalid_lookback={lookback!r}")
        except (TypeError, ValueError):
            errors.append(f"invalid_lookback={lookback!r}")

        if len(raw_trades) > MAX_BASKET_TRADES:
            errors.append(
                f"too_many_trades={len(raw_trades)} (max={MAX_BASKET_TRADES})"
            )

        populated: list[TradeEntry] = []
        for i, raw in enumerate(raw_trades[:MAX_BASKET_TRADES]):
            entry, errs = _parse_entry(raw, index=i)
            errors.extend(errs)
            if entry is not None:
                populated.append(entry)

        if len(populated) > MAX_BASKET_TRADES:
            errors.append(
                f"too_many_populated_trades={len(populated)} (max={MAX_BASKET_TRADES})"
            )
            populated = populated[:MAX_BASKET_TRADES]

        # Phase 4: reject only exact duplicate trades (same instrument + same
        # direction). Shared currency legs across distinct pairs are allowed.
        # Same instrument opposite directions are accepted (offsetting trades).
        seen_same_direction: set[tuple[str, str]] = set()
        seen_instruments: dict[str, str] = {}
        for entry in populated:
            key = (entry.instrument_id, entry.direction)
            if key in seen_same_direction:
                errors.append(
                    f"duplicate_instrument_direction="
                    f"{entry.instrument_id!r} {entry.direction}"
                )
            else:
                seen_same_direction.add(key)

            prev = seen_instruments.get(entry.instrument_id)
            if prev is not None and prev != entry.direction:
                warnings.append(
                    f"offsetting_same_instrument="
                    f"{entry.instrument_id!r} ({prev} vs {entry.direction})"
                )
            else:
                seen_instruments[entry.instrument_id] = entry.direction

        # Attach warnings on the validator instance for the engine to pick up.
        self.last_warnings = warnings
        return populated, errors
