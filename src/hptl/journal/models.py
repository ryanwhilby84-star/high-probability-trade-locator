"""Trade journal entry model (planning / logging only)."""
from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

TRADE_STATUSES = frozenset(
    {
        "idea",
        "planned",
        "order_set",
        "triggered",
        "invalidated",
        "closed",
    }
)

TRADE_DIRECTIONS = frozenset({"long", "short", "flat"})


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass
class TradeJournalEntry:
    trade_id: str
    market: str
    symbol: str = ""
    direction: str = "long"
    status: str = "idea"
    entry_price: float | None = None
    stop_loss: float | None = None
    target_1: float | None = None
    target_2: float | None = None
    risk_amount: float | None = None
    timeframe: str = ""
    setup_type: str = ""
    thesis: str = ""
    dashboard_snapshot: dict[str, Any] = field(default_factory=dict)
    cot_bias: str = ""
    cot_score: float | None = None
    macro_bias: str = ""
    weather_bias: str = ""
    catalyst_risk: str = ""
    created_at: str = ""
    updated_at: str = ""
    notes: str = ""
    source: str = "manual"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TradeJournalEntry:
        snap = data.get("dashboard_snapshot")
        if not isinstance(snap, dict):
            snap = {}
        return cls(
            trade_id=str(data.get("trade_id") or uuid4()),
            market=str(data.get("market") or "").strip(),
            symbol=str(data.get("symbol") or "").strip(),
            direction=str(data.get("direction") or "long").strip().lower(),
            status=str(data.get("status") or "idea").strip().lower(),
            entry_price=_num(data.get("entry_price")),
            stop_loss=_num(data.get("stop_loss")),
            target_1=_num(data.get("target_1")),
            target_2=_num(data.get("target_2")),
            risk_amount=_num(data.get("risk_amount")),
            timeframe=str(data.get("timeframe") or "").strip(),
            setup_type=str(data.get("setup_type") or "").strip(),
            thesis=str(data.get("thesis") or data.get("notes") or "").strip(),
            dashboard_snapshot=snap,
            cot_bias=str(data.get("cot_bias") or "").strip(),
            cot_score=_num(data.get("cot_score")),
            macro_bias=str(data.get("macro_bias") or "").strip(),
            weather_bias=str(data.get("weather_bias") or "").strip(),
            catalyst_risk=str(data.get("catalyst_risk") or "").strip(),
            created_at=str(data.get("created_at") or _now_iso()),
            updated_at=str(data.get("updated_at") or _now_iso()),
            notes=str(data.get("notes") or "").strip(),
            source=str(data.get("source") or "manual").strip(),
        )


def _num(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        n = float(v)
        return n if n == n else None
    except (TypeError, ValueError):
        return None


def normalize_payload(data: dict[str, Any], *, source: str = "manual") -> dict[str, Any]:
    """Validate and normalize inbound journal payload."""
    if not isinstance(data, dict):
        raise ValueError("Payload must be a JSON object")
    market = str(data.get("market") or "").strip()
    if not market:
        raise ValueError("market is required")

    direction = str(data.get("direction") or "long").strip().lower()
    if direction not in TRADE_DIRECTIONS:
        raise ValueError(f"direction must be one of: {', '.join(sorted(TRADE_DIRECTIONS))}")

    status = str(data.get("status") or "idea").strip().lower()
    if status not in TRADE_STATUSES:
        raise ValueError(f"status must be one of: {', '.join(sorted(TRADE_STATUSES))}")

    trade_id = str(data.get("trade_id") or "").strip() or str(uuid4())
    now = _now_iso()
    created = str(data.get("created_at") or now)
    updated = str(data.get("updated_at") or now)

    snap = data.get("dashboard_snapshot")
    if snap is not None and not isinstance(snap, dict):
        raise ValueError("dashboard_snapshot must be an object")

    return {
        "trade_id": trade_id,
        "market": market,
        "symbol": str(data.get("symbol") or "").strip(),
        "direction": direction,
        "status": status,
        "entry_price": _num(data.get("entry_price")),
        "stop_loss": _num(data.get("stop_loss")),
        "target_1": _num(data.get("target_1")),
        "target_2": _num(data.get("target_2")),
        "risk_amount": _num(data.get("risk_amount")),
        "timeframe": str(data.get("timeframe") or "").strip(),
        "setup_type": str(data.get("setup_type") or "").strip(),
        "thesis": str(data.get("thesis") or "").strip(),
        "dashboard_snapshot": snap if isinstance(snap, dict) else {},
        "cot_bias": str(data.get("cot_bias") or "").strip(),
        "cot_score": _num(data.get("cot_score")),
        "macro_bias": str(data.get("macro_bias") or "").strip(),
        "weather_bias": str(data.get("weather_bias") or "").strip(),
        "catalyst_risk": str(data.get("catalyst_risk") or "").strip(),
        "created_at": created,
        "updated_at": updated,
        "notes": str(data.get("notes") or "").strip(),
        "source": source,
    }
