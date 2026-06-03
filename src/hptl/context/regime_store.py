"""Persistent per-market structural regime state (causal, week-by-week)."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

DEFAULT_STATE_PATH = Path("data/processed/institutional_regime_state.json")

# Weeks required to confirm a structural flip (guardrail).
REGIME_FLIP_WEEKS_REQUIRED = 2


@dataclass
class PendingFlip:
    target: str
    weeks_confirmed: int = 1

    def to_dict(self) -> dict[str, Any]:
        return {"target": self.target, "weeks_confirmed": self.weeks_confirmed}

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> PendingFlip | None:
        if not d or not d.get("target"):
            return None
        return cls(target=str(d["target"]), weeks_confirmed=int(d.get("weeks_confirmed") or 1))


@dataclass
class MarketRegimeState:
    structural_regime: str = "neutral_rotation"
    structural_score_ema: float = 0.0
    regime_since_cot_week: str = ""
    weeks_in_regime: int = 0
    pending_flip: PendingFlip | None = None
    last_flow_momentum: str = "mixed"
    last_tactical_posture: str = "wait_confirmation"

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "structural_regime": self.structural_regime,
            "structural_score_ema": round(self.structural_score_ema, 2),
            "regime_since_cot_week": self.regime_since_cot_week,
            "weeks_in_regime": self.weeks_in_regime,
            "last_flow_momentum": self.last_flow_momentum,
            "last_tactical_posture": self.last_tactical_posture,
        }
        if self.pending_flip:
            out["pending_flip"] = self.pending_flip.to_dict()
        return out

    @classmethod
    def from_dict(cls, d: dict[str, Any] | None) -> MarketRegimeState:
        if not d:
            return cls()
        pf = PendingFlip.from_dict(d.get("pending_flip"))
        return cls(
            structural_regime=str(d.get("structural_regime") or "neutral_rotation"),
            structural_score_ema=float(d.get("structural_score_ema") or 0.0),
            regime_since_cot_week=str(d.get("regime_since_cot_week") or ""),
            weeks_in_regime=int(d.get("weeks_in_regime") or 0),
            pending_flip=pf,
            last_flow_momentum=str(d.get("last_flow_momentum") or "mixed"),
            last_tactical_posture=str(d.get("last_tactical_posture") or "wait_confirmation"),
        )


class RegimeStore:
    def __init__(self, path: Path | None = None) -> None:
        self.path = Path(path or DEFAULT_STATE_PATH)
        self._markets: dict[str, MarketRegimeState] = {}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                for k, v in raw.items():
                    if isinstance(v, dict):
                        self._markets[str(k)] = MarketRegimeState.from_dict(v)
        except (OSError, json.JSONDecodeError):
            pass

    def save(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {m: s.to_dict() for m, s in self._markets.items()}
        self.path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    def get(self, market: str) -> MarketRegimeState:
        if market not in self._markets:
            self._markets[market] = MarketRegimeState()
        return self._markets[market]

    def commit_regime(
        self,
        market: str,
        *,
        proposed_regime: str,
        cot_week: str,
        score_ema: float,
    ) -> MarketRegimeState:
        """Apply hysteresis: no single-week structural flip."""
        st = self.get(market)
        current = st.structural_regime
        st.structural_score_ema = score_ema

        if proposed_regime == current:
            st.pending_flip = None
            st.weeks_in_regime = (st.weeks_in_regime + 1) if st.regime_since_cot_week else 1
            if not st.regime_since_cot_week:
                st.regime_since_cot_week = cot_week
            return st

        pf = st.pending_flip
        if pf is None or pf.target != proposed_regime:
            st.pending_flip = PendingFlip(target=proposed_regime, weeks_confirmed=1)
            st.weeks_in_regime = st.weeks_in_regime + 1 if st.regime_since_cot_week else 0
            return st

        pf.weeks_confirmed += 1
        if pf.weeks_confirmed >= REGIME_FLIP_WEEKS_REQUIRED:
            st.structural_regime = proposed_regime
            st.regime_since_cot_week = cot_week
            st.weeks_in_regime = 1
            st.pending_flip = None
        return st
