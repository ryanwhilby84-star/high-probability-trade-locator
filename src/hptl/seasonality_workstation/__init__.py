"""Seasonality Workstation V1 — standalone research pillar.

Single source of truth for calendar seasonality across HPTL.
Does not depend on COT / Valuation / Trajectory / Scanner workstations.
"""

from hptl.seasonality_workstation.payload import build_seasonality_workstation_payload

__all__ = ["build_seasonality_workstation_payload"]
