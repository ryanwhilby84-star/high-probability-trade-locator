"""Short first-order macro implications for major calendar prints (context only)."""
from __future__ import annotations

import re
from typing import Any


def _norm(name: str) -> str:
    return re.sub(r"\s+", " ", (name or "").lower()).strip()


def _usd_block(direction_fc: str | None, *, hot: bool) -> str:
    if direction_fc == "beat":
        return "USD supportive · yields up · gold pressured" if hot else "USD slightly firm · yields bid"
    if direction_fc == "miss":
        return "USD softer · yields down · gold supported"
    return "USD reaction likely muted · check magnitude"


def interpret_calendar_event(event: dict[str, Any]) -> str:
    """One-line implication from event name + surprise direction (no trade signal)."""
    name = _norm(str(event.get("event_name") or ""))
    country = str(event.get("country") or "").upper()
    d_fc = event.get("direction_vs_forecast")
    mag = event.get("magnitude_vs_forecast") or "small"
    hot = mag == "large"

    if "crude" in name and "inventor" in name:
        if d_fc == "beat":
            return "Crude inventories build larger than expected → oil bearish"
        if d_fc == "miss":
            return "Crude inventories draw vs forecast → oil bullish"
        return "Crude inventory print — direction from actual vs forecast"

    if "natural gas" in name and "storage" in name:
        if d_fc == "beat":
            return "Gas storage build larger than expected → nat gas bearish"
        if d_fc == "miss":
            return "Gas storage draw larger than expected → nat gas bullish"
        return "Gas storage print — compare actual vs forecast"

    if "cpi" in name or "pce" in name or "consumer price" in name:
        if d_fc == "beat":
            return f"CPI/PCE hotter than forecast → {_usd_block('beat', hot=hot)} · risk assets pressured"
        if d_fc == "miss":
            return f"CPI/PCE cooler than forecast → {_usd_block('miss', hot=hot)} · risk assets supported"
        return "Inflation print — hotter = USD/yields up, cooler = the opposite"

    if "jobless" in name or "initial claims" in name:
        if d_fc == "beat":
            return "Claims higher than forecast → labour soft → USD softer · risk sentiment cautious"
        if d_fc == "miss":
            return "Claims lower than forecast → labour tight → USD firm · risk sentiment supportive"
        return "Claims: lower than forecast is USD/risk positive"

    if "retail sales" in name:
        if d_fc == "beat":
            return f"Retail sales beat → growth firm → {_usd_block('beat', hot=hot)}"
        if d_fc == "miss":
            return f"Retail sales miss → growth soft → {_usd_block('miss', hot=hot)}"
        return "Retail sales: beat = USD/yields bid, miss = the opposite"

    if "nonfarm" in name or "non farm" in name or "payroll" in name:
        if d_fc == "beat":
            return f"Payrolls beat → {_usd_block('beat', hot=hot)} · equities mixed-to-positive if growth"
        if d_fc == "miss":
            return f"Payrolls miss → {_usd_block('miss', hot=hot)} · risk assets cautious"
        return "NFP week — beat/miss drives USD, yields, and risk tone"

    if "gdp" in name:
        if d_fc == "beat":
            return "GDP beat → growth positive → USD firm · cyclicals supported"
        if d_fc == "miss":
            return "GDP miss → growth concern → USD softer · defensives favoured"
        return "GDP: beat vs forecast sets growth/USD tone"

    if "pmi" in name or "ism" in name:
        if d_fc == "beat":
            return "Activity beat → growth/implied demand supportive for cyclicals & USD"
        if d_fc == "miss":
            return "Activity miss → growth concern → USD softer · commodities mixed"
        return "PMI/ISM: beat = growth/USD positive vs forecast"

    if "fomc" in name or "fed " in name or "federal reserve" in name:
        return "Fed communication — policy path drives USD, yields, and gold"

    if "rate decision" in name or "interest rate" in name:
        return "Policy rate decision — yields and FX first order"

    if country in {"US", "USA", "UNITED STATES"}:
        return f"US macro print ({event.get('event_name', '')[:40]}) — USD, yields, and risk assets react first"

    return f"{country or 'Macro'} calendar event — check actual vs forecast for direction"
