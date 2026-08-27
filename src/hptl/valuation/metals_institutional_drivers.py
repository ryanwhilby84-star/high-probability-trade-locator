"""Phase 4B — metal-specific driver loading with strict availability checks."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.prices.canonical_timeline import load_canonical_timeline

CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "metals_institutional_sources.json"
MIN_DRIVER_WEEKS = 52
MAX_STALE_DAYS = 45


def _num(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if math.isfinite(f) else None


def _load_config() -> dict[str, Any]:
    if CONFIG_PATH.exists():
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    return {}


def _asof_value(series: dict[str, float], date: str) -> float | None:
    if not series:
        return None
    d = str(date)[:10]
    best: float | None = None
    for k in sorted(series.keys()):
        if k <= d:
            best = series[k]
        else:
            break
    return best


def _load_fred(series_id: str) -> dict[str, float]:
    from hptl.fx.fx_macro_history import load_fred_daily_map

    try:
        return load_fred_daily_map(series_id)
    except Exception:
        return {}


def _load_cache_series(rel_path: str) -> dict[str, float]:
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    rows = doc.get("series") or doc.get("observations") or doc.get("data") or []
    out: dict[str, float] = {}
    if isinstance(rows, dict):
        for k, v in rows.items():
            fv = _num(v)
            if fv is not None:
                out[str(k)[:10]] = fv
        return out
    if isinstance(rows, list):
        for row in rows:
            if not isinstance(row, dict):
                continue
            d = str(row.get("date") or row.get("observation_date") or "")[:10]
            fv = _num(row.get("value"))
            if d and fv is not None:
                out[d] = fv
    return out


def _weekly_from_daily(daily: dict[str, float], weekly_dates: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for d in weekly_dates:
        v = _asof_value(daily, d)
        if v is not None:
            out[d] = v
    return out


def _weekly_log_ratio(
    num_market: str,
    den_market: str,
    weekly_dates: list[str],
) -> dict[str, float]:
    num_tl = load_canonical_timeline(num_market)
    den_tl = load_canonical_timeline(den_market)
    if not num_tl or not den_tl:
        return {}
    num_map = {str(d)[:10]: _num(p) for d, p in num_tl.daily_closes()}
    den_map = {str(d)[:10]: _num(p) for d, p in den_tl.daily_closes()}
    out: dict[str, float] = {}
    for d in weekly_dates:
        n = _asof_value({k: v for k, v in num_map.items() if v and v > 0}, d)
        den = _asof_value({k: v for k, v in den_map.items() if v and v > 0}, d)
        if n and den and den > 0:
            out[d] = math.log(n / den)
    return out


def _series_freshness(series: dict[str, float], as_of: str) -> tuple[bool, str | None]:
    if not series:
        return False, None
    latest = max(series.keys())
    try:
        delta = (
            datetime.strptime(str(as_of)[:10], "%Y-%m-%d")
            - datetime.strptime(latest[:10], "%Y-%m-%d")
        ).days
    except ValueError:
        return False, latest
    return delta <= MAX_STALE_DAYS, latest


@dataclass
class DriverBundle:
    """Aligned weekly driver columns keyed by feature name."""

    dates: list[str]
    price: list[float]
    features: dict[str, list[float]] = field(default_factory=dict)
    lineage: dict[str, dict[str, str]] = field(default_factory=dict)
    missing_required: list[str] = field(default_factory=list)
    stale: list[str] = field(default_factory=list)
    as_of: str = ""

    @property
    def n(self) -> int:
        return len(self.dates)


def build_driver_bundle(market: str, *, as_of_week: str | None = None) -> DriverBundle:
    cfg = _load_config()
    model_cfg = (cfg.get("models") or {}).get(market) or {}
    fred_map = cfg.get("fred_series") or {}
    cache_map = cfg.get("cache_paths") or {}

    tl = load_canonical_timeline(market)
    if not tl:
        return DriverBundle(dates=[], price=[], missing_required=["canonical_price"])

    weekly_pairs, _ = tl.derive_weekly_iso()
    dates = [str(d)[:10] for d, _ in weekly_pairs]
    if as_of_week:
        dates = [d for d in dates if d <= str(as_of_week)[:10]]
    prices = [_num(p) for _, p in weekly_pairs if str(_)[0][:10] in set(dates)]
    # Re-align prices to filtered dates
    price_by_date = {str(d)[:10]: _num(p) for d, p in weekly_pairs}
    prices = [price_by_date[d] for d in dates if price_by_date.get(d) and price_by_date[d] > 0]
    dates = [d for d in dates if price_by_date.get(d) and price_by_date[d] and price_by_date[d] > 0]

    if len(dates) < MIN_DRIVER_WEEKS:
        return DriverBundle(
            dates=dates,
            price=prices,
            missing_required=["insufficient_price_history"],
            as_of=dates[-1] if dates else "",
        )

    as_of = dates[-1]
    bundle = DriverBundle(dates=dates, price=prices, as_of=as_of)

    def add_fred(key: str, field_name: str, *, required: bool = False) -> None:
        sid = fred_map.get(key, key)
        daily = _load_fred(sid)
        weekly = _weekly_from_daily(daily, dates)
        if len(weekly) < MIN_DRIVER_WEEKS:
            if required:
                bundle.missing_required.append(field_name)
            return
        fresh, latest = _series_freshness(weekly, as_of)
        if not fresh:
            bundle.stale.append(field_name)
        bundle.features[field_name] = [weekly[d] for d in dates]
        bundle.lineage[field_name] = {
            "source_name": "FRED",
            "source_id": sid,
            "source_date": latest or as_of,
        }

    add_fred("real_yield_10y", "real_yield", required=True)
    add_fred("dxy_broad", "log_dxy", required=True)
    if bundle.features.get("log_dxy"):
        bundle.features["log_dxy"] = [math.log(v) for v in bundle.features["log_dxy"] if v > 0]
        bundle.features["log_dxy"] = [
            math.log(_asof_value(_load_fred(fred_map.get("dxy_broad", "DTWEXBGS")), d) or 1.0)
            for d in dates
        ]

    # Derived ratios
    gs = _weekly_log_ratio("Gold", "Silver", dates)
    if len(gs) >= MIN_DRIVER_WEEKS:
        bundle.features["log_gold_silver_ratio"] = [gs[d] for d in dates]
        bundle.lineage["log_gold_silver_ratio"] = {
            "source_name": "Derived",
            "source_id": "Gold/Silver canonical",
            "source_date": as_of,
        }

    pt_pd = _weekly_log_ratio("Platinum", "Palladium", dates)
    if len(pt_pd) >= MIN_DRIVER_WEEKS:
        bundle.features["log_pt_pd_ratio"] = [pt_pd[d] for d in dates]
        bundle.lineage["log_pt_pd_ratio"] = {
            "source_name": "Derived",
            "source_id": "XPT/XPD canonical",
            "source_date": as_of,
        }

    # FRED optional/required per metal
    if market == "Copper / HG":
        add_fred("china_pmi", "china_pmi", required=True)

    if market in {"Platinum", "Palladium", "Silver"}:
        add_fred("us_industrial_production", "indpro", required=market in {"Platinum", "Palladium"})
        add_fred("us_vehicle_sales", "vehicle_sales", required=market in {"Platinum", "Palladium"})
        if market in {"Platinum", "Palladium"}:
            bundle.features["autocat_demand_proxy"] = []
            for d in dates:
                ind = _asof_value(_weekly_from_daily(_load_fred(fred_map.get("us_industrial_production", "INDPRO")), dates), d)
                veh = _asof_value(_weekly_from_daily(_load_fred(fred_map.get("us_vehicle_sales", "TOTALSA")), dates), d)
                if ind is None or veh is None:
                    bundle.features["autocat_demand_proxy"] = []
                    break
                bundle.features["autocat_demand_proxy"].append(math.log(max(ind, 1.0) * max(veh, 1.0)))
            if len(bundle.features.get("autocat_demand_proxy") or []) != len(dates):
                bundle.missing_required.append("autocat_demand_proxy")
                bundle.features.pop("autocat_demand_proxy", None)
            else:
                bundle.lineage["autocat_demand_proxy"] = {
                    "source_name": "FRED composite",
                    "source_id": "INDPRO×TOTALSA",
                    "source_date": as_of,
                }

    if market == "Silver":
        add_fred("us_industrial_production", "industrial_demand_proxy", required=True)

    if market == "Platinum":
        add_fred("brent_crude", "energy_autocat_cost", required=False)
        add_fred("sa_gold_production_proxy", "sa_supply_proxy", required=False)

    if market == "Palladium":
        add_fred("brent_crude", "energy_autocat_cost", required=False)

    # Cache-backed institutional drivers
    def add_cache(key: str, field_name: str, *, required: bool = False) -> None:
        rel = cache_map.get(key, "")
        if not rel:
            if required:
                bundle.missing_required.append(field_name)
            return
        daily = _load_cache_series(rel)
        weekly = _weekly_from_daily(daily, dates)
        if len(weekly) < MIN_DRIVER_WEEKS:
            if required:
                bundle.missing_required.append(field_name)
            return
        fresh, latest = _series_freshness(weekly, as_of)
        if not fresh:
            bundle.stale.append(field_name)
        bundle.features[field_name] = [weekly[d] for d in dates]
        bundle.lineage[field_name] = {
            "source_name": "Institutional cache",
            "source_id": rel,
            "source_date": latest or as_of,
        }

    if market == "Gold":
        add_cache("central_bank_gold_net_purchases", "cb_net_purchases", required=True)
        has_etf = False
        for k, fn in (("gold_etf_holdings", "etf_holdings"), ("gold_etf_flows", "etf_flows")):
            rel = cache_map.get(k, "")
            daily = _load_cache_series(rel) if rel else {}
            weekly = _weekly_from_daily(daily, dates)
            if len(weekly) >= MIN_DRIVER_WEEKS:
                has_etf = True
                bundle.features[fn] = [weekly[d] for d in dates]
                bundle.lineage[fn] = {"source_name": "ETF cache", "source_id": rel, "source_date": as_of}
        if not has_etf:
            bundle.missing_required.append("etf_holdings_or_flows")

    if market == "Silver":
        has_etf = False
        for k, fn in (("silver_etf_holdings", "etf_holdings"), ("silver_etf_flows", "etf_flows")):
            rel = cache_map.get(k, "")
            daily = _load_cache_series(rel) if rel else {}
            weekly = _weekly_from_daily(daily, dates)
            if len(weekly) >= MIN_DRIVER_WEEKS:
                has_etf = True
                bundle.features[fn] = [weekly[d] for d in dates]
                bundle.lineage[fn] = {"source_name": "ETF cache", "source_id": rel, "source_date": as_of}
        if not has_etf:
            bundle.missing_required.append("etf_holdings_or_flows")
        add_cache("silver_inventory", "inventory_proxy", required=False)

    if market == "Copper / HG":
        add_cache("lme_copper_inventory", "lme_inventory", required=True)
        add_cache("shfe_copper_inventory", "shfe_inventory", required=False)
        add_cache("copper_mine_deficit", "mine_deficit_proxy", required=False)

    if market == "Platinum":
        add_cache("platinum_jewelry_demand", "jewelry_demand_proxy", required=False)
        add_cache("sa_platinum_supply", "sa_supply_proxy", required=False)

    if market == "Palladium":
        add_cache("palladium_russia_supply", "russia_supply_proxy", required=False)
        add_cache("ev_adoption_proxy", "ev_erosion_proxy", required=False)

    # Enforce model-level required keys from config
    required_keys = model_cfg.get("required") or []
    feature_aliases = {
        "central_bank_net_purchases": "cb_net_purchases",
        "gold_silver_ratio": "log_gold_silver_ratio",
        "lme_inventory": "lme_inventory",
        "pt_pd_substitution_spread": "log_pt_pd_ratio",
        "pt_pd_substitution": "log_pt_pd_ratio",
        "industrial_demand_proxy": "industrial_demand_proxy",
    }
    for req in required_keys:
        fname = feature_aliases.get(req, req)
        if fname not in bundle.features and req not in bundle.missing_required:
            if fname not in bundle.features:
                bundle.missing_required.append(req)

    return bundle
