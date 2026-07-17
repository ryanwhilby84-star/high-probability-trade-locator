"""Natural Gas / NG institutional driver loading (Energy V1).

Reuses the metals-style as-of weekly alignment pattern. Missing EIA series do not
block the model — they are reported with available=false for the UI cards.
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROJECT_ROOT
from hptl.prices.canonical_timeline import load_canonical_timeline

CONFIG_PATH = PROJECT_ROOT / "data" / "config" / "energy_ng_valuation_sources.json"
MIN_WEEKS = 52
MARKET = "Natural Gas / NG"


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


def _load_cache_doc(rel_path: str) -> dict[str, Any]:
    path = PROJECT_ROOT / rel_path
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return doc if isinstance(doc, dict) else {}


def _load_cache_series(rel_path: str) -> dict[str, float]:
    doc = _load_cache_doc(rel_path)
    if not doc:
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


def _cache_meta(rel_path: str) -> dict[str, Any]:
    doc = _load_cache_doc(rel_path)
    if not doc:
        return {"status": "UNAVAILABLE"}
    return {
        "status": doc.get("status") or ("LIVE" if doc.get("series") or doc.get("observations") else "UNAVAILABLE"),
        "official_source": doc.get("official_source"),
        "series_identifier": doc.get("series_identifier") or doc.get("dataset_identifier"),
        "units": doc.get("units"),
        "frequency": doc.get("frequency"),
        "latest_observation_date": doc.get("latest_observation_date"),
        "last_successful_refresh": doc.get("last_successful_refresh"),
        "api_key_required": doc.get("api_key_required"),
        "concept": doc.get("concept"),
    }


def _weekly_from_daily(daily: dict[str, float], weekly_dates: list[str]) -> dict[str, float]:
    out: dict[str, float] = {}
    for d in weekly_dates:
        v = _asof_value(daily, d)
        if v is not None:
            out[d] = v
    return out


def _storage_surplus_vs_5y(storage_weekly: dict[str, float]) -> dict[str, float]:
    """Bcf surplus/deficit vs trailing 5-year same-week average (ISO week number)."""
    by_week: dict[int, list[tuple[str, float]]] = {}
    for d, v in storage_weekly.items():
        try:
            wn = datetime.strptime(d[:10], "%Y-%m-%d").isocalendar()[1]
        except ValueError:
            continue
        by_week.setdefault(wn, []).append((d, v))

    out: dict[str, float] = {}
    for d, v in storage_weekly.items():
        try:
            dt = datetime.strptime(d[:10], "%Y-%m-%d")
            wn = dt.isocalendar()[1]
            year = dt.year
        except ValueError:
            continue
        peers = [val for date, val in by_week.get(wn, []) if date[:4] != str(year) and date < d]
        peers = peers[-5:] if len(peers) > 5 else peers
        if len(peers) < 3:
            continue
        avg = sum(peers) / len(peers)
        out[d] = v - avg
    return out


def _seasonality_factor(weekly_dates: list[str]) -> dict[str, float]:
    """Week-of-year seasonal factor from multi-year NG price history (z-scored)."""
    tl = load_canonical_timeline(MARKET)
    if not tl:
        return {}
    weekly_pairs, _ = tl.derive_weekly_iso()
    by_wn: dict[int, list[float]] = {}
    dated: list[tuple[str, float, int]] = []
    for date, price in weekly_pairs:
        px = _num(price)
        if px is None or px <= 0:
            continue
        d = str(date)[:10]
        try:
            wn = datetime.strptime(d, "%Y-%m-%d").isocalendar()[1]
        except ValueError:
            continue
        by_wn.setdefault(wn, []).append(px)
        dated.append((d, px, wn))

    mean_wn = {wn: sum(vs) / len(vs) for wn, vs in by_wn.items() if len(vs) >= 3}
    if not mean_wn:
        return {}
    global_mean = sum(mean_wn.values()) / len(mean_wn)
    global_std = math.sqrt(sum((m - global_mean) ** 2 for m in mean_wn.values()) / len(mean_wn)) or 1.0

    out: dict[str, float] = {}
    date_set = set(weekly_dates)
    for d, _px, wn in dated:
        if d not in date_set:
            continue
        m = mean_wn.get(wn)
        if m is None:
            continue
        out[d] = (m - global_mean) / global_std
    return out


def _seasonality_export_bias() -> dict[str, Any]:
    path = PROJECT_ROOT / "data" / "seasonality_latest.json"
    if not path.exists():
        return {}
    try:
        doc = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    block = (doc.get("instruments") or {}).get(MARKET) or {}
    return {
        "seasonality_bias": block.get("seasonality_bias"),
        "seasonality_score": block.get("seasonality_score"),
        "seasonality_reason": block.get("seasonality_reason"),
        "calendar_month": block.get("calendar_month"),
        "month_avg_return_pct": block.get("month_avg_return_pct"),
        "wired": bool(block.get("wired")),
    }


@dataclass
class NgDriverBundle:
    dates: list[str] = field(default_factory=list)
    price: list[float] = field(default_factory=list)
    features: dict[str, list[float]] = field(default_factory=dict)
    driver_cards: dict[str, dict[str, Any]] = field(default_factory=dict)
    lineage: dict[str, dict[str, str]] = field(default_factory=dict)
    as_of: str = ""

    @property
    def n(self) -> int:
        return len(self.dates)


def build_ng_driver_bundle(*, as_of_week: str | None = None) -> NgDriverBundle:
    cfg = _load_config()
    fred_map = cfg.get("fred_series") or {}
    cache_map = cfg.get("cache_paths") or {}

    tl = load_canonical_timeline(MARKET)
    if not tl:
        return NgDriverBundle()

    weekly_pairs, _ = tl.derive_weekly_iso()
    dates = [str(d)[:10] for d, _ in weekly_pairs]
    if as_of_week:
        dates = [d for d in dates if d <= str(as_of_week)[:10]]
    price_by_date = {str(d)[:10]: _num(p) for d, p in weekly_pairs}
    dates = [d for d in dates if price_by_date.get(d) and price_by_date[d] and price_by_date[d] > 0]
    prices = [float(price_by_date[d]) for d in dates]

    if len(dates) < MIN_WEEKS:
        return NgDriverBundle(dates=dates, price=prices, as_of=dates[-1] if dates else "")

    as_of = dates[-1]
    bundle = NgDriverBundle(dates=dates, price=prices, as_of=as_of)

    # --- Price card ---
    bundle.driver_cards["market_price"] = {
        "id": "market_price",
        "label": "Natural Gas Price",
        "unit": "USD/MMBtu",
        "available": True,
        "current": round(prices[-1], 4),
        "as_of": as_of,
        "source": "canonical_price_timeline",
        "institutional_effect": "Anchor",
        "tone": "neutral",
        "interpretation": "Weekly NG close used as the valuation dependent variable.",
    }

    # --- DXY ---
    dxy_daily = _load_fred(fred_map.get("dxy_broad", "DTWEXBGS"))
    dxy_weekly = _weekly_from_daily(dxy_daily, dates)
    if len(dxy_weekly) >= MIN_WEEKS:
        vals = [dxy_weekly[d] for d in dates]
        bundle.features["log_dxy"] = [math.log(v) for v in vals]
        bundle.lineage["log_dxy"] = {
            "source_name": "FRED",
            "source_id": fred_map.get("dxy_broad", "DTWEXBGS"),
            "source_date": max(dxy_weekly.keys()),
        }
        latest = vals[-1]
        # Higher DXY → bearish for USD commodities
        effect = "Bearish" if latest >= (sum(vals[-26:]) / min(26, len(vals))) else "Bullish"
        bundle.driver_cards["dxy"] = {
            "id": "dxy",
            "label": "US Dollar (Broad)",
            "unit": "index",
            "available": True,
            "current": round(latest, 3),
            "as_of": as_of,
            "source": fred_map.get("dxy_broad", "DTWEXBGS"),
            "institutional_effect": effect,
            "tone": "bearish" if effect == "Bearish" else "bullish",
            "interpretation": (
                "A firm dollar typically pressures energy commodities. "
                f"Broad USD index at {latest:.2f}."
            ),
        }
    else:
        bundle.driver_cards["dxy"] = {
            "id": "dxy",
            "label": "US Dollar (Broad)",
            "unit": "index",
            "available": False,
            "current": None,
            "institutional_effect": "Unavailable",
            "tone": "neutral",
            "interpretation": "DXY series not loaded — awaiting FRED DTWEXBGS.",
        }

    # --- Storage (EIA working gas) ---
    storage_path = cache_map.get("working_gas_storage", "")
    storage_meta = _cache_meta(storage_path)
    storage_raw = _load_cache_series(storage_path)
    # Surplus on native EIA week dates (no look-ahead: peers strictly prior)
    surplus_raw = _storage_surplus_vs_5y(storage_raw) if storage_raw else {}

    if storage_raw:
        stor_dates = sorted(storage_raw.keys())
        latest_sd = stor_dates[-1]
        cur = float(storage_raw[latest_sd])
        prev_s = float(storage_raw[stor_dates[-2]]) if len(stor_dates) >= 2 else None
        weekly_chg = (cur - prev_s) if prev_s is not None else None
        sur = _asof_value(surplus_raw, latest_sd)
        if sur is not None:
            avg5 = cur - sur
            pct = (100.0 * sur / avg5) if avg5 else None
            effect = "Bullish" if sur < 0 else "Bearish" if sur > 0 else "Neutral"
            bundle.driver_cards["storage"] = {
                "id": "storage",
                "label": "Working Gas Storage",
                "unit": "Bcf",
                "available": True,
                "current": round(cur, 1),
                "weekly_change": round(weekly_chg, 1) if weekly_chg is not None else None,
                "five_year_average": round(avg5, 1),
                "difference": round(sur, 1),
                "surplus_deficit_pct": round(pct, 2) if pct is not None else None,
                "as_of": storage_meta.get("latest_observation_date") or latest_sd,
                "source": storage_meta.get("official_source") or "EIA working gas cache",
                "series_id": storage_meta.get("series_identifier"),
                "freshness": storage_meta.get("status") or "LIVE",
                "institutional_effect": effect,
                "tone": "bullish" if effect == "Bullish" else "bearish" if effect == "Bearish" else "neutral",
                "interpretation": (
                    f"Storage is {abs(sur):.0f} Bcf {'below' if sur < 0 else 'above'} the 5-year "
                    f"same-week average ({avg5:.0f} Bcf)"
                    + (f", {abs(pct):.1f}% {'deficit' if sur < 0 else 'surplus'}." if pct is not None else ".")
                ),
            }

    storage_weekly = _weekly_from_daily(storage_raw, dates) if storage_raw else {}
    surplus_weekly = _weekly_from_daily(surplus_raw, dates) if surplus_raw else {}
    if storage_weekly and surplus_weekly:
        filled_storage: list[float | None] = []
        filled_surplus: list[float | None] = []
        last_s: float | None = None
        last_u: float | None = None
        for d in dates:
            if d in storage_weekly:
                last_s = storage_weekly[d]
            if d in surplus_weekly:
                last_u = surplus_weekly[d]
            filled_storage.append(last_s)
            filled_surplus.append(last_u)
        start = 0
        while start < len(dates) and (filled_storage[start] is None or filled_surplus[start] is None):
            start += 1
        if len(dates) - start >= MIN_WEEKS:
            if start > 0:
                dates = dates[start:]
                prices = prices[start:]
                as_of = dates[-1]
                bundle.dates = dates
                bundle.price = prices
                bundle.as_of = as_of
                for fk in list(bundle.features.keys()):
                    bundle.features[fk] = bundle.features[fk][start:]
                filled_storage = filled_storage[start:]
                filled_surplus = filled_surplus[start:]
                if "market_price" in bundle.driver_cards:
                    bundle.driver_cards["market_price"]["current"] = round(prices[-1], 4)
                    bundle.driver_cards["market_price"]["as_of"] = as_of
            bundle.features["storage_surplus_bcf"] = [float(v) for v in filled_surplus]  # type: ignore[arg-type]
            bundle.lineage["storage_surplus_bcf"] = {
                "source_name": storage_meta.get("official_source") or "EIA Working Gas (cache)",
                "source_id": storage_meta.get("series_identifier") or storage_path,
                "source_date": max(storage_raw.keys()),
            }

    if "storage" not in bundle.driver_cards:
        key_note = storage_meta.get("api_key_required") or "EIA_API_KEY"
        bundle.driver_cards["storage"] = {
            "id": "storage",
            "label": "Working Gas Storage",
            "unit": "Bcf",
            "available": False,
            "current": None,
            "five_year_average": None,
            "difference": None,
            "institutional_effect": "UNAVAILABLE",
            "tone": "neutral",
            "freshness": storage_meta.get("status") or "UNAVAILABLE",
            "api_key_required": key_note,
            "interpretation": (
                "Official EIA working-gas series unavailable. "
                "Run python scripts/refresh_natural_gas_drivers.py"
            ),
            "required_for_v2": True,
        }

    # --- Dry gas production (official cache preferred; IPN213111S only as labelled FALLBACK) ---
    prod_path = cache_map.get("dry_gas_production", "")
    prod_meta = _cache_meta(prod_path)
    prod_cache = _load_cache_series(prod_path)
    using_proxy = False
    if prod_cache:
        prod_daily = prod_cache
        prod_src_label = prod_meta.get("official_source") or "Official dry-gas production cache"
        prod_series_id = prod_meta.get("series_identifier")
    else:
        prod_daily = _load_fred(fred_map.get("dry_gas_production_proxy", "IPN213111S"))
        using_proxy = True
        prod_src_label = "FALLBACK: FRED IPN213111S (oil & gas extraction IP — not official dry-gas)"
        prod_series_id = fred_map.get("dry_gas_production_proxy", "IPN213111S")
    prod_weekly = _weekly_from_daily(prod_daily, dates)
    if len(prod_weekly) >= MIN_WEEKS:
        vals = [prod_weekly[d] for d in dates]
        mean = sum(vals) / len(vals)
        std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals)) or 1.0
        bundle.features["dry_gas_production"] = [(v - mean) / std for v in vals]
        bundle.lineage["dry_gas_production"] = {
            "source_name": "FALLBACK proxy" if using_proxy else (prod_meta.get("official_source") or "Official"),
            "source_id": prod_series_id or prod_path,
            "source_date": max(prod_weekly.keys()),
        }
        latest = vals[-1]
        prev = vals[-5] if len(vals) >= 5 else vals[-2] if len(vals) >= 2 else None
        avg4 = sum(vals[-4:]) / min(4, len(vals))
        yoy = None
        if len(vals) >= 53 and vals[-53]:
            yoy = 100.0 * (latest - vals[-53]) / vals[-53]
        recent = sum(vals[-13:]) / min(13, len(vals))
        effect = "Bearish" if latest >= recent else "Bullish"
        bundle.driver_cards["production"] = {
            "id": "production",
            "label": "US Dry Gas Production",
            "unit": "index" if using_proxy else (prod_meta.get("units") or "Bcf/d"),
            "available": True,
            "current": round(latest, 3),
            "previous": round(prev, 3) if prev is not None else None,
            "avg_4": round(avg4, 3),
            "yoy_pct": round(yoy, 2) if yoy is not None else None,
            "as_of": prod_meta.get("latest_observation_date") or as_of,
            "source": prod_src_label,
            "series_id": prod_series_id,
            "proxy": using_proxy,
            "fallback": using_proxy,
            "freshness": "FALLBACK" if using_proxy else (prod_meta.get("status") or "LIVE"),
            "institutional_effect": effect,
            "tone": "bearish" if effect == "Bearish" else "bullish",
            "interpretation": (
                "Elevated production is typically bearish for price. "
                + (
                    "FALLBACK only — oil & gas extraction IP is not official dry-gas production."
                    if using_proxy
                    else "Official dry natural gas production."
                )
            ),
        }
    else:
        bundle.driver_cards["production"] = {
            "id": "production",
            "label": "US Dry Gas Production",
            "unit": "Bcf/d",
            "available": False,
            "current": None,
            "institutional_effect": "UNAVAILABLE",
            "tone": "neutral",
            "freshness": "UNAVAILABLE",
            "interpretation": "Production driver unavailable — run refresh_natural_gas_drivers.py",
            "required_for_v2": True,
        }

    # --- LNG exports ---
    lng_path = cache_map.get("lng_exports", "")
    lng_meta = _cache_meta(lng_path)
    lng_daily = _load_cache_series(lng_path)
    lng_weekly = _weekly_from_daily(lng_daily, dates) if lng_daily else {}
    if len(lng_weekly) >= MIN_WEEKS:
        vals = [lng_weekly[d] for d in dates]
        mean = sum(vals) / len(vals)
        std = math.sqrt(sum((v - mean) ** 2 for v in vals) / len(vals)) or 1.0
        bundle.features["lng_exports"] = [(v - mean) / std for v in vals]
        bundle.lineage["lng_exports"] = {
            "source_name": lng_meta.get("official_source") or "EIA LNG exports",
            "source_id": lng_meta.get("series_identifier") or lng_path,
            "source_date": max(lng_weekly.keys()),
        }
        latest = vals[-1]
        prev = vals[-5] if len(vals) >= 5 else vals[-2] if len(vals) >= 2 else None
        avg4 = sum(vals[-4:]) / min(4, len(vals))
        recent = sum(vals[-13:]) / min(13, len(vals))
        effect = "Bullish" if latest >= recent else "Bearish"
        bundle.driver_cards["lng_exports"] = {
            "id": "lng_exports",
            "label": "LNG Exports",
            "unit": lng_meta.get("units") or "Bcf/d",
            "available": True,
            "current": round(latest, 3),
            "previous": round(prev, 3) if prev is not None else None,
            "avg_4": round(avg4, 3),
            "as_of": lng_meta.get("latest_observation_date") or as_of,
            "source": lng_meta.get("official_source") or lng_path,
            "series_id": lng_meta.get("series_identifier"),
            "concept": lng_meta.get("concept") or "U.S. LNG exports",
            "freshness": lng_meta.get("status") or "LIVE",
            "institutional_effect": effect,
            "tone": "bullish" if effect == "Bullish" else "bearish",
            "interpretation": (
                "Strong LNG exports are typically supportive for domestic prices "
                "(export volumes — not terminal feedgas)."
            ),
        }
    else:
        key_note = lng_meta.get("api_key_required") or "EIA_API_KEY"
        bundle.driver_cards["lng_exports"] = {
            "id": "lng_exports",
            "label": "LNG Exports",
            "unit": "Bcf/d",
            "available": False,
            "current": None,
            "institutional_effect": "UNAVAILABLE",
            "tone": "neutral",
            "freshness": lng_meta.get("status") or "UNAVAILABLE",
            "api_key_required": key_note,
            "interpretation": (
                f"Official US LNG exports unavailable. Set {key_note} (or FRED LNG series) and run "
                "python scripts/refresh_natural_gas_drivers.py"
            ),
            "required_for_v2": True,
        }

    # --- HDD / CDD ---
    # Zero midsummer HDD is a genuine observation (not missing). Anomalies must use
    # same ISO-week climatology — never full-sample annual z-score.
    for weather_key, label, feature_name in (
        ("hdd", "Heating Degree Days", "hdd_anomaly"),
        ("cdd", "Cooling Degree Days", "cdd_anomaly"),
    ):
        wpath = cache_map.get(weather_key, f"data/cache/energy_drivers/noaa_{weather_key}.json")
        wmeta = _cache_meta(wpath)
        wseries = _load_cache_series(wpath)
        # Prefer native week keys from cache; asof onto valuation dates
        wweekly = _weekly_from_daily(wseries, dates) if wseries else {}
        if len(wweekly) >= MIN_WEEKS // 2:
            # Week-of-year climatology from prior years only (no look-ahead)
            by_wn: dict[int, list[float]] = {}
            dated: list[tuple[str, float, int]] = []
            for d, v in sorted(wseries.items()):
                try:
                    wn = datetime.strptime(d[:10], "%Y-%m-%d").isocalendar()[1]
                except ValueError:
                    continue
                dated.append((d[:10], float(v), wn))
                by_wn.setdefault(wn, []).append(float(v))

            anomalies: list[float] = []
            levels: list[float] = []
            normals: list[float | None] = []
            ok = True
            for d in dates:
                v = wweekly.get(d)
                if v is None:
                    ok = False
                    break
                try:
                    wn = datetime.strptime(d[:10], "%Y-%m-%d").isocalendar()[1]
                    year = datetime.strptime(d[:10], "%Y-%m-%d").year
                except ValueError:
                    ok = False
                    break
                # peers: same ISO week, strictly earlier years
                peers = [
                    val
                    for date, val, w in dated
                    if w == wn and date < d and datetime.strptime(date, "%Y-%m-%d").year < year
                ]
                peers = peers[-10:] if len(peers) > 10 else peers
                if len(peers) < 3:
                    # insufficient climatology — do not invent anomaly
                    ok = False
                    break
                mu = sum(peers) / len(peers)
                sd = math.sqrt(sum((p - mu) ** 2 for p in peers) / len(peers)) or 1.0
                levels.append(float(v))
                normals.append(mu)
                anomalies.append((float(v) - mu) / sd)

            latest = levels[-1] if levels else None
            # Card always shows actual level when cache has asof value
            card_level = wweekly.get(as_of)
            if card_level is None and wseries:
                card_level = _asof_value(wseries, as_of)
            card_normal = normals[-1] if normals else None
            card_anom = anomalies[-1] if anomalies and ok else None

            # Zero is valid in summer for HDD — never coerce missing→0
            zero_note = ""
            if weather_key == "hdd" and card_level is not None and abs(card_level) < 1e-9:
                zero_note = (
                    " Current HDD of 0 is a genuine midsummer observation (no heating demand), "
                    "not missing data."
                )

            if ok and len(anomalies) == len(dates):
                # Keep series for experimental testing only — promotion decided in valuation module
                bundle.features[feature_name] = anomalies

            effect = "Neutral"
            if card_anom is not None:
                effect = "Bullish" if card_anom > 0.25 else "Bearish" if card_anom < -0.25 else "Neutral"

            data_quality = "OK" if card_normal is not None else "CLIMATOLOGY_INSUFFICIENT"
            if not ok:
                data_quality = "ANOMALY_INVALID_FOR_REGRESSION"

            bundle.driver_cards[weather_key] = {
                "id": weather_key,
                "label": label,
                "unit": "degree-days",
                "available": card_level is not None,
                "current": round(card_level, 2) if card_level is not None else None,
                "normal": round(card_normal, 2) if card_normal is not None else None,
                "anomaly": round(card_anom, 3) if card_anom is not None else None,
                "as_of": wmeta.get("latest_observation_date") or as_of,
                "source": wmeta.get("official_source") or wpath,
                "freshness": wmeta.get("status") or "LIVE",
                "data_quality": data_quality,
                "valuation_role": "EXPERIMENTAL DRIVER",
                "in_fair_value": False,
                "valuation_note": "NOT INCLUDED IN FAIR VALUE pending walk-forward promotion",
                "institutional_effect": effect,
                "tone": "bullish" if effect == "Bullish" else "bearish" if effect == "Bearish" else "neutral",
                "interpretation": (
                    f"{label}: actual={card_level:.1f}, week-of-year normal="
                    f"{card_normal:.1f}, anomaly="
                    f"{card_anom:+.2f}σ vs same-week climatology."
                    if card_level is not None and card_normal is not None and card_anom is not None
                    else f"{label} level available; week-of-year climatology incomplete."
                )
                + zero_note,
            }
        if weather_key not in bundle.driver_cards:
            bundle.driver_cards[weather_key] = {
                "id": weather_key,
                "label": label,
                "unit": "degree-days",
                "available": False,
                "current": None,
                "institutional_effect": "UNAVAILABLE",
                "tone": "neutral",
                "freshness": "UNAVAILABLE",
                "valuation_role": "INVALID / DATA QUALITY FAILURE",
                "in_fair_value": False,
                "valuation_note": "NOT INCLUDED IN FAIR VALUE",
                "interpretation": "Official weather degree-day series unavailable in cache.",
            }

    # --- Seasonality ---
    seas = _seasonality_factor(dates)
    seas_meta = _seasonality_export_bias()
    if len(seas) >= MIN_WEEKS // 2:
        last = None
        col: list[float] = []
        for d in dates:
            v = seas.get(d, last)
            if v is None:
                col = []
                break
            last = v
            col.append(float(v))
        if len(col) == len(dates):
            bundle.features["seasonality_factor"] = col
            bundle.lineage["seasonality_factor"] = {
                "source_name": "HPTL seasonality",
                "source_id": "week_of_year_price_factor",
                "source_date": as_of,
            }
            latest = col[-1]
            effect = "Bullish" if latest > 0.15 else "Bearish" if latest < -0.15 else "Neutral"
            bias = seas_meta.get("seasonality_bias") or effect
            bundle.driver_cards["seasonality"] = {
                "id": "seasonality",
                "label": "Seasonality",
                "unit": "z-score",
                "available": True,
                "current": round(latest, 3),
                "seasonality_bias": bias,
                "month_avg_return_pct": seas_meta.get("month_avg_return_pct"),
                "as_of": as_of,
                "source": "Existing HPTL seasonality + week-of-year factor",
                "valuation_role": "INFORMATIONAL ONLY",
                "in_fair_value": False,
                "valuation_note": "NOT INCLUDED IN FAIR VALUE",
                "institutional_effect": str(bias) if bias else effect,
                "tone": (
                    "bullish"
                    if str(bias).lower().startswith("bull") or effect == "Bullish"
                    else "bearish"
                    if str(bias).lower().startswith("bear") or effect == "Bearish"
                    else "neutral"
                ),
                "interpretation": (
                    "Informational only — not a validated Natural Gas seasonality valuation model. "
                    + (
                        seas_meta.get("seasonality_reason")
                        or f"Seasonal week-of-year factor {latest:+.2f}σ vs multi-year average."
                    )
                ),
            }
    if "seasonality" not in bundle.driver_cards:
        bundle.driver_cards["seasonality"] = {
            "id": "seasonality",
            "label": "Seasonality",
            "unit": "—",
            "available": bool(seas_meta.get("wired")),
            "current": seas_meta.get("month_avg_return_pct"),
            "seasonality_bias": seas_meta.get("seasonality_bias"),
            "institutional_effect": seas_meta.get("seasonality_bias") or "Unavailable",
            "tone": "neutral",
            "interpretation": seas_meta.get("seasonality_reason")
            or "Seasonality export present but factor not aligned for regression.",
        }

    return bundle
