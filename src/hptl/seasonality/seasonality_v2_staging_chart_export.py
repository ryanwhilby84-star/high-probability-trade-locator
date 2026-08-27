"""Seasonality V2 staging chart export — visual validation only (not production).

Usage:
    python -m hptl.seasonality.seasonality_v2_staging_chart_export

Writes:
    data/processed/seasonality_v2_staging_latest.json
    web-dashboard/public/data/seasonality_v2_staging_latest.json

Uses 10-year OANDA FX backfill from data/processed/prices/backfill/.
Does not modify live seasonality scoring or production price store.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.prices.fx_daily_backfill import STAGING_DIR, staging_path
from hptl.prices.fx_oanda_backfill_feasibility_audit import TEST_PAIRS
from hptl.seasonality.seasonality_engine import (
    build_chart_series,
    direction,
    divergence_read,
    interpolate_path,
    iso_week,
    normalized_year_path,
    project_forward,
    year_week_closes,
)
from hptl.seasonality.seasonality_price_bars import weekly_closes_from_record
from hptl.seasonality.seasonality_v2 import compute_seasonality_v2_from_daily, normalize_daily_bars

CANONICAL_PATH = PROCESSED_DIR / "seasonality_v2_staging_latest.json"
PUBLIC_PATH = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "seasonality_v2_staging_latest.json"

# store_key -> slash pair aliases for instrument page resolution
FX_MARKET_ALIASES: dict[str, tuple[str, ...]] = {
    "Euro FX / 6E": ("EUR/USD",),
    "British Pound / 6B": ("GBP/USD",),
    "Australian Dollar / 6A": ("AUD/USD",),
    "NZ Dollar / 6N": ("NZD/USD",),
    "Japanese Yen / 6J": ("USD/JPY",),
    "Canadian Dollar / 6C": ("USDCAD", "USD/CAD"),
    "Swiss Franc / 6S": ("USDCHF", "USD/CHF"),
    "EUR/JPY": ("EURJPY",),
}

PROJECTION_LABEL = "Historical 10Y seasonal projection — not a price forecast."


def _forward_window_read_extended(
    *,
    current_week: int,
    horizon: int,
    hist_years: list[int],
    yw: dict[int, dict[int, float]],
) -> dict[str, Any]:
    """Forward window stats with win rate (staging chart / audit only)."""
    end_week = min(52, current_week + horizon)
    if current_week >= 52:
        return {
            "weeks": horizon,
            "horizon_weeks": 0,
            "avg_return_pct": None,
            "win_rate_pct": None,
            "direction": "Neutral",
            "sample_years": 0,
            "available": False,
        }
    rets: list[float] = []
    for y in hist_years:
        path = normalized_year_path(yw.get(y, {}))
        if current_week in path and end_week in path and path[current_week] != 0:
            rets.append((path[end_week] / path[current_week] - 1.0) * 100.0)
    avg_ret = sum(rets) / len(rets) if rets else None
    wins = sum(1 for r in rets if r > 0)
    return {
        "weeks": horizon,
        "horizon_weeks": end_week - current_week,
        "avg_return_pct": round(avg_ret, 2) if avg_ret is not None else None,
        "win_rate_pct": round(wins / len(rets) * 100.0, 1) if rets else None,
        "direction": direction(avg_ret),
        "sample_years": len(rets),
        "available": avg_ret is not None and len(rets) > 0,
    }


def _historical_year_paths(hist_years: list[int], yw: dict[int, dict[int, float]]) -> list[dict[str, Any]]:
    """Normalized index paths for prior years (faint overlay on research chart)."""
    paths: list[dict[str, Any]] = []
    for year in hist_years[-10:]:
        raw = normalized_year_path(yw.get(year, {}))
        if not raw:
            continue
        filled = interpolate_path({w: raw.get(w) for w in range(1, 53)})
        if not filled:
            continue
        paths.append(
            {
                "year": year,
                "points": [{"week": w, "index": round(filled[w], 2)} for w in range(1, 53)],
            }
        )
    return paths


def _load_staging_record(store_key: str) -> dict[str, Any] | None:
    path = staging_path(store_key)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None


def _avg_path_10y(hist_years: list[int], yw: dict[int, dict[int, float]]) -> dict[int, float]:
    """Average normalized index path across up to 10 historical years."""
    from hptl.seasonality.seasonality_engine import avg_path

    raw = avg_path(hist_years[-10:], yw)
    return interpolate_path(raw)


def compute_v2_staging_chart_block(
    market: str,
    display_symbol: str,
    *,
    store_key: str,
    oanda_symbol: str,
) -> dict[str, Any]:
    """Build chart block from staged daily OHLC (Seasonality V2 validation)."""
    doc = _load_staging_record(store_key)
    if not doc:
        return {
            "market": market,
            "available": False,
            "reason": f"No staging backfill file for {store_key}",
            "reason_code": "staging_missing",
            "seasonality_v2_staging": True,
        }

    daily = normalize_daily_bars(doc.get("daily") or [])
    if len(daily) < 252:
        return {
            "market": market,
            "available": False,
            "reason": "Insufficient staged daily bars for 10Y chart.",
            "reason_code": "insufficient_history",
            "seasonality_v2_staging": True,
        }

    bars = weekly_closes_from_record({"daily": daily, "weekly": []})
    if not bars:
        return {
            "market": market,
            "available": False,
            "reason": "Could not derive weekly closes from staging.",
            "seasonality_v2_staging": True,
        }

    yw = year_week_closes(bars)
    all_years = sorted(yw.keys())
    latest_date = bars[-1][0]
    latest_close = bars[-1][1]
    latest_year, latest_week = iso_week(latest_date)
    current_year = latest_year
    hist_years = [y for y in all_years if y < current_year]
    years_count = len(hist_years)

    current_path_raw = normalized_year_path(yw.get(current_year, {}))
    if not current_path_raw:
        return {
            "market": market,
            "available": False,
            "reason": "No current-year weekly price path in staging.",
            "seasonality_v2_staging": True,
        }

    anchor_week = latest_week if latest_week in current_path_raw else max(
        w for w in current_path_raw if w <= latest_week
    )
    anchor_index = current_path_raw.get(anchor_week)
    anchor_close = yw.get(current_year, {}).get(anchor_week, latest_close)

    years_10y = hist_years[-10:] if years_count >= 10 else hist_years
    avg_10y = _avg_path_10y(hist_years, yw) if years_10y else {}
    proj_10y = (
        project_forward(anchor_week=anchor_week, anchor_index=anchor_index or 100.0, avg_filled=avg_10y)
        if avg_10y
        else {}
    )

    chart_series = build_chart_series(
        anchor_week=anchor_week,
        anchor_index=anchor_index,
        current_path_raw=current_path_raw,
        avg_3y={},
        avg_5y={},
        avg_10y=avg_10y,
        proj_3y={},
        proj_5y={},
        proj_10y=proj_10y,
        yw=yw,
        current_year=current_year,
    )

    v2 = compute_seasonality_v2_from_daily(
        daily,
        asset=display_symbol,
        data_source=f"staging:{oanda_symbol}:{store_key}",
    )

    read_4w = _forward_window_read_extended(
        current_week=anchor_week, horizon=4, hist_years=years_10y, yw=yw
    )
    read_8w = _forward_window_read_extended(
        current_week=anchor_week, horizon=8, hist_years=years_10y, yw=yw
    )
    read_12w = _forward_window_read_extended(
        current_week=anchor_week, horizon=12, hist_years=years_10y, yw=yw
    )
    div_read = divergence_read(
        anchor_week=anchor_week,
        anchor_index=anchor_index,
        avg_3y={},
        avg_5y={},
        avg_10y=avg_10y,
    )

    forward_available = bool(proj_10y) and any(
        proj_10y.get(w) is not None for w in range(anchor_week + 1, 53)
    )

    return {
        "market": market,
        "available": True,
        "seasonality_v2_staging": True,
        "data_source_mode": "seasonality_v2_staging",
        "display_symbol": display_symbol,
        "price_store_key": store_key,
        "oanda_symbol": oanda_symbol,
        "bar_source": "staging_daily_resampled_weekly",
        "latest_price": {
            "date": latest_date,
            "close": latest_close,
            "week": anchor_week,
            "index": round(anchor_index, 2) if anchor_index is not None else None,
        },
        "current_year": current_year,
        "current_week": anchor_week,
        "years_of_history": years_count,
        "windows_available": ["10Y"] if years_count >= 10 else (["10Y"] if years_count >= 5 else []),
        "forward_projection_available": forward_available,
        "availability_note": (
            "10Y history available (Seasonality V2 staging — OANDA backfill; not production)."
            if years_count >= 10
            else f"{years_count}Y staging history — partial 10Y average."
        ),
        "projection_label": PROJECTION_LABEL,
        "price_stale_note": (
            f"Latest price is {latest_date} ({v2.get('price_age_days')} days old)."
            if v2.get("price_stale")
            else None
        ),
        "chart_series": chart_series,
        "historical_year_paths": _historical_year_paths(years_10y, yw),
        "divergence_read": div_read,
        "v2_current_week_stats": {
            "iso_week": v2.get("current_iso_week"),
            "sample_size": v2.get("sample_size"),
            "win_rate_pct": v2.get("win_rate_pct"),
            "average_return_pct": v2.get("avg_return_pct"),
            "median_return_pct": v2.get("median_return_pct"),
            "standard_deviation_pct": v2.get("std_dev_pct"),
            "z_score": v2.get("z_score"),
            "confidence": v2.get("confidence"),
            "bias": v2.get("bias"),
        },
        "forward_read": {
            "next_4w": read_4w,
            "next_8w": read_8w,
            "next_12w": read_12w,
            "summary": (
                f"Seasonality V2 staging — ISO week {v2.get('current_iso_week')}: "
                f"{v2.get('bias')} bias, {v2.get('confidence')} confidence "
                f"(n={v2.get('sample_size')}, z={v2.get('z_score')}). "
                f"{PROJECTION_LABEL}"
            ),
        },
        "confidence": {
            "level": v2.get("confidence"),
            "detail": f"V2 audit confidence for current ISO week (not live pillar).",
        },
    }


def build_payload() -> dict[str, Any]:
    markets: dict[str, Any] = {}
    for display, oanda, store_key in TEST_PAIRS:
        block = compute_v2_staging_chart_block(
            store_key,
            display,
            store_key=store_key,
            oanda_symbol=oanda,
        )
        markets[store_key] = block
        for alias in FX_MARKET_ALIASES.get(store_key, ()):
            markets[alias] = {**block, "market": alias, "alias_of": store_key}

    ok = sum(1 for m in markets.values() if m.get("available") and not m.get("alias_of"))
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "parser": "hptl.seasonality.seasonality_v2_staging_chart_export",
        "audit_only": True,
        "live_wired": False,
        "data_source_mode": "seasonality_v2_staging",
        "staging_dir": str(STAGING_DIR),
        "source": f"{STAGING_DIR} (10Y OANDA FX backfill; visual validation only)",
        "notes": (
            "Seasonality V2 staging chart data for FX validation. "
            "Toggle in dashboard — does not replace production seasonality or trade signals."
        ),
        "projection_label": PROJECTION_LABEL,
        "summary": {
            "fx_pairs": len(TEST_PAIRS),
            "available": ok,
            "market_keys": len(markets),
        },
        "markets": markets,
    }


def write_exports(payload: dict[str, Any] | None = None) -> Path:
    payload = payload or build_payload()
    text = json.dumps(payload, indent=2, ensure_ascii=False)
    CANONICAL_PATH.parent.mkdir(parents=True, exist_ok=True)
    CANONICAL_PATH.write_text(text, encoding="utf-8")
    PUBLIC_PATH.parent.mkdir(parents=True, exist_ok=True)
    PUBLIC_PATH.write_text(text, encoding="utf-8")
    dist = PROJECT_ROOT / "web-dashboard" / "dist" / "data" / "seasonality_v2_staging_latest.json"
    if dist.parent.exists():
        dist.write_text(text, encoding="utf-8")
    return CANONICAL_PATH


def run() -> Path:
    path = write_exports()
    s = (json.loads(path.read_text(encoding="utf-8"))).get("summary") or {}
    print(f"Wrote {path} ({s.get('available')}/{s.get('fx_pairs')} FX V2 staging charts).")
    print(f"Public: {PUBLIC_PATH}")
    return path


def main() -> int:
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
