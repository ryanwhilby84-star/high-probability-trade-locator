"""Canonical price timeline — one source of truth per instrument.

All HPTL consumers (COT alignment, seasonality, valuation, charts) must derive
price from this module. Daily bars are canonical; weekly/monthly series are
explicitly derived and labelled.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from hptl.config import PROCESSED_DIR, PROJECT_ROOT
from hptl.markets.instrument_registry import InstrumentSpec, get_instrument
from hptl.alpha_vantage.mappings import resolve_alpha_mapping
from hptl.prices.coverage import load_price_coverage, oanda_symbol_for, select_price_source
from hptl.prices.data_integrity import _instrument_price_row
from hptl.prices.price_store import CANONICAL_PATH, load_price_store, load_instrument_record_internal

Confidence = Literal["high", "medium", "low"]
MatchType = Literal["exact", "prior_close", "null"]

# Alternate price-store keys (same instrument, different id).
PRICE_ALIASES: dict[str, list[str]] = {
    "Copper / HG": ["Copper"],
}

# FRED / OANDA supplements when store history is shorter than required window.
FRED_PRICE_FALLBACK: dict[str, str] = {
    "NASDAQ / NQ": "NASDAQCOM",
    "S&P 500 / ES": "SP500",
    "Dow / YM": "DJIA",
    "US Nas 100": "NASDAQCOM",
    "US SPX 500": "SP500",
    "US Wall St 30": "DJIA",
    "US Dollar Index / DX": "DTWEXBGS",
    "Crude Oil / CL": "DCOILWTICO",
    "Natural Gas / NG": "DHHNGSP",
    "Cocoa": "PCOCOUSDM",
    "Coffee": "PCOFFOTMUSDM",
    "Cotton": "PCOTTINDUSDM",
}

OANDA_PRICE_FALLBACK: dict[str, str] = {
    "Gold": "XAU_USD",
    "Silver": "XAG_USD",
    "Crude Oil / CL": "WTICO_USD",
    "Copper / HG": "XCU_USD",
    "Natural Gas / NG": "NATGAS_USD",
    "Wheat": "WHEAT_USD",
    "Soybeans": "SOYBN_USD",
    "Sugar": "SUGAR_USD",
    "Platinum": "XPT_USD",
    "Palladium": "XPD_USD",
    "Bitcoin": "BTC_USD",
}

FRED_OBS_START = "2016-01-01"

DERIVED_WEEKLY_ISO = "derived_iso_week_end_from_canonical_daily"
DERIVED_WEEKLY_NATIVE = "derived_native_weekly_from_canonical_store"
COT_MATCH_METHOD = "canonical_daily_match_as_of_cot_date"


def _num(v: Any) -> float | None:
    try:
        f = float(v)
    except (TypeError, ValueError):
        return None
    return f if f == f else None


def _norm_key(s: str) -> str:
    return str(s or "").lower().replace(" ", " ").strip()


def merge_price_series(
    primary: list[tuple[str, float]],
    supplement: list[tuple[str, float]],
) -> list[tuple[str, float]]:
    """Prepend supplement bars strictly before the first primary bar; primary wins on overlap."""
    if not supplement:
        return list(primary)
    if not primary:
        return list(supplement)
    cutoff = primary[0][0]
    prefix = [(d, c) for d, c in supplement if d < cutoff]
    merged = prefix + list(primary)
    by_date: dict[str, float] = {}
    for d, c in merged:
        by_date[d] = c
    return sorted(by_date.items(), key=lambda t: t[0])


def resample_weekly_closes(daily_bars: list[dict[str, Any]]) -> list[tuple[str, float]]:
    """Last close per ISO week from daily OHLC bars (derived series)."""
    buckets: dict[str, tuple[str, float]] = {}
    for b in daily_bars:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if not d or c is None:
            continue
        try:
            wk = pd.Timestamp(d).strftime("%G-W%V")
        except (TypeError, ValueError):
            wk = d[:7]
        prev = buckets.get(wk)
        if prev is None or d >= prev[0]:
            buckets[wk] = (d, c)
    return sorted(buckets.values(), key=lambda t: t[0])


def match_close_as_of(
    series: list[tuple[str, float]] | None,
    target: str,
) -> tuple[float | None, str | None, MatchType | None, int | None]:
    """Return (close, bar_date, match_type, lag_days) from canonical daily closes."""
    if not series or not target:
        return None, None, None, None
    cot_ts = pd.Timestamp(str(target)[:10])
    cot_key = cot_ts.strftime("%Y-%m-%d")

    for d, c in series:
        if d == cot_key:
            return c, d, "exact", 0

    prior: tuple[str, float] | None = None
    for d, c in series:
        if pd.Timestamp(d) <= cot_ts:
            prior = (d, c)
        else:
            break
    if prior is None:
        return None, None, None, None
    bar_date, close = prior
    lag = int((cot_ts - pd.Timestamp(bar_date)).days)
    return close, bar_date, "prior_close", lag


def _load_fred_price_bars(series_id: str, *, observation_start: str = FRED_OBS_START) -> list[tuple[str, float]]:
    try:
        from hptl.macro import fred_client

        df = fred_client.get_series_df(series_id, observation_start)
    except Exception:
        return []
    if df is None or df.empty:
        return []
    out: list[tuple[str, float]] = []
    for _, row in df.iterrows():
        d = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
        c = _num(row["value"])
        if c is not None:
            out.append((d, c))
    return out


def _load_oanda_price_bars(
    oanda_symbol: str,
    *,
    observation_start: str = FRED_OBS_START,
) -> list[tuple[str, float]]:
    try:
        from hptl.prices.fx_daily_backfill import fetch_chunked_daily

        start = pd.Timestamp(str(observation_start)[:10]).date()
        end = date.today()
        bars, _warnings = fetch_chunked_daily(oanda_symbol, start=start, end=end, chunk_size=500)
    except Exception:
        return []
    out: list[tuple[str, float]] = []
    for b in bars:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if d and c is not None:
            out.append((d, c))
    return out


def resolve_store_key(
    instrument_id: str,
    instruments: dict[str, dict[str, Any]] | None = None,
) -> tuple[dict[str, Any] | None, str | None]:
    """Resolve price store record + key for *instrument_id* (handles cot_proxy_of)."""
    doc = instruments
    if doc is None:
        doc = (load_price_store().get("instruments") or {})

    if instrument_id in doc:
        row = _instrument_price_row(instrument_id, doc)
        if row.get("daily") or row.get("weekly"):
            return row, instrument_id

    spec = get_instrument(instrument_id)
    if spec and spec.cot_proxy_of and spec.cot_proxy_of in doc:
        row = _instrument_price_row(spec.cot_proxy_of, doc)
        if row.get("daily") or row.get("weekly"):
            return row, spec.cot_proxy_of

    for iid, rec in doc.items():
        other = get_instrument(iid)
        if other and other.cot_proxy_of == instrument_id:
            row = _instrument_price_row(iid, doc)
            if row.get("daily") or row.get("weekly"):
                return row, iid

    for alias in PRICE_ALIASES.get(instrument_id, []):
        if alias in doc:
            row = _instrument_price_row(alias, doc)
            if row.get("daily") or row.get("weekly"):
                return row, alias

    target = _norm_key(instrument_id)
    for key, rec in doc.items():
        if _norm_key(key) == target:
            row = _instrument_price_row(key, doc)
            if row.get("daily") or row.get("weekly"):
                return row, key

    base = _norm_key(str(instrument_id).split("/")[0])
    for key, rec in doc.items():
        if _norm_key(str(key).split("/")[0]) == base:
            row = _instrument_price_row(key, doc)
            if row.get("daily") or row.get("weekly"):
                return row, key

    return None, None


def _canonical_symbol(instrument_id: str, store_key: str | None, source: str) -> str:
    spec = get_instrument(instrument_id)
    cov = load_price_coverage()
    if source == "oanda":
        if instrument_id in OANDA_PRICE_FALLBACK:
            return OANDA_PRICE_FALLBACK[instrument_id]
        if spec:
            sym = oanda_symbol_for(spec, cov)
            if sym:
                return sym
    if source == "alpha_vantage" and spec:
        mapping = resolve_alpha_mapping(spec)
        if mapping:
            return f"alpha_vantage:{mapping.symbol}"
    if source == "fred" and instrument_id in FRED_PRICE_FALLBACK:
        return f"fred:{FRED_PRICE_FALLBACK[instrument_id]}"
    if instrument_id in OANDA_PRICE_FALLBACK:
        return OANDA_PRICE_FALLBACK[instrument_id]
    if instrument_id in FRED_PRICE_FALLBACK:
        return f"fred:{FRED_PRICE_FALLBACK[instrument_id]}"
    return store_key or instrument_id


@dataclass
class CanonicalBar:
    date: str
    open: float | None
    high: float | None
    low: float | None
    close: float
    volume: float | None
    source: str
    source_freshness: str
    confidence: Confidence
    proxy: bool
    proxy_explanation: str | None = None


@dataclass
class CanonicalTimeline:
    instrument_id: str
    resolved_store_key: str | None
    canonical_source: str
    canonical_symbol: str
    proxy: bool
    proxy_explanation: str | None
    source_freshness: str | None
    confidence: Confidence
    bars: list[CanonicalBar] = field(default_factory=list)
    store_path: str = str(CANONICAL_PATH)
    store_generated_at: str | None = None
    supplement_meta: dict[str, Any] = field(default_factory=dict)

    @property
    def bar_count(self) -> int:
        return len(self.bars)

    @property
    def date_start(self) -> str | None:
        return self.bars[0].date if self.bars else None

    @property
    def date_end(self) -> str | None:
        return self.bars[-1].date if self.bars else None

    def daily_closes(self) -> list[tuple[str, float]]:
        return [(b.date, b.close) for b in self.bars]

    def daily_ohlc_public(self) -> list[dict[str, Any]]:
        return [
            {
                "date": b.date,
                "open": b.open,
                "high": b.high,
                "low": b.low,
                "close": b.close,
                "volume": b.volume,
                "source": b.source,
                "source_freshness": b.source_freshness,
                "confidence": b.confidence,
                "proxy": b.proxy,
                "proxy_explanation": b.proxy_explanation,
            }
            for b in self.bars
        ]

    def match_close_as_of(self, target: str) -> tuple[float | None, str | None, MatchType | None, int | None]:
        return match_close_as_of(self.daily_closes(), target)

    def derive_weekly_iso(self) -> tuple[list[tuple[str, float]], str]:
        """Derived weekly closes — ISO week-end last canonical daily close."""
        daily_dicts = [
            {"date": b.date, "close": b.close, "open": b.open, "high": b.high, "low": b.low}
            for b in self.bars
        ]
        return resample_weekly_closes(daily_dicts), DERIVED_WEEKLY_ISO

    def derive_weekly_native(self, store_weekly: list[dict[str, Any]]) -> tuple[list[tuple[str, float]], str]:
        """Use native weekly from store when present (still canonical store, labelled derived)."""
        out: list[tuple[str, float]] = []
        seen: set[str] = set()
        for b in store_weekly or []:
            d = str(b.get("date") or "")[:10]
            c = _num(b.get("close"))
            if d and c is not None and d not in seen:
                seen.add(d)
                out.append((d, c))
        if out:
            return out, DERIVED_WEEKLY_NATIVE
        return self.derive_weekly_iso()

    def weekly_for_seasonality(self, store_record: dict[str, Any] | None = None) -> tuple[list[tuple[str, float]], str]:
        """Single seasonality weekly derivation from canonical daily (always ISO from daily)."""
        return self.derive_weekly_iso()

    def range_52w(self) -> dict[str, Any] | None:
        if not self.bars:
            return None
        window = self.bars[-252:] if len(self.bars) > 252 else self.bars
        highs = [b.high for b in window if b.high is not None]
        lows = [b.low for b in window if b.low is not None]
        if not highs or not lows:
            closes = [b.close for b in window]
            if not closes:
                return None
            return {
                "high": max(closes),
                "low": min(closes),
                "as_of": window[-1].date,
                "start_date": window[0].date,
                "end_date": window[-1].date,
            }
        return {
            "high": max(highs),
            "low": min(lows),
            "as_of": window[-1].date,
            "start_date": window[0].date,
            "end_date": window[-1].date,
        }

    def to_summary(self) -> dict[str, Any]:
        weekly, weekly_method = self.derive_weekly_iso()
        return {
            "instrument_id": self.instrument_id,
            "resolved_store_key": self.resolved_store_key,
            "canonical_source": self.canonical_source,
            "canonical_symbol": self.canonical_symbol,
            "proxy": self.proxy,
            "proxy_explanation": self.proxy_explanation,
            "source_freshness": self.source_freshness,
            "confidence": self.confidence,
            "store_path": self.store_path,
            "store_generated_at": self.store_generated_at,
            "date_start": self.date_start,
            "date_end": self.date_end,
            "bar_count": self.bar_count,
            "derived_weekly_count": len(weekly),
            "derived_weekly_method": weekly_method,
            "cot_match_method": COT_MATCH_METHOD,
            "supplement_meta": self.supplement_meta,
            "consumers": {
                "cot": {"uses": "canonical_daily", "method": COT_MATCH_METHOD},
                "seasonality": {"uses": "canonical_daily", "method": weekly_method},
                "valuation": {"uses": "canonical_daily", "method": f"{weekly_method}+range_52w_from_canonical_daily"},
                "workstation_chart": {"uses": "canonical_daily", "method": COT_MATCH_METHOD},
            },
        }


def _apply_supplements(
    closes: list[tuple[str, float]],
    meta: dict[str, Any],
    *,
    instrument_id: str,
    window_start: str | None,
) -> list[tuple[str, float]]:
    if not window_start:
        return closes
    obs = str(window_start)[:10]
    cot_ts = pd.Timestamp(obs)
    earliest = closes[0][0] if closes else None
    need = not closes or pd.Timestamp(earliest) > cot_ts + pd.Timedelta(days=7)
    if not need:
        return closes

    fred_id = FRED_PRICE_FALLBACK.get(instrument_id)
    if fred_id:
        fred_bars = _load_fred_price_bars(fred_id, observation_start=obs)
        if fred_bars:
            meta["fred_series"] = fred_id
            meta["fred_bar_count"] = len(fred_bars)
            closes = merge_price_series(closes, fred_bars)

    earliest = closes[0][0] if closes else None
    still_short = not closes or pd.Timestamp(earliest) > cot_ts + pd.Timedelta(days=7)
    oanda_sym = OANDA_PRICE_FALLBACK.get(instrument_id)
    if still_short and oanda_sym:
        oanda_bars = _load_oanda_price_bars(oanda_sym, observation_start=obs)
        if oanda_bars:
            meta["oanda_symbol"] = oanda_sym
            meta["oanda_bar_count"] = len(oanda_bars)
            closes = merge_price_series(closes, oanda_bars)

    return closes


def build_canonical_timeline(
    instrument_id: str,
    *,
    window_start: str | None = None,
    instruments: dict[str, dict[str, Any]] | None = None,
    apply_supplements: bool = True,
) -> CanonicalTimeline | None:
    """Build canonical daily timeline for one instrument."""
    store_doc = load_price_store()
    instruments = instruments or (store_doc.get("instruments") or {})
    record, store_key = resolve_store_key(instrument_id, instruments)
    if not record:
        return None

    daily_raw = record.get("daily") or []
    if not daily_raw:
        return None

    source = select_price_source(instrument_id) or "price_store"
    internal = load_instrument_record_internal(instrument_id) or (
        load_instrument_record_internal(store_key) if store_key else None
    )
    fetched_via = (internal or {}).get("_fetched_via")
    if fetched_via in ("oanda", "oanda_backfill"):
        source = "oanda"
    elif fetched_via == "alpha_vantage":
        source = "alpha_vantage"
    elif fetched_via == "fred":
        source = "fred"
    scale = record.get("price_scale") or {}
    if scale.get("source") == "fred":
        source = "fred"
        symbol = f"fred:{scale.get('series_id') or FRED_PRICE_FALLBACK.get(instrument_id, instrument_id)}"
    else:
        symbol = _canonical_symbol(instrument_id, store_key, source)
    proxy = store_key != instrument_id if store_key else False
    proxy_expl: str | None = None
    if proxy and store_key:
        proxy_expl = f"Proxy price used: store key '{store_key}' for instrument '{instrument_id}'."

    supplement_meta: dict[str, Any] = {
        "fred_series": None,
        "fred_bar_count": 0,
        "oanda_symbol": None,
        "oanda_bar_count": 0,
    }

    closes: list[tuple[str, float]] = []
    bar_by_date: dict[str, dict[str, Any]] = {}
    for b in daily_raw:
        d = str(b.get("date") or "")[:10]
        c = _num(b.get("close"))
        if not d or c is None:
            continue
        bar_by_date[d] = b
        closes.append((d, c))
    closes.sort(key=lambda t: t[0])

    primary_start = closes[0][0] if closes else None
    if apply_supplements and window_start:
        closes = _apply_supplements(closes, supplement_meta, instrument_id=instrument_id, window_start=window_start)

    confidence: Confidence = "high" if source == "oanda" else "medium" if source == "alpha_vantage" else "medium"
    if supplement_meta.get("fred_bar_count") or supplement_meta.get("oanda_bar_count"):
        confidence = "medium"

    canonical_bars: list[CanonicalBar] = []
    for d, c in closes:
        raw = bar_by_date.get(d) or {}
        is_proxy_bar = bool(primary_start and d < primary_start)
        bar_source = source
        bar_proxy_expl = proxy_expl
        if is_proxy_bar:
            if supplement_meta.get("fred_series") and d < (primary_start or d):
                bar_source = f"fred:{supplement_meta['fred_series']}"
                bar_proxy_expl = (
                    f"Proxy price used: FRED {supplement_meta['fred_series']} prepended because "
                    f"store history starts {primary_start}."
                )
            elif supplement_meta.get("oanda_symbol"):
                bar_source = f"oanda:{supplement_meta['oanda_symbol']}"
                bar_proxy_expl = (
                    f"Proxy price used: OANDA {supplement_meta['oanda_symbol']} prepended because "
                    f"store history starts {primary_start}."
                )
            proxy = True
            if not proxy_expl:
                proxy_expl = bar_proxy_expl

        canonical_bars.append(
            CanonicalBar(
                date=d,
                open=_num(raw.get("open")) if raw else c,
                high=_num(raw.get("high")) if raw else c,
                low=_num(raw.get("low")) if raw else c,
                close=c,
                volume=_num(raw.get("volume")) if raw else None,
                source=bar_source,
                source_freshness=d,
                confidence=confidence,
                proxy=is_proxy_bar or (proxy and store_key != instrument_id),
                proxy_explanation=bar_proxy_expl if is_proxy_bar else proxy_expl,
            )
        )

    instrument_proxy = proxy or bool(supplement_meta.get("fred_bar_count") or supplement_meta.get("oanda_bar_count"))

    return CanonicalTimeline(
        instrument_id=instrument_id,
        resolved_store_key=store_key,
        canonical_source=source,
        canonical_symbol=symbol,
        proxy=instrument_proxy,
        proxy_explanation=proxy_expl,
        source_freshness=canonical_bars[-1].date if canonical_bars else None,
        confidence=confidence,
        bars=canonical_bars,
        store_path=str(CANONICAL_PATH),
        store_generated_at=store_doc.get("generated_at"),
        supplement_meta=supplement_meta,
    )


def load_canonical_timeline(
    instrument_id: str,
    *,
    window_start: str | None = None,
) -> CanonicalTimeline | None:
    return build_canonical_timeline(instrument_id, window_start=window_start)


def export_all_summaries(instrument_ids: list[str] | None = None) -> dict[str, Any]:
    from hptl.markets.instrument_registry import all_instrument_ids

    ids = instrument_ids or all_instrument_ids()
    summaries: dict[str, Any] = {}
    for iid in ids:
        tl = build_canonical_timeline(iid)
        if tl:
            summaries[iid] = tl.to_summary()
        else:
            summaries[iid] = {
                "instrument_id": iid,
                "available": False,
                "reason": "no_canonical_daily_bars",
            }
    return {
        "schema_version": 1,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "canonical_store": str(CANONICAL_PATH),
        "notes": (
            "One canonical daily timeline per instrument. Weekly/monthly series are derived only "
            "via canonical_timeline.derive_weekly_iso()."
        ),
        "instruments": summaries,
    }


def write_canonical_export(path: Path | None = None) -> Path:
    out = path or (PROCESSED_DIR / "canonical_price_timeline_latest.json")
    payload = export_all_summaries()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    public = PROJECT_ROOT / "web-dashboard" / "public" / "data" / "canonical_price_timeline_latest.json"
    public.parent.mkdir(parents=True, exist_ok=True)
    public.write_text(out.read_text(encoding="utf-8"), encoding="utf-8")
    return out
