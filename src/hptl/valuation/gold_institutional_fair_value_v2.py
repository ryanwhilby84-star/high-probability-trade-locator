from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
PROCESSED = ROOT / "data" / "processed"
OUT = PROCESSED / "gold_valuation_drivers_latest.csv"


GOLD_ALIASES = {
    "gold", "gold / gc", "gc", "xauusd", "xau/usd",
    "gold futures", "gold - commodity exchange inc.",
}

SILVER_ALIASES = {
    "silver", "silver / si", "si", "xagusd", "xag/usd",
    "silver futures",
}

DXY_ALIASES = {
    "us dollar index", "dxy", "dx", "usd index", "dollar index",
    "u.s. dollar index", "usdx", "dxy index", "broad dollar",
}


def read_json(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def rows_from_json(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]

    if isinstance(data, dict):
        for key in ["records", "rows", "data", "history", "series", "prices", "timeline", "markets"]:
            value = data.get(key)
            if isinstance(value, list):
                return [x for x in value if isinstance(x, dict)]

        rows: list[dict[str, Any]] = []
        for key, value in data.items():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        row = dict(item)
                        row.setdefault("market", key)
                        rows.append(row)
        return rows

    return []


def find_col(df: pd.DataFrame, candidates: list[str]) -> str | None:
    lower = {str(c).lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lower:
            return lower[candidate.lower()]
    return None


def normalise_market(value: Any) -> str:
    return str(value or "").strip().lower()


def is_gold_name(value: Any) -> bool:
    v = normalise_market(value)
    return v in GOLD_ALIASES or v.startswith("gold")


def is_silver_name(value: Any) -> bool:
    v = normalise_market(value)
    return v in SILVER_ALIASES or v.startswith("silver")


def is_dxy_name(value: Any) -> bool:
    v = normalise_market(value)
    return (
        v in DXY_ALIASES
        or "dxy" in v
        or "dollar index" in v
        or "usd index" in v
        or "broad dollar" in v
    )


def extract_market_series(df: pd.DataFrame, picker, output_col: str) -> pd.DataFrame:
    date_col = find_col(df, ["date", "week", "timestamp", "time", "cot_report_date", "report_date", "observation_date"])
    market_col = find_col(df, ["market", "symbol", "instrument", "name", "raw_market", "label", "ticker", "series", "series_id"])
    close_col = find_col(df, ["close", "price", "value", "last", "settle", "adj_close", "close_price", "level"])

    if not date_col or not market_col or not close_col:
        return pd.DataFrame(columns=["date", output_col])

    tmp = df.copy()
    mask = tmp[market_col].map(picker)

    out = tmp.loc[mask, [date_col, close_col]].copy()
    out = out.rename(columns={date_col: "date", close_col: output_col})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out[output_col] = pd.to_numeric(out[output_col], errors="coerce")

    return out.dropna().sort_values("date").drop_duplicates("date")


def flatten_json_to_rows(data: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []

    def walk(node: Any, context: dict[str, Any] | None = None) -> None:
        context = context or {}

        if isinstance(node, list):
            for item in node:
                walk(item, context)
            return

        if isinstance(node, dict):
            keys = set(node.keys())
            has_date = bool(keys & {"date", "week", "timestamp", "time", "report_date", "observation_date"})
            has_value = bool(keys & {"close", "price", "value", "last", "settle", "adj_close", "close_price", "level"})

            if has_date and has_value:
                row = dict(context)
                row.update(node)
                rows.append(row)
                return

            for key, value in node.items():
                next_context = dict(context)
                if is_gold_name(key) or is_silver_name(key) or is_dxy_name(key):
                    next_context.setdefault("market", key)
                walk(value, next_context)

    walk(data)
    return rows


def load_prices_from_json_file(path: Path) -> pd.DataFrame:
    data = read_json(path)
    rows = rows_from_json(data) or flatten_json_to_rows(data)

    if not rows:
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    gold = extract_market_series(df, is_gold_name, "gold_close")
    silver = extract_market_series(df, is_silver_name, "silver_close")
    dxy = extract_market_series(df, is_dxy_name, "dxy")

    if gold.empty:
        return pd.DataFrame()

    merged = gold
    for frame in [silver, dxy]:
        if not frame.empty:
            merged = merged.merge(frame, on="date", how="left")

    return merged.sort_values("date").drop_duplicates("date")


def load_price_series() -> pd.DataFrame:
    candidates = [
        PROCESSED / "canonical_price_timeline_latest.json",
        PROCESSED / "prices_latest.json",
        PROCESSED / "seasonality_price_latest.json",
        PROCESSED / "valuation_latest.json",
    ]

    for path in candidates:
        frame = load_prices_from_json_file(path)
        if not frame.empty:
            print(f"[gold-v3-drivers] price source: {path}")
            return frame

    raise ValueError("Could not find Gold rows in known processed price sources.")


def extract_history_series(path_name: str, output_col: str, value_candidates: list[str]) -> pd.DataFrame:
    path = PROCESSED / path_name
    data = read_json(path)
    rows = rows_from_json(data) or flatten_json_to_rows(data)

    if not rows:
        return pd.DataFrame(columns=["date", output_col])

    df = pd.DataFrame(rows)

    date_col = find_col(df, ["date", "week", "as_of_date", "report_date", "observation_date"])
    value_col = find_col(df, [output_col] + value_candidates)

    if not date_col or not value_col:
        return pd.DataFrame(columns=["date", output_col])

    out = df[[date_col, value_col]].copy()
    out = out.rename(columns={date_col: "date", value_col: output_col})
    out["date"] = pd.to_datetime(out["date"], errors="coerce")
    out[output_col] = pd.to_numeric(out[output_col], errors="coerce")

    return out.dropna().sort_values("date").drop_duplicates("date")


def load_dxy_from_any_processed_file() -> pd.DataFrame:
    # First try obvious dedicated files.
    dedicated_names = [
        "dxy_latest.json",
        "dxy_history_latest.json",
        "dxy_price_latest.json",
        "dollar_index_latest.json",
        "macro_hub_latest.json",
        "macro_latest.json",
        "valuation_macro_latest.json",
    ]

    for name in dedicated_names:
        path = PROCESSED / name
        if not path.exists():
            continue

        data = read_json(path)
        rows = rows_from_json(data) or flatten_json_to_rows(data)
        if not rows:
            continue

        df = pd.DataFrame(rows)

        direct = extract_history_series(name, "dxy", ["dxy", "dxy_broad", "dollar_index", "usd_index", "close", "price", "value", "level"])
        if not direct.empty:
            print(f"[gold-v3-drivers] DXY source: {path}")
            return direct

        market = extract_market_series(df, is_dxy_name, "dxy")
        if not market.empty:
            print(f"[gold-v3-drivers] DXY source: {path}")
            return market

    # Then brute-force scan processed JSON files.
    for path in sorted(PROCESSED.glob("*.json")):
        data = read_json(path)
        rows = rows_from_json(data) or flatten_json_to_rows(data)
        if not rows:
            continue

        df = pd.DataFrame(rows)

        market = extract_market_series(df, is_dxy_name, "dxy")
        if not market.empty and len(market) >= 60:
            print(f"[gold-v3-drivers] DXY source: {path}")
            return market

        date_col = find_col(df, ["date", "week", "as_of_date", "report_date", "observation_date"])
        value_col = find_col(df, ["dxy", "dxy_broad", "dollar_index", "usd_index", "broad_dollar"])
        if date_col and value_col:
            out = df[[date_col, value_col]].copy()
            out = out.rename(columns={date_col: "date", value_col: "dxy"})
            out["date"] = pd.to_datetime(out["date"], errors="coerce")
            out["dxy"] = pd.to_numeric(out["dxy"], errors="coerce")
            out = out.dropna().sort_values("date").drop_duplicates("date")
            if len(out) >= 60:
                print(f"[gold-v3-drivers] DXY source: {path}")
                return out

    return pd.DataFrame(columns=["date", "dxy"])


def merge_optional(drivers: pd.DataFrame, frame: pd.DataFrame, col: str) -> pd.DataFrame:
    if frame.empty:
        print(f"[gold-v3-drivers] optional missing: {col}")
        return drivers

    if col in drivers.columns:
        drivers = drivers.drop(columns=[col])

    print(f"[gold-v3-drivers] merged optional: {col} rows={len(frame)}")
    return drivers.merge(frame, on="date", how="left")


def main() -> None:
    drivers = load_price_series()

    dxy = load_dxy_from_any_processed_file()
    drivers = merge_optional(drivers, dxy, "dxy")

    optional_series = [
        (
            "gold_real_yield_research_latest.json",
            "real_yield_or_tips",
            ["real_yield", "real_yield_10y", "tips", "value", "level", "feature_value"],
        ),
        (
            "gold_breakeven_inflation_research_latest.json",
            "breakeven_inflation",
            ["breakeven", "breakeven_10y", "inflation_expectations", "value", "level", "feature_value"],
        ),
        (
            "gold_cb_driver_comparison_latest.json",
            "central_bank_net_purchases",
            ["cb_roll12", "cb_lag1", "central_bank_net_purchases", "net_purchases", "value", "feature_value"],
        ),
        (
            "gold_production_cb_driver_latest.json",
            "central_bank_net_purchases",
            ["cb_roll12", "cb_lag1", "central_bank_net_purchases", "net_purchases", "value", "feature_value"],
        ),
    ]

    for file_name, col, candidates in optional_series:
        frame = extract_history_series(file_name, col, candidates)
        drivers = merge_optional(drivers, frame, col)

    drivers = drivers.sort_values("date").drop_duplicates("date").ffill()

    OUT.parent.mkdir(parents=True, exist_ok=True)
    drivers.to_csv(OUT, index=False)

    print(f"[gold-v3-drivers] wrote: {OUT}")
    print(f"[gold-v3-drivers] rows: {len(drivers)}")
    print(f"[gold-v3-drivers] columns: {list(drivers.columns)}")
    print(drivers.tail(10).to_string(index=False))


if __name__ == "__main__":
    main()