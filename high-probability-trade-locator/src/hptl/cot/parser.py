from __future__ import annotations

import io
import re
from pathlib import Path
from zipfile import ZipFile

import pandas as pd


def clean_column_name(name: object) -> str:
    """Convert CFTC column names into consistent snake_case."""
    text = str(name).strip().lower()
    text = text.replace("%", "pct")
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    cleaned = df.copy()
    cleaned.columns = [clean_column_name(col) for col in cleaned.columns]
    return cleaned


def _read_first_data_file_from_zip(zip_path: Path) -> pd.DataFrame:
    with ZipFile(zip_path) as zf:
        candidates = [name for name in zf.namelist() if name.lower().endswith((".txt", ".csv"))]
        if not candidates:
            raise ValueError(f"No .txt or .csv data file found inside {zip_path}")

        with zf.open(candidates[0]) as raw_file:
            data = raw_file.read()

    # CFTC historical compressed text files are comma-separated even when the extension is .txt.
    return pd.read_csv(io.BytesIO(data), low_memory=False)


def load_cot_file(raw_file_path: Path) -> pd.DataFrame:
    """Load a CFTC raw ZIP/CSV/TXT file into pandas."""
    suffix = raw_file_path.suffix.lower()
    if suffix == ".zip":
        return _read_first_data_file_from_zip(raw_file_path)
    if suffix in {".csv", ".txt"}:
        return pd.read_csv(raw_file_path, low_memory=False)
    raise ValueError(f"Unsupported COT file type: {raw_file_path.suffix}")


def normalise_cot_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Clean COT dataframe and add future-friendly market helper columns.

    Market mapping note:
    Add mappings later using columns such as market_and_exchange_names,
    cftc_contract_market_code, cftc_market_code, or commodity_name depending on
    the selected report type. Keep this parser generic so multiple COT report
    formats can flow through the same pipeline.
    """
    cleaned = clean_columns(df)

    if "market_and_exchange_names" in cleaned.columns:
        cleaned["market_name_clean"] = cleaned["market_and_exchange_names"].astype(str).str.strip().str.upper()
    elif "commodity_name" in cleaned.columns:
        cleaned["market_name_clean"] = cleaned["commodity_name"].astype(str).str.strip().str.upper()
    else:
        cleaned["market_name_clean"] = "UNKNOWN"

    for col in ["report_date_as_yyyy_mm_dd", "as_of_date_in_form_yyyy_mm_dd", "report_date_as_mm_dd_yyyy"]:
        if col in cleaned.columns:
            cleaned["report_date"] = pd.to_datetime(cleaned[col], errors="coerce")
            break

    preferred_columns = [
        "report_date",
        "market_name_clean",
        "market_and_exchange_names",
        "cftc_contract_market_code",
        "cftc_market_code",
        "open_interest_all",
        "noncomm_positions_long_all",
        "noncomm_positions_short_all",
        "comm_positions_long_all",
        "comm_positions_short_all",
        "nonrept_positions_long_all",
        "nonrept_positions_short_all",
    ]
    existing_preferred = [col for col in preferred_columns if col in cleaned.columns]
    remaining = [col for col in cleaned.columns if col not in existing_preferred]
    return cleaned[existing_preferred + remaining]


def parse_cot_file(raw_file_path: Path) -> pd.DataFrame:
    return normalise_cot_dataframe(load_cot_file(raw_file_path))


def markets_included(df: pd.DataFrame, limit: int = 25) -> list[str]:
    if "market_name_clean" not in df.columns:
        return []
    values = sorted(v for v in df["market_name_clean"].dropna().astype(str).unique() if v and v != "UNKNOWN")
    return values[:limit]


def _to_number(value: str) -> int | None:
    text = value.strip().replace(",", "")
    if text == ".":
        return None
    try:
        return int(text)
    except ValueError:
        return None


def _parse_numbers(line: str) -> list[int | None]:
    return [_to_number(item) for item in re.findall(r"-?\d[\d,]*|\.", line)]


def _split_market_line(line: str) -> tuple[str, str, str] | None:
    match = re.match(r"^\s*(?P<market>.+?)\s+-\s+(?P<exchange>.+?)\s+Code-(?P<code>[A-Z0-9]+)\s*$", line)
    if not match:
        return None
    return (
        match.group("market").strip(),
        match.group("exchange").strip(),
        match.group("code").strip(),
    )


def _cot_bias(noncommercial_net: int | None) -> str:
    if noncommercial_net is None:
        return "NEUTRAL"
    if noncommercial_net > 0:
        return "BULLISH"
    if noncommercial_net < 0:
        return "BEARISH"
    return "NEUTRAL"


def parse_cme_futures_only_text(raw_text: str) -> pd.DataFrame:
    """Parse the CFTC CME Futures Only HTML/text report into dashboard rows.

    The CME page is a fixed-width report embedded in HTML. Each market block
    starts with a market/exchange/code line, followed by a report date, open
    interest, commitments, and one-week changes. Four-week change is not
    available in this source page, so it is intentionally returned as <NA>.
    """
    from html import unescape

    text = unescape(raw_text)
    lines = text.splitlines()
    rows: list[dict[str, object]] = []

    for index, line in enumerate(lines):
        market_parts = _split_market_line(line)
        if not market_parts:
            continue

        market_name, exchange, code = market_parts
        block = lines[index : index + 18]
        report_date = pd.NaT
        open_interest: int | None = None
        commitments: list[int | None] = []
        changes: list[int | None] = []

        for block_index, block_line in enumerate(block):
            if block_index > 0 and _split_market_line(block_line):
                break
            date_match = re.search(r"POSITIONS AS OF\s+(\d{2}/\d{2}/\d{2})", block_line)
            if date_match:
                report_date = pd.to_datetime(date_match.group(1), format="%m/%d/%y", errors="coerce")

            oi_match = re.search(r"OPEN INTEREST:\s+([\d,\.]+)", block_line)
            if oi_match and "CHANGE IN OPEN INTEREST" not in block_line:
                open_interest = _to_number(oi_match.group(1))

            if block_line.strip() == "COMMITMENTS" and block_index + 1 < len(block):
                commitments = _parse_numbers(block[block_index + 1])

            if "CHANGES FROM" in block_line and block_index + 1 < len(block):
                changes = _parse_numbers(block[block_index + 1])

        if len(commitments) < 5:
            continue

        noncommercial_long = commitments[0]
        noncommercial_short = commitments[1]
        commercial_long = commitments[3]
        commercial_short = commitments[4]
        commercial_net = None if commercial_long is None or commercial_short is None else commercial_long - commercial_short
        noncommercial_net = None if noncommercial_long is None or noncommercial_short is None else noncommercial_long - noncommercial_short

        weekly_change = None
        if len(changes) >= 2 and changes[0] is not None and changes[1] is not None:
            weekly_change = changes[0] - changes[1]

        rows.append(
            {
                "report_date": report_date,
                "market_name": market_name,
                "exchange": exchange,
                "cftc_contract_market_code": code,
                "open_interest": open_interest,
                "noncommercial_long": noncommercial_long,
                "noncommercial_short": noncommercial_short,
                "commercial_long": commercial_long,
                "commercial_short": commercial_short,
                "commercial_net": commercial_net,
                "noncommercial_net": noncommercial_net,
                "weekly_change": weekly_change,
                "four_week_change": pd.NA,
                "bias": _cot_bias(noncommercial_net),
                "source_report": "CME Futures Only",
            }
        )

    return pd.DataFrame(rows)


def parse_cme_futures_only_file(raw_file_path: Path) -> pd.DataFrame:
    return parse_cme_futures_only_text(raw_file_path.read_text(encoding="utf-8", errors="ignore"))


def filter_cme_index_markets(df: pd.DataFrame) -> pd.DataFrame:
    from hptl.cot.contracts import CME_INDEX_MAPPINGS

    if df.empty or "cftc_contract_market_code" not in df.columns:
        return df.copy()

    filtered = df[df["cftc_contract_market_code"].astype(str).isin(CME_INDEX_MAPPINGS)].copy()
    if filtered.empty:
        return filtered

    filtered["dashboard_market"] = filtered["cftc_contract_market_code"].map(
        {code: mapping.dashboard_name for code, mapping in CME_INDEX_MAPPINGS.items()}
    )
    filtered["asset_class"] = filtered["cftc_contract_market_code"].map(
        {code: mapping.asset_class for code, mapping in CME_INDEX_MAPPINGS.items()}
    )
    return filtered


def _first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    return next((column for column in candidates if column in df.columns), None)


def _series_or_na(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    column = _first_existing_column(df, candidates)
    if column is None:
        return pd.Series([pd.NA] * len(df), index=df.index)
    return df[column]


def _numeric_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    return pd.to_numeric(_series_or_na(df, candidates), errors="coerce")


def _market_exchange_from_name(values: pd.Series) -> tuple[pd.Series, pd.Series]:
    text = values.fillna("").astype(str).str.strip()
    parts = text.str.rsplit(" - ", n=1, expand=True)
    if len(parts.columns) == 2:
        return parts[0].str.strip(), parts[1].str.strip()
    return text, pd.Series([pd.NA] * len(text), index=text.index)


def cot_history_to_dashboard_rows(df: pd.DataFrame, source_report: str = "CFTC Historical COT") -> pd.DataFrame:
    """Convert a normalised CFTC historical dataframe into workbook dashboard fields.

    This keeps the pipeline report-format tolerant. Legacy Futures Only reports
    provide classic noncommercial/commercial columns directly. Disaggregated
    reports do not always expose those same columns, so unavailable fields are
    left blank rather than inventing values.
    """
    cleaned = clean_columns(df)
    date_col = _first_existing_column(
        cleaned,
        ["report_date", "report_date_as_yyyy_mm_dd", "as_of_date_in_form_yyyy_mm_dd", "report_date_as_mm_dd_yyyy"],
    )
    report_date = pd.to_datetime(cleaned[date_col], errors="coerce") if date_col else pd.Series(pd.NaT, index=cleaned.index)

    market_source = _series_or_na(cleaned, ["market_and_exchange_names", "commodity_name", "market_name_clean", "market_name"])
    market_name, exchange = _market_exchange_from_name(market_source)

    # Report-family tolerant field mapping:
    # - Legacy Futures Only uses classic noncommercial/commercial columns.
    # - Disaggregated Futures Only uses Managed Money and Producer/Merchant columns.
    # The workbook labels are kept unchanged from the good template:
    #   noncommercial_* == Managed Money where disaggregated fields are present
    #   commercial_*    == Producer/Merchant where disaggregated fields are present
    noncommercial_long = _numeric_series(
        cleaned,
        [
            "noncommercial_positions_long_all",
            "noncomm_positions_long_all",
            "m_money_positions_long_all",
            "managed_money_positions_long_all",
            "money_manager_positions_long_all",
        ],
    )
    noncommercial_short = _numeric_series(
        cleaned,
        [
            "noncommercial_positions_short_all",
            "noncomm_positions_short_all",
            "m_money_positions_short_all",
            "managed_money_positions_short_all",
            "money_manager_positions_short_all",
        ],
    )
    commercial_long = _numeric_series(
        cleaned,
        [
            "commercial_positions_long_all",
            "comm_positions_long_all",
            "prod_merc_positions_long_all",
            "producer_merchant_positions_long_all",
            "producer_merchant_processor_user_positions_long_all",
        ],
    )
    commercial_short = _numeric_series(
        cleaned,
        [
            "commercial_positions_short_all",
            "comm_positions_short_all",
            "prod_merc_positions_short_all",
            "producer_merchant_positions_short_all",
            "producer_merchant_processor_user_positions_short_all",
        ],
    )

    noncommercial_net = noncommercial_long - noncommercial_short
    commercial_net = commercial_long - commercial_short
    commercial_long_change = _numeric_series(
        cleaned,
        [
            "change_in_commercial_long_all",
            "change_in_comm_long_all",
            "change_in_commercial_positions_long_all",
            "change_in_prod_merc_long_all",
            "change_in_prod_merc_positions_long_all",
            "change_in_producer_merchant_positions_long_all",
        ],
    )
    commercial_short_change = _numeric_series(
        cleaned,
        [
            "change_in_commercial_short_all",
            "change_in_comm_short_all",
            "change_in_commercial_positions_short_all",
            "change_in_prod_merc_short_all",
            "change_in_prod_merc_positions_short_all",
            "change_in_producer_merchant_positions_short_all",
        ],
    )
    weekly_change = commercial_long_change - commercial_short_change

    rows = pd.DataFrame(
        {
            "report_date": report_date,
            "market_name": market_name,
            "exchange": exchange,
            "cftc_contract_market_code": _series_or_na(cleaned, ["cftc_contract_market_code", "cftc_market_code"]),
            "open_interest": _numeric_series(cleaned, ["open_interest_all", "open_interest"]),
            "noncommercial_long": noncommercial_long,
            "noncommercial_short": noncommercial_short,
            "commercial_long": commercial_long,
            "commercial_short": commercial_short,
            "commercial_net": commercial_net,
            "noncommercial_net": noncommercial_net,
            "weekly_change": weekly_change,
            "four_week_change": pd.NA,
            "source_report": source_report,
        }
    )
    rows["bias"] = rows["noncommercial_net"].apply(lambda value: _cot_bias(None if pd.isna(value) else int(value)))
    return rows



def _normalise_market_key(value: object) -> str:
    return re.sub(r"\s+", " ", str(value).strip().upper())


def _canonical_index_market(row: pd.Series) -> str | None:
    """Return dashboard market for approved CME equity-index rows only.

    This deliberately rejects broad/nearby CFTC rows such as ICE energy or
    California power/carbon markets. Code and/or exact CME contract name must
    match the approved index futures list.
    """
    from hptl.cot.contracts import CME_INDEX_MAPPINGS, CME_INDEX_NAME_TO_DASHBOARD

    code = _normalise_market_key(row.get("cftc_contract_market_code", row.get("cftc_market_code", "")))
    full_name = _normalise_market_key(
        row.get("market_and_exchange_names", row.get("market_name", row.get("commodity_name", "")))
    )
    exchange = _normalise_market_key(row.get("exchange", ""))

    if full_name in CME_INDEX_NAME_TO_DASHBOARD:
        return CME_INDEX_NAME_TO_DASHBOARD[full_name]

    # Some historical files split the exchange from the market name. Accept the
    # known CFTC codes only when the row is clearly a Chicago Mercantile contract.
    if code in CME_INDEX_MAPPINGS and (
        "CHICAGO MERCANTILE EXCHANGE" in full_name or "CHICAGO MERCANTILE EXCHANGE" in exchange
    ):
        return CME_INDEX_MAPPINGS[code].dashboard_name

    return None


def _financial_series(df: pd.DataFrame, candidates: list[str]) -> pd.Series:
    return _numeric_series(df, candidates)


def financial_history_to_dashboard_rows(df: pd.DataFrame, source_report: str = "Financial Futures Only Historical") -> pd.DataFrame:
    """Convert Traders in Financial Futures rows into the workbook schema.

    Financial COT reports use participant buckets (dealer, asset manager,
    leveraged money, etc.) rather than the classic commercial/noncommercial
    buckets. For this starter dashboard we map:
      - noncommercial_* = leveraged money
      - commercial_* = dealer + asset manager

    This preserves the existing workbook field names without adding scoring or
    changing the commodity pipeline.
    """
    cleaned = clean_columns(df)
    date_col = _first_existing_column(
        cleaned,
        ["report_date", "report_date_as_yyyy_mm_dd", "as_of_date_in_form_yyyy_mm_dd", "report_date_as_mm_dd_yyyy"],
    )
    report_date = pd.to_datetime(cleaned[date_col], errors="coerce") if date_col else pd.Series(pd.NaT, index=cleaned.index)
    market_source = _series_or_na(cleaned, ["market_and_exchange_names", "commodity_name", "market_name_clean", "market_name"])
    market_name, exchange = _market_exchange_from_name(market_source)

    dealer_long = _financial_series(cleaned, ["dealer_positions_long_all", "dealer_long_all"])
    dealer_short = _financial_series(cleaned, ["dealer_positions_short_all", "dealer_short_all"])
    asset_mgr_long = _financial_series(cleaned, ["asset_mgr_positions_long_all", "asset_manager_positions_long_all", "asset_mgr_long_all"])
    asset_mgr_short = _financial_series(cleaned, ["asset_mgr_positions_short_all", "asset_manager_positions_short_all", "asset_mgr_short_all"])

    noncommercial_long = _financial_series(cleaned, ["lev_money_positions_long_all", "leveraged_money_positions_long_all", "lev_money_long_all"])
    noncommercial_short = _financial_series(cleaned, ["lev_money_positions_short_all", "leveraged_money_positions_short_all", "lev_money_short_all"])
    commercial_long = dealer_long.fillna(0) + asset_mgr_long.fillna(0)
    commercial_short = dealer_short.fillna(0) + asset_mgr_short.fillna(0)

    weekly_change = _financial_series(
        cleaned,
        ["change_in_lev_money_long_all", "change_in_lev_money_positions_long_all", "change_in_leveraged_money_long_all"],
    ) - _financial_series(
        cleaned,
        ["change_in_lev_money_short_all", "change_in_lev_money_positions_short_all", "change_in_leveraged_money_short_all"],
    )

    rows = pd.DataFrame(
        {
            "report_date": report_date,
            "market_name": market_name,
            "exchange": exchange,
            "cftc_contract_market_code": _series_or_na(cleaned, ["cftc_contract_market_code", "cftc_market_code"]),
            "open_interest": _numeric_series(cleaned, ["open_interest_all", "open_interest"]),
            "noncommercial_long": noncommercial_long,
            "noncommercial_short": noncommercial_short,
            "commercial_long": commercial_long,
            "commercial_short": commercial_short,
            "commercial_net": commercial_long - commercial_short,
            "noncommercial_net": noncommercial_long - noncommercial_short,
            "weekly_change": weekly_change,
            "four_week_change": pd.NA,
            "source_report": source_report,
        }
    )
    rows["bias"] = rows["noncommercial_net"].apply(lambda value: _cot_bias(None if pd.isna(value) else int(value)))
    return rows


def filter_cme_index_history(df: pd.DataFrame) -> pd.DataFrame:
    """Filter annual historical rows for only approved CME NASDAQ/S&P contracts."""
    from hptl.cot.contracts import CME_INDEX_MAPPINGS

    cleaned = clean_columns(df)
    if cleaned.empty:
        return pd.DataFrame()

    candidate_rows = []
    for idx, row in cleaned.iterrows():
        canonical = _canonical_index_market(row)
        if canonical is not None:
            candidate_rows.append((idx, canonical))

    if not candidate_rows:
        return pd.DataFrame()

    index_lookup = {idx: canonical for idx, canonical in candidate_rows}
    filtered = cleaned.loc[list(index_lookup)].copy()

    # Prefer the Financial Futures converter when those fields exist; fall back
    # to classic legacy conversion for tests or alternate official CFTC files.
    financial_cols = {"lev_money_positions_long_all", "dealer_positions_long_all", "asset_mgr_positions_long_all"}
    if financial_cols.intersection(set(filtered.columns)):
        dashboard_rows = financial_history_to_dashboard_rows(filtered)
    else:
        dashboard_rows = cot_history_to_dashboard_rows(filtered, source_report="CME Futures Only Historical")

    dashboard_rows["dashboard_market"] = [index_lookup[idx] for idx in filtered.index]
    dashboard_rows["market_name"] = dashboard_rows["dashboard_market"]
    dashboard_rows["asset_class"] = "Equity Index"

    code_to_exchange = {code: mapping.exchange for code, mapping in CME_INDEX_MAPPINGS.items()}
    dashboard_rows["exchange"] = dashboard_rows["cftc_contract_market_code"].astype(str).str.strip().map(code_to_exchange).fillna(
        "CHICAGO MERCANTILE EXCHANGE"
    )
    return dashboard_rows.sort_values(["market_name", "report_date"]).reset_index(drop=True)



def _canonical_good_workbook_market(row: pd.Series) -> str | None:
    """Map only the required 12 workbook markets to canonical names.

    Important detail: ``cot_history_to_dashboard_rows`` splits CFTC values like
    ``COFFEE C - ICE FUTURES U.S.`` into ``market_name=COFFEE C`` and
    ``exchange=ICE FUTURES U.S.``. Earlier versions only checked the full CFTC
    name, which is why Copper, Crude Oil, Natural Gas, Coffee and similar
    markets disappeared even though their numbers existed upstream.

    This function therefore checks, in order:
      1. explicit dashboard/index mappings,
      2. full CFTC market + exchange strings,
      3. exact short contract aliases,
      4. known CFTC commodity codes.

    It still rejects AECO, CAISO, carbon, electricity, ISO hubs and random
    energy contracts because only this narrow allow-list is accepted.
    """
    from hptl.cot.contracts import (
        ALLOWED_GOOD_WORKBOOK_MARKETS,
        COMMODITY_CODE_TO_DASHBOARD,
        COMMODITY_NAME_TO_DASHBOARD,
        COMMODITY_SHORT_NAME_TO_DASHBOARD,
    )

    market = row.get("market_name")
    exchange = row.get("exchange")
    full_from_split = None
    if market is not None and not pd.isna(market) and exchange is not None and not pd.isna(exchange):
        full_from_split = f"{market} - {exchange}"

    candidates = [
        row.get("dashboard_market"),
        full_from_split,
        row.get("market_and_exchange_names"),
        row.get("commodity_name"),
        row.get("market_name_clean"),
        row.get("market_name"),
    ]

    for candidate in candidates:
        key = _normalise_market_key(candidate)
        if key in COMMODITY_NAME_TO_DASHBOARD:
            return COMMODITY_NAME_TO_DASHBOARD[key]
        if key in COMMODITY_SHORT_NAME_TO_DASHBOARD:
            return COMMODITY_SHORT_NAME_TO_DASHBOARD[key]
        if key == "CRUDE OIL (WTI)":
            return "CRUDE OIL"
        if key in ALLOWED_GOOD_WORKBOOK_MARKETS:
            return key

    code = _normalise_market_key(row.get("cftc_contract_market_code", row.get("cftc_market_code", "")))
    if code in COMMODITY_CODE_TO_DASHBOARD:
        return COMMODITY_CODE_TO_DASHBOARD[code]

    return _canonical_index_market(row)


def filter_good_workbook_markets(df: pd.DataFrame) -> pd.DataFrame:
    """Return only markets that existed in the good workbook plus NASDAQ/S&P.

    The returned frame keeps all existing numeric columns and rewrites
    ``market_name`` to the canonical dashboard label used by the template.
    """
    if df.empty:
        return df.copy()

    cleaned = df.copy()
    canonical = cleaned.apply(_canonical_good_workbook_market, axis=1)
    filtered = cleaned[canonical.notna()].copy()
    if filtered.empty:
        return filtered
    filtered["market_name"] = canonical[canonical.notna()].values
    if "dashboard_market" not in filtered.columns:
        filtered["dashboard_market"] = filtered["market_name"]
    else:
        filtered["dashboard_market"] = filtered["market_name"]
    return filtered.reset_index(drop=True)


def align_index_history_to_date_range(index_rows: pd.DataFrame, template_rows: pd.DataFrame) -> pd.DataFrame:
    """Trim NASDAQ/S&P history to the same date range as the good workbook rows."""
    if index_rows.empty or template_rows.empty or "report_date" not in index_rows.columns or "report_date" not in template_rows.columns:
        return index_rows.copy()
    result = index_rows.copy()
    result["report_date"] = pd.to_datetime(result["report_date"], errors="coerce")
    template_dates = pd.to_datetime(template_rows["report_date"], errors="coerce").dropna()
    if template_dates.empty:
        return result
    return result[result["report_date"].between(template_dates.min(), template_dates.max())].reset_index(drop=True)


def deduplicate_market_weeks(df: pd.DataFrame) -> pd.DataFrame:
    """Keep one populated row per market/week without changing workbook layout."""
    if df.empty or not {"market_name", "report_date"}.issubset(df.columns):
        return df.copy()
    result = df.copy()
    result["report_date"] = pd.to_datetime(result["report_date"], errors="coerce")
    # Prefer rows with the most numeric values populated. This prevents blank
    # duplicate blocks from winning when CFTC files include multiple aliases.
    value_cols = [
        "open_interest", "commercial_long", "commercial_short", "commercial_net",
        "noncommercial_long", "noncommercial_short", "noncommercial_net",
    ]
    existing = [col for col in value_cols if col in result.columns]
    result["_populated_values"] = result[existing].notna().sum(axis=1) if existing else 0
    result = result.sort_values(["market_name", "report_date", "_populated_values"], ascending=[True, True, False])
    result = result.drop_duplicates(["market_name", "report_date"], keep="first")
    return result.drop(columns=["_populated_values"]).reset_index(drop=True)

def latest_row_per_market(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty or "market_name" not in df.columns or "report_date" not in df.columns:
        return df.copy()
    ordered = df.sort_values(["market_name", "report_date"])
    return ordered.groupby("market_name", as_index=False, dropna=False).tail(1).reset_index(drop=True)
