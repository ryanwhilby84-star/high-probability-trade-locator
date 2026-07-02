"""Deep G10 macro history loaders for FX valuation (fx_carry_real_yield_v3 foundation).

Uses labelled first-party caches where available; FRED macro_cache for USD legs;
BoE GLC nominal archive for GBP 2Y/10Y; RBA F2/F1 for AUD; SNB rendoblid for CHF.
"""
from __future__ import annotations

import csv
import io
import json
import zipfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd

from hptl.fx.rate_adapter_base import CACHE_DIR, fetch_bytes, fetch_text, offline_mode, to_float

FRED_OBS_START = "2016-01-01"
MIN_FOUNDATION_OBS = 52
MIN_PANEL_POINTS = 20

# BoE archive (full GLC nominal daily history by year-range workbooks).
BOE_GLC_ARCHIVE_URL = (
    "https://www.bankofengland.co.uk/-/media/boe/files/statistics/yield-curves/glcnominalddata.zip"
)
BOE_GLC_ARCHIVE_CACHE = "boe_glc_nominal_archive"
BOE_GLC_SOURCE = "Bank of England GLC nominal spot curve (glcnominalddata.zip archive)"
BOE_GLC_SHEET = "4. nominal spot curve"

JGB_URL = "https://www.mof.go.jp/english/policy/jgbs/reference/interest_rate/jgbcme.csv"
JGB_SOURCE = "Japan MoF JGB constant-maturity yields (jgbcme.csv)"

BIS_POLICY_SOURCE = "BIS WS_CBPOL (cached CSV)"
BIS_POLICY_HISTORY_SOURCE = "BIS WS_CBPOL deep history (bis_cbpol_*_history.txt)"
BIS_POLICY_HISTORY_URL = (
    "https://stats.bis.org/api/v1/data/WS_CBPOL/D.{ref}/all?startPeriod=2016-01-01&format=csv"
)
BIS_HISTORY_REF_AREA: dict[str, str] = {
    "jp": "JP",
    "nz": "NZ",
    "ch": "CH",
}
FRED_JPY_Y2_FALLBACK_ID = "IR3TIB01JPM156N"
FRED_NZD_Y2_FALLBACK_ID = "IR3TIB01NZM156N"
FRED_CHF_Y2_FALLBACK_ID = "IR3TIB01CHM156N"
FRED_CHF_Y2_FALLBACK_SOURCE = (
    f"FRED OECD {FRED_CHF_Y2_FALLBACK_ID} (3M interbank; extends SNB rendoblid when frozen)"
)
FRED_JPY_Y2_FALLBACK_SOURCE = (
    f"FRED OECD {FRED_JPY_Y2_FALLBACK_ID} (short-term; MoF JGB cache shallow fallback)"
)
FRED_NZD_Y2_FALLBACK_SOURCE = (
    f"FRED OECD {FRED_NZD_Y2_FALLBACK_ID} (short-term; no native NZD 2Y daily loader)"
)
FRED_GBP_POLICY_ID = "IR3TIB01GBM156N"
FRED_GBP_POLICY_SOURCE = f"FRED {FRED_GBP_POLICY_ID} (3M interbank; monthly forward-filled)"
ECB_SOURCE = "ECB data-api csvdata cache"
ECB_HISTORY_SOURCE = "ECB data-api csvdata history cache (startPeriod=2016-01-01)"
_ECB_BASE = "https://data-api.ecb.europa.eu/service/data"
ECB_HISTORY_URLS: dict[str, str] = {
    "eur_dfr_history": f"{_ECB_BASE}/FM/B.U2.EUR.4F.KR.DFR.LEV?format=csvdata&startPeriod=2016-01-01",
    "eur_2y_history": f"{_ECB_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_2Y?format=csvdata&startPeriod=2016-01-01",
    "eur_10y_history": f"{_ECB_BASE}/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?format=csvdata&startPeriod=2016-01-01",
}
RBA_F2_SOURCE = "RBA statistical table F2 (AGB 2Y/10Y, aud_f2.bin cache)"
RBA_F1_SOURCE = "RBA statistical table F1 (cash rate, aud_f1.bin cache)"
SNB_RENDOBLID_SOURCE = "SNB rendoblid cube (chf_rendoblid.bin cache)"
CAD_VALET_SOURCE = "Bank of Canada Valet (cad_valet.txt cache)"
CAD_VALET_DEEP_URL = (
    "https://www.bankofcanada.ca/valet/observations/"
    "BD.CDN.2YR.DQ.YLD,BD.CDN.10YR.DQ.YLD,V39079/json?start_date=2016-01-01"
)
FRED_USD_SOURCE = {
    "policy": "FRED DFF (effective federal funds rate, macro_cache)",
    "y2": "FRED DGS2 (US Treasury 2Y constant maturity, macro_cache)",
    "y10": "FRED DGS10 (US Treasury 10Y constant maturity, macro_cache)",
}


def _parse_iso_date(value: Any) -> str | None:
    if not value:
        return None
    s = str(value).strip()
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%d %b %Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s[:10] if fmt != "%d %b %Y" else s[:11], fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _read_cache_text(name: str) -> str | None:
    path = CACHE_DIR / name
    if not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return None


def _read_cache_bytes(name: str) -> bytes | None:
    path = CACHE_DIR / f"{name}.bin" if not name.endswith(".bin") else CACHE_DIR / name
    if not path.exists():
        path = CACHE_DIR / name
    if not path.exists():
        return None
    try:
        return path.read_bytes()
    except OSError:
        return None


def load_fred_daily_map(series_id: str, observation_start: str = FRED_OBS_START) -> dict[str, float]:
    """Daily {iso_date: value} from resilient FRED macro_cache."""
    from hptl.macro import fred_client

    starts = [observation_start, "2018-01-01", "2019-01-01", "2005-01-01", "2000-01-01"]
    seen: set[str] = set()
    out: dict[str, float] = {}
    for start in starts:
        if start in seen:
            continue
        seen.add(start)
        try:
            df = fred_client.get_series_df(series_id, start)
        except Exception:
            continue
        for _, row in df.iterrows():
            d = pd.Timestamp(row["date"]).strftime("%Y-%m-%d")
            v = to_float(row["value"])
            if v is not None:
                out[d] = float(v)
        if len(out) >= MIN_FOUNDATION_OBS:
            break
    return out


def _merge_series(*maps: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for m in maps:
        out.update(m)
    return out


def load_usd_fred_history() -> dict[str, dict[str, Any]]:
    policy = load_fred_daily_map("DFF")
    y2 = load_fred_daily_map("DGS2")
    y10 = load_fred_daily_map("DGS10")
    return {
        "policy": policy,
        "y2": y2,
        "y10": y10,
        "sources": dict(FRED_USD_SOURCE),
    }


def load_usd_treasury_history() -> dict[str, dict[str, float | None]]:
    """Legacy Treasury CSV (current year) — merged as fallback only."""
    raw = _read_cache_text("usd_treasury.txt")
    if not raw:
        return {}
    reader = csv.DictReader(io.StringIO(raw))
    out: dict[str, dict[str, float | None]] = {}
    for row in reader:
        iso = _parse_iso_date(row.get("Date"))
        if not iso:
            continue
        out[iso] = {"y2": to_float(row.get("2 Yr")), "y10": to_float(row.get("10 Yr"))}
    return out


def load_usd_combined_history() -> dict[str, Any]:
    """USD macro history — FRED primary, Treasury CSV supplemental."""
    fred = load_usd_fred_history()
    treas = load_usd_treasury_history()
    t_y2 = {d: v["y2"] for d, v in treas.items() if v.get("y2") is not None}
    t_y10 = {d: v["y10"] for d, v in treas.items() if v.get("y10") is not None}
    return {
        "policy": fred["policy"],
        "y2": _merge_series(fred["y2"], {k: float(v) for k, v in t_y2.items()}),
        "y10": _merge_series(fred["y10"], {k: float(v) for k, v in t_y10.items()}),
        "sources": fred["sources"],
    }


def _parse_jgb_table(raw: str) -> dict[str, dict[str, float | None]]:
    """Parse MoF JGB CSV — supports title row + Date header variants."""
    reader = csv.reader(io.StringIO(raw))
    header: list[str] | None = None
    out: dict[str, dict[str, float | None]] = {}
    for row in reader:
        if not row:
            continue
        first = (row[0] or "").strip()
        if header is None:
            if first == "Date" or (len(row) > 2 and "2Y" in row):
                header = [c.strip() for c in row]
            continue
        if not first or not first[0].isdigit():
            continue
        parts = first.replace("-", "/").split("/")
        if len(parts) != 3:
            continue
        iso = f"{int(parts[0]):04d}-{int(parts[1]):02d}-{int(parts[2]):02d}"
        idx = {name: i for i, name in enumerate(header or [])}

        def col(label: str) -> float | None:
            i = idx.get(label)
            return to_float(row[i]) if i is not None and i < len(row) else None

        out[iso] = {"y2": col("2Y"), "y10": col("10Y")}
    return out


def ensure_jpy_jgb_cache() -> None:
    if offline_mode():
        return
    try:
        fetch_text(JGB_URL, cache_key="jpy_jgb")
    except Exception:
        pass


def load_jpy_jgb_history() -> dict[str, dict[str, float | None]]:
    ensure_jpy_jgb_cache()
    raw = _read_cache_text("jpy_jgb.txt")
    if not raw:
        return {}
    return _parse_jgb_table(raw)


def load_jpy_y2_history() -> tuple[dict[str, float], str]:
    """JPY 2Y — MoF JGB primary; FRED OECD monthly when MoF cache is shallow."""
    jgb = load_jpy_jgb_history()
    y2 = {d: float(v["y2"]) for d, v in jgb.items() if v.get("y2") is not None}
    if len(y2) >= MIN_FOUNDATION_OBS:
        return y2, JGB_SOURCE
    fallback = load_fred_daily_map(FRED_JPY_Y2_FALLBACK_ID)
    if fallback:
        merged = _merge_series(fallback, y2)
        if len(merged) >= MIN_FOUNDATION_OBS:
            return merged, FRED_JPY_Y2_FALLBACK_SOURCE if len(y2) < MIN_FOUNDATION_OBS else JGB_SOURCE
    return y2, JGB_SOURCE if y2 else FRED_JPY_Y2_FALLBACK_SOURCE


def load_jpy_y10_history() -> tuple[dict[str, float], str]:
    jgb = load_jpy_jgb_history()
    y10 = {d: float(v["y10"]) for d, v in jgb.items() if v.get("y10") is not None}
    if len(y10) >= MIN_FOUNDATION_OBS:
        return y10, JGB_SOURCE
    fallback = load_fred_daily_map("IRLTLT01JPM156N")
    if fallback:
        merged = _merge_series(fallback, y10)
        if len(merged) >= MIN_FOUNDATION_OBS:
            return merged, "FRED OECD IRLTLT01JPM156N (10Y; MoF JGB cache shallow fallback)"
    return y10, JGB_SOURCE if y10 else JGB_SOURCE


def load_chf_y2_history() -> tuple[dict[str, float], str]:
    """CHF 2Y — SNB rendoblid daily history only (no OECD monthly extension)."""
    chf = load_chf_rendoblid_history()
    y2 = dict(chf.get("y2") or {})
    return y2, SNB_RENDOBLID_SOURCE if y2 else SNB_RENDOBLID_SOURCE


def load_chf_y10_history() -> tuple[dict[str, float], str]:
    chf = load_chf_rendoblid_history()
    y10 = dict(chf.get("y10") or {})
    return y10, SNB_RENDOBLID_SOURCE if y10 else SNB_RENDOBLID_SOURCE


def load_nzd_y2_history() -> tuple[dict[str, float], str]:
    """NZD 2Y — RBNZ B2 daily history when manual/live file available."""
    from hptl.fx.rbnz_adapter import MANUAL_PATH, _b2_bytes, _parse_b2_series

    try:
        content, detail = _b2_bytes()
        series = _parse_b2_series(content)
        y2 = series.get("y2") or {}
        if y2:
            return y2, f"RBNZ B2 2Y ({detail})"
    except Exception:
        pass
    if MANUAL_PATH.exists():
        try:
            series = _parse_b2_series(MANUAL_PATH.read_bytes())
            y2 = series.get("y2") or {}
            if y2:
                return y2, f"RBNZ B2 2Y (manual file {MANUAL_PATH.name})"
        except Exception:
            pass
    return {}, "RBNZ B2 daily source missing"


def load_nzd_y10_history() -> tuple[dict[str, float], str]:
    from hptl.fx.rbnz_adapter import MANUAL_PATH, _b2_bytes, _parse_b2_series

    try:
        content, detail = _b2_bytes()
        series = _parse_b2_series(content)
        y10 = series.get("y10") or {}
        if y10:
            return y10, f"RBNZ B2 10Y ({detail})"
    except Exception:
        pass
    if MANUAL_PATH.exists():
        try:
            series = _parse_b2_series(MANUAL_PATH.read_bytes())
            y10 = series.get("y10") or {}
            if y10:
                return y10, f"RBNZ B2 10Y (manual file {MANUAL_PATH.name})"
        except Exception:
            pass
    return {}, "RBNZ B2 daily source missing"


def _parse_boe_spot_workbook(content: bytes) -> tuple[dict[str, float], dict[str, float]]:
    """Extract 2Y and 10Y spot yields from one BoE GLC nominal workbook."""
    y2: dict[str, float] = {}
    y10: dict[str, float] = {}
    xl = pd.ExcelFile(io.BytesIO(content))
    sheet = BOE_GLC_SHEET if BOE_GLC_SHEET in xl.sheet_names else xl.sheet_names[-1]
    df = xl.parse(sheet, header=None)
    hdr_idx = None
    for i in range(min(20, len(df))):
        if str(df.iloc[i, 0]).strip().lower().startswith("years"):
            hdr_idx = i
            break
    if hdr_idx is None:
        return y2, y10
    maturities = df.iloc[hdr_idx].tolist()

    def col_for(target: float) -> int | None:
        for j, v in enumerate(maturities):
            try:
                if abs(float(v) - target) < 1e-6:
                    return j
            except (TypeError, ValueError):
                continue
        return None

    c2, c10 = col_for(2.0), col_for(10.0)
    if c2 is None or c10 is None:
        return y2, y10
    data = df.iloc[hdr_idx + 1 :].copy()
    data["_date"] = pd.to_datetime(data.iloc[:, 0], errors="coerce")
    data = data.dropna(subset=["_date"])
    for _, row in data.iterrows():
        iso = row["_date"].date().isoformat()
        v2 = to_float(row.iloc[c2])
        v10 = to_float(row.iloc[c10])
        if v2 is not None:
            y2[iso] = float(v2)
        if v10 is not None:
            y10[iso] = float(v10)
    return y2, y10


def ensure_boe_glc_archive() -> None:
    if offline_mode():
        return
    try:
        fetch_bytes(BOE_GLC_ARCHIVE_URL, cache_key=BOE_GLC_ARCHIVE_CACHE)
    except Exception:
        pass


_GBP_YIELD_CACHE = CACHE_DIR / "gbp_boe_glc_parsed.json"
_GBP_YIELD_CACHE_MTIME: float | None = None
_GBP_YIELD_MEM: dict[str, dict[str, float]] | None = None


def load_gbp_boe_yield_history() -> dict[str, dict[str, float]]:
    """GBP 2Y/10Y from BoE GLC nominal daily archive zip (cached parse)."""
    global _GBP_YIELD_MEM, _GBP_YIELD_CACHE_MTIME
    archive = CACHE_DIR / f"{BOE_GLC_ARCHIVE_CACHE}.bin"
    if not archive.exists():
        ensure_boe_glc_archive()
    archive_mtime = archive.stat().st_mtime if archive.exists() else 0.0
    if (
        _GBP_YIELD_MEM is not None
        and _GBP_YIELD_CACHE_MTIME == archive_mtime
        and _GBP_YIELD_CACHE.exists()
    ):
        return _GBP_YIELD_MEM
    if _GBP_YIELD_CACHE.exists() and _GBP_YIELD_CACHE.stat().st_mtime >= archive_mtime:
        try:
            doc = json.loads(_GBP_YIELD_CACHE.read_text(encoding="utf-8"))
            _GBP_YIELD_MEM = {
                "y2": {k: float(v) for k, v in (doc.get("y2") or {}).items()},
                "y10": {k: float(v) for k, v in (doc.get("y10") or {}).items()},
            }
            _GBP_YIELD_CACHE_MTIME = archive_mtime
            return _GBP_YIELD_MEM
        except (OSError, json.JSONDecodeError, TypeError, ValueError):
            pass

    blob = _read_cache_bytes(BOE_GLC_ARCHIVE_CACHE)
    if not blob:
        return {"y2": {}, "y10": {}}
    y2: dict[str, float] = {}
    y10: dict[str, float] = {}
    z = zipfile.ZipFile(io.BytesIO(blob))
    for name in z.namelist():
        if not name.lower().endswith(".xlsx") or "nominal daily" not in name.lower():
            continue
        part_y2, part_y10 = _parse_boe_spot_workbook(z.read(name))
        y2.update(part_y2)
        y10.update(part_y10)
    _GBP_YIELD_MEM = {"y2": y2, "y10": y10}
    _GBP_YIELD_CACHE_MTIME = archive_mtime
    try:
        _GBP_YIELD_CACHE.write_text(json.dumps({"y2": y2, "y10": y10}), encoding="utf-8")
    except OSError:
        pass
    return _GBP_YIELD_MEM


def _forward_fill_daily(monthly: dict[str, float]) -> dict[str, float]:
    if not monthly:
        return {}
    dates = sorted(monthly.keys())
    start = date.fromisoformat(dates[0][:10])
    end = date.fromisoformat(dates[-1][:10])
    out: dict[str, float] = {}
    cur = monthly[dates[0]]
    idx = 0
    d = start
    while d <= end:
        iso = d.isoformat()
        while idx + 1 < len(dates) and dates[idx + 1] <= iso:
            idx += 1
            cur = monthly[dates[idx]]
        out[iso] = float(cur)
        d += timedelta(days=1)
    return out


def _merge_policy_series(*maps: dict[str, float]) -> dict[str, float]:
    out: dict[str, float] = {}
    for mp in maps:
        for k, v in mp.items():
            out[k] = v
    return out


def load_gbp_bank_rate_history() -> dict[str, float]:
    """BoE Bank Rate — merge FRED monthly proxy, IADB deep cache, and recent BoE daily."""
    fred_monthly = load_fred_daily_map(FRED_GBP_POLICY_ID)
    fred_daily = _forward_fill_daily(fred_monthly) if fred_monthly else {}
    shallow = _parse_gbp_bank_rate_csv(_read_cache_text("gbp_bank_rate.txt") or "")
    deep = _parse_gbp_bank_rate_csv(_read_cache_text("gbp_bank_rate_history.txt") or "")
    merged = _merge_policy_series(fred_daily, deep, shallow)
    if len(merged) >= MIN_FOUNDATION_OBS:
        return merged
    if not offline_mode():
        try:
            fetched = _fetch_gbp_bank_rate_history()
            if fetched:
                path = CACHE_DIR / "gbp_bank_rate_history.txt"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(_bank_rate_csv_text(fetched), encoding="utf-8")
                merged = _merge_policy_series(fred_daily, fetched, shallow)
                if len(merged) >= MIN_FOUNDATION_OBS:
                    return merged
        except Exception:
            pass
    return merged


def _parse_gbp_bank_rate_csv(raw: str) -> dict[str, float]:
    reader = csv.DictReader(io.StringIO(raw))
    out: dict[str, float] = {}
    for row in reader:
        iso = _parse_iso_date(row.get("DATE"))
        val = to_float(row.get("IUDBEDR"))
        if iso and val is not None:
            out[iso] = val
    return out


def _bank_rate_csv_text(series: dict[str, float]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(["DATE", "IUDBEDR"])
    for d in sorted(series.keys()):
        writer.writerow([d, series[d]])
    return buf.getvalue()


def _fetch_gbp_bank_rate_history() -> dict[str, float]:
    """Fetch BoE Bank Rate from IADB (2010-present)."""
    start = date(2010, 1, 1)
    end = date.today()
    url = (
        "https://www.bankofengland.co.uk/boeapps/iadb/fromshowcolumns.asp"
        "?csv.x=yes"
        f"&Datefrom={start.strftime('%d/%b/%Y')}"
        f"&Dateto={end.strftime('%d/%b/%Y')}"
        "&SeriesCodes=IUDBEDR&CSVF=TN&UsingCodes=Y&VPD=Y&VFD=N"
    )
    raw = fetch_text(url, cache_key="gbp_bank_rate_history")
    return _parse_gbp_bank_rate_csv(raw)


def _parse_rba_workbook(content: bytes, series_id: str) -> dict[str, float]:
    xl = pd.ExcelFile(io.BytesIO(content))
    df = xl.parse(xl.sheet_names[0], header=None)
    id_row = None
    for i in range(min(20, len(df))):
        if str(df.iloc[i, 0]).strip() == "Series ID":
            id_row = i
            break
    if id_row is None:
        return {}
    ids = [str(x).strip() for x in df.iloc[id_row].tolist()]
    if series_id not in ids:
        return {}
    col = ids.index(series_id)
    data = df.iloc[id_row + 1 :, [0, col]].copy()
    data.columns = ["date", "value"]
    data["value"] = pd.to_numeric(data["value"], errors="coerce")
    data["date"] = pd.to_datetime(data["date"], errors="coerce")
    data = data.dropna(subset=["date", "value"])
    return {r["date"].date().isoformat(): float(r["value"]) for _, r in data.iterrows()}


def load_aud_rba_history() -> dict[str, dict[str, float]]:
    f1 = _read_cache_bytes("aud_f1")
    f2 = _read_cache_bytes("aud_f2")
    policy = _parse_rba_workbook(f1, "FIRMMCRTD") if f1 else {}
    y2 = _parse_rba_workbook(f2, "FCMYGBAG2D") if f2 else {}
    y10 = _parse_rba_workbook(f2, "FCMYGBAG10D") if f2 else {}
    return {"policy": policy, "y2": y2, "y10": y10}


def load_chf_rendoblid_history() -> dict[str, dict[str, float]]:
    blob = _read_cache_bytes("chf_rendoblid")
    if not blob:
        return {"y2": {}, "y10": {}}
    text = blob.decode("utf-8-sig", errors="replace")
    y2: dict[str, float] = {}
    y10: dict[str, float] = {}
    for line in text.splitlines():
        parts = line.split(";")
        if len(parts) != 3:
            continue
        d = parts[0].strip('"').strip()
        mat = parts[1].strip('"').strip()
        val = to_float(parts[2].strip('"').strip())
        if val is None or not d[:4].isdigit():
            continue
        if mat == "2J":
            y2[d] = val
        elif mat in {"10J", "10J0"}:
            y10[d] = val
    return {"y2": y2, "y10": y10}


def ensure_cad_valet_deep_cache() -> None:
    if offline_mode():
        return
    try:
        fetch_text(CAD_VALET_DEEP_URL, cache_key="cad_valet")
    except Exception:
        pass


def load_cad_valet_history() -> dict[str, dict[str, float | None]]:
    ensure_cad_valet_deep_cache()
    raw = _read_cache_text("cad_valet.txt")
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    out: dict[str, dict[str, float | None]] = {}
    for obs in data.get("observations") or []:
        iso = _parse_iso_date(obs.get("d"))
        if not iso:
            continue
        out[iso] = {
            "policy": to_float((obs.get("V39079") or {}).get("v")),
            "y2": to_float((obs.get("BD.CDN.2YR.DQ.YLD") or {}).get("v")),
            "y10": to_float((obs.get("BD.CDN.10YR.DQ.YLD") or {}).get("v")),
        }
    return out


def _parse_bis_policy_csv(raw: str) -> dict[str, float]:
    reader = csv.DictReader(io.StringIO(raw))
    out: dict[str, float] = {}
    for row in reader:
        iso = _parse_iso_date(row.get("TIME_PERIOD"))
        val = to_float(row.get("OBS_VALUE"))
        if iso and val is not None:
            out[iso] = val
    return out


def ensure_bis_policy_history_cache(ref: str) -> None:
    """Fetch deep BIS policy history into bis_cbpol_{ref}_history.txt (online only)."""
    if offline_mode():
        return
    area = BIS_HISTORY_REF_AREA.get(ref.lower())
    if not area:
        return
    try:
        fetch_text(
            BIS_POLICY_HISTORY_URL.format(ref=area),
            cache_key=f"bis_cbpol_{ref.lower()}_history",
        )
    except Exception:
        pass


def load_bis_policy_history(ref: str) -> dict[str, float]:
    """BIS policy history — prefer deep ``*_history.txt`` over shallow live adapter cache."""
    ref_l = ref.lower()
    candidates: list[dict[str, float]] = []
    for name in (f"bis_cbpol_{ref_l}_history.txt", f"bis_cbpol_{ref_l}.txt"):
        raw = _read_cache_text(name)
        if raw:
            parsed = _parse_bis_policy_csv(raw)
            if parsed:
                candidates.append(parsed)
    if not candidates:
        return {}
    return max(candidates, key=len)


def load_ecb_yield_history(cache_key: str) -> dict[str, float]:
    raw = _read_cache_text(f"{cache_key}.txt")
    if not raw:
        return {}
    reader = csv.DictReader(io.StringIO(raw))
    out: dict[str, float] = {}
    for row in reader:
        iso = _parse_iso_date(row.get("TIME_PERIOD"))
        val = to_float(row.get("OBS_VALUE"))
        if iso and val is not None:
            out[iso] = val
    return out


def ensure_ecb_yield_history_caches() -> None:
    """Deepen ECB yield caches for regression (online only)."""
    if offline_mode():
        return
    for key, url in ECB_HISTORY_URLS.items():
        try:
            fetch_text(url, cache_key=key)
        except Exception:
            pass


def load_eur_y2_history() -> tuple[dict[str, float], str]:
    """EUR 2Y — deep history cache + live point (never single-row live overwrite)."""
    ensure_ecb_yield_history_caches()
    hist = load_ecb_yield_history("eur_2y_history")
    live = load_ecb_yield_history("eur_2y_live")
    if not hist:
        hist = load_ecb_yield_history("eur_2y")
    merged = _merge_series(hist, live)
    src = ECB_HISTORY_SOURCE if len(hist) >= MIN_FOUNDATION_OBS else f"{ECB_SOURCE} eur_2y_live"
    return merged, src


def load_eur_y10_history() -> tuple[dict[str, float], str]:
    ensure_ecb_yield_history_caches()
    hist = load_ecb_yield_history("eur_10y_history")
    live = load_ecb_yield_history("eur_10y_live")
    if not hist:
        hist = load_ecb_yield_history("eur_10y")
    merged = _merge_series(hist, live)
    src = ECB_HISTORY_SOURCE if len(hist) >= MIN_FOUNDATION_OBS else f"{ECB_SOURCE} eur_10y_live"
    return merged, src


def load_eur_policy_history() -> tuple[dict[str, float], str]:
    ensure_ecb_yield_history_caches()
    hist = load_ecb_yield_history("eur_dfr_history")
    live = load_ecb_yield_history("eur_dfr_live")
    if not hist:
        hist = load_ecb_yield_history("eur_dfr")
    merged = _merge_series(hist, live)
    return merged, f"{ECB_SOURCE} eur_dfr_history + eur_dfr_live"


def _series_meta(
    series: dict[str, float],
    *,
    source: str,
    update_frequency: str,
    min_obs: int = MIN_FOUNDATION_OBS,
) -> dict[str, Any]:
    dates = sorted(series.keys())
    count = len(dates)
    status = "PASS" if count >= min_obs else "FAIL"
    return {
        "observation_count": count,
        "earliest_date": dates[0] if dates else None,
        "latest_date": dates[-1] if dates else None,
        "source": source,
        "update_frequency": update_frequency,
        "audit_status": status,
    }


def audit_g10_currency_legs(reference_end: date | None = None) -> list[dict[str, Any]]:
    """G10 macro leg audit table for foundation report."""
    reference_end = reference_end or date.today()
    histories = currency_histories()
    from hptl.fx.currency_rates import get_currency_rate

    rows: list[dict[str, Any]] = []
    for ccy in ("USD", "EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF"):
        h = histories.get(ccy) or {}
        rec = get_currency_rate(ccy)
        real_ok = rec.y2 is not None and rec.cpi_yoy is not None
        pol = h.get("policy") or {}
        y2 = h.get("y2") or {}
        y10 = h.get("y10") or {}
        src = h.get("sources") or {}
        pol_meta = _series_meta(
            pol,
            source=src.get("policy") or CURRENCY_POLICY_SOURCE.get(ccy, "unknown"),
            update_frequency="step",
            min_obs=1,
        )
        y2_meta = _series_meta(
            y2,
            source=src.get("y2") or CURRENCY_Y2_SOURCE.get(ccy, "unknown"),
            update_frequency="daily",
        )
        y10_meta = _series_meta(
            y10,
            source=src.get("y10") or CURRENCY_Y10_SOURCE.get(ccy, "unknown"),
            update_frequency="daily",
        )
        cpi_meta = {
            "observation_count": 1 if rec.cpi_yoy is not None else 0,
            "earliest_date": rec.cpi_yoy_as_of,
            "latest_date": rec.cpi_yoy_as_of,
            "audit_status": "PASS" if rec.cpi_yoy is not None else "FAIL",
        }
        real_meta = {
            "observation_count": 1 if real_ok else 0,
            "earliest_date": rec.y2_as_of or rec.cpi_yoy_as_of,
            "latest_date": rec.y2_as_of or rec.cpi_yoy_as_of,
            "audit_status": "PASS" if real_ok else "FAIL",
        }
        earliest = min(
            [d for d in (pol_meta["earliest_date"], y2_meta["earliest_date"]) if d] or [None]
        )
        latest = max(
            [d for d in (y2_meta["latest_date"], pol_meta["latest_date"]) if d] or [None]
        )
        row_pass = all(
            m.get("audit_status") == "PASS"
            for m in (pol_meta, y2_meta, cpi_meta, real_meta)
        )
        rows.append(
            {
                "currency": ccy,
                "policy_obs": pol_meta["observation_count"],
                "yield_2y_obs": y2_meta["observation_count"],
                "yield_10y_obs": y10_meta["observation_count"],
                "cpi_obs": cpi_meta["observation_count"],
                "real_yield_obs": real_meta["observation_count"],
                "earliest": earliest,
                "latest": latest,
                "pass_fail": "PASS" if row_pass else "FAIL",
                "sources": src,
                "detail": {
                    "policy": pol_meta,
                    "yield_2y": y2_meta,
                    "yield_10y": y10_meta,
                    "cpi": cpi_meta,
                    "real_yield": real_meta,
                },
            }
        )
    return rows


CURRENCY_POLICY_SOURCE: dict[str, str] = {
    "USD": FRED_USD_SOURCE["policy"],
    "EUR": f"{ECB_SOURCE} eur_dfr",
    "GBP": "BoE IUDBEDR + FRED IR3TIB01GBM156N fallback (gbp_bank_rate*.txt)",
    "JPY": f"{BIS_POLICY_HISTORY_SOURCE} jp",
    "AUD": RBA_F1_SOURCE,
    "NZD": f"{BIS_POLICY_HISTORY_SOURCE} nz",
    "CAD": CAD_VALET_SOURCE,
    "CHF": f"{BIS_POLICY_SOURCE} ch",
}

CURRENCY_Y2_SOURCE: dict[str, str] = {
    "USD": FRED_USD_SOURCE["y2"],
    "EUR": f"{ECB_SOURCE} eur_2y",
    "GBP": BOE_GLC_SOURCE,
    "JPY": JGB_SOURCE,
    "AUD": RBA_F2_SOURCE,
    "NZD": FRED_NZD_Y2_FALLBACK_SOURCE,
    "CAD": CAD_VALET_SOURCE,
    "CHF": SNB_RENDOBLID_SOURCE,
}

CURRENCY_Y10_SOURCE: dict[str, str] = {
    "USD": FRED_USD_SOURCE["y10"],
    "EUR": f"{ECB_SOURCE} eur_10y",
    "GBP": BOE_GLC_SOURCE,
    "JPY": JGB_SOURCE,
    "AUD": RBA_F2_SOURCE,
    "NZD": "FRED OECD IRLTLT01NZM156N (long-term fallback)",
    "CAD": CAD_VALET_SOURCE,
    "CHF": SNB_RENDOBLID_SOURCE,
}


def currency_histories() -> dict[str, dict[str, Any]]:
    """Per-currency macro history maps for valuation regression alignment."""
    usd = load_usd_combined_history()
    gbp_y = load_gbp_boe_yield_history()
    aud = load_aud_rba_history()
    chf_y2, chf_y2_src = load_chf_y2_history()
    chf_y10, chf_y10_src = load_chf_y10_history()
    jpy_y2, jpy_y2_src = load_jpy_y2_history()
    jpy_y10, jpy_y10_src = load_jpy_y10_history()
    nzd_y2, nzd_y2_src = load_nzd_y2_history()
    nzd_y10, nzd_y10_src = load_nzd_y10_history()
    cad = load_cad_valet_history()
    eur_y2, eur_y2_src = load_eur_y2_history()
    eur_y10, eur_y10_src = load_eur_y10_history()
    eur_pol, eur_pol_src = load_eur_policy_history()

    return {
        "USD": {
            "policy": usd["policy"],
            "y2": usd["y2"],
            "y10": usd["y10"],
            "sources": usd["sources"],
        },
        "EUR": {
            "policy": eur_pol,
            "y2": eur_y2,
            "y10": eur_y10,
            "sources": {
                "policy": eur_pol_src,
                "y2": eur_y2_src,
                "y10": eur_y10_src,
            },
        },
        "GBP": {
            "policy": load_gbp_bank_rate_history(),
            "y2": gbp_y["y2"],
            "y10": gbp_y["y10"],
            "sources": {
                "policy": CURRENCY_POLICY_SOURCE["GBP"],
                "y2": BOE_GLC_SOURCE,
                "y10": BOE_GLC_SOURCE,
            },
        },
        "JPY": {
            "policy": load_bis_policy_history("jp"),
            "y2": jpy_y2,
            "y10": jpy_y10,
            "sources": {
                "policy": CURRENCY_POLICY_SOURCE["JPY"],
                "y2": jpy_y2_src,
                "y10": jpy_y10_src,
            },
        },
        "CAD": {
            "policy": {d: v["policy"] for d, v in cad.items() if v.get("policy") is not None},
            "y2": {d: v["y2"] for d, v in cad.items() if v.get("y2") is not None},
            "y10": {d: v["y10"] for d, v in cad.items() if v.get("y10") is not None},
            "sources": {
                "policy": CAD_VALET_SOURCE,
                "y2": CAD_VALET_SOURCE,
                "y10": CAD_VALET_SOURCE,
            },
        },
        "AUD": {
            "policy": aud["policy"],
            "y2": aud["y2"],
            "y10": aud["y10"],
            "sources": {
                "policy": RBA_F1_SOURCE,
                "y2": RBA_F2_SOURCE,
                "y10": RBA_F2_SOURCE,
            },
        },
        "NZD": {
            "policy": load_bis_policy_history("nz"),
            "y2": nzd_y2,
            "y10": nzd_y10,
            "sources": {
                "policy": CURRENCY_POLICY_SOURCE["NZD"],
                "y2": nzd_y2_src,
                "y10": nzd_y10_src,
            },
        },
        "CHF": {
            "policy": load_bis_policy_history("ch"),
            "y2": chf_y2,
            "y10": chf_y10,
            "sources": {
                "policy": CURRENCY_POLICY_SOURCE["CHF"],
                "y2": chf_y2_src,
                "y10": chf_y10_src,
            },
        },
    }


def build_differential_series(
    base: str,
    quote: str,
    field: str,
    histories: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    base_map = (histories.get(base) or {}).get(field) or {}
    quote_map = (histories.get(quote) or {}).get(field) or {}
    if not base_map or not quote_map:
        return []
    dates = sorted(set(base_map.keys()) & set(quote_map.keys()))
    rows: list[dict[str, Any]] = []
    for d in dates:
        b = base_map.get(d)
        q = quote_map.get(d)
        if b is None or q is None:
            continue
        rows.append({"date": d, "value": round(float(b) - float(q), 4)})
    return rows


def ensure_fx_macro_caches() -> None:
    """Refresh shallow caches when online (foundation build step)."""
    ensure_jpy_jgb_cache()
    ensure_boe_glc_archive()
    ensure_cad_valet_deep_cache()
    for ref in ("jp", "nz", "ch"):
        ensure_bis_policy_history_cache(ref)
    if not offline_mode():
        try:
            hist = _fetch_gbp_bank_rate_history()
            if len(hist) >= MIN_FOUNDATION_OBS:
                path = CACHE_DIR / "gbp_bank_rate_history.txt"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(_bank_rate_csv_text(hist), encoding="utf-8")
        except Exception:
            pass
