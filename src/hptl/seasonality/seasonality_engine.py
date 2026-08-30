"""Generic forward-looking seasonality engine (visual audit layer only).

Seasonal curves are built from historical week-to-week returns, not averaged
rebased price levels. This preserves the market-like sequence of advances and
pullbacks while keeping the public chart on a readable weekly grid.
"""

from __future__ import annotations

from datetime import datetime, timezone
from statistics import median
from typing import Any

import pandas as pd

BULL_FORWARD_PCT = 1.0
BEAR_FORWARD_PCT = -1.0
MIN_HIST_YEARS_FOR_FULL = 3
MIN_SAMPLE_YEARS_FOR_DIRECTION = 5
INSUFFICIENT_HISTORY = "Insufficient history"
LOW_SAMPLE_RELIABILITY = "Low sample reliability"


def iso_week(date: str) -> tuple[int, int]:
    dt = pd.Timestamp(str(date)[:10])
    cal = dt.isocalendar()
    year = int(cal.year)
    week = int(cal.week)
    if week > 52:
        week = 52
    return year, week


def year_week_closes(bars: list[tuple[str, float]]) -> dict[int, dict[int, float]]:
    """Map chronological observations to the final close observed in each ISO week."""
    yw: dict[int, dict[int, float]] = {}
    for date, close in bars:
        year, week = iso_week(date)
        yw.setdefault(year, {})[week] = float(close)
    return yw


def normalized_year_path(week_closes: dict[int, float]) -> dict[int, float]:
    """Rebase an individual year's observed weekly closes to 100."""
    if not week_closes:
        return {}
    base_week = 1 if 1 in week_closes else min(week_closes.keys())
    base = week_closes.get(base_week)
    if base is None or base == 0:
        return {}
    return {w: (c / base) * 100.0 for w, c in week_closes.items()}


def weekly_returns(week_closes: dict[int, float]) -> dict[int, float]:
    """Return the observed close-to-close return ending at each ISO week.

    Missing weeks are deliberately not bridged: a seasonal week only receives a
    return when both adjacent weekly closes exist. That prevents holidays or data
    gaps from silently turning a multi-week move into a one-week seasonal move.
    """
    out: dict[int, float] = {}
    for week in range(2, 53):
        prev = week_closes.get(week - 1)
        cur = week_closes.get(week)
        if prev is None or cur is None or prev == 0:
            continue
        out[week] = (cur / prev) - 1.0
    return out


def _trimmed_mean(values: list[float], trim_fraction: float = 0.10) -> float | None:
    if not values:
        return None
    vals = sorted(float(v) for v in values)
    if len(vals) < 5:
        return sum(vals) / len(vals)
    trim = int(len(vals) * trim_fraction)
    if trim <= 0 or trim * 2 >= len(vals):
        return sum(vals) / len(vals)
    vals = vals[trim:-trim]
    return sum(vals) / len(vals)


def seasonal_return_profile(years: list[int], yw: dict[int, dict[int, float]]) -> dict[int, dict[str, Any]]:
    """Robust week-to-week seasonal return statistics for an historical window."""
    by_year = {y: weekly_returns(yw.get(y, {})) for y in years}
    profile: dict[int, dict[str, Any]] = {}
    for week in range(2, 53):
        samples = [rets[week] for rets in by_year.values() if week in rets]
        robust = _trimmed_mean(samples)
        profile[week] = {
            "return": robust,
            "median_return": median(samples) if samples else None,
            "mean_return": sum(samples) / len(samples) if samples else None,
            "sample_years": len(samples),
            "positive_rate": (sum(1 for r in samples if r > 0) / len(samples)) if samples else None,
        }
    return profile


def avg_path(years: list[int], yw: dict[int, dict[int, float]]) -> dict[int, float | None]:
    """Construct a seasonal index by compounding historical week-to-week returns.

    The old engine averaged normalized price levels for every week, which naturally
    produced rounded, overly smooth curves. The new path starts at 100 and compounds
    the robust historical return for each week. No spline/interpolation/smoothing is
    applied, so recurring advances, pullbacks and reversals remain visible.
    """
    if not years:
        return {}
    profile = seasonal_return_profile(years, yw)
    out: dict[int, float | None] = {1: 100.0}
    level = 100.0
    for week in range(2, 53):
        ret = profile[week]["return"]
        if ret is None:
            out[week] = None
            continue
        level *= 1.0 + float(ret)
        out[week] = level
    return out


def build_chart_series(*, anchor_week: int, anchor_index: float | None, current_path_raw: dict[int, float], avg_3y: dict[int, float | None], avg_5y: dict[int, float | None], avg_10y: dict[int, float | None], proj_3y: dict[int, float | None], proj_5y: dict[int, float | None], proj_10y: dict[int, float | None], yw: dict[int, dict[int, float]], current_year: int) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for w in range(1, 53):
        actual = current_path_raw.get(w) if w <= anchor_week else None
        s3 = avg_3y.get(w)
        s5 = avg_5y.get(w) if avg_5y else None
        s10 = avg_10y.get(w) if avg_10y else None
        seasonal_primary = s3 if s3 is not None else (s5 if s5 is not None else s10)
        p3 = proj_3y.get(w) if w >= anchor_week else None
        p5 = proj_5y.get(w) if w >= anchor_week else None
        p10 = proj_10y.get(w) if w >= anchor_week else None
        div = actual - seasonal_primary if actual is not None and seasonal_primary is not None else None
        rows.append({"week": w, "close": (yw.get(current_year) or {}).get(w), "actual": round(actual, 2) if actual is not None else None, "seasonal_3y": round(s3, 2) if s3 is not None else None, "seasonal_5y": round(s5, 2) if s5 is not None else None, "seasonal_10y": round(s10, 2) if s10 is not None else None, "proj_3y": round(p3, 2) if p3 is not None else None, "proj_5y": round(p5, 2) if p5 is not None else None, "proj_10y": round(p10, 2) if p10 is not None else None, "divergence": round(div, 2) if div is not None else None, "is_anchor": w == anchor_week, "is_forward": w > anchor_week})
    return rows


def divergence_read(*, anchor_week: int, anchor_index: float | None, avg_3y: dict[int, float | None], avg_5y: dict[int, float | None], avg_10y: dict[int, float | None]) -> dict[str, Any]:
    ref = avg_3y.get(anchor_week); label = "3Y"
    if ref is None and avg_5y: ref = avg_5y.get(anchor_week); label = "5Y"
    if ref is None and avg_10y: ref = avg_10y.get(anchor_week); label = "10Y"
    if anchor_index is None or ref is None: return {"available": False}
    div = anchor_index - ref
    return {"available": True, "anchor_week": anchor_week, "actual_index": round(anchor_index, 2), "seasonal_index": round(ref, 2), "seasonal_window": label, "divergence": round(div, 2), "position": "above" if div > 0.5 else "below" if div < -0.5 else "inline", "summary": f"Current index {anchor_index:.1f} is {abs(div):.1f} pts {'above' if div > 0 else 'below' if div < 0 else 'on'} {label} seasonal ({ref:.1f}) at week {anchor_week}."}


def direction(pct: float | None) -> str:
    if pct is None: return "Neutral"
    if pct >= BULL_FORWARD_PCT: return "Bullish"
    if pct <= BEAR_FORWARD_PCT: return "Bearish"
    return "Neutral"


def _forward_direction_label(*, avg_ret: float | None, sample_years: int) -> str:
    if sample_years < MIN_SAMPLE_YEARS_FOR_DIRECTION:
        return INSUFFICIENT_HISTORY if sample_years <= 0 else LOW_SAMPLE_RELIABILITY
    return direction(avg_ret)


def forward_window_read(*, current_week: int, horizon: int, hist_years: list[int], yw: dict[int, dict[int, float]]) -> dict[str, Any]:
    end_week = min(52, current_week + horizon)
    if current_week >= 52: return {"weeks": horizon, "avg_return_pct": None, "direction": "Neutral", "sample_years": 0, "available": False}
    rets: list[float] = []
    for y in hist_years:
        closes = yw.get(y, {})
        start = closes.get(current_week); end = closes.get(end_week)
        if start is not None and end is not None and start != 0: rets.append((end / start - 1.0) * 100.0)
    avg_ret = _trimmed_mean(rets)
    wins = sum(1 for r in rets if r > 0); sample_years = len(rets)
    win_rate_pct = round(wins / sample_years * 100.0, 1) if rets else None
    dir_label = _forward_direction_label(avg_ret=avg_ret, sample_years=sample_years)
    reliable = sample_years >= MIN_SAMPLE_YEARS_FOR_DIRECTION and avg_ret is not None
    return {"weeks": end_week-current_week, "avg_return_pct": round(avg_ret,2) if avg_ret is not None else None, "median_return_pct": round(median(rets),2) if rets else None, "direction": dir_label, "sample_years": sample_years, "sample_reliability": "reliable" if reliable else LOW_SAMPLE_RELIABILITY if sample_years > 0 else INSUFFICIENT_HISTORY, "win_rate_pct": win_rate_pct, "available": reliable}


def build_hist_year_paths(yw: dict[int, dict[int, float]], years: list[int]) -> list[dict[str, Any]]:
    out=[]
    for y in years:
        path=normalized_year_path(yw.get(y,{}))
        if path: out.append({"year":y,"points":[{"week":w,"index":round(v,2)} for w,v in sorted(path.items())]})
    return out


def path_alignment_label(divergence_read: dict[str, Any] | None) -> str:
    if not divergence_read or not divergence_read.get("available"): return "Unknown"
    div=divergence_read.get("divergence"); pos=str(divergence_read.get("position") or "")
    if div is not None and abs(float(div)) >= 8.0: return "Diverging from seasonal path"
    if pos == "above": return "Above seasonal path"
    if pos == "below": return "Below seasonal path"
    return "Following seasonal path"


def seasonal_phase(forward_8w: dict[str, Any] | None) -> str:
    if not forward_8w or not forward_8w.get("available"):
        rel=str((forward_8w or {}).get("sample_reliability") or "")
        return rel if rel in {LOW_SAMPLE_RELIABILITY,INSUFFICIENT_HISTORY} else "Unknown"
    d=str(forward_8w.get("direction") or "Neutral")
    return "Bullish phase" if d=="Bullish" else "Bearish phase" if d=="Bearish" else d if d in {LOW_SAMPLE_RELIABILITY,INSUFFICIENT_HISTORY} else "Neutral phase"


def build_seasonality_timeline(bars:list[tuple[str,float]],*,yw:dict[int,dict[int,float]],latest_date:str,anchor_week:int,anchor_index:float|None,avg_3y:dict[int,float|None],avg_5y:dict[int,float|None],avg_10y:dict[int,float|None],proj_3y:dict[int,float|None],proj_5y:dict[int,float|None],proj_10y:dict[int,float|None],max_years:int=10)->list[dict[str,Any]]:
    if not bars:return []
    latest_ts=pd.Timestamp(str(latest_date)[:10]);cutoff=latest_ts-pd.DateOffset(years=max_years);rows=[]
    for date,close in bars:
        ts=pd.Timestamp(str(date)[:10])
        if ts<cutoff:continue
        year,week=iso_week(date);actual_idx=normalized_year_path(yw.get(year,{})).get(week)
        rows.append({"date":str(date)[:10],"label":str(date)[:10],"price":round(float(close),6),"iso_week":week,"seasonal_actual":round(actual_idx,2) if actual_idx is not None else None,"seasonal_3y":round(v,2) if (v:=avg_3y.get(week)) is not None else None,"seasonal_5y":round(v,2) if (v:=avg_5y.get(week)) is not None else None,"seasonal_10y":round(v,2) if (v:=avg_10y.get(week)) is not None else None,"proj_3y":None,"proj_5y":None,"proj_10y":None,"is_projection":False})
    if not rows or anchor_index is None:return rows
    cursor=latest_ts
    for w in range(anchor_week+1,53):
        p3=proj_3y.get(w);p5=proj_5y.get(w);p10=proj_10y.get(w)
        if p3 is None and p5 is None and p10 is None:continue
        cursor+=pd.Timedelta(days=7)
        rows.append({"date":cursor.strftime("%Y-%m-%d"),"label":cursor.strftime("%Y-%m-%d"),"price":None,"iso_week":w,"seasonal_actual":None,"seasonal_3y":None,"seasonal_5y":None,"seasonal_10y":None,"proj_3y":round(p3,2) if p3 is not None else None,"proj_5y":round(p5,2) if p5 is not None else None,"proj_10y":round(p10,2) if p10 is not None else None,"is_projection":True})
    return rows


def project_forward(*,anchor_week:int,anchor_index:float,avg:dict[int,float|None])->dict[int,float|None]:
    """Anchor the return-built seasonal path to the current market index."""
    base=avg.get(anchor_week)
    if base is None or base==0:return {}
    return {w:(anchor_index*(v/base) if v is not None else None) for w in range(anchor_week,53) for v in [avg.get(w)]}


def confidence(*,current_week:int,horizon:int,years_3y:list[int],years_5y:list[int],years_10y:list[int],yw:dict[int,dict[int,float]])->dict[str,Any]:
    windows=[(l,y) for l,y in [("3Y",years_3y),("5Y",years_5y),("10Y",years_10y)] if y]
    eligible=[(l,y) for l,y in windows if len(y)>=MIN_SAMPLE_YEARS_FOR_DIRECTION]
    if not eligible:
        m=max((len(y) for _,y in windows),default=0);return {"level":LOW_SAMPLE_RELIABILITY if m else INSUFFICIENT_HISTORY,"detail":f"Only {m} historical year(s) — need {MIN_SAMPLE_YEARS_FOR_DIRECTION}+ for seasonal confidence.","agreement":0,"windows":len(windows),"horizon_weeks":horizon,"min_sample_years":m}
    dirs=[]
    for l,y in eligible:
        r=forward_window_read(current_week=current_week,horizon=horizon,hist_years=y,yw=yw)
        if r.get("available"):dirs.append((l,r["direction"]))
    if not dirs:return {"level":"Weak","detail":"Insufficient historical sample for confidence.","agreement":0,"windows":0}
    ds=[d for _,d in dirs];bull=ds.count("Bullish");bear=ds.count("Bearish");neutral=ds.count("Neutral");n=len(ds)
    if n>=2 and (bull==n or bear==n):level="Strong";detail=f"{'/'.join(l for l,_ in dirs)} agree {'bullish' if bull==n else 'bearish'} over the next {horizon} weeks."
    elif n>=2 and max(bull,bear,neutral)>=2:level="Medium";detail=f"Partial agreement ({', '.join(f'{l}: {d}' for l,d in dirs)})."
    else:level="Weak";detail=f"Mixed signals ({', '.join(f'{l}: {d}' for l,d in dirs)})."
    return {"level":level,"detail":detail,"agreement":max(bull,bear,neutral),"windows":n,"horizon_weeks":horizon}


def build_summary(*,market:str,current_week:int,anchor_index:float|None,read_8w:dict[str,Any],read_12w:dict[str,Any],confidence_block:dict[str,Any],latest_date:str)->str:
    parts=[]
    if anchor_index is not None:parts.append(f"As of {latest_date} (week {current_week}), the {market} index stands at {anchor_index:.1f} (rebased 100 at week 1).")
    if read_8w.get("available"):parts.append(f"Seasonal history suggests the next {read_8w['weeks']} weeks are typically {read_8w['direction'].lower()} ({read_8w['avg_return_pct']:+.2f}% robust avg, {read_8w['win_rate_pct']:.0f}% positive, n={read_8w['sample_years']}).")
    if read_12w.get("available") and read_12w["weeks"]!=read_8w.get("weeks"):parts.append(f"Over {read_12w['weeks']} weeks, historical robust average is {read_12w['avg_return_pct']:+.2f}% ({read_12w['direction'].lower()}, n={read_12w['sample_years']}).")
    parts.append(f"Seasonality confidence: {confidence_block.get('level','Weak')} — {confidence_block.get('detail','')}");parts.append("Forward-looking audit only — not a trade signal.");return " ".join(parts)


def compute_seasonality_price_block(market:str,bars:list[tuple[str,float]],*,price_store_key:str,bar_source:str="weekly",canonical_source:str|None=None,canonical_symbol:str|None=None,price_derivation:str|None=None,proxy:bool|None=None,proxy_explanation:str|None=None)->dict[str,Any]:
    if not bars:return {"market":market,"available":False,"reason":"No weekly price bars."}
    yw=year_week_closes(bars);all_years=sorted(yw)
    if not all_years:return {"market":market,"available":False,"reason":"No year/week price mapping."}
    latest_date=bars[-1][0];latest_close=bars[-1][1];latest_year,latest_week=iso_week(latest_date);current_year=latest_year
    hist_years=[y for y in all_years if y<current_year];years_count=len(hist_years);windows=[]
    years_3y=hist_years[-3:] if years_count>=3 else hist_years[:];years_5y=hist_years[-5:] if years_count>=5 else [];years_10y=hist_years[-10:] if years_count>=10 else []
    if years_count>=3:windows.append("3Y")
    if years_count>=5:windows.append("5Y")
    if years_count>=10:windows.append("10Y")
    current_path_raw=normalized_year_path(yw.get(current_year,{}))
    if not current_path_raw:return {"market":market,"available":False,"reason":"No current-year weekly price path.","years_of_history":years_count}
    candidates=[w for w in current_path_raw if w<=latest_week]
    if not candidates:return {"market":market,"available":False,"reason":"No current-year close at or before latest week."}
    anchor_week=latest_week if latest_week in current_path_raw else max(candidates);anchor_index=current_path_raw.get(anchor_week);anchor_close=yw.get(current_year,{}).get(anchor_week,latest_close)
    avg_3y=avg_path(years_3y,yw) if years_3y else {};avg_5y=avg_path(years_5y,yw) if years_5y else {};avg_10y=avg_path(years_10y,yw) if years_10y else {}
    proj_3y=project_forward(anchor_week=anchor_week,anchor_index=anchor_index or 100.0,avg=avg_3y) if years_3y else {};proj_5y=project_forward(anchor_week=anchor_week,anchor_index=anchor_index or 100.0,avg=avg_5y) if years_5y else {};proj_10y=project_forward(anchor_week=anchor_week,anchor_index=anchor_index or 100.0,avg=avg_10y) if years_10y else {}
    current_path_series=[{"week":w,"index":round(v,2),"close":yw.get(current_year,{}).get(w)} for w,v in sorted(current_path_raw.items()) if w<=anchor_week]
    forward_projection=[{"week":w,"anchor":round(anchor_index,2) if w==anchor_week and anchor_index is not None else None,"proj_3y":round(proj_3y[w],2) if proj_3y.get(w) is not None else None,"proj_5y":round(proj_5y[w],2) if proj_5y.get(w) is not None else None,"proj_10y":round(proj_10y[w],2) if proj_10y.get(w) is not None else None} for w in range(anchor_week,53)]
    ref=years_10y or years_5y or years_3y;read_4w=forward_window_read(current_week=anchor_week,horizon=4,hist_years=ref,yw=yw);read_8w=forward_window_read(current_week=anchor_week,horizon=8,hist_years=ref,yw=yw);read_12w=forward_window_read(current_week=anchor_week,horizon=12,hist_years=ref,yw=yw)
    conf=confidence(current_week=anchor_week,horizon=8,years_3y=years_3y,years_5y=years_5y,years_10y=years_10y,yw=yw)
    availability_note=f"Only {years_count} year(s) of price history — forward seasonality is limited." if years_count<3 else "Only 3Y seasonality available." if years_count<5 else "3Y and 5Y seasonality available; 10Y not available." if years_count<10 else None
    price_age=(pd.Timestamp(datetime.now(timezone.utc).date())-pd.Timestamp(latest_date)).days;price_stale_note=f"Latest price is {latest_date} ({price_age} days old). Forward projection anchors to this close." if price_age>14 else None
    chart_series=build_chart_series(anchor_week=anchor_week,anchor_index=anchor_index,current_path_raw=current_path_raw,avg_3y=avg_3y,avg_5y=avg_5y,avg_10y=avg_10y,proj_3y=proj_3y,proj_5y=proj_5y,proj_10y=proj_10y,yw=yw,current_year=current_year)
    div=divergence_read(anchor_week=anchor_week,anchor_index=anchor_index,avg_3y=avg_3y,avg_5y=avg_5y,avg_10y=avg_10y);hist_paths=build_hist_year_paths(yw,ref);summary=build_summary(market=market,current_week=anchor_week,anchor_index=anchor_index,read_8w=read_8w,read_12w=read_12w,confidence_block=conf,latest_date=latest_date)
    timeline=build_seasonality_timeline(bars,yw=yw,latest_date=latest_date,anchor_week=anchor_week,anchor_index=anchor_index,avg_3y=avg_3y,avg_5y=avg_5y,avg_10y=avg_10y,proj_3y=proj_3y,proj_5y=proj_5y,proj_10y=proj_10y,max_years=10)
    return {"market":market,"available":True,"price_store_key":price_store_key,"bar_source":bar_source,"price_derivation":price_derivation or bar_source,"canonical_source":canonical_source,"canonical_symbol":canonical_symbol,"proxy":proxy,"proxy_explanation":proxy_explanation,"seasonality_method":"robust_weekly_returns_v2","seasonality_method_note":"Historical close-to-close weekly returns compounded into a seasonal path; 10% trimmed mean when sample permits; no curve smoothing or interpolation.","years_available":len(all_years),"years_used":years_count,"sample_size":len(years_3y),"weekly_bars_count":len(bars),"latest_price":{"date":latest_date,"close":latest_close,"week":anchor_week,"index":round(anchor_index,2) if anchor_index is not None else None},"current_year":current_year,"current_week":anchor_week,"years_of_history":years_count,"windows_available":windows,"forward_projection_available":any(proj_3y.get(w) is not None or proj_5y.get(w) is not None or proj_10y.get(w) is not None for w in range(anchor_week+1,53)),"availability_note":availability_note,"price_stale_note":price_stale_note,"chart_series":chart_series,"divergence_read":div,"path_alignment":path_alignment_label(div),"seasonal_phase":seasonal_phase(read_8w),"hist_year_paths":hist_paths,"timeline_series":timeline,"timeline_start":timeline[0]["date"] if timeline else None,"timeline_end":timeline[-1]["date"] if timeline else None,"timeline_anchor_date":latest_date,"current_path":current_path_series,"forward_projection":forward_projection,"return_profiles":{"3Y":seasonal_return_profile(years_3y,yw) if years_3y else {},"5Y":seasonal_return_profile(years_5y,yw) if years_5y else {},"10Y":seasonal_return_profile(years_10y,yw) if years_10y else {}},"forward_read":{"next_4w":read_4w,"next_8w":read_8w,"next_12w":read_12w,"summary":summary},"confidence":conf}
