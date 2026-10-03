"""Build a calendar-rolled sugar return index from the supplied ZIP.
Usage: python process_sugar.py INPUT.zip OUTPUT_DIRECTORY
Standard library only. No network access.
"""
import csv, datetime as dt, hashlib, io, json, math, statistics, sys, zipfile
from pathlib import Path
from collections import Counter

MONTHS = dict(zip('FGHJKMNQUVXZ', range(1, 13)))

def parse_date(s):
    if '-' in s: return dt.date.fromisoformat(s)
    if '/' in s:
        return dt.datetime.strptime(s, '%m/%d/%Y').date()
    y = int(s[:2])
    return dt.date(1900+y if y >= 50 else 2000+y, int(s[2:4]), int(s[4:6]))

def write_csv(path, rows, fields):
    with path.open('w', newline='') as f:
        w = csv.DictWriter(f, fieldnames=fields); w.writeheader(); w.writerows(rows)

def run(source, out):
    out.mkdir(parents=True, exist_ok=True)
    contracts = {}; normalized = []; duplicates = 0
    with zipfile.ZipFile(source) as z:
        for name in sorted(z.namelist()):
            if not name.lower().endswith('.txt'): continue
            code = Path(name).stem.upper(); yy = int(code[2:4])
            maturity = (1900+yy if yy >= 50 else 2000+yy, MONTHS[code[4]])
            records = {}
            for r in csv.reader(io.StringIO(z.read(name).decode('utf-8-sig'))):
                if not r or r[0].lower() == 'date': continue
                assert len(r) == 7, (name, r)
                date = parse_date(r[0]); values = list(map(float, r[1:]))
                assert all(math.isfinite(v) for v in values)
                o,h,l,c,v,oi = values
                assert min(o,h,l,c) > 0 and v >= 0 and oi >= 0
                row = dict(date=date.isoformat(), contract=code, open=o, high=h, low=l, close=c, volume=v, open_interest=oi)
                if date in records:
                    assert records[date] == row, ('conflicting duplicate', name, date)
                    duplicates += 1
                records[date] = row
            contracts[code] = (maturity, records)
            normalized.extend(records.values())
    dates = sorted({parse_date(r['date']) for r in normalized})
    daily=[]; rolls=[]; selected=None; index=100.; missing=[]
    for i,date in enumerate(dates):
        prev = dates[i-1] if i else None
        candidates=[]
        if prev:
            for code,(maturity,rs) in contracts.items():
                if selected and maturity < contracts[selected][0]: continue
                p=rs.get(prev); c=rs.get(date)
                if p and c and maturity > (date.year, date.month):
                    candidates.append((-maturity[0], -maturity[1], code))
        old=selected
        if candidates: selected=max(candidates)[-1]
        if not candidates:
            missing.append(date.isoformat()); continue
        rs=contracts[selected][1]; p=rs[prev]; c=rs[date]
        ret=c['close']/p['close']-1; index*=1+ret
        gap=None
        if old and old != selected:
            oldp=contracts[old][1].get(prev)
            if oldp: gap=p['close']/oldp['close']-1
            rolls.append(dict(date=date.isoformat(), from_contract=old, to_contract=selected,
                old_previous_close=oldp['close'] if oldp else '', new_previous_close=p['close'],
                new_current_close=c['close'], excluded_contract_price_gap_pct=100*gap if gap is not None else '',
                included_same_contract_return_pct=ret*100))
        daily.append(dict(date=date.isoformat(), previous_session=prev.isoformat(), active_contract=selected,
            raw_close=c['close'], previous_same_contract_close=p['close'], daily_return=ret,
            return_index=index, selection_previous_volume=p['volume'], current_volume=c['volume'],
            open_interest=c['open_interest'], contract_switch=int(bool(old and old!=selected)),
            calendar_gap_days=(date-prev).days, zero_current_volume=int(c['volume']==0),
            zero_previous_volume=int(p['volume']==0), large_return_review=int(abs(ret)>.10),
            strict_window_eligible=int(c['volume']>0 and p['volume']>0 and abs(ret)<=.10)))
    assert len({r['date'] for r in daily})==len(daily)
    assert all(math.isclose(r['daily_return'],r['raw_close']/r['previous_same_contract_close']-1,abs_tol=1e-12) for r in daily)
    assert all(contracts[b['active_contract']][0]>=contracts[a['active_contract']][0] for a,b in zip(daily,daily[1:]))
    counts=Counter(parse_date(r['date']).year for r in daily)
    annual=[dict(year=y, sessions=n, source_sessions=sum(d.year==y for d in dates),
        calendar_year_complete=int(y < dates[-1].year),
        selected_zero_volume_sessions=sum(parse_date(r['date']).year==y and r['zero_current_volume'] for r in daily)) for y,n in sorted(counts.items())]
    write_csv(out/'sugar_daily_returns.csv',daily,list(daily[0]))
    write_csv(out/'sugar_contracts_normalized.csv',sorted(normalized,key=lambda r:(r['date'],r['contract'])),list(normalized[0]))
    write_csv(out/'sugar_roll_audit.csv',rolls,list(rolls[0]))
    write_csv(out/'sugar_year_coverage.csv',annual,list(annual[0]))
    audit=dict(source_sha256=hashlib.sha256(source.read_bytes()).hexdigest(),contracts=len(contracts),raw_rows=len(normalized),
        source_first_date=dates[0].isoformat(),source_last_date=dates[-1].isoformat(),source_sessions=len(dates),
        return_sessions=len(daily),duplicate_rows_removed=duplicates,unusable_sessions=missing,
        rolls=len(rolls),zero_current_volume_sessions=sum(r['zero_current_volume'] for r in daily),
        largest_absolute_daily_return_pct=max(abs(r['daily_return'])*100 for r in daily),
        returns_over_10_pct=[r for r in daily if abs(r['daily_return'])>.10],
        validation='unique daily dates; same-contract return reconciliation; forward-only contract selection passed')
    (out/'audit.json').write_text(json.dumps(audit,indent=2))
    payload = dict(schema_version=1, instrument='Sugar', source='User-supplied SB contract archive',
        first_date=dates[0].isoformat(), last_date=dates[-1].isoformat(),
        full_years=list(range(dates[0].year, dates[-1].year)),
        method='Calendar roll; same-contract close returns; delivery month excluded',
        columns=['date','previous_session','daily_return','strict_window_eligible','active_contract'],
        rows=[[r['date'],r['previous_session'],r['daily_return'],r['strict_window_eligible'],r['active_contract']] for r in daily])
    (out/'sugar_archive_daily.json').write_text(json.dumps(payload,separators=(',',':')))
    print(json.dumps({k:v for k,v in audit.items() if k not in ('returns_over_10_pct','unusable_sessions')},indent=2))
    print('Unusable session count:',len(missing))

if __name__=='__main__': run(Path(sys.argv[1]),Path(sys.argv[2]))
