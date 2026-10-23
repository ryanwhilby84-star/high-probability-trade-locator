import ast,concurrent.futures,csv,io,json,urllib.request,zipfile,subprocess
from pathlib import Path
src=ast.parse(Path('src/hptl/cot/legacy_cot.py').read_text())
codes=next(ast.literal_eval(n.value) for n in src.body if isinstance(n,ast.AnnAssign) and getattr(n.target,'id','')=='CANONICAL_LEGACY_CODE')
def load(year):
 p=Path(f'/tmp/ztack-cftc-{year}-audit.zip')
 if not p.exists():
  subprocess.run(['curl','-fsSL','--retry','1','--max-time','40',f'https://www.cftc.gov/files/dea/history/deacot{year}.zip','-o',str(p)],check=True)
 z=zipfile.ZipFile(p)
 return list(csv.DictReader(io.StringIO(z.read(z.namelist()[0]).decode('utf-8-sig'))))
with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
 raw=[r for rows in pool.map(load,range(2011,2027)) for r in rows]
indexed={}
for r in raw:
 code=r['CFTC Contract Market Code'].strip();date=r['As of Date in Form YYYY-MM-DD'].strip()
 indexed.setdefault((code,date),[]).append(r)
source=json.loads(Path('web-dashboard/public/data/cot_3y_series_latest.json').read_text())
checks=0;bad=[];missing=[]
for name,b in source['markets'].items():
 for row in b.get('series',[]):
  matches=indexed.get((codes[name],row['date']),[])
  if len(matches)!=1:missing.append((name,row['date'],len(matches)));continue
  r=matches[0]
  for key,prefix in [('commercial_net','Commercial'),('institutional_net','Noncommercial'),('retail_net','Nonreportable')]:
   expected=float(r[f'{prefix} Positions-Long (All)'].replace(',',''))-float(r[f'{prefix} Positions-Short (All)'].replace(',',''))
   if row.get(key)!=expected:bad.append((name,row['date'],key,row.get(key),expected))
   checks+=1
print(json.dumps({'raw_cftc_net_checks':checks,'mismatches':len(bad),'missing_or_duplicate_source_weeks':len(missing),'failures':bad[:10],'missing':missing[:10]}))
assert not bad and not missing
