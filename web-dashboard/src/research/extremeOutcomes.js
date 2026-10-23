/** Exploratory COT event study. Uses point-in-time min/max indices, not rank percentiles. */
export const BASELINES = {'3y':156,'5y':260,'10y':520,all:null}
export const HORIZONS = [1,4,8,12,26]
const finite = v => typeof v === 'number' && Number.isFinite(v)
const day = date => Date.parse(String(date).slice(0,10)+'T00:00:00Z')
const WEEK = 7*86400000
const median = a => {if(!a.length)return null;const s=[...a].sort((a,b)=>a-b);const i=Math.floor(s.length/2);return s.length%2?s[i]:(s[i-1]+s[i])/2}
export function rangeIndex(values,current){
 if(!finite(current)||!values.length||values.some(v=>!finite(v)))return null
 const lo=Math.min(...values),hi=Math.max(...values)
 return hi===lo?null:100*(current-lo)/(hi-lo)
}
export function buildExtremeStudy(series,{baseline='3y',group='commercial_net',tail='low',band=90,direction='long',target=5}={}){
 if(!(baseline in BASELINES)||!['commercial_net','institutional_net','retail_net'].includes(group)||!['low','high'].includes(tail)||![80,90,95].includes(Number(band))||!['long','short'].includes(direction)||!finite(target)||target<=0)throw Error('Invalid research settings')
 const rows=[...(Array.isArray(series)?series:[])].sort((a,b)=>day(a.date)-day(b.date))
 if(rows.some((r,i)=>!finite(day(r.date))||(i&&day(r.date)===day(rows[i-1].date))))throw Error('Research dates must be valid and unique')
 const width=BASELINES[baseline],minimum=width||156,sign=direction==='long'?1:-1
 const episodes=[];let inside=false,eligibleWeeks=0
 for(let i=0;i<rows.length;i++){
  const start=width?i-width+1:0
  if(start<0||i-start+1<minimum){inside=false;continue}
  const window=rows.slice(start,i+1)
  // Missing COT or calendar gaps must never masquerade as a complete lookback.
  if(window.some((r,j)=>!finite(r[group])||(j&&Math.abs(day(r.date)-day(window[j-1].date)-WEEK)>86400000))){inside=false;continue}
  const index=rangeIndex(window.map(r=>r[group]),rows[i][group]);if(index==null){inside=false;continue}eligibleWeeks++
  const extreme=tail==='high'?index>=Number(band):index<=100-Number(band)
  if(extreme&&!inside){
   const entry=i+1,entryRow=rows[entry]
   const validEntry=entryRow&&finite(entryRow.price)&&entryRow.price>0&&day(entryRow.date)-day(rows[i].date)===WEEK&&day(entryRow.price_date||entryRow.date)>=day(rows[i].date)+3*86400000&&day(entryRow.price_date||entryRow.date)<=day(entryRow.date)
   episodes.push({signalIndex:i,signalDate:rows[i].date,index,baselineStart:window[0].date,baselineEnd:rows[i].date,observations:window.length,entryIndex:entry,entryDate:entryRow?.date||null,entryPrice:validEntry?entryRow.price:null})
  }
  inside=extreme
 }
 const horizons=HORIZONS.map(h=>{
  let incomplete=0,overlapping=0,lastEnd=-1;const cases=[]
  for(const e of episodes){
   const end=e.entryIndex+h,path=rows.slice(e.entryIndex,end+1)
   if(e.entryPrice==null||path.length!==h+1||path.some((r,j)=>!finite(r.price)||r.price<=0||(j&&day(r.date)-day(path[j-1].date)!==WEEK))){incomplete++;continue}
   if(e.entryIndex<=lastEnd){overlapping++;continue}lastEnd=end
   const returns=path.slice(1).map(r=>sign*(r.price/e.entryPrice-1)*100)
   const terminal=returns.at(-1),favourable=Math.max(0,...returns),adverse=Math.min(0,...returns)
   cases.push({...e,exitDate:rows[end].date,exitPrice:rows[end].price,returnPct:terminal,favourablePct:favourable,adversePct:adverse,targetHit:favourable>=target})
  }
  const n=cases.length
  return{weeks:h,n,incomplete,overlapping,targetHitRate:n?100*cases.filter(c=>c.targetHit).length/n:null,positiveReturnRate:n?100*cases.filter(c=>c.returnPct>0).length/n:null,medianReturn:median(cases.map(c=>c.returnPct)),medianFavourable:median(cases.map(c=>c.favourablePct)),medianAdverse:median(cases.map(c=>c.adversePct)),cases}
 })
 const cot=rows.filter(r=>finite(r[group])),prices=rows.filter(r=>finite(r.price)&&r.price>0)
 return{baseline,minimum,eligibleWeeks,episodeCount:episodes.length,cotStart:cot[0]?.date||null,cotEnd:cot.at(-1)?.date||null,priceStart:prices[0]?.date||null,priceEnd:prices.at(-1)?.date||null,horizons}
}
/** Keep COT's own weekly cadence; do not count chart carry-forward rows as extra reports. */
export function attachResearchPrices(cotRows,bars){
 const prices=[...(bars||[])].filter(b=>finite(b.close)&&b.close>0&&finite(day(b.date))).sort((a,b)=>day(a.date)-day(b.date))
 let cursor=-1
 return (cotRows||[]).map(row=>{
  while(cursor+1<prices.length&&day(prices[cursor+1].date)<=day(row.date))cursor++
  const p=prices[cursor]
  if(p&&day(row.date)-day(p.date)<WEEK)return{...row,price:p.close,price_date:p.date}
  if(finite(row.price)&&row.price>0&&row.price_date&&day(row.price_date)<=day(row.date)&&day(row.date)-day(row.price_date)<WEEK)return row
  return{...row,price:null,price_date:null}
 })
}
