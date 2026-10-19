/** Pure seasonal window statistics. Input is verified daily close observations. */
export function scanSeasonalWindows(rows, options = {}) {
  const { lookbackYears = 15, windowDays = [10, 20, 30, 40, 60], minYears = 8, minCoverage = 0.7 } = options
  const valid = (rows || []).map(r => ({ date: String(r.date || r.timestamp || '').slice(0, 10), close: Number(r.close) }))
    .filter(r => /^\d{4}-\d{2}-\d{2}$/.test(r.date) && Number.isFinite(r.close) && r.close > 0)
    .sort((a,b) => a.date.localeCompare(b.date))
  const years = [...new Set(valid.map(r => Number(r.date.slice(0,4))))].sort((a,b)=>a-b)
  // Never include an incomplete current year in historical outcome statistics.
  const completed = years.filter(y => y < new Date().getUTCFullYear()).slice(-lookbackYears)
  const byYear = new Map(completed.map(y => [y, valid.filter(r => Number(r.date.slice(0,4)) === y)]))
  const results = []
  for (let day = 1; day <= 365; day += 7) {
    for (const duration of windowDays) {
      const samples = []
      for (const [year, observations] of byYear) {
        const start = new Date(Date.UTC(year,0,day))
        const end = new Date(start.getTime() + duration * 86400000)
        const first = observations.find(r => r.date >= start.toISOString().slice(0,10))
        const last = [...observations].reverse().find(r => r.date <= end.toISOString().slice(0,10) && (!first || r.date > first.date))
        if (!first || !last) continue
        const actualDays = (Date.parse(last.date) - Date.parse(first.date)) / 86400000
        if (actualDays < duration * minCoverage) continue
        samples.push((last.close / first.close - 1) * 100)
      }
      if (samples.length < minYears) continue
      const sorted = [...samples].sort((a,b)=>a-b)
      const mean = samples.reduce((a,b)=>a+b,0)/samples.length
      const median = sorted.length % 2 ? sorted[(sorted.length-1)/2] : (sorted[sorted.length/2-1]+sorted[sorted.length/2])/2
      const positive = samples.filter(v=>v>0).length
      const negative = samples.filter(v=>v<0).length
      const start = new Date(Date.UTC(2025,0,day)).toISOString().slice(5,10)
      const end = new Date(Date.UTC(2025,0,day+duration)).toISOString().slice(5,10)
      results.push({ start, end, calendarDay:day, durationDays:duration, sampleYears:samples.length,
        medianReturnPct:median, meanReturnPct:mean, positiveYears:positive, negativeYears:negative,
        bullishFrequency:positive/samples.length, bearishFrequency:negative/samples.length,
        direction:median >= 0 ? 'BULLISH':'BEARISH' })
    }
  }
  return results.sort((a,b)=>Math.max(b.bullishFrequency,b.bearishFrequency)-Math.max(a.bullishFrequency,a.bearishFrequency) || Math.abs(b.medianReturnPct)-Math.abs(a.medianReturnPct))
}
