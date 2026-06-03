/**
 * Economic calendar interpretation and surprise tone (aligned with calendar_interpretation.py).
 */

const fmt = (v) => (v === null || v === undefined || v === '' ? null : v)

function normName(name) {
  return String(name || '')
    .toLowerCase()
    .replace(/\s+/g, ' ')
    .trim()
}

function usdBlock(direction, hot) {
  if (direction === 'beat') return hot ? 'USD bullish · yields bullish · gold bearish' : 'USD slightly firm · yields bid'
  if (direction === 'miss') return 'USD softer · yields down · gold supported'
  return 'USD reaction likely muted · check magnitude'
}

export function interpretCalendarEvent(ev) {
  const existing = String(ev?.interpretation || '').trim()
  if (existing) return existing

  const name = normName(ev?.event_name)
  const country = String(ev?.country || '').toUpperCase()
  const d = ev?.direction_vs_forecast
  const mag = ev?.magnitude_vs_forecast || 'small'
  const hot = mag === 'large'

  if (ev?.actual == null && d !== 'beat' && d !== 'miss') {
    if (/cpi|pce|inflation|jobless|claims|retail sales|nonfarm|payroll|fomc|gdp/i.test(name)) {
      return `Scheduled ${ev?.event_name || 'release'} — interpretation depends on actual vs forecast.`
    }
    return ''
  }

  if (/cpi|pce|consumer price/.test(name)) {
    if (d === 'beat') return `CPI/PCE hotter than forecast → ${usdBlock('beat', hot)} · risk assets pressured`
    if (d === 'miss') return `CPI/PCE cooler than forecast → ${usdBlock('miss', hot)} · risk assets supported`
    return 'Inflation print — hotter = USD/yields up, cooler = the opposite'
  }

  if (/jobless|initial claims/.test(name)) {
    if (d === 'beat') return 'Claims higher than forecast → labour soft → USD softer · risk cautious'
    if (d === 'miss') return 'Claims lower than forecast → labour tight → USD bullish · cuts less likely'
    return 'Claims: lower than forecast is USD/risk positive'
  }

  if (/retail sales/.test(name)) {
    if (d === 'beat') return `Retail sales beat → ${usdBlock('beat', hot)} · risk sentiment may improve but rate-cut hopes fade`
    if (d === 'miss') return `Retail sales miss → ${usdBlock('miss', hot)} · growth concern`
    return 'Retail sales: beat = USD/yields bid, miss = the opposite'
  }

  if (/nonfarm|non farm|payroll/.test(name)) {
    if (d === 'beat') return `Payrolls beat → ${usdBlock('beat', hot)} · equities mixed-to-positive if growth`
    if (d === 'miss') return `Payrolls miss → ${usdBlock('miss', hot)} · risk assets cautious`
    return 'NFP — beat/miss drives USD, yields, and risk tone'
  }

  if (/gdp/.test(name)) {
    if (d === 'beat') return 'GDP beat → growth firm → USD firm · cyclicals supported'
    if (d === 'miss') return 'GDP miss → growth concern → USD softer'
    return 'GDP: beat vs forecast sets growth/USD tone'
  }

  if (/pmi|ism/.test(name)) {
    if (d === 'beat') return 'Activity beat → growth supportive · USD firm'
    if (d === 'miss') return 'Activity miss → growth concern · USD softer'
    return 'PMI/ISM: beat = growth/USD positive vs forecast'
  }

  if (/fomc|fed |federal reserve/.test(name)) {
    return 'Fed communication — policy path drives USD, yields, and gold'
  }

  if (country === 'US' || country === 'USA' || country === 'UNITED STATES') {
    return `US macro print — USD, yields, and risk assets react first`
  }

  return country ? `${country} macro event — compare actual vs forecast` : ''
}

/** hawkish | dovish | neutral | mixed */
export function surpriseTone(ev) {
  const d = ev?.direction_vs_forecast
  if (!d || (d !== 'beat' && d !== 'miss')) return 'neutral'
  const name = normName(ev?.event_name)

  if (/cpi|pce|inflation|ppi/.test(name)) {
    return d === 'beat' ? 'hawkish' : 'dovish'
  }
  if (/jobless|initial claims/.test(name)) {
    return d === 'beat' ? 'dovish' : 'hawkish'
  }
  if (/retail sales|gdp|pmi|ism|nonfarm|payroll/.test(name)) {
    return d === 'beat' ? 'hawkish' : 'dovish'
  }
  if (/crude.*inventor|natural gas.*storage/.test(name)) {
    return d === 'beat' ? 'bearish_commodity' : 'bullish_commodity'
  }
  return d === 'beat' ? 'hawkish' : 'dovish'
}

export function surpriseLabel(ev) {
  const d = ev?.direction_vs_forecast
  if (!d || ev?.actual == null) return 'Upcoming'
  const mag = ev?.magnitude_vs_forecast || ''
  return `${d} vs forecast${mag ? ` (${mag})` : ''}`
}

export function valueTone(ev, field) {
  const tone = surpriseTone(ev)
  if (ev?.actual == null) return 'neutral'
  if (field === 'actual' && (tone === 'hawkish' || tone === 'bearish_commodity')) return 'hot'
  if (field === 'actual' && (tone === 'dovish' || tone === 'bullish_commodity')) return 'cool'
  return 'neutral'
}

export function formatValues(ev) {
  const unit = ev?.unit ? ` ${ev.unit}` : ''
  return {
    actual: fmt(ev.actual),
    forecast: fmt(ev.forecast),
    previous: fmt(ev.previous),
    unit,
    actualTone: valueTone(ev, 'actual'),
    forecastTone: 'neutral',
    previousTone: 'neutral',
  }
}
