/** Seasonality workstation toggle state (per instrument, localStorage). */

const STORAGE_PREFIX = 'hptl_seasonality_controls_v1'

export const TOGGLE_KEYS = [
  'showSeasonality',
  'currentYearOnly',
  'show3y',
  'show5y',
  'show10y',
  'forwardProjection',
  'hideUnreliable',
]

export function defaultToggles(block) {
  const grade = block?.trust_grade || 'C'
  const windows = block?.windows_available || []
  const toggles = {
    showSeasonality: grade !== 'C',
    currentYearOnly: false,
    show3y: false,
    show5y: false,
    show10y: false,
    forwardProjection: grade === 'A',
    hideUnreliable: true,
  }
  if (windows.includes('10Y')) toggles.show10y = true
  else if (windows.includes('5Y')) toggles.show5y = true
  else if (windows.includes('3Y')) toggles.show3y = true
  return toggles
}

export function loadToggles(marketId, block) {
  const base = defaultToggles(block)
  if (!marketId || typeof window === 'undefined') return base
  try {
    const raw = window.localStorage.getItem(`${STORAGE_PREFIX}:${marketId}`)
    if (!raw) return base
    const saved = JSON.parse(raw)
    return { ...base, ...saved }
  } catch {
    return base
  }
}

export function saveToggles(marketId, toggles) {
  if (!marketId || typeof window === 'undefined') return
  try {
    window.localStorage.setItem(`${STORAGE_PREFIX}:${marketId}`, JSON.stringify(toggles))
  } catch {
    /* ignore quota */
  }
}

/** Whether chart + forward detail should render. */
export function isSeasonalityVisible(block, toggles) {
  if (!block?.available) return false
  const grade = block.trust_grade || 'C'
  if (!toggles.showSeasonality) return false
  if (toggles.hideUnreliable && grade === 'C') return false
  return true
}

export function seasonalBiasLabel(block) {
  const row = block?.forward_read?.next_8w
  if (row?.available && row.direction) return row.direction
  const phase = String(block?.seasonal_phase || '')
  if (phase.includes('Bullish')) return 'Bullish'
  if (phase.includes('Bearish')) return 'Bearish'
  if (phase.includes('Neutral')) return 'Neutral'
  return '—'
}

export function dataSourceLabel(block) {
  const parts = []
  if (block?.bar_source) parts.push(block.bar_source)
  if (block?.price_store_key) parts.push(block.price_store_key)
  return parts.length ? parts.join(' · ') : '—'
}

export function weeksAvailable(block) {
  if (typeof block?.seasonal_3y_weeks === 'number') return block.seasonal_3y_weeks
  const chart = block?.chart_series || []
  return chart.filter((r) => r?.seasonal_3y != null || r?.seasonal_5y != null || r?.seasonal_10y != null).length
}
