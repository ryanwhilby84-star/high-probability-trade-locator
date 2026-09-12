import { classifyRoadmapHorizon } from './roadmapView.js'

export const DXY_MARKET_ID = 'US Dollar Index / DX'

export const DXY_COMPONENTS = [
  { id: 'Euro FX / 6E', code: '6E', label: 'Euro', weight: 0.576 },
  { id: 'Japanese Yen / 6J', code: '6J', label: 'Yen', weight: 0.136 },
  { id: 'British Pound / 6B', code: '6B', label: 'Pound', weight: 0.119 },
  { id: 'Canadian Dollar / 6C', code: '6C', label: 'Canadian Dollar', weight: 0.091 },
  { id: 'Swiss Franc / 6S', code: '6S', label: 'Swiss Franc', weight: 0.036 },
]

export const DXY_MISSING_COMPONENT_WEIGHT = 0.042 // SEK: no equivalent major CME contract in HPTL.
export const DXY_TRACKED_COMPONENT_WEIGHT = DXY_COMPONENTS.reduce((sum, row) => sum + row.weight, 0)
export const CROSS_MARKET_LOOKBACKS = ['5Y', '10Y', '15Y']
export const CROSS_MARKET_HORIZONS = [4, 8, 12]

export function roadmapStats(payload, horizon) {
  const roadmap = payload?.seasonal_roadmap || payload?.seasonality?.seasonal_roadmap || null
  return roadmap?.forecast_stats?.[`${horizon}w`] || null
}

export function seasonalDirection(payload, horizon) {
  const row = roadmapStats(payload, horizon)
  if (!row) return 'Unavailable'
  return classifyRoadmapHorizon(row)
}

export function inverseDirection(direction) {
  if (direction === 'Bullish') return 'Bearish'
  if (direction === 'Bearish') return 'Bullish'
  return 'Mixed'
}

export function buildCrossMarketHorizonResult({ dxyPayload, componentPayloads, horizon }) {
  const dxyDirection = seasonalDirection(dxyPayload, horizon)
  const expectedCurrencyDirection = inverseDirection(dxyDirection)

  let agreeingWeight = 0
  let conflictingWeight = 0
  let mixedWeight = 0
  let unavailableWeight = 0

  const components = DXY_COMPONENTS.map((component) => {
    const payload = componentPayloads?.[component.id] || null
    const direction = seasonalDirection(payload, horizon)
    let state = 'unavailable'

    if (direction === 'Unavailable') {
      unavailableWeight += component.weight
    } else if (direction === 'Mixed' || expectedCurrencyDirection === 'Mixed') {
      state = 'mixed'
      mixedWeight += component.weight
    } else if (direction === expectedCurrencyDirection) {
      state = 'agree'
      agreeingWeight += component.weight
    } else {
      state = 'conflict'
      conflictingWeight += component.weight
    }

    return {
      ...component,
      direction,
      expectedDirection: expectedCurrencyDirection,
      state,
    }
  })

  const directionalWeight = agreeingWeight + conflictingWeight
  const agreementPct = directionalWeight > 0 ? (agreeingWeight / directionalWeight) * 100 : null
  const directionalCoveragePct = directionalWeight * 100
  const dataCoveragePct = (DXY_TRACKED_COMPONENT_WEIGHT - unavailableWeight) * 100

  let verdict = 'INSUFFICIENT'
  if (dxyDirection === 'Mixed' || dxyDirection === 'Unavailable') {
    verdict = 'DXY MIXED'
  } else if (directionalCoveragePct < 40) {
    verdict = 'LOW COVERAGE'
  } else if (agreementPct >= 75) {
    verdict = 'CONFIRMED'
  } else if (agreementPct >= 55) {
    verdict = 'PARTIAL'
  } else {
    verdict = 'CONTRADICTION'
  }

  return {
    horizon,
    dxyDirection,
    expectedCurrencyDirection,
    components,
    agreementPct,
    directionalCoveragePct,
    dataCoveragePct,
    agreeingWeight,
    conflictingWeight,
    mixedWeight,
    unavailableWeight,
    verdict,
  }
}

export function buildCrossMarketLookbackResult({ dxyPayload, componentPayloads, lookback }) {
  const horizons = CROSS_MARKET_HORIZONS.map((horizon) =>
    buildCrossMarketHorizonResult({ dxyPayload, componentPayloads, horizon }),
  )

  const scoreable = horizons.filter((row) => row.agreementPct != null && row.directionalCoveragePct >= 40)
  const averageAgreementPct = scoreable.length
    ? scoreable.reduce((sum, row) => sum + row.agreementPct, 0) / scoreable.length
    : null
  const averageCoveragePct = horizons.length
    ? horizons.reduce((sum, row) => sum + row.directionalCoveragePct, 0) / horizons.length
    : 0

  const confirmedCount = horizons.filter((row) => row.verdict === 'CONFIRMED').length
  const contradictionCount = horizons.filter((row) => row.verdict === 'CONTRADICTION').length

  let verdict = 'INSUFFICIENT'
  if (contradictionCount >= 2) verdict = 'CONTRADICTION'
  else if (confirmedCount >= 2) verdict = 'CONFIRMED'
  else if (scoreable.length) verdict = 'MIXED'

  return {
    lookback,
    horizons,
    averageAgreementPct,
    averageCoveragePct,
    verdict,
  }
}

export function buildCrossMarketStability(resultsByLookback) {
  const rows = CROSS_MARKET_LOOKBACKS.map((lookback) => resultsByLookback?.[lookback]).filter(Boolean)
  if (!rows.length) return { verdict: 'UNAVAILABLE', stable: false, confirmed: 0, contradictions: 0 }

  const confirmed = rows.filter((row) => row.verdict === 'CONFIRMED').length
  const contradictions = rows.filter((row) => row.verdict === 'CONTRADICTION').length
  const stable = confirmed === rows.length || contradictions === rows.length

  return {
    verdict: stable ? (confirmed === rows.length ? 'STABLE CONFIRMATION' : 'STABLE CONTRADICTION') : 'LOOKBACKS DISAGREE',
    stable,
    confirmed,
    contradictions,
    total: rows.length,
  }
}
