/** Compare COT workstation price vs seasonality — must share canonical timeline. */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export function auditPriceSourceContract(cotBlock, seasonBlock, marketId) {
  const cotSeries = (cotBlock?.series || []).filter((p) => isNum(p?.price))
  const cotAudit = cotBlock?.price_audit || {}
  const cotStore = cotAudit.price_store_key || cotBlock?.price_store_key || marketId
  const seaStore = seasonBlock?.price_store_key || marketId

  const cotCanonical = cotAudit.canonical_source || null
  const seaCanonical = seasonBlock?.canonical_source || null
  const cotSymbol = cotAudit.canonical_symbol || null
  const seaSymbol = seasonBlock?.canonical_symbol || null

  const cotLatest = cotSeries[cotSeries.length - 1] || null

  const cotPanel = {
    sourceFile: 'cot_3y_series_latest.json',
    instrumentKey: marketId,
    priceField: 'series[].price',
    underlyingFile: 'canonical_price_timeline (prices_latest.json)',
    priceStoreKey: cotStore,
    canonicalSource: cotCanonical,
    canonicalSymbol: cotSymbol,
    barCadence: cotAudit.cot_match_method || 'canonical_daily_match_as_of_cot_date',
    dateStart: cotSeries[0]?.date?.slice(0, 10) || null,
    dateEnd: cotSeries[cotSeries.length - 1]?.date?.slice(0, 10) || null,
    barCount: cotSeries.length,
    latestDate: cotLatest?.date?.slice(0, 10) || null,
    latestPrice: cotLatest?.price ?? null,
    priceDate: (cotLatest?.price_date || cotLatest?.date || '').slice(0, 10) || null,
    transformation: 'Raw close from canonical daily — matched as-of each COT report date.',
    proxy: cotAudit.proxy,
    proxyExplanation: cotAudit.proxy_explanation,
  }

  const tl = (seasonBlock?.timeline_series || []).filter((r) => !r?.is_projection && isNum(r?.price))
  const seaLatest = tl[tl.length - 1] || seasonBlock?.latest_price || null

  const seaPanel = {
    sourceFile: 'seasonality_price_latest.json',
    instrumentKey: marketId,
    priceField: 'timeline_series[].price / weekly derivation',
    underlyingFile: 'canonical_price_timeline (prices_latest.json)',
    priceStoreKey: seaStore,
    canonicalSource: seaCanonical,
    canonicalSymbol: seaSymbol,
    barSource: seasonBlock?.price_derivation || seasonBlock?.bar_source || '—',
    barCadence: 'derived_iso_week_end_from_canonical_daily',
    dateStart: seasonBlock?.timeline_start || tl[0]?.date || null,
    dateEnd: seasonBlock?.timeline_end || tl[tl.length - 1]?.date || null,
    barCount: tl.length || seasonBlock?.weekly_bars_count,
    latestDate: seaLatest?.date || seasonBlock?.latest_price?.date || null,
    latestPrice: seaLatest?.price ?? seaLatest?.close ?? null,
    transformation: 'Weekly closes derived from same canonical daily; seasonality line is indexed.',
    proxy: seasonBlock?.proxy,
    proxyExplanation: seasonBlock?.proxy_explanation,
  }

  const reasons = []
  if (seaStore !== cotStore) {
    reasons.push(`Different store keys: COT "${cotStore}" vs seasonality "${seaStore}".`)
  }
  if (cotCanonical && seaCanonical && cotCanonical !== seaCanonical) {
    reasons.push(`Different canonical sources: COT "${cotCanonical}" vs seasonality "${seaCanonical}".`)
  }
  if (cotAudit.proxy || seasonBlock?.proxy) {
    reasons.push(
      cotAudit.proxy_explanation ||
        seasonBlock?.proxy_explanation ||
        'Proxy price in use — see panel details.',
    )
  }
  if (!reasons.length) {
    reasons.push(
      'All panels trace to the same canonical daily timeline. COT samples as-of report dates; seasonality uses ISO week-end derivation from the same daily series.',
    )
  }

  const proxy = Boolean(cotAudit.proxy || seasonBlock?.proxy || seaStore !== marketId || cotStore !== marketId)
  let status = 'ALIGNED'
  if (seaStore !== cotStore || (cotCanonical && seaCanonical && cotCanonical !== seaCanonical)) {
    status = 'MISMATCH'
  } else if (proxy) {
    status = 'PROXY'
  }

  const disclosure =
    status === 'MISMATCH'
      ? `Price paths use different canonical sources or store keys. COT: ${cotStore} (${cotCanonical}). Seasonality: ${seaStore} (${seaCanonical}).`
      : status === 'PROXY'
        ? cotAudit.proxy_explanation ||
          seasonBlock?.proxy_explanation ||
          `Proxy price used for ${marketId}.`
        : `One canonical daily timeline (${cotCanonical || 'price_store'} · ${cotSymbol || cotStore}). COT chart: as-of COT dates. Seasonality: ${seaPanel.barSource} from same daily.`

  return {
    status,
    cotPanel,
    seaPanel,
    reasons,
    disclosure,
    timelineHidden: status === 'MISMATCH',
  }
}
