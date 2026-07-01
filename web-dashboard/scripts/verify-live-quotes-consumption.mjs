/**
 * Verify live_quotes_latest.json consumption for Gold and NASDAQ / NQ.
 * Mirrors dashboard hooks: useLiveQuotes + buildPriceContextFromSources + readIVE merge.
 */
import fs from 'node:fs'
import path from 'node:path'
import { fileURLToPath } from 'node:url'

import { buildPriceContextFromSources } from '../src/workstation/data/instrumentPriceDiagnostics.js'
import { readIVE } from '../src/valuation/iveDisplay.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const publicDir = path.join(__dirname, '..', 'public', 'data')

function loadJson(name) {
  const p = path.join(publicDir, name)
  if (!fs.existsSync(p)) throw new Error(`Missing ${p}`)
  return JSON.parse(fs.readFileSync(p, 'utf8'))
}

function mergeIveFromLiveQuote(base, liveQuote) {
  if (!base || liveQuote?.live_price == null) return base
  if (base.valuationPriceSource || base.livePrice != null) return base
  const live = Number(liveQuote.live_price)
  if (!Number.isFinite(live)) return base
  const fair = base.fairValue
  const displayPct =
    Number.isFinite(fair) && fair > 0
      ? Math.round(((100 * (live - fair)) / fair) * 100) / 100
      : base.valuationPct
  return {
    ...base,
    livePrice: live,
    livePriceSource: liveQuote.live_price_source ?? null,
    livePriceAsOf: liveQuote.live_price_as_of ?? null,
    currentPrice: live,
    valuationPct: displayPct,
    valuationPriceSource: 'live/latest',
  }
}

function lastVisibleBarFromExport(exportBlock) {
  const matched = exportBlock?.tail_alignment_audit?.final_12_matched || []
  const last = [...matched].reverse().find((m) => m.matched && m.close != null)
  if (!last) return null
  return { date: last.ohlc_date, close: last.close, cotDate: last.cot_date }
}

const INSTRUMENTS = ['Gold', 'NASDAQ / NQ']
const liveDoc = loadJson('live_quotes_latest.json')
const wsDoc = loadJson('workstation_ohlc_latest.json')
const valDoc = loadJson('valuation_latest.json')

console.log('live_quotes_latest.json generated_at:', liveDoc.generated_at)
console.log('workstation_ohlc_latest.json generated_at:', wsDoc.generated_at)
console.log('valuation_latest.json generated_at:', valDoc.generated_at)
console.log('')

let failures = 0

for (const marketId of INSTRUMENTS) {
  console.log('='.repeat(60))
  console.log(marketId)

  const liveQuote = liveDoc.instruments?.[marketId]
  const exportBlock = wsDoc.instruments?.[marketId]
  const valuationBlock = valDoc.instruments?.[marketId]

  if (!liveQuote) {
    console.error('FAIL: no live_quotes block')
    failures++
    continue
  }
  if (!liveQuote.live_fetch_ok || liveQuote.live_price == null) {
    console.error('FAIL: live quote not fetched', liveQuote.live_fetch_error)
    failures++
  }

  const bar = lastVisibleBarFromExport(exportBlock)
  const visibleBars = bar ? [{ date: bar.date, close: bar.close }] : []

  const priceContext = buildPriceContextFromSources({
    marketId,
    ohlcSourceLabel: exportBlock?.price_source,
    exportBlock,
    liveQuote,
    visibleBars,
    valuationBlock,
  })

  const baseIve = readIVE(valuationBlock)
  const ive =
    marketId === 'Gold' ? mergeIveFromLiveQuote(baseIve, liveQuote) : mergeIveFromLiveQuote(baseIve, liveQuote)

  const checks = [
    {
      name: 'chart close is completed weekly OHLC',
      ok: priceContext.chartClose != null && priceContext.chartClose === bar?.close,
      detail: `chartClose=${priceContext.chartClose} bar=${bar?.close}`,
    },
    {
      name: 'live price from OANDA quote',
      ok: priceContext.livePrice === liveQuote.live_price && liveQuote.live_price_source?.startsWith('oanda:'),
      detail: `live=${priceContext.livePrice} source=${priceContext.livePriceSource}`,
    },
    {
      name: 'valuation price source is live/latest',
      ok: priceContext.valuationPriceSource === 'live/latest',
      detail: priceContext.valuationPriceSource,
    },
    {
      name: 'valuation price used equals live quote',
      ok: priceContext.valuationPriceUsed === liveQuote.live_price,
      detail: `used=${priceContext.valuationPriceUsed}`,
    },
    {
      name: 'IVE uses live currentPrice when export lacks display fields',
      ok: ive?.currentPrice === liveQuote.live_price || ive?.valuationPriceSource === 'live/latest',
      detail: `currentPrice=${ive?.currentPrice} valuationPriceSource=${ive?.valuationPriceSource}`,
    },
  ]

  if (valuationBlock?.display_current_price != null) {
    checks.push({
      name: 'IVE prefers display_current_price from export',
      ok: ive?.currentPrice === valuationBlock.display_current_price || ive?.currentPrice === valuationBlock.valuation_price_used,
      detail: `display_current_price=${valuationBlock.display_current_price}`,
    })
  }

  const consolePayload = {
    instrument: marketId,
    historical_ohlc_source: priceContext.historicalOhlcSource,
    latest_completed_ohlc_close: priceContext.latestCompletedOhlcClose,
    latest_completed_ohlc_date: priceContext.latestCompletedOhlcDate,
    chart_close: priceContext.chartClose,
    chart_close_date: priceContext.chartCloseDate,
    live_price: priceContext.livePrice,
    live_price_source: priceContext.livePriceSource,
    live_price_as_of: priceContext.livePriceAsOf,
    valuation_price_used: priceContext.valuationPriceUsed,
    valuation_price_source: priceContext.valuationPriceSource,
  }
  console.log('[price-context]', marketId, consolePayload)

  for (const c of checks) {
    console.log(c.ok ? 'PASS' : 'FAIL', '-', c.name, `(${c.detail})`)
    if (!c.ok) failures++
  }
  console.log('')
}

console.log(failures ? `RESULT: ${failures} failure(s)` : 'RESULT: ALL PASS')
process.exit(failures ? 1 : 0)
