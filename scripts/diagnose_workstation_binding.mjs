import fs from 'fs'
import path from 'path'
import { fileURLToPath } from 'url'

import { buildCotWorkstation } from '../web-dashboard/src/cot/buildCotWorkstation.js'
import { getInstrumentPrices } from '../web-dashboard/src/priceData.js'
import {
  buildPositioningWorkstationSeries,
} from '../web-dashboard/src/workstation/data/buildPositioningWorkstationSeries.js'
import {
  diagnoseWorkstationBinding,
} from '../web-dashboard/src/workstation/data/workstationBindingDiagnostics.js'

const __dirname = path.dirname(fileURLToPath(import.meta.url))
const root = path.resolve(__dirname, '..')

const marketId = process.argv[2] || 'NASDAQ / NQ'

const cotPath = path.join(root, 'web-dashboard/public/data/cot_3y_series_latest.json')
const pricesPath = path.join(root, 'web-dashboard/public/data/prices_latest.json')

const cot = JSON.parse(fs.readFileSync(cotPath, 'utf8'))
const prices = JSON.parse(fs.readFileSync(pricesPath, 'utf8'))
const block = cot.markets[marketId]
const model = buildCotWorkstation(block)
const priceRec = getInstrumentPrices(prices, marketId)
const bound = buildPositioningWorkstationSeries(model, priceRec)
const diag = diagnoseWorkstationBinding(marketId, { cotBlock: block, priceStore: prices })

const outDir = path.join(root, 'data/audits')
fs.mkdirSync(outDir, { recursive: true })

const report = {
  generated_at: new Date().toISOString(),
  market: marketId,
  diagnostic: diag,
  binding_meta: bound.meta,
  sample_prices: {
    first: bound.rows[0],
    mid: bound.rows[Math.floor(bound.rows.length / 2)],
    last: bound.rows[bound.rows.length - 1],
  },
  notes: [
    'Workstation binds one unified row per COT week (price OHLC + positioning nets).',
    'prices_latest weekly OHLC is used only when coverage and scale match COT prices.',
    'NASDAQ / NQ: store currently holds Alpha Vantage QQQ-scale (~746) with 52 weeks; COT uses FRED NASDAQCOM history with scale break at 2025-06-17.',
    'Canonical NQ futures (OANDA NAS100USD ~29k) requires price store export fix — not changed in this pass.',
  ],
}

const jsonPath = path.join(outDir, 'workstation_binding_nasdaq_latest.json')
const mdPath = path.join(outDir, 'workstation_binding_nasdaq_latest.md')

fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2))

const md = `# Workstation binding diagnostic — ${marketId}

Generated: ${report.generated_at}

## Row counts

| Dataset | Count |
|---------|------:|
| Weekly price bars (bound) | ${diag.weeklyPriceBars} |
| Unified workstation rows | ${diag.unifiedRows} |
| COT series weeks | ${diag.cotSeriesWeeks} |
| Commercial points | ${diag.commercialPoints} |
| Non-Commercial points | ${diag.nonCommercialPoints} |
| Non-Reportable points | ${diag.nonReportablePoints} |
| Price (close) points | ${diag.pricePoints} |

## Dates & prices

| | First | Last |
|--|-------|------|
| Date | ${diag.firstDate} | ${diag.lastDate} |
| Close | ${diag.firstClose} | ${diag.lastClose} |
| Commercial net | ${diag.firstCommercial} | ${diag.lastCommercial} |

## Alignment

- **Timestamps match exactly:** ${diag.timestampsMatchExactly ? 'yes' : 'no'}
- **Price source:** ${diag.priceSource}
- **Store price mid (prices_latest):** ${diag.storePriceMid}
- **Scale break date:** ${diag.bindingMeta.scaleBreakDate ?? '—'}
- **Use store OHLC:** ${diag.bindingMeta.useStoreOhlc}

## Notes

${report.notes.map((n) => `- ${n}`).join('\n')}
`

fs.writeFileSync(mdPath, md)
console.log('Wrote', jsonPath)
console.log('Wrote', mdPath)
console.log(JSON.stringify(diag, null, 2))
