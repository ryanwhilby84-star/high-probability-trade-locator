#!/usr/bin/env node
/**
 * Verify dashboard positioning chart sources and final plotted points.
 * Run: node scripts/verify_dashboard_chart_render.mjs
 */
import { readFileSync } from 'node:fs'
import { fileURLToPath } from 'node:url'
import path from 'node:path'
import {
  buildCotWorkstation,
  COT_WS_DEFAULT_WEEKS,
  presetRange,
  sliceCotWorkstationRange,
} from '../src/cot/buildCotWorkstation.js'
import { enrichChartAnalytics } from '../src/charts/chartAnalytics.js'

const root = path.dirname(path.dirname(fileURLToPath(import.meta.url)))
const chartFile = path.join(root, 'public/data/cot_3y_series_latest.json')
const doc = JSON.parse(readFileSync(chartFile, 'utf8'))

const instruments = [
  { label: 'Gold', id: 'Gold' },
  { label: 'EURUSD', id: 'Euro FX / 6E' },
  { label: 'Soybeans', id: 'Soybeans' },
  { label: 'Copper', id: 'Copper / HG' },
  { label: 'USDCHF', id: 'Swiss Franc / 6S' },
]

const TARGET_WEEK = '2026-06-16'

console.log('Chart JSON (browser):', chartFile)
console.log('HTTP path (Vite dev): /data/cot_3y_series_latest.json')
console.log('generated_at:', doc.generated_at)
console.log('')

let allPass = true

for (const { label, id } of instruments) {
  const block = doc.markets?.[id]
  console.log(`=== ${label} (${id}) ===`)
  if (!block) {
    console.log('FAIL — missing from cot_3y export')
    allPass = false
    continue
  }
  const series = block.series || []
  const last2 = series.slice(-2)
  console.log('file last 2 rows:')
  for (const row of last2) {
    console.log(
      `  ${row.date} commercial=${row.commercial_net} NR=${row.retail_net} NC=${row.institutional_net} price=${row.price}`,
    )
  }

  const model = buildCotWorkstation(block)
  const enriched = enrichChartAnalytics(model.series)
  const range = presetRange(enriched.length, COT_WS_DEFAULT_WEEKS)
  const visible = sliceCotWorkstationRange(enriched, range.startIndex, range.endIndex)
  const plotted = visible.at(-1)
  const pass = plotted?.date === TARGET_WEEK
  if (!pass) allPass = false
  console.log('frontend final plotted point:')
  console.log(
    `  ${plotted?.date} commercial=${plotted?.commercial_net} NR=${plotted?.retail_net} NC=${plotted?.institutional_net} price=${plotted?.price}`,
  )
  console.log(pass ? 'PASS' : `FAIL — expected last plotted ${TARGET_WEEK}`)
  console.log('')
}

process.exit(allPass ? 0 : 1)
