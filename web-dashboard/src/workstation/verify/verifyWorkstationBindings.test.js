import { readFileSync } from 'node:fs'
import { dirname, join } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'

import { buildCotWorkstation } from '../../cot/buildCotWorkstation.js'
import { buildPositioningWorkstationSeries } from '../data/buildPositioningWorkstationSeries.js'
import { filterValidCandleBars } from '../safeWorkstationSeries.js'

const __dir = dirname(fileURLToPath(import.meta.url))
const root = join(__dir, '../../../../..')

function loadJson(rel) {
  return JSON.parse(readFileSync(join(root, rel), 'utf-8'))
}

const INSTRUMENTS = ['Sugar', 'Gold', 'Australian Dollar / 6A', 'NASDAQ / NQ']

function verifyInstrument(name, cotDoc, ohlcDoc) {
  const block = cotDoc.markets?.[name]
  const exportBlock = ohlcDoc.instruments?.[name]
  const model = buildCotWorkstation(block)
  const binding = buildPositioningWorkstationSeries(model, null, exportBlock)
  const bars = filterValidCandleBars(binding.weeklyBars)

  const candleDates = bars.map((b) => b.date)
  const uniqueDates = new Set(candleDates)
  expect(uniqueDates.size).toBe(candleDates.length)

  const ohlcByCot = []
  let prevOhlcDate = null
  for (const row of binding.rows.slice(-12)) {
    const bar = bars.find((b) => b.date === row.date)
    if (bar) {
      expect(bar.date).not.toBe(prevOhlcDate)
      prevOhlcDate = bar.date
      ohlcByCot.push({ cot: row.date, ohlc: bar.date })
    }
  }

  const tailMatched = (exportBlock?.tail_alignment_audit?.final_12_matched || []).filter((m) => m.matched)
  for (const m of tailMatched) {
    const candle = bars.find((b) => b.date === m.cot_date)
    if (candle) {
      expect(candle.close).toBe(m.close)
    }
  }

  return { bars: bars.length, tail: ohlcByCot.slice(-5) }
}

describe('workstation binding verification', () => {
  const cotDoc = loadJson('web-dashboard/public/data/cot_3y_series_latest.json')
  const ohlcDoc = loadJson('data/processed/workstation_ohlc_latest.json')

  for (const name of INSTRUMENTS) {
    it(`${name} — no duplicate/stale candles in binding`, () => {
      const result = verifyInstrument(name, cotDoc, ohlcDoc)
      expect(result.bars).toBeGreaterThan(0)
    })
  }
})

describe('workstation drawings', () => {
  it('persists drawings to localStorage key', async () => {
    const { workstationDrawingStorageKey } = await import('../canvas/workstationDrawingTypes.js')
    const key = workstationDrawingStorageKey('Sugar')
    expect(key).toContain('Sugar')
  })

  it('drawingsForWorkstationPanel includes global vlines', async () => {
    const { drawingsForWorkstationPanel } = await import('../canvas/workstationDrawingTypes.js')
    const drawings = [
      { id: '1', type: 'vline', time: 1, date: '2026-01-01' },
      { id: '2', type: 'hline', panelId: 'commercial', value: 100 },
      { id: '3', type: 'rect', panelId: 'institutional', valueTop: 1, valueBottom: 0 },
    ]
    const commercial = drawingsForWorkstationPanel(drawings, 'commercial')
    expect(commercial.some((d) => d.type === 'vline')).toBe(true)
    expect(commercial.some((d) => d.type === 'hline')).toBe(true)
    expect(commercial.some((d) => d.type === 'rect')).toBe(false)

    const inst = drawingsForWorkstationPanel(drawings, 'institutional')
    expect(inst.some((d) => d.type === 'vline')).toBe(true)
    expect(inst.some((d) => d.type === ' 'rect')).toBe(true)
  })
})
