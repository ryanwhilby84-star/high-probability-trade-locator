import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { prepareSugarArchive, evaluateSugarWindow, findSugarWindows, scanSugarCalendar } from './sugarArchiveWindows.js'

const archive = prepareSugarArchive(JSON.parse(fs.readFileSync(new URL('../../public/data/sugar_archive_daily.json', import.meta.url), 'utf8')))

test('archive date windows compound same-contract returns and exclude entry-day return', () => {
  const r = evaluateSugarWindow(archive, '10-03', '10-10', { strict: false })
  assert.equal(r.n, 41)
  for (const outcome of r.outcomes) {
    const selected = archive.rows.filter((x) => x.date > outcome.entry && x.date <= outcome.exit)
    const expected = (selected.reduce((p, x) => p * (1 + x.value), 1) - 1) * 100
    assert(Math.abs(outcome.returnPct - expected) < 1e-12)
    assert.equal(selected[0].previous, outcome.entry)
    assert.equal(selected.at(-1).date, outcome.exit)
  }
})
test('quality filter excludes whole windows, preserving all-observation comparison', () => {
  const r = evaluateSugarWindow(archive, '10-03', '10-10')
  assert.equal(r.all.n, 41)
  assert.equal(r.n, 32)
  assert.equal(r.hits, 22)
  assert.equal(r.n + r.exclusions.length, 41)
  assert(r.outcomes.every((x) => x.flagged === 0))
})
test('year wrap excludes partial 2002 and short returns reverse the long direction', () => {
  const long = evaluateSugarWindow(archive, '12-15', '01-15', { strict: false })
  const short = evaluateSugarWindow(archive, '12-15', '01-15', { strict: false, direction: 'short' })
  assert.equal(long.n, 40)
  assert(long.exclusions.some((x) => x.year === 2001 && x.reason === 'partial_final_year'))
  assert(Math.abs(long.median + short.median) < 1e-10)
  assert(long.outcomes.every((x) => x.exit.slice(0, 4) === String(x.year + 1)))
})
test('missing intervals reject an entire year rather than skipping its return', () => {
  const row = archive.rows.find((x) => x.date > '1990-10-03' && x.date < '1990-10-10')
  const damaged = { ...archive, rows: archive.rows.filter((x) => x !== row) }
  const r = evaluateSugarWindow(damaged, '10-03', '10-10', { strict: false })
  assert(r.exclusions.some((x) => x.year === 1990 && x.reason === 'missing_interval'))
  assert.equal(r.n, 40)
})
test('window search honours minimum sample and rejects zero-length windows', () => {
  assert(evaluateSugarWindow(archive, '10-03', '10-03').error)
  const windows = findSugarWindows(archive, '10-03')
  assert(windows.length > 0)
  assert(windows.every((x) => x.n >= 15))
  assert(windows.every((x, i) => i === 0 || x.hitRate <= windows[i - 1].hitRate))
})

test('ranked Sugar candidates do not double-count heavily overlapping calendar windows', () => {
  const windows = findSugarWindows(archive, '10-03')
  const day = (md) => Date.parse(`2001-${md}T00:00:00Z`)
  for (let i = 0; i < windows.length; i++) {
    for (let j = i + 1; j < windows.length; j++) {
      const a = windows[i], b = windows[j]
      const a0 = day(a.start), b0 = day(b.start)
      const a1 = a0 + a.calendarDays * 86400000
      const b1 = b0 + b.calendarDays * 86400000
      const overlap = Math.max(0, Math.min(a1, b1) - Math.max(a0, b0))
      assert(overlap / Math.min(a1 - a0, b1 - b0) < 0.75, `duplicate windows: ${a.start}-${a.end} and ${b.start}-${b.end}`)
    }
  }
})

test('full calendar scanner returns ranked, qualified directional windows', () => {
  const windows = scanSugarCalendar(archive, { stepDays: 14, minYears: 15, minHitRate: 60, limit: 12 })
  assert(windows.length > 0)
  assert(windows.length <= 12)
  for (const w of windows) {
    assert(['long', 'short'].includes(w.direction))
    assert(w.n >= 15)
    assert(w.hitRate >= 60)
    assert(w.median > 0)
    assert(w.hits <= w.n)
  }
  for (let i = 1; i < windows.length; i++) assert(windows[i - 1].hitRate >= windows[i].hitRate)
})
