import { describe, it } from 'node:test'
import assert from 'node:assert/strict'
import { dailyClosesToSugarContract } from './dailyClosesToSugarContract.js'
import { evaluateSugarWindow, prepareSugarArchive, findSugarWindows } from './sugarArchiveWindows.js'

function years(start, end) {
  const rows = []
  for (let y = start; y <= end; y++) {
    for (let d = Date.UTC(y, 0, 1); d <= Date.UTC(y, 11, 31); d += 86400000) {
      const date = new Date(d).toISOString().slice(0, 10)
      if ([0, 6].includes(new Date(d).getUTCDay())) continue
      rows.push({ date, close: 100 + (d - Date.UTC(start, 0, 1)) / 86400000 * 0.01 })
    }
  }
  return rows
}
describe('26-market Sugar-compatible daily adapter', () => {
  it('rejects a daily series with no full year', () => {
    assert.throws(() => dailyClosesToSugarContract([{date:'2025-06-01',close:100},{date:'2025-06-02',close:101}]))
  })
  it('rejects duplicate dates', () => {
    assert.throws(() => dailyClosesToSugarContract([{date:'2025-01-01',close:100},{date:'2025-01-01',close:101}]))
  })
  it('excludes partial calendar years from the sample', () => {
    const data = years(2010, 2025).filter(b => b.date >= '2010-06-01')
    const archive = dailyClosesToSugarContract(data)
    assert(!archive.full_years.includes(2010))
    assert(archive.full_years.includes(2011))
    assert(archive.full_years.includes(2025))
  })
  it('feeds the original Sugar engine without changing its return arithmetic', () => {
    const archive = prepareSugarArchive(dailyClosesToSugarContract(years(2005, 2025)))
    const result = evaluateSugarWindow(archive, '03-01', '03-22', { strict: true, direction: 'long' })
    assert.equal(result.error, undefined)
    assert(result.n >= 15)
    assert.equal(result.hitRate, 100)
    assert(findSugarWindows(archive, '03-01', {minYears:15}).length > 0)
  })
  it('flags discontinuities rather than inventing missing sessions', () => {
    const bars = years(2020, 2025).filter(b => b.date !== '2022-06-14' && b.date !== '2022-06-15' && b.date !== '2022-06-16')
    const archive = prepareSugarArchive(dailyClosesToSugarContract(bars))
    const result = evaluateSugarWindow(archive, '06-01', '06-30', { strict: true })
    assert(result.exclusions.some(x => x.year === 2022 && x.reason === 'quality_flag'))
  })
})
