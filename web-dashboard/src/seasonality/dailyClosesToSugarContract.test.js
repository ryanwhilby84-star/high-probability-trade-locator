import { describe, expect, it } from 'vitest'
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
    expect(() => dailyClosesToSugarContract([{date:'2025-06-01',close:100},{date:'2025-06-02',close:101}])).toThrow()
  })
  it('rejects duplicate dates', () => {
    expect(() => dailyClosesToSugarContract([{date:'2025-01-01',close:100},{date:'2025-01-01',close:101}])).toThrow()
  })
  it('excludes partial calendar years from the sample', () => {
    const data = years(2010, 2025).filter(b => b.date >= '2010-06-01')
    const archive = dailyClosesToSugarContract(data)
    expect(archive.full_years).not.toContain(2010)
    expect(archive.full_years).toContain(2011)
    expect(archive.full_years).toContain(2025)
  })
  it('feeds the original Sugar engine without changing its return arithmetic', () => {
    const archive = prepareSugarArchive(dailyClosesToSugarContract(years(2005, 2025)))
    const result = evaluateSugarWindow(archive, '03-01', '03-22', { strict: true, direction: 'long' })
    expect(result.error).toBeUndefined()
    expect(result.n).toBeGreaterThanOrEqual(15)
    expect(result.hitRate).toBe(100)
    expect(findSugarWindows(archive, '03-01', {minYears:15}).length).toBeGreaterThan(0)
  })
  it('flags discontinuities rather than inventing missing sessions', () => {
    const bars = years(2020, 2025).filter(b => b.date !== '2022-06-14' && b.date !== '2022-06-15' && b.date !== '2022-06-16')
    const archive = prepareSugarArchive(dailyClosesToSugarContract(bars))
    const result = evaluateSugarWindow(archive, '06-01', '06-30', { strict: true })
    expect(result.exclusions.some(x => x.year === 2022 && x.reason === 'quality_flag')).toBe(true)
  })
})
