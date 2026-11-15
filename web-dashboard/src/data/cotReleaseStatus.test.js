import { test } from 'node:test'
import assert from 'node:assert/strict'
import { CFTC_RELEASE_DATES_2026, cotReleaseStatus } from './cotReleaseStatus.js'

test('official 2026 release calendar contains 52 dates', () => {
  assert.equal(CFTC_RELEASE_DATES_2026.length, 52)
})
test('Nov 3 data is valid all Sunday Nov 15', () => {
  assert.equal(cotReleaseStatus('2026-11-03', new Date('2026-11-15T17:11:00Z')).needsRefresh, false)
})
test('Veterans Day delay: new snapshot due Monday 16 Nov at 20:30 UTC', () => {
  assert.equal(cotReleaseStatus('2026-11-03', new Date('2026-11-16T20:29:00Z')).needsRefresh, false)
  assert.deepEqual(cotReleaseStatus('2026-11-03', new Date('2026-11-16T20:30:00Z')), {
    needsRefresh: true, expectedReportDate: '2026-11-10', releaseDate: '2026-11-16',
  })
})
test('new certified data clears pending warning', () => {
  assert.equal(cotReleaseStatus('2026-11-10', new Date('2026-11-16T22:00:00Z')).needsRefresh, false)
})
test('Thanksgiving delayed November 30 release', () => {
  assert.equal(cotReleaseStatus('2026-11-17', new Date('2026-11-29T22:00:00Z')).needsRefresh, false)
  assert.equal(cotReleaseStatus('2026-11-17', new Date('2026-11-30T20:31:00Z')).needsRefresh, true)
})
test('never disable historical data for an unknown calendar', () => {
  assert.equal(cotReleaseStatus('2026-12-22', new Date('2027-01-03T12:00:00Z')).needsRefresh, false)
})
