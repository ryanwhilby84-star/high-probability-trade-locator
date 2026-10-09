import test from 'node:test'
import assert from 'node:assert/strict'
import { selectDistinctWindows, validationPass } from './seasonalWindowSelection.js'

test('nearby same-direction windows group, but opposing and subsequent phases remain', () => {
  const edges = selectDistinctWindows([
    { window: '26 Oct–4 Nov', direction: 'Bullish', days_until_start: 19, end_offset: 28 },
    { window: '27 Oct–4 Nov', direction: 'Bullish', days_until_start: 20, end_offset: 28 },
    { window: '14 Oct–17 Nov', direction: 'Bearish', days_until_start: 7, end_offset: 41 },
    { window: '15 Oct–17 Nov', direction: 'Bearish', days_until_start: 8, end_offset: 41 },
    { window: '4 Nov–11 Nov', direction: 'Bearish', days_until_start: 28, end_offset: 35 },
  ])
  assert.equal(edges.length, 3)
  assert.equal(edges[0].variant_count, 2)
  assert.equal(edges[1].variant_count, 2)
  assert.equal(edges[1].opposing_windows.length, 1)
  assert.equal(edges[2].opposing_windows.length, 0)
})

test('later-year validation needs sample, frequency, mean and median agreement', () => {
  const stats = { n: 10, frequency: .7, mean: 1, median: 1 }
  assert.equal(validationPass(stats, 'Bullish'), true)
  assert.equal(validationPass({ ...stats, mean: -1 }, 'Bullish'), false)
  assert.equal(validationPass({ ...stats, median: -1 }, 'Bullish'), false)
  assert.equal(validationPass({ ...stats, n: 5 }, 'Bullish'), false)
  assert.equal(validationPass({ ...stats, frequency: .6 }, 'Bullish'), false)
})
