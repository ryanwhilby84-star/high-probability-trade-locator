import test from 'node:test'
import assert from 'node:assert/strict'
import { mergeHistoricalPrices } from './mergeHistoricalPrices.js'

test('matches most recent daily close to COT Tuesday without altering positioning', () => {
  const series = [{ date: '2026-10-27', price: null, institutional_net: 123 }]
  const daily = [{ date: '2026-10-26', close: 100 }, { date: '2026-10-27', close: 102 }, { date: '2026-10-28', close: 110 }]
  assert.deepEqual(mergeHistoricalPrices(series, daily), [
    { date: '2026-10-27', price: 102, price_date: '2026-10-27', institutional_net: 123 },
  ])
})

test('does not invent candles across gaps longer than seven days', () => {
  const series = [{ date: '2026-10-27', price: null }]
  const daily = [{ date: '2026-10-10', close: 100 }]
  assert.deepEqual(mergeHistoricalPrices(series, daily), series)
})

test('preserves COT bundle when daily history is unavailable', () => {
  const series = [{ date: '2026-10-27', price: 99 }]
  assert.equal(mergeHistoricalPrices(series, []), series)
})
