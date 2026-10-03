import test from 'node:test'
import assert from 'node:assert/strict'
import fs from 'node:fs'
import { prepareSugarArchive, evaluateSugarWindow } from './sugarArchiveWindows.js'
import { buildSugarArchiveEdges, archiveDateLabel } from './sugarArchiveEdges.js'
const archive = prepareSugarArchive(JSON.parse(fs.readFileSync(new URL('../../public/data/sugar_archive_daily.json', import.meta.url))))
test('existing cards receive archive counts and signed price moves for both directions', () => {
  const edges = buildSugarArchiveEdges(archive, new Date(2026,9,3))
  assert.ok(edges.some(e => e.direction === 'Bearish'))
  assert.ok(edges.some(e => e.direction === 'Bullish'))
  for (const edge of edges) {
    const stats = edge.lookbacks.ARCHIVE
    assert.equal(stats.n + edge.exclusions.length, 41)
    assert.equal(stats.returns.length, stats.n)
    assert.equal(stats.directional_frequency, (edge.direction === 'Bullish' ? stats.bullish : stats.bearish) / stats.n)
    assert.ok(edge.direction === 'Bullish' ? stats.median_pct > 0 && stats.mean_pct > 0 : stats.median_pct < 0 && stats.mean_pct < 0)
    assert.equal(edge.lookbacks['15Y'], undefined)
    assert.match(edge.window, /\d{2} [A-Z][a-z]{2} → \d{2} [A-Z][a-z]{2}/)
  }
})
test('adapter preserves established October calculation and readable dates', () => {
  const result = evaluateSugarWindow(archive, '10-03', '10-31')
  assert.equal(result.n, 28); assert.equal(result.hits, 19)
  assert.ok(Math.abs(result.median - 2.337723006) < .00001)
  assert.equal(archiveDateLabel('10-03'), '03 Oct')
})
