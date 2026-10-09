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
    assert.ok(edge.direction === 'Bullish' ? edge.discovery.median > 0 && edge.discovery.mean > 0 : edge.discovery.median < 0 && edge.discovery.mean < 0)
    assert.equal(edge.lookbacks['15Y'], undefined)
    assert.match(edge.window, /\d{2} [A-Z][a-z]{2} → \d{2} [A-Z][a-z]{2}/)
  }
})
test('corrected archive windows retain readable dates and bounded exits', () => {
  const result = evaluateSugarWindow(archive, '10-03', '10-31')
  assert.equal(result.n, 28); assert.equal(result.hits, 18)
  assert.ok(result.outcomes.every(row => row.entry >= `${row.year}-10-03` && row.exit <= `${row.year}-10-31`))
  assert.equal(archiveDateLabel('10-03'), '03 Oct')
})

test('later-year price changes cannot choose the windows or their directions', () => {
  const altered = prepareSugarArchive({ ...archive.doc, rows: archive.doc.rows.map(row => row[0] >= '1988-01-01' ? [row[0], row[1], -.01, row[3], row[4]] : row) })
  const before = buildSugarArchiveEdges(archive, new Date(2026, 9, 7))
  const after = buildSugarArchiveEdges(altered, new Date(2026, 9, 7))
  assert.deepEqual(after.map(e => [e.window, e.direction, e.discovery]), before.map(e => [e.window, e.direction, e.discovery]))
  assert.ok(after.filter(e => e.direction === 'Bullish').every(e => !e.validation.passed))
  assert.ok(after.some(e => e.grade === 'UNCONFIRMED'))
})

test('real October archive retains unconfirmed opposite patterns with common-year comparisons', () => {
  const edges = buildSugarArchiveEdges(archive, new Date(2026, 9, 7))
  assert.ok(edges.some(e => e.validation.passed))
  assert.ok(edges.some(e => !e.validation.passed && e.direction === 'Bearish'))
  assert.ok(edges.some(e => e.variant_count > 1))
  for (const edge of edges) {
    assert.equal(edge.grade === 'VALIDATED', edge.validation.passed)
    for (const other of edge.opposing_windows) {
      assert.ok(other.common_years > 0)
      assert.ok(other.common_years <= edge.lookbacks.ARCHIVE.n)
      assert.notEqual(other.direction, edge.direction)
    }
  }
})
