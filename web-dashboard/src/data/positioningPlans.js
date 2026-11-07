// Immutable COT evidence with editable research kept separately. Browser-local MVP.
export const KEY = 'institutional-edge.positioning-plans.v1'
export function getPositioningPlans() {
  try { const value = JSON.parse(localStorage.getItem(KEY) || '[]'); return Array.isArray(value) ? value : [] } catch { return [] }
}
export function savePositioningPlan(snapshot) {
  const existing = getPositioningPlans()
  const duplicate = existing.find(p => p.market === snapshot.market && p.reportDate === snapshot.reportDate && p.lookback === snapshot.lookback)
  if (duplicate) return duplicate
  const item = { id: globalThis.crypto?.randomUUID?.() || String(Date.now()), createdAt: new Date().toISOString(), snapshot: structuredClone(snapshot), market: snapshot.market, reportDate: snapshot.reportDate, lookback: snapshot.lookback, status: 'Watching', thesis: '', updates: [] }
  localStorage.setItem(KEY, JSON.stringify([item, ...existing]))
  window.dispatchEvent(new Event('positioning-plans-changed'))
  return item
}
export function updatePositioningPlan(id, changes) {
  const next = getPositioningPlans().map(p => p.id === id ? { ...p, ...changes, snapshot: p.snapshot } : p)
  localStorage.setItem(KEY, JSON.stringify(next))
  window.dispatchEvent(new Event('positioning-plans-changed'))
}
export function deletePositioningPlan(id) {
  localStorage.setItem(KEY, JSON.stringify(getPositioningPlans().filter(p => p.id !== id)))
  window.dispatchEvent(new Event('positioning-plans-changed'))
}
export function positioningTimeline(marketBlock, saved) {
  const rows = marketBlock?.rows || []
  const index = { '3y': 4, '5y': 12, '10y': 14, '15y': 13, all: 15 }[saved.lookback] ?? 4
  return rows.filter(r => r[0] >= saved.reportDate).map((r, i, selected) => {
    const prev = selected[i - 1]
    const net = r[2]?.[0], previousNet = prev?.[2]?.[0]
    const change = prev && Number.isFinite(net) && Number.isFinite(previousNet) ? net - previousNet : null
    const flip = prev && Number.isFinite(net) && Number.isFinite(previousNet) && ((previousNet > 0 && net < 0) || (previousNet < 0 && net > 0))
    const event = flip ? 'NC net-zero flip' : change == null ? 'Original snapshot' : change < 0 ? (net < 0 ? 'NC short accumulation / long reduction' : 'NC longs reducing') : change > 0 ? (net < 0 ? 'NC short covering' : 'NC longs increasing') : 'NC unchanged'
    return { date: r[0], net, change, nc: r[2]?.[index], c: r[1]?.[index], nr: r[3]?.[index], event }
  })
}
