/** Resolve instrument keys in dashboard JSON exports (seasonality, etc.). */

export function normalizeFxMarketKey(market) {
  const m = String(market || '').trim()
  if (/^[A-Z]{6}$/.test(m)) return `${m.slice(0, 3)}/${m.slice(3)}`
  return m
}

export function resolveMarketBlock(doc, market) {
  if (!doc || !doc.markets || !market) return { block: null, matchedKey: null }
  const markets = doc.markets
  const candidates = [String(market).trim(), normalizeFxMarketKey(market)]
  for (const key of candidates) {
    if (key && markets[key]) return { block: markets[key], matchedKey: key }
  }
  const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim()
  const target = norm(market)
  const targetSlash = norm(normalizeFxMarketKey(market))
  for (const k of Object.keys(markets)) {
    const nk = norm(k)
    if (nk === target || nk === targetSlash) return { block: markets[k], matchedKey: k }
  }
  const compact = (s) => norm(s).replace(/\//g, '').replace(/\s+/g, '')
  const targetCompact = compact(market)
  for (const k of Object.keys(markets)) {
    const block = markets[k]
    if (compact(k) === targetCompact) return { block, matchedKey: k }
    if (block?.display_symbol && compact(block.display_symbol) === targetCompact) {
      return { block, matchedKey: k }
    }
  }
  return { block: null, matchedKey: null }
}
