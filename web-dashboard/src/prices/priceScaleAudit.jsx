import React from 'react'

/** Load price scale audit and resolve quarantine for chart pipelines. */

let _cache = null
let _promise = null

export const QUARANTINE_MSG =
  'Price data failed audit — chart disabled until source mapping is fixed.'

export function usePriceScaleAudit() {
  const [doc, setDoc] = React.useState(_cache)
  React.useEffect(() => {
    if (_cache) {
      setDoc(_cache)
      return
    }
    if (!_promise) {
      _promise = fetch('/data/price_scale_audit_latest.json')
        .then((r) => (r.ok ? r.json() : null))
        .then((d) => {
          _cache = d
          return d
        })
        .catch(() => null)
    }
    let active = true
    _promise.then((d) => {
      if (active) setDoc(d)
    })
    return () => {
      active = false
    }
  }, [])
  return doc
}

export function resolvePriceAuditRow(audit, market) {
  if (!audit?.markets || !market) return null
  const markets = audit.markets
  if (markets[market]) return markets[market]
  const norm = (s) => String(s || '').toLowerCase().replace(/\s+/g, ' ').trim()
  const target = norm(market)
  for (const k of Object.keys(markets)) {
    if (norm(k) === target) return markets[k]
  }
  const base = norm(String(market).split('/')[0])
  for (const k of Object.keys(markets)) {
    if (norm(String(k).split('/')[0]) === base) return markets[k]
  }
  return null
}

export function isPriceChartQuarantined(audit, market) {
  const row = resolvePriceAuditRow(audit, market)
  if (!row?.chart_quarantined) return { quarantined: false, reason: null, row: row || null }
  return {
    quarantined: true,
    reason: row.reason || audit?.quarantine_message || QUARANTINE_MSG,
    row,
  }
}

export function PriceAuditQuarantineBanner({ reason, title }) {
  return (
    <div className="price-audit-quarantine" role="alert">
      <strong>{title || QUARANTINE_MSG}</strong>
      {reason ? <p>{reason}</p> : null}
    </div>
  )
}
