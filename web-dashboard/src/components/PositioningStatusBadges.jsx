import React from 'react'
import { isCotRowResolved } from '../marketResolution.js'
import { getInstrumentMeta, positioningStatus } from '../instrumentRegistry.js'
import { priorityTier } from '../marketAttention.js'

// Canonical COT status vocabulary (single source of truth for COT provenance).
const COT_STATUS_BADGE = {
  direct_cot: { cls: 'pos-badge-cot', label: 'Direct COT' },
  leg_derived_cot: { cls: 'pos-badge-leg', label: 'Leg-derived COT' },
  proxy_cot: { cls: 'pos-badge-proxy', label: 'Proxy only' },
  macro_only: { cls: 'pos-badge-macro-only', label: 'Macro only' },
  no_cot_available: { cls: 'pos-badge-nodata', label: 'No reliable data' },
  broken_mapping: { cls: 'pos-badge-broken', label: 'Broken mapping' },
  invalid_data: { cls: 'pos-badge-broken', label: 'Invalid data' },
}

// Legacy fallback for records built before the COT cleanup phase.
const STATUS_BADGE = {
  complete: { cls: 'pos-badge-cot', label: 'Direct COT' },
  cot_missing: { cls: 'pos-badge-nodirect', label: 'COT missing' },
  cot_mapping_missing: { cls: 'pos-badge-broken', label: 'Broken mapping' },
  macro_only: { cls: 'pos-badge-macro-only', label: 'Macro only' },
  proxy_required: { cls: 'pos-badge-proxy', label: 'Proxy only' },
  broken_mapping: { cls: 'pos-badge-broken', label: 'Broken mapping' },
  no_data: { cls: 'pos-badge-nodata', label: 'No reliable data' },
}

export function PositioningStatusBadges({ row, compact }) {
  const meta = row?.instrument_meta || getInstrumentMeta(row?.market)
  const ps = positioningStatus(row)
  const cotOk = isCotRowResolved(row)
  const tier = priorityTier(row)
  const cotStatus = row?.cot_status || meta?.cot_status
  const dataQuality = row?.data_quality_status || meta?.data_quality_status
  const dataStatus = row?.data_status || meta?.data_status

  const badges = []

  if (cotStatus && COT_STATUS_BADGE[cotStatus]) {
    const b = COT_STATUS_BADGE[cotStatus]
    badges.push({ key: 'cot_status', cls: b.cls, label: b.label })
  } else if (dataStatus && STATUS_BADGE[dataStatus]) {
    const b = STATUS_BADGE[dataStatus]
    badges.push({ key: 'status', cls: b.cls, label: b.label })
  } else if (cotOk || ps === 'cot_available' || meta?.has_cot_mapping) {
    badges.push({ key: 'cot', cls: 'pos-badge-cot', label: 'Direct COT' })
  } else if (ps === 'proxy_required' || meta?.cot_proxy_of) {
    badges.push({ key: 'proxy', cls: 'pos-badge-proxy', label: 'Proxy only' })
  } else {
    badges.push({ key: 'macro', cls: 'pos-badge-macro-only', label: 'Macro only' })
  }

  // Data-quality flags layered on top of provenance.
  if (dataQuality === 'invalid_rows_detected') {
    badges.push({ key: 'invrows', cls: 'pos-badge-broken', label: 'Invalid rows quarantined' })
  } else if (dataQuality === 'stale') {
    badges.push({ key: 'stale', cls: 'pos-badge-nodirect', label: 'Stale COT' })
  } else if (dataQuality === 'duplicate' && cotStatus !== 'proxy_cot') {
    badges.push({ key: 'dup', cls: 'pos-badge-proxy', label: 'Duplicate of leg' })
  }

  if (row?.macro_transmission?.generic_rates_only || row?.institutional_context?.macro_transmission?.generic_rates_only) {
    badges.push({ key: 'generic', cls: 'pos-badge-generic-macro', label: 'Generic macro only' })
  }

  if (tier === 'high_attention') {
    badges.push({ key: 'hi', cls: 'pos-badge-high-attn', label: 'High attention' })
  }

  if (compact) {
    return (
      <span className="pos-badge-row">
        {badges.slice(0, 2).map((b) => (
          <span key={b.key} className={`pos-badge ${b.cls}`}>
            {b.label}
          </span>
        ))}
      </span>
    )
  }

  return (
    <div className="pos-badge-row">
      {badges.map((b) => (
        <span key={b.key} className={`pos-badge ${b.cls}`}>
          {b.label}
        </span>
      ))}
    </div>
  )
}
