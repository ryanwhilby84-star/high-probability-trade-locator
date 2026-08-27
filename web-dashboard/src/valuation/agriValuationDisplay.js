/** Agriculture fundamental valuation display helpers (separate from FX). */

import { iveSummaryLine, readIVE } from './iveDisplay.js'

export const AGRI_VALUATION_MARKETS = new Set([
  'Soybeans',
  'Wheat',
  'Corn',
  'Sugar',
  'Cotton',
  'Coffee',
  'Cocoa',
])

export function isAgriValuationMarket(market) {
  return AGRI_VALUATION_MARKETS.has(String(market || '').trim())
}

export function valuationBlockForMarket(doc, marketId) {
  if (!doc || !marketId) return null
  const instruments = doc.instruments || doc.markets || {}
  return instruments[marketId] || null
}

export function mergeAgriValuationRow(row, valuationDoc) {
  const block = valuationBlockForMarket(valuationDoc, row?.market)
  if (!block) return row || {}
  const ive = readIVE(block)
  return {
    ...row,
    valuation_wired: block.wired ?? row?.valuation_wired,
    valuation_bias: block.valuation_bias ?? row?.valuation_bias,
    valuation_state: block.valuation_state ?? row?.valuation_state,
    valuation_reason: block.valuation_reason ?? block.unavailable_reason ?? row?.valuation_reason,
    unavailable_reason: block.unavailable_reason ?? row?.unavailable_reason,
    deviation_pct: block.deviation_pct ?? row?.deviation_pct,
    fair_value: block.fair_value ?? row?.fair_value,
    spot_price: block.spot_price ?? row?.spot_price,
    valuation_model_id: block.model_id ?? row?.valuation_model_id,
    valuation_grade: ive?.valuationGrade ?? block.valuation_grade ?? row?.valuation_grade,
    valuation_model_status: ive?.modelStatus ?? block.model_status ?? row?.valuation_model_status,
    valuation_pillar: block.valuation_pillar ?? row?.valuation_pillar,
  }
}

export function agriValuationTone(bias) {
  const b = String(bias || '').toLowerCase()
  if (b.includes('under')) return 'bullish'
  if (b.includes('over')) return 'bearish'
  return 'neutral'
}

export function agriValuationDisplay(row, valuationDoc) {
  const merged = mergeAgriValuationRow(row, valuationDoc)
  if (!isAgriValuationMarket(merged?.market)) return null

  const wired = merged.valuation_wired === true
  const bias = merged.valuation_bias || merged.valuation_state
  const gap = merged.deviation_pct
  const block = valuationBlockForMarket(valuationDoc, row?.market)
  const ive = readIVE(block)
  const reason =
    merged.unavailable_reason ||
    merged.valuation_reason ||
    'USDA balance sheet not available for stocks-to-use valuation.'

  if (wired && bias && String(bias).toUpperCase() !== 'UNAVAILABLE') {
    return {
      wired: true,
      gap,
      bias,
      tone: agriValuationTone(bias),
      summary: [
        merged.valuation_reason || merged.model_note || `Agri fair value · ${merged.valuation_model_id || 'agri_fundamental'}`,
        ive ? iveSummaryLine(ive) : null,
      ]
        .filter(Boolean)
        .join(' · '),
      fairValue: merged.fair_value,
      spot: merged.spot_price,
      valuationGrade: merged.valuation_grade,
      modelStatus: merged.valuation_model_status,
    }
  }

  const shortReason = String(reason).replace(/^Agri valuation unavailable —\s*/i, '')
  return {
    wired: false,
    unavailable: true,
    reason: shortReason,
    title: reason.startsWith('Agri valuation unavailable') ? reason : `Agri valuation unavailable — ${reason}`,
    summary: reason,
  }
}

