import React from 'react'
import { fmtGapPct } from '../fx/fxInstitutionalValuation.js'
import { locationBlockForMarket } from '../hooks/useLocationLatest.js'
import { agriValuationDisplay, isAgriValuationMarket } from '../valuation/agriValuationDisplay.js'
import {
  currencyFuturesValuationDisplay,
  isCurrencyFuturesMarket,
} from '../valuation/currencyFuturesIveDisplay.js'
import { macroValuationDisplay, isMacroFairValueMarket } from '../valuation/macroValuationDisplay.js'
import { metalsValuationDisplay, isMetalsValuationMarket } from '../valuation/metalsValuationDisplay.js'

/** Scanner / table cell — currency futures IVE primary; agri/metals/macro unchanged. */
export function ValuationCell({ row, locationDoc, valuationDoc, futuresIveDoc, compact = false }) {
  if (!row) return <span className="val-cell val-cell-empty">—</span>

  if (isCurrencyFuturesMarket(row.market)) {
    const fut = currencyFuturesValuationDisplay(row.market, futuresIveDoc)
    if (fut?.wired) {
      const tone = fut.tone || 'neutral'
      const label = compact ? null : `Futures ${fut.futuresSymbol || ''}`.trim()
      return (
        <div className={`val-cell val-tone-${tone}`} title={fut.summary}>
          {label ? <div className="val-cell-label">{label}</div> : null}
          <span className="val-cell-gap">{fmtGapPct(fut.gap)}</span>
          <span className="val-cell-bias">{fut.bias}</span>
        </div>
      )
    }
    const title = fut?.reason || 'Futures valuation unavailable'
    return (
      <div className="val-cell val-cell-pending" title={title}>
        {compact ? 'N/A' : fut?.modelStatus || 'Unavailable'}
      </div>
    )
  }

  if (isMetalsValuationMarket(row.market)) {
    const metals = metalsValuationDisplay(row, valuationDoc)
    if (metals?.wired) {
      const tone = metals.tone || 'neutral'
      return (
        <div className={`val-cell val-tone-${tone}`} title={metals.summary}>
          <span className="val-cell-gap">{fmtGapPct(metals.gap)}</span>
          <span className="val-cell-bias">{metals.bias}</span>
        </div>
      )
    }
    const title = metals?.title || metals?.summary || 'Metals valuation unavailable'
    const label = compact
      ? 'Metals N/A'
      : metals?.reason
        ? `Valuation pending — ${metals.reason}`
        : 'Valuation pending (metals model V3.1)'
    return (
      <div className="val-cell val-cell-pending" title={title}>
        {label}
      </div>
    )
  }

  if (isAgriValuationMarket(row.market)) {
    const agri = agriValuationDisplay(row, valuationDoc)
    if (agri?.wired) {
      const tone = agri.tone || 'neutral'
      if (compact) {
        return (
          <div className={`val-cell val-tone-${tone}`} title={agri.summary}>
            <span className="val-cell-gap">{fmtGapPct(agri.gap)}</span>
            <span className="val-cell-bias">{agri.bias}</span>
          </div>
        )
      }
      return (
        <div className={`val-cell val-tone-${tone}`} title={agri.summary}>
          <div className="val-cell-label">Agri valuation</div>
          <span className="val-cell-gap">{fmtGapPct(agri.gap)}</span>
          <span className="val-cell-bias">{agri.bias}</span>
        </div>
      )
    }
    const title = agri?.title || agri?.summary || 'Agri valuation unavailable'
    const label = agri?.reason
      ? `Agri valuation unavailable — ${agri.reason}`
      : 'Agri valuation unavailable'
    return (
      <div className="val-cell val-cell-pending" title={title}>
        {compact ? 'Agri N/A' : label}
      </div>
    )
  }

  if (isMacroFairValueMarket(row.market)) {
    const macro = macroValuationDisplay(row, valuationDoc)
    if (macro?.wired) {
      const tone = macro.tone || 'neutral'
      return (
        <div className={`val-cell val-tone-${tone}`} title={macro.summary}>
          {!compact && macro.label ? <div className="val-cell-label">{macro.label}</div> : null}
          <span className="val-cell-gap">{fmtGapPct(macro.gap)}</span>
          <span className="val-cell-bias">{macro.bias}</span>
        </div>
      )
    }
    const title = macro?.title || macro?.summary || 'Macro valuation unavailable'
    return (
      <div className="val-cell val-cell-pending" title={title}>
        {compact ? 'Macro N/A' : macro?.reason || 'Macro valuation unavailable'}
      </div>
    )
  }

  const locBlock = locationBlockForMarket(locationDoc, row.market)
  const locWired = row.location_wired === true || locBlock?.wired === true
  const locBias = row.location_bias || locBlock?.location_bias
  const pct =
    row.location_price_percentile_52w ??
    locBlock?.price_percentile_52w ??
    row.valuation_price_percentile_52w
  const reason =
    row.location_reason ||
    locBlock?.location_reason ||
    row.valuation_reason ||
    'Location export unavailable for this instrument.'

  if (locWired && locBias && String(locBias).toUpperCase() !== 'UNAVAILABLE') {
    return (
      <div className="val-cell val-tone-neutral" title={reason}>
        <span className="val-cell-gap">{pct != null ? `${Number(pct).toFixed(0)}th pct` : '—'}</span>
        <span className="val-cell-bias">{locBias}</span>
      </div>
    )
  }

  if (row.valuation_wired && row.valuation_bias && String(row.valuation_bias).toUpperCase() !== 'UNAVAILABLE') {
    return (
      <div className="val-cell val-tone-neutral" title={`Location (legacy row field): ${row.valuation_reason || ''}`}>
        <span className="val-cell-gap">{pct != null ? `${Number(pct).toFixed(0)}th pct` : '—'}</span>
        <span className="val-cell-bias">{row.valuation_bias}</span>
      </div>
    )
  }

  return (
    <div className="val-cell val-cell-empty" title={reason}>
      Location pending
    </div>
  )
}

