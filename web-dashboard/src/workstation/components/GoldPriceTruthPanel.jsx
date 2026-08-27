import React from 'react'



import { fmtPrice } from '../../priceData.js'

import {
  buildPriceTruthTable,
  TV_AUDIT_STORAGE_KEY,
  logPriceTruthTable,
} from '../data/buildPriceTruthTable.js'



function statusClass(status) {

  if (status === 'FAIL') return 'gold-truth-status--fail'

  if (status === 'WARN') return 'gold-truth-status--warn'

  if (status === 'OK') return 'gold-truth-status--ok'

  if (status === 'INFO') return 'gold-truth-status--info'

  return 'gold-truth-status--skip'

}



function categoryClass(category) {

  if (category === 'LIVE') return 'gold-truth-cat--live'

  if (category === 'WEEKLY_CLOSE') return 'gold-truth-cat--weekly'

  if (category === 'COT_HISTORICAL') return 'gold-truth-cat--cot'

  if (category === 'MANUAL') return 'gold-truth-cat--manual'

  return ''

}



export function GoldPriceTruthPanel({
  marketId,
  valuationBlock,
  priceContext,
  displaySnapshot,
}) {

  const lastLoggedSignature = React.useRef(null)

  const [tvInput, setTvInput] = React.useState(() => {

    try {

      return localStorage.getItem(TV_AUDIT_STORAGE_KEY) || ''

    } catch {

      return ''

    }

  })



  const tvParsed = React.useMemo(() => {

    const n = Number(String(tvInput).replace(/,/g, '').trim())

    return Number.isFinite(n) ? n : null

  }, [tvInput])



  const audit = React.useMemo(

    () =>

      buildPriceTruthTable({
        marketId,
        valuationBlock,
        priceContext,
        displaySnapshot,
        tradingViewAuditPrice: tvParsed,
      }),

    [
      marketId,
      valuationBlock,
      priceContext,
      displaySnapshot,
      tvParsed,
    ],

  )



  React.useEffect(() => {

    if (!audit) return

    const signature = JSON.stringify({
      summary: audit.summary,
      table: audit.table?.map((r) => [r.field, r.value, r.timestamp, r.source]),
      comparisons: audit.comparisons?.map((c) => [
        c.name,
        c.status,
        c.expected,
        c.name === 'Live quote freshness' ? c.status : c.actual,
      ]),
    })
    if (lastLoggedSignature.current === signature) return
    lastLoggedSignature.current = signature

    logPriceTruthTable(audit)

  }, [audit])



  const onTvChange = React.useCallback((e) => {

    const v = e.target.value

    setTvInput(v)

    try {

      if (v.trim()) localStorage.setItem(TV_AUDIT_STORAGE_KEY, v.trim())
      else localStorage.removeItem(TV_AUDIT_STORAGE_KEY)

    } catch {

      /* ignore */

    }

  }, [])



  if (!audit) return null



  const { table, comparisons, summary, exports: expMeta, cotContext } = audit



  return (

    <section className="gold-truth-panel" aria-label={`${marketId} price truth table`}>
      <header className="gold-truth-head">
        <div>
          <h4 className="gold-truth-title">{marketId} price truth table</h4>

          <p className="gold-truth-sub">

            Mandatory audit — every displayed price with source, timestamp, and consuming component.

          </p>

        </div>

        <span className={`gold-truth-overall gold-truth-overall--${summary.overall.toLowerCase()}`}>

          {summary.overall} · {summary.failCount} fail · {summary.warnCount} warn · {summary.okCount} ok

        </span>

      </header>



      <div className="gold-truth-meta">

        <span>Live export: {expMeta.liveQuotesGeneratedAt?.slice(0, 19) ?? '—'}</span>

        <span>OHLC export: {expMeta.workstationOhlcGeneratedAt?.slice(0, 19) ?? '—'}</span>

        <span>Valuation export: {expMeta.valuationGeneratedAt?.slice(0, 19) ?? '—'}</span>

        <span>

          COT {cotContext.lastCotWeek ?? '—'} → OHLC {cotContext.matchedOhlcWeek ?? '—'}

          {cotContext.cotRowPrice != null ? ` · COT row price ${fmtPrice(cotContext.cotRowPrice, 2)}` : ''}

        </span>

      </div>



      <label className="gold-truth-tv-input">

        <span>TradingView XAUUSD (manual audit)</span>

        <input

          type="text"

          inputMode="decimal"

          placeholder="e.g. 4032.50"

          value={tvInput}

          onChange={onTvChange}

        />

      </label>



      <div className="gold-truth-table-wrap">

        <table className="gold-truth-table">

          <thead>

            <tr>

              <th>Field</th>

              <th>Value</th>

              <th>Source</th>

              <th>Timestamp</th>

              <th>Component using it</th>

              <th>Store</th>
              <th>Kind</th>

            </tr>

          </thead>

          <tbody>

            {table.map((row) => (

              <tr key={row.field}>

                <td>{row.field}</td>

                <td className="gold-truth-v--mono">{row.valueDisplay}</td>

                <td>{row.source}</td>

                <td className="gold-truth-v--mono">{String(row.timestamp).slice(0, 19)}</td>

                <td>{row.component}</td>

                <td className="gold-truth-v--mono">{row.store}</td>

                <td>

                  <span className={`gold-truth-cat ${categoryClass(row.category)}`}>{row.category}</span>

                </td>

              </tr>

            ))}

          </tbody>

        </table>

      </div>



      <div className="gold-truth-section">

        <h5 className="gold-truth-section-title">Comparisons</h5>

        <ul className="gold-truth-compare-list">

          {comparisons.map((c) => (

            <li key={c.name} className={`gold-truth-compare ${statusClass(c.status)}`}>

              <span className="gold-truth-compare-status">{c.status}</span>

              <strong>{c.name}</strong>

              <span className="gold-truth-compare-detail">

                expected {c.expected} · actual {c.actual} · {c.detail}

              </span>

              <span className="gold-truth-compare-component">{c.component}</span>

            </li>

          ))}

        </ul>

      </div>



      <p className="gold-truth-footnote">

        LIVE/CURRENT labels must use OANDA live quote only. WEEKLY CLOSE uses completed OHLC only. COT price is

        historical and never live. Console: <code>[gold-price-truth]</code>

      </p>

    </section>

  )

}


