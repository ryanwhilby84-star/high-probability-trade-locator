import React from 'react'

const fmt = (v, digits = 0) => {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: 0 })
}

function PositioningCard({ title, primary, explanation, latest, labels }) {
  return (
    <article className="mh-pos-card">
      <header className="mh-pos-head">
        <h3>{title}</h3>
        <span className="mh-pos-primary">{primary || '—'}</span>
      </header>
      {Array.isArray(labels) && labels.length > 1 ? (
        <ul className="mh-pos-labels">
          {labels.map((l) => (
            <li key={l}>{l}</li>
          ))}
        </ul>
      ) : null}
      {latest ? (
        <dl className="mh-dl mh-pos-metrics">
          <div>
            <dt>Net</dt>
            <dd>{fmt(latest.net)}</dd>
          </div>
          <div>
            <dt>Weekly Δ</dt>
            <dd>{fmt(latest.one_week_net_change)}</dd>
          </div>
          <div>
            <dt>13w %ile</dt>
            <dd>{fmt(latest.net_percentile_13w, 1)}</dd>
          </div>
          <div>
            <dt>Open interest</dt>
            <dd>{fmt(latest.open_interest)}</dd>
          </div>
        </dl>
      ) : (
        <p className="mh-empty">No TFF positioning row.</p>
      )}
      <p className="mh-footnote">{explanation || ''}</p>
    </article>
  )
}

function TreasuryTenorGrid({ treas }) {
  const nets = treas?.tenor_nets || {}
  const changes = treas?.tenor_changes || {}
  const rows = Object.entries(nets)
  if (!rows.length) return null
  return (
    <div className="mh-tenor-grid">
      {rows.map(([iid, net]) => (
        <div key={iid} className="mh-tenor-row">
          <span className="mh-tenor-label">{iid.replace('US ', '').replace(' / ', ' ')}</span>
          <span className="mh-tenor-net">{fmt(net)}</span>
          <span className="mh-tenor-chg">Δ {fmt(changes[iid])}</span>
        </div>
      ))}
    </div>
  )
}

/** TFF macro positioning widgets — DXY, Treasury, rates sentiment. */
export function MacroPositioningPanel({ doc, loading, error }) {
  if (loading) return <p className="ws-topbar-meta">Loading TFF positioning…</p>
  if (error) {
    return (
      <p className="ws-error-banner" role="alert">
        TFF positioning unavailable ({error}). Run{' '}
        <code>python -m hptl.cot.run_tff_macro_positioning</code>.
      </p>
    )
  }
  if (!doc) return null

  const widgets = doc.widgets || {}
  const dxy = widgets.us_dollar_positioning || doc.macro_positioning?.dollar_positioning || {}
  const treas = widgets.treasury_positioning || doc.macro_positioning?.treasury_positioning || {}
  const rates = widgets.rates_yield_sentiment || doc.macro_positioning?.rates_yield_sentiment || {}

  const dxyInst = (doc.instruments || []).find((x) => x.symbol === 'DXY')
  const dxyLatest = dxyInst?.positioning || {
    net: dxy.net,
    one_week_net_change: dxy.one_week_net_change,
    net_percentile_13w: dxy.net_percentile_13w,
    open_interest: dxy.open_interest,
  }

  return (
    <section className="mh-section mh-positioning-section">
      <header className="mh-section-head">
        <h2>TFF Macro Positioning</h2>
        <span className="ws-topbar-meta">
          {doc.trader_group || 'Leveraged Money'} · {doc.source || 'CFTC TFF'}
        </span>
      </header>
      <div className="mh-pos-grid">
        <PositioningCard
          title="US Dollar Positioning"
          primary={dxy.primary_label}
          labels={dxy.score_labels}
          explanation={dxy.explanation}
          latest={dxyLatest}
        />
        <PositioningCard
          title="Treasury Positioning"
          primary={treas.score_label}
          labels={[treas.bond_bias, treas.yield_bias].filter(Boolean)}
          explanation={treas.explanation}
          latest={{
            net: treas.aggregate_net,
            one_week_net_change: treas.aggregate_weekly_change,
            net_percentile_13w: null,
            open_interest: null,
          }}
        />
        <article className="mh-pos-card">
          <header className="mh-pos-head">
            <h3>Rates &amp; Yield Sentiment</h3>
            <span className="mh-pos-primary">{rates.label || treas.yield_bias || '—'}</span>
          </header>
          <p className="mh-intro">
            Bond bias: <strong>{rates.bond_bias || treas.bond_bias || '—'}</strong>
            {rates.report_date ? ` · Report ${rates.report_date}` : ''}
          </p>
          <TreasuryTenorGrid treas={treas} />
        </article>
      </div>
    </section>
  )
}
