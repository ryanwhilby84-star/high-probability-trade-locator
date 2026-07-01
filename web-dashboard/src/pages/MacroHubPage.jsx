import React from 'react'
import { AppShell } from '../components/AppShell.jsx'
import { MacroPositioningPanel } from '../components/MacroPositioningPanel.jsx'
import { useMacroHub } from '../hooks/useMacroHub.js'
import { useTffMacroPositioning } from '../hooks/useTffMacroPositioning.js'
import { navigateToScanner } from '../routing.js'
import { tffCotBlockFromDoc } from '../tffMacroPositioning.js'

const fmt = (v, digits = 2) => {
  if (v === null || v === undefined || v === '') return '—'
  const n = Number(v)
  if (!Number.isFinite(n)) return String(v)
  return n.toLocaleString(undefined, { maximumFractionDigits: digits, minimumFractionDigits: 0 })
}

function FreshnessPill({ freshness }) {
  const status = freshness?.status || 'missing'
  const cls =
    status === 'fresh' ? 'mh-fresh-fresh' : status === 'stale' ? 'mh-fresh-stale' : 'mh-fresh-missing'
  const label =
    status === 'fresh'
      ? 'Fresh'
      : status === 'stale'
        ? `Stale (${freshness?.age_days ?? '?'}d)`
        : 'Missing'
  return (
    <span className={`mh-fresh-pill ${cls}`} title={freshness?.as_of ? `As of ${freshness.as_of}` : 'No date'}>
      {label}
    </span>
  )
}

function MetricRow({ label, value, date, source, freshness }) {
  return (
    <div className="mh-metric-row">
      <span className="mh-metric-label">{label}</span>
      <span className="mh-metric-value">{value}</span>
      <span className="mh-metric-meta">
        {date ? ` ${date}` : ''}
        {source ? ` · ${source}` : ''}
      </span>
      <FreshnessPill freshness={freshness} />
    </div>
  )
}

function CotBlock({ title, cot }) {
  if (!cot) return null
  return (
    <div className="mh-cot-block">
      <h4 className="mh-subtitle">{title}</h4>
      <dl className="mh-dl">
        <div>
          <dt>Long</dt>
          <dd>{fmt(cot.long, 0)}</dd>
        </div>
        <div>
          <dt>Short</dt>
          <dd>{fmt(cot.short, 0)}</dd>
        </div>
        <div>
          <dt>Net</dt>
          <dd>{fmt(cot.net, 0)}</dd>
        </div>
        <div>
          <dt>Weekly Δ net</dt>
          <dd>{fmt(cot.weekly_net_change, 0)}</dd>
        </div>
        <div>
          <dt>4-week Δ net</dt>
          <dd>{fmt(cot.four_week_net_change, 0)}</dd>
        </div>
        <div>
          <dt>Open interest</dt>
          <dd>{fmt(cot.open_interest, 0)}</dd>
        </div>
        <div>
          <dt>Net 13W %ile</dt>
          <dd>{fmt(cot.net_percentile_13w ?? cot.net_percentile_3y, 1)}</dd>
        </div>
        <div>
          <dt>Short 3Y %ile</dt>
          <dd>{fmt(cot.short_percentile_3y, 1)}</dd>
        </div>
        <div>
          <dt>OI 3Y %ile</dt>
          <dd>{fmt(cot.oi_percentile_3y, 1)}</dd>
        </div>
      </dl>
      <p className="mh-footnote">
        {cot.report_date ? `Report ${cot.report_date}` : 'No COT report date'}
        {cot.source ? ` · ${cot.source}` : ''}
        {cot.error ? ` · ${cot.error}` : ''}
        <FreshnessPill freshness={cot.freshness} />
      </p>
    </div>
  )
}

function CrossAssetGrid({ crossAssets }) {
  const entries = Object.entries(crossAssets || {})
  if (!entries.length) return <p className="mh-empty">No cross-asset data.</p>
  return (
    <div className="mh-cross-grid">
      {entries.map(([key, block]) => (
        <article key={key} className="mh-cross-card">
          <header className="mh-cross-head">
            <h3>{block?.label || key}</h3>
            <FreshnessPill freshness={block?.freshness} />
          </header>
          <p className="mh-cross-price">{fmt(block?.latest_price)}</p>
          <p className="mh-cross-meta">
            {block?.latest_date || '—'} · {block?.source || '—'}
          </p>
          {block?.cot?.net != null ? (
            <p className="mh-cross-cot">
              COT net {fmt(block.cot.net, 0)}
              {block.cot.net_percentile_3y != null ? ` · 3Y ${fmt(block.cot.net_percentile_3y, 1)}%ile` : ''}
            </p>
          ) : null}
        </article>
      ))}
    </div>
  )
}

function SourceHealthPanel({ health }) {
  if (!health) return null
  const issues = health.issues || []
  return (
    <section className="mh-section mh-health">
      <header className="mh-section-head">
        <h2>Source health</h2>
        <span className={`mh-health-badge ${health.healthy ? 'ok' : 'warn'}`}>
          {health.healthy ? 'All fresh' : `${health.issue_count} issue(s)`}
        </span>
      </header>
      {issues.length === 0 ? (
        <p className="mh-empty">All macro hub feeds are fresh.</p>
      ) : (
        <ul className="mh-health-list">
          {issues.map((item) => (
            <li key={item.field} className={`mh-health-item mh-health-${item.status}`}>
              <strong>{item.label}</strong>
              <span>{item.detail}</span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

export function MacroHubPage({ sidebarClass, onSidebarClass }) {
  const { data, loading, error } = useMacroHub()
  const tff = useTffMacroPositioning()

  const usd = data?.usd || {}
  const treas = data?.treasuries || {}
  const btc = data?.bitcoin || {}
  const dxyTffCot = tffCotBlockFromDoc(tff.data, 'US Dollar Index / DX')
  const usdCot = dxyTffCot || usd.cot

  return (
    <AppShell
      title="Macro Hub"
      subtitle={
        data?.as_of_date
          ? `USD · Treasuries · Bitcoin · cross-asset pool · as of ${data.as_of_date}`
          : 'Shared macro data foundation for USD themes and cross-asset confirmation'
      }
      sidebarClass={sidebarClass}
      onSidebarClass={onSidebarClass}
      topActions={
        <button type="button" className="ws-btn" onClick={navigateToScanner}>
          ← Scanner
        </button>
      }
    >
      {loading ? <p className="ws-topbar-meta">Loading macro hub…</p> : null}
      {error ? (
        <p className="ws-error-banner" role="alert">
          Macro hub data unavailable ({error}). Run <code>python -m hptl.macro_hub.run_macro_hub</code> to export.
        </p>
      ) : null}

      {!loading && !error && data ? (
        <>
          <MacroPositioningPanel doc={tff.data} loading={tff.loading} error={tff.error} />

          <section className="mh-section">
            <header className="mh-section-head">
              <h2>USD / DXY</h2>
            </header>
            <MetricRow
              label="DXY proxy (FRED broad USD)"
              value={fmt(usd.dxy_price, 3)}
              date={usd.dxy_price_date}
              source={usd.dxy_series_id || usd.dxy_source}
              freshness={usd.dxy_freshness}
            />
            <MetricRow
              label="DX futures price"
              value="—"
              date={usd.dx_futures_price_date}
              source={usd.dx_futures_note || usd.dx_futures_source}
              freshness={{ status: 'missing' }}
            />
            <CotBlock
              title={
                dxyTffCot
                  ? 'U.S. Dollar Index — TFF Leveraged Money positioning'
                  : 'U.S. Dollar Index futures COT (institutional / non-commercial)'
              }
              cot={usdCot}
            />
          </section>

          <section className="mh-section">
            <header className="mh-section-head">
              <h2>Treasuries / yields</h2>
              <FreshnessPill freshness={treas.freshness} />
            </header>
            <div className="mh-yield-grid">
              <MetricRow label="US 2Y" value={`${fmt(treas.us_2y_yield, 3)}%`} date={treas.latest_date} source={treas.source} freshness={treas.freshness} />
              <MetricRow label="US 10Y" value={`${fmt(treas.us_10y_yield, 3)}%`} date={treas.latest_date} source={treas.source} freshness={treas.freshness} />
              <MetricRow label="US 30Y" value={`${fmt(treas.us_30y_yield, 3)}%`} date={treas.latest_date} source={treas.source} freshness={treas.freshness} />
              <MetricRow label="2s10s curve" value={`${fmt(treas.curve_2s10s, 3)}%`} date={treas.latest_date} source={treas.source} freshness={treas.freshness} />
              <MetricRow label="10s30s curve" value={`${fmt(treas.curve_10s30s, 3)}%`} date={treas.latest_date} source={treas.source} freshness={treas.freshness} />
              <MetricRow label="10Y real yield (TIPS)" value={`${fmt(treas.real_yield_10y, 3)}%`} date={treas.latest_date} source="DFII10" freshness={treas.freshness} />
            </div>
          </section>

          <section className="mh-section">
            <header className="mh-section-head">
              <h2>Bitcoin</h2>
            </header>
            <MetricRow
              label="BTCUSD spot"
              value={fmt(btc.btcusd_price)}
              date={btc.btcusd_price_date}
              source={btc.btcusd_source}
              freshness={btc.btcusd_freshness}
            />
            <MetricRow
              label="BTC futures price"
              value="—"
              date={btc.btc_futures_price_date}
              source={btc.btc_futures_note || btc.btc_futures_source}
              freshness={{ status: 'missing' }}
            />
            <CotBlock title="Bitcoin futures COT" cot={btc.cot} />
          </section>

          <section className="mh-section">
            <header className="mh-section-head">
              <h2>Cross-asset confirmation</h2>
            </header>
            <p className="mh-intro">
              Daily close history stored for rolling correlation prep (30 / 90 / 180 day windows — engine not built yet).
            </p>
            <CrossAssetGrid crossAssets={data.cross_assets} />
          </section>

          <SourceHealthPanel health={data.source_health} />
        </>
      ) : null}
    </AppShell>
  )
}
