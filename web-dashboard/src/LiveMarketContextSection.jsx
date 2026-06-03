import React from 'react'

import { computeLiveMarketContext } from './liveMarketContext.js'



function wireStateClass(status) {

  const u = String(status || '').toUpperCase()

  if (u === 'LIVE') return 'lmc-wire lmc-wire-live'

  if (u === 'STALE') return 'lmc-wire lmc-wire-stale'

  if (u === 'LOW CONFIDENCE') return 'lmc-wire lmc-wire-low'

  return 'lmc-wire lmc-wire-off'

}



function Row({ label, state, children }) {

  return (

    <div className="lmc-row">

      <div className="lmc-row-head">

        <span className="lmc-label">{label}</span>

        <span className={`lmc-state ${wireStateClass(state)}`}>{state}</span>

      </div>

      <div className="lmc-row-body">{children}</div>

    </div>

  )

}



export function LiveMarketContextSection({

  row,

  pack,

  globalMarketRegime,

  globalCalendar = null,

  weatherContext = null,

  weatherLoadError = null,

}) {

  const ctx = React.useMemo(

    () =>

      computeLiveMarketContext(row, pack, globalMarketRegime, {

        globalCalendar,

        weatherContext,

        weatherLoadError,

      }),

    [row, pack, globalMarketRegime, globalCalendar, weatherContext, weatherLoadError],

  )

  const { ratesMacro, newsFlow, eventWeather, relatedMarkets, meta, macroDigest } = ctx

  const cal = macroDigest.calendar



  return (

    <section className="lmc-section" aria-label="Macro and market context">

      <div className="lmc-head">

        <h4 className="lmc-title">Macro &amp; context</h4>

        {meta.showLiveBadge ? (

          <span className="lmc-badge" title="Validated live environment bundle within freshness window">

            Live bundle

          </span>

        ) : (

          <span className="lmc-badge lmc-badge-muted">COT-week export + optional live feeds</span>

        )}

      </div>



      <Row label="Macro regime" state={macroDigest.convictionLevel || '—'}>

        <p className="lmc-line lmc-em">{macroDigest.regimeLabel}</p>

        {macroDigest.convictionLevel ? (

          <p className="lmc-line">

            Conviction: {macroDigest.convictionLevel}

            {macroDigest.convictionDetail ? ` (${macroDigest.convictionDetail})` : ''}

          </p>

        ) : null}

        <p className="lmc-meta">Macro bias (intermarket): {macroDigest.macroBiasTag}</p>

      </Row>



      <Row label="Rates" state={ratesMacro.state}>

        {macroDigest.ratesLines.length ? (

          <ul className="lmc-bullets">

            {macroDigest.ratesLines.map((line, i) => (

              <li key={i}>{line}</li>

            ))}

          </ul>

        ) : (

          <p className="lmc-line">{ratesMacro.wired ? 'Rates fields present but thin — open audit for detail.' : ratesMacro.sentence}</p>

        )}

        <p className="lmc-meta">

          {ratesMacro.source}

          {ratesMacro.timestamp ? ` · ${ratesMacro.timestamp}` : ''}

        </p>

      </Row>



      <Row label="Next event" state={eventWeather.state}>

        {cal ? (

          <>

            <p className="lmc-line">

              <span className="lmc-focus">{cal.headline}</span> · {cal.timing}

            </p>

            <p className="lmc-meta">

              {cal.contextLine}

              {cal.importance ? ` · Impact ${cal.importance}` : ''}

            </p>

            <p className="lmc-sub">

              {cal.source}

              {cal.published_at ? ` · ${String(cal.published_at).slice(0, 16).replace('T', ' ')}` : ''}

            </p>

          </>

        ) : (

          <p className="lmc-line">{eventWeather.detail}</p>

        )}

        {eventWeather.weatherStatus ? (

          <p className="lmc-meta">

            Weather: {eventWeather.weatherStatus}

            {eventWeather.weatherDetail ? ` — ${eventWeather.weatherDetail}` : ''}

          </p>

        ) : null}

        {eventWeather.weatherLines?.length ? (

          <ul className="lmc-tight-list">

            {eventWeather.weatherLines.map((w, i) => (

              <li key={i}>

                <span className="lmc-src">{w.source}</span>

                {w.fetched ? ` · ${w.fetched}` : ''} — {w.text}

              </li>

            ))}

          </ul>

        ) : (

          <p className="lmc-sub">{eventWeather.focusLine}</p>

        )}

      </Row>



      <Row label="News flow" state={newsFlow.state}>

        {newsFlow.wired && newsFlow.items.length ? (

          <ul className="lmc-tight-list">

            {newsFlow.items.slice(0, 2).map((n, i) => (

              <li key={i}>

                <span className="lmc-src">{n.source}</span> · {n.headline}{' '}

                <span className="lmc-sub">

                  ({n.published_at ? n.published_at.slice(0, 16).replace('T', ' ') : '—'})

                </span>

              </li>

            ))}

          </ul>

        ) : (

          <p className="lmc-line">{newsFlow.detail}</p>

        )}

        {newsFlow.bundleChecked ? (

          <p className="lmc-meta">Bundle checked {newsFlow.bundleChecked.slice(0, 16).replace('T', ' ')} UTC</p>

        ) : null}

      </Row>



      <Row label="Related markets" state={relatedMarkets.state}>

        {relatedMarkets.feedLines?.length ? (

          <ul className="lmc-tight-list">

            {relatedMarkets.feedLines.map((line, i) => (

              <li key={i}>

                <span className="lmc-src">{line.source || 'Feed'}</span>

                {line.fetched ? ` · ${line.fetched}` : ''} — {line.text}

              </li>

            ))}

          </ul>

        ) : null}

        <p className="lmc-line">{relatedMarkets.lens}</p>

        <p className="lmc-meta">

          {relatedMarkets.feedLines?.length

            ? 'Cross-market macro lines from related instruments in market_environment_feed.'

            : 'Intermarket confirmation from COT-week row export.'}

        </p>

      </Row>

    </section>

  )

}

