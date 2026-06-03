import React from 'react'
import { resolveWeatherForMarket, WEATHER_CONTEXT_MARKETS } from '../weatherData.js'

function formatTs(ts) {
  if (!ts) return '—'
  const s = String(ts)
  return s.length >= 16 ? s.slice(0, 16).replace('T', ' ') + ' UTC' : s
}

function ImpactBadge({ badge, children }) {
  const cls =
    badge === 'green' ? 'wx-badge wx-badge-green' : badge === 'red' ? 'wx-badge wx-badge-red' : 'wx-badge wx-badge-amber'
  return <span className={cls}>{children}</span>
}

export function WeatherCropPanel({ row, weatherContext, weatherLoadError }) {
  const market = String(row?.market || '').trim()
  const wx = React.useMemo(
    () => resolveWeatherForMarket(row, weatherContext, { loadError: weatherLoadError }),
    [row, weatherContext, weatherLoadError],
  )

  if (!WEATHER_CONTEXT_MARKETS.has(market)) {
    return null
  }

  const showBundleError = wx.loadError || (wx.records.length === 0 && wx.bundleError)
  const showPartialBundle = wx.bundleError && wx.records.length > 0
  const isNg = market === 'Natural Gas / NG'
  const cropCol = isNg ? 'Demand impact' : 'Crop impact'

  return (
    <section className="mcat-section" aria-label="Weather and crop conditions">
      <h3 className="mcat-title">Weather / Crop</h3>
      {wx.provider ? <p className="mcat-meta-line">Provider: {wx.provider}</p> : null}
      {wx.hasOk && wx.weeklyBiasLine ? <p className="wx-weekly-bias">{wx.weeklyBiasLine}</p> : null}
      {showBundleError ? (
        <p className="mcat-not-wired" role="alert">
          {wx.loadError || wx.bundleError}
        </p>
      ) : null}
      {showPartialBundle ? (
        <p className="mcat-meta-line" role="status">
          {wx.bundleError}
        </p>
      ) : null}
      {wx.records.length ? (
        <table className="mcat-table">
          <thead>
            <tr>
              <th>Region</th>
              <th>Temp / Precip</th>
              <th>{cropCol}</th>
              <th>Price impact</th>
              <th>Confidence</th>
              <th>Reason</th>
            </tr>
          </thead>
          <tbody>
            {wx.records.map((r, i) => {
              const interp = r.interpretation
              return (
                <tr key={`${r.region}-${i}`} className={r.ok ? '' : 'wx-row-error'}>
                  <td>{r.region}</td>
                  {r.ok && interp ? (
                    <>
                      <td className="mcat-mono">
                        <div>{r.temperature_display || '—'}</div>
                        <div className="mcat-meta">{r.precipitation_display || '—'}</div>
                        <div className="mcat-meta">{formatTs(r.timestamp)}</div>
                      </td>
                      <td>
                        <ImpactBadge badge={interp.badge}>{interp.crop_impact_label}</ImpactBadge>
                      </td>
                      <td>
                        <ImpactBadge badge={interp.badge}>{interp.price_impact_label}</ImpactBadge>
                      </td>
                      <td className="mcat-meta">{interp.confidence}</td>
                      <td className="mcat-why">{interp.reason}</td>
                    </>
                  ) : r.ok ? (
                    <td colSpan={5} className="mcat-why">
                      {r.forecast_summary || '—'}
                    </td>
                  ) : (
                    <td colSpan={5} className="mcat-not-wired" role="alert">
                      {r.error || 'OpenWeather request failed'}
                    </td>
                  )}
                </tr>
              )
            })}
          </tbody>
        </table>
      ) : !showBundleError ? (
        <p className="mcat-not-wired" role="alert">
          No weather records in bundle. Run: python -m hptl.intelligence.run_weather_context_update
        </p>
      ) : null}
    </section>
  )
}
