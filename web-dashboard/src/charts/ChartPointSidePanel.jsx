import React from 'react'

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const fmtInt = (v) => (isNum(v) ? Math.round(v).toLocaleString() : '—')
const fmtNum = (v, d = 5) => (isNum(v) ? v.toFixed(d) : '—')
const fmtPct = (v) => (isNum(v) ? `${v.toFixed(1)}%` : '—')
const fmtRet = (v) => {
  if (!isNum(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${v.toFixed(2)}%`
}
const fmtDelta = (v) => {
  if (!isNum(v)) return '—'
  const sign = v > 0 ? '+' : ''
  return `${sign}${Math.round(v).toLocaleString()}`
}

function Row({ label, value, sub = null, tone = null }) {
  return (
    <div className={`chart-ws-side-row${tone ? ` chart-ws-side-row--${tone}` : ''}`}>
      <span className="chart-ws-side-k">{label}</span>
      <span className="chart-ws-side-v">
        {value}
        {sub ? <span className="chart-ws-side-sub">{sub}</span> : null}
      </span>
    </div>
  )
}

function extremeBadge(label) {
  if (!label) return null
  const tone = label.includes('Top') ? 'high' : 'low'
  return <span className={`chart-ws-extreme-badge chart-ws-extreme-badge--${tone}`}>{label}</span>
}

/** Click-to-explain side panel for a historical chart point. */
export function ChartPointSidePanel({
  point,
  onClose,
  onReplayFromHere,
  replayActive = false,
}) {
  if (!point) return null

  return (
    <aside className="chart-ws-side" aria-label="Point analysis">
      <div className="chart-ws-side-head">
        <h2 className="chart-ws-side-title">{point.date}</h2>
        <button type="button" className="chart-ws-side-close" onClick={onClose} aria-label="Close panel">
          ×
        </button>
      </div>

      <div className="chart-ws-side-section">
        <h3 className="chart-ws-side-section-k">Positioning</h3>
        <Row label="Price" value={fmtNum(point.price, 4)} tone="price" />
        <Row
          label="Non-commercial net"
          value={fmtInt(point.institutional_net)}
          sub={
            <>
              {fmtPct(point.institutional_pct)}
              {extremeBadge(point.institutional_extreme_label)}
            </>
          }
          tone="institutional"
        />
        <Row
          label="Non-reportable net"
          value={fmtInt(point.retail_net)}
          sub={
            <>
              {fmtPct(point.retail_pct)}
              {extremeBadge(point.retail_extreme_label)}
            </>
          }
          tone="retail"
        />
        <Row label="NC weekly Δ" value={fmtDelta(point.institutional_wow)} />
        <Row label="NR weekly Δ" value={fmtDelta(point.retail_wow)} />
      </div>

      <div className="chart-ws-side-section">
        <h3 className="chart-ws-side-section-k">Forward price return</h3>
        <p className="chart-ws-side-note">What happened after this COT date (weekly bars).</p>
        <Row label="4 weeks" value={fmtRet(point.forward_return_4w)} />
        <Row label="8 weeks" value={fmtRet(point.forward_return_8w)} />
        <Row label="12 weeks" value={fmtRet(point.forward_return_12w)} />
      </div>

      {(isNum(point.location)) && (
        <div className="chart-ws-side-section">
          <h3 className="chart-ws-side-section-k">Location</h3>
          <Row
            label="52w range position"
            value={fmtNum(point.location, 1)}
            sub={point.location_state}
            tone="location"
          />
        </div>
      )}

      <div className="chart-ws-side-actions">
        {!replayActive ? (
          <button type="button" className="chart-ws-side-btn chart-ws-side-btn--primary" onClick={onReplayFromHere}>
            Replay from here
          </button>
        ) : (
          <p className="chart-ws-side-replay-note">Replay active — future data hidden from this date.</p>
        )}
      </div>
    </aside>
  )
}

/** Live crosshair header — updates on hover across synced panels. */
export function ChartCrosshairHeader({ point, supplement }) {
  if (!point) return null

  return (
    <div className="chart-ws-readout chart-ws-readout-v2" role="status" aria-live="polite">
      <span className="chart-ws-readout-date">{point.label || point.date}</span>
      <span className="chart-ws-readout-item chart-ws-readout--price">
        <span className="chart-ws-readout-k">COT price</span>
        <span className="chart-ws-readout-v">{fmtNum(point.price, 4)}</span>
      </span>
      <span className="chart-ws-readout-item chart-ws-readout--institutional">
        <span className="chart-ws-readout-k">NC net</span>
        <span className="chart-ws-readout-v">
          {fmtInt(point.institutional_net)}
          <span className="chart-ws-readout-d">{fmtPct(point.institutional_pct)}</span>
          {point.institutional_extreme_label ? (
            <span className="chart-ws-readout-tag chart-ws-readout-tag--inst">{point.institutional_extreme_label}</span>
          ) : null}
        </span>
      </span>
      <span className="chart-ws-readout-item chart-ws-readout--retail">
        <span className="chart-ws-readout-k">NR net</span>
        <span className="chart-ws-readout-v">
          {fmtInt(point.retail_net)}
          <span className="chart-ws-readout-d">{fmtPct(point.retail_pct)}</span>
          {point.retail_extreme_label ? (
            <span className="chart-ws-readout-tag chart-ws-readout-tag--ret">{point.retail_extreme_label}</span>
          ) : null}
        </span>
      </span>
      {isNum(point.commercial_net) ? (
        <span className="chart-ws-readout-item chart-ws-readout--commercial">
          <span className="chart-ws-readout-k">Comm net</span>
          <span className="chart-ws-readout-v">
            {fmtInt(point.commercial_net)}
            <span className="chart-ws-readout-d">{fmtPct(point.commercial_pct)}</span>
            {point.commercial_extreme_label ? (
              <span className="chart-ws-readout-tag chart-ws-readout-tag--comm">{point.commercial_extreme_label}</span>
            ) : null}
          </span>
        </span>
      ) : null}
      {supplement?.hasLocation && isNum(point.location) ? (
        <span className="chart-ws-readout-item chart-ws-readout--location">
          <span className="chart-ws-readout-k">Location</span>
          <span className="chart-ws-readout-v">
            {fmtNum(point.location, 1)}
            {point.location_state ? (
              <span className="chart-ws-readout-d">{point.location_state}</span>
            ) : null}
          </span>
        </span>
      ) : null}
    </div>
  )
}
