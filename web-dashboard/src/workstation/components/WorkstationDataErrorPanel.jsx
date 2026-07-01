import React from 'react'

export function WorkstationDataErrorPanel({ title = 'Workstation data unavailable', message, detail = null }) {
  if (!message && !detail) return null
  return (
    <div className="pos-chart-panel pos-chart-panel--error" role="alert">
      <p className="pos-chart-panel-warn">
        <strong>{title}</strong>
        {message ? ` — ${message}` : null}
      </p>
      {detail ? <p className="pos-chart-panel-empty">{detail}</p> : null}
    </div>
  )
}
