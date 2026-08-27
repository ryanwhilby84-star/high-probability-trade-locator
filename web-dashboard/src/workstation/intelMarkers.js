/**
 * Convert workstation intelligence markers → Lightweight Charts setMarkers payloads.
 */

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

const GROUP_COLOR = {
  commercial: '#b45309',
  noncommercial: '#1d4ed8',
  nonreportable: '#7c3aed',
  multi: '#0f766e',
  analogue: '#334155',
}

export function dateToTimeMap(timelineRows) {
  const map = new Map()
  for (const row of timelineRows || []) {
    const d = String(row?.date || row?.label || '').slice(0, 10)
    if (d && isNum(row.time)) map.set(d, row.time)
  }
  return map
}

export function filterMarkersByLayers(markers, layers) {
  return (markers || []).filter((m) => {
    const layer = m.layer || ''
    if (layer === 'commercial_extremes') return layers.commercial_extremes
    if (layer === 'noncommercial_extremes') return layers.noncommercial_extremes
    if (layer === 'nonreportable_extremes') return layers.nonreportable_extremes
    if (layer === 'multi_group') return layers.multi_group
    return false
  })
}

export function toPaneMarkers(events, timelineRows) {
  const times = dateToTimeMap(timelineRows)
  const out = []
  for (const e of events || []) {
    const time = times.get(String(e.date || '').slice(0, 10))
    if (!isNum(time)) continue
    const bearish = String(e.kind || e.label || '').toLowerCase().includes('bearish')
    out.push({
      time,
      position: bearish ? 'belowBar' : 'aboveBar',
      color: GROUP_COLOR[e.group] || GROUP_COLOR.multi,
      shape: bearish ? 'arrowDown' : e.group === 'multi' ? 'square' : 'arrowUp',
      text: '',
    })
  }
  return out
}

/** Comm↔NR divergence / research configuration markers — distinct from extreme arrows. */
export function toResearchMarkers(events, timelineRows, selectedDate = null) {
  const times = dateToTimeMap(timelineRows)
  const out = []
  for (const e of events || []) {
    const d = String(e.date || '').slice(0, 10)
    const time = times.get(d)
    if (!isNum(time)) continue
    const selected = selectedDate && d === String(selectedDate).slice(0, 10)
    const low = String(e.side || e.label || '').toLowerCase().includes('low')
    out.push({
      time,
      position: low ? 'belowBar' : 'aboveBar',
      color: selected ? '#dc2626' : '#0f766e',
      shape: 'circle',
      text: '',
    })
  }
  return out
}

export function spreadSeriesToLinePoints(spreadSeries, timelineRows) {
  const times = dateToTimeMap(timelineRows)
  const out = []
  for (const row of spreadSeries || []) {
    const time = times.get(String(row.date || '').slice(0, 10))
    const value = Number(row.spread)
    if (!isNum(time) || !Number.isFinite(value)) continue
    out.push({ time, value })
  }
  return out
}

export function toPriceSyncMarkers(events, timelineRows, selectedDate = null) {
  const times = dateToTimeMap(timelineRows)
  const out = []
  const seen = new Set()
  for (const e of events || []) {
    const d = String(e.date || '').slice(0, 10)
    const time = times.get(d)
    if (!isNum(time) || seen.has(time)) continue
    seen.add(time)
    const selected = selectedDate && d === String(selectedDate).slice(0, 10)
    const type = String(e.event_type || '')
    const isDiv = type === 'comm_nr_divergence'
    out.push({
      time,
      position: selected ? 'aboveBar' : 'inBar',
      color: selected ? '#fb7185' : isDiv ? '#2dd4bf' : '#fbbf24',
      shape: isDiv ? 'circle' : 'square',
      text: selected ? 'EVENT' : isDiv ? 'DIV' : 'EX',
    })
  }
  return out
}

export function findTimeForDate(timelineRows, date) {
  const d = String(date || '').slice(0, 10)
  const row = (timelineRows || []).find((r) => String(r.date || r.label || '').slice(0, 10) === d)
  return row?.time ?? null
}
