/**
 * Visual layer helpers for COT positioning research markers.
 * Shapes limited by Lightweight Charts: arrowUp | arrowDown | circle | square.
 */

export const RESEARCH_LAYERS = {
  commercial_extremes: {
    id: 'commercial_extremes',
    label: 'Commercial Extremes',
    hint: 'Absolute + local/relative Commercial extremes',
  },
  commercial_rotations: {
    id: 'commercial_rotations',
    label: 'Commercial Rotations',
    hint: 'Major Commercial percentile migrations',
  },
  noncommercial_extremes: {
    id: 'noncommercial_extremes',
    label: 'NC Extremes',
    hint: 'Absolute + local/relative Non-Commercial extremes',
  },
  noncommercial_rotations: {
    id: 'noncommercial_rotations',
    label: 'NC Rotations',
    hint: 'Major Non-Commercial percentile migrations',
  },
  divergence: {
    id: 'divergence',
    label: 'Comm↔NR Divergence',
    hint: 'Unusual Commercial vs Non-Reportable spread',
  },
  nr_extremes: {
    id: 'nr_extremes',
    label: 'NR Extremes / Rotations',
    hint: 'Non-Reportable extremes and major NR rotations',
  },
}

export const DEFAULT_LAYER_STATE = {
  commercial_extremes: true,
  commercial_rotations: true,
  noncommercial_extremes: true,
  noncommercial_rotations: true,
  divergence: true,
  // NR rotations are classified under nr_extremes in classifyResearchLayer.
  nr_extremes: true,
}

/** Legend rows — must match compact marker visuals. */
export const MARKER_LEGEND = [
  {
    key: 'abs',
    label: 'Extreme',
    color: '#fbbf24',
    shape: '◆',
  },
  {
    key: 'divergence',
    label: 'Divergence',
    color: '#2dd4bf',
    shape: '◈',
  },
  {
    key: 'rotation',
    label: 'Rotation',
    color: '#38bdf8',
    shape: '●',
  },
  {
    key: 'nr',
    label: 'NR extreme',
    color: '#c084fc',
    shape: '◇',
  },
]

/** Compact marker geometry for ResearchPinsOverlay. */
export function markerShapeForTone(tone) {
  if (tone === 'divergence') return 'split-diamond'
  if (tone === 'rotation') return 'circle'
  if (tone === 'nr') return 'diamond-outline'
  return 'diamond'
}

const isNum = (v) => typeof v === 'number' && Number.isFinite(v)

export function classifyResearchLayer(event) {
  const type = String(event?.event_type || '')
  const group = String(event?.group || '')
  const nrPct = event?.nonreportable?.long_history_percentile
  const nrExtreme = isNum(nrPct) && (nrPct <= 10 || nrPct >= 90)

  if (type === 'comm_nr_divergence') return 'divergence'
  if (type === 'major_rotation' || type === 'rapid_velocity') {
    if (group === 'nonreportable') return 'nr_extremes'
    if (group === 'noncommercial') return 'noncommercial_rotations'
    return 'commercial_rotations'
  }
  if (type === 'absolute_extreme' || type === 'local_extreme') {
    if (group === 'nonreportable') return 'nr_extremes'
    if (group === 'noncommercial') return 'noncommercial_extremes'
    return 'commercial_extremes'
  }
  if (nrExtreme && group !== 'commercial' && group !== 'noncommercial') {
    return 'nr_extremes'
  }
  return null
}

/** An event may paint on multiple layers (e.g. commercial extreme + NR opposing). */
export function eventLayerIds(event) {
  const primary = classifyResearchLayer(event)
  const layers = new Set()
  if (primary) layers.add(primary)

  const group = String(event?.group || '')
  const nrPct = event?.nonreportable?.long_history_percentile
  const nrExtreme = isNum(nrPct) && (nrPct <= 10 || nrPct >= 90)
  // Opposing NR percentile is a secondary paint only for Comm/NR research events —
  // never fold genuine NC events into nr_extremes.
  if (nrExtreme && group !== 'noncommercial') layers.add('nr_extremes')

  return [...layers]
}

export function eventMatchesLayers(event, layerState) {
  return eventLayerIds(event).some((id) => layerState?.[id])
}

function isBearishSide(event) {
  const side = String(event?.side || event?.label || '').toLowerCase()
  return side.includes('bear') || side.includes('low_spread')
}

export function eventTone(event) {
  const type = String(event?.event_type || '')
  const layer = classifyResearchLayer(event)
  if (type === 'comm_nr_divergence') return 'divergence'
  if (type === 'absolute_extreme' || type === 'local_extreme') return 'extreme'
  if (type === 'major_rotation' || type === 'rapid_velocity') {
    if (layer === 'nr_extremes') return 'nr'
    return 'rotation'
  }
  if (layer === 'nr_extremes') return 'nr'
  return 'extreme'
}

export function eventBadge(event) {
  const tone = eventTone(event)
  if (tone === 'divergence') return 'DIV'
  if (tone === 'rotation') return 'ROT'
  if (tone === 'nr') return 'NR'
  return 'EX'
}

/**
 * Compact HTML pin descriptors (primary visibility layer).
 * Multiple events on the same week stack vertically; abbreviation only via tooltip.
 */
export function toResearchPins(events, timelineRows, selectedDate = null, selectedEventId = null) {
  const times = new Map()
  for (const row of timelineRows || []) {
    const d = String(row?.date || row?.label || '').slice(0, 10)
    if (d && isNum(row.time)) times.set(d, row.time)
  }

  const toneRank = { divergence: 0, extreme: 1, rotation: 2, nr: 3 }
  const byTime = new Map()
  for (const e of events || []) {
    const d = String(e.date || '').slice(0, 10)
    const time = times.get(d)
    if (!isNum(time)) continue
    const tone = eventTone(e)
    const eventId = [
      d,
      e.event_type || '',
      e.group || '',
      e.side || '',
      e.label || '',
    ].join('|')
    const pin = {
      time,
      date: d,
      tone,
      shape: markerShapeForTone(tone),
      label: eventBadge(e),
      title: `${eventBadge(e)} · ${e.label || e.event_type || ''} · ${d}`,
      bearish: isBearishSide(e),
      selected: Boolean(
        (selectedEventId && eventId === selectedEventId) ||
          (!selectedEventId && selectedDate && d === String(selectedDate).slice(0, 10)),
      ),
      eventId,
      group: e.group || null,
      eventType: e.event_type || null,
    }
    if (!byTime.has(time)) byTime.set(time, [])
    byTime.get(time).push(pin)
  }

  const out = []
  for (const [, pins] of byTime) {
    pins.sort(
      (a, b) =>
        (toneRank[a.tone] ?? 9) - (toneRank[b.tone] ?? 9) ||
        String(a.eventId).localeCompare(String(b.eventId)),
    )
    // Deduplicate identical tone+label on same week (stacked unique tones).
    const seen = new Set()
    let stack = 0
    for (const pin of pins) {
      const key = `${pin.tone}:${pin.label}:${pin.group || ''}`
      if (seen.has(key)) continue
      seen.add(key)
      out.push({ ...pin, stackIndex: stack })
      stack += 1
    }
  }
  return out.sort((a, b) => a.time - b.time || a.stackIndex - b.stackIndex)
}

/**
 * Map research events → Lightweight Charts markers (secondary; pins are primary).
 * Prefer ResearchPinsOverlay on COT panes — keep this for price sync / fallbacks.
 */
export function toTypedResearchMarkers(events, timelineRows, selectedDate = null) {
  const times = new Map()
  for (const row of timelineRows || []) {
    const d = String(row?.date || row?.label || '').slice(0, 10)
    if (d && isNum(row.time)) times.set(d, row.time)
  }

  const out = []
  for (const e of events || []) {
    const d = String(e.date || '').slice(0, 10)
    const time = times.get(d)
    if (!isNum(time)) continue
    const selected = selectedDate && d === String(selectedDate).slice(0, 10)
    const type = String(e.event_type || '')
    const bearish = isBearishSide(e)
    const tone = eventTone(e)
    const badge = eventBadge(e)

    let color = '#fbbf24'
    let shape = bearish ? 'arrowDown' : 'arrowUp'
    let position = bearish ? 'belowBar' : 'aboveBar'

    if (tone === 'extreme') {
      color = selected ? '#fecaca' : '#fbbf24'
      shape = type === 'local_extreme' ? 'square' : bearish ? 'arrowDown' : 'arrowUp'
    } else if (tone === 'divergence') {
      color = selected ? '#fecaca' : '#2dd4bf'
      shape = 'circle'
    } else if (tone === 'rotation') {
      color = selected ? '#fecaca' : '#38bdf8'
      shape = 'circle'
    } else if (tone === 'nr') {
      color = selected ? '#fecaca' : '#c084fc'
      shape = bearish ? 'arrowDown' : 'arrowUp'
    }

    out.push({ time, position, color, shape, text: badge })
  }
  return out
}

export function shortSampleQuality(raw) {
  const s = String(raw || '')
  if (s.includes('INSUFFICIENT')) return 'INSUFFICIENT'
  if (s.includes('LOW')) return 'LOW'
  if (s.includes('MODERATE')) return 'MODERATE'
  if (s.includes('STRONGER') || s.includes('STRONG')) return 'STRONG'
  return s || '—'
}

export function fmtPctile(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  return `${Number(v).toFixed(0)}th`
}

export function fmtRet(v) {
  if (v == null || !Number.isFinite(Number(v))) return '—'
  const n = Number(v)
  const sign = n > 0 ? '+' : ''
  return `${sign}${n.toFixed(2)}%`
}
