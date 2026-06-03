// Phase 1 client-side persistence overlay for the Thesis Tracker.
//
// The canonical store is the Python-exported thesis_tracker_latest.json (read-only
// from the browser). Until the Phase 2 write server is wired, user actions
// (track from instrument page, status change, notes, remove) are persisted in
// localStorage and merged on top of the seeded export at read time.

import {
  ageWeeks,
  computeConviction,
  computeTrend,
  directionFromSnapshot,
  normStatus,
  snapshotFromRow,
  TERMINAL_STATUSES,
} from './thesisModel.js'
import { buildDecision } from './thesisNarrative.js'
import { buildOpportunity } from './alignmentEngine.js'

const KEY = 'hptl.thesisTracker.overlay.v1'

function emptyOverlay() {
  return { added: [], patches: {} }
}

export function loadOverlay() {
  try {
    const raw = window.localStorage.getItem(KEY)
    if (!raw) return emptyOverlay()
    const parsed = JSON.parse(raw)
    return {
      added: Array.isArray(parsed.added) ? parsed.added : [],
      patches: parsed.patches && typeof parsed.patches === 'object' ? parsed.patches : {},
    }
  } catch {
    return emptyOverlay()
  }
}

function saveOverlay(overlay) {
  try {
    window.localStorage.setItem(KEY, JSON.stringify(overlay))
  } catch {
    /* storage unavailable (private mode / quota) — overlay is best-effort */
  }
}

function newId() {
  if (window.crypto?.randomUUID) return window.crypto.randomUUID()
  return `local-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

function nowIso() {
  return new Date().toISOString()
}

export function addThesisFromRow({ market, row, week, status = 'DISCOVERED' }) {
  if (!market) throw new Error('market is required')
  const overlay = loadOverlay()
  if (overlay.added.some((t) => t.market === market)) {
    return overlay.added.find((t) => t.market === market)
  }
  const snap = snapshotFromRow(row || {}, week)
  const thesis = {
    thesis_id: newId(),
    market,
    symbol: '',
    asset_class: null,
    status: normStatus(status),
    direction_bias: directionFromSnapshot(snap),
    created_at: nowIso(),
    created_week: snap.week || null,
    source: 'local',
    archived: false,
    archived_at: null,
    outcome: null,
    summary_manual: null,
    tags: [],
    snapshots: snap.week ? [snap] : [],
    evolution_log: [
      {
        week: snap.week || null,
        auto: true,
        text: 'Thesis tracked from instrument page (local).',
        created_at: nowIso(),
      },
    ],
  }
  overlay.added.push(thesis)
  saveOverlay(overlay)
  return thesis
}

function patchFor(overlay, thesisId) {
  if (!overlay.patches[thesisId]) overlay.patches[thesisId] = {}
  return overlay.patches[thesisId]
}

export function setStatus(thesisId, status) {
  const next = normStatus(status)
  const overlay = loadOverlay()
  const added = overlay.added.find((t) => t.thesis_id === thesisId)
  if (added) {
    const prev = added.status
    added.status = next
    if (TERMINAL_STATUSES.has(next)) {
      added.archived = true
      added.archived_at = nowIso()
    }
    added.evolution_log.push({ week: added.last_update_week || null, auto: false, text: `Status ${prev} → ${next}.`, created_at: nowIso() })
  } else {
    const p = patchFor(overlay, thesisId)
    p.status = next
    if (TERMINAL_STATUSES.has(next)) p.archived = true
    p.notes = [...(p.notes || []), { week: null, auto: false, text: `Status → ${next}.`, created_at: nowIso() }]
  }
  saveOverlay(overlay)
}

export function addNote(thesisId, text) {
  const clean = String(text || '').trim()
  if (!clean) return
  const overlay = loadOverlay()
  const added = overlay.added.find((t) => t.thesis_id === thesisId)
  const entry = { week: null, auto: false, text: clean, created_at: nowIso() }
  if (added) {
    added.evolution_log.push(entry)
  } else {
    const p = patchFor(overlay, thesisId)
    p.notes = [...(p.notes || []), entry]
  }
  saveOverlay(overlay)
}

export function removeThesis(thesisId) {
  const overlay = loadOverlay()
  const before = overlay.added.length
  overlay.added = overlay.added.filter((t) => t.thesis_id !== thesisId)
  if (overlay.added.length === before) {
    patchFor(overlay, thesisId).removed = true
  }
  saveOverlay(overlay)
}

export function clearOverlay() {
  saveOverlay(emptyOverlay())
}

// Merge seeded (server) theses with the local overlay and recompute derived fields.
export function mergeWithOverlay(seededTheses = []) {
  const overlay = loadOverlay()
  const seededMarkets = new Set(seededTheses.map((t) => t.market))

  const patched = seededTheses
    .filter((t) => !overlay.patches[t.thesis_id]?.removed)
    .map((t) => {
      const p = overlay.patches[t.thesis_id]
      if (!p) return { ...t, _local: false }
      return {
        ...t,
        status: p.status ? normStatus(p.status) : t.status,
        archived: p.archived ?? t.archived,
        evolution_log: [...(t.evolution_log || []), ...(p.notes || [])],
        _local: true,
        _statusPatched: Boolean(p.status),
      }
    })

  const localOnly = overlay.added
    .filter((t) => !seededMarkets.has(t.market))
    .map((t) => ({ ...t, _local: true }))

  return [...patched, ...localOnly].map(withDerived)
}

// Recompute derived fields so overlay items match the server-derived shape.
// Local-only theses recompute the decision block in JS; server theses keep the
// Python-computed `decision` from the export but refresh it if it was patched.
export function withDerived(t) {
  const snaps = Array.isArray(t.snapshots) ? t.snapshots : []
  const current = snaps.length ? computeConviction(snaps[snaps.length - 1]).score : null
  const derived = {
    ...t,
    age_weeks: ageWeeks(snaps),
    conviction_current: t.conviction_current ?? current,
    conviction_trend: t.conviction_trend || computeTrend(snaps),
    last_update_week: snaps.length ? snaps[snaps.length - 1].week : t.last_update_week || null,
  }
  if (!derived.decision || t._local || t._statusPatched) {
    derived.decision = buildDecision(derived)
  }
  derived.opportunity = buildOpportunity(derived)
  return derived
}
