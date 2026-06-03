const API_BASE = import.meta.env.VITE_JOURNAL_API_BASE || '/api/journal'

function headers(secret) {
  const h = { 'Content-Type': 'application/json' }
  if (secret) {
    h['X-TradingView-Webhook-Secret'] = secret
  }
  return h
}

export async function saveJournalEntry(payload, { secret = '' } = {}) {
  const res = await fetch(`${API_BASE}/journal/entries`, {
    method: 'POST',
    headers: headers(secret),
    body: JSON.stringify(payload),
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    const msg = data?.error || `Journal API HTTP ${res.status}`
    throw new Error(msg)
  }
  return data
}

export async function fetchJournalEntries({ status, market, secret = '' } = {}) {
  const params = new URLSearchParams()
  if (status) params.set('status', status)
  if (market) params.set('market', market)
  const qs = params.toString()
  const res = await fetch(`${API_BASE}/journal/entries${qs ? `?${qs}` : ''}`, {
    headers: headers(secret),
  })
  const data = await res.json().catch(() => ({}))
  if (!res.ok) {
    throw new Error(data?.error || `Journal API HTTP ${res.status}`)
  }
  return data.entries || []
}
