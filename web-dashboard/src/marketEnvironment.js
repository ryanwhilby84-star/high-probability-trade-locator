/**
 * Market environment — trust-first. No simulated “live” news, calendar, or sentiment.
 *
 * Supply validated bundles on each row as `market_environment_feed`, for example:
 *
 * {
 *   live_bundle_last_checked_at: "2026-05-13T09:42:00Z",
 *   news_items: [{
 *     source: "Reuters",
 *     published_at: "2026-05-13T08:10:00Z",
 *     fetched_at: "2026-05-13T09:40:12Z",
 *     headline: "…",
 *     url: "https://…",
 *     related_instruments: ["Copper / HG"],
 *     classification: "neutral",
 *     explanation: "One-line reason.",
 *     confidence: "medium"
 *   }],
 *   event_items: [{
 *     source: "Economic calendar",
 *     published_at: "2026-05-14T12:30:00Z",
 *     fetched_at: "2026-05-13T09:41:00Z",
 *     headline: "US CPI",
 *     risk_level: "elevated",
 *     classification: "risk",
 *     explanation: "High-impact print.",
 *     confidence: "high",
 *     related_instruments: ["NASDAQ / NQ"]
 *   }],
 *   volatility_assessment: { level: "Elevated", source: "CBOE VIX feed", fetched_at: "2026-05-13T09:42:00Z" },
 *   weather_feed_connected: false
 * }
 *
 * Snapshot-only: macro from confluence row; intermarket from row JSON — never labeled as live news.
 */

/** Stale if bundle last-check older than this (client-side heuristic). */
export const LIVE_STALE_MS = 30 * 60 * 1000

const CLASSIFICATIONS = new Set(['supportive', 'neutral', 'contradicting', 'risk'])
const CONFIDENCE = new Set(['low', 'medium', 'high'])
const EVENT_LEVELS = new Set(['low', 'moderate', 'elevated'])

function num(v) {
  const n = Number(v)
  return Number.isFinite(n) ? n : NaN
}

function clip(s, n) {
  const t = String(s || '').trim()
  if (t.length <= n) return t
  return `${t.slice(0, n - 1)}…`
}

function isoParseMs(iso) {
  if (iso == null || iso === '') return NaN
  const t = Date.parse(String(iso))
  return Number.isFinite(t) ? t : NaN
}

function rowAsOf(row) {
  const d = row?.date || row?.latest_report_date || row?.as_of_date || ''
  return String(d || '').trim() || null
}

/**
 * @typedef {object} LiveNewsItemExpected
 * @property {string} source
 * @property {string} published_at ISO-8601
 * @property {string} fetched_at ISO-8601
 * @property {string} headline
 * @property {string} [url]
 * @property {string[]} related_instruments
 * @property {'supportive'|'neutral'|'contradicting'|'risk'} classification
 * @property {string} explanation
 * @property {'low'|'medium'|'high'} confidence
 */

export function validateNewsItem(raw, instrumentMarket) {
  const errs = []
  if (!raw || typeof raw !== 'object') return { ok: false, errors: ['item missing'], item: null }
  const source = String(raw.source || '').trim()
  const published_at = String(raw.published_at || '').trim()
  const fetched_at = String(raw.fetched_at || '').trim()
  const headline = String(raw.headline || '').trim()
  const explanation = String(raw.explanation || '').trim()
  const classification = String(raw.classification || '').trim().toLowerCase()
  const confidence = String(raw.confidence || '').trim().toLowerCase()
  let related_instruments = Array.isArray(raw.related_instruments) ? raw.related_instruments.map((x) => String(x || '').trim()).filter(Boolean) : []
  if (!source) errs.push('source')
  if (!published_at || !Number.isFinite(isoParseMs(published_at))) errs.push('published_at')
  if (!fetched_at || !Number.isFinite(isoParseMs(fetched_at))) errs.push('fetched_at')
  if (!headline) errs.push('headline')
  if (!explanation) errs.push('explanation')
  if (!CLASSIFICATIONS.has(classification)) errs.push('classification')
  if (!CONFIDENCE.has(confidence)) errs.push('confidence')
  const inst = String(instrumentMarket || '').trim()
  if (inst && related_instruments.length && !related_instruments.some((m) => canonicalInstrumentMatch(m, inst))) {
    errs.push('related_instruments must include this instrument when specified')
  }
  if (!related_instruments.length && inst) {
    related_instruments = [inst]
  }

  const item = {
    source,
    published_at,
    fetched_at,
    headline,
    url: raw.url != null && String(raw.url).trim() !== '' ? String(raw.url).trim() : null,
    related_instruments,
    classification,
    explanation,
    confidence,
  }
  return { ok: errs.length === 0, errors: errs, item }
}

export function validateEventItem(raw, instrumentMarket) {
  const errs = []
  if (!raw || typeof raw !== 'object') return { ok: false, errors: ['item missing'], item: null }
  const source = String(raw.source || '').trim()
  const published_at = String(raw.published_at || '').trim()
  const fetched_at = String(raw.fetched_at || '').trim()
  const headline = String(raw.headline || '').trim()
  const explanation = String(raw.explanation || '').trim()
  const classification = String(raw.classification || '').trim().toLowerCase()
  const confidence = String(raw.confidence || '').trim().toLowerCase()
  const risk_level = String(raw.risk_level || raw.event_risk || '').trim().toLowerCase()
  let related_instruments = Array.isArray(raw.related_instruments) ? raw.related_instruments.map((x) => String(x || '').trim()).filter(Boolean) : []
  if (!source) errs.push('source')
  if (!published_at || !Number.isFinite(isoParseMs(published_at))) errs.push('published_at')
  if (!fetched_at || !Number.isFinite(isoParseMs(fetched_at))) errs.push('fetched_at')
  if (!headline) errs.push('headline')
  if (!explanation) errs.push('explanation')
  if (!CLASSIFICATIONS.has(classification)) errs.push('classification')
  if (!CONFIDENCE.has(confidence)) errs.push('confidence')
  if (!EVENT_LEVELS.has(risk_level)) errs.push('risk_level')
  const inst = String(instrumentMarket || '').trim()
  if (inst && related_instruments.length && !related_instruments.some((m) => canonicalInstrumentMatch(m, inst))) {
    errs.push('related_instruments')
  }
  if (!related_instruments.length && inst) related_instruments = [inst]

  const item = {
    source,
    published_at,
    fetched_at,
    headline,
    url: raw.url != null && String(raw.url).trim() !== '' ? String(raw.url).trim() : null,
    related_instruments,
    classification,
    explanation,
    confidence,
    risk_level,
  }
  return { ok: errs.length === 0, errors: errs, item }
}

function canonicalInstrumentMatch(a, b) {
  return String(a || '').trim().toLowerCase() === String(b || '').trim().toLowerCase()
}

export function aggregateNewsClassification(items) {
  const counts = { supportive: 0, neutral: 0, contradicting: 0, risk: 0 }
  for (const it of items) {
    const c = it.classification
    if (c === 'risk') counts.risk += 1
    else if (c in counts) counts[c] += 1
  }
  if (counts.risk > 0) return 'Contradicting'
  if (counts.contradicting > counts.supportive && counts.contradicting >= counts.neutral) return 'Contradicting'
  if (counts.supportive > counts.contradicting && counts.supportive >= counts.neutral) return 'Supportive'
  if (counts.neutral >= counts.supportive && counts.neutral >= counts.contradicting) return 'Neutral'
  return 'Neutral'
}

export function aggregateEventRiskLevels(items) {
  let max = 0
  for (const it of items) {
    const r = it.risk_level
    const n = r === 'elevated' ? 3 : r === 'moderate' ? 2 : 1
    if (n > max) max = n
  }
  if (max >= 3) return 'Elevated'
  if (max === 2) return 'Moderate'
  return 'Low'
}

function formatTimeHm(iso) {
  const ms = isoParseMs(iso)
  if (!Number.isFinite(ms)) return null
  try {
    const d = new Date(ms)
    return d.toLocaleTimeString(undefined, { hour: '2-digit', minute: '2-digit', hour12: false })
  } catch {
    return null
  }
}

function newestFetchedMs(items) {
  let m = 0
  for (const it of items) {
    const t = isoParseMs(it.fetched_at)
    if (Number.isFinite(t) && t > m) m = t
  }
  return m || NaN
}

function isBundleStale(lastCheckedIso, newsItems, eventItems, nowMs = Date.now()) {
  const candidates = []
  if (lastCheckedIso) candidates.push(isoParseMs(lastCheckedIso))
  const nf = newestFetchedMs(newsItems)
  const ef = newestFetchedMs(eventItems)
  if (Number.isFinite(nf)) candidates.push(nf)
  if (Number.isFinite(ef)) candidates.push(ef)
  const newest = candidates.length ? Math.max(...candidates.filter(Number.isFinite)) : NaN
  if (!Number.isFinite(newest)) return { stale: true, newestMs: NaN }
  return { stale: nowMs - newest > LIVE_STALE_MS, newestMs: newest }
}

/**
 * @param {object} row
 * @param {object} pack
 * @param {object|null} globalMarketRegime
 * @param {{ feed?: object, override?: object, nowMs?: number }} [options]
 */
export function computeMarketEnvironment(row, pack, globalMarketRegime, options = {}) {
  const nowMs = Number.isFinite(options.nowMs) ? options.nowMs : Date.now()
  const feedIn = options.feed || row?.market_environment_feed || null
  const staticRow = row?.market_environment && typeof row.market_environment === 'object' ? row.market_environment : null
  const mergedFeed = { ...staticRow, ...feedIn, ...(options.override || {}) }

  const market = String(row?.market || '').trim()

  const provenanceSnapshot = {
    macroSource: 'COT confluence row snapshot',
    macroAsOf: rowAsOf(row),
    intermarketSource: 'Intermarket fields on confluence row',
  }

  /** --- Macro: only from snapshot fields; never labeled as live --- */
  let macro = 'Neutral'
  const ms = num(row?.macro_score)
  const mr = String(row?.macro_regime || '').toLowerCase()
  const rm = row?.rates_macro && typeof row.rates_macro === 'object' ? row.rates_macro : null
  const bias = String(rm?.rates_bias || '').toLowerCase()
  let macroDataMissing = !Number.isFinite(ms) && !mr && !rm

  if (!macroDataMissing) {
    if (Number.isFinite(ms)) {
      if (ms >= 6) macro = 'Supportive'
      else if (ms <= 3) macro = 'Restrictive'
    }
    if (mr.includes('risk_on') && macro === 'Neutral' && Number.isFinite(ms) && ms >= 4) macro = 'Supportive'
    if (mr.includes('risk_off')) macro = 'Restrictive'
    if (bias.includes('rising') && (bias.includes('yield') || bias.includes('rate'))) {
      if (macro === 'Supportive') macro = 'Neutral'
      else if (macro === 'Neutral') macro = 'Restrictive'
    }
  } else {
    macro = 'Insufficient data'
  }

  /** --- Intermarket: row JSON only --- */
  const inter = row?.intermarket_impulse_context && typeof row.intermarket_impulse_context === 'object' ? row.intermarket_impulse_context : {}
  const conf = String(inter.intermarket_confirmation || '').trim().toUpperCase()
  const hasInterDrivers =
    (Array.isArray(inter.supporting_drivers) && inter.supporting_drivers.length > 0) ||
    (Array.isArray(inter.conflicting_drivers) && inter.conflicting_drivers.length > 0)

  let intermarket = 'Data Missing'
  if (conf === 'CONFIRMING') intermarket = 'Supportive'
  else if (conf === 'MIXED') intermarket = 'Mixed'
  else if (conf === 'DIVERGING' || conf === 'WARNING') intermarket = 'Contradicting'
  else if (!conf && !hasInterDrivers) intermarket = 'Data Missing'
  else if (!conf && hasInterDrivers) intermarket = 'Mixed'

  /** --- Live news --- */
  const rawNews = Array.isArray(mergedFeed.news_items) ? mergedFeed.news_items : []
  const validatedNews = []
  const newsErrors = []
  for (let i = 0; i < rawNews.length; i++) {
    const v = validateNewsItem(rawNews[i], market)
    if (v.ok && v.item) validatedNews.push(v.item)
    else newsErrors.push({ index: i, errors: v.errors })
  }

  let newsSources = []
  if (validatedNews.length > 0) {
    newsSources = [...new Set(validatedNews.map((x) => x.source).filter(Boolean))]
  }

  /** --- Live calendar / events --- */
  const rawEvents = Array.isArray(mergedFeed.event_items) ? mergedFeed.event_items : []
  const validatedEvents = []
  const eventErrors = []
  for (let i = 0; i < rawEvents.length; i++) {
    const v = validateEventItem(rawEvents[i], market)
    if (v.ok && v.item) validatedEvents.push(v.item)
    else eventErrors.push({ index: i, errors: v.errors })
  }

  let eventSources = []
  if (validatedEvents.length > 0) {
    eventSources = [...new Set(validatedEvents.map((x) => x.source).filter(Boolean))]
  }

  /** --- Volatility: live feed only --- */
  const volFeed = mergedFeed.volatility_assessment && typeof mergedFeed.volatility_assessment === 'object' ? mergedFeed.volatility_assessment : null
  let volatility = 'NOT WIRED'
  if (volFeed) {
    const lvl = String(volFeed.level || '').trim()
    const src = String(volFeed.source || '').trim()
    const fa = String(volFeed.fetched_at || '').trim()
    if (['Calm', 'Elevated', 'Extreme'].includes(lvl) && src && Number.isFinite(isoParseMs(fa))) {
      volatility = lvl
    }
  }

  /** --- Weather --- */
  const weatherConnected = mergedFeed.weather_feed_connected === true
  const weatherLabel = weatherConnected ? String(mergedFeed.weather_feed_status || 'Connected').trim() : 'Not Connected'

  /** --- Freshness / last updated --- */
  const bundleChecked = mergedFeed.live_bundle_last_checked_at ? String(mergedFeed.live_bundle_last_checked_at).trim() : ''
  const { stale, newestMs } = isBundleStale(bundleChecked, validatedNews, validatedEvents, nowMs)

  const hasValidatedLive =
    validatedNews.length > 0 || validatedEvents.length > 0 || (volFeed && volatility !== 'NOT WIRED')
  const hasRawLiveAttempt = rawNews.length > 0 || rawEvents.length > 0 || !!volFeed

  let newsDisplay = 'NOT WIRED'
  if (validatedNews.length > 0) {
    newsDisplay = stale ? 'STALE' : aggregateNewsClassification(validatedNews)
  } else if (rawNews.length > 0) {
    newsDisplay = 'Unknown'
  }

  let eventDisplay = 'NOT WIRED'
  if (validatedEvents.length > 0) {
    const agg = aggregateEventRiskLevels(validatedEvents)
    const tier = agg === 'Low' ? 'Low' : agg === 'Moderate' ? 'Moderate' : 'Elevated'
    eventDisplay = stale ? 'STALE' : tier
  } else if (rawEvents.length > 0) {
    eventDisplay = 'Unknown'
  }

  const lastCheckedHm = formatTimeHm(bundleChecked) || (Number.isFinite(newestMs) ? formatTimeHm(new Date(newestMs).toISOString()) : null)

  const sourceParts = []
  if (newsSources.length) sourceParts.push(...newsSources)
  if (eventSources.length) sourceParts.push(...eventSources)
  if (volatility !== 'NOT WIRED' && volFeed?.source) sourceParts.push(String(volFeed.source))
  const uniqueSources = [...new Set(sourceParts)]

  let freshnessHeadline = 'Last updated: No live feed connected'
  if (hasValidatedLive && !stale) {
    freshnessHeadline = lastCheckedHm ? `Last checked: ${lastCheckedHm}` : 'Live feed connected'
  } else if (hasValidatedLive && stale) {
    freshnessHeadline = lastCheckedHm ? `Stale (last checked ${lastCheckedHm})` : 'Stale — refresh overdue'
  } else if (!hasValidatedLive && hasRawLiveAttempt) {
    freshnessHeadline = 'Live payload incomplete or rejected — open validation detail'
  }

  /** --- Intermarket detail (dataset, not live news) --- */
  const interLines = []
  const sup = Array.isArray(inter.supporting_drivers) ? inter.supporting_drivers : []
  const con = Array.isArray(inter.conflicting_drivers) ? inter.conflicting_drivers : []
  sup.slice(0, 4).forEach((t) => interLines.push({ tone: 'ok', text: clip(String(t), 96), provenance: provenanceSnapshot.intermarketSource }))
  con.slice(0, 3).forEach((t) => interLines.push({ tone: 'bad', text: clip(String(t), 96), provenance: provenanceSnapshot.intermarketSource }))
  if (!interLines.length) {
    interLines.push({
      tone: 'mid',
      text:
        intermarket === 'Data Missing'
          ? 'Intermarket confirmation and driver lists are absent on this row — regenerate confluence export if expected.'
          : conf
            ? `Row shows intermarket confirmation: ${conf}.`
            : 'No intermarket detail on this row.',
      provenance: provenanceSnapshot.intermarketSource,
    })
  }

  const disclosures = [
    'News flow and event risk show live labels only when row.market_environment_feed supplies validated items (see module docs).',
    'Macro uses the same-week confluence snapshot only — not a live macro desk feed.',
    `Intermarket lines above come from the exported row JSON (${provenanceSnapshot.intermarketSource}).`,
  ]

  return {
    summary: {
      macro,
      newsFlow: newsDisplay,
      intermarket,
      eventRisk: eventDisplay,
      volatility,
      weatherFeed: weatherLabel,
    },
    provenance: {
      macro: `${provenanceSnapshot.macroSource}${provenanceSnapshot.macroAsOf ? ` · As-of ${provenanceSnapshot.macroAsOf}` : ''}`,
      news:
        validatedNews.length > 0
          ? `Aggregated from ${validatedNews.length} validated item(s) with source + timestamps.`
          : 'No validated live news items — display shows Not Wired.',
      events:
        validatedEvents.length > 0
          ? `From ${validatedEvents.length} validated calendar/event item(s).`
          : 'No validated calendar/event items — display shows Not Wired.',
      volatility:
        volatility === 'NOT WIRED'
          ? 'Volatility: connect volatility_assessment in feed with level, source, fetched_at.'
          : `${volFeed?.source || 'Feed'} · fetched ${volFeed?.fetched_at || '—'}`,
    },
    freshness: {
      headline: freshnessHeadline,
      stale,
      last_checked_at: bundleChecked || (Number.isFinite(newestMs) ? new Date(newestMs).toISOString() : null),
      sources_line: uniqueSources.join(' · ') || null,
      no_live_feed: !hasValidatedLive && !hasRawLiveAttempt,
    },
    live: {
      news_items: validatedNews,
      event_items: validatedEvents,
      validation_errors: { news: newsErrors, events: eventErrors },
      weather_connected: weatherConnected,
    },
    detail: {
      intermarket: interLines,
      disclosures,
    },
    meta: { rulesVersion: 'v2-trust' },
  }
}
