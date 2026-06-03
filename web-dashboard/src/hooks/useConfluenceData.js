import React from 'react'

import {

  TRACKED_MARKET_IDS as TRACKED_MARKETS,

  canonicalMarketId as canonical,

  normalizeReportDate as normalizeDate,

  recordCalendarDate,

  recordCotReportDate,

  resolveRowForMarketWeek,

  defaultDashboardWeek,

  logCotResolutionForWeek,

  isCotRowResolved,

} from '../marketResolution.js'

import { setInstrumentRegistry, allInstrumentIds } from '../instrumentRegistry.js'

import { enrichRowHistoryContext } from '../legacy/dashboardLegacy.jsx'



const rowDate = (r = {}) => recordCalendarDate(r) || recordCotReportDate(r) || ''



function hasRealValue(v) {

  if (v === null || v === undefined) return false

  if (typeof v === 'string') {

    const s = v.trim().toLowerCase()

    if (!s || s === 'n/a' || s === 'nan' || s === 'null' || s === 'undefined' || s === '—') return false

  }

  return true

}



const completenessScore = (row = {}) => {

  const fields = [

    row.raw_cftc_market_name,

    row.long_value,

    row.short_value,

    row.net_value,

    row.one_week_net_change,

    row.four_week_net_change,

    row.cot_bias,

    row.cot_score,

    row.macro_regime || row.macro_signal,

    row.macro_score,

    row.positioning_state,

    row.institutional_flow_summary,

  ]

  return fields.reduce((acc, v) => acc + (hasRealValue(v) ? 1 : 0), 0)

}



const sanitizeInvalidNumericLiterals = (text = '') => text.replace(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g, 'null')



const sanitizeObject = (value, stats = { sanitized: false, replacements: 0 }) => {

  if (Array.isArray(value)) return value.map((item) => sanitizeObject(item, stats))

  if (value && typeof value === 'object') {

    return Object.fromEntries(Object.entries(value).map(([k, v]) => [k, sanitizeObject(v, stats)]))

  }

  if (value === undefined || value === null) return null

  if (typeof value === 'number' && !Number.isFinite(value)) {

    stats.sanitized = true

    stats.replacements += 1

    return null

  }

  return value

}



const safeJsonParse = (text = '') => {

  try {

    return { parsed: JSON.parse(text), sanitized: false, replacements: 0 }

  } catch (err) {

    const repaired = sanitizeInvalidNumericLiterals(text)

    const replacements = ((text.match(/\b(?:NaN|Infinity|-Infinity|undefined)\b/g)) || []).length

    const parsed = JSON.parse(repaired)

    return { parsed, sanitized: true, replacements, parseError: err }

  }

}



async function fetchJsonText(url) {

  const res = await fetch(url)

  if (!res.ok) {

    const err = new Error(`HTTP ${res.status} loading ${url}`)

    err.status = res.status

    throw err

  }

  return res.text()

}



function macroMapsFromPayload(payload) {

  const embedded = payload?.macro_relationship_maps

  if (embedded && typeof embedded === 'object') return embedded

  return {}

}



function normalizeRecords(payload) {

  if (!payload || typeof payload !== 'object') return []

  const rows = payload.records

  if (!Array.isArray(rows)) return []

  return rows.filter((r) => r && typeof r === 'object')

}



export function useConfluenceData() {

  const [data, setData] = React.useState([])

  const [date, setDate] = React.useState('')

  const [latestCotReportDate, setLatestCotReportDate] = React.useState('')

  const [macroRelationshipMaps, setMacroRelationshipMaps] = React.useState({})

  const [globalRegimeFromPayload, setGlobalRegimeFromPayload] = React.useState(null)

  const [loading, setLoading] = React.useState(true)

  const [error, setError] = React.useState(null)

  const [economicCalendar, setEconomicCalendar] = React.useState(null)

  const [weatherContext, setWeatherContext] = React.useState(null)

  const [weatherLoadError, setWeatherLoadError] = React.useState(null)

  const [cotFeedStatus, setCotFeedStatus] = React.useState(null)
  const [scannerAttentionWeek, setScannerAttentionWeek] = React.useState(null)
  const [priorityDebug, setPriorityDebug] = React.useState(null)
  const [relativeStrength, setRelativeStrength] = React.useState(null)
  const [payloadGeneratedAt, setPayloadGeneratedAt] = React.useState(null)



  React.useEffect(() => {

    fetch('/data/economic_calendar_latest.json')

      .then((r) => {

        if (!r.ok) {

          return {

            wired: false,

            message: 'NOT WIRED — run environment feed update after setting FINNHUB_API_KEY in .env',

            events: [],

            event_risk_by_market: {},

          }

        }

        return r.json()

      })

      .then((doc) => setEconomicCalendar(doc && typeof doc === 'object' ? doc : null))

      .catch(() =>

        setEconomicCalendar({

          wired: false,

          message: 'NOT WIRED — economic_calendar_latest.json missing; run environment feed update',

          events: [],

          event_risk_by_market: {},

        }),

      )

  }, [])



  React.useEffect(() => {

    fetch('/data/weather_context_latest.json')

      .then((r) => {

        if (!r.ok) {

          setWeatherContext(null)

          setWeatherLoadError(`Failed to load weather_context_latest.json: HTTP ${r.status}`)

          return null

        }

        return r.json()

      })

      .then((doc) => {

        if (doc && typeof doc === 'object') {

          setWeatherContext(doc)

          setWeatherLoadError(null)

        }

      })

      .catch((e) => {

        setWeatherContext(null)

        setWeatherLoadError(e?.message || 'Failed to fetch weather_context_latest.json')

      })

  }, [])



  React.useEffect(() => {

    let cancelled = false

    setLoading(true)



    const applyPayload = (payload, mapsOverride) => {

      const stats = { sanitized: false, replacements: 0 }

      const payloadClean = sanitizeObject(payload, stats)

      const rows = normalizeRecords(payloadClean)

      const metaLatest = payloadClean?.latest_cot_report_date != null ? String(payloadClean.latest_cot_report_date) : ''

      const cfs = payloadClean?.cot_feed_status

      const mrm =

        mapsOverride && typeof mapsOverride === 'object'

          ? mapsOverride

          : macroMapsFromPayload(payloadClean)

      const gro = payloadClean?.global_market_regime_latest_week

      const ds = [...new Set(rows.map(rowDate).filter(Boolean))].sort()



      if (cancelled) return

      setData(rows)

      setLatestCotReportDate(metaLatest)

      setCotFeedStatus(cfs && typeof cfs === 'object' ? cfs : null)

      setDate(defaultDashboardWeek(rows, metaLatest || ds.at(-1) || ''))

      setMacroRelationshipMaps(mrm)

      setGlobalRegimeFromPayload(gro && typeof gro === 'object' ? gro : null)

      const saw = payloadClean?.scanner_attention_week

      setScannerAttentionWeek(saw && typeof saw === 'object' ? saw : null)

      setPayloadGeneratedAt(payloadClean?.generated_at != null ? String(payloadClean.generated_at) : null)

      const genQ = payloadClean?.generated_at ? `?v=${encodeURIComponent(String(payloadClean.generated_at))}` : ''
      fetch(`/data/priority_debug_latest.json${genQ}`)
        .then((r) => (r.ok ? r.json() : null))
        .then((doc) => {
          if (doc && !cancelled) setPriorityDebug(doc)
        })
        .catch(() => {
          if (!cancelled) setPriorityDebug(null)
        })

      fetch(`/data/relative_strength_latest.json${genQ}`)
        .then((r) => (r.ok ? r.json() : null))
        .then((doc) => {
          if (doc && !cancelled) setRelativeStrength(doc)
        })
        .catch(() => {
          if (!cancelled) setRelativeStrength(null)
        })

      if (payloadClean?.instrument_registry) {
        setInstrumentRegistry(payloadClean.instrument_registry)
      } else {
        fetch('/data/instrument_registry.json')
          .then((r) => (r.ok ? r.json() : null))
          .then((reg) => {
            if (reg && !cancelled) setInstrumentRegistry(reg)
          })
          .catch(() => {})
      }

      setError(null)

    }



    const fail = (message) => {

      if (cancelled) return

      setData([])

      setLatestCotReportDate('')

      setCotFeedStatus(null)

      setMacroRelationshipMaps({})

      setGlobalRegimeFromPayload(null)

      setScannerAttentionWeek(null)

      setPriorityDebug(null)

      setPayloadGeneratedAt(null)

      setError(message)

    }



    ;(async () => {

      try {

        const [confluenceText, mapsText] = await Promise.all([

          fetchJsonText('/data/confluence_history_latest.json'),

          fetchJsonText('/data/macro_relationship_maps_latest.json').catch(() => null),

        ])



        const confluenceResult = safeJsonParse(confluenceText)

        const payload = confluenceResult.parsed



        let maps = macroMapsFromPayload(payload)

        if (mapsText) {

          try {

            const mapsPayload = safeJsonParse(mapsText).parsed

            const split = mapsPayload?.macro_relationship_maps

            if (split && typeof split === 'object') maps = split

          } catch {

            /* macro maps are optional for scanner; instrument charts degrade gracefully */

          }

        }



        applyPayload(payload, maps)

      } catch (e) {

        const msg = String(e?.message || '')

        if (msg.includes('Failed to fetch') || e?.name === 'TypeError') {

          fail(

            'Failed to load confluence_history_latest.json (network error or file too large). Re-run: python -m hptl.confluence.build_decision_table',

          )

        } else if (e instanceof SyntaxError || msg.includes('JSON')) {

          fail(`Invalid confluence JSON: ${msg}`)

        } else {

          fail(msg || 'Failed to load data')

        }

      } finally {

        if (!cancelled) setLoading(false)

      }

    })()



    return () => {

      cancelled = true

    }

  }, [])



  const dates = React.useMemo(() => [...new Set(data.map((r) => normalizeDate(rowDate(r))).filter(Boolean))].sort(), [data])



  const week = React.useMemo(

    () =>

      data

        .filter((r) => normalizeDate(rowDate(r)) === normalizeDate(date))

        .map((r) => {

          const marketSource = r.market || r.raw_cftc_market_name || ''

          return { ...r, market_key: canonical(marketSource) }

        }),

    [data, date],

  )



  React.useEffect(() => {

    if (!date || !data.length) return

    const tracked = allInstrumentIds()

    logCotResolutionForWeek(data, date, tracked)

  }, [data, date])



  const trackedMarketsList = React.useMemo(() => {
    const ids = allInstrumentIds()
    return ids.length ? ids : [...TRACKED_MARKETS]
  }, [data])



  const marketRows = React.useMemo(() => {

    const weekNorm = normalizeDate(date)

    const byMarket = new Map()

    week.forEach((row) => {

      if (!trackedMarketsList.includes(row.market_key)) return

      const prev = byMarket.get(row.market_key)

      if (!prev) {

        byMarket.set(row.market_key, row)

        return

      }

      if (completenessScore(row) > completenessScore(prev)) byMarket.set(row.market_key, row)

    })

    return trackedMarketsList.map((market) => {

      let row = byMarket.get(market)

      if (!isCotRowResolved(row)) {

        const resolved = resolveRowForMarketWeek(data, market, weekNorm)

        if (resolved.row) {

          row = {

            ...resolved.row,

            market_key: market,

            market,

            _cot_resolve_mode: resolved.matchMode,

            _cot_calendar_week: weekNorm,

          }

        }

      } else if (row) {

        row = { ...row, market, market_key: market }

      }

      if (!row) {

        return {

          market,

          market_key: market,

          cot_bias: 'N/A',

          cot_score: null,

          macro_regime: 'N/A',

          positioning_state: 'N/A',

          institutional_flow_summary: 'N/A: no COT row for this market and date',

          latest_report_date: null,

          one_week_long_change: null,

          one_week_short_change: null,

          one_week_net_change: null,

        }

      }

      try {

        return enrichRowHistoryContext({ ...row, market, market_key: market })

      } catch {

        return { ...row, market, market_key: market }

      }

    })

  }, [data, date, week, trackedMarketsList])



  const peersByMarket = React.useMemo(() => {

    const m = {}

    marketRows.forEach((r) => {

      if (r?.market) m[r.market] = r

    })

    return m

  }, [marketRows])



  const globalMarketRegime = React.useMemo(() => {

    const fromRow = marketRows.find((r) => r?.global_market_regime && typeof r.global_market_regime === 'object')

    return fromRow?.global_market_regime || globalRegimeFromPayload

  }, [marketRows, globalRegimeFromPayload])



  return {

    data,

    date,

    setDate,

    dates,

    latestCotReportDate,

    macroRelationshipMaps,

    marketRows,

    peersByMarket,

    globalMarketRegime,

    loading,

    error,

    trackedMarkets: trackedMarketsList,

    economicCalendar,

    weatherContext,

    weatherLoadError,

    cotFeedStatus,

    scannerAttentionWeek,

    priorityDebug,

    relativeStrength,

    payloadGeneratedAt,

  }

}

