/** Temporary render/viewport diagnostics — remove when stability pass is verified. */

const renders = new Map()
let fitAllCalls = 0
let setVisibleRangeCalls = 0
let fitContentCalls = 0
let chartMounts = 0
let chartUnmounts = 0
let lastInstrument = ''

export function setDiagInstrument(marketId) {
  lastInstrument = marketId || ''
}

export function bumpRender(component) {
  const key = `${lastInstrument}:${component}`
  renders.set(key, (renders.get(key) || 0) + 1)
}

export function recordFitAll() {
  fitAllCalls += 1
}

export function recordSetVisibleRange() {
  setVisibleRangeCalls += 1
}

export function recordFitContent() {
  fitContentCalls += 1
}

export function recordChartMount(panelId) {
  chartMounts += 1
  if (typeof console !== 'undefined' && import.meta.env?.DEV) {
    console.debug('[cot-ws-diag] chart mount', lastInstrument, panelId)
  }
}

export function recordChartUnmount(panelId) {
  chartUnmounts += 1
  if (typeof console !== 'undefined' && import.meta.env?.DEV) {
    console.debug('[cot-ws-diag] chart unmount', lastInstrument, panelId)
  }
}

export function resetDiagCounters() {
  fitAllCalls = 0
  setVisibleRangeCalls = 0
  fitContentCalls = 0
  chartMounts = 0
  chartUnmounts = 0
  renders.clear()
}

export function getDiagSnapshot() {
  return {
    instrument: lastInstrument,
    renders: Object.fromEntries(renders),
    fitAllCalls,
    setVisibleRangeCalls,
    fitContentCalls,
    chartMounts,
    chartUnmounts,
  }
}

export function logDiagSnapshot(label = 'cot-ws-diag') {
  if (typeof console === 'undefined') return
  console.info(`[${label}]`, getDiagSnapshot())
}

if (typeof window !== 'undefined' && import.meta.env?.DEV) {
  window.__COT_WS_DIAG__ = {
    get: getDiagSnapshot,
    log: logDiagSnapshot,
    reset: resetDiagCounters,
  }
}
