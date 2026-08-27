/** Candlestick positioning workstation — set VITE_ENABLE_WORKSTATION=false to disable. */
export function isWorkstationCandlesEnabled() {
  try {
    return import.meta.env?.VITE_ENABLE_WORKSTATION !== 'false'
  } catch {
    return true
  }
}
