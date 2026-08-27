import { describe, expect, it } from 'vitest'

import { isRadarEligible, RADAR_ELIGIBLE } from './radarEligibility.js'
import { canonicalMarketId } from './marketResolution.js'

describe('DXY radar discoverability', () => {
  it('includes US Dollar Index / DX in radar-eligible navigable markets', () => {
    expect(RADAR_ELIGIBLE.has('US Dollar Index / DX')).toBe(true)
    expect(isRadarEligible('US Dollar Index / DX')).toBe(true)
  })

  it('resolves common DXY aliases to the canonical instrument id', () => {
    expect(canonicalMarketId('DXY')).toBe('US Dollar Index / DX')
    expect(canonicalMarketId('Dixie')).toBe('US Dollar Index / DX')
    expect(canonicalMarketId('US Dollar Index')).toBe('US Dollar Index / DX')
  })
})
