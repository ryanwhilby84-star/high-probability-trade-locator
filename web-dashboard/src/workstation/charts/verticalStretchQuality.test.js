/**
 * @vitest-environment node
 */
import { describe, expect, it } from 'vitest'

import {
  VERTICAL_STRETCH_DEFAULTS,
  clampVerticalStretch,
  magnifyByAxisDrag,
} from './verticalStretch.js'

describe('COT vertical stretch quality bounds', () => {
  it('never compresses an indicator into the old near-flat range', () => {
    expect(VERTICAL_STRETCH_DEFAULTS.minFactor).toBeGreaterThanOrEqual(0.7)
    expect(clampVerticalStretch(0.25)).toBe(VERTICAL_STRETCH_DEFAULTS.minFactor)
    expect(magnifyByAxisDrag(1, 10000)).toBe(VERTICAL_STRETCH_DEFAULTS.minFactor)
  })

  it('caps extreme magnification and keeps normal drags gradual', () => {
    expect(clampVerticalStretch(100)).toBe(VERTICAL_STRETCH_DEFAULTS.maxFactor)
    const afterNormalDrag = magnifyByAxisDrag(1, 40)
    expect(afterNormalDrag).toBeGreaterThan(VERTICAL_STRETCH_DEFAULTS.minFactor)
    expect(afterNormalDrag).toBeLessThan(1)
  })
})
