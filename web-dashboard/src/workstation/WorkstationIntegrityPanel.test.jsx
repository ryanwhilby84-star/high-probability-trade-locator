/**
 * @vitest-environment node
 */
import React from 'react'
import { describe, it, expect } from 'vitest'
import { renderToStaticMarkup } from 'react-dom/server'
import {
  WorkstationIntegrityPanel,
  WorkstationRenderErrorPanel,
} from './WorkstationIntegrityPanel.jsx'

describe('WorkstationIntegrityPanel', () => {
  it('renders DATA INTEGRITY FAILURE with missing fields and actions', () => {
    const html = renderToStaticMarkup(
      <WorkstationIntegrityPanel
        instrumentId="Crude Oil / CL"
        reportDate="2026-07-21"
        stage="Derived COT"
        missingFields={['commercial.percentile', 'cross_group.flow']}
        onRetry={() => {}}
      />,
    )
    expect(html).toContain('DATA INTEGRITY FAILURE')
    expect(html).toContain('Crude Oil / CL')
    expect(html).toContain('2026-07-21')
    expect(html).toContain('commercial.percentile')
    expect(html).toContain('Retry')
    expect(html).toContain('Back to Scanner')
    expect(html).not.toMatch(/^\s*$/)
  })

  it('renders WORKSTATION RENDERING ERROR without blanking', () => {
    const html = renderToStaticMarkup(
      <WorkstationRenderErrorPanel
        instrumentId="Crude Oil / CL"
        error={new Error('boom')}
        onRetry={() => {}}
      />,
    )
    expect(html).toContain('WORKSTATION RENDERING ERROR')
    expect(html).toContain('Error reference:')
    expect(html).toContain('boom')
  })
})
