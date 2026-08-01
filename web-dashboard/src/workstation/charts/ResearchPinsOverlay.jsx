import React from 'react'

/**
 * Compact event markers over a chart pane.
 * Positions from Lightweight Charts timeScale — paint-only, no camera mutation.
 * Hover abbreviation via native title; no permanent text boxes.
 */
export function ResearchPinsOverlay({
  chartRef,
  containerRef,
  pins = [],
  mounted = false,
  variant = 'cot', // 'price' | 'cot'
  onPinClick = null,
}) {
  const hostRef = React.useRef(null)
  const pinsRef = React.useRef(pins)
  pinsRef.current = pins

  React.useEffect(() => {
    const chart = chartRef?.current
    const host = hostRef.current
    if (!chart || !host || !mounted) {
      if (host) host.replaceChildren()
      return undefined
    }

    const timeScale = chart.timeScale()

    const relayout = () => {
      const width = host.clientWidth || containerRef?.current?.clientWidth || 0
      const frag = document.createDocumentFragment()
      const currentPins = pinsRef.current || []

      for (const pin of currentPins) {
        const time = Number(pin?.time)
        if (!Number.isFinite(time)) continue
        const x = timeScale.timeToCoordinate(time)
        if (x == null || !Number.isFinite(x)) continue
        if (width > 0 && (x < -2 || x > width + 2)) continue

        const btn = document.createElement('button')
        btn.type = 'button'
        const stack = Number(pin.stackIndex) || 0
        btn.className = [
          'cot-ws-research-pin',
          `cot-ws-research-pin--${pin.tone || 'extreme'}`,
          `cot-ws-research-pin--shape-${pin.shape || 'diamond'}`,
          pin.selected ? 'is-selected' : '',
          pin.bearish ? 'is-bearish' : 'is-bullish',
        ]
          .filter(Boolean)
          .join(' ')
        btn.style.left = `${Math.round(x)}px`
        btn.style.top = `${2 + stack * 10}px`
        btn.title = pin.title || `${pin.label || ''} · ${pin.date || ''}`
        btn.setAttribute('aria-label', btn.title)
        btn.addEventListener('pointerdown', (e) => e.stopPropagation())
        btn.addEventListener('mousedown', (e) => e.stopPropagation())
        btn.addEventListener('click', (e) => {
          e.stopPropagation()
          onPinClick?.(pin)
        })

        const mark = document.createElement('span')
        mark.className = 'cot-ws-research-pin-mark'
        mark.setAttribute('aria-hidden', 'true')
        btn.appendChild(mark)
        frag.appendChild(btn)
      }

      host.replaceChildren(frag)
    }

    relayout()
    try {
      timeScale.subscribeVisibleLogicalRangeChange(relayout)
    } catch {
      // ignore
    }
    const ro = containerRef?.current ? new ResizeObserver(relayout) : null
    if (ro && containerRef.current) ro.observe(containerRef.current)

    return () => {
      try {
        timeScale.unsubscribeVisibleLogicalRangeChange(relayout)
      } catch {
        // ignore
      }
      ro?.disconnect()
      host.replaceChildren()
    }
  }, [chartRef, containerRef, mounted, onPinClick, pins])

  return (
    <div
      ref={hostRef}
      className={`cot-ws-research-pins cot-ws-research-pins--${variant}`}
      aria-hidden="true"
    />
  )
}
