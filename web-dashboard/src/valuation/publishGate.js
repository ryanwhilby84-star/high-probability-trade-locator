/** Phase 3A — scanner may only display valuation % when publish=true. */

export function isValuationPublished(block) {
  if (!block || typeof block !== 'object') return false
  if (block.publish === true) return true
  if (block.publish === false) return false
  // Legacy exports pre-3A: wired implied publish.
  return block.wired === true
}

export function withheldReason(block) {
  if (!block) return 'Institutional publish gate failed'
  const raw =
    block.withheld_reason ||
    block.unavailable_reason ||
    block.valuation_reason ||
    'Institutional publish gate failed'
  return String(raw).replace(/^WITHHELD —\s*/i, '').trim()
}

export function withheldDisplayLine(block) {
  const reason = withheldReason(block)
  return reason ? `WITHHELD — ${reason}` : 'WITHHELD'
}
