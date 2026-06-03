import { getOpportunity } from './alignmentEngine.js'

export const ACTION_META = {
  high_attention: { label: 'HIGH ATTENTION', tone: 'violet', sort: 1 },
  pay_attention: { label: 'PAY ATTENTION', tone: 'emerald', sort: 2 },
  watch: { label: 'WATCH', tone: 'sky', sort: 3 },
  no_edge: { label: 'NO EDGE', tone: 'slate', sort: 4 },
  closed: { label: 'CLOSED', tone: 'slate', sort: 9 },
}

export function actionMeta(thesis) {
  const key = getOpportunity(thesis).action_key || 'no_edge'
  return ACTION_META[key] || ACTION_META.no_edge
}

export function sortByOpportunity(theses) {
  return [...theses].sort((a, b) => {
    const ra = getOpportunity(a).rank_score ?? -1
    const rb = getOpportunity(b).rank_score ?? -1
    return rb - ra
  })
}

export { getOpportunity, buildOpportunity } from './alignmentEngine.js'
