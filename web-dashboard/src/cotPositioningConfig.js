/** COT positioning chart groups — Legacy Futures Only (visualization only, no scoring). */



export const LEGACY_POSITIONING_GROUPS = {

  'managed-money': {

    id: 'managed_money',

    tabLabel: 'Non-Commercial',

    title: 'Non-commercial positioning',

    subtitle: 'Legacy non-commercial / speculative (NC)',

    routeSlug: 'managed-money',

  },

  commercial: {

    id: 'commercial',

    tabLabel: 'Commercial',

    title: 'Commercial positioning',

    subtitle: 'Legacy commercial hedgers',

    routeSlug: 'commercial',

  },

  'non-reportable': {

    id: 'nonreportable',

    tabLabel: 'Non-Reportable',

    title: 'Non-reportable positioning',

    subtitle: 'Legacy non-reportable / retail proxy',

    routeSlug: 'non-reportable',

  },

  combined: {

    id: 'combined',

    tabLabel: 'Combined COT',

    title: 'Combined Legacy COT positioning',

    subtitle: 'Non-commercial, commercial, and non-reportable net',

    routeSlug: 'combined',

  },

}



/** @deprecated Use LEGACY_POSITIONING_GROUPS — kept for imports that expect commodity key. */

export const COMMODITY_POSITIONING_GROUPS = LEGACY_POSITIONING_GROUPS



/** @deprecated TFF groups removed — do not use. */

export const FINANCIAL_POSITIONING_GROUPS = LEGACY_POSITIONING_GROUPS



/** @deprecated Indices use Legacy NC, not TFF. */

export const FINANCIAL_FUTURES_MARKETS = new Set()



export function isFinancialFuturesMarket() {

  return false

}



export function cotPositioningProfile(market, row) {

  const fromRow = row?.cot_positioning_groups?.profile

  if (fromRow === 'legacy' || fromRow === 'commodity') return 'legacy'

  return 'legacy'

}



export function positioningGroupsForMarket() {

  return LEGACY_POSITIONING_GROUPS

}



export function normalizePositioningSlug(market, slug, row) {

  const groups = LEGACY_POSITIONING_GROUPS

  if (groups[slug]) return slug

  return 'managed-money'

}



export function resolvePositioningGroup(market, slug, row) {

  const norm = normalizePositioningSlug(market, slug, row)

  return LEGACY_POSITIONING_GROUPS[norm] || null

}



export function defaultPositioningSlug() {

  return 'managed-money'

}



export const POSITIONING_CHART_WEEKS_DEFAULT = 52

export const POSITIONING_BAND_WEEKS_13 = 13

export const POSITIONING_BAND_WEEKS_52 = 52

