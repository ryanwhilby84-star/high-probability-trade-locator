import { eventRiskBadge, eventRiskLabel } from '../macroCalendarCatalyst.js'
import { weeklyBiasLine } from '../weatherInterpretation.js'

export const TRADE_STATUSES = ['idea', 'planned', 'order_set', 'triggered', 'invalidated', 'closed']
export const TRADE_DIRECTIONS = ['long', 'short', 'flat']

export function buildJournalPrefill({ row, date, weatherContext, economicCalendar }) {
  const market = String(row?.market || '').trim()
  const weatherBlock = weatherContext?.markets?.[market]
  const wBias = weatherBlock?.weekly_bias_line || weeklyBiasLine(weatherContext, market)
  const catalyst = eventRiskLabel(eventRiskBadge(row, economicCalendar))

  return {
    market,
    symbol: '',
    direction: 'long',
    status: 'idea',
    entry_price: '',
    stop_loss: '',
    target_1: '',
    target_2: '',
    risk_amount: '',
    timeframe: '',
    setup_type: '',
    thesis: '',
    notes: '',
    cot_bias: String(row?.cot_bias || '').trim(),
    cot_score: row?.cot_score ?? '',
    macro_bias: String(row?.macro_regime || row?.macro_signal || '').trim(),
    weather_bias: wBias.replace(/^Weather bias this week:\s*/i, '').trim(),
    catalyst_risk: catalyst,
    dashboard_snapshot: {
      cot_calendar_week: date || '',
      cot_report_date: row?.latest_report_date || row?.date || '',
      positioning_state: row?.positioning_state || '',
      action_label: row?.cot_bias || '',
      macro_regime: row?.macro_regime || '',
      weather_bias_line: wBias,
      catalyst_risk: catalyst,
      logged_from: 'instrument_page',
    },
  }
}
