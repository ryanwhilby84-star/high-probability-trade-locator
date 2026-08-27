import React from 'react'

const WeeklyTimelineContext = React.createContext(null)

export function WeeklyTimelineProvider({ value, children }) {
  return <WeeklyTimelineContext.Provider value={value}>{children}</WeeklyTimelineContext.Provider>
}

export function useWeeklyTimeline() {
  const ctx = React.useContext(WeeklyTimelineContext)
  if (!ctx) {
    throw new Error('useWeeklyTimeline requires WeeklyTimelineProvider')
  }
  return ctx
}

export function useWeeklyTimelineOptional() {
  return React.useContext(WeeklyTimelineContext)
}
