/** Minimal external store pattern for React useSyncExternalStore. */

export function createExternalStore(getSnapshot, subscribe) {
  return { getSnapshot, subscribe }
}
