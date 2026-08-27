/** Fetch a public JSON artifact without browser or module-level caching. */
export function fetchPublicJson(path) {
  const url = `${path}${path.includes('?') ? '&' : '?'}v=${Date.now()}`
  return fetch(url, { cache: 'no-store' }).then((r) => (r.ok ? r.json() : null))
}
