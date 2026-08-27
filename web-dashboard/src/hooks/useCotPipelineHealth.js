import React from 'react'

let _promise = null

export function useCotPipelineHealth() {
  const [doc, setDoc] = React.useState(null)

  React.useEffect(() => {
    if (!_promise) {
      _promise = fetch('/data/cot_pipeline_health.json')
        .then((r) => (r.ok ? r.json() : null))
        .catch(() => null)
    }
    _promise.then(setDoc)
  }, [])

  return doc
}
