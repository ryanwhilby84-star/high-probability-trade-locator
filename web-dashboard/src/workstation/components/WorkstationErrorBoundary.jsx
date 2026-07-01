import React from 'react'

export class WorkstationErrorBoundary extends React.Component {
  constructor(props) {
    super(props)
    this.state = { error: null }
  }

  static getDerivedStateFromError(error) {
    return { error }
  }

  componentDidCatch(error, info) {
    console.error('[workstation] render error', error, info)
  }

  render() {
    if (this.state.error) {
      return (
        <section className="irw-root" id="instrument-research-workstation">
          <div className="irw-panel irw-empty">
            <p>Research workstation failed to render.</p>
            <p className="irw-muted">{String(this.state.error?.message || this.state.error)}</p>
          </div>
          {this.props.children}
        </section>
      )
    }
    return this.props.children
  }
}
