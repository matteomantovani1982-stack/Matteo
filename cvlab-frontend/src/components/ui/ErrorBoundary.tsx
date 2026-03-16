import { Component, ErrorInfo, ReactNode } from 'react'
import { captureError } from '../../monitoring/sentry'
import s from './ErrorBoundary.module.css'

interface Props {
  children: ReactNode
  fallback?: ReactNode
}

interface State {
  error: Error | null
}

export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null }

  static getDerivedStateFromError(error: Error): State {
    return { error }
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    captureError(error, { componentStack: info.componentStack ?? '' })
    console.error('ErrorBoundary caught:', error, info)
  }

  render() {
    if (this.state.error) {
      if (this.props.fallback) return this.props.fallback
      return (
        <div className={s.wrap}>
          <div className={s.card}>
            <div className={s.icon}>⚠</div>
            <div className={s.title}>Something went wrong</div>
            <div className={s.message}>
              {this.state.error.message || 'An unexpected error occurred.'}
            </div>
            <button
              className={s.btn}
              onClick={() => {
                this.setState({ error: null })
                window.location.reload()
              }}
            >
              Reload page
            </button>
          </div>
        </div>
      )
    }
    return this.props.children
  }
}
