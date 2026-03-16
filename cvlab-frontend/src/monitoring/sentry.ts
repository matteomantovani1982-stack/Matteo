/**
 * Frontend Sentry initialisation.
 * Called once in main.tsx before React renders.
 *
 * Captures:
 * - Unhandled JS errors + promise rejections
 * - React error boundary errors (via captureException)
 * - Failed API calls (axios interceptor breadcrumbs)
 * - Performance traces for page loads and API calls
 */
export function initSentry(): void {
  const dsn = import.meta.env.VITE_SENTRY_DSN
  if (!dsn) return

  import('@sentry/react').then(({ init, browserTracingIntegration }) => {
    init({
      dsn,
      environment: import.meta.env.VITE_APP_ENV ?? 'production',
      release: `cvlab-frontend@${import.meta.env.VITE_APP_VERSION ?? '1.0.0'}`,

      integrations: [
        browserTracingIntegration(),
      ],

      tracesSampleRate: 0.1,

      // Don't send errors from browser extensions or localhost
      beforeSend(event) {
        if (event.request?.url?.includes('localhost')) return null
        return event
      },
    })
  }).catch(() => {
    // Sentry not installed — silent fail
  })
}

export function captureError(error: unknown, context?: Record<string, unknown>): void {
  import('@sentry/react').then(({ captureException, withScope }) => {
    if (context) {
      withScope((scope) => {
        Object.entries(context).forEach(([k, v]) => scope.setExtra(k, v))
        captureException(error)
      })
    } else {
      captureException(error)
    }
  }).catch(() => {})
}
