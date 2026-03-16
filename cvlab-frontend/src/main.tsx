import { StrictMode } from 'react'
import { createRoot } from 'react-dom/client'
import App from './App'
import { initSentry } from './monitoring/sentry'
import { ErrorBoundary } from './components/ui/ErrorBoundary'

initSentry()

createRoot(document.getElementById('root')!).render(
  <StrictMode>
    <ErrorBoundary>
      <App />
    </ErrorBoundary>
  </StrictMode>
)
