import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL ?? ''

export const api = axios.create({
  baseURL: BASE_URL,
  headers: { 'Content-Type': 'application/json' },
})

// Attach JWT + fix Content-Type for FormData (multipart must not have explicit Content-Type)
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('cvlab_token')
  if (token) config.headers.Authorization = `Bearer ${token}`

  // If body is FormData, delete the Content-Type header so the browser sets it
  // automatically with the correct multipart boundary
  if (config.data instanceof FormData) {
    delete config.headers['Content-Type']
  }

  return config
})

// On 401 → clear token and redirect to login
api.interceptors.response.use(
  (res) => res,
  (err) => {
    if (err.response?.status === 401) {
      localStorage.removeItem('cvlab_token')
      window.location.href = '/login'
    }
    return Promise.reject(err)
  }
)
