import { api } from './client'

export const scoutApi = {
  getSignals: async () => {
    const res = await api.get('/api/v1/scout/signals')
    return res.data
  },

  getJobs: async (params?: { query?: string; region?: string; sector?: string }) => {
    const res = await api.get('/api/v1/scout/jobs', { params })
    return res.data
  },

  getConfig: async () => {
    const res = await api.get('/api/v1/scout/config')
    return res.data
  },
}
