import { api } from './client'
import type { JobRun, JobRunSummary, JobRunCreateRequest, JDExtractResponse } from '../types'

export const jobsApi = {
  listCVs: async () => {
    const res = await api.get('/api/v1/cvs')
    return res.data
  },
  list: async (): Promise<JobRunSummary[]> => {
    const res = await api.get<JobRunSummary[]>('/api/v1/jobs')
    return res.data
  },

  get: async (runId: string): Promise<JobRun> => {
    const res = await api.get<JobRun>(`/api/v1/jobs/${runId}`)
    return res.data
  },

  create: async (payload: JobRunCreateRequest): Promise<JobRun> => {
    const res = await api.post<JobRun>('/api/v1/jobs', payload)
    return res.data
  },

  extractJd: async (url: string): Promise<JDExtractResponse> => {
    const res = await api.post<JDExtractResponse>('/api/v1/jobs/extract-jd', { url })
    return res.data
  },

  downloadUrl: async (runId: string): Promise<{ url: string }> => {
    const res = await api.get<{ download_url: string }>(`/api/v1/jobs/${runId}/download`)
    return { url: res.data.download_url }
  },
  updateStage: async (runId: string, stage: string, notes?: string): Promise<void> => {
    await api.patch(`/api/v1/jobs/${runId}/stage`, { stage, notes })
  },
}