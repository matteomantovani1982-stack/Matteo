import { api } from './client'
import type { CandidateProfile, UsageSummary } from '../types'

export interface ExperienceEntry {
  id: string
  profile_id: string
  company_name: string
  role_title: string
  start_date?: string | null
  end_date?: string | null
  location?: string | null
  context?: string | null
  contribution?: string | null
  outcomes?: string | null
  methods?: string | null
  hidden?: string | null
  freeform?: string | null
  display_order: number
  is_complete: boolean
  fields_completed: number
}

export const profileApi = {
  get: async (): Promise<CandidateProfile | null> => {
    try {
      // First get the list to find the profile id
      const listRes = await api.get<CandidateProfile[]>('/api/v1/profiles')
      const list = listRes.data
      if (!list || list.length === 0) return null
      // Then fetch the full profile with experiences
      const fullRes = await api.get<CandidateProfile>(`/api/v1/profiles/${list[0].id}`)
      return fullRes.data
    } catch (e: unknown) {
      if ((e as { response?: { status?: number } }).response?.status === 404) return null
      throw e
    }
  },

  create: async (data: Partial<CandidateProfile>): Promise<CandidateProfile> => {
    const res = await api.post<CandidateProfile>('/api/v1/profiles', data)
    return res.data
  },

  update: async (profileId: string, data: Partial<CandidateProfile>): Promise<CandidateProfile> => {
    const res = await api.patch<CandidateProfile>(`/api/v1/profiles/${profileId}`, data)
    return res.data
  },

  // Experience CRUD
  addExperience: async (profileId: string, data: Partial<ExperienceEntry>): Promise<ExperienceEntry> => {
    const res = await api.post<ExperienceEntry>(`/api/v1/profiles/${profileId}/experiences`, data)
    return res.data
  },

  updateExperience: async (profileId: string, expId: string, data: Partial<ExperienceEntry>): Promise<ExperienceEntry> => {
    const res = await api.patch<ExperienceEntry>(`/api/v1/profiles/${profileId}/experiences/${expId}`, data)
    return res.data
  },

  deleteExperience: async (profileId: string, expId: string): Promise<void> => {
    await api.delete(`/api/v1/profiles/${profileId}/experiences/${expId}`)
  },

  importFromCV: async (profileId: string, cvId: string) => {
    const res = await api.post(`/api/v1/profiles/${profileId}/import-cv`, { cv_id: cvId })
    return res.data
  },

  importFromLinkedIn: async (profileId: string, linkedinUrl: string, pasteText = '') => {
    const res = await api.post(`/api/v1/profiles/${profileId}/import-linkedin`, { linkedin_url: linkedinUrl, paste_text: pasteText })
    return res.data
  },

  applyImport: async (profileId: string, imported: object, overwriteExisting = false) => {
    const res = await api.post(`/api/v1/profiles/${profileId}/apply-import`, { imported, overwrite_existing: overwriteExisting })
    return res.data
  },

  usage: async (): Promise<UsageSummary> => {
    const res = await api.get<UsageSummary>('/api/v1/billing/status')
    return res.data
  },
}
