import { api } from './client'
import type {
  CV, CVStatusResponse, BestPracticesResponse, CVTemplate,
} from '../types'

export const cvsApi = {
  list: async (): Promise<CV[]> => {
    const res = await api.get<CV[]>('/api/v1/cvs')
    return res.data
  },

  upload: async (file: File, onProgress?: (pct: number) => void): Promise<CV> => {
    const form = new FormData()
    form.append('file', file)
    const res = await api.post<CV>('/api/v1/cvs', form, {
      onUploadProgress: (e) => {
        if (onProgress && e.total) onProgress(Math.round((e.loaded / e.total) * 100))
      },
    })
    return res.data
  },

  status: async (cvId: string): Promise<CVStatusResponse> => {
    const res = await api.get<CVStatusResponse>(`/api/v1/cvs/${cvId}`)
    return res.data
  },

  feedback: async (cvId: string): Promise<BestPracticesResponse> => {
    const res = await api.get<BestPracticesResponse>(`/api/v1/cv-builder/feedback/${cvId}`)
    return res.data
  },

  templates: async (): Promise<CVTemplate[]> => {
    const res = await api.get<CVTemplate[]>('/api/v1/cv-builder/templates')
    return res.data
  },

  setMode: async (cvId: string, buildMode: string, templateSlug?: string): Promise<CVStatusResponse> => {
    const res = await api.patch<CVStatusResponse>(`/api/v1/cv-builder/mode/${cvId}`, {
      build_mode: buildMode,
      template_slug: templateSlug,
    })
    return res.data
  },

  createFromScratch: async (): Promise<CV> => {
    const res = await api.post<CV>('/api/v1/cv-builder/scratch')
    return res.data
  },
}
