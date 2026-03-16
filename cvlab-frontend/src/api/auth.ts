import { api } from './client'
import type { User } from '../types'

export interface TokenResponse {
  access_token: string
  refresh_token: string
  token_type: string
}

export interface RegisterResponse {
  user: User
  access_token: string
  refresh_token: string
}

export const authApi = {
  // Register — returns user + tokens in one call
  register: async (email: string, password: string, fullName: string): Promise<RegisterResponse> => {
    const res = await api.post<RegisterResponse>('/api/v1/auth/register', {
      email,
      password,
      full_name: fullName,
    })
    return res.data
  },

  // Login — JSON body (not OAuth2 form)
  login: async (email: string, password: string): Promise<TokenResponse> => {
    const res = await api.post<TokenResponse>('/api/v1/auth/login', {
      email,
      password,
    })
    return res.data
  },

  me: async (): Promise<User> => {
    const res = await api.get<User>('/api/v1/auth/me')
    return res.data
  },

  logout: async (): Promise<void> => {
    await api.post('/api/v1/auth/logout')
  },
}
