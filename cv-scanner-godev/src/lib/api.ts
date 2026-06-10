import axios from "axios"

const API_BASE_URL = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8005"
const AUTH_STORAGE_KEY = "cv-scanner-session"
const SESSION_EVENT = "cv-scanner-session-change"

export const http = axios.create({
  baseURL: API_BASE_URL,
  timeout: 60000,
})

http.interceptors.request.use(config => {
  if (typeof window === "undefined") return config

  try {
    const rawSession = window.localStorage.getItem(AUTH_STORAGE_KEY)
    const token = rawSession ? JSON.parse(rawSession)?.token : ""
    if (token) {
      config.headers.Authorization = `Bearer ${token}`
    }
  } catch {
    window.localStorage.removeItem(AUTH_STORAGE_KEY)
  }

  return config
})

http.interceptors.response.use(
  response => response,
  error => {
    if (typeof window !== "undefined" && error?.response?.status === 401) {
      window.localStorage.removeItem(AUTH_STORAGE_KEY)
      window.dispatchEvent(new Event(SESSION_EVENT))
    }
    return Promise.reject(error)
  }
)

export interface JobSummary {
  uuid: string
  title: string
  company: string
  experience_level: string
  years_of_experience_required: number
  location: string
}

export interface CandidateSummary {
  uuid: string
  full_name: string
  years_of_experience: number
  company_source: string
}

export interface MatchResult {
  uuid: string
  score: number
  rerank_score?: number
  search_method: string
  individual_scores: Record<string, number>
  properties: Record<string, unknown>
}

export interface Stats {
  total_candidates: number
  total_jobs: number
  by_company: Record<string, number>
  by_seniority: Record<string, number>
}

export interface AuthUserResponse {
  email: string
  name: string
  role: "admin" | "recruiter" | "manager" | "reader"
  permissions: string[]
  lastLoginAt: string
}

export interface LoginResponse {
  access_token: string
  token_type: string
  expires_in: number
  user: AuthUserResponse
}

export interface UploadDuplicate {
  uuid: string
  label: string
  company?: string
  reason: string
}

export type UploadCandidateResponse = {
  status: string
  name: string
  uuid: string
  message: string
  inserted?: boolean
  target_type?: "candidate"
  duplicates?: UploadDuplicate[]
}

export type UploadJobResponse = {
  status: string
  title: string
  uuid: string
  message: string
  inserted?: boolean
  target_type?: "job"
  duplicates?: UploadDuplicate[]
}

export const api = {
  login: (email: string, password: string): Promise<LoginResponse> =>
    http.post("/api/auth/login", { email, password }).then(r => r.data),

  me: (): Promise<AuthUserResponse> =>
    http.get("/api/auth/me").then(r => r.data),

  stats: (): Promise<Stats> =>
    http.get("/api/stats/").then(r => r.data),

  jobTitles: (): Promise<{ jobs: JobSummary[] }> =>
    http.get("/api/jobs/titles/").then(r => r.data),

  candidateNames: (): Promise<{ candidates: CandidateSummary[] }> =>
    http.get("/api/candidates/names/").then(r => r.data),

  job: (uuid: string) =>
    http.get(`/api/jobs/${uuid}`).then(r => r.data),

  candidate: (uuid: string) =>
    http.get(`/api/candidates/${uuid}`).then(r => r.data),

  candidatesForJob: (job_uuid: string, mode = "hybride", limit = 10) =>
    http.get("/api/search/candidates-for-job/", {
      params: { job_uuid, mode, limit }
    }).then(r => r.data),

  jobsForCandidate: (candidate_uuid: string, mode = "hybride", limit = 10) =>
    http.get("/api/search/jobs-for-candidate/", {
      params: { candidate_uuid, mode, limit }
    }).then(r => r.data),

  advancedSearch: (criteria: object) =>
    http.post("/api/search/advanced/", criteria).then(r => r.data),

  rerank: (query: string, candidates: MatchResult[], top_k = 10) =>
    http.post("/api/search/rerank/", { query, candidates, top_k }).then(r => r.data),

  uploadCandidateFile: (file: File, forceInsert = false): Promise<UploadCandidateResponse> => {
    const form = new FormData()
    form.append("file", file)
    form.append("force_insert", String(forceInsert))
    return http.post("/api/upload/candidate/file", form, {
      headers: { "Content-Type": "multipart/form-data" },
      timeout: 120000
    }).then(r => r.data)
  },

  uploadJobFile: (file: File, forceInsert = false): Promise<UploadJobResponse> => {
    const form = new FormData()
    form.append("file", file)
    form.append("force_insert", String(forceInsert))
    return http.post("/api/upload/job/file", form, {
      headers: { "Content-Type": "multipart/form-data" },
      timeout: 120000
    }).then(r => r.data)
  },

  uploadFromUrl: (url: string, type: "candidate" | "job", forceInsert = false) =>
    http.post("/api/upload/from-url", { url, type, force_insert: forceInsert }, { timeout: 120000 }).then(r => r.data),

  deleteCandidate: (uuid: string, confirm: boolean = false, scope: "archive" | "hard" = "archive") =>
    http.delete(`/api/candidates/${uuid}`, { params: { confirm, scope } }).then(r => r.data),

  deleteJob: (uuid: string, confirm: boolean = false, scope: "archive" | "hard" = "archive") =>
    http.delete(`/api/jobs/${uuid}`, { params: { confirm, scope } }).then(r => r.data),
}
