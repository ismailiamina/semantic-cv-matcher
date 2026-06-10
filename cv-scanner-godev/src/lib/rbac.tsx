"use client"

import { createContext, useContext, useMemo, useSyncExternalStore } from "react"
import { ShieldAlert } from "lucide-react"
import { api } from "@/lib/api"

export type Role = "admin" | "recruiter" | "manager" | "reader"

export type Permission =
  | "viewCandidates"
  | "manageCandidates"
  | "viewOffers"
  | "manageOffers"
  | "matchCandidates"
  | "matchJobs"
  | "freeSearch"
  | "analytics"
  | "profile"
  | "activity"
  | "rerank"
  | "llm"

type RoleConfig = {
  label: string
  description: string
  permissions: Permission[]
}

export type AuthUser = {
  email: string
  name: string
  role: Role
  permissions: Permission[]
  token: string
  expiresAt: number
  lastLoginAt?: string
}

export const ROLE_CONFIG: Record<Role, RoleConfig> = {
  admin: {
    label: "Administrateur",
    description: "Acces complet a toutes les pages et actions.",
    permissions: [
      "viewCandidates",
      "manageCandidates",
      "viewOffers",
      "manageOffers",
      "matchCandidates",
      "matchJobs",
      "freeSearch",
      "analytics",
      "profile",
      "activity",
      "rerank",
      "llm",
    ],
  },
  recruiter: {
    label: "Recruteur RH",
    description: "Gestion des candidats, offres, matching et analyses IA.",
    permissions: [
      "viewCandidates",
      "manageCandidates",
      "viewOffers",
      "manageOffers",
      "matchCandidates",
      "matchJobs",
      "freeSearch",
      "analytics",
      "profile",
      "activity",
      "rerank",
      "llm",
    ],
  },
  manager: {
    label: "Manager",
    description: "Consultation, matching et decision sans modification des donnees.",
    permissions: [
      "viewCandidates",
      "viewOffers",
      "matchCandidates",
      "matchJobs",
      "freeSearch",
      "analytics",
      "profile",
      "activity",
      "rerank",
      "llm",
    ],
  },
  reader: {
    label: "Lecteur",
    description: "Consultation simple des donnees et du dashboard.",
    permissions: ["viewCandidates", "viewOffers", "analytics", "profile"],
  },
}

type AuthContextValue = {
  role: Role
  roleLabel: string
  user: AuthUser | null
  hydrated: boolean
  login: (email: string, password: string) => Promise<boolean>
  logout: () => void
  can: (permission: Permission) => boolean
}

const AuthContext = createContext<AuthContextValue | null>(null)
const STORAGE_KEY = "cv-scanner-session"
const SESSION_EVENT = "cv-scanner-session-change"

function isRole(value: unknown): value is Role {
  return value === "admin" || value === "recruiter" || value === "manager" || value === "reader"
}

function parseStoredUser(raw: string | null): AuthUser | null {
  if (!raw) return null

  try {
    const parsed = JSON.parse(raw) as Partial<AuthUser>
    if (
      parsed.email &&
      parsed.name &&
      isRole(parsed.role) &&
      parsed.token &&
      parsed.expiresAt &&
      parsed.expiresAt > Date.now()
    ) {
      return {
        email: parsed.email,
        name: parsed.name,
        role: parsed.role,
        permissions: (parsed.permissions || ROLE_CONFIG[parsed.role].permissions) as Permission[],
        token: parsed.token,
        expiresAt: parsed.expiresAt,
        lastLoginAt: parsed.lastLoginAt,
      }
    }
  } catch {
    return null
  }

  return null
}

function subscribeToSession(callback: () => void) {
  window.addEventListener(SESSION_EVENT, callback)
  window.addEventListener("storage", callback)
  return () => {
    window.removeEventListener(SESSION_EVENT, callback)
    window.removeEventListener("storage", callback)
  }
}

function getSessionSnapshot() {
  return window.localStorage.getItem(STORAGE_KEY) || ""
}

function getServerSessionSnapshot() {
  return ""
}

function notifySessionChange() {
  window.dispatchEvent(new Event(SESSION_EVENT))
}

export function AuthProvider({ children }: { children: React.ReactNode }) {
  const sessionSnapshot = useSyncExternalStore(
    subscribeToSession,
    getSessionSnapshot,
    getServerSessionSnapshot
  )
  const user = useMemo(() => parseStoredUser(sessionSnapshot), [sessionSnapshot])

  const login = async (email: string, password: string) => {
    try {
      const result = await api.login(email, password)
      const sessionUser: AuthUser = {
        email: result.user.email,
        name: result.user.name,
        role: result.user.role,
        permissions: result.user.permissions as Permission[],
        token: result.access_token,
        expiresAt: Date.now() + result.expires_in * 1000,
        lastLoginAt: result.user.lastLoginAt,
      }
      window.localStorage.setItem(STORAGE_KEY, JSON.stringify(sessionUser))
      notifySessionChange()
      return true
    } catch {
      return false
    }
  }

  const logout = () => {
    window.localStorage.removeItem(STORAGE_KEY)
    notifySessionChange()
  }

  const value = useMemo<AuthContextValue>(() => {
    const role = user?.role ?? "reader"
    const permissions = new Set(user?.permissions || ROLE_CONFIG[role].permissions)
    return {
      role,
      roleLabel: ROLE_CONFIG[role].label,
      user,
      hydrated: true,
      login,
      logout,
      can: permission => permissions.has(permission),
    }
  }, [user])

  return <AuthContext.Provider value={value}>{children}</AuthContext.Provider>
}

export function useAuth() {
  const value = useContext(AuthContext)
  if (!value) throw new Error("useAuth must be used inside AuthProvider")
  return value
}

export function RoleGuard({
  permission,
  children,
}: {
  permission: Permission
  children: React.ReactNode
}) {
  const { can, roleLabel, user } = useAuth()

  if (!user) {
    return (
      <div className="flex h-full items-center justify-center p-8" style={{ background: "#F0F4F8" }}>
        <div className="max-w-md rounded-2xl p-6 text-center" style={{ background: "white", border: "1px solid #E2E8F0" }}>
          <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-xl" style={{ background: "#EEF4FF", color: "#003B8E" }}>
            <ShieldAlert className="h-6 w-6" />
          </div>
          <h1 className="text-lg font-semibold" style={{ color: "#1A2B4B" }}>Connexion requise</h1>
          <p className="mt-2 text-sm leading-relaxed" style={{ color: "#64748B" }}>
            Veuillez vous connecter pour acceder a l&apos;application.
          </p>
        </div>
      </div>
    )
  }

  if (can(permission)) return <>{children}</>

  return (
    <div className="flex h-full items-center justify-center p-8" style={{ background: "#F0F4F8" }}>
      <div className="max-w-md rounded-2xl p-6 text-center" style={{ background: "white", border: "1px solid #E2E8F0" }}>
        <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-xl" style={{ background: "#FEF2F2", color: "#B91C1C" }}>
          <ShieldAlert className="h-6 w-6" />
        </div>
        <h1 className="text-lg font-semibold" style={{ color: "#1A2B4B" }}>Acces non autorise</h1>
        <p className="mt-2 text-sm leading-relaxed" style={{ color: "#64748B" }}>
          Le role actuel, {roleLabel}, ne dispose pas de cette permission.
        </p>
      </div>
    </div>
  )
}
