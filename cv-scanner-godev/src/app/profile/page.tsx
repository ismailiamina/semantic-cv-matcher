"use client"

import { useRouter } from "next/navigation"
import { CheckCircle2, Clock3, LogOut, ShieldCheck, UserRound } from "lucide-react"
import { Topbar } from "@/components/layout/Topbar"
import { ROLE_CONFIG, RoleGuard, useAuth, type Permission } from "@/lib/rbac"

const PERMISSION_LABELS: Record<Permission, string> = {
  viewCandidates: "Consulter les candidats",
  manageCandidates: "Ajouter et archiver les candidats",
  viewOffers: "Consulter les offres",
  manageOffers: "Ajouter et archiver les offres",
  matchCandidates: "Lancer le matching offre vers candidats",
  matchJobs: "Lancer le matching candidat vers offres",
  freeSearch: "Utiliser la recherche libre",
  analytics: "Consulter les tableaux de bord",
  profile: "Consulter la session active",
  activity: "Consulter l'historique des actions",
  rerank: "Relancer le reranking",
  llm: "Utiliser les analyses LLM",
}

function formatDate(value?: string) {
  if (!value) return "Session locale"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "Session locale"
  return new Intl.DateTimeFormat("fr-FR", {
    dateStyle: "long",
    timeStyle: "short",
  }).format(date)
}

export default function ProfilePage() {
  const router = useRouter()
  const { user, role, roleLabel, logout } = useAuth()
  const permissions = user?.permissions ?? ROLE_CONFIG[role].permissions

  const handleLogout = () => {
    logout()
    router.replace("/login")
  }

  return (
    <RoleGuard permission="profile">
      <div className="flex h-full flex-col overflow-hidden" style={{ background: "#F0F4F8" }}>
        <Topbar title="Profil et session" />

        <div className="flex-1 overflow-y-auto p-6">
          <div className="grid grid-cols-[0.9fr_1.1fr] gap-5">
            <section className="rounded-2xl p-5" style={{ background: "white", border: "1px solid #E2E8F0" }}>
              <div className="flex items-start gap-4">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl" style={{ background: "#EEF4FF", color: "#003B8E" }}>
                  <UserRound className="h-7 w-7" />
                </div>
                <div className="min-w-0 flex-1">
                  <p className="text-xl font-bold" style={{ color: "#10213F" }}>
                    {user?.name || "Utilisateur"}
                  </p>
                  <p className="mt-1 truncate text-sm" style={{ color: "#64748B" }}>
                    {user?.email || "session.locale"}
                  </p>
                </div>
              </div>

              <div className="mt-6 space-y-3">
                <InfoRow label="Role actif" value={roleLabel} icon={<ShieldCheck className="h-4 w-4" />} />
                <InfoRow label="Derniere connexion" value={formatDate(user?.lastLoginAt)} icon={<Clock3 className="h-4 w-4" />} />
              </div>

              <div className="mt-5 rounded-2xl p-4" style={{ background: "#F0FDF4", border: "1px solid #BBF7D0" }}>
                <div className="flex items-center gap-2">
                  <CheckCircle2 className="h-4 w-4" style={{ color: "#059669" }} />
                  <p className="text-sm font-semibold" style={{ color: "#065F46" }}>
                    Acces interne Go & Dev
                  </p>
                </div>
                <p className="mt-2 text-xs leading-5" style={{ color: "#047857" }}>
                  La session conserve les informations du compte et applique les droits visibles dans cette interface.
                </p>
              </div>

              <button
                onClick={handleLogout}
                className="mt-5 flex w-full items-center justify-center gap-2 rounded-xl px-4 py-3 text-sm font-semibold transition-all"
                style={{ background: "#EF4444", color: "white" }}
              >
                <LogOut className="h-4 w-4" />
                Se deconnecter
              </button>
            </section>

            <section className="rounded-2xl p-5" style={{ background: "white", border: "1px solid #E2E8F0" }}>
              <div className="mb-4">
                <p className="text-xs font-semibold uppercase tracking-[0.18em]" style={{ color: "#0066CC" }}>
                  Permissions visibles
                </p>
                <h2 className="mt-2 text-lg font-bold" style={{ color: "#10213F" }}>
                  Modules accessibles pour ce profil
                </h2>
                <p className="mt-2 text-sm leading-6" style={{ color: "#64748B" }}>
                  Les pages et actions sensibles sont filtrees selon le role actif.
                </p>
              </div>

              <div className="grid grid-cols-2 gap-3">
                {permissions.map(permission => (
                  <div
                    key={permission}
                    className="rounded-xl p-3"
                    style={{ background: "#F8FAFC", border: "1px solid #E2E8F0" }}
                  >
                    <div className="flex items-start gap-2">
                      <CheckCircle2 className="mt-0.5 h-4 w-4 flex-shrink-0" style={{ color: "#0066CC" }} />
                      <p className="text-sm font-medium leading-5" style={{ color: "#1A2B4B" }}>
                        {PERMISSION_LABELS[permission]}
                      </p>
                    </div>
                  </div>
                ))}
              </div>
            </section>
          </div>
        </div>
      </div>
    </RoleGuard>
  )
}

function InfoRow({ label, value, icon }: { label: string; value: string; icon: React.ReactNode }) {
  return (
    <div className="flex items-center gap-3 rounded-xl px-4 py-3" style={{ background: "#F8FAFC", border: "1px solid #E2E8F0" }}>
      <div className="flex h-9 w-9 items-center justify-center rounded-xl" style={{ background: "#EEF4FF", color: "#003B8E" }}>
        {icon}
      </div>
      <div>
        <p className="text-xs" style={{ color: "#94A3B8" }}>{label}</p>
        <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>{value}</p>
      </div>
    </div>
  )
}
