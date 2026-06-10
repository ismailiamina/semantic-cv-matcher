"use client"
import Link from "next/link"
import { usePathname } from "next/navigation"
import { Target, Briefcase, Search, BarChart3, Users, FileText, LogOut, UserRound, History } from "lucide-react"
import { GoDevLogo } from "@/components/GoDevLogo"
import { type Permission, useAuth } from "@/lib/rbac"

const nav = [
  {
    section: "Gestion",
    items: [
      { href: "/candidates", icon: Users,    label: "Candidats", permission: "viewCandidates" as Permission },
      { href: "/offers",     icon: FileText, label: "Offres",    permission: "viewOffers" as Permission },
    ]
  },
  {
    section: "Matching",
    items: [
      { href: "/match",     icon: Target,    label: "Candidats -> Offre", permission: "matchCandidates" as Permission },
      { href: "/jobs",      icon: Briefcase, label: "Offres -> Candidat", permission: "matchJobs" as Permission },
      { href: "/search",    icon: Search,    label: "Recherche libre",    permission: "freeSearch" as Permission },
    ]
  },
  {
    section: "Analytics",
    items: [
      { href: "/analytics", icon: BarChart3, label: "Dashboard", permission: "analytics" as Permission },
    ]
  },
  {
    section: "Session",
    items: [
      { href: "/profile", icon: UserRound, label: "Profil", permission: "profile" as Permission },
      { href: "/activity", icon: History, label: "Historique", permission: "activity" as Permission },
    ]
  }
]

export function Sidebar() {
  const path = usePathname()
  const { roleLabel, user, logout, can } = useAuth()
  return (
    <div className="w-56 flex-shrink-0 flex flex-col"
         style={{ background: "#00245A", borderRight: "1px solid rgba(255,255,255,0.07)" }}>

      {/* Logo */}
      <div className="px-5 py-5"
           style={{ borderBottom: "1px solid rgba(255,255,255,0.1)" }}>
        <GoDevLogo size={28} />
        <p className="text-xs mt-1.5" style={{ color: "rgba(255,255,255,0.6)" }}>
          CV-Scanner-IA · Matching sémantique
        </p>
      </div>

      {/* Navigation */}
      <div className="flex-1 px-3 py-4 overflow-y-auto">
        {nav.map(group => (
          <div key={group.section} className="mb-5">
            <p className="uppercase tracking-widest px-2 mb-2 font-semibold"
               style={{ color: "rgba(255,255,255,0.5)", fontSize: "9px", letterSpacing: "0.1em" }}>
              {group.section}
            </p>
            {group.items.filter(item => can(item.permission)).map(item => {
              const active = path.startsWith(item.href)
              return (
                <Link key={item.href} href={item.href}>
                  <div
                    className="flex items-center gap-2.5 px-2.5 py-2 rounded-lg text-xs mb-0.5 cursor-pointer transition-all duration-150"
                    style={{
                      background: active ? "rgba(255,255,255,0.15)" : "transparent",
                      color: active ? "white" : "rgba(255,255,255,0.75)",
                      fontWeight: active ? 600 : 400,
                    }}
                    onMouseEnter={e => {
                      if (!active) {
                        (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.08)"
                        ;(e.currentTarget as HTMLElement).style.color = "white"
                      }
                    }}
                    onMouseLeave={e => {
                      if (!active) {
                        (e.currentTarget as HTMLElement).style.background = "transparent"
                        ;(e.currentTarget as HTMLElement).style.color = "rgba(255,255,255,0.75)"
                      }
                    }}
                  >
                    <item.icon className="w-3.5 h-3.5 flex-shrink-0" />
                    <span className="flex-1">{item.label}</span>
                  </div>
                </Link>
              )
            })}
          </div>
        ))}
      </div>

      {/* User */}
      <div className="p-3" style={{ borderTop: "1px solid rgba(255,255,255,0.1)" }}>
        <div className="rounded-lg px-2 py-2"
             style={{ background: "rgba(255,255,255,0.08)" }}>
          <div className="flex items-center gap-2.5">
          <div className="w-7 h-7 rounded-full flex items-center justify-center text-xs font-semibold text-white flex-shrink-0"
               style={{ background: "linear-gradient(135deg,#0066CC,#0099FF)" }}>
            {roleLabel.slice(0, 2).toUpperCase()}
          </div>
          <div className="min-w-0">
            <p className="text-xs font-semibold" style={{ color: "white" }}>
              {user?.name || "Utilisateur"}
            </p>
            <p className="truncate" style={{ fontSize: "10px", color: "rgba(255,255,255,0.65)" }}>
              {user?.email || roleLabel}
            </p>
          </div>
          </div>
          <div className="mt-2 flex items-center gap-2">
            <span className="min-w-0 flex-1 truncate rounded-md px-2 py-1 text-xs"
                  style={{ background: "rgba(255,255,255,0.12)", border: "1px solid rgba(255,255,255,0.16)", color: "white" }}>
              {roleLabel}
            </span>
            <button
              onClick={logout}
              className="rounded-md p-1.5 transition-all"
              style={{ background: "rgba(255,255,255,0.12)", border: "1px solid rgba(255,255,255,0.16)", color: "white" }}
              title="Se deconnecter"
              aria-label="Se deconnecter"
            >
              <LogOut className="h-3.5 w-3.5" />
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
