"use client"
import { useQuery } from "@tanstack/react-query"
import { api } from "@/lib/api"
import { Users, Briefcase, Building2, Layers } from "lucide-react"
import { useAuth } from "@/lib/rbac"

export function Topbar({ title }: { title: string }) {
  const { roleLabel } = useAuth()
  const { data: stats } = useQuery({
    queryKey: ["stats"],
    queryFn: api.stats,
  })

  const kpis = [
    { label: "Candidats",   value: stats?.total_candidates ?? "—",                        icon: Users      },
    { label: "Offres",      value: stats?.total_jobs ?? "—",                              icon: Briefcase  },
    { label: "Entreprises", value: stats ? Object.keys(stats.by_company).length : "—",   icon: Building2  },
    { label: "Niveaux",     value: stats ? Object.keys(stats.by_seniority).length : "—", icon: Layers     },
  ]

  return (
    <div className="flex-shrink-0"
         style={{ background: "white", borderBottom: "1px solid #E2E8F0" }}>
      <div className="px-6 py-3">
        <div className="flex items-center justify-between gap-3">
          <h1 className="text-sm font-semibold" style={{ color: "#003B8E" }}>{title}</h1>
          <span className="rounded-full px-3 py-1 text-xs font-semibold" style={{ background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>
            Role : {roleLabel}
          </span>
        </div>
      </div>
      <div className="grid grid-cols-4 gap-3 px-6 pb-4">
        {kpis.map(k => (
          <div key={k.label} className="rounded-xl px-4 py-3 flex items-center gap-3"
               style={{ background: "#EEF4FF", border: "1px solid #C7D9F5" }}>
            <k.icon className="w-4 h-4 flex-shrink-0" style={{ color: "#003B8E" }} />
            <div>
              <p style={{ fontSize: "9px", color: "#64748B", textTransform: "uppercase", letterSpacing: "0.08em" }}>
                {k.label}
              </p>
              <p className="text-xl font-bold" style={{ color: "#003B8E" }}>
                {String(k.value)}
              </p>
            </div>
          </div>
        ))}
      </div>
    </div>
  )
}
