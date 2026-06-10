"use client"
import { useQuery } from "@tanstack/react-query"
import { api } from "@/lib/api"
import { Topbar } from "@/components/layout/Topbar"
import {
  BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer,
  PieChart, Pie, Cell
} from "recharts"
import { RoleGuard } from "@/lib/rbac"

type CompanyStat = {
  name: string
  shortName: string
  count: number
  color: string
}

type TooltipPayload = {
  value?: number
  payload?: Partial<CompanyStat>
}

type CustomTooltipProps = {
  active?: boolean
  payload?: TooltipPayload[]
  label?: string
}

const COMPANY_COLORS = [
  "#003B8E", "#0066CC", "#0099FF", "#00B4D8",
  "#48CAE4", "#0096C7", "#023E8A", "#0077B6",
  "#00B4D8", "#90E0EF", "#ADE8F4"
]

const SENIORITY_COLORS: Record<string, string> = {
  Junior:    "#94A3B8",
  Medior:    "#F59E0B",
  Confirmé:  "#10B981",
  Senior:    "#0066CC",
  Expert:    "#7C3AED",
}

const CustomTooltip = ({ active, payload, label }: CustomTooltipProps) => {
  if (active && payload && payload.length) {
    const item = payload[0]
    const name = item.payload?.name || label
    return (
      <div style={{ background: "white", border: "1px solid #E2E8F0", borderRadius: "8px", padding: "8px 12px", fontSize: "12px", color: "#1A2B4B", boxShadow: "0 4px 6px rgba(0,0,0,0.07)" }}>
        <p style={{ fontWeight: 600, marginBottom: "2px" }}>{name}</p>
        <p style={{ color: "#0066CC" }}>{item.value ?? 0} candidat(s)</p>
      </div>
    )
  }
  return null
}

export default function AnalyticsPage() {
  const { data: stats } = useQuery({ queryKey: ["stats"], queryFn: api.stats })

  const companyData: CompanyStat[] = Object.entries(stats?.by_company || {})
    .map(([name, count], i) => ({
      name,
      shortName: name.length > 14 ? `${name.slice(0, 13)}...` : name,
      count,
      color: COMPANY_COLORS[i % COMPANY_COLORS.length],
    }))
    .sort((a, b) => b.count - a.count)

  const seniorityOrder = ["Junior", "Medior", "Confirmé", "Senior", "Expert"]
  const seniorityData = seniorityOrder.map(level => ({
    name: level,
    count: stats?.by_seniority?.[level] || 0,
    color: SENIORITY_COLORS[level]
  }))

  const total = stats?.total_candidates || 1

  return (
    <RoleGuard permission="analytics">
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#F0F4F8" }}>
      <Topbar title="Dashboard Analytics" />
      <div className="flex-1 overflow-y-auto p-6">
        <div className="max-w-5xl mx-auto space-y-5">

          {/* Bar chart entreprises */}
          <div className="rounded-2xl p-6" style={{ background: "white", border: "1px solid #E2E8F0" }}>
            <div className="flex items-center justify-between mb-5">
              <div>
                <p className="font-semibold text-sm" style={{ color: "#1A2B4B" }}>Candidats par entreprise</p>
                <p className="text-xs mt-0.5" style={{ color: "#94A3B8" }}>Distribution des {stats?.total_candidates ?? 0} consultants</p>
              </div>
              <span className="text-xs px-3 py-1 rounded-full font-medium"
                style={{ background: "#EEF4FF", color: "#003B8E", border: "1px solid #C7D9F5" }}>
                {companyData.length} entreprises
              </span>
            </div>
            <ResponsiveContainer width="100%" height={240}>
              <BarChart data={companyData} barSize={32} barCategoryGap="30%">
                <XAxis dataKey="shortName" tick={{ fill: "#64748B", fontSize: 11 }} axisLine={false} tickLine={false} />
                <YAxis tick={{ fill: "#94A3B8", fontSize: 10 }} axisLine={false} tickLine={false} width={30} />
                <Tooltip content={<CustomTooltip />} cursor={{ fill: "#F1F5F9", radius: 4 }} />
                <Bar dataKey="count" radius={[6, 6, 0, 0]}>
                  {companyData.map(item => (
                    <Cell key={item.name} fill={item.color} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>

            <div className="mt-5 pt-4" style={{ borderTop: "1px solid #F1F5F9" }}>
              <div className="flex items-center justify-between mb-3">
                <p className="font-semibold text-sm" style={{ color: "#1A2B4B" }}>
                  Classement des entreprises
                </p>
                <p className="text-xs" style={{ color: "#94A3B8" }}>
                  Noms complets et volumes
                </p>
              </div>
              <div className="grid grid-cols-2 gap-x-6 gap-y-2">
                {companyData.map((item, i) => {
                  const pct = Math.round(item.count / total * 100)
                  return (
                    <div key={item.name} className="flex items-center gap-3 py-1.5">
                      <span className="text-xs font-mono w-5 text-right" style={{ color: "#94A3B8" }}>
                        #{i + 1}
                      </span>
                      <div className="w-2.5 h-2.5 rounded-full flex-shrink-0" style={{ background: item.color }} />
                      <span className="text-xs flex-1 truncate" title={item.name} style={{ color: "#475569" }}>
                        {item.name || "Entreprise non renseignée"}
                      </span>
                      <span className="text-xs font-bold w-8 text-right" style={{ color: "#003B8E" }}>
                        {item.count}
                      </span>
                      <span className="text-xs w-10 text-right" style={{ color: "#94A3B8" }}>
                        {pct}%
                      </span>
                    </div>
                  )
                })}
              </div>
            </div>
          </div>

          {/* Ligne 2 — Pie + KPIs */}
          <div className="grid grid-cols-2 gap-5">

            {/* Pie séniorité */}
            <div className="rounded-2xl p-6" style={{ background: "white", border: "1px solid #E2E8F0" }}>
              <p className="font-semibold text-sm mb-1" style={{ color: "#1A2B4B" }}>Répartition par séniorité</p>
              <p className="text-xs mb-5" style={{ color: "#94A3B8" }}>5 niveaux · {total} candidats</p>

              <div className="flex items-center gap-6">
                <div style={{ width: 160, height: 160 }}>
                  <ResponsiveContainer width="100%" height="100%">
                    <PieChart>
                      <Pie data={seniorityData} cx="50%" cy="50%"
                        innerRadius={45} outerRadius={72}
                        dataKey="count" paddingAngle={3} startAngle={90} endAngle={-270}>
                        {seniorityData.map((entry, i) => (
                          <Cell key={i} fill={entry.color} stroke="white" strokeWidth={2} />
                        ))}
                      </Pie>
                      <Tooltip
                        formatter={(val: number | string, name: string) => [`${val} candidats`, name]}
                        contentStyle={{ background: "white", border: "1px solid #E2E8F0", borderRadius: "8px", fontSize: "12px" }}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                </div>

                <div className="flex-1 space-y-2.5">
                  {seniorityData.map(item => {
                    const pct = Math.round(item.count / total * 100)
                    return (
                      <div key={item.name}>
                        <div className="flex items-center justify-between mb-1">
                          <div className="flex items-center gap-2">
                            <div className="w-2.5 h-2.5 rounded-full" style={{ background: item.color }} />
                            <span className="text-xs font-medium" style={{ color: "#475569" }}>{item.name}</span>
                          </div>
                          <div className="flex items-center gap-2">
                            <span className="text-xs font-bold" style={{ color: item.color }}>{item.count}</span>
                            <span className="text-xs" style={{ color: "#94A3B8" }}>{pct}%</span>
                          </div>
                        </div>
                        <div className="rounded-full" style={{ height: "4px", background: "#F1F5F9" }}>
                          <div className="rounded-full" style={{ height: "4px", width: `${pct}%`, background: item.color }} />
                        </div>
                      </div>
                    )
                  })}
                </div>
              </div>
            </div>

            {/* KPIs */}
            <div className="rounded-2xl p-6" style={{ background: "white", border: "1px solid #E2E8F0" }}>
              <p className="font-semibold text-sm mb-1" style={{ color: "#1A2B4B" }}>Résumé de la base</p>
              <p className="text-xs mb-5" style={{ color: "#94A3B8" }}>Statistiques globales</p>

              <div className="grid grid-cols-2 gap-3 mb-4">
                {[
                  { label: "Candidats",   value: stats?.total_candidates ?? 0,                        bg: "#EEF4FF", color: "#003B8E"  },
                  { label: "Offres",      value: stats?.total_jobs ?? 0,                              bg: "#F0FDF4", color: "#065F46"  },
                  { label: "Entreprises", value: Object.keys(stats?.by_company ?? {}).length,          bg: "#FFFBEB", color: "#92400E"  },
                  { label: "Niveaux",     value: Object.keys(stats?.by_seniority ?? {}).length,        bg: "#F5F3FF", color: "#5B21B6"  },
                ].map(item => (
                  <div key={item.label} className="rounded-xl p-3 text-center"
                    style={{ background: item.bg }}>
                    <p className="text-2xl font-bold" style={{ color: item.color }}>{item.value}</p>
                    <p className="text-xs mt-0.5" style={{ color: item.color, opacity: 0.7 }}>{item.label}</p>
                  </div>
                ))}
              </div>

              <div className="space-y-2 pt-3" style={{ borderTop: "1px solid #F1F5F9" }}>
                {seniorityData.filter(s => s.count > 0).map(item => (
                  <div key={item.name} className="flex items-center justify-between py-1">
                    <div className="flex items-center gap-2">
                      <div className="w-2 h-2 rounded-full" style={{ background: item.color }} />
                      <span className="text-xs" style={{ color: "#64748B" }}>{item.name}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-bold" style={{ color: item.color }}>{item.count}</span>
                      <span className="text-xs px-1.5 py-0.5 rounded"
                        style={{ background: "#F8FAFC", color: "#94A3B8", fontSize: "10px" }}>
                        {Math.round(item.count / total * 100)}%
                      </span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>

        </div>
      </div>
    </div>
    </RoleGuard>
  )
}
