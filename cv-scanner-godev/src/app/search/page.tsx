"use client"
import { useState } from "react"
import { api, type MatchResult, http } from "@/lib/api"
import { Topbar } from "@/components/layout/Topbar"
import { ScoreRing } from "@/components/match/ScoreRing"
import { yearsToLevel, levelColors, initials } from "@/lib/utils"
import { Search, Zap, X } from "lucide-react"
import { RoleGuard } from "@/lib/rbac"

type SearchCriteria = {
  loc?: string[]
  exclude?: string
  skills?: string[]
  programming_languages?: string[]
  role?: string
  experience?: {
    min?: number | null
    max?: number | null
  }
  industry?: string[]
  limit?: number
}

type ApiError = {
  response?: {
    data?: {
      detail?: string
    }
  }
}

export default function SearchPage() {
  const [query, setQuery] = useState("")
  const [results, setResults] = useState<MatchResult[]>([])
  const [loading, setLoading] = useState(false)
  const [criteria, setCriteria] = useState<object | null>(null)
  const [error, setError] = useState("")

const handleSearch = async () => {
  if (!query.trim()) return
  setLoading(true); setError(""); setResults([])
  try {
    const parseRes = await http.post("/api/llm/parse-query/", { query })
    const parsed = parseRes.data.criteria as SearchCriteria
    setCriteria(parsed)

    // Nettoyer les valeurs null avant d'envoyer
    const cleanCriteria: SearchCriteria = { ...parsed, limit: 10 }

    // Nettoyer experience.max si null
    if (cleanCriteria.experience) {
      if (cleanCriteria.experience.max === null) {
        delete cleanCriteria.experience.max
      }
      if (cleanCriteria.experience.min === null) {
        delete cleanCriteria.experience.min
      }
    }

    // Séparer skills et role car advanced_search les traite différemment
    if (cleanCriteria.skills && Array.isArray(cleanCriteria.skills)) {
      // Filtrer les rôles qui se sont glissés dans skills
      const roles = ["Tech Lead", "Lead", "Manager", "Architect", "Director", "CTO", "CEO"]
      const roleSkills = cleanCriteria.skills.filter((s: string) =>
        roles.some(r => s.toLowerCase().includes(r.toLowerCase()))
      )
      const realSkills = cleanCriteria.skills.filter((s: string) =>
        !roles.some(r => s.toLowerCase().includes(r.toLowerCase()))
      )
      cleanCriteria.skills = realSkills
      if (roleSkills.length > 0 && !cleanCriteria.role) {
        cleanCriteria.role = roleSkills[0]
      }
    }

    const res = await api.advancedSearch(cleanCriteria)
    setResults(res.results)
  } catch (e: unknown) {
    const detail = (e as ApiError)?.response?.data?.detail || "Erreur lors de la recherche."
    setError(detail)
  } finally { setLoading(false) }
}
  return (
    <RoleGuard permission="freeSearch">
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#F0F4F8" }}>
      <Topbar title="Recherche libre" />
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-6 py-8">

          <p className="text-sm mb-3" style={{ color: "#64748B" }}>Décris le profil que tu cherches en langage naturel</p>

          <div className="flex gap-2 mb-6">
            <div className="flex-1 relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: "#94A3B8" }} />
              <input value={query} onChange={e => setQuery(e.target.value)}
                onKeyDown={e => e.key === "Enter" && handleSearch()}
                placeholder="Ex: Développeur Angular 5 ans Rabat..."
                className="w-full text-sm rounded-xl pl-10 pr-4 py-3 focus:outline-none focus:ring-2"
                style={{ background: "white", border: "1px solid #CBD5E1", color: "#1A2B4B" }} />
            </div>
            {query && (
              <button onClick={() => { setQuery(""); setResults([]); setCriteria(null); setError("") }}
                className="px-3 rounded-xl transition-all"
                style={{ background: "white", border: "1px solid #CBD5E1", color: "#94A3B8" }}>
                <X className="w-4 h-4" />
              </button>
            )}
            <button onClick={handleSearch} disabled={!query.trim() || loading}
              className="px-5 text-sm font-semibold rounded-xl transition-all flex items-center gap-2 disabled:opacity-40"
              style={{ background: "#0066CC", color: "white" }}>
              <Zap className="w-4 h-4" />
              {loading ? "Analyse..." : "Rechercher"}
            </button>
          </div>

          {/* Exemples */}
          {!results.length && !loading && !error && (
            <div className="mb-8">
              <p className="text-xs mb-3 uppercase tracking-widest" style={{ color: "#94A3B8", fontSize: "9px" }}>Exemples</p>
              <div className="flex flex-wrap gap-2">
                {[
                  "Angular 5 ans Rabat",
                  "DevOps Kubernetes Terraform 4 ans",
                  "Java Spring Boot Casablanca 3-7 ans",
                  "Tech Lead Angular React 6 ans",
                  "Full Stack React Node.js 3 ans",
                ].map(ex => (
                  <button key={ex} onClick={() => setQuery(ex)}
                    className="text-xs px-3 py-1.5 rounded-lg transition-all"
                    style={{ background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>
                    {ex}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Critères */}
          {criteria && (
            <div className="mb-4 rounded-lg px-4 py-3 text-xs animate-fade-in"
              style={{ background: "white", border: "1px solid #E2E8F0", color: "#64748B", fontFamily: "monospace" }}>
              Critères : {JSON.stringify(criteria)}
            </div>
          )}

          {error && (
            <div className="mb-4 rounded-lg px-4 py-3 text-sm"
              style={{ background: "#FEF2F2", border: "1px solid #FECACA", color: "#991B1B" }}>{error}</div>
          )}

          {loading && (
            <div className="flex items-center justify-center py-16">
              <p className="text-sm" style={{ color: "#94A3B8" }}>Analyse de la requête...</p>
            </div>
          )}

          {results.length > 0 && (
            <>
              <p className="text-xs mb-4" style={{ color: "#64748B" }}>
                <span style={{ color: "#0066CC", fontWeight: 700 }}>{results.length}</span> candidat(s) trouvé(s)
              </p>
              <div className="space-y-3">
                {results.map((r, i) => {
                  const p = r.properties
                  const years = Number(p.years_of_experience || 0)
                  const level = yearsToLevel(years)
                  const color = levelColors[level] || "#94A3B8"
                  return (
                    <div key={r.uuid} className="rounded-xl p-4 animate-fade-in"
                      style={{ background: "white", border: "1px solid #E2E8F0" }}>
                      <div className="flex items-center gap-3 mb-3">
                        <span className="text-xs font-mono w-5" style={{ color: "#94A3B8" }}>#{i + 1}</span>
                        <div className="w-9 h-9 rounded-full flex items-center justify-center text-xs font-semibold flex-shrink-0"
                          style={{ background: color + "22", color }}>
                          {initials(p.full_name || "")}
                        </div>
                        <div className="flex-1 min-w-0">
                          <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>{p.full_name}</p>
                          <p className="text-xs mt-0.5" style={{ color: "#64748B", fontSize: "10px" }}>
                            {years} ans · <span style={{ color }}>{level}</span> · {p.location} · {p.company_source}
                          </p>
                        </div>
                        <ScoreRing score={r.score} size={48} stroke={4} />
                      </div>
                      <div className="flex flex-wrap gap-1.5">
                        {(p.technical_skills || []).slice(0, 8).map((s: string) => (
                          <span key={s} className="rounded px-2 py-0.5"
                            style={{ fontSize: "10px", background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>{s}</span>
                        ))}
                        {(p.programming_languages || []).slice(0, 4).map((l: string) => (
                          <span key={l} className="rounded px-2 py-0.5"
                            style={{ fontSize: "10px", background: "#F5F3FF", border: "1px solid #DDD6FE", color: "#5B21B6" }}>{l}</span>
                        ))}
                      </div>
                    </div>
                  )
                })}
              </div>
            </>
          )}
        </div>
      </div>
    </div>
    </RoleGuard>
  )
}
