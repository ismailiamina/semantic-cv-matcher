"use client"
import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { api, type MatchResult, http } from "@/lib/api"
import { Topbar } from "@/components/layout/Topbar"
import { ScoreRing } from "@/components/match/ScoreRing"
import { yearsToLevel, scoreColor, initials, renderLLMText } from "@/lib/utils"
import { FilterPanel } from "@/components/common/FilterPanel"
import { SkeletonCard } from "@/components/common/SkeletonCard"
import { Toast } from "@/components/common/Toast"
import { SearchableSelect } from "@/components/common/SearchableSelect"
import { Brain } from "lucide-react"
import { RoleGuard, useAuth } from "@/lib/rbac"
import { recordAuditEvent } from "@/lib/audit"

type JobProperties = {
  title?: string
  company?: string
  location?: string
  experience_level?: string
  years_of_experience_required?: number | string
  technical_skills?: string[]
  programming_languages?: string[]
  certifications?: string[]
  summary?: string
}

export default function JobsPage() {
  const { can, user } = useAuth()
  const [selectedCandUuid, setSelectedCandUuid] = useState("")
  const [mode, setMode] = useState("hybride")
  const [limit, setLimit] = useState(10)
  const [expMin, setExpMin] = useState(0)
  const [expMax, setExpMax] = useState(20)
  const [results, setResults] = useState<MatchResult[]>([])
  const [loading, setLoading] = useState(false)
  const [reranking, setReranking] = useState(false)
  const [selected, setSelected] = useState<MatchResult | null>(null)
  const [gap, setGap] = useState<Record<string, string>>({})
  const [gapLoading, setGapLoading] = useState<string | null>(null)
  const [toast, setToast] = useState<{ msg: string; type: "success" | "error" } | null>(null)

  const { data: candsData } = useQuery({ queryKey: ["cand-names"], queryFn: api.candidateNames })
  const { data: candDetail } = useQuery({
    queryKey: ["cand", selectedCandUuid],
    queryFn: () => api.candidate(selectedCandUuid),
    enabled: !!selectedCandUuid,
  })

  const handleSearch = async () => {
    if (!selectedCandUuid) return
    setLoading(true); setResults([]); setSelected(null)
    try {
      const res = await api.jobsForCandidate(selectedCandUuid, mode, limit * 3)
      let filtered = res.results
      if (expMin > 0 || expMax < 20) {
        filtered = filtered.filter((r: MatchResult) => {
          const p = r.properties as JobProperties
          const y = Number(p.years_of_experience_required || 0)
          return y >= expMin && y <= expMax
        })
        if (filtered.length < res.results.length) {
          setToast({ msg: `${res.results.length - filtered.length} offre(s) exclue(s) par le filtre expérience`, type: "success" })
        }
      }
      const visibleResults = filtered.slice(0, limit)
      setResults(visibleResults)
      recordAuditEvent({
        action: "matching_launched",
        targetLabel: candDetail?.full_name || selectedCandUuid,
        targetUuid: selectedCandUuid,
        context: `Candidat vers offres - ${mode} - ${visibleResults.length} resultat(s)`,
        actor: user,
      })
    } catch {
      setToast({ msg: "Erreur lors de la recherche", type: "error" })
    } finally { setLoading(false) }
  }

  const handleRerank = async () => {
    if (!results.length || !candDetail) return
    setReranking(true)
    try {
      const years = candDetail.years_of_experience || 0
      const level = years >= 10 ? "Expert" : years >= 6 ? "Senior" : years >= 4 ? "Confirmé" : years >= 2 ? "Medior" : "Junior"
      const parts = [
        `Candidat : ${candDetail.full_name}`,
        `Experience : ${years} ans — niveau ${level}`,
        `Roles occupes : ${(candDetail.roles_held || []).slice(0, 4).join(", ") || "Non precise"}`,
        `Competences techniques : ${(candDetail.technical_skills || []).slice(0, 15).join(", ") || "Non precise"}`,
        `Langages maitrises : ${(candDetail.programming_languages || []).slice(0, 6).join(", ") || "Non precise"}`,
      ]
      if ((candDetail.certifications || []).length > 0)
        parts.push(`Certifications : ${(candDetail.certifications || []).slice(0, 4).join(", ")}`)
      const res = await api.rerank(parts.join(". "), results, limit)
      setResults(res.results)
      recordAuditEvent({
        action: "reranking_launched",
        targetLabel: candDetail.full_name,
        targetUuid: selectedCandUuid,
        context: `${res.results.length} offre(s) reordonnees`,
        actor: user,
      })
      setToast({ msg: `Reranking appliqué — ${res.results.length} offres reordonnées`, type: "success" })
    } catch {
      setToast({ msg: "Erreur lors du reranking", type: "error" })
    } finally { setReranking(false) }
  }

  const handleGap = async (r: MatchResult) => {
    if (!candDetail || gap[r.uuid]) return
    setGapLoading(r.uuid)
    try {
      const res = await http.post("/api/llm/gap/", { job_props: r.properties, cand_props: candDetail })
      setGap(prev => ({ ...prev, [r.uuid]: res.data.result }))
    } catch {
      setGap(prev => ({ ...prev, [r.uuid]: "Erreur lors de l'analyse." }))
    } finally { setGapLoading(null) }
  }

  return (
    <RoleGuard permission="matchJobs">
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#F0F4F8" }}>
      <Topbar title="Offres pour un candidat" />

      {toast && <Toast message={toast.msg} type={toast.type} onClose={() => setToast(null)} />}

      <div className="flex flex-1 overflow-hidden">

        {/* Panel gauche */}
        <div className="flex flex-col overflow-hidden"
             style={{ width: "52%", borderRight: "1px solid #E2E8F0", background: "#F8FAFC" }}>

          <div className="p-4 space-y-3 flex-shrink-0"
               style={{ borderBottom: "1px solid #E2E8F0", background: "white" }}>

            {/* Recherche candidat */}
            <SearchableSelect
              items={candsData?.candidates ?? []}
              value={selectedCandUuid}
              onChange={uuid => { setSelectedCandUuid(uuid); setResults([]); setSelected(null) }}
              placeholder="Rechercher un candidat..."
              emptyText="Aucun candidat trouve"
              getValue={c => c.uuid}
              getLabel={c => c.full_name}
              getMeta={c => `${c.company_source || "Source non precisee"} - ${c.years_of_experience} ans`}
              getSearchText={c => `${c.full_name} ${c.company_source} ${c.years_of_experience} ans`}
            />

            {/* Carte candidat */}
            {candDetail && (
              <div className="rounded-xl p-3 animate-fade-in"
                   style={{ background: "#F0FDF4", border: "1px solid #BBF7D0" }}>
                <div className="flex items-center gap-3">
                  <div className="w-10 h-10 rounded-full flex items-center justify-center text-sm font-semibold flex-shrink-0"
                       style={{ background: "#D1FAE5", color: "#065F46" }}>
                    {initials(candDetail.full_name || "")}
                  </div>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>
                      {candDetail.full_name}
                    </p>
                    <p className="text-xs mt-0.5" style={{ color: "#64748B" }}>
                      {candDetail.years_of_experience} ans · {yearsToLevel(candDetail.years_of_experience)} · {candDetail.company_source}
                    </p>
                  </div>
                </div>
                <div className="flex flex-wrap gap-1 mt-2">
                  {(candDetail.technical_skills || []).slice(0, 6).map((s: string) => (
                    <span key={s} className="text-xs px-1.5 py-0.5 rounded"
                          style={{ fontSize: "10px", background: "white", border: "1px solid #CBD5E1", color: "#475569" }}>
                      {s}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* Filtres */}
            <FilterPanel
              mode={mode} onModeChange={setMode}
              limit={limit} onLimitChange={setLimit}
              expMin={expMin} expMax={expMax}
              onExpMinChange={setExpMin} onExpMaxChange={setExpMax}
              onSearch={handleSearch}
              onRerank={handleRerank}
              loading={loading} reranking={reranking}
              hasResults={results.length > 0}
              disabled={!selectedCandUuid}
              canRerank={can("rerank")}
            />
          </div>

          {/* Compteur */}
          {results.length > 0 && (
            <div className="flex items-center justify-between px-4 py-2 flex-shrink-0"
                 style={{ background: "white", borderBottom: "1px solid #F1F5F9" }}>
              <span className="text-xs" style={{ color: "#64748B" }}>
                <span style={{ color: "#0066CC", fontWeight: 700 }}>{results.length}</span> offre(s)
              </span>
              <div className="flex items-center gap-2">
                <span className="text-xs px-2 py-0.5 rounded-full"
                      style={{ background: "#EEF4FF", color: "#003B8E", border: "1px solid #C7D9F5", fontSize: "10px" }}>
                  {mode}
                </span>
                {results[0]?.rerank_score !== undefined && (
                  <span className="text-xs px-2 py-0.5 rounded-full"
                        style={{ background: "#F0FDF4", color: "#065F46", border: "1px solid #BBF7D0", fontSize: "10px" }}>
                    ✓ Reranké
                  </span>
                )}
                {(expMin > 0 || expMax < 20) && (
                  <span className="text-xs px-2 py-0.5 rounded-full"
                        style={{ background: "#FFFBEB", color: "#92400E", border: "1px solid #FCD34D", fontSize: "10px" }}>
                    {expMin}–{expMax} ans
                  </span>
                )}
              </div>
            </div>
          )}

          {/* Liste offres */}
          <div className="flex-1 overflow-y-auto p-3 space-y-2">
            {loading && (
              <div className="space-y-2">
                {[1, 2, 3, 4, 5].map(i => <SkeletonCard key={i} />)}
              </div>
            )}

            {!loading && results.length === 0 && selectedCandUuid && (
              <div className="flex flex-col items-center justify-center py-16 gap-2"
                   style={{ color: "#CBD5E1" }}>
                <p className="text-sm">Aucun résultat</p>
                <p style={{ fontSize: "11px" }}>Lance une recherche pour voir les offres</p>
              </div>
            )}

            {results.map((r, i) => {
              const p = r.properties as JobProperties
              const score = r.rerank_score ?? r.score
              const isActive = selected?.uuid === r.uuid
              return (
                <div key={r.uuid} onClick={() => setSelected(r)}
                     className="rounded-xl p-3 cursor-pointer transition-all animate-fade-in"
                     style={{
                       background: isActive ? "#EEF4FF" : "white",
                       border: `1px solid ${isActive ? "#0066CC" : "#E2E8F0"}`
                     }}>
                  <div className="flex items-center gap-2.5 mb-2.5">
                    <span className="text-xs font-mono w-5 flex-shrink-0" style={{ color: "#94A3B8" }}>#{i + 1}</span>
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-semibold truncate" style={{ color: "#1A2B4B" }}>{p.title}</p>
                      <p className="text-xs truncate" style={{ color: "#64748B", fontSize: "10px" }}>
                        {p.company} · {p.location}
                      </p>
                    </div>
                    <span className="text-xs px-2 py-0.5 rounded flex-shrink-0"
                          style={{ fontSize: "10px", background: "#FFFBEB", border: "1px solid #FCD34D", color: "#92400E" }}>
                      {p.experience_level} · {p.years_of_experience_required} ans
                    </span>
                    <ScoreRing score={score} />
                  </div>

                  <div className="grid grid-cols-2 gap-x-4 gap-y-1.5 mb-2.5">
                    {Object.entries(r.individual_scores || {}).slice(0, 4).map(([dim, val]) => (
                      <div key={dim} className="flex items-center gap-1.5">
                        <span style={{ fontSize: "9px", color: "#94A3B8", width: "64px", flexShrink: 0 }}>{dim}</span>
                        <div className="flex-1 rounded-full" style={{ height: "3px", background: "#E2E8F0" }}>
                          <div className="rounded-full"
                               style={{ height: "3px", width: `${Math.round(val * 100)}%`, background: scoreColor(val) }} />
                        </div>
                        <span style={{ fontSize: "9px", color: scoreColor(val), width: "24px", textAlign: "right" }}>
                          {Math.round(val * 100)}%
                        </span>
                      </div>
                    ))}
                  </div>

                  <div className="flex flex-wrap gap-1 mb-2.5">
                    {(p.technical_skills || []).slice(0, 5).map((s: string) => (
                      <span key={s} className="rounded px-1.5 py-0.5"
                            style={{ fontSize: "9px", background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>
                        {s}
                      </span>
                    ))}
                  </div>

                  {can("llm") && (
                    <button onClick={e => { e.stopPropagation(); handleGap(r) }}
                            disabled={gapLoading === r.uuid}
                            className="w-full text-xs rounded-lg py-1.5 transition-all flex items-center justify-center gap-1.5"
                            style={{ background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>
                      <Brain className="w-3 h-3" />
                      {gapLoading === r.uuid ? "Analyse en cours..." : gap[r.uuid] ? "Plan de formation ✓" : "Analyser le gap"}
                    </button>
                  )}

                  {gap[r.uuid] && (
                    <div className="mt-2 rounded-lg p-2.5 text-xs leading-relaxed animate-fade-in"
                         style={{ background: "#FFFBEB", border: "1px solid #FCD34D", color: "#78350F" }}
                         dangerouslySetInnerHTML={{ __html: renderLLMText(gap[r.uuid]) }}
                    />
                  )}
                </div>
              )
            })}
          </div>
        </div>

        {/* Panel droit */}
        <div className="flex-1 overflow-y-auto p-5" style={{ background: "#F8FAFC" }}>
          {selected ? <JobDetail result={selected} /> : (
            <div className="h-full flex flex-col items-center justify-center gap-3" style={{ color: "#CBD5E1" }}>
              <div className="w-16 h-16 rounded-2xl flex items-center justify-center"
                   style={{ background: "white", border: "1px solid #E2E8F0" }}>
                <Brain className="w-7 h-7" style={{ color: "#CBD5E1" }} />
              </div>
              <p className="text-sm">Sélectionne une offre</p>
              <p style={{ fontSize: "11px" }}>pour voir les détails</p>
            </div>
          )}
        </div>
      </div>
    </div>
    </RoleGuard>
  )
}

function JobDetail({ result }: { result: MatchResult }) {
  const p = result.properties as JobProperties
  const score = result.rerank_score ?? result.score
  return (
    <div className="space-y-4 animate-fade-in">
      <div className="rounded-2xl p-4" style={{ background: "#FFFBEB", border: "1px solid #FCD34D" }}>
        <div className="flex items-start justify-between gap-3">
          <div>
            <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>{p.title}</p>
            <p className="text-xs mt-0.5" style={{ color: "#64748B" }}>{p.company} · {p.location}</p>
          </div>
          <div className="text-center flex-shrink-0">
            <ScoreRing score={score} size={60} stroke={5} />
            <p className="text-xs mt-1" style={{ color: "#94A3B8", fontSize: "9px" }}>score global</p>
          </div>
        </div>
        <div className="flex gap-2 mt-3">
          <span className="text-xs px-2 py-1 rounded-md"
                style={{ background: "#FFFBEB", border: "1px solid #FCD34D", color: "#92400E" }}>
            {p.experience_level}
          </span>
          <span className="text-xs px-2 py-1 rounded-md"
                style={{ background: "white", border: "1px solid #E2E8F0", color: "#475569" }}>
            {p.years_of_experience_required} ans requis
          </span>
        </div>
      </div>

      <Section title="Scores par dimension">
        <div className="space-y-2.5">
          {Object.entries(result.individual_scores || {}).map(([dim, val]) => (
            <div key={dim} className="flex items-center gap-3">
              <span className="text-xs flex-shrink-0" style={{ color: "#64748B", width: "90px" }}>{dim}</span>
              <div className="flex-1 rounded-full" style={{ height: "5px", background: "#E2E8F0" }}>
                <div className="rounded-full"
                     style={{ height: "5px", width: `${Math.round(val * 100)}%`, background: scoreColor(val) }} />
              </div>
              <span className="text-xs font-semibold text-right flex-shrink-0"
                    style={{ color: scoreColor(val), width: "32px" }}>
                {Math.round(val * 100)}%
              </span>
            </div>
          ))}
        </div>
      </Section>

      <Section title="Compétences requises">
        <div className="flex flex-wrap gap-1.5">
          {(p.technical_skills || []).map((s: string) => <Tag key={s} color="blue">{s}</Tag>)}
        </div>
      </Section>

      {(p.programming_languages || []).length > 0 && (
        <Section title="Langages requis">
          <div className="flex flex-wrap gap-1.5">
            {(p.programming_languages || []).map((l: string) => <Tag key={l} color="purple">{l}</Tag>)}
          </div>
        </Section>
      )}

      {(p.certifications || []).length > 0 && (
        <Section title="Certifications souhaitées">
          <div className="flex flex-wrap gap-1.5">
            {(p.certifications || []).map((c: string) => <Tag key={c} color="green">{c}</Tag>)}
          </div>
        </Section>
      )}

      {p.summary && (
        <Section title="Description du poste">
          <p className="text-xs leading-relaxed" style={{ color: "#475569" }}>
            {(p.summary || "").slice(0, 500)}{(p.summary || "").length > 500 ? "..." : ""}
          </p>
        </Section>
      )}
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl p-4" style={{ background: "white", border: "1px solid #E2E8F0" }}>
      <p className="uppercase tracking-widest mb-3"
         style={{ fontSize: "9px", color: "#94A3B8", fontWeight: 600 }}>
        {title}
      </p>
      {children}
    </div>
  )
}

function Tag({ children, color }: { children: React.ReactNode; color: "blue" | "purple" | "green" | "amber" }) {
  const styles = {
    blue:   { background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" },
    purple: { background: "#F5F3FF", border: "1px solid #DDD6FE", color: "#5B21B6" },
    green:  { background: "#F0FDF4", border: "1px solid #BBF7D0", color: "#166534" },
    amber:  { background: "#FFFBEB", border: "1px solid #FCD34D", color: "#92400E" },
  }
  return (
    <span className="rounded-md px-2 py-0.5" style={{ ...styles[color], fontSize: "10px" }}>
      {children}
    </span>
  )
}
