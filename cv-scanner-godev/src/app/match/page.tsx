"use client"
import { useState } from "react"
import { useQuery } from "@tanstack/react-query"
import { api, http, type MatchResult } from "@/lib/api"
import { Topbar } from "@/components/layout/Topbar"
import { ScoreRing } from "@/components/match/ScoreRing"
import { yearsToLevel, levelColors, scoreColor, initials, renderLLMText } from "@/lib/utils"
import { FilterPanel } from "@/components/common/FilterPanel"
import { SkeletonCard } from "@/components/common/SkeletonCard"
import { Toast } from "@/components/common/Toast"
import { SearchableSelect } from "@/components/common/SearchableSelect"
import { Brain } from "lucide-react"
import { RoleGuard, useAuth } from "@/lib/rbac"
import { recordAuditEvent } from "@/lib/audit"

type SeniorityLevel = "Junior" | "Medior" | "Confirmé" | "Senior" | "Expert"
type SeniorityTechnology = {
  technology: string
  level: SeniorityLevel
}

type CandidateProperties = {
  full_name?: string
  years_of_experience?: number | string
  company_source?: string
  location?: string
  roles_held?: string[]
  technical_skills?: string[]
  programming_languages?: string[]
  seniority_technologies?: SeniorityTechnology[]
  career_trajectory_direction?: string
  career_trajectory_progression_speed?: string
  career_trajectory_skills_in_progress?: string[]
  certifications?: string[]
  summary?: string
}

export default function MatchPage() {
  const { can, user } = useAuth()
  const [selectedJobUuid, setSelectedJobUuid] = useState("")
  const [mode, setMode] = useState("hybride")
  const [limit, setLimit] = useState(10)
  const [expMin, setExpMin] = useState(0)
  const [expMax, setExpMax] = useState(20)
  const [results, setResults] = useState<MatchResult[]>([])
  const [loading, setLoading] = useState(false)
  const [reranking, setReranking] = useState(false)
  const [selected, setSelected] = useState<MatchResult | null>(null)
  const [llm, setLlm] = useState<Record<string, string>>({})
  const [llmLoading, setLlmLoading] = useState<string | null>(null)
  const [toast, setToast] = useState<{ msg: string; type: "success" | "error" } | null>(null)

  const { data: jobsData } = useQuery({ queryKey: ["job-titles"], queryFn: api.jobTitles })
  const { data: jobDetail } = useQuery({
    queryKey: ["job", selectedJobUuid],
    queryFn: () => api.job(selectedJobUuid),
    enabled: !!selectedJobUuid,
  })

  const handleSearch = async () => {
    if (!selectedJobUuid) return
    setLoading(true); setResults([]); setSelected(null)
    try {
      const res = await api.candidatesForJob(selectedJobUuid, mode, limit * 3)
      let filtered = res.results
      if (expMin > 0 || expMax < 20) {
        filtered = filtered.filter((r: MatchResult) => {
          const p = r.properties as CandidateProperties
          const y = Number(p.years_of_experience || 0)
          return y >= expMin && y <= expMax
        })
        if (filtered.length < res.results.length) {
          setToast({ msg: `${res.results.length - filtered.length} candidat(s) exclus par le filtre expérience`, type: "success" })
        }
      }
      const visibleResults = filtered.slice(0, limit)
      setResults(visibleResults)
      recordAuditEvent({
        action: "matching_launched",
        targetLabel: jobDetail?.title || selectedJobUuid,
        targetUuid: selectedJobUuid,
        context: `Offre vers candidats - ${mode} - ${visibleResults.length} resultat(s)`,
        actor: user,
      })
    } catch {
      setToast({ msg: "Erreur lors de la recherche", type: "error" })
    } finally { setLoading(false) }
  }

  const handleRerank = async () => {
    if (!results.length || !jobDetail) return
    setReranking(true)
    try {
      const parts = [
        `Poste recherche : ${jobDetail.title}`,
        `Entreprise : ${jobDetail.company}`,
        `Niveau requis : ${jobDetail.experience_level} avec minimum ${jobDetail.years_of_experience_required} ans`,
      ]
      if ((jobDetail.technical_skills || []).length > 0)
        parts.push(`Competences requises : ${(jobDetail.technical_skills || []).slice(0, 12).join(", ")}`)
      if ((jobDetail.programming_languages || []).length > 0)
        parts.push(`Langages requis : ${(jobDetail.programming_languages || []).join(", ")}`)
      if (jobDetail.summary)
        parts.push(`Description : ${(jobDetail.summary || "").slice(0, 400)}`)
      const res = await api.rerank(parts.join(". "), results, limit)
      setResults(res.results)
      recordAuditEvent({
        action: "reranking_launched",
        targetLabel: jobDetail.title,
        targetUuid: selectedJobUuid,
        context: `${res.results.length} candidat(s) reordonnes`,
        actor: user,
      })
      setToast({ msg: `Reranking appliqué — ${res.results.length} candidats reordonnés`, type: "success" })
    } catch {
      setToast({ msg: "Erreur lors du reranking", type: "error" })
    } finally { setReranking(false) }
  }

  const handleLLM = async (r: MatchResult) => {
    if (!jobDetail || llm[r.uuid]) return
    setLlmLoading(r.uuid)
    try {
      const res = await http.post("/api/llm/explain/", { job_props: jobDetail, cand_props: r.properties })
      setLlm(prev => ({ ...prev, [r.uuid]: res.data.result }))
    } catch {
      setLlm(prev => ({ ...prev, [r.uuid]: "Erreur lors de l'analyse." }))
    } finally { setLlmLoading(null) }
  }

  return (
    <RoleGuard permission="matchCandidates">
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#F0F4F8" }}>
      <Topbar title="Candidats pour une offre" />

      {toast && <Toast message={toast.msg} type={toast.type} onClose={() => setToast(null)} />}

      <div className="flex flex-1 overflow-hidden">

        {/* Panel gauche */}
        <div className="flex flex-col overflow-hidden"
             style={{ width: "52%", borderRight: "1px solid #E2E8F0", background: "#F8FAFC" }}>

          <div className="p-4 space-y-3 flex-shrink-0"
               style={{ borderBottom: "1px solid #E2E8F0", background: "white" }}>

            {/* Recherche offre */}
            <SearchableSelect
              items={jobsData?.jobs ?? []}
              value={selectedJobUuid}
              onChange={uuid => { setSelectedJobUuid(uuid); setResults([]); setSelected(null) }}
              placeholder="Rechercher une offre..."
              emptyText="Aucune offre trouvee"
              getValue={j => j.uuid}
              getLabel={j => j.title}
              getMeta={j => `${j.company} - ${j.location} - ${j.experience_level} (${j.years_of_experience_required} ans)`}
              getSearchText={j => `${j.title} ${j.company} ${j.location} ${j.experience_level}`}
            />

            {/* Carte offre sélectionnée */}
            {jobDetail && (
              <div className="rounded-xl p-3 animate-fade-in"
                   style={{ background: "#EEF4FF", border: "1px solid #C7D9F5" }}>
                <div className="flex items-start justify-between gap-2">
                  <div className="min-w-0 flex-1">
                    <p className="text-sm font-semibold truncate" style={{ color: "#003B8E" }}>{jobDetail.title}</p>
                    <p className="text-xs mt-0.5" style={{ color: "#64748B" }}>
                      {jobDetail.company} · {jobDetail.location}
                    </p>
                  </div>
                  <div className="flex items-center gap-2 flex-shrink-0">
                    <span className="text-xs px-2 py-1 rounded-md"
                          style={{ background: "#FFFBEB", border: "1px solid #FCD34D", color: "#92400E" }}>
                      {jobDetail.experience_level} · {jobDetail.years_of_experience_required} ans
                    </span>
                  </div>
                </div>
                <div className="flex flex-wrap gap-1 mt-2">
                  {(jobDetail.technical_skills || []).slice(0, 7).map((s: string) => (
                    <span key={s} className="text-xs px-1.5 py-0.5 rounded"
                          style={{ fontSize: "10px", background: "white", border: "1px solid #CBD5E1", color: "#475569" }}>
                      {s}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {/* FilterPanel */}
            <FilterPanel
              mode={mode} onModeChange={setMode}
              limit={limit} onLimitChange={setLimit}
              expMin={expMin} expMax={expMax}
              onExpMinChange={setExpMin} onExpMaxChange={setExpMax}
              onSearch={handleSearch}
              onRerank={handleRerank}
              loading={loading} reranking={reranking}
              hasResults={results.length > 0}
              disabled={!selectedJobUuid}
              canRerank={can("rerank")}
            />
          </div>

          {/* Compteur résultats */}
          {results.length > 0 && (
            <div className="flex items-center justify-between px-4 py-2 flex-shrink-0"
                 style={{ background: "white", borderBottom: "1px solid #F1F5F9" }}>
              <span className="text-xs" style={{ color: "#64748B" }}>
                <span style={{ color: "#0066CC", fontWeight: 700 }}>{results.length}</span> candidat(s)
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

          {/* Liste candidats */}
          <div className="flex-1 overflow-y-auto p-3 space-y-2">
            {loading && (
              <div className="space-y-2">
                {[1, 2, 3, 4, 5].map(i => <SkeletonCard key={i} />)}
              </div>
            )}

            {!loading && results.length === 0 && selectedJobUuid && (
              <div className="flex flex-col items-center justify-center py-16 gap-2"
                   style={{ color: "#CBD5E1" }}>
                <p className="text-sm">Aucun résultat</p>
                <p style={{ fontSize: "11px" }}>Lance une recherche pour voir les candidats</p>
              </div>
            )}

            {results.map((r, i) => {
              const p = r.properties as CandidateProperties
              const years = Number(p.years_of_experience || 0)
              const level = yearsToLevel(years)
              const color = levelColors[level] || "#94A3B8"
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
                    <span className="text-xs font-mono w-5 flex-shrink-0" style={{ color: "#94A3B8" }}>
                      #{i + 1}
                    </span>
                    <div className="w-8 h-8 rounded-full flex items-center justify-center text-xs font-semibold flex-shrink-0"
                         style={{ background: color + "22", color }}>
                      {initials(p.full_name || "")}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-semibold truncate" style={{ color: "#1A2B4B" }}>{p.full_name}</p>
                      <p className="text-xs truncate" style={{ color: "#64748B", fontSize: "10px" }}>
                        {years} ans · <span style={{ color }}>{level}</span> · {p.company_source}
                      </p>
                    </div>
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
                    <button onClick={e => { e.stopPropagation(); handleLLM(r) }}
                            disabled={llmLoading === r.uuid}
                            className="w-full text-xs rounded-lg py-1.5 transvp ion-all flex items-center justify-center gap-1.5"
                            style={{ background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}>
                      <Brain className="w-3 h-3" />
                      {llmLoading === r.uuid ? "Analyse en cours..." : llm[r.uuid] ? "Analyse LLM ✓" : "Pourquoi ce candidat ?"}
                    </button>
                  )}

                  {llm[r.uuid] && (
                    <div className="mt-2 rounded-lg p-2.5 text-xs leading-relaxed animate-fade-in"
                         style={{ background: "#F0FDF4", border: "1px solid #BBF7D0", color: "#166534" }}
                         dangerouslySetInnerHTML={{ __html: renderLLMText(llm[r.uuid]) }}
                    />
                  )}
                </div>
              )
            })}
          </div>
        </div>

        {/* Panel droit */}
        <div className="flex-1 overflow-y-auto p-5" style={{ background: "#F8FAFC" }}>
          {selected ? (
            <CandidateDetail result={selected} />
          ) : (
            <div className="h-full flex flex-col items-center justify-center gap-3" style={{ color: "#CBD5E1" }}>
              <div className="w-16 h-16 rounded-2xl flex items-center justify-center"
                   style={{ background: "white", border: "1px solid #E2E8F0" }}>
                <Brain className="w-7 h-7" style={{ color: "#CBD5E1" }} />
              </div>
              <p className="text-sm">Sélectionne un candidat</p>
              <p style={{ fontSize: "11px" }}>pour voir son profil complet</p>
            </div>
          )}
        </div>
      </div>
    </div>
    </RoleGuard>
  )
}

function CandidateDetail({ result }: { result: MatchResult }) {
  const p = result.properties as CandidateProperties
  const years = Number(p.years_of_experience || 0)
  const level = yearsToLevel(years)
  const color = levelColors[level] || "#94A3B8"
  const score = result.rerank_score ?? result.score

  return (
    <div className="space-y-4 animate-fade-in">
      <div className="rounded-2xl p-4" style={{ background: "#EEF4FF", border: "1px solid #C7D9F5" }}>
        <div className="flex items-center gap-3">
          <div className="w-12 h-12 rounded-xl flex items-center justify-center text-sm font-semibold flex-shrink-0"
               style={{ background: color + "22", color }}>
            {initials(p.full_name || "")}
          </div>
          <div className="flex-1 min-w-0">
            <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>{p.full_name}</p>
            <p className="text-xs mt-0.5" style={{ color: "#64748B" }}>
              {(p.roles_held || []).slice(0, 2).join(" · ")}
            </p>
            <p className="text-xs mt-0.5" style={{ color: "#94A3B8", fontSize: "10px" }}>
              {p.location} · {(p.company_source || "").toUpperCase()}
            </p>
          </div>
          <div className="text-center">
            <ScoreRing score={score} size={60} stroke={5} />
            <p className="text-xs mt-1" style={{ color: "#94A3B8", fontSize: "9px" }}>score global</p>
          </div>
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

      <Section title="Compétences techniques">
        <div className="flex flex-wrap gap-1.5">
          {(p.technical_skills || []).map((s: string) => <Tag key={s} color="blue">{s}</Tag>)}
        </div>
      </Section>

      {(p.programming_languages || []).length > 0 && (
        <Section title="Langages">
          <div className="flex flex-wrap gap-1.5">
            {(p.programming_languages || []).map((l: string) => <Tag key={l} color="purple">{l}</Tag>)}
          </div>
        </Section>
      )}

      {(p.seniority_technologies || []).length > 0 && (
        <Section title="Séniorité par technologie">
          <div className="space-y-2">
            {(p.seniority_technologies || []).slice(0, 6).map((s: SeniorityTechnology) => {
              const lvlColor = levelColors[s.level] || "#64748B"
              const levelPct: Record<SeniorityLevel, number> = {
                Junior: 20,
                Medior: 40,
                Confirmé: 60,
                Senior: 80,
                Expert: 100,
              }
              const pct = levelPct[s.level] ?? 20
              return (
                <div key={s.technology} className="flex items-center gap-2.5">
                  <span className="text-xs flex-shrink-0"
                        style={{ color: "#475569", width: "110px", fontSize: "11px" }}>
                    {s.technology}
                  </span>
                  <div className="flex-1 rounded-full" style={{ height: "4px", background: "#E2E8F0" }}>
                    <div className="rounded-full" style={{ height: "4px", width: `${pct}%`, background: lvlColor }} />
                  </div>
                  <span className="text-xs flex-shrink-0 text-right"
                        style={{ color: lvlColor, width: "60px", fontSize: "10px" }}>
                    {s.level}
                  </span>
                </div>
              )
            })}
          </div>
        </Section>
      )}

      {p.career_trajectory_direction && (
        <Section title="Trajectoire de carrière">
          <p className="text-xs" style={{ color: "#475569", lineHeight: "1.6" }}>
            {p.career_trajectory_direction}
          </p>
          {p.career_trajectory_progression_speed && (
            <p className="text-xs mt-1" style={{ color: "#64748B" }}>
              Progression : <span style={{ color: "#0066CC", fontWeight: 600 }}>
                {p.career_trajectory_progression_speed}
              </span>
            </p>
          )}
          {(p.career_trajectory_skills_in_progress || []).length > 0 && (
            <div className="flex flex-wrap gap-1 mt-2">
              {(p.career_trajectory_skills_in_progress || []).map((s: string) => (
                <Tag key={s} color="amber">{s}</Tag>
              ))}
            </div>
          )}
        </Section>
      )}

      {(p.certifications || []).length > 0 && (
        <Section title="Certifications">
          <div className="flex flex-wrap gap-1.5">
            {(p.certifications || []).map((c: string) => <Tag key={c} color="green">{c}</Tag>)}
          </div>
        </Section>
      )}

      {p.summary && (
        <Section title="Résumé">
          <p className="text-xs leading-relaxed" style={{ color: "#475569" }}>
            {(p.summary || "").slice(0, 400)}{(p.summary || "").length > 400 ? "..." : ""}
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
