"use client"

import { useEffect, useMemo, useState } from "react"
import { useSearchParams } from "next/navigation"
import { useQuery, useQueryClient } from "@tanstack/react-query"
import { Archive, Briefcase, Check, Copy, MapPin, Search, UserRound } from "lucide-react"
import { api, type CandidateSummary } from "@/lib/api"
import { Topbar } from "@/components/layout/Topbar"
import { Toast } from "@/components/common/Toast"
import { DeleteConfirmModal } from "@/components/common/DeleteConfirmModal"
import { UploadPanel } from "@/components/common/UploadPanel"
import { initials, yearsToLevel } from "@/lib/utils"
import { RoleGuard, useAuth } from "@/lib/rbac"
import { recordAuditEvent } from "@/lib/audit"

type ToastState = { msg: string; type: "success" | "error" } | null

type CandidateDetail = CandidateSummary & {
  location?: string
  roles_held?: string[]
  programming_languages?: string[]
  technical_skills?: string[]
  certifications?: string[]
  summary?: string
  education_level?: string
  field_of_studies?: string
}

export default function CandidatesPage() {
  const qc = useQueryClient()
  const searchParams = useSearchParams()
  const { can, user } = useAuth()
  const [query, setQuery] = useState("")
  const [selectedUuid, setSelectedUuid] = useState("")
  const [deleteTarget, setDeleteTarget] = useState<{ uuid: string; name: string } | null>(null)
  const [toast, setToast] = useState<ToastState>(null)

  const { data, isLoading } = useQuery({
    queryKey: ["cand-names"],
    queryFn: api.candidateNames,
  })

  const { data: selected } = useQuery<CandidateDetail>({
    queryKey: ["candidate-detail", selectedUuid],
    queryFn: () => api.candidate(selectedUuid),
    enabled: !!selectedUuid,
  })

  useEffect(() => {
    const candidateUuid = searchParams.get("candidate")
    if (!candidateUuid || candidateUuid === selectedUuid) return
    const timer = window.setTimeout(() => setSelectedUuid(candidateUuid), 0)
    return () => window.clearTimeout(timer)
  }, [searchParams, selectedUuid])

  const filtered = useMemo(() => {
    const candidates = data?.candidates ?? []
    const q = query.trim().toLowerCase()
    if (!q) return candidates
    return candidates.filter(c =>
      `${c.full_name} ${c.company_source} ${c.years_of_experience}`
        .toLowerCase()
        .includes(q)
    )
  }, [data?.candidates, query])

  const handleDelete = async () => {
    if (!deleteTarget) return
    try {
      await api.deleteCandidate(deleteTarget.uuid, true)
      if (selectedUuid === deleteTarget.uuid) setSelectedUuid("")
      setDeleteTarget(null)
      await Promise.all([
        qc.invalidateQueries({ queryKey: ["cand-names"] }),
        qc.invalidateQueries({ queryKey: ["stats"] }),
      ])
      recordAuditEvent({
        action: "candidate_archived",
        targetLabel: deleteTarget.name,
        targetUuid: deleteTarget.uuid,
        actor: user,
      })
      setToast({ msg: `Candidat archivé : ${deleteTarget.name}`, type: "success" })
    } catch {
      setToast({ msg: "Erreur lors de l'archivage du candidat", type: "error" })
      setDeleteTarget(null)
    }
  }

  return (
    <RoleGuard permission="viewCandidates">
    <div className="flex flex-col h-full overflow-hidden" style={{ background: "#F0F4F8" }}>
      <Topbar title="Gestion candidats" />
      {toast && <Toast message={toast.msg} type={toast.type} onClose={() => setToast(null)} />}
      {deleteTarget && (
        <DeleteConfirmModal
          name={deleteTarget.name}
          uuid={deleteTarget.uuid}
          type="candidate"
          onConfirm={handleDelete}
          onCancel={() => setDeleteTarget(null)}
        />
      )}

      <div className="flex flex-1 overflow-hidden">
        <div className="flex flex-col overflow-hidden"
             style={{ width: "46%", background: "white", borderRight: "1px solid #E2E8F0" }}>
          <div className="p-4 space-y-3" style={{ borderBottom: "1px solid #E2E8F0" }}>
            {can("manageCandidates") && (
              <UploadPanel
                type="candidate"
                onSuccess={msg => {
                  setToast({ msg, type: "success" })
                  qc.invalidateQueries({ queryKey: ["cand-names"] })
                  qc.invalidateQueries({ queryKey: ["stats"] })
                }}
                onError={msg => setToast({ msg, type: "error" })}
              />
            )}
            <div className="relative">
              <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4" style={{ color: "#94A3B8" }} />
              <input
                value={query}
                onChange={e => setQuery(e.target.value)}
                placeholder="Rechercher par nom, source ou expérience..."
                className="w-full text-sm rounded-lg pl-10 pr-3 py-2.5 focus:outline-none"
                style={{ background: "#F8FAFC", border: "1px solid #CBD5E1", color: "#1A2B4B" }}
              />
            </div>
            <p className="text-xs mt-3" style={{ color: "#64748B" }}>
              <span style={{ color: "#0066CC", fontWeight: 700 }}>{filtered.length}</span> candidat(s)
            </p>
          </div>

          <div className="flex-1 overflow-y-auto p-3 space-y-2">
            {isLoading && <p className="text-sm p-4" style={{ color: "#94A3B8" }}>Chargement...</p>}
            {!isLoading && filtered.map(candidate => {
              const active = selectedUuid === candidate.uuid
              return (
                <button
                  key={candidate.uuid}
                  onClick={() => setSelectedUuid(candidate.uuid)}
                  className="w-full text-left rounded-xl p-3 transition-all"
                  style={{
                    background: active ? "#EEF4FF" : "#F8FAFC",
                    border: `1px solid ${active ? "#0066CC" : "#E2E8F0"}`,
                  }}
                >
                  <div className="flex items-center gap-3">
                    <div className="w-9 h-9 rounded-full flex items-center justify-center text-xs font-semibold flex-shrink-0"
                         style={{ background: "#D1FAE5", color: "#065F46" }}>
                      {initials(candidate.full_name || "")}
                    </div>
                    <div className="flex-1 min-w-0">
                      <p className="text-sm font-semibold truncate" style={{ color: "#1A2B4B" }}>
                        {candidate.full_name}
                      </p>
                      <p className="text-xs truncate mt-0.5" style={{ color: "#64748B" }}>
                        {candidate.years_of_experience} ans · {yearsToLevel(candidate.years_of_experience)} · {candidate.company_source || "Source non précisée"}
                      </p>
                    </div>
                  </div>
                </button>
              )
            })}
            {!isLoading && filtered.length === 0 && (
              <p className="text-sm p-4 text-center" style={{ color: "#94A3B8" }}>Aucun candidat trouvé</p>
            )}
          </div>
        </div>

        <div className="flex-1 overflow-y-auto p-5">
          {selected ? (
            <CandidatePanel
              candidate={selected}
              onDelete={() => setDeleteTarget({ uuid: selected.uuid, name: selected.full_name })}
              canManage={can("manageCandidates")}
            />
          ) : (
            <EmptyState />
          )}
        </div>
      </div>
    </div>
    </RoleGuard>
  )
}

function CandidatePanel({ candidate, onDelete, canManage }: { candidate: CandidateDetail; onDelete: () => void; canManage: boolean }) {
  const [copied, setCopied] = useState(false)
  const copyUuid = async () => {
    await navigator.clipboard.writeText(candidate.uuid)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }

  return (
    <div className="space-y-4">
      <div className="rounded-xl p-4 flex items-start gap-4" style={{ background: "white", border: "1px solid #E2E8F0" }}>
        <div className="w-12 h-12 rounded-xl flex items-center justify-center text-sm font-semibold"
             style={{ background: "#D1FAE5", color: "#065F46" }}>
          {initials(candidate.full_name || "")}
        </div>
        <div className="flex-1 min-w-0">
          <p className="text-lg font-semibold" style={{ color: "#1A2B4B" }}>{candidate.full_name}</p>
          <div className="flex items-center gap-2 mt-1">
            <p className="text-xs font-mono truncate" style={{ color: "#94A3B8" }}>
              UUID : {candidate.uuid}
            </p>
            <button
              onClick={copyUuid}
              className="text-xs rounded-md px-2 py-1 flex items-center gap-1"
              style={{ background: "#F8FAFC", border: "1px solid #E2E8F0", color: "#64748B" }}
            >
              {copied ? <Check className="w-3 h-3" /> : <Copy className="w-3 h-3" />}
              {copied ? "Copié" : "Copier"}
            </button>
          </div>
          <div className="flex flex-wrap gap-3 mt-2 text-xs" style={{ color: "#64748B" }}>
            <span className="flex items-center gap-1"><Briefcase className="w-3.5 h-3.5" />{candidate.years_of_experience} ans</span>
            <span>{yearsToLevel(candidate.years_of_experience)}</span>
            {candidate.location && <span className="flex items-center gap-1"><MapPin className="w-3.5 h-3.5" />{candidate.location}</span>}
          </div>
        </div>
        {canManage && (
          <button
            onClick={onDelete}
            className="text-xs font-semibold rounded-lg px-3 py-2 flex items-center gap-1.5"
            style={{ background: "#FEF2F2", border: "1px solid #FECACA", color: "#B91C1C" }}
          >
            <Archive className="w-3.5 h-3.5" />
            Archiver
          </button>
        )}
      </div>

      <Section title="Résumé">
        <p className="text-sm leading-relaxed" style={{ color: "#475569" }}>
          {candidate.summary || "Aucun résumé disponible."}
        </p>
      </Section>

      <Section title="Compétences techniques">
        <TagList values={candidate.technical_skills} color="#003B8E" bg="#EEF4FF" border="#C7D9F5" />
      </Section>

      <Section title="Langages">
        <TagList values={candidate.programming_languages} color="#5B21B6" bg="#F5F3FF" border="#DDD6FE" />
      </Section>

      <Section title="Parcours">
        <div className="grid grid-cols-2 gap-3 text-sm">
          <Info label="Source" value={candidate.company_source || "Non précisée"} />
          <Info label="Formation" value={[candidate.education_level, candidate.field_of_studies].filter(Boolean).join(" · ") || "Non précisée"} />
        </div>
      </Section>
    </div>
  )
}

function Section({ title, children }: { title: string; children: React.ReactNode }) {
  return (
    <div className="rounded-xl p-4" style={{ background: "white", border: "1px solid #E2E8F0" }}>
      <p className="uppercase tracking-widest mb-3" style={{ fontSize: "9px", color: "#94A3B8", fontWeight: 700 }}>{title}</p>
      {children}
    </div>
  )
}

function TagList({ values, color, bg, border }: { values?: string[]; color: string; bg: string; border: string }) {
  if (!values?.length) return <p className="text-sm" style={{ color: "#94A3B8" }}>Aucune donnée</p>
  return (
    <div className="flex flex-wrap gap-1.5">
      {values.map(value => (
        <span key={value} className="rounded-md px-2 py-1 text-xs" style={{ color, background: bg, border: `1px solid ${border}` }}>
          {value}
        </span>
      ))}
    </div>
  )
}

function Info({ label, value }: { label: string; value: string }) {
  return (
    <div>
      <p className="text-xs mb-1" style={{ color: "#94A3B8" }}>{label}</p>
      <p style={{ color: "#1A2B4B" }}>{value}</p>
    </div>
  )
}

function EmptyState() {
  return (
    <div className="h-full flex flex-col items-center justify-center gap-3" style={{ color: "#CBD5E1" }}>
      <div className="w-16 h-16 rounded-2xl flex items-center justify-center" style={{ background: "white", border: "1px solid #E2E8F0" }}>
        <UserRound className="w-7 h-7" />
      </div>
      <p className="text-sm">Sélectionne un candidat</p>
    </div>
  )
}
