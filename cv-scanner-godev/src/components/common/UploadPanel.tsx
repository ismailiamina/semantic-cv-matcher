"use client"

import { useRef, useState } from "react"
import { useRouter } from "next/navigation"
import { useQueryClient } from "@tanstack/react-query"
import { AlertTriangle, CheckCircle, ExternalLink, FileText, Link, Loader2, Upload, X } from "lucide-react"
import { api, type CandidateSummary, type JobSummary, type UploadCandidateResponse, type UploadDuplicate, type UploadJobResponse } from "@/lib/api"
import { recordAuditEvent } from "@/lib/audit"
import { useAuth } from "@/lib/rbac"

interface UploadPanelProps {
  type: "candidate" | "job"
  onSuccess: (msg: string) => void
  onError: (msg: string) => void
}

type PendingUpload =
  | { kind: "file"; file: File }
  | { kind: "url"; url: string }

type DuplicateEntity = {
  targetType: "candidate" | "job"
  uuid: string
  label: string
  subtitle?: string
  reason: string
}

type DuplicateState = {
  entity: DuplicateEntity
  pending: PendingUpload
}

type CandidateDetailForDuplicate = CandidateSummary & {
  email?: string
  linkedin?: string
  location?: string
}

type CandidateHints = {
  name?: string
  email?: string
  linkedin?: string
}

function normalizeText(value?: string) {
  return (value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9\s]/g, " ")
    .replace(/\s+/g, " ")
    .trim()
}

function normalizeEmail(value?: string) {
  return (value || "").trim().toLowerCase()
}

function normalizeLinkedInUrl(value?: string) {
  if (!value) return ""
  const clean = value.trim().toLowerCase().split("?")[0].replace(/\/+$/, "")
  const profile = clean.match(/linkedin\.com\/in\/([^/]+)/)
  return profile ? profile[1].replace(/\/+$/, "") : clean
}

function extractLinkedInName(url: string) {
  const slug = normalizeLinkedInUrl(url)
  if (!slug || slug.includes("linkedin.com")) return ""
  const words = slug
    .split(/[-_]/)
    .filter(word => word && !/\d/.test(word) && word.length > 1)
  return words.length >= 2 ? words.join(" ") : ""
}

function extractFileNameHint(fileName: string) {
  const base = fileName.replace(/\.[^.]+$/, "")
  const cleaned = base
    .replace(/cv|resume|curriculum|vitae|candidat|candidate|offre|job|final/gi, " ")
    .replace(/[_\-().]/g, " ")
    .replace(/\d+/g, " ")
    .replace(/\s+/g, " ")
    .trim()
  return cleaned
}

function includesNormalized(source: string, value?: string, minLength = 3) {
  const normalized = normalizeText(value)
  return normalized.length >= minLength && source.includes(normalized)
}

async function enrichCandidate(candidate: CandidateSummary): Promise<CandidateDetailForDuplicate> {
  try {
    const detail = await api.candidate(candidate.uuid)
    return { ...candidate, ...detail }
  } catch {
    return candidate
  }
}

function getApiErrorMessage(error: unknown, fallback: string) {
  if (typeof error === "object" && error && "response" in error) {
    const response = (error as { response?: { data?: { detail?: unknown } } }).response
    if (typeof response?.data?.detail === "string") return response.data.detail
  }
  return fallback
}

async function findSimilarCandidate(pending: PendingUpload): Promise<DuplicateEntity | null> {
  const hints: CandidateHints = pending.kind === "url"
    ? { linkedin: pending.url, name: extractLinkedInName(pending.url) }
    : { name: extractFileNameHint(pending.file.name) }

  const name = normalizeText(hints.name)
  const email = normalizeEmail(hints.email)
  const linkedin = normalizeLinkedInUrl(hints.linkedin)

  if (!name && !email && !linkedin) return null

  const data = await api.candidateNames()
  const candidates = data.candidates ?? []

  const nameMatches = name
    ? candidates.filter(candidate => {
      const candidateName = normalizeText(candidate.full_name)
      return candidateName.length >= 3 && (candidateName === name || name.includes(candidateName))
    })
    : []

  const candidatesToInspect = linkedin || email
    ? candidates
    : nameMatches

  const detailedCandidates = await Promise.all(candidatesToInspect.map(enrichCandidate))

  if (linkedin) {
    const match = detailedCandidates.find(candidate => normalizeLinkedInUrl(candidate.linkedin) === linkedin)
    if (match) {
      return {
        targetType: "candidate",
        uuid: match.uuid,
        label: match.full_name,
        subtitle: `${match.years_of_experience} ans - ${match.company_source || "Source non precisee"}`,
        reason: "Meme profil LinkedIn detecte",
      }
    }
  }

  if (email) {
    const match = detailedCandidates.find(candidate => normalizeEmail(candidate.email) === email)
    if (match) {
      return {
        targetType: "candidate",
        uuid: match.uuid,
        label: match.full_name,
        subtitle: `${match.years_of_experience} ans - ${match.company_source || "Source non precisee"}`,
        reason: "Meme adresse email detectee",
      }
    }
  }

  const exactName = nameMatches[0]
  if (exactName) {
    const detail = await enrichCandidate(exactName)
    return {
      targetType: "candidate",
      uuid: detail.uuid,
      label: detail.full_name,
      subtitle: `${detail.years_of_experience} ans - ${detail.company_source || "Source non precisee"}`,
      reason: "Meme nom candidat detecte",
    }
  }

  return null
}

async function findSimilarJob(pending: PendingUpload): Promise<DuplicateEntity | null> {
  if (pending.kind === "url") return null

  const fileHint = normalizeText(extractFileNameHint(pending.file.name))
  if (!fileHint) return null

  const data = await api.jobTitles()
  const jobs = data.jobs ?? []

  const sameTitleAndCompany = jobs.find(job =>
    includesNormalized(fileHint, job.title, 6) &&
    includesNormalized(fileHint, job.company, 3)
  )
  if (sameTitleAndCompany) return jobToDuplicate(sameTitleAndCompany, "Meme titre et meme entreprise detectes")

  const sameTitleAndLocation = jobs.find(job =>
    includesNormalized(fileHint, job.title, 6) &&
    includesNormalized(fileHint, job.location, 4)
  )
  if (sameTitleAndLocation) return jobToDuplicate(sameTitleAndLocation, "Meme titre et meme localisation detectes")

  const sameTitle = jobs.find(job => {
    const title = normalizeText(job.title)
    return title.length >= 10 && (fileHint === title || fileHint.includes(title))
  })
  if (sameTitle) return jobToDuplicate(sameTitle, "Titre d'offre deja present dans la base")

  return null
}

function jobToDuplicate(job: JobSummary, reason: string): DuplicateEntity {
  return {
    targetType: "job",
    uuid: job.uuid,
    label: job.title,
    subtitle: `${job.company || "Entreprise non precisee"} - ${job.location || "Localisation non precisee"}`,
    reason,
  }
}

function uploadDuplicateToEntity(
  targetType: "candidate" | "job",
  label: string,
  duplicates?: UploadDuplicate[]
): DuplicateEntity | null {
  const duplicate = duplicates?.[0]
  if (!duplicate?.uuid) return null

  return {
    targetType,
    uuid: duplicate.uuid,
    label: duplicate.label || label,
    subtitle: targetType === "job" && duplicate.company ? duplicate.company : undefined,
    reason: duplicate.reason || "Doublon potentiel detecte",
  }
}

export function UploadPanel({ type, onSuccess, onError }: UploadPanelProps) {
  const router = useRouter()
  const { user } = useAuth()
  const [tab, setTab] = useState<"file" | "url">("file")
  const [dragging, setDragging] = useState(false)
  const [loading, setLoading] = useState(false)
  const [url, setUrl] = useState("")
  const [fileName, setFileName] = useState("")
  const [duplicate, setDuplicate] = useState<DuplicateState | null>(null)
  const inputRef = useRef<HTMLInputElement>(null)
  const qc = useQueryClient()

  const label = type === "candidate" ? "CV candidat" : "offre d'emploi"
  const accent = type === "candidate" ? "#10B981" : "#0066CC"
  const accentLight = type === "candidate" ? "#F0FDF4" : "#EEF4FF"
  const accentBorder = type === "candidate" ? "#BBF7D0" : "#C7D9F5"

  const invalidateData = () => {
    qc.invalidateQueries({ queryKey: ["cand-names"] })
    qc.invalidateQueries({ queryKey: ["job-titles"] })
    qc.invalidateQueries({ queryKey: ["stats"] })
  }

  const executeUpload = async (pending: PendingUpload, forceInsert = false) => {
    setLoading(true)
    let keepDuplicateModal = false
    try {
      if (pending.kind === "file") {
        setFileName(pending.file.name)
        if (type === "candidate") {
          const res = await api.uploadCandidateFile(pending.file, forceInsert)
          if (res.status === "duplicate" && !forceInsert) {
            const entity = uploadDuplicateToEntity("candidate", res.name, res.duplicates)
            if (entity) {
              keepDuplicateModal = true
              setDuplicate({ entity, pending })
              return
            }
          }
          recordAuditEvent({
            action: "candidate_added",
            targetLabel: res.name,
            targetUuid: res.uuid,
            context: "Upload fichier",
            actor: user,
          })
          onSuccess(res.message)
        } else {
          const res = await api.uploadJobFile(pending.file, forceInsert)
          if (res.status === "duplicate" && !forceInsert) {
            const entity = uploadDuplicateToEntity("job", res.title, res.duplicates)
            if (entity) {
              keepDuplicateModal = true
              setDuplicate({ entity, pending })
              return
            }
          }
          recordAuditEvent({
            action: "job_added",
            targetLabel: res.title,
            targetUuid: res.uuid,
            context: "Upload fichier",
            actor: user,
          })
          onSuccess(res.message)
        }
        setFileName("")
      } else {
        const res = await api.uploadFromUrl(pending.url, type, forceInsert) as UploadCandidateResponse | UploadJobResponse
        if (type === "candidate") {
          const candidateRes = res as UploadCandidateResponse
          if (candidateRes.status === "duplicate" && !forceInsert) {
            const entity = uploadDuplicateToEntity("candidate", candidateRes.name, candidateRes.duplicates)
            if (entity) {
              keepDuplicateModal = true
              setDuplicate({ entity, pending })
              return
            }
          }
          recordAuditEvent({
            action: "candidate_added",
            targetLabel: candidateRes.name,
            targetUuid: candidateRes.uuid,
            context: "Upload LinkedIn",
            actor: user,
          })
          onSuccess(`Candidat ajoute : ${candidateRes.name}`)
        } else {
          const jobRes = res as UploadJobResponse
          if (jobRes.status === "duplicate" && !forceInsert) {
            const entity = uploadDuplicateToEntity("job", jobRes.title, jobRes.duplicates)
            if (entity) {
              keepDuplicateModal = true
              setDuplicate({ entity, pending })
              return
            }
          }
          recordAuditEvent({
            action: "job_added",
            targetLabel: jobRes.title,
            targetUuid: jobRes.uuid,
            context: "Upload LinkedIn",
            actor: user,
          })
          onSuccess(`Offre ajoutee : ${jobRes.title}`)
        }
        setUrl("")
      }
      invalidateData()
    } catch (error: unknown) {
      onError(getApiErrorMessage(error, pending.kind === "url" ? "Erreur lors du scraping LinkedIn" : "Erreur lors de l'upload"))
      setFileName("")
    } finally {
      setLoading(false)
      if (!keepDuplicateModal) setDuplicate(null)
    }
  }

  const prepareUpload = async (pending: PendingUpload) => {
    setLoading(true)
    try {
      const similar = type === "candidate"
        ? await findSimilarCandidate(pending)
        : await findSimilarJob(pending)

      if (similar) {
        setDuplicate({ entity: similar, pending })
        return
      }

      await executeUpload(pending)
    } catch (error: unknown) {
      onError(getApiErrorMessage(error, "Erreur lors de la verification des doublons"))
    } finally {
      setLoading(false)
    }
  }

  const handleFile = async (file: File) => {
    const allowed = [".pdf", ".docx", ".txt"]
    const ext = "." + file.name.split(".").pop()?.toLowerCase()
    if (!allowed.includes(ext)) {
      onError("Format non supporte. Utilisez PDF, DOCX ou TXT.")
      return
    }
    setFileName(file.name)
    await prepareUpload({ kind: "file", file })
  }

  const handleUrl = async () => {
    const cleanUrl = url.trim()
    if (!cleanUrl) return
    if (!cleanUrl.includes("linkedin.com")) {
      onError("URL LinkedIn requise (linkedin.com/in/... ou linkedin.com/jobs/...)")
      return
    }

    await prepareUpload({ kind: "url", url: cleanUrl })
  }

  const viewExistingDuplicate = () => {
    if (!duplicate) return
    const uuid = duplicate.entity.uuid
    setDuplicate(null)
    router.push(duplicate.entity.targetType === "candidate" ? `/candidates?candidate=${uuid}` : `/offers?job=${uuid}`)
  }

  return (
    <div className="rounded-xl overflow-hidden" style={{ border: "1px solid #E2E8F0" }}>
      {duplicate && (
        <DuplicateModal
          duplicate={duplicate}
          onViewExisting={viewExistingDuplicate}
          onContinue={() => executeUpload(duplicate.pending, true)}
          onCancel={() => {
            setDuplicate(null)
            setLoading(false)
          }}
          loading={loading}
        />
      )}

      <div className="px-4 py-3 flex items-center justify-between"
           style={{ background: accentLight, borderBottom: "1px solid " + accentBorder }}>
        <p className="text-xs font-semibold" style={{ color: accent }}>
          Ajouter un {label}
        </p>
        <div className="flex gap-1">
          {(["file", "url"] as const).map(item => (
            <button
              key={item}
              onClick={() => setTab(item)}
              className="text-xs px-2.5 py-1 rounded-md transition-all"
              style={{
                background: tab === item ? accent : "transparent",
                color: tab === item ? "white" : accent,
                fontWeight: tab === item ? 600 : 400,
              }}
            >
              {item === "file" ? "Fichier" : "LinkedIn"}
            </button>
          ))}
        </div>
      </div>

      <div className="p-3" style={{ background: "white" }}>
        {tab === "file" && (
          <div>
            <div
              onClick={() => !loading && inputRef.current?.click()}
              onDragOver={event => { event.preventDefault(); setDragging(true) }}
              onDragLeave={() => setDragging(false)}
              onDrop={event => {
                event.preventDefault()
                setDragging(false)
                const file = event.dataTransfer.files[0]
                if (file) handleFile(file)
              }}
              className="rounded-lg flex flex-col items-center justify-center py-6 gap-2 cursor-pointer transition-all"
              style={{
                border: `2px dashed ${dragging ? accent : "#CBD5E1"}`,
                background: dragging ? accentLight : "#FAFAFA",
              }}
            >
              {loading ? (
                <>
                  <Loader2 className="w-6 h-6 animate-spin" style={{ color: accent }} />
                  <p className="text-xs font-medium" style={{ color: accent }}>
                    Verification en cours...
                  </p>
                  {fileName && <p className="text-xs" style={{ color: "#94A3B8" }}>{fileName}</p>}
                </>
              ) : (
                <>
                  <div className="w-10 h-10 rounded-full flex items-center justify-center" style={{ background: accentLight }}>
                    <Upload className="w-4 h-4" style={{ color: accent }} />
                  </div>
                  <p className="text-xs font-medium" style={{ color: "#1A2B4B" }}>
                    Glisse ou clique pour uploader
                  </p>
                  <p className="text-xs" style={{ color: "#94A3B8" }}>
                    PDF - DOCX - TXT
                  </p>
                </>
              )}
            </div>
            <input
              ref={inputRef}
              type="file"
              accept=".pdf,.docx,.txt"
              className="hidden"
              onChange={event => {
                const file = event.target.files?.[0]
                if (file) handleFile(file)
                event.target.value = ""
              }}
            />
          </div>
        )}

        {tab === "url" && (
          <div className="space-y-2">
            <div className="flex gap-2">
              <div className="flex-1 relative">
                <Link className="absolute left-2.5 top-1/2 -translate-y-1/2 w-3.5 h-3.5" style={{ color: "#94A3B8" }} />
                <input
                  value={url}
                  onChange={event => setUrl(event.target.value)}
                  onKeyDown={event => event.key === "Enter" && handleUrl()}
                  placeholder={type === "candidate" ? "linkedin.com/in/nom-prenom" : "linkedin.com/jobs/view/..."}
                  className="w-full text-xs rounded-lg pl-8 pr-3 py-2 focus:outline-none"
                  style={{ background: "#F8FAFC", border: "1px solid #CBD5E1", color: "#1A2B4B" }}
                />
              </div>
              {url && (
                <button onClick={() => setUrl("")} className="px-2 rounded-lg" style={{ color: "#94A3B8" }}>
                  <X className="w-4 h-4" />
                </button>
              )}
            </div>
            <button
              onClick={handleUrl}
              disabled={!url.trim() || loading}
              className="w-full text-xs font-semibold rounded-lg py-2 transition-all flex items-center justify-center gap-1.5 disabled:opacity-40"
              style={{ background: accent, color: "white" }}
            >
              {loading ? (
                <>
                  <Loader2 className="w-3 h-3 animate-spin" />
                  Verification LinkedIn...
                </>
              ) : (
                <>
                  <FileText className="w-3 h-3" />
                  Extraire et ajouter
                </>
              )}
            </button>
            <p className="text-xs text-center" style={{ color: "#94A3B8" }}>
              Profil ou offre LinkedIn public uniquement
            </p>
          </div>
        )}
      </div>
    </div>
  )
}

function DuplicateModal({
  duplicate,
  onViewExisting,
  onContinue,
  onCancel,
  loading,
}: {
  duplicate: DuplicateState
  onViewExisting: () => void
  onContinue: () => void
  onCancel: () => void
  loading: boolean
}) {
  const isCandidate = duplicate.entity.targetType === "candidate"

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center" style={{ background: "rgba(15,23,42,0.42)", backdropFilter: "blur(5px)" }}>
      <div className="w-full max-w-lg rounded-2xl p-6" style={{ background: "white", border: "1px solid #E2E8F0", boxShadow: "0 24px 80px rgba(15,23,42,0.18)" }}>
        <div className="mb-5 flex items-start justify-between gap-4">
          <div className="flex items-start gap-3">
            <div className="flex h-11 w-11 items-center justify-center rounded-xl" style={{ background: "#FFFBEB", color: "#92400E", border: "1px solid #FCD34D" }}>
              <AlertTriangle className="h-5 w-5" />
            </div>
            <div>
              <p className="text-base font-bold" style={{ color: "#10213F" }}>
                {isCandidate ? "Profil similaire detecte" : "Offre similaire detectee"}
              </p>
              <p className="mt-1 text-sm" style={{ color: "#64748B" }}>
                {duplicate.entity.reason}. Verifiez l&apos;element existant avant de creer une nouvelle version.
              </p>
            </div>
          </div>
          <button onClick={onCancel} className="rounded-lg p-1" style={{ color: "#94A3B8" }}>
            <X className="h-4 w-4" />
          </button>
        </div>

        <div className="rounded-xl p-4" style={{ background: "#F8FAFC", border: "1px solid #E2E8F0" }}>
          <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>
            {duplicate.entity.label}
          </p>
          {duplicate.entity.subtitle && (
            <p className="mt-1 text-xs" style={{ color: "#64748B" }}>
              {duplicate.entity.subtitle}
            </p>
          )}
          <p className="mt-2 truncate text-xs font-mono" style={{ color: "#94A3B8" }}>
            UUID : {duplicate.entity.uuid}
          </p>
        </div>

        <div className="mt-5 grid grid-cols-3 gap-3">
          <button
            onClick={onCancel}
            disabled={loading}
            className="rounded-xl px-3 py-2.5 text-sm font-semibold"
            style={{ background: "white", border: "1px solid #E2E8F0", color: "#64748B" }}
          >
            Annuler
          </button>
          <button
            onClick={onViewExisting}
            disabled={loading}
            className="flex items-center justify-center gap-2 rounded-xl px-3 py-2.5 text-sm font-semibold"
            style={{ background: "#EEF4FF", border: "1px solid #C7D9F5", color: "#003B8E" }}
          >
            <ExternalLink className="h-4 w-4" />
            {isCandidate ? "Voir le profil" : "Voir l'offre"}
          </button>
          <button
            onClick={onContinue}
            disabled={loading}
            className="flex items-center justify-center gap-2 rounded-xl px-3 py-2.5 text-sm font-semibold"
            style={{ background: "#10B981", color: "white" }}
          >
            {loading ? <Loader2 className="h-4 w-4 animate-spin" /> : <CheckCircle className="h-4 w-4" />}
            Continuer
          </button>
        </div>
      </div>
    </div>
  )
}
