"use client"

import { useMemo, useSyncExternalStore } from "react"
import type { AuthUser } from "@/lib/rbac"

export type AuditAction =
  | "candidate_added"
  | "job_added"
  | "candidate_archived"
  | "job_archived"
  | "matching_launched"
  | "reranking_launched"

export type AuditEvent = {
  id: string
  action: AuditAction
  targetLabel: string
  targetUuid?: string
  context?: string
  createdAt: string
  actorName: string
  actorEmail: string
  actorRole: string
}

const STORAGE_KEY = "cv-scanner-audit-log"
const AUDIT_EVENT = "cv-scanner-audit-change"
const MAX_EVENTS = 120

export const AUDIT_LABELS: Record<AuditAction, string> = {
  candidate_added: "Candidat ajoute",
  job_added: "Offre ajoutee",
  candidate_archived: "Candidat archive",
  job_archived: "Offre archivee",
  matching_launched: "Matching lance",
  reranking_launched: "Reranking lance",
}

function safeWindow() {
  return typeof window !== "undefined" ? window : null
}

function readRawAudit() {
  return safeWindow()?.localStorage.getItem(STORAGE_KEY) || "[]"
}

function notifyAuditChange() {
  safeWindow()?.dispatchEvent(new Event(AUDIT_EVENT))
}

function parseAuditEvents(raw: string | null): AuditEvent[] {
  if (!raw) return []
  try {
    const parsed = JSON.parse(raw)
    if (!Array.isArray(parsed)) return []
    return parsed.filter(item =>
      item &&
      typeof item.id === "string" &&
      typeof item.action === "string" &&
      typeof item.targetLabel === "string" &&
      typeof item.createdAt === "string"
    ) as AuditEvent[]
  } catch {
    return []
  }
}

function subscribeToAudit(callback: () => void) {
  const win = safeWindow()
  if (!win) return () => {}
  win.addEventListener(AUDIT_EVENT, callback)
  win.addEventListener("storage", callback)
  return () => {
    win.removeEventListener(AUDIT_EVENT, callback)
    win.removeEventListener("storage", callback)
  }
}

function getAuditSnapshot() {
  return readRawAudit()
}

function getServerAuditSnapshot() {
  return "[]"
}

export function getAuditEvents() {
  return parseAuditEvents(readRawAudit())
}

export function recordAuditEvent({
  action,
  targetLabel,
  targetUuid,
  context,
  actor,
}: {
  action: AuditAction
  targetLabel: string
  targetUuid?: string
  context?: string
  actor: AuthUser | null
}) {
  const win = safeWindow()
  if (!win) return

  const previous = getAuditEvents()
  const event: AuditEvent = {
    id: `${Date.now()}-${Math.random().toString(16).slice(2)}`,
    action,
    targetLabel,
    targetUuid,
    context,
    createdAt: new Date().toISOString(),
    actorName: actor?.name || "Utilisateur",
    actorEmail: actor?.email || "session.locale",
    actorRole: actor?.role || "reader",
  }

  win.localStorage.setItem(STORAGE_KEY, JSON.stringify([event, ...previous].slice(0, MAX_EVENTS)))
  notifyAuditChange()
}

export function clearAuditEvents() {
  const win = safeWindow()
  if (!win) return
  win.localStorage.removeItem(STORAGE_KEY)
  notifyAuditChange()
}

export function useAuditEvents() {
  const snapshot = useSyncExternalStore(
    subscribeToAudit,
    getAuditSnapshot,
    getServerAuditSnapshot
  )

  return useMemo(() => parseAuditEvents(snapshot), [snapshot])
}

