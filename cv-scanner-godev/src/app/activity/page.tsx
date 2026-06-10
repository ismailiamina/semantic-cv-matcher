"use client"

import { Archive, Briefcase, History, RotateCcw, SearchCheck, Trash2, UserPlus } from "lucide-react"
import { Topbar } from "@/components/layout/Topbar"
import { RoleGuard } from "@/lib/rbac"
import { AUDIT_LABELS, clearAuditEvents, useAuditEvents, type AuditAction } from "@/lib/audit"

const ACTION_STYLES: Record<AuditAction, { icon: React.ElementType; bg: string; color: string; border: string }> = {
  candidate_added: { icon: UserPlus, bg: "#F0FDF4", color: "#047857", border: "#BBF7D0" },
  job_added: { icon: Briefcase, bg: "#EEF4FF", color: "#003B8E", border: "#C7D9F5" },
  candidate_archived: { icon: Archive, bg: "#FEF2F2", color: "#B91C1C", border: "#FECACA" },
  job_archived: { icon: Archive, bg: "#FEF2F2", color: "#B91C1C", border: "#FECACA" },
  matching_launched: { icon: SearchCheck, bg: "#F5F3FF", color: "#5B21B6", border: "#DDD6FE" },
  reranking_launched: { icon: RotateCcw, bg: "#FFFBEB", color: "#92400E", border: "#FCD34D" },
}

function formatDate(value: string) {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return new Intl.DateTimeFormat("fr-FR", {
    dateStyle: "medium",
    timeStyle: "short",
  }).format(date)
}

export default function ActivityPage() {
  const events = useAuditEvents()

  return (
    <RoleGuard permission="activity">
      <div className="flex h-full flex-col overflow-hidden" style={{ background: "#F0F4F8" }}>
        <Topbar title="Historique des actions" />

        <div className="flex-1 overflow-y-auto p-6">
          <div className="mb-5 flex items-center justify-between gap-4">
            <div>
              <p className="text-xs font-semibold uppercase tracking-[0.18em]" style={{ color: "#0066CC" }}>
                Traçabilite locale
              </p>
              <h1 className="mt-2 text-2xl font-bold" style={{ color: "#10213F" }}>
                Journal des operations
              </h1>
              <p className="mt-2 text-sm" style={{ color: "#64748B" }}>
                Suivi des uploads, archivages, recherches de matching et reranking lances dans cette interface.
              </p>
            </div>
            {events.length > 0 && (
              <button
                onClick={clearAuditEvents}
                className="flex items-center gap-2 rounded-xl px-4 py-2.5 text-sm font-semibold"
                style={{ background: "white", border: "1px solid #E2E8F0", color: "#64748B" }}
              >
                <Trash2 className="h-4 w-4" />
                Vider
              </button>
            )}
          </div>

          <div className="rounded-2xl" style={{ background: "white", border: "1px solid #E2E8F0" }}>
            {events.length === 0 ? (
              <div className="flex flex-col items-center justify-center px-6 py-16 text-center">
                <div className="flex h-14 w-14 items-center justify-center rounded-2xl" style={{ background: "#F8FAFC", color: "#94A3B8" }}>
                  <History className="h-7 w-7" />
                </div>
                <p className="mt-4 text-sm font-semibold" style={{ color: "#1A2B4B" }}>
                  Aucun evenement enregistre
                </p>
                <p className="mt-2 max-w-md text-sm leading-6" style={{ color: "#64748B" }}>
                  Les actions seront ajoutees automatiquement apres un upload, un archivage, un matching ou un reranking.
                </p>
              </div>
            ) : (
              <div className="divide-y" style={{ borderColor: "#E2E8F0" }}>
                {events.map(event => {
                  const style = ACTION_STYLES[event.action]
                  const Icon = style.icon
                  return (
                    <div key={event.id} className="grid grid-cols-[auto_1fr_auto] items-center gap-4 px-5 py-4">
                      <div className="flex h-11 w-11 items-center justify-center rounded-xl" style={{ background: style.bg, color: style.color, border: `1px solid ${style.border}` }}>
                        <Icon className="h-5 w-5" />
                      </div>
                      <div className="min-w-0">
                        <div className="flex flex-wrap items-center gap-2">
                          <p className="text-sm font-semibold" style={{ color: "#10213F" }}>
                            {AUDIT_LABELS[event.action]}
                          </p>
                          <span className="rounded-full px-2 py-0.5 text-xs" style={{ background: style.bg, color: style.color, border: `1px solid ${style.border}` }}>
                            {event.actorRole}
                          </span>
                        </div>
                        <p className="mt-1 truncate text-sm" style={{ color: "#475569" }}>
                          {event.targetLabel}
                        </p>
                        {event.context && (
                          <p className="mt-1 text-xs" style={{ color: "#94A3B8" }}>
                            {event.context}
                          </p>
                        )}
                      </div>
                      <div className="text-right">
                        <p className="text-xs font-medium" style={{ color: "#64748B" }}>
                          {formatDate(event.createdAt)}
                        </p>
                        <p className="mt-1 text-xs" style={{ color: "#94A3B8" }}>
                          {event.actorName}
                        </p>
                      </div>
                    </div>
                  )
                })}
              </div>
            )}
          </div>
        </div>
      </div>
    </RoleGuard>
  )
}

