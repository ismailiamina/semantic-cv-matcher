"use client"
import { useState } from "react"
import { Archive, AlertTriangle, X, Loader2 } from "lucide-react"

interface DeleteConfirmModalProps {
  name: string
  uuid: string
  type: "candidate" | "job"
  onConfirm: () => Promise<void>
  onCancel: () => void
}

export function DeleteConfirmModal({ name, uuid, type, onConfirm, onCancel }: DeleteConfirmModalProps) {
  const [loading, setLoading] = useState(false)

  const handleConfirm = async () => {
    setLoading(true)
    try {
      await onConfirm()
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center"
         style={{ background: "rgba(0,0,0,0.4)", backdropFilter: "blur(4px)" }}>
      <div className="rounded-2xl p-6 w-full max-w-md mx-4 animate-fade-in"
           style={{ background: "white", border: "1px solid #E2E8F0", boxShadow: "0 20px 60px rgba(0,0,0,0.15)" }}>

        {/* Header */}
        <div className="flex items-start justify-between mb-4">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0"
                 style={{ background: "#FEF2F2" }}>
              <AlertTriangle className="w-5 h-5" style={{ color: "#EF4444" }} />
            </div>
            <div>
              <p className="text-sm font-semibold" style={{ color: "#1A2B4B" }}>
                Confirmer l&apos;archivage
              </p>
              <p className="text-xs mt-0.5" style={{ color: "#94A3B8" }}>
                Cette action archive la donnée
              </p>
            </div>
          </div>
          <button onClick={onCancel} style={{ color: "#94A3B8" }}>
            <X className="w-4 h-4" />
          </button>
        </div>

        {/* Contenu */}
        <div className="rounded-xl p-3 mb-5"
             style={{ background: "#FEF2F2", border: "1px solid #FECACA" }}>
          <p className="text-xs" style={{ color: "#991B1B" }}>
            Tu vas archiver le {type === "candidate" ? "candidat" : "poste"} :
          </p>
          <p className="text-sm font-semibold mt-1" style={{ color: "#1A2B4B" }}>{name}</p>
          <p className="text-xs mt-1 font-mono" style={{ color: "#94A3B8" }}>UUID : {uuid}</p>
        </div>

        <p className="text-xs mb-5" style={{ color: "#64748B" }}>
          L&apos;élément restera dans Weaviate, mais il sera masqué des listes et du matching actif.
        </p>

        {/* Boutons */}
        <div className="flex gap-3">
          <button onClick={onCancel} disabled={loading}
            className="flex-1 text-sm font-medium rounded-xl py-2.5 transition-all"
            style={{ background: "white", border: "1px solid #E2E8F0", color: "#64748B" }}>
            Annuler
          </button>
          <button onClick={handleConfirm} disabled={loading}
            className="flex-1 text-sm font-semibold rounded-xl py-2.5 transition-all flex items-center justify-center gap-2"
            style={{ background: "#EF4444", color: "white" }}>
            {loading ? (
              <><Loader2 className="w-4 h-4 animate-spin" />Archivage...</>
            ) : (
              <><Archive className="w-4 h-4" />Archiver</>
            )}
          </button>
        </div>
      </div>
    </div>
  )
}
