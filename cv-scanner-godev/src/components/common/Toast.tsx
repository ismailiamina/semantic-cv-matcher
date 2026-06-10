"use client"
import { useEffect } from "react"
import { CheckCircle, XCircle, X } from "lucide-react"

interface ToastProps {
  message: string
  type: "success" | "error"
  onClose: () => void
}

export function Toast({ message, type, onClose }: ToastProps) {
  useEffect(() => {
    const t = setTimeout(onClose, 4000)
    return () => clearTimeout(t)
  }, [onClose])

  return (
    <div className="fixed bottom-6 right-6 z-50 flex items-center gap-3 px-4 py-3 rounded-xl animate-fade-in"
         style={{
           background: "white",
           border: `1px solid ${type === "success" ? "#BBF7D0" : "#FECACA"}`,
           boxShadow: "0 4px 16px rgba(0,0,0,0.08)",
           minWidth: "280px"
         }}>
      {type === "success"
        ? <CheckCircle className="w-4 h-4 flex-shrink-0" style={{ color: "#10B981" }} />
        : <XCircle    className="w-4 h-4 flex-shrink-0" style={{ color: "#EF4444" }} />
      }
      <span className="text-xs flex-1" style={{ color: "#1A2B4B" }}>{message}</span>
      <button onClick={onClose} className="flex-shrink-0 transition-opacity hover:opacity-60"
              style={{ color: "#94A3B8" }}>
        <X className="w-3.5 h-3.5" />
      </button>
    </div>
  )
}