"use client"
import { useState } from "react"
import { Settings2 } from "lucide-react"

interface FilterPanelProps {
  mode: string
  onModeChange: (v: string) => void
  limit: number
  onLimitChange: (v: number) => void
  expMin: number
  expMax: number
  onExpMinChange: (v: number) => void
  onExpMaxChange: (v: number) => void
  onSearch: () => void
  onRerank: () => void
  loading: boolean
  reranking: boolean
  hasResults: boolean
  disabled: boolean
  canRerank?: boolean
}

export function FilterPanel({
  mode, onModeChange,
  limit, onLimitChange,
  expMin, expMax, onExpMinChange, onExpMaxChange,
  onSearch, onRerank,
  loading, reranking, hasResults, disabled, canRerank = true
}: FilterPanelProps) {
  const [open, setOpen] = useState(false)

  return (
    <div className="space-y-3">

      {/* Ligne principale */}
      <div className="flex gap-2">
        <select value={mode} onChange={e => onModeChange(e.target.value)}
          className="text-xs rounded-lg px-3 py-2 focus:outline-none"
          style={{ background: "white", border: "1px solid #CBD5E1", color: "#475569", minWidth: "105px" }}>
          <option value="hybride">🔀 Hybride</option>
          <option value="vecteur">🔵 Vecteur</option>
          <option value="texte">🔤 Texte</option>
        </select>

        <button onClick={onSearch} disabled={disabled || loading}
          className="flex-1 text-xs font-semibold rounded-lg px-4 py-2 transition-all flex items-center justify-center gap-1.5 disabled:opacity-40"
          style={{ background: "#0066CC", color: "white" }}>
          {loading ? (
            <>
              <span className="w-3 h-3 border-2 rounded-full animate-spin"
                    style={{ borderColor: "rgba(255,255,255,0.3)", borderTopColor: "white" }} />
              Recherche...
            </>
          ) : (
            <>
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                      d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z" />
              </svg>
              Lancer la recherche
            </>
          )}
        </button>

        {canRerank && (
          <button onClick={onRerank} disabled={!hasResults || reranking}
            className="text-xs font-semibold rounded-lg px-3 py-2 transition-all disabled:opacity-40 flex items-center gap-1.5"
            style={{ background: "white", border: "1px solid #003B8E", color: "#003B8E" }}>
            {reranking ? (
              <span className="w-3 h-3 border-2 rounded-full animate-spin"
                    style={{ borderColor: "rgba(0,59,142,0.2)", borderTopColor: "#003B8E" }} />
            ) : (
              <svg className="w-3.5 h-3.5" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2}
                      d="M3 4h13M3 8h9m-9 4h6m4 0l4-4m0 0l4 4m-4-4v12" />
              </svg>
            )}
            Reranker
          </button>
        )}

        <button onClick={() => setOpen(!open)}
          className="text-xs rounded-lg px-2.5 py-2 transition-all"
          style={{
            background: open ? "#EEF4FF" : "white",
            border: `1px solid ${open ? "#0066CC" : "#CBD5E1"}`,
            color: open ? "#0066CC" : "#64748B"
          }}>
          <Settings2 className="w-4 h-4" />
        </button>
      </div>

      {/* Panel filtres avancés */}
      {open && (
        <div className="rounded-xl p-4 space-y-4 animate-fade-in"
             style={{ background: "#F8FAFC", border: "1px solid #E2E8F0" }}>

          {/* Nombre de résultats */}
          <div>
            <div className="flex items-center justify-between mb-2">
              <span className="text-xs font-medium" style={{ color: "#475569" }}>Nombre de résultats</span>
              <span className="text-xs font-bold px-2 py-0.5 rounded"
                    style={{ background: "#EEF4FF", color: "#003B8E", fontFamily: "monospace" }}>
                {limit}
              </span>
            </div>
            <div className="flex items-center gap-1.5">
              {[5, 10, 15, 20, 25, 30].map(n => (
                <button key={n} onClick={() => onLimitChange(n)}
                  className="flex-1 text-xs py-1.5 rounded-lg transition-all font-medium"
                  style={{
                    background: limit === n ? "#0066CC" : "white",
                    border: `1px solid ${limit === n ? "#0066CC" : "#E2E8F0"}`,
                    color: limit === n ? "white" : "#64748B"
                  }}>
                  {n}
                </button>
              ))}
            </div>
          </div>

          <div style={{ borderTop: "1px solid #E2E8F0" }} />

          {/* Filtre expérience */}
          <div>
            <div className="flex items-center justify-between mb-3">
              <span className="text-xs font-medium" style={{ color: "#475569" }}>Filtre expérience</span>
              <span className="text-xs font-bold px-2 py-0.5 rounded"
                    style={{ background: "#EEF4FF", color: "#003B8E", fontFamily: "monospace" }}>
                {expMin} – {expMax} ans
              </span>
            </div>

            <div className="space-y-2.5 mb-3">
              <div className="flex items-center gap-3">
                <span className="text-xs w-8 text-right flex-shrink-0" style={{ color: "#94A3B8" }}>Min</span>
                <input type="range" min={0} max={20} step={1} value={expMin}
                  onChange={e => { const v = Number(e.target.value); if (v <= expMax) onExpMinChange(v) }}
                  className="flex-1 cursor-pointer" style={{ accentColor: "#0066CC" }} />
                <span className="text-xs font-mono w-5 text-center font-semibold" style={{ color: "#0066CC" }}>
                  {expMin}
                </span>
              </div>
              <div className="flex items-center gap-3">
                <span className="text-xs w-8 text-right flex-shrink-0" style={{ color: "#94A3B8" }}>Max</span>
                <input type="range" min={0} max={20} step={1} value={expMax}
                  onChange={e => { const v = Number(e.target.value); if (v >= expMin) onExpMaxChange(v) }}
                  className="flex-1 cursor-pointer" style={{ accentColor: "#0066CC" }} />
                <span className="text-xs font-mono w-5 text-center font-semibold" style={{ color: "#0066CC" }}>
                  {expMax}
                </span>
              </div>
            </div>

            {/* Shortcuts niveaux */}
            <div className="flex gap-1">
              {[
                { label: "Tous",     min: 0,  max: 20, color: "#64748B" },
                { label: "Junior",   min: 0,  max: 1,  color: "#94A3B8" },
                { label: "Medior",   min: 2,  max: 3,  color: "#F59E0B" },
                { label: "Confirmé", min: 4,  max: 5,  color: "#10B981" },
                { label: "Senior",   min: 6,  max: 9,  color: "#0066CC" },
                { label: "Expert",   min: 10, max: 20, color: "#7C3AED" },
              ].map(s => {
                const isActive = expMin === s.min && expMax === s.max
                return (
                  <button key={s.label}
                    onClick={() => { onExpMinChange(s.min); onExpMaxChange(s.max) }}
                    className="flex-1 py-1 rounded-md transition-all font-medium"
                    style={{
                      background: isActive ? s.color : "white",
                      border: `1px solid ${isActive ? s.color : "#E2E8F0"}`,
                      color: isActive ? "white" : "#64748B",
                      fontSize: "9px"
                    }}>
                    {s.label}
                  </button>
                )
              })}
            </div>
          </div>

          <div style={{ borderTop: "1px solid #E2E8F0" }} />

          {/* Mode de recherche */}
          <div>
            <p className="text-xs font-medium mb-2" style={{ color: "#475569" }}>Mode de recherche</p>
            <div className="grid grid-cols-3 gap-2">
              {[
                { value: "hybride", label: "Hybride", desc: "BM25 + Vecteur", icon: "🔀" },
                { value: "vecteur", label: "Vecteur", desc: "Sémantique",     icon: "🔵" },
                { value: "texte",   label: "Texte",   desc: "Mots-clés",      icon: "🔤" },
              ].map(m => (
                <button key={m.value} onClick={() => onModeChange(m.value)}
                  className="rounded-lg p-2.5 text-left transition-all"
                  style={{
                    background: mode === m.value ? "#EEF4FF" : "white",
                    border: `1px solid ${mode === m.value ? "#0066CC" : "#E2E8F0"}`,
                  }}>
                  <p className="mb-0.5" style={{ fontSize: "14px" }}>{m.icon}</p>
                  <p className="text-xs font-semibold" style={{ color: mode === m.value ? "#003B8E" : "#1A2B4B" }}>
                    {m.label}
                  </p>
                  <p style={{ fontSize: "9px", color: "#94A3B8" }}>{m.desc}</p>
                </button>
              ))}
            </div>
          </div>

        </div>
      )}
    </div>
  )
}
