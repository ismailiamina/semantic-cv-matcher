"use client"

import { useEffect, useMemo, useRef, useState } from "react"
import { Check, Search, X } from "lucide-react"

interface SearchableSelectProps<T> {
  items: T[]
  value: string
  onChange: (value: string) => void
  placeholder: string
  emptyText?: string
  getValue: (item: T) => string
  getLabel: (item: T) => string
  getMeta?: (item: T) => string
  getSearchText?: (item: T) => string
}

export function SearchableSelect<T>({
  items,
  value,
  onChange,
  placeholder,
  emptyText = "Aucun resultat",
  getValue,
  getLabel,
  getMeta,
  getSearchText,
}: SearchableSelectProps<T>) {
  const rootRef = useRef<HTMLDivElement>(null)
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState("")

  const selectedItem = useMemo(
    () => items.find(item => getValue(item) === value),
    [items, value, getValue]
  )
  const selectedLabel = selectedItem ? getLabel(selectedItem) : ""
  const inputValue = open ? query : selectedLabel

  useEffect(() => {
    if (!open) return
    const handlePointerDown = (event: PointerEvent) => {
      if (!rootRef.current?.contains(event.target as Node)) setOpen(false)
    }
    document.addEventListener("pointerdown", handlePointerDown)
    return () => document.removeEventListener("pointerdown", handlePointerDown)
  }, [open])

  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase()
    if (!q || (selectedItem && query === selectedLabel)) {
      return items.slice(0, 60)
    }
    return items
      .filter(item => {
        const text = (getSearchText?.(item) ?? `${getLabel(item)} ${getMeta?.(item) ?? ""}`)
          .toLowerCase()
        return text.includes(q)
      })
      .slice(0, 60)
  }, [items, query, selectedItem, selectedLabel, getLabel, getMeta, getSearchText])

  const handleSelect = (item: T) => {
    onChange(getValue(item))
    setQuery("")
    setOpen(false)
  }

  const handleClear = () => {
    onChange("")
    setQuery("")
    setOpen(false)
  }

  return (
    <div ref={rootRef} className="relative">
      <Search
        className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 pointer-events-none"
        style={{ color: "#94A3B8" }}
      />
      <input
        value={inputValue}
        onChange={e => {
          setQuery(e.target.value)
          setOpen(true)
          if (value) onChange("")
        }}
        onFocus={e => {
          setQuery(selectedLabel)
          setOpen(true)
          e.currentTarget.select()
        }}
        placeholder={placeholder}
        className="w-full text-sm rounded-lg pl-10 pr-10 py-2.5 focus:outline-none"
        style={{
          background: "white",
          border: `1px solid ${open ? "#0066CC" : "#CBD5E1"}`,
          color: inputValue ? "#1A2B4B" : "#94A3B8",
        }}
      />
      {(inputValue || value) && (
        <button
          type="button"
          onClick={handleClear}
          className="absolute right-2.5 top-1/2 -translate-y-1/2 p-1 rounded-md transition-all"
          style={{ color: "#94A3B8" }}
        >
          <X className="w-3.5 h-3.5" />
        </button>
      )}

      {open && (
        <div
          className="absolute z-30 mt-1 w-full rounded-lg overflow-hidden"
          style={{
            background: "white",
            border: "1px solid #CBD5E1",
            boxShadow: "0 12px 24px rgba(15,23,42,0.12)",
          }}
        >
          <div className="max-h-72 overflow-y-auto py-1">
            {filtered.length === 0 ? (
              <div className="px-3 py-3 text-xs" style={{ color: "#94A3B8" }}>
                {emptyText}
              </div>
            ) : (
              filtered.map(item => {
                const itemValue = getValue(item)
                const active = itemValue === value
                return (
                  <button
                    key={itemValue}
                    type="button"
                    onClick={() => handleSelect(item)}
                    className="w-full text-left px-3 py-2.5 flex items-center gap-2 transition-all"
                    style={{ background: active ? "#EEF4FF" : "white" }}
                  >
                    <div className="flex-1 min-w-0">
                      <p className="text-xs font-semibold truncate" style={{ color: "#1A2B4B" }}>
                        {getLabel(item)}
                      </p>
                      {getMeta && (
                        <p className="text-xs truncate mt-0.5" style={{ color: "#64748B", fontSize: "10px" }}>
                          {getMeta(item)}
                        </p>
                      )}
                    </div>
                    {active && <Check className="w-4 h-4 flex-shrink-0" style={{ color: "#0066CC" }} />}
                  </button>
                )
              })
            )}
          </div>
        </div>
      )}
    </div>
  )
}
