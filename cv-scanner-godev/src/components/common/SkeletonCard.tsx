export function SkeletonCard() {
  return (
    <div className="rounded-xl p-3 animate-pulse"
         style={{ background: "white", border: "1px solid #E2E8F0" }}>
      <div className="flex items-center gap-2.5 mb-3">
        <div className="rounded" style={{ background: "#F1F5F9", width: "16px", height: "12px" }} />
        <div className="rounded-full" style={{ background: "#F1F5F9", width: "32px", height: "32px", flexShrink: 0 }} />
        <div className="flex-1 space-y-1.5">
          <div className="rounded" style={{ background: "#F1F5F9", height: "10px", width: "55%" }} />
          <div className="rounded" style={{ background: "#F1F5F9", height: "8px", width: "35%" }} />
        </div>
        <div className="rounded-full" style={{ background: "#F1F5F9", width: "44px", height: "44px", flexShrink: 0 }} />
      </div>
      <div className="grid grid-cols-2 gap-2 mb-3">
        {[1, 2, 3, 4].map(i => (
          <div key={i} className="flex items-center gap-1.5">
            <div className="rounded" style={{ background: "#F1F5F9", width: "50px", height: "8px", flexShrink: 0 }} />
            <div className="flex-1 rounded-full" style={{ background: "#F1F5F9", height: "3px" }} />
          </div>
        ))}
      </div>
      <div className="flex gap-1 mb-2">
        {[60, 45, 55].map((w, i) => (
          <div key={i} className="rounded" style={{ background: "#F1F5F9", height: "18px", width: `${w}px` }} />
        ))}
      </div>
      <div className="rounded-lg" style={{ background: "#F1F5F9", height: "28px" }} />
    </div>
  )
}