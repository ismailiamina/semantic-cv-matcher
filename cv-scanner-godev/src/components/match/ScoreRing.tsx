import { scoreColor } from "@/lib/utils"

export function ScoreRing({
  score, size = 44, stroke = 4
}: {
  score: number; size?: number; stroke?: number
}) {
  const r = (size - stroke * 2) / 2
  const c = 2 * Math.PI * r
  const offset = c - score * c
  const color = scoreColor(score)

  return (
    <div className="relative flex-shrink-0" style={{ width: size, height: size }}>
      <svg
        width={size} height={size}
        viewBox={`0 0 ${size} ${size}`}
        style={{ transform: "rotate(-90deg)" }}
      >
        <circle
          cx={size/2} cy={size/2} r={r}
          fill="none"
          stroke="rgba(255,255,255,0.06)"
          strokeWidth={stroke}
        />
        <circle
          cx={size/2} cy={size/2} r={r}
          fill="none"
          stroke={color}
          strokeWidth={stroke}
          strokeDasharray={c}
          strokeDashoffset={offset}
          strokeLinecap="round"
        />
      </svg>
      <div className="absolute inset-0 flex items-center justify-center">
        <span
          className="font-mono font-semibold"
          style={{ fontSize: size < 50 ? "10px" : "14px", color }}
        >
          {Math.round(score * 100)}%
        </span>
      </div>
    </div>
  )
}