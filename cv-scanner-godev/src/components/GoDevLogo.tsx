export function GoDevLogo({ size = 32 }: { size?: number }) {
  return (
    <svg
      width={size * 3.2}
      height={size}
      viewBox="0 0 128 40"
      fill="none"
    >
      {/* Barres chart //  */}
      <rect x="0"  y="20" width="6" height="20" rx="1.5" fill="white" opacity="0.7"/>
      <rect x="9"  y="10" width="6" height="30" rx="1.5" fill="white" opacity="0.85"/>
      <rect x="18" y="2"  width="6" height="38" rx="1.5" fill="white"/>
      {/* Texte Go&Dev */}
      <text x="32" y="28"
        fontFamily="Inter, sans-serif"
        fontSize="18"
        fontWeight="600"
        fill="white"
        letterSpacing="-0.3"
      >
        Go
        <tspan fill="rgba(255,255,255,0.6)">&amp;</tspan>
        Dev
      </text>
    </svg>
  )
}