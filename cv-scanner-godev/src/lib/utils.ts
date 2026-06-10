import { clsx, type ClassValue } from "clsx"

export function cn(...inputs: ClassValue[]) {
  return clsx(inputs)
}

export function yearsToLevel(y: number) {
  if (y >= 10) return "Expert"
  if (y >= 6)  return "Senior"
  if (y >= 4)  return "Confirmé"
  if (y >= 2)  return "Medior"
  return "Junior"
}

export const levelColors: Record<string, string> = {
  Expert:    "#C084FC",
  Senior:    "#60A5FA",
  Confirmé:  "#34D399",
  Medior:    "#FBBF24",
  Junior:    "#94A3B8",
}

export function scoreColor(s: number) {
  if (s >= 0.65) return "#34D399"
  if (s >= 0.40) return "#FBBF24"
  return "#F87171"
}

export function initials(name: string) {
  return name.split(" ").map(n => n[0]).join("").slice(0, 2).toUpperCase()
}
export function renderLLMText(text: string): string {
  if (!text) return ""
  let html = text
  // Liens [texte](url)
  html = html.replace(
    /\[([^\]]+)\]\((https?:\/\/[^\)]+)\)/g,
    '<a href="$2" target="_blank" rel="noopener" style="color:#0066CC;text-decoration:underline;font-weight:500;">$1</a>'
  )
  // Gras **texte**
  html = html.replace(/\*\*([^*]+)\*\*/g, '<strong style="color:#1A2B4B;font-weight:600;">$1</strong>')
  // Sauts de ligne
  html = html.replace(/\n\n/g, '<br/><br/>')
  html = html.replace(/\n/g, '<br/>')
  // Listes numérotées — 1. texte
  html = html.replace(
    /(\d+)\.\s+(.+?)(?=<br\/>|$)/g,
    '<div style="display:flex;gap:8px;margin:4px 0;"><span style="font-weight:700;color:#0066CC;min-width:18px;">$1.</span><span>$2</span></div>'
  )
  // Listes à tirets
  html = html.replace(
    /^-\s+(.+?)(?=<br\/>|$)/gm,
    '<div style="display:flex;gap:8px;margin:2px 0;padding-left:8px;"><span style="color:#0066CC;">•</span><span>$1</span></div>'
  )
  return html
}