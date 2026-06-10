"use client"

import { FormEvent, useState } from "react"
import { useRouter } from "next/navigation"
import { ArrowRight, BarChart3, Lock, Mail, ShieldCheck, Sparkles } from "lucide-react"
import { GoDevLogo } from "@/components/GoDevLogo"
import { useAuth } from "@/lib/rbac"

export default function LoginPage() {
  const router = useRouter()
  const { login } = useAuth()
  const [email, setEmail] = useState("")
  const [password, setPassword] = useState("")
  const [error, setError] = useState("")
  const [loading, setLoading] = useState(false)

  const handleSubmit = async (event: FormEvent<HTMLFormElement>) => {
    event.preventDefault()
    setError("")
    setLoading(true)
    const ok = await login(email, password)
    setLoading(false)
    if (!ok) {
      setError("Email ou mot de passe incorrect.")
      return
    }
    router.replace("/candidates")
  }

  return (
    <main className="min-h-screen overflow-hidden" style={{ background: "#F3F6FA" }}>
      <div className="grid min-h-screen grid-cols-[1.08fr_0.92fr]">
        <section className="relative flex flex-col justify-between px-16 py-12" style={{ background: "#00245A", color: "white" }}>
          <div className="absolute inset-0 opacity-20" style={{
            backgroundImage: "linear-gradient(rgba(255,255,255,0.08) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.08) 1px, transparent 1px)",
            backgroundSize: "44px 44px",
          }} />

          <div className="relative">
            <GoDevLogo size={34} />

            <div className="mt-20 max-w-2xl">
              <div className="mb-6 inline-flex items-center gap-2 rounded-full px-3 py-1.5 text-xs font-semibold"
                   style={{ background: "rgba(255,255,255,0.12)", border: "1px solid rgba(255,255,255,0.18)" }}>
                <ShieldCheck className="h-3.5 w-3.5" />
                Plateforme RH intelligente
              </div>

              <h1 className="text-5xl font-bold leading-tight tracking-tight">
                CV Scanner IA
              </h1>
              <p className="mt-5 max-w-xl text-base leading-8" style={{ color: "rgba(255,255,255,0.76)" }}>
                Espace professionnel pour centraliser les talents, qualifier les offres et piloter le matching semantique avec des droits d&apos;acces adaptes a chaque profil interne.
              </p>
            </div>
          </div>

          <div className="relative grid grid-cols-3 gap-4">
            {[
              { icon: ShieldCheck, title: "Acces par role", text: "Permissions appliquees aux pages et actions sensibles." },
              { icon: Sparkles, title: "Matching IA", text: "Recherche hybride, reranking et explications metier." },
              { icon: BarChart3, title: "Pilotage RH", text: "Vue analytique du vivier candidats et des offres." },
            ].map(item => (
              <div key={item.title} className="rounded-2xl p-4"
                   style={{ background: "rgba(255,255,255,0.1)", border: "1px solid rgba(255,255,255,0.16)", backdropFilter: "blur(10px)" }}>
                <item.icon className="h-5 w-5" style={{ color: "#7DD3FC" }} />
                <p className="mt-3 text-sm font-semibold">{item.title}</p>
                <p className="mt-2 text-xs leading-5" style={{ color: "rgba(255,255,255,0.68)" }}>{item.text}</p>
              </div>
            ))}
          </div>
        </section>

        <section className="flex items-center justify-center px-10">
          <form
            onSubmit={handleSubmit}
            className="w-full max-w-[460px] rounded-2xl p-8"
            style={{ background: "rgba(255,255,255,0.95)", border: "1px solid #DDE6F2", boxShadow: "0 28px 80px rgba(15,23,42,0.12)" }}
          >
            <div className="mb-8">
              <p className="text-xs font-semibold uppercase tracking-[0.22em]" style={{ color: "#0066CC" }}>
                Connexion securisee
              </p>
              <h2 className="mt-3 text-3xl font-bold tracking-tight" style={{ color: "#10213F" }}>
                Acceder a l&apos;espace RH
              </h2>
              <p className="mt-3 text-sm leading-6" style={{ color: "#64748B" }}>
                Connectez-vous avec votre compte interne pour acceder aux modules autorises.
              </p>
            </div>

            <div className="space-y-5">
              <label className="block">
                <span className="text-xs font-semibold" style={{ color: "#475569" }}>Adresse email</span>
                <div className="mt-2 flex items-center gap-3 rounded-xl px-4 py-3.5 transition-all"
                     style={{ border: "1px solid #CBD5E1", background: "#F8FAFC" }}>
                  <Mail className="h-4 w-4 flex-shrink-0" style={{ color: "#94A3B8" }} />
                  <input
                    value={email}
                    onChange={event => setEmail(event.target.value)}
                    className="w-full bg-transparent text-sm outline-none"
                    style={{ color: "#1A2B4B" }}
                    placeholder="prenom.nom@gmail.com"
                    autoComplete="email"
                    type="email"
                    required
                  />
                </div>
              </label>

              <label className="block">
                <span className="text-xs font-semibold" style={{ color: "#475569" }}>Mot de passe</span>
                <div className="mt-2 flex items-center gap-3 rounded-xl px-4 py-3.5 transition-all"
                     style={{ border: "1px solid #CBD5E1", background: "#F8FAFC" }}>
                  <Lock className="h-4 w-4 flex-shrink-0" style={{ color: "#94A3B8" }} />
                  <input
                    value={password}
                    onChange={event => setPassword(event.target.value)}
                    type="password"
                    className="w-full bg-transparent text-sm outline-none"
                    style={{ color: "#1A2B4B" }}
                    placeholder="Votre mot de passe"
                    autoComplete="current-password"
                    required
                  />
                </div>
              </label>
            </div>

            {error && (
              <p className="mt-5 rounded-xl px-4 py-3 text-sm" style={{ background: "#FEF2F2", border: "1px solid #FECACA", color: "#B91C1C" }}>
                {error}
              </p>
            )}

            <button
              type="submit"
              disabled={loading}
              className="mt-7 flex w-full items-center justify-center gap-2 rounded-xl px-4 py-3.5 text-sm font-semibold transition-all"
              style={{ background: "#0066CC", color: "white", boxShadow: "0 12px 28px rgba(0,102,204,0.25)", opacity: loading ? 0.72 : 1 }}
            >
              {loading ? "Verification..." : "Se connecter"}
              <ArrowRight className="h-4 w-4" />
            </button>

            <p className="mt-5 text-center text-xs" style={{ color: "#94A3B8" }}>
              Acces reserve aux utilisateurs autorises Go & Dev.
            </p>
          </form>
        </section>
      </div>
    </main>
  )
}
