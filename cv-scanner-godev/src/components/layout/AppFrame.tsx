"use client"

import { useEffect } from "react"
import { usePathname, useRouter } from "next/navigation"
import { Sidebar } from "@/components/layout/Sidebar"
import { useAuth } from "@/lib/rbac"

export function AppFrame({ children }: { children: React.ReactNode }) {
  const pathname = usePathname()
  const router = useRouter()
  const { user, hydrated } = useAuth()
  const isLoginPage = pathname === "/login"

  useEffect(() => {
    if (!hydrated) return
    if (!user && !isLoginPage) router.replace("/login")
    if (user && isLoginPage) router.replace("/candidates")
  }, [user, hydrated, isLoginPage, router])

  if (!hydrated) {
    return (
      <div className="flex h-screen items-center justify-center" style={{ background: "#F0F4F8", color: "#64748B" }}>
        <p className="text-sm">Initialisation de la session...</p>
      </div>
    )
  }

  if (isLoginPage) return <>{children}</>

  if (!user) {
    return (
      <div className="flex h-screen items-center justify-center" style={{ background: "#F0F4F8", color: "#64748B" }}>
        <p className="text-sm">Redirection vers la connexion...</p>
      </div>
    )
  }

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar />
      <main className="flex-1 overflow-hidden flex flex-col">
        {children}
      </main>
    </div>
  )
}
