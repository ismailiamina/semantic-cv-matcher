import type { Metadata } from "next"
import "./globals.css"
import { Providers } from "@/lib/providers"
import { AppFrame } from "@/components/layout/AppFrame"

export const metadata: Metadata = {
  title: "CV-Scanner-IA | Go & Dev",
  description: "Matching sémantique CV / Offres",
}

export default function Layout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="fr">
      <body>
        <Providers>
          <AppFrame>{children}</AppFrame>
        </Providers>
      </body>
    </html>
  )
}
