# Application Web Next.js

L'application web du projet est developpee avec Next.js. Elle constitue l'espace de travail des utilisateurs pour consulter la base, ajouter des donnees, lancer les matchings et analyser les resultats.

Le code de l'application se trouve dans :

```text
cv-scanner-godev/
```

Cette interface consomme l'API FastAPI sans reimplementer la logique backend.

## Structure Frontend

| Dossier | Role |
| --- | --- |
| `src/app` | Pages principales de l'application |
| `src/components` | Composants reutilisables |
| `src/components/common` | Upload, modal, toast, skeleton, select recherche |
| `src/components/layout` | Sidebar et topbar |
| `src/components/match` | Composants de scoring |
| `src/lib/api.ts` | Client Axios centralise |

## Pages

| Page | Fichier | Role |
| --- | --- | --- |
| Analytics | `src/app/analytics/page.tsx` | KPIs et graphiques |
| Candidats | `src/app/candidates/page.tsx` | Liste, recherche, details, upload, archivage |
| Offres | `src/app/jobs/page.tsx` ou `src/app/offers/page.tsx` selon navigation | Liste, recherche, details, upload, archivage |
| Offre -> Candidats | `src/app/match/page.tsx` | Matching depuis une offre |
| Recherche libre | `src/app/search/page.tsx` | Recherche multicritere |

## Client API

Tous les appels HTTP passent par :

```text
src/lib/api.ts
```

Fonctions principales :

- `stats()`
- `candidateNames()`
- `jobTitles()`
- `candidate(uuid)`
- `job(uuid)`
- `candidatesForJob(job_uuid, mode, limit)`
- `jobsForCandidate(candidate_uuid, mode, limit)`
- `uploadCandidateFile(file)`
- `uploadJobFile(file)`
- `deleteCandidate(uuid, confirm, scope)`
- `deleteJob(uuid, confirm, scope)`

## Experience Utilisateur

L'interface est concue pour un usage operationnel :

- recherche rapide dans les listes ;
- affichage UUID pour diagnostic ;
- copie UUID ;
- upload direct depuis les pages metier ;
- feedback visuel avec toasts ;
- confirmation avant archivage ;
- affichage detaille des competences, langues, resume et parcours.

## Frontend Et Backend

Le frontend reste volontairement mince :

- il ne calcule pas les scores ;
- il ne fait pas la vectorisation ;
- il ne valide pas seul les extractions ;
- il declenche les actions et affiche les retours API.

Cette separation rend l'application plus facile a integrer, tester et maintenir.
