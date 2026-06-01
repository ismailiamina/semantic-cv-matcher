# Fichiers Techniques

Cette page sert de cartographie rapide des fichiers importants du projet.

## Backend FastAPI

| Fichier | Role |
| --- | --- |
| `API/main_api.py` | Point d'entree FastAPI, CORS, routers |
| `API/CRUD_API.py` | Endpoints d'ajout, batch, archivage/suppression |
| `API/List_API.py` | Endpoints de lecture, details, statistiques |
| `API/Search_API.py` | Endpoints matching, recherche, reranking et LLM |
| `API/Upload_API.py` | Upload fichiers, extraction texte, Mistral, validation |
| `API/llm_utils.py` | Fonctions d'explication et analyse LLM |

## Weaviate

| Fichier | Role |
| --- | --- |
| `weaviate_db/setup_weaviate.py` | Creation schema, collections, tenants, named vectors |
| `weaviate_db/CRUD.py` | Transformation et insertion candidats/offres |
| `weaviate_db/List.py` | Lecture, listes compactes, stats |
| `weaviate_db/insert_data.py` | Insertion depuis fichiers de donnees |

## Recherche Et Matching

| Fichier | Role |
| --- | --- |
| `weaviate_db/search/candidates_for_job.py` | Matching offre vers candidats |
| `weaviate_db/search/jobs_for_candidate.py` | Matching candidat vers offres |
| `weaviate_db/search/keywords.py` | Recherches par criteres simples |
| `weaviate_db/search/advanced_search.py` | Recherche avancee multicritere |

## Configuration

| Fichier | Role |
| --- | --- |
| `config/settings.py` | Ports, collections, tenants, chemins data, cles API |
| `.env` | Secrets locaux et variables sensibles |

## Interface Next.js

| Fichier | Role |
| --- | --- |
| `cv-scanner-godev/src/lib/api.ts` | Client Axios centralise |
| `cv-scanner-godev/src/app/analytics/page.tsx` | Dashboard analytique |
| `cv-scanner-godev/src/app/candidates/page.tsx` | Gestion candidats |
| `cv-scanner-godev/src/app/offers/page.tsx` | Gestion offres |
| `cv-scanner-godev/src/app/match/page.tsx` | Matching offre vers candidats |
| `cv-scanner-godev/src/app/jobs/page.tsx` | Matching candidat vers offres |
| `cv-scanner-godev/src/app/search/page.tsx` | Recherche libre |

## Composants Next.js

| Fichier | Role |
| --- | --- |
| `src/components/common/UploadPanel.tsx` | Upload candidat/offre |
| `src/components/common/SearchableSelect.tsx` | Selection avec recherche |
| `src/components/common/FilterPanel.tsx` | Parametres de matching |
| `src/components/common/DeleteConfirmModal.tsx` | Confirmation archivage |
| `src/components/common/Toast.tsx` | Notifications |
| `src/components/match/ScoreRing.tsx` | Affichage visuel des scores |

## Endpoints Consommes Par L'Interface

```text
GET    /api/stats/
GET    /api/candidates/names/
GET    /api/candidates/{uuid}
GET    /api/jobs/titles/
GET    /api/jobs/{uuid}
GET    /api/search/candidates-for-job/
GET    /api/search/jobs-for-candidate/
POST   /api/search/rerank/
POST   /api/llm/explain/
POST   /api/llm/gap/
POST   /api/llm/parse-query/
POST   /api/search/advanced/
POST   /api/upload/candidate/file
POST   /api/upload/job/file
POST   /api/upload/from-url
DELETE /api/candidates/{uuid}
DELETE /api/jobs/{uuid}
```
