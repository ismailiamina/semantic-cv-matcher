# Fichiers Techniques

Cette page sert de cartographie rapide des fichiers importants du projet.

## Backend FastAPI

| Fichier | Rôle |
| --- | --- |
| `API/main_api.py` | Point d'entrée FastAPI, CORS, routers |
| `API/CRUD_API.py` | Endpoints d'ajout, batch, archivage/suppression |
| `API/List_API.py` | Endpoints de lecture, détails, statistiques |
| `API/Search_API.py` | Endpoints matching, recherche, reranking et LLM |
| `API/Upload_API.py` | Upload fichiers, extraction texte, Mistral, validation |
| `API/Workflow_API.py` | Endpoints des workflows LangGraph |
| `API/llm_utils.py` | Fonctions d'explication et analyse LLM |

## Weaviate

| Fichier | Rôle |
| --- | --- |
| `weaviate_db/setup_weaviate.py` | Création schéma, collections, tenants, named vectors |
| `weaviate_db/CRUD.py` | Transformation et insertion candidats/offres |
| `weaviate_db/List.py` | Lecture, listes compactes, stats |
| `weaviate_db/insert_data.py` | Insertion depuis fichiers de données |

## Recherche Et Matching

| Fichier | Rôle |
| --- | --- |
| `weaviate_db/search/candidates_for_job.py` | Matching offre vers candidats |
| `weaviate_db/search/jobs_for_candidate.py` | Matching candidat vers offres |
| `weaviate_db/search/keywords.py` | Recherches par critères simples |
| `weaviate_db/search/advanced_search.py` | Recherche avancée multicritère |

## Workflows LangGraph

| Fichier | Rôle |
| --- | --- |
| `langgraph_workflows/ingestion_workflow.py` | Orchestration de l'ingestion contrôlée, validation, normalisation et détection des doublons |
| `langgraph_workflows/matching_workflow.py` | Orchestration du matching, reranking optionnel et explication LLM |

## Configuration

| Fichier | Rôle |
| --- | --- |
| `config/settings.py` | Ports, collections, tenants, chemins data, clés API |
| `.env` | Secrets locaux et variables sensibles |

## Interface Next.js

| Fichier | Rôle |
| --- | --- |
| `cv-scanner-godev/src/lib/api.ts` | Client Axios centralisé |
| `cv-scanner-godev/src/app/analytics/page.tsx` | Dashboard analytique |
| `cv-scanner-godev/src/app/candidates/page.tsx` | Gestion candidats |
| `cv-scanner-godev/src/app/offers/page.tsx` | Gestion offres |
| `cv-scanner-godev/src/app/match/page.tsx` | Matching offre vers candidats |
| `cv-scanner-godev/src/app/jobs/page.tsx` | Matching candidat vers offres |
| `cv-scanner-godev/src/app/search/page.tsx` | Recherche libre |

## Composants Next.js

| Fichier | Rôle |
| --- | --- |
| `src/components/common/UploadPanel.tsx` | Upload candidat/offre |
| `src/components/common/SearchableSelect.tsx` | Sélection avec recherche |
| `src/components/common/FilterPanel.tsx` | Paramètres de matching |
| `src/components/common/DeleteConfirmModal.tsx` | Confirmation archivage |
| `src/components/common/Toast.tsx` | Notifications |
| `src/components/match/ScoreRing.tsx` | Affichage visuel des scores |

## Endpoints Consommés Par L'Interface

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
POST   /api/workflows/ingestion/file
POST   /api/workflows/ingestion/url
POST   /api/workflows/matching
DELETE /api/candidates/{uuid}
DELETE /api/jobs/{uuid}
```
