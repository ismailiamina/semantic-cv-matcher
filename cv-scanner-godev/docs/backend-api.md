# FastAPI Et Endpoints

Le backend FastAPI est le point d'entrée applicatif. Il expose les routes REST utilisées par Swagger et par l'interface Next.js.

## Point D'Entrée

Fichier principal :

```text
API/main_api.py
```

Ce fichier crée l'application FastAPI, configure CORS et enregistre les routers suivants :

| Router | Préfixe | Rôle |
| --- | --- | --- |
| `CRUD_API.py` | `/api` | insertion, batch, archivage ou suppression |
| `List_API.py` | `/api` | listes, détails, statistiques |
| `Search_API.py` | `/api` | matching, recherche, reranking, LLM |
| `Upload_API.py` | `/api` | upload fichier ou URL |
| `Workflow_API.py` | `/api` | workflows LangGraph d'ingestion et de matching |

!!! tip "Documentation Swagger"
    Pour une vue fonctionnelle des familles d'endpoints et de leur rôle, voir la page [Swagger Et API REST](swagger-api.md).

## Endpoints De Lecture

| Endpoint | Méthode | Description |
| --- | --- | --- |
| `/api/candidates/` | GET | Liste complète des candidats |
| `/api/candidates/names/` | GET | Liste compacte nom + UUID + expérience + entreprise |
| `/api/candidates/{candidate_id}` | GET | Détail complet d'un candidat |
| `/api/jobs/` | GET | Liste complète des offres |
| `/api/jobs/titles/` | GET | Liste compacte titre + UUID + entreprise + niveau |
| `/api/jobs/{job_id}` | GET | Détail complet d'une offre |
| `/api/stats/` | GET | KPIs globaux du dashboard |

## Endpoints CRUD

| Endpoint | Méthode | Description |
| --- | --- | --- |
| `/api/candidates/` | POST | Ajout candidat depuis JSON |
| `/api/candidates/batch/` | POST | Insertion batch candidats |
| `/api/candidates/{candidate_id}` | DELETE | Archivage par défaut ou suppression hard |
| `/api/jobs/` | POST | Ajout offre depuis JSON |
| `/api/jobs/batch/` | POST | Insertion batch offres |
| `/api/jobs/{job_id}` | DELETE | Archivage par défaut ou suppression hard |

La suppression utilise les paramètres :

```text
confirm=true
scope=archive | hard
```

Par défaut, le comportement fonctionnel attendu est l'archivage.

## Endpoints Upload

| Endpoint | Méthode | Description |
| --- | --- | --- |
| `/api/upload/candidate/file` | POST | Upload CV fichier PDF/DOCX/TXT |
| `/api/upload/job/file` | POST | Upload offre fichier PDF/DOCX/TXT |
| `/api/upload/from-url` | POST | Extraction depuis URL publique |

Ces endpoints réutilisent `add_candidate_endpoint` et `add_job_endpoint`. Cela évite de maintenir deux logiques d'insertion différentes.

## Endpoints Matching Et LLM

| Endpoint | Méthode | Description |
| --- | --- | --- |
| `/api/search/candidates-for-job/` | GET | Retourne les meilleurs candidats pour une offre |
| `/api/search/jobs-for-candidate/` | GET | Retourne les meilleures offres pour un candidat |
| `/api/search/advanced/` | POST | Recherche multicritère |
| `/api/search/rerank/` | POST | Reranking VoyageAI |
| `/api/llm/explain/` | POST | Explication LLM d'un match |
| `/api/llm/gap/` | POST | Analyse d'écart candidat/offre |
| `/api/llm/parse-query/` | POST | Interprétation d'une requête libre |

Les modes de matching sont :

```text
texte | vecteur | hybride
```

## Endpoints Workflows

| Endpoint | Méthode | Description |
| --- | --- | --- |
| `/api/workflows/ingestion/file` | POST | Ingestion contrôlée depuis fichier |
| `/api/workflows/ingestion/url` | POST | Ingestion contrôlée depuis URL |
| `/api/workflows/matching` | POST | Matching orchestré avec options de reranking et d'explication |

Ces endpoints exposent les workflows LangGraph décrits dans la page [Workflows LangGraph](langgraph-workflows.md).

## Consommation Depuis Next.js

Tous les appels frontend sont centralisés dans :

```text
cv-scanner-godev/src/lib/api.ts
```

Ce fichier configure Axios avec :

```text
baseURL: http://localhost:8005
timeout: 60000
```

Il expose des fonctions applicatives comme `candidateNames`, `jobTitles`, `candidatesForJob`, `jobsForCandidate`, `uploadCandidateFile`, `uploadJobFile`, `deleteCandidate` et `deleteJob`.
