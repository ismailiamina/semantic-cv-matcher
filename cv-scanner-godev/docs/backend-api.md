# FastAPI Et Endpoints

Le backend FastAPI est le point d'entree applicatif. Il expose les routes REST utilisees par Swagger et par l'interface Next.js.

## Point D'Entree

Fichier principal :

```text
API/main_api.py
```

Ce fichier cree l'application FastAPI, configure CORS et enregistre les routers suivants :

| Router | Prefixe | Role |
| --- | --- | --- |
| `CRUD_API.py` | `/api` | insertion, batch, archivage ou suppression |
| `List_API.py` | `/api` | listes, details, statistiques |
| `Search_API.py` | `/api` | matching, recherche, reranking, LLM |
| `Upload_API.py` | `/api` | upload fichier ou URL |

## Endpoints De Lecture

| Endpoint | Methode | Description |
| --- | --- | --- |
| `/api/candidates/` | GET | Liste complete des candidats |
| `/api/candidates/names/` | GET | Liste compacte nom + UUID + experience + entreprise |
| `/api/candidates/{candidate_id}` | GET | Detail complet d'un candidat |
| `/api/jobs/` | GET | Liste complete des offres |
| `/api/jobs/titles/` | GET | Liste compacte titre + UUID + entreprise + niveau |
| `/api/jobs/{job_id}` | GET | Detail complet d'une offre |
| `/api/stats/` | GET | KPIs globaux du dashboard |

## Endpoints CRUD

| Endpoint | Methode | Description |
| --- | --- | --- |
| `/api/candidates/` | POST | Ajout candidat depuis JSON |
| `/api/candidates/batch/` | POST | Insertion batch candidats |
| `/api/candidates/{candidate_id}` | DELETE | Archivage par defaut ou suppression hard |
| `/api/jobs/` | POST | Ajout offre depuis JSON |
| `/api/jobs/batch/` | POST | Insertion batch offres |
| `/api/jobs/{job_id}` | DELETE | Archivage par defaut ou suppression hard |

La suppression utilise les parametres :

```text
confirm=true
scope=archive | hard
```

Par defaut, le comportement fonctionnel attendu est l'archivage.

## Endpoints Upload

| Endpoint | Methode | Description |
| --- | --- | --- |
| `/api/upload/candidate/file` | POST | Upload CV fichier PDF/DOCX/TXT |
| `/api/upload/job/file` | POST | Upload offre fichier PDF/DOCX/TXT |
| `/api/upload/from-url` | POST | Extraction depuis URL publique |

Ces endpoints reutilisent `add_candidate_endpoint` et `add_job_endpoint`. Cela evite de maintenir deux logiques d'insertion differentes.

## Endpoints Matching

| Endpoint | Methode | Description |
| --- | --- | --- |
| `/api/search/candidates-for-job/` | GET | Retourne les meilleurs candidats pour une offre |
| `/api/search/jobs-for-candidate/` | GET | Retourne les meilleures offres pour un candidat |
| `/api/search/advanced/` | POST | Recherche multicritere |
| `/api/search/rerank/` | POST | Reranking VoyageAI |
| `/api/llm/explain/` | POST | Explication LLM d'un match |
| `/api/llm/gap/` | POST | Analyse d'ecart candidat/offre |

Les modes de matching sont :

```text
texte | vecteur | hybride
```

## Consommation Depuis Next.js

Tous les appels frontend sont centralises dans :

```text
cv-scanner-godev/src/lib/api.ts
```

Ce fichier configure Axios avec :

```text
baseURL: http://localhost:8005
timeout: 60000
```

Il expose des fonctions applicatives comme `candidateNames`, `jobTitles`, `candidatesForJob`, `jobsForCandidate`, `uploadCandidateFile`, `uploadJobFile`, `deleteCandidate` et `deleteJob`.
