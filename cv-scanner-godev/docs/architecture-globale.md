# Architecture Globale

L'architecture est organisee autour de quatre blocs : interface Next.js, API FastAPI, moteur de recherche Weaviate et services IA externes.

```text
Utilisateur RH
    |
    v
Interface Next.js
    |
    v
API FastAPI
    |
    +--> Upload / Extraction Mistral
    +--> CRUD / List
    +--> Matching / Reranking / LLM
    |
    v
Weaviate
    |
    +--> Collection Candidate
    +--> Collection Job
    +--> Named vectors VoyageAI
```

## Responsabilites

### Interface Next.js

L'interface fournit les ecrans de travail :

- tableau de bord analytique ;
- liste et detail candidats ;
- liste et detail offres ;
- upload candidat et offre ;
- matching offre vers candidats ;
- matching candidat vers offres ;
- recherche libre et filtres ;
- archivage fonctionnel.

### API FastAPI

FastAPI expose les endpoints consommes par l'interface et par Swagger. Le fichier d'entree est `API/main_api.py`.

Les routers sont separes par responsabilite :

- `CRUD_API.py` pour l'insertion et l'archivage ;
- `List_API.py` pour la lecture et les statistiques ;
- `Search_API.py` pour la recherche, le matching, le reranking et les endpoints LLM ;
- `Upload_API.py` pour l'upload fichier ou URL et l'extraction IA.

### Weaviate

Weaviate stocke les objets candidats et offres, ainsi que les vecteurs nommes. Les collections principales sont :

- `Candidate`, tenant `cv` ;
- `Job`, tenant `job`.

Les objets sont recherches par UUID pour les details et par vecteurs/BM25 pour le matching.

### Services IA

Deux services IA interviennent dans le pipeline :

- Mistral : extraction structuree depuis du texte brut et analyse LLM ;
- VoyageAI : vectorisation via Weaviate et reranking optionnel.

## Flux Candidat

```text
CV PDF/DOCX/TXT
  -> extraction texte
  -> extraction JSON par Mistral
  -> validation full_name
  -> mapping CandidatePayload
  -> add_candidate_endpoint
  -> insertion Weaviate Candidate
  -> vectorisation champs candidats
  -> disponible pour matching
```

## Flux Offre

```text
Offre PDF/DOCX/TXT
  -> extraction texte
  -> extraction JSON par Mistral
  -> validation title
  -> mapping JobPayload
  -> add_job_endpoint
  -> insertion Weaviate Job
  -> vectorisation champs offres
  -> disponible pour matching
```

## Decisions D'Architecture

- Le backend reste la source de verite pour les validations.
- Le frontend ne reconstruit pas la logique de matching.
- L'upload reutilise les fonctions CRUD existantes.
- Les donnees archivees sont exclues des listes fonctionnelles selon la logique de lecture.
- Les UUID sont visibles dans l'interface pour faciliter le diagnostic dans Weaviate.
