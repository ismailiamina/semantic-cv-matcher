# Architecture Globale

L'architecture est organisée autour de cinq blocs : interface Next.js, API FastAPI, workflows LangGraph, moteur de recherche Weaviate et services IA externes.

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
    +--> Workflow API
    |
    v
LangGraph
    |
    +--> Ingestion contrôlée
    +--> Matching enrichi
    |
    v
Weaviate
    |
    +--> Collection Candidate
    +--> Collection Job
    +--> Named vectors VoyageAI
```

## Responsabilités

### Interface Next.js

L'interface fournit les écrans de travail :

- tableau de bord analytique ;
- liste et détail candidats ;
- liste et détail offres ;
- upload candidat et offre ;
- matching offre vers candidats ;
- matching candidat vers offres ;
- recherche libre et filtres ;
- archivage fonctionnel.

### API FastAPI

FastAPI expose les endpoints consommés par l'interface et par Swagger. Le fichier d'entrée est `API/main_api.py`.

Les routers sont séparés par responsabilité :

- `CRUD_API.py` pour l'insertion et l'archivage ;
- `List_API.py` pour la lecture et les statistiques ;
- `Search_API.py` pour la recherche, le matching, le reranking et les endpoints LLM ;
- `Upload_API.py` pour l'upload fichier ou URL et l'extraction IA ;
- `Workflow_API.py` pour exposer les workflows d'ingestion et de matching.

### LangGraph

LangGraph orchestre les traitements qui nécessitent plusieurs étapes contrôlées. Deux workflows principaux sont utilisés :

- ingestion contrôlée : extraction, parsing, validation, normalisation, détection des doublons et insertion ;
- matching enrichi : recherche, contrôle des résultats, reranking optionnel et explication LLM.

### Weaviate

Weaviate stocke les objets candidats et offres, ainsi que les vecteurs nommés. Les collections principales sont :

- `Candidate`, tenant `cv` ;
- `Job`, tenant `job`.

Les objets sont recherchés par UUID pour les détails et par vecteurs/BM25 pour le matching.

### Services IA

Deux services IA interviennent dans le pipeline :

- Mistral : extraction structurée depuis du texte brut et analyse LLM ;
- VoyageAI : vectorisation via Weaviate et reranking optionnel.

## Flux Candidat

```text
CV PDF/DOCX/TXT
  -> extraction texte
  -> extraction JSON par Mistral
  -> validation full_name
  -> mapping CandidatePayload
  -> détection des doublons
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
  -> détection des doublons
  -> insertion Weaviate Job
  -> vectorisation champs offres
  -> disponible pour matching
```

## Décisions D'Architecture

- Le backend reste la source de vérité pour les validations.
- Le frontend ne reconstruit pas la logique de matching.
- L'upload réutilise les fonctions CRUD existantes.
- Les données archivées sont exclues des listes fonctionnelles selon la logique de lecture.
- Les UUID sont visibles dans l'interface pour faciliter le diagnostic dans Weaviate.
- Les workflows LangGraph rendent les traitements plus traçables et plus faciles à diagnostiquer.
