# Swagger Et API REST

Cette page présente l'exposition des services via API REST, en cohérence avec la partie du rapport consacrée à l'architecture FastAPI et à la documentation Swagger.

L'objectif de cette couche est de rendre les fonctionnalités du système interopérables, testables et réutilisables : consultation des données, upload, opérations CRUD, recherche intelligente, reranking, analyse LLM et workflows d'orchestration.

## Point D'Accès

Une fois le backend lancé, la documentation Swagger est disponible à l'adresse suivante :

```text
http://localhost:8005/docs
```

Swagger permet de tester les endpoints indépendamment de l'interface Next.js. Il est utilisé pour valider les paramètres attendus, les réponses JSON et le comportement des services avant leur exploitation côté interface.

## Familles D'Endpoints

| Famille | Endpoints principaux | Rôle |
| --- | --- | --- |
| Consultation | `GET /api/candidates/`, `GET /api/jobs/`, `GET /api/stats/` | Lecture des candidats, offres, détails par UUID et statistiques globales. |
| Upload | `POST /api/upload/candidate/file`, `POST /api/upload/job/file`, `POST /api/upload/from-url` | Ingestion de CV ou d'offres depuis un fichier ou une URL. |
| CRUD | `POST /api/candidates/`, `POST /api/jobs/`, `DELETE /api/candidates/{id}`, `DELETE /api/jobs/{id}` | Gestion du cycle de vie des candidats et des offres. |
| Recherche | `GET /api/search/candidates-for-job/`, `GET /api/search/jobs-for-candidate/`, `POST /api/search/advanced/` | Matching bidirectionnel et recherche multicritères. |
| Reranking | `POST /api/search/rerank/` | Reclassement des meilleurs résultats après une première recherche Weaviate. |
| Analyse LLM | `POST /api/llm/explain/`, `POST /api/llm/gap/`, `POST /api/llm/parse-query/` | Explication des résultats, analyse d'écarts et interprétation de requêtes libres. |
| Workflows | `POST /api/workflows/matching`, `POST /api/workflows/ingestion/file` | Orchestration LangGraph, contrôle qualité, détection des doublons et enrichissement des réponses. |

## Lecture Des Méthodes HTTP

Les méthodes HTTP permettent de comprendre rapidement le rôle d'un endpoint :

| Méthode | Usage dans le projet |
| --- | --- |
| `GET` | Consulter des données sans modifier la base : listes, détails, statistiques ou résultats de recherche. |
| `POST` | Envoyer des données ou déclencher un traitement : upload, insertion, recherche avancée, reranking, explication LLM. |
| `DELETE` | Archiver ou supprimer une ressource à partir de son identifiant unique. |

## Vue Fonctionnelle

```text
Interface Next.js / Swagger
        |
        v
API FastAPI
        |
        +--> List API      -> consultation candidats/offres/statistiques
        +--> Upload API    -> ingestion fichier ou URL
        +--> CRUD API      -> création, batch, archivage/suppression
        +--> Search API    -> matching, recherche avancée, reranking
        +--> LLM endpoints -> explication, gap analysis, parsing de requête
        +--> Workflow API  -> orchestration LangGraph
        |
        v
Weaviate + Services IA
```

## Apport Pour Le Projet

L'exposition REST apporte trois avantages majeurs :

- les services peuvent être testés depuis Swagger avant l'intégration frontend ;
- l'interface Next.js consomme des contrats API clairs et centralisés ;
- les modules restent séparés par responsabilité, ce qui facilite la maintenance et l'évolution du système.

!!! note "Alignement avec le rapport"
    Cette page correspond à la section du chapitre 4 intitulée **Exposition des services via API REST**. Elle complète les captures Swagger utilisées dans le rapport en expliquant les familles d'endpoints et leur rôle métier.
