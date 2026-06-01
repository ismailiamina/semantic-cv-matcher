# Vue D'ensemble Du Projet

CV Scanner IA est une solution complète pour structurer, rechercher et comparer des profils candidats avec des offres d'emploi IT. Le projet couvre toute la chaîne : ingestion de documents, extraction IA, validation, stockage sémantique, matching bidirectionnel, orchestration des workflows et exploitation dans une application web.

## Enjeu Métier

Les équipes RH manipulent souvent des CV et des offres sous forme de fichiers ou de textes peu structurés. Sans automatisation, la comparaison entre un besoin et une base de candidats demande beaucoup de lecture manuelle.

CV Scanner IA automatise cette analyse en transformant les documents en données exploitables, puis en calculant des correspondances entre candidats et offres selon plusieurs dimensions.

## Capacités Principales

- ajout de candidats et d'offres depuis fichiers ;
- extraction structurée avec Mistral ;
- contrôle des champs obligatoires avant insertion ;
- détection des doublons avant insertion ;
- stockage dans Weaviate avec vecteurs nommés ;
- matching offre vers candidats ;
- matching candidat vers offres ;
- recherche avancée et recherche par critères ;
- reranking VoyageAI ;
- analyse LLM d'un match ou d'un écart ;
- workflows LangGraph pour l'ingestion et le matching ;
- dashboard de suivi ;
- archivage fonctionnel ;
- application web Next.js pour l'exploitation quotidienne.

## Composants Principaux

| Couche | Rôle | Fichiers principaux |
| --- | --- | --- |
| FastAPI | Exposition des endpoints HTTP | `API/main_api.py`, `API/CRUD_API.py`, `API/List_API.py`, `API/Search_API.py`, `API/Upload_API.py` |
| Extraction IA | Lecture fichier, prompt, extraction JSON, validation | `API/Upload_API.py` |
| Weaviate | Stockage objet + vecteurs nommés | `weaviate_db/setup_weaviate.py`, `weaviate_db/CRUD.py`, `weaviate_db/List.py` |
| Matching | Recherche texte, vectorielle et hybride | `weaviate_db/search/candidates_for_job.py`, `weaviate_db/search/jobs_for_candidate.py` |
| Workflows LangGraph | Orchestration ingestion, matching, reranking, explication | `langgraph_workflows/ingestion_workflow.py`, `langgraph_workflows/matching_workflow.py`, `API/Workflow_API.py` |
| Application web | Interface Next.js de gestion et matching | `cv-scanner-godev/src/app`, `cv-scanner-godev/src/components`, `cv-scanner-godev/src/lib/api.ts` |

## Flux Général

![Architecture globale](assets/images/architecture.svg)

1. L'utilisateur ajoute ou consulte une donnée depuis l'application web.
2. L'application appelle les endpoints FastAPI.
3. Le backend extrait, valide ou recherche les données selon l'action demandée.
4. Weaviate stocke les objets et exécute les recherches texte, vectorielles ou hybrides.
5. Les services IA interviennent pour l'extraction, le reranking ou l'explication.
6. LangGraph orchestre les traitements complexes lorsqu'un suivi étape par étape est nécessaire.
7. Les résultats sont retournés à l'application web avec scores, détails et UUID.

## Principes Techniques

- Le backend est la source de vérité pour les validations.
- L'upload réutilise les endpoints CRUD afin d'éviter la duplication.
- Weaviate conserve les objets et les vecteurs nommés.
- Les modes `texte`, `vecteur` et `hybride` donnent plusieurs stratégies de recherche.
- Les workflows LangGraph renforcent la traçabilité des traitements d'ingestion et de matching.
- L'archivage est privilégié pour conserver la traçabilité.
- L'application web Next.js consomme l'API et présente les workflows métier.
