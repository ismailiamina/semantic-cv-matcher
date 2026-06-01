# Matching Offre -> Candidats

Cette page repond a la question :

> Quels candidats correspondent le mieux a cette offre ?

## Workflow

1. Selectionner une offre avec recherche.
2. Choisir un mode de recherche :
   - `vecteur`
   - `texte`
   - `hybride`
3. Definir la limite de resultats.
4. Lancer le matching.
5. Consulter les scores.
6. Appliquer le reranking.
7. Demander une explication LLM.

## API Utilisee

```text
GET /api/search/candidates-for-job/?job_uuid={uuid}&mode={mode}&limit={limit}
```

## Reranking

```text
POST /api/search/rerank/
```

## Explication LLM

```text
POST /api/llm/explain/
```

## Fichier Principal

```text
src/app/match/page.tsx
```

## Lecture Des Scores

Le score principal indique la proximite entre l'offre et le candidat. Quand le reranking est applique, l'interface affiche un score reranke plus adapte a la requete.

!!! note "Modes de recherche"
    Le mode `vecteur` exploite les vecteurs deja stockes. Le mode `hybride` combine BM25 et vecteurs via Weaviate.
