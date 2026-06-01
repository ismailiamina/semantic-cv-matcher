# Matching Candidat -> Offres

Cette page repond a la question :

> Quelles offres correspondent le mieux a ce candidat ?

## Workflow

1. Selectionner un candidat.
2. Choisir un mode de recherche.
3. Lancer la recherche d'offres.
4. Lire les scores de correspondance.
5. Appliquer le reranking si necessaire.
6. Demander une analyse des ecarts LLM.

## API Utilisee

```text
GET /api/search/jobs-for-candidate/?candidate_uuid={uuid}&mode={mode}&limit={limit}
```

## Gap Analysis LLM

```text
POST /api/llm/gap/
```

## Fichier Principal

```text
src/app/jobs/page.tsx
```

## Interet Metier

Ce workflow aide un recruteur ou un commercial RH a identifier rapidement les opportunites compatibles avec un profil donne.
