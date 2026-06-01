# Moteur De Recherche Intelligent

Le moteur de recherche intelligent compare les candidats et les offres selon plusieurs dimensions. Il est exposé par `API/Search_API.py` et implémenté dans les modules du dossier `weaviate_db/search`.

## Modules

| Fichier | Rôle |
| --- | --- |
| `weaviate_db/search/candidates_for_job.py` | Trouver les meilleurs candidats pour une offre |
| `weaviate_db/search/jobs_for_candidate.py` | Trouver les meilleures offres pour un candidat |
| `weaviate_db/search/keywords.py` | Recherche par compétences, langages, localisation, rôle, expérience, industrie |
| `weaviate_db/search/advanced_search.py` | Recherche multicritère |

## Modes De Recherche

### Texte

Le mode `texte` utilise BM25 sur les propriétés textuelles indexées.

Il est utile lorsque :

- les termes sont très explicites ;
- l'utilisateur cherche une compétence précise ;
- la disponibilité du service de vectorisation est limitée.

### Vecteur

Le mode `vecteur` utilise les vecteurs déjà stockés dans Weaviate.

Il est utile pour :

- récupérer des profils sémantiquement proches ;
- comparer des descriptions longues ;
- réduire la dépendance aux mots exacts.

### Hybride

Le mode `hybride` combine BM25 et vecteurs avec un paramètre `alpha`.

Il est le mode par défaut car il équilibre :

- précision lexicale ;
- similarité sémantique ;
- tolérance aux variations de vocabulaire.

## Matching Bidirectionnel

Le système prend en charge deux directions de recherche :

- recherche de candidats pour une offre ;
- recherche d'offres pour un candidat.

Cette bidirectionnalité correspond aux deux besoins RH principaux : pourvoir une mission et valoriser un profil disponible.

## Matching Offre Vers Candidats

Endpoint :

```text
GET /api/search/candidates-for-job/?job_uuid=<uuid>&mode=hybride&limit=10
```

Pipeline :

1. Récupération de l'offre par UUID.
2. Récupération des vecteurs nommés de l'offre.
3. Recherche dans la collection `Candidate`.
4. Calcul des scores par dimension.
5. Application de poids dynamiques selon le domaine détecté.
6. Filtres métiers éventuels.
7. Retour des meilleurs candidats.

Dimensions typiques :

- compétences techniques ;
- résumé ;
- langages de programmation ;
- secteur d'activité ;
- titre de poste vs rôles occupés ;
- description d'offre vs expérience professionnelle.

## Matching Candidat Vers Offres

Endpoint :

```text
GET /api/search/jobs-for-candidate/?candidate_uuid=<uuid>&mode=hybride&limit=10
```

Pipeline :

1. Récupération du candidat par UUID.
2. Récupération des vecteurs nommés du candidat.
3. Recherche dans la collection `Job`.
4. Calcul des scores par dimension.
5. Tri par score final.
6. Retour des offres les plus pertinentes.

## Structure D'Un Résultat

Un résultat de matching contient :

```json
{
  "uuid": "uuid",
  "score": 0.82,
  "search_method": "hybrid_search",
  "individual_scores": {
    "tech_skills": 0.91,
    "summary": 0.74
  },
  "properties": {}
}
```

L'interface Next.js exploite `score`, `individual_scores` et `properties` pour afficher les cartes de résultat et les détails.

## Recherche Hybride, Reranking Et Explication

Le reranking est une étape optionnelle appliquée après le premier matching. Il prend les candidats ou offres déjà retournés et les réordonne avec un modèle dédié.

Endpoint :

```text
POST /api/search/rerank/
```

Le reranking ne remplace pas la recherche Weaviate : il affine un top N déjà calculé. Les explications LLM peuvent ensuite synthétiser les points forts, les écarts et le verdict final d'un résultat.
