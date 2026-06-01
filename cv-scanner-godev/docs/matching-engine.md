# Recherche Et Matching

Le moteur de matching compare les candidats et les offres selon plusieurs dimensions. Il est expose par `API/Search_API.py` et implemente dans les modules du dossier `weaviate_db/search`.

## Modules

| Fichier | Role |
| --- | --- |
| `weaviate_db/search/candidates_for_job.py` | Trouver les meilleurs candidats pour une offre |
| `weaviate_db/search/jobs_for_candidate.py` | Trouver les meilleures offres pour un candidat |
| `weaviate_db/search/keywords.py` | Recherche par competences, langages, localisation, role, experience, industrie |
| `weaviate_db/search/advanced_search.py` | Recherche multicritere |

## Modes De Recherche

### Texte

Le mode `texte` utilise BM25 sur les proprietes textuelles indexees.

Il est utile lorsque :

- les termes sont tres explicites ;
- l'utilisateur cherche une competence precise ;
- la disponibilite du service de vectorisation est limitee.

### Vecteur

Le mode `vecteur` utilise les vecteurs deja stockes dans Weaviate.

Il est utile pour :

- recuperer des profils semantiquement proches ;
- comparer des descriptions longues ;
- reduire la dependance aux mots exacts.

### Hybride

Le mode `hybride` combine BM25 et vecteurs avec un parametre `alpha`.

Il est le mode par defaut car il equilibre :

- precision lexicale ;
- similarite semantique ;
- tolerance aux variations de vocabulaire.

## Matching Offre Vers Candidats

Endpoint :

```text
GET /api/search/candidates-for-job/?job_uuid=<uuid>&mode=hybride&limit=10
```

Pipeline :

1. Recuperation de l'offre par UUID.
2. Recuperation des vecteurs nommes de l'offre.
3. Recherche dans la collection `Candidate`.
4. Calcul des scores par dimension.
5. Application de poids dynamiques selon le domaine detecte.
6. Filtres metiers eventuels.
7. Retour des meilleurs candidats.

Dimensions typiques :

- technical skills ;
- summary ;
- programming languages ;
- industry ;
- job title vs roles ;
- job description vs work experience.

## Matching Candidat Vers Offres

Endpoint :

```text
GET /api/search/jobs-for-candidate/?candidate_uuid=<uuid>&mode=hybride&limit=10
```

Pipeline :

1. Recuperation du candidat par UUID.
2. Recuperation des vecteurs nommes du candidat.
3. Recherche dans la collection `Job`.
4. Calcul des scores par dimension.
5. Tri par score final.
6. Retour des offres les plus pertinentes.

## Structure D'Un Resultat

Un resultat de matching contient :

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

L'interface Next.js exploite `score`, `individual_scores` et `properties` pour afficher les cartes de resultat et les details.

## Reranking

Le reranking est une etape optionnelle appliquee apres le premier matching. Il prend les candidats ou offres deja retournes et les reordonne avec un modele dedie.

Endpoint :

```text
POST /api/search/rerank/
```

Le reranking ne remplace pas la recherche Weaviate : il affine un top N deja calcule.
