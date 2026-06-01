# Workflows LangGraph

LangGraph est utilisé comme couche d'orchestration intelligente. Il ne remplace pas les endpoints FastAPI classiques, mais organise les traitements complexes sous forme de workflows explicites, traçables et contrôlables.

Cette approche est cohérente avec le rapport, qui présente LangGraph comme un moyen de renforcer la traçabilité, la qualité des traitements et l'explicabilité des résultats.

## Pourquoi Une Couche D'Orchestration ?

Les traitements d'ingestion et de matching ne sont pas de simples appels unitaires. Ils combinent plusieurs étapes :

- extraction de texte ;
- parsing avec Mistral ;
- validation et normalisation ;
- détection des doublons ;
- insertion dans Weaviate ;
- recherche hybride ;
- reranking ;
- explication LLM ;
- formatage d'une réponse exploitable.

LangGraph permet de représenter ces étapes sous forme de nœuds. Chaque nœud réalise une responsabilité précise, ce qui facilite le diagnostic, la reprise et l'évolution du pipeline.

## Workflow D'Ingestion Contrôlée

Le workflow d'ingestion formalise le traitement d'un CV ou d'une offre avant insertion dans la base vectorielle.

```text
route_input_type
  -> extract_file_text / extract_url_text
  -> parse_with_mistral
  -> evaluate_extraction_quality
  -> retry_parse_with_mistral
  -> validate_data
  -> normalize_payload
  -> detect_duplicates
  -> decide_insertion
  -> insert_into_weaviate
  -> format_response
```

## Rôle Des Étapes

| Étape | Rôle |
| --- | --- |
| Routage | Identifier le type d'entrée : fichier, URL, candidat ou offre. |
| Extraction | Convertir le contenu source en texte brut exploitable. |
| Parsing Mistral | Transformer le texte en JSON structuré selon le schéma cible. |
| Contrôle qualité | Vérifier la complétude des champs essentiels. |
| Normalisation | Harmoniser les listes, champs vides, niveaux d'expérience et propriétés attendues. |
| Détection des doublons | Éviter l'insertion répétée d'un même profil ou d'une même offre. |
| Décision d'insertion | Autoriser, bloquer ou signaler un élément nécessitant une revue. |
| Insertion Weaviate | Stocker l'objet dans la collection `Candidate` ou `Job`. |
| Réponse finale | Retourner le statut, les avertissements, les scores qualité et les nœuds exécutés. |

## Workflow De Matching

Le workflow de matching orchestre les différents scénarios de recherche :

```text
route_matching_type
  -> run_candidates_for_job / run_jobs_for_candidate
  -> check_results_quality
  -> optional_rerank
  -> optional_explain
  -> format_results
```

## Scénarios Pris En Charge

| Scénario | Description |
| --- | --- |
| Candidats pour une offre | Identifier les profils les plus pertinents pour une offre donnée. |
| Offres pour un candidat | Identifier les opportunités les plus adaptées à un profil candidat. |
| Reranking optionnel | Réordonner les meilleurs résultats selon une lecture plus contextuelle. |
| Explication LLM | Ajouter une justification textuelle aux résultats les plus importants. |

## Métadonnées De Traçabilité

Les workflows peuvent retourner des informations utiles au diagnostic :

```json
{
  "status": "success",
  "decision": "inserted",
  "quality_score": 0.92,
  "executed_nodes": [
    "parse_with_mistral",
    "validate_data",
    "detect_duplicates",
    "insert_into_weaviate"
  ],
  "warnings": []
}
```

Ces métadonnées facilitent le suivi des traitements et rendent le comportement du système plus explicable.

## Valeur Ajoutée

LangGraph apporte une valeur technique et métier :

- **traçabilité** : les étapes exécutées sont identifiables ;
- **contrôle qualité** : l'insertion peut être bloquée si les données sont insuffisantes ;
- **robustesse** : les erreurs peuvent être isolées dans des nœuds dédiés ;
- **modularité** : les étapes peuvent évoluer sans réécrire tout le pipeline ;
- **explicabilité** : les résultats peuvent être enrichis par des justifications LLM.

!!! note "Alignement avec le rapport"
    Cette page correspond à la section du chapitre 4 intitulée **Orchestration intelligente avec LangGraph**. Elle reprend les deux workflows principaux : ingestion contrôlée et matching enrichi.
