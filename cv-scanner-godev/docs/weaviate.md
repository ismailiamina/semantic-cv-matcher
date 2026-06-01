# Weaviate

Weaviate est utilise comme base de donnees vectorielle et comme stockage principal des objets candidats et offres.

## Configuration

La configuration applicative est centralisee dans :

```text
config/settings.py
```

Parametres principaux :

| Parametre | Valeur |
| --- | --- |
| Host | `localhost` |
| Port HTTP | `8087` |
| Port gRPC | `50051` |
| Collection candidats | `Candidate` |
| Collection offres | `Job` |
| Tenant candidats | `cv` |
| Tenant offres | `job` |

## Initialisation Du Schema

Le schema Weaviate est defini dans :

```text
weaviate_db/setup_weaviate.py
```

Ce script cree les collections, les proprietes et les vecteurs nommes.

!!! warning "Attention"
    Le script de setup supprime et recree les collections existantes. Il doit etre utilise uniquement dans un environnement controle ou lorsque la reinitialisation des donnees est volontaire.

## Collection Candidate

La collection `Candidate` contient les profils candidats.

Vecteurs nommes principaux :

| Vecteur | Source |
| --- | --- |
| `tech_skills_vector` | `technical_skills` |
| `location_vector` | `location` |
| `summary_vector` | `summary` |
| `prog_langs_vector` | `programming_languages` |
| `industry_vector` | `industry_primary_industries` |
| `roles_held_vector` | `roles_held` |
| `work_experience_vector` | `work_experience` |
| `projects_vector` | `projects` |

Champs fonctionnels importants :

- `full_name`
- `email`
- `location`
- `years_of_experience`
- `roles_held`
- `programming_languages`
- `technical_skills`
- `summary`
- `company_source`
- `is_archived`, selon l'evolution du schema et des donnees archivees

## Collection Job

La collection `Job` contient les offres d'emploi.

Vecteurs nommes principaux :

| Vecteur | Source |
| --- | --- |
| `job_tech_skills_vector` | `technical_skills` |
| `job_summary_vector` | `summary` |
| `job_prog_langs_vector` | `programming_languages` |
| `job_industry_vector` | `industry` |
| `job_title_vector` | `title` |
| `job_description_vector` | `job_description` |

Champs fonctionnels importants :

- `title`
- `company`
- `industry`
- `location`
- `experience_level`
- `years_of_experience_required`
- `technical_skills`
- `programming_languages`
- `summary`
- `job_description`
- `is_archived`, selon l'evolution du schema et des donnees archivees

## Role Des Vecteurs

Les vecteurs permettent de comparer les dimensions importantes entre une offre et un candidat :

- competences techniques ;
- langages de programmation ;
- resume ;
- industrie ;
- titre de poste et roles ;
- description de poste et experience professionnelle.

La recherche vectorielle utilise les vecteurs deja stockes dans Weaviate. La recherche hybride combine ces vecteurs avec BM25.
