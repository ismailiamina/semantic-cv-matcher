# CV Scanner IA

CV Scanner IA est une plateforme de gestion et de matching intelligent entre candidats et offres d'emploi IT. Elle automatise l'analyse des CV et des offres, structure les informations avec l'IA, stocke les profils dans une base vectorielle, puis fournit une application web Next.js pour exploiter les donnees au quotidien.

La solution repond a un besoin simple : aider les equipes RH et techniques a identifier plus rapidement les bons candidats pour une offre, ou les meilleures opportunites pour un candidat, avec des resultats consultables, explicables et traçables.

## Vision Produit

Le projet s'organise autour d'un pipeline complet :

- ingestion de CV et d'offres depuis des fichiers ;
- extraction des informations importantes avec Mistral ;
- validation backend des champs critiques ;
- insertion dans Weaviate ;
- vectorisation des dimensions metier ;
- recherche texte, vectorielle et hybride ;
- scoring, reranking et analyse LLM ;
- exploitation dans une interface web Next.js.

Cette approche evite de limiter l'application a une simple liste de CV. Le systeme devient un outil d'aide a la decision, capable de comparer les profils et les besoins d'une offre sur plusieurs dimensions : competences, langages, experience, role, resume, industrie et trajectoire.

## Architecture Applicative

![Architecture](assets/images/architecture.svg)

```text
Fichiers CV / Offres
        |
        v
API FastAPI
        |
        +--> Extraction texte
        +--> Extraction IA Mistral
        +--> Validation metier
        +--> CRUD et archivage
        +--> Recherche et matching
        |
        v
Weaviate
        |
        +--> Collection Candidate
        +--> Collection Job
        +--> Vecteurs VoyageAI
        |
        v
Application web Next.js
```

FastAPI orchestre les traitements, Weaviate conserve les donnees et les vecteurs, les services IA enrichissent l'extraction et l'analyse, et l'application Next.js expose les fonctionnalites dans une interface exploitable par les utilisateurs.

## Chaine De Traitement

### Traitement D'Un CV

```text
CV PDF/DOCX/TXT
  -> extraction du texte
  -> extraction JSON par Mistral
  -> controle du nom candidat
  -> mapping vers CandidatePayload
  -> insertion dans la collection Candidate
  -> vectorisation automatique
  -> disponibilite pour le matching
```

Le backend empeche l'insertion d'un candidat si le nom reste invalide apres extraction et fallback. Cette validation protege la qualite de la base et evite les profils inutilisables.

### Traitement D'Une Offre

```text
Offre PDF/DOCX/TXT
  -> extraction du texte
  -> extraction JSON par Mistral
  -> controle du titre de l'offre
  -> mapping vers JobPayload
  -> insertion dans la collection Job
  -> vectorisation automatique
  -> disponibilite pour le matching
```

Une offre sans titre valide est rejetee avant insertion. Le systeme conserve ainsi des objets exploitables dans les listes, les recherches et les tableaux de bord.

## Modules Fonctionnels

| Module | Role |
| --- | --- |
| Upload | Ajouter un candidat ou une offre depuis un fichier |
| Extraction IA | Transformer un texte brut en donnees structurees |
| CRUD | Inserer, consulter et archiver les objets |
| List | Alimenter les listes, details et statistiques |
| Matching | Comparer candidats et offres en texte, vecteur ou hybride |
| Reranking | Reordonner les resultats avec VoyageAI |
| LLM | Expliquer un match ou analyser les ecarts |
| Dashboard | Suivre les volumes et repartitions de la base |
| Interface Next.js | Centraliser l'exploitation metier dans une application web |

## Experience Utilisateur

L'application web Next.js fournit l'espace de travail principal :

- tableau de bord analytique ;
- gestion des candidats ;
- gestion des offres ;
- upload de CV et d'offres ;
- recherche rapide dans les listes ;
- matching offre vers candidats ;
- matching candidat vers offres ;
- affichage des scores et details ;
- copie des UUID pour diagnostic ;
- archivage des elements.

L'interface arrive en derniere couche de l'architecture : elle ne remplace pas le backend, elle le rend utilisable. Les calculs de matching, la vectorisation, les validations et les appels IA restent centralises cote API.

## Organisation De La Documentation

La documentation suit la logique du produit :

| Section | Contenu |
| --- | --- |
| Projet | Presentation generale, architecture et installation |
| Backend et API | Endpoints, schemas, upload, recherche, LLM |
| Base vectorielle | Collections Weaviate, tenants, named vectors |
| Interface Next.js | Pages, composants, workflows utilisateur |
| Exploitation | demonstration, publication, depannage |

Cette progression permet de comprendre d'abord le systeme, puis les services techniques, puis l'application web qui les exploite.
