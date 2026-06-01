# Installation Complete

Cette page explique comment lancer le projet complet en local : backend FastAPI, Weaviate et interface Next.js.

## Prerequis

- Python avec les dependances du backend installees ;
- Node.js et npm pour l'interface Next.js ;
- Docker ou une instance Weaviate disponible ;
- une cle `MISTRAL_API_KEY` pour l'extraction et les endpoints LLM ;
- une cle `VOYAGEAI_APIKEY` pour la vectorisation et le reranking ;
- un fichier `.env` a la racine du projet.

## Variables D'Environnement

Le fichier de configuration principal est :

```text
config/settings.py
```

Il lit les cles depuis :

```text
.env
```

Variables importantes :

```text
MISTRAL_API_KEY=...
VOYAGEAI_APIKEY=...
```

La configuration Weaviate attend par defaut :

```text
WEAVIATE_HOST=localhost
WEAVIATE_PORT=8087
WEAVIATE_GRPC_PORT=50051
```

## Lancer Weaviate

Le backend attend une instance Weaviate accessible en local. Si le projet utilise un `docker-compose.yml`, lancer le service depuis la racine du projet :

```powershell
docker compose up -d
```

Verifier ensuite que Weaviate repond sur :

```text
http://localhost:8087/v1/.well-known/ready
```

## Initialiser Le Schema

Le schema est defini dans :

```text
weaviate_db/setup_weaviate.py
```

!!! warning "Operation sensible"
    Le setup peut supprimer et recreer les collections. Il ne doit pas etre lance sur une base contenant des donnees importantes sans sauvegarde ou accord explicite.

Commande :

```powershell
python weaviate_db/setup_weaviate.py
```

## Lancer Le Backend FastAPI

Depuis la racine du projet `CV_JOB` :

```powershell
uvicorn API.main_api:app --host 0.0.0.0 --port 8005 --reload
```

Swagger est disponible sur :

```text
http://localhost:8005/docs
```

Verification rapide :

```text
GET /health
GET /api/stats/
GET /api/candidates/names/
GET /api/jobs/titles/
```

## Lancer L'Interface Next.js

Depuis le dossier frontend :

```powershell
cd cv-scanner-godev
npm run dev
```

L'interface est disponible sur :

```text
http://localhost:3000
```

## Lancer La Documentation

Depuis le dossier `cv-scanner-godev` :

```powershell
mkdocs serve
```

La documentation locale est disponible sur :

```text
http://127.0.0.1:8000
```

## Verification Fonctionnelle

1. Ouvrir Swagger et tester `GET /api/stats/`.
2. Ouvrir l'interface Next.js.
3. Verifier que les listes candidats et offres se chargent.
4. Uploader un CV test.
5. Verifier que le candidat apparait dans `/api/candidates/names/`.
6. Uploader une offre test.
7. Lancer un matching en mode `hybride`.
