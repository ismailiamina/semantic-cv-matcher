# CV-Scanner-IA — Semantic Talent Matching

> Système de matching sémantique entre profils IT et offres d'emploi  
> Projet PFE — Go & Dev · Mars–Juillet 2026

---

## Architecture

```
LinkedIn (profils)          LinkedIn (offres)
       ↓                          ↓
linkedin_scraper.py       jobs_scraper.py
       ↓                          ↓
       └──────────┬───────────────┘
                  ↓
           insert_data.py
           setup_weaviate.py
                  ↓
            Weaviate DB
           (768 dimensions)
                  ↓
      candidates_for_job.py
                  ↓
        app.py  ←── Upload_job.py
```

---

## Stack Technique

| Composant | Technologie |
|---|---|
| Base vectorielle | Weaviate 1.28.2 |
| Embedding | Ollama + nomic-embed-text (768d) |
| Extraction structurée | Mistral Large (LangChain) |
| Re-ranking | Mistral Small (HTTP direct) |
| Scraping | Selenium + Chrome |
| Interface | Streamlit |
| Orchestration | Docker Compose |

---

## Installation

### Prérequis
- Docker Desktop
- Python 3.10+
- Ollama avec `nomic-embed-text`
- Clé API Mistral

### 1 — Cloner le repo
```bash
git clone https://github.com/ismailiamina/semantic-cv-matcher.git
cd semantic-cv-matcher
```

### 2 — Installer les dépendances
```bash
pip install -r requirements.txt
```

### 3 — Configurer les variables d'environnement
Créer un fichier `.env` à la racine :
```env
MISTRAL_API_KEY=votre_cle_mistral
LINKEDIN_EMAIL=votre_email_linkedin
LINKEDIN_PASSWORD=votre_mot_de_passe_linkedin
```

### 4 — Lancer Ollama et télécharger le modèle
```bash
ollama pull nomic-embed-text
```

### 5 — Lancer Weaviate
```bash
docker-compose up -d
```

### 6 — Initialiser la base vectorielle
```bash
cd weaviate
python setup_weaviate.py
python insert_data.py
```

### 7 — Lancer l'interface
```bash
cd ..
streamlit run app.py
```

---

## Fonctionnalités

### Matching candidats → offre
- Sélection d'une offre dans l'interface
- Recherche hybride Weaviate (BM25 + vecteurs)
- Re-ranking Mistral Small
- Score détaillé par dimension (Skills, Langages, Résumé, Industrie, Rôle)
- Filtres : séniorité, localisation, expérience
- Sliders de pondération

### Upload d'offres
- Fichier `.docx` / `.pdf` / `.txt`
- URL LinkedIn (API jobs-guest)
- Structuration automatique par Mistral Large
- Insertion et vectorisation en temps réel

---

## Structure du projet

```
CV_JOB/
├── app.py                          # Interface Streamlit
├── Upload_job.py                   # Module upload offres
├── docker-compose.yaml             # Weaviate
├── requirements.txt
├── data/
│   ├── Linkedin_scraper.py         # Scraping profils Go & Dev
│   ├── jobs_scraper.py             # Scraping offres LinkedIn
│   ├── enrich_profiles.py          # Enrichissement profils
│   └── data/real_data/             # Données réelles
│       ├── extracted_cvs_real_enriched.json   # 53 candidats
│       └── extracted_jobs_real.json            # 13 offres
└── weaviate/
    ├── setup_weaviate.py           # Schéma Weaviate (8+6 named vectors)
    ├── insert_data.py              # Insertion données
    └── search/
        └── candidates_for_job.py  # Pipeline matching
```

---

## Pipeline de matching

```
1. Weaviate Hybrid Search (α=0.7)
   → Top-10 candidats par similarité sémantique

2. Jaccard Matching
   → % exact des skills/langages requis couverts

3. Mistral Small Re-ranking
   → Compréhension des équivalences métier IT
   → "outillage QA" = Selenium/Cypress
   → "DevOps" = CI/CD + Docker + Kubernetes

4. Score final
   Score = (Weaviate×20% + Mistral×80%) × pénalité_séniorité
```

---

## Données

- **53 candidats** : profils consultants IT Go & Dev (LinkedIn)
- **13 offres** : Go & Dev (6) · Cleva (4) · Centreon (3)

---

## Auteur

**Amina Ismaili** — Stagiaire PFE AI Engineer @ Go & Dev  
ENSAM Meknès · Génie Industriel, IA & Data Science  
Superviseur : Yasser Jebbari (AI Expert, Go & Dev)
