# Demonstration de la solution

Cette page presente les deux videos de demonstration de CV Scanner IA. Elles illustrent le fonctionnement de la solution dans des scenarios proches d'une utilisation reelle par une equipe RH : gestion des donnees, ingestion, matching intelligent, analyse des resultats et pilotage global.

## Videos Loom

<div class="loom-grid">
  <div class="loom-card">
    <div class="loom-embed">
      <iframe src="https://www.loom.com/embed/af618e622fc34a30a8d3d61dde22048a" allowfullscreen></iframe>
    </div>
    <h3>Video 1 - Gestion des donnees RH</h3>
    <p>Demonstration du parcours de gestion des candidats et des offres : authentification, consultation, upload, detection de doublons et archivage.</p>
    <div class="loom-actions">
      <a class="demo-button" href="https://www.loom.com/share/af618e622fc34a30a8d3d61dde22048a" target="_blank" rel="noopener">
        Ouvrir sur Loom
      </a>
    </div>
  </div>

  <div class="loom-card">
    <div class="loom-embed">
      <iframe src="https://www.loom.com/embed/31faea56b77b4587a75eef9e4f688fc3" allowfullscreen></iframe>
    </div>
    <h3>Video 2 - Matching intelligent et pilotage</h3>
    <p>Demonstration des fonctionnalites avancees : matching, analyse des scores, reranking, analyses LLM, recherche libre, dashboard et historique.</p>
    <div class="loom-actions">
      <a class="demo-button" href="https://www.loom.com/share/31faea56b77b4587a75eef9e4f688fc3" target="_blank" rel="noopener">
        Ouvrir sur Loom
      </a>
    </div>
  </div>
</div>

## Video 1 - Gestion des donnees RH

Cette premiere demonstration presente les fonctionnalites liees a la gestion et a l'alimentation de la base RH :

- authentification et acces securise a l'application ;
- navigation dans l'interface Next.js ;
- gestion des candidats ;
- consultation d'un profil candidat ;
- upload d'un candidat par fichier ou URL ;
- detection des doublons avant insertion ;
- gestion des offres d'emploi ;
- upload d'une offre ;
- archivage des candidats ou des offres.

## Video 2 - Matching intelligent et pilotage

Cette seconde demonstration met en avant les fonctionnalites de recherche intelligente, d'aide a la decision et de pilotage :

- matching entre une offre et les candidats les plus pertinents ;
- analyse des scores de correspondance ;
- analyses generees par LLM ;
- reranking des resultats ;
- recherche libre ;
- dashboard global ;
- gestion du profil utilisateur ;
- roles et permissions ;
- historique des actions.

## Architecture fonctionnelle resumee

```text
Interface Next.js
    |
    v
API FastAPI
    |
    v
Modules CRUD, Upload, Search et Matching
    |
    v
Base vectorielle Weaviate
    |
    v
Embeddings VoyageAI + extraction Mistral
```

Cette architecture montre le chemin principal suivi par les donnees : l'utilisateur interagit avec l'interface, les traitements sont exposes par FastAPI, les modules metier executent l'ingestion ou le matching, puis les donnees structurees et vectorisees sont stockees et interrogees dans Weaviate.
