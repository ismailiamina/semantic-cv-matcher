# Démonstration de l'application

Cette page regroupe les démonstrations Loom du fonctionnement de CV Scanner IA. Les vidéos présentent le parcours utilisateur, les fonctionnalités de matching et les interactions entre l'interface Next.js, les endpoints FastAPI et la base vectorielle Weaviate.

## Vidéos Loom

<div class="loom-grid">
  <div class="loom-card">
    <div class="loom-embed">
      <iframe src="https://www.loom.com/embed/af618e622fc34a30a8d3d61dde22048a" allowfullscreen></iframe>
    </div>
    <h3>Démonstration 1 - Interface et matching</h3>
    <p>Présentation du tableau de bord, de la consultation des candidats et offres, puis du matching avec scores et explications.</p>
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
    <h3>Démonstration 2 - Ingestion et workflows</h3>
    <p>Présentation de l'upload, du parsing, de l'indexation dans Weaviate, des endpoints Swagger et des workflows d'orchestration.</p>
    <div class="loom-actions">
      <a class="demo-button" href="https://www.loom.com/share/31faea56b77b4587a75eef9e4f688fc3" target="_blank" rel="noopener">
        Ouvrir sur Loom
      </a>
    </div>
  </div>
</div>

## Scénarios présentés

Les démonstrations couvrent les scénarios principaux de l'application :

1. consultation du dashboard et des indicateurs globaux ;
2. recherche et consultation des candidats ;
3. recherche et consultation des offres d'emploi ;
4. ajout d'un CV ou d'une offre depuis l'interface ;
5. déclenchement d'un matching offre vers candidats ;
6. déclenchement d'un matching candidat vers offres ;
7. lecture des scores globaux et des dimensions de matching ;
8. utilisation du reranking ;
9. génération d'explications LLM ;
10. validation des endpoints et workflows via Swagger.

## Points à observer

Pendant les vidéos, les éléments importants sont :

- la fluidité de navigation entre les pages ;
- la centralisation des données candidat et offre ;
- l'appel aux endpoints FastAPI depuis l'interface ;
- la disponibilité des données après upload ;
- la lisibilité des scores de matching ;
- la présence des UUID pour le diagnostic technique ;
- l'apport du reranking et des explications LLM ;
- la traçabilité des traitements via les workflows.
