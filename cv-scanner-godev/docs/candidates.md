# Gestion Candidats

La page Candidats permet de consulter et gerer les profils CV indexes.

## Workflow

1. Rechercher un candidat par nom, source ou experience.
2. Selectionner un candidat.
3. Consulter ses informations detaillees.
4. Copier son UUID pour Swagger ou Weaviate.
5. Uploader un nouveau CV.
6. Archiver un profil si necessaire.

## Donnees Affichees

- nom complet ;
- UUID ;
- experience ;
- niveau estime ;
- localisation ;
- resume ;
- competences techniques ;
- langages ;
- source / entreprise ;
- formation.

## APIs Utilisees

```text
GET    /api/candidates/names/
GET    /api/candidates/{uuid}
POST   /api/upload/candidate/file
POST   /api/upload/from-url
DELETE /api/candidates/{uuid}?confirm=true&scope=archive
```

## Fichiers Principaux

```text
src/app/candidates/page.tsx
src/components/common/UploadPanel.tsx
src/components/common/DeleteConfirmModal.tsx
```

!!! tip "Bonne pratique"
    La page affiche l'UUID directement, ce qui facilite la verification dans Swagger et Weaviate.
