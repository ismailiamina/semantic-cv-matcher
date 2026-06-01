# Archivage Logique

L'application utilise une logique d'archivage plutot qu'une suppression directe.

## Pourquoi Archiver ?

En contexte entreprise, supprimer physiquement un CV ou une offre peut poser probleme :

- perte de tracabilite ;
- erreurs utilisateur difficiles a corriger ;
- audit impossible ;
- donnees RH sensibles.

## Principe

Quand l'utilisateur clique sur **Archiver**, le backend marque l'objet comme archive.

```text
is_archived = true
archived_at = date ISO
delete_scope = archive
```

## APIs

```text
DELETE /api/candidates/{uuid}?confirm=true&scope=archive
DELETE /api/jobs/{uuid}?confirm=true&scope=archive
```

## UX

L'interface affiche clairement :

- bouton `Archiver` ;
- modal de confirmation ;
- toast de succes ou erreur.

## Fichier Frontend

```text
src/components/common/DeleteConfirmModal.tsx
```
