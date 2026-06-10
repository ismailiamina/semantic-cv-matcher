"""
weaviate/search/advanced_search.py
====================================
Recherche avancee multi-criteres ponderee.
Combine localisation + skills + langages + role + experience + industrie.

Architecture :
  - Filtres durs  : localisation, experience (appliques en post-traitement)
  - Scores souples: skills, langages, role (BM25 normalises 0-1)
  - Score final   : moyenne ponderee des dimensions actives
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from typing import List, Dict, Any, Optional
from weaviate.classes.query import MetadataQuery
from weaviate_db.search.candidates_for_job import (
    MatchResult,
    normalize_score,
    is_active_properties,
)
from weaviate_db.search.keywords import (
    search_candidates_by_tech_skills,
    search_candidates_by_prog_langs,
    search_candidates_by_location,
    search_candidates_by_roles,
    search_candidates_by_industry,
)
from config.settings import *


# ─── UTILITAIRES ──────────────────────────────────────────────────────

def _cid(obj) -> str:
    return str(getattr(obj, "id", getattr(obj, "uuid", ""))) or ""


def _props(obj) -> dict:
    return obj.properties if hasattr(obj, "properties") else (obj or {})


def _is_active(obj) -> bool:
    return is_active_properties(_props(obj))


def _normalize_list(results: list) -> Dict[str, float]:
    """Normalise les scores BM25 entre 0 et 1."""
    if not results:
        return {}
    max_score = max(r.score for r in results) or 1.0
    return {_cid(r): r.score / max_score for r in results if _cid(r)}


# ─── RECHERCHE AVANCEE ────────────────────────────────────────────────

def advanced_search(client, json_input: Dict[str, Any]) -> List[MatchResult]:
    """
    Recherche avancee multi-criteres.

    json_input :
    {
        "loc"                  : ["Rabat"],          # filtre dur localisation
        "exclude"              : "Casablanca",        # exclusion localisation
        "skills"               : ["Angular", "React"], # scoring BM25
        "programming_languages": ["JavaScript"],      # scoring BM25
        "role"                 : "Tech Lead",         # scoring BM25
        "experience"           : {"min": 3, "max": 7},# filtre dur experience
        "industry"             : ["IT Services"],     # scoring BM25
        "limit"                : 20
    }
    """
    loc        = json_input.get("loc", []) or []
    exclude    = json_input.get("exclude")
    skills     = json_input.get("skills", []) or []
    langs      = json_input.get("programming_languages", []) or []
    role       = json_input.get("role")
    experience = json_input.get("experience", {}) or {}
    industry   = json_input.get("industry", []) or []
    limit      = int(json_input.get("limit", 20))

    # ── 1. Pool initial — tous les candidats ou filtre localisation ───
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    if loc:
        loc_lower  = [l.lower() for l in loc]
        raw        = tenant_col.query.fetch_objects(
            limit=500,
            return_metadata=MetadataQuery(distance=True),
        )
        candidates = [
            obj for obj in raw.objects
            if _is_active(obj)
            if any(
                l in (obj.properties.get("location", "") or "").lower()
                for l in loc_lower
            )
        ]
    else:
        raw        = tenant_col.query.fetch_objects(limit=limit * 10)
        candidates = [obj for obj in raw.objects if _is_active(obj)]

    if not candidates:
        return []

    # Exclusion localisation
    if exclude:
        candidates = [
            c for c in candidates
            if exclude.lower() not in (_props(c).get("location", "") or "").lower()
        ]

    if not candidates:
        return []

    # ── 2. Filtre dur experience DIRECT ───────────────────────────────
    # On filtre ici directement sur les proprietes — pas de BM25 pour le numerique
    min_exp = 0
    max_exp = 100
    if isinstance(experience, dict):
        min_exp = int(experience.get("min", 0) or 0)
        max_exp = int(experience.get("max", 100) or 100)

    if experience:
        candidates = [
            c for c in candidates
            if min_exp <= int(_props(c).get("years_of_experience") or 0) <= max_exp
        ]

    if not candidates:
        return []

    # Construire le pool d'IDs apres filtres durs
    candidate_map = {_cid(c): c for c in candidates if _cid(c)}
    allowed_ids   = set(candidate_map.keys())

    print(f"  [Tab3] Pool apres filtres durs : {len(allowed_ids)} candidats")

    # ── 3. Scores souples par dimension ──────────────────────────────
    # Chaque dimension retourne un dict {cid: score_normalise_0_1}

    scores_by_dim: Dict[str, Dict[str, float]] = {}

    # Skills
    if skills and allowed_ids:
        skill_results = search_candidates_by_tech_skills(client, skills, len(allowed_ids), allowed_ids)
        skill_results = [r for r in skill_results if _cid(r) in allowed_ids]
        scores_by_dim["skills"] = _normalize_list(skill_results)
        print(f"  [Tab3] Skills scores : {len(scores_by_dim['skills'])} candidats")

    # Programming Languages
    if langs and allowed_ids:
        lang_results = search_candidates_by_prog_langs(client, langs, len(allowed_ids), allowed_ids)
        lang_results = [r for r in lang_results if _cid(r) in allowed_ids]
        scores_by_dim["programming_languages"] = _normalize_list(lang_results)
        print(f"  [Tab3] Langs scores : {len(scores_by_dim['programming_languages'])} candidats")

    # Role
    if role and allowed_ids:
        role_results = search_candidates_by_roles(client, role, len(allowed_ids), allowed_ids)
        role_results = [r for r in role_results if _cid(r) in allowed_ids]
        scores_by_dim["role"] = _normalize_list(role_results)
        print(f"  [Tab3] Role scores : {len(scores_by_dim['role'])} candidats")

    # Industry
    if industry and allowed_ids:
        ind_results = search_candidates_by_industry(client, industry, len(allowed_ids), allowed_ids)
        ind_results = [r for r in ind_results if _cid(r) in allowed_ids]
        scores_by_dim["industry"] = _normalize_list(ind_results)
        print(f"  [Tab3] Industry scores : {len(scores_by_dim['industry'])} candidats")

    # ── 4. Poids normalises selon dimensions actives ──────────────────
    RAW_WEIGHTS = {
        "skills":                0.45,
        "programming_languages": 0.35,
        "role":                  0.15,
        "industry":              0.05,
    }

    active_dims = [d for d in RAW_WEIGHTS if d in scores_by_dim and scores_by_dim[d]]
    if active_dims:
        total_w = sum(RAW_WEIGHTS[d] for d in active_dims)
        weights = {d: RAW_WEIGHTS[d] / total_w for d in active_dims}
    else:
        weights = {}

    print(f"  [Tab3] Dimensions actives : {active_dims} | Poids : {weights}")

    # ── 5. Score final pour chaque candidat ──────────────────────────
   # ── 5. Score final pour chaque candidat ──────────────────────────
    # Iterer sur les candidats qui ont au moins 1 score (pas sur allowed_ids)
    scored_cids = set()
    for dim_scores_dict in scores_by_dim.values():
        scored_cids.update(dim_scores_dict.keys())

    # Garder seulement ceux dans le pool filtré
    scored_cids = scored_cids & allowed_ids

    print(f"  [Tab3] CIDs avec scores : {len(scored_cids)}")

    final_results: List[MatchResult] = []

    for cid in scored_cids:
        cand_obj = candidate_map.get(cid)
        if not cand_obj:
            # Chercher avec str() normalisé
            cand_obj = next(
                (v for k, v in candidate_map.items() if str(k) == str(cid)),
                None
            )
        if not cand_obj:
            continue

        dim_scores = {
            d: scores_by_dim[d].get(cid, 0.0)
            for d in active_dims
        }

        final_score = sum(weights[d] * dim_scores.get(d, 0.0) for d in active_dims)

        props = _props(cand_obj)
        if not is_active_properties(props):
            continue

        final_results.append(MatchResult(
            id=cid,
            properties=props,
            score=final_score,
            individual_scores=dim_scores,
            search_method="weighted_advanced",
        ))

    print(f"  [Tab3] Resultats avant tri : {len(final_results)}")

    final_results.sort(key=lambda x: x.score, reverse=True)
    print(f"  [DEBUG] {cid[:8]} | dim_scores={dim_scores} | final={final_score:.6f}")
    return final_results[:limit]
