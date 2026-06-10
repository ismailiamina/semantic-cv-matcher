"""
weaviate/search/keywords.py
============================
Recherches par mots-cles .
Fonctions de recherche directe par skills, langages, localisation, role, experience, industrie.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))
from typing import List, Optional, Set
from weaviate.classes.query import MetadataQuery, Filter, Sort
from weaviate_db.search.candidates_for_job import (
    SearchConfig,
    MatchResult,
    normalize_distance,
    normalize_score,
    is_active_properties,
)
from config.settings import *


# ─── RECHERCHE PAR COMPETENCES TECHNIQUES ────────────────────────────

def search_candidates_by_tech_skills(
    client,
    skills: List[str],
    limit: int = 10,
    allowed_ids: Optional[Set[str]] = None,
    config: SearchConfig = None
) -> List[MatchResult]:
    """Recherche candidats par competences techniques — hybrid search."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    query_text      = ", ".join(skills)
    exact_filter    = Filter.by_property("technical_skills").contains_any(skills)

    if allowed_ids:
        id_filter       = Filter.by_id().contains_any(list(allowed_ids))
        combined_filter = exact_filter & id_filter
    else:
        combined_filter = exact_filter

    results = tenant_col.query.hybrid(
        query=query_text,
        query_properties=["technical_skills"],
        alpha=0.5,
        limit=limit,
        filters=combined_filter,
        target_vector="tech_skills_vector",
        return_metadata=MetadataQuery(score=True, distance=True)
    )

    return [
    MatchResult(
        id=str(obj.uuid),
        properties=obj.properties,
        score=normalize_score(obj.metadata.score),  # ← corrigé
        search_method="hybrid",
    )
    for obj in results.objects
    if is_active_properties(obj.properties)
]


# ─── RECHERCHE PAR LANGAGES DE PROGRAMMATION ─────────────────────────

def search_candidates_by_prog_langs(
    client,
    langs: List[str],
    limit: int = 10,
    allowed_ids: Optional[Set[str]] = None,
    config: SearchConfig = None
) -> List[MatchResult]:
    """Recherche candidats par langages de programmation — BM25."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    query_text = ", ".join(langs)

    filters = None
    if allowed_ids:
        filters = Filter.by_id().contains_any(list(allowed_ids))

    results = tenant_col.query.bm25(
        query=query_text,
        query_properties=["programming_languages", "projects"],
        limit=limit,
        filters=filters,
        return_metadata=MetadataQuery(score=True)
    )

    return [
        MatchResult(
            id=str(obj.uuid),
            properties=obj.properties,
            score=normalize_score(obj.metadata.score),
            search_method="bm25",
        )
        for obj in results.objects
        if is_active_properties(obj.properties)
    ]


# ─── RECHERCHE PAR LOCALISATION ───────────────────────────────────────

def search_candidates_by_location(
    client,
    loc: List[str],
    limit: int = 10,
    config: SearchConfig = None
) -> List[MatchResult]:
    """Recherche candidats par localisation — hybrid search."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    query_text = ", ".join(loc)

    results = tenant_col.query.hybrid(
        query=query_text,
        query_properties=["location"],
        alpha=0.25,
        limit=limit,
        target_vector="location_vector",
        return_metadata=MetadataQuery(distance=True, score=True),
    )

    return [
        MatchResult(
            id=str(obj.uuid),
            properties=obj.properties,
            score=normalize_score(obj.metadata.score),
            search_method="hybrid",
        )
        for obj in results.objects
        if is_active_properties(obj.properties)
    ]


# ─── RECHERCHE PAR ROLE ───────────────────────────────────────────────

def search_candidates_by_roles(
    client,
    role: str,
    limit: int = 10,
    allowed_ids: Optional[Set[str]] = None,
    config: SearchConfig = None
) -> List[MatchResult]:
    """Recherche candidats par role — hybrid search sur roles_held + work_experience."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    filters = None
    if allowed_ids:
        filters = Filter.by_id().contains_any(list(allowed_ids))

    results = tenant_col.query.hybrid(
        query=role,
        query_properties=["roles_held", "work_experience"],
        alpha=0.25,
        limit=limit,
        filters=filters,
        target_vector=["roles_held_vector", "work_experience_vector"],
        return_metadata=MetadataQuery(distance=True, score=True),
    )

    return [
        MatchResult(
            id=str(obj.uuid),
            properties=obj.properties,
            score=normalize_score(obj.metadata.score),
            search_method="hybrid",
        )
        for obj in results.objects
        if is_active_properties(obj.properties)
    ]


# ─── RECHERCHE PAR EXPERIENCE ─────────────────────────────────────────

def normalize_experience(years: int, min_exp: int, max_exp: int) -> float:
    if max_exp <= min_exp:
        return 0.0
    return (years - min_exp) / (max_exp - min_exp)


def search_candidates_by_experience(
    client,
    min: int,
    max: int,
    limit: int = 10,
    config: SearchConfig = None
) -> List[MatchResult]:
    """Recherche candidats par annees d'experience — filter + sort."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    experience_filter = (
        Filter.by_property("years_of_experience").greater_than(min - 1) &
        Filter.by_property("years_of_experience").less_than(max + 1)
    )
    sort_option = Sort.by_property("years_of_experience", ascending=False)

    results = tenant_col.query.fetch_objects(
        filters=experience_filter,
        limit=limit,
        sort=sort_option,
        return_metadata=MetadataQuery(distance=True, score=True),
    )

    return [
        MatchResult(
            id=str(obj.uuid),
            properties=obj.properties,
            score=normalize_experience(
                obj.properties.get("years_of_experience", 0), min, max
            ),
            search_method="filter_sort",
        )
        for obj in results.objects
        if is_active_properties(obj.properties)
    ]


# ─── RECHERCHE PAR INDUSTRIE ──────────────────────────────────────────

def search_candidates_by_industry(
    client,
    industry: List[str],
    limit: int = 10,
    allowed_ids: Optional[Set[str]] = None,
    config: Optional[SearchConfig] = None
) -> List[MatchResult]:
    """Recherche candidats par industrie — hybrid search."""
    if not industry:
        return []

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    query_text = ", ".join(industry)

    filters = None
    if allowed_ids:
        filters = Filter.by_id().contains_any(list(allowed_ids))

    results = tenant_col.query.hybrid(
        query=query_text,
        query_properties=["industry_primary_industries"],
        alpha=0.25,
        limit=limit,
        filters=filters,
        target_vector="industry_vector",
        return_metadata=MetadataQuery(distance=True, score=True),
    )

    return [
        MatchResult(
            id=str(obj.uuid),
            properties=obj.properties,
            score=normalize_score(obj.metadata.score),
            search_method="hybrid",
        )
        for obj in results.objects
        if is_active_properties(obj.properties)
    ]
