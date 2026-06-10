"""
weaviate/search/jobs_for_candidate.py
=======================================
Matching candidat -> offres.
Inverse exact de candidate_for_job.py :
  tech_skills_vector     ->  job_tech_skills_vector
  summary_vector         ->  job_summary_vector
  prog_langs_vector      ->  job_prog_langs_vector
  industry_vector        ->  job_industry_vector
  roles_held_vector      ->  job_title_vector
  work_experience_vector ->  job_description_vector
3 modes : vecteur / texte / hybride
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))


from config.settings import *
from typing import Dict, List, Any, Optional
from weaviate.classes.query import MetadataQuery
from collections import defaultdict
from weaviate_db.search.candidates_for_job  import (
    SearchConfig, MatchResult, normalize_distance, normalize_score,
    is_active_properties
)


# ─── MODE VECTEUR ─────────────────────────────────────────────────────

def _execute_vector_search_jobs(client,
                                 candidate_vectors: Dict[str, Any],
                                 limit: int,
                                 config: SearchConfig) -> List[MatchResult]:
    """
    Recherche vectorielle pure — utilise les vecteurs du candidat
    pour chercher les offres les plus proches.
    """
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    job_scores = defaultdict(lambda: {
        "tech_skills":    0.0,
        "summary":        0.0,
        "prog_langs":     0.0,
        "industry":       0.0,
        "roles_title":    0.0,
        "work_experience":0.0,
        "final_score":    0.0,
        "properties":     None
    })

    query_limit = limit * 10

    # 1. tech_skills_vector -> job_tech_skills_vector
    cand_tech_vec = candidate_vectors.get("tech_skills_vector")
    if cand_tech_vec:
        try:
            print("  Recherche : Technical Skills (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_tech_vec,
                target_vector="job_tech_skills_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["tech_skills"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills : {e}")

    # 2. summary_vector -> job_summary_vector
    cand_summary_vec = candidate_vectors.get("summary_vector")
    if cand_summary_vec:
        try:
            print("  Recherche : Summary (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_summary_vec,
                target_vector="job_summary_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["summary"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary : {e}")

    # 3. prog_langs_vector -> job_prog_langs_vector
    cand_langs_vec = candidate_vectors.get("prog_langs_vector")
    if cand_langs_vec:
        try:
            print("  Recherche : Programming Languages (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_langs_vec,
                target_vector="job_prog_langs_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["prog_langs"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages : {e}")

    # 4. industry_vector -> job_industry_vector
    cand_industry_vec = candidate_vectors.get("industry_vector")
    if cand_industry_vec:
        try:
            print("  Recherche : Industry (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_industry_vec,
                target_vector="job_industry_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["industry"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Industry : {e}")

    # 5. roles_held_vector -> job_title_vector
    cand_roles_vec = candidate_vectors.get("roles_held_vector")
    if cand_roles_vec:
        try:
            print("  Recherche : Roles vs Job Title (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_roles_vec,
                target_vector="job_title_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["roles_title"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Roles vs Job Title : {e}")

    # 6. work_experience_vector -> job_description_vector
    cand_exp_vec = candidate_vectors.get("work_experience_vector")
    if cand_exp_vec:
        try:
            print("  Recherche : Work Experience vs Job Description (Vecteur)...")
            response = job_tenant.query.near_vector(
                near_vector=cand_exp_vec,
                target_vector="job_description_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                norm_score = normalize_distance(obj.metadata.distance)
                job_scores[obj.uuid]["work_experience"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Work Experience vs Job Description : {e}")

    # Poids mode vecteur
    weights = {
        "tech_skills":     0.27,
        "prog_langs":      0.27,
        "summary":         0.21,
        "industry":        0.10,
        "roles_title":     0.10,
        "work_experience": 0.05,
    }

    match_results = []
    for uuid, scores in job_scores.items():
        if scores["properties"] is None or not is_active_properties(scores["properties"]):
            continue
        final_score = sum(
            scores[k] * weights.get(k, 0.0)
            for k in weights
        )
        if final_score >= config.score_threshold:
            match_results.append(MatchResult(
                id=str(uuid),
                score=final_score,
                properties=scores["properties"],
                individual_scores={
                    "tech_skills":     scores["tech_skills"],
                    "summary":         scores["summary"],
                    "prog_langs":      scores["prog_langs"],
                    "industry":        scores["industry"],
                    "roles_title":     scores["roles_title"],
                    "work_experience": scores["work_experience"],
                },
                search_method="vector_search"
            ))

    match_results.sort(key=lambda x: x.score, reverse=True)
    return match_results[:limit]


# ─── MODE TEXTE ───────────────────────────────────────────────────────

def _execute_text_search_jobs(client,
                               candidate_properties: Dict[str, Any],
                               limit: int) -> List[MatchResult]:
    """
    Recherche BM25 pure sur les proprietes textuelles du candidat.
    """
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    job_scores = defaultdict(lambda: {
        "tech_skills": 0.0,
        "prog_langs":  0.0,
        "summary":     0.0,
        "roles_title": 0.0,
        "final_score": 0.0,
        "properties":  None
    })

    query_limit = limit * 10

    # 1. Technical Skills BM25
    tech_skills = candidate_properties.get("technical_skills", []) or []
    if tech_skills:
        query_text = " ".join(tech_skills) if isinstance(tech_skills, list) else str(tech_skills)
        try:
            print("  Recherche : Technical Skills (BM25)...")
            response = job_tenant.query.bm25(
                query=query_text,
                query_properties=["technical_skills"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["tech_skills"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills BM25 : {e}")

    # 2. Programming Languages BM25
    prog_langs = candidate_properties.get("programming_languages", []) or []
    if prog_langs:
        query_text = " ".join(prog_langs) if isinstance(prog_langs, list) else str(prog_langs)
        try:
            print("  Recherche : Programming Languages (BM25)...")
            response = job_tenant.query.bm25(
                query=query_text,
                query_properties=["programming_languages"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["prog_langs"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages BM25 : {e}")

    # 3. Summary BM25
    summary = candidate_properties.get("summary", "") or ""
    if summary:
        try:
            print("  Recherche : Summary (BM25)...")
            response = job_tenant.query.bm25(
                query=str(summary),
                query_properties=["summary", "job_description"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["summary"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary BM25 : {e}")

    # 4. Roles vs Job Title BM25
    roles = candidate_properties.get("roles_held", []) or []
    if roles:
        query_text = " ".join(roles) if isinstance(roles, list) else str(roles)
        try:
            print("  Recherche : Roles vs Job Title (BM25)...")
            response = job_tenant.query.bm25(
                query=query_text,
                query_properties=["title"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["roles_title"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Roles vs Job Title BM25 : {e}")

    # Poids mode texte
    weights = {
        "tech_skills": 0.35,
        "prog_langs":  0.35,
        "summary":     0.20,
        "roles_title": 0.10,
    }

    match_results = []
    for uuid, scores in job_scores.items():
        if scores["properties"] is None or not is_active_properties(scores["properties"]):
            continue
        final_score = sum(
            scores[k] * weights.get(k, 0.0)
            for k in weights
        )
        match_results.append(MatchResult(
            id=str(uuid),
            score=final_score,
            properties=scores["properties"],
            individual_scores={
                "tech_skills": scores["tech_skills"],
                "prog_langs":  scores["prog_langs"],
                "summary":     scores["summary"],
                "roles_title": scores["roles_title"],
            },
            search_method="text_search"
        ))

    match_results.sort(key=lambda x: x.score, reverse=True)
    return match_results[:limit]


# ─── MODE HYBRIDE ─────────────────────────────────────────────────────

def _execute_hybrid_search_jobs(client,
                                 candidate_properties: Dict[str, Any],
                                 candidate_vectors: Dict[str, Any],
                                 limit: int,
                                 config: SearchConfig = None) -> List[MatchResult]:
    """
    Recherche hybride — combine BM25 + vecteurs pour chaque dimension.
    """
    if config is None:
        config = SearchConfig()

    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    job_scores = defaultdict(lambda: {
        "tech_skills": 0.0,
        "summary":     0.0,
        "prog_langs":  0.0,
        "industry":    0.0,
        "roles_title": 0.0,
        "final_score": 0.0,
        "properties":  None
    })

    query_limit = limit * 10

    # 1. Technical Skills Hybride
    tech_skills = candidate_properties.get("technical_skills", []) or []
    tech_text   = " ".join(tech_skills) if isinstance(tech_skills, list) else str(tech_skills)
    if tech_text:
        try:
            print("  Recherche : Technical Skills (Hybride)...")
            response = job_tenant.query.hybrid(
                query=tech_text,
                target_vector="job_tech_skills_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["tech_skills"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills hybride : {e}")

    # 2. Summary Hybride
    summary = candidate_properties.get("summary", "") or ""
    if summary:
        try:
            print("  Recherche : Summary (Hybride)...")
            response = job_tenant.query.hybrid(
                query=str(summary),
                target_vector="job_summary_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["summary"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary hybride : {e}")

    # 3. Programming Languages Hybride
    prog_langs = candidate_properties.get("programming_languages", []) or []
    langs_text = " ".join(prog_langs) if isinstance(prog_langs, list) else str(prog_langs)
    if langs_text:
        try:
            print("  Recherche : Programming Languages (Hybride)...")
            response = job_tenant.query.hybrid(
                query=langs_text,
                target_vector="job_prog_langs_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["prog_langs"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages hybride : {e}")

    # 4. Industry Hybride
    industries = candidate_properties.get("industry_primary_industries", []) or []
    industry_text = " ".join(industries) if isinstance(industries, list) else str(industries)
    if industry_text:
        try:
            print("  Recherche : Industry (Hybride)...")
            response = job_tenant.query.hybrid(
                query=industry_text,
                target_vector="job_industry_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["industry"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Industry hybride : {e}")

    # 5. Roles vs Job Title Hybride
    roles     = candidate_properties.get("roles_held", []) or []
    roles_text = " ".join(roles) if isinstance(roles, list) else str(roles)
    if roles_text:
        try:
            print("  Recherche : Roles vs Job Title (Hybride)...")
            response = job_tenant.query.hybrid(
                query=roles_text,
                target_vector="job_title_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                norm_score = normalize_score(obj.metadata.score)
                job_scores[obj.uuid]["roles_title"] = norm_score
                if job_scores[obj.uuid]["properties"] is None:
                    job_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Roles vs Job Title hybride : {e}")

    # Poids hybride (identiques au projet existant)
    weights = {
        "tech_skills": 0.27,
        "summary":     0.21,
        "prog_langs":  0.27,
        "industry":    0.15,
        "roles_title": 0.10,
    }

    match_results = []
    for uuid, scores in job_scores.items():
        if scores["properties"] is None or not is_active_properties(scores["properties"]):
            continue
        final_score = (
            scores["tech_skills"] * weights["tech_skills"] +
            scores["summary"]     * weights["summary"]     +
            scores["prog_langs"]  * weights["prog_langs"]  +
            scores["industry"]    * weights["industry"]    +
            scores["roles_title"] * weights["roles_title"]
        )
        if final_score >= config.score_threshold:
            match_results.append(MatchResult(
                id=str(uuid),
                score=final_score,
                properties=scores["properties"],
                individual_scores={
                    "tech_skills": scores["tech_skills"],
                    "summary":     scores["summary"],
                    "prog_langs":  scores["prog_langs"],
                    "industry":    scores["industry"],
                    "roles_title": scores["roles_title"],
                },
                search_method="hybrid_search"
            ))

    match_results.sort(key=lambda x: x.score, reverse=True)
    return match_results[:limit]


# ─── FILTRE SÉNIORITÉ ─────────────────────────────────────────────────

def seniority_filter_jobs(
    jobs: List[MatchResult],
    candidate_years: int,
    tolerance: int = 4,
    min_results: int = 5
) -> List[MatchResult]:
    """
    Filtre les offres dont le niveau requis est trop éloigné
    des années d'expérience du candidat.
    Ex: candidat 3 ans  → garde offres 0–7 ans requis
        candidat 10 ans → garde offres 6–14 ans requis
    """
    min_years = max(0, candidate_years - tolerance)
    max_years = candidate_years + tolerance

    print(f"  Seniority filter : candidat {candidate_years} ans → offres {min_years}–{max_years} ans requis")

    filtered = [
        job for job in jobs
        if min_years <= int((job.properties or {}).get("years_of_experience_required") or 0) <= max_years
    ]

    if len(filtered) < min_results:
        print(f"  Seniority filter trop restrictif ({len(filtered)}) → ignoré")
        return jobs

    print(f"  Seniority filter : {len(jobs)} → {len(filtered)} offres")
    return filtered


# ─── POINT D'ENTREE PRINCIPAL ─────────────────────────────────────────

def search_job_for_candidate_by_id(client,
                                    candidate_uuid: str,
                                    search_mode: Optional[str] = "hybride",
                                    limit: int = 10) -> List[MatchResult]:
    """
    Recherche les meilleures offres pour un candidat donne (par UUID).
    search_mode : 'vecteur' | 'texte' | 'hybride'
    """
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    vector_names = [
        "tech_skills_vector",
        "summary_vector",
        "prog_langs_vector",
        "industry_vector",
        "roles_held_vector",
        "work_experience_vector",
    ]

    cand_obj = candidate_tenant.query.fetch_object_by_id(
        candidate_uuid, include_vector=vector_names
    )
    if cand_obj is None:
        raise ValueError(f"Aucun candidat trouve avec l'UUID {candidate_uuid}")

    candidate_properties = cand_obj.properties
    if not is_active_properties(candidate_properties):
        raise ValueError(f"Le candidat {candidate_uuid} est archive")

    candidate_vectors    = {
        name: cand_obj.vector[name]
        for name in vector_names
        if name in cand_obj.vector
    }

    config = SearchConfig()

    print(f"\nCandidat UUID : {candidate_uuid}")
    print(f"Mode de recherche : {search_mode}")

    # APRÈS
    candidate_years = int(candidate_properties.get("years_of_experience") or 0)

    if search_mode == "vecteur":
        results = _execute_vector_search_jobs(client, candidate_vectors, limit * 2, config)
    elif search_mode == "texte":
        results = _execute_text_search_jobs(client, candidate_properties, limit * 2)
    else:
        results = _execute_hybrid_search_jobs(
            client, candidate_properties, candidate_vectors, limit * 2, config
        )

    results = seniority_filter_jobs(results, candidate_years)
    return results[:limit]


# ─── RECHERCHE PAR TITRE ─────────────────────────────────────────────

def search_job_by_title(client, title: str) -> Any:
    """Recherche une offre par titre (hybride)."""
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    response = job_tenant.query.hybrid(
        query=str(title),
        query_properties=["title"],
        target_vector="job_title_vector",
        limit=10,
        return_metadata=MetadataQuery(distance=True, score=True),
    )
    response.objects = [
        obj for obj in response.objects
        if is_active_properties(obj.properties)
    ][:3]
    return response
