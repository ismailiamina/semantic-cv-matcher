"""
weaviate/search/candidate_for_job.py
======================================
Matching offre -> candidats.
3 modes : vecteur / texte / hybride

Améliorations v2 :
  - Poids dynamiques selon le domaine du job (DevOps / Frontend / Data / Backend)
  - Hard filter : au moins 1 skill critique de l'offre obligatoire
  - Jaccard minimum : élimine les candidats hors sujet
  - Résultats nettement plus précis sans et avec reranker
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent.parent))

from dataclasses import dataclass
from config.settings import *
from typing import Dict, List, Any, Optional
from weaviate.classes.query import MetadataQuery
from collections import defaultdict


# ─── DATA CLASSES ─────────────────────────────────────────────────────

@dataclass
class SearchConfig:
    """Configuration des parametres de recherche."""
    score_threshold: float = 0.0
    alpha: float = 0.5


@dataclass
class MatchResult:
    """Resultat standardise pour tous les types de recherche."""
    id: str
    score: float
    properties: Dict[str, Any]
    individual_scores: Optional[Dict[str, float]] = None
    distance: Optional[float] = None
    search_method: Optional[str] = None
    rerank_score: Optional[float] = None
    vector_type: Optional[str] = None
    search_type: Optional[str] = None


# ─── NORMALISATION ────────────────────────────────────────────────────

def normalize_distance(distance) -> float:
    """Normalise la distance (plus faible = meilleur match)."""
    if distance is None:
        return 0.0
    return 1.0 / (1.0 + distance)


def normalize_score(score) -> float:
    """Normalise le score hybride (plus eleve = meilleur match)."""
    if score is None:
        return 0.0
    return max(0.0, min(1.0, score))


def is_active_properties(properties: Dict[str, Any]) -> bool:
    """Retourne False pour les objets archives par soft delete."""
    return not bool((properties or {}).get("is_archived", False))


# ─── POIDS DYNAMIQUES PAR DOMAINE ─────────────────────────────────────

# Mots-clés pour détecter le domaine d'une offre
DOMAIN_KEYWORDS = {
    "devops": [
        "terraform", "kubernetes", "ansible", "docker", "ci/cd", "devops",
        "gitlab ci", "jenkins", "helm", "argocd", "prometheus", "grafana",
        "openshift", "cloudformation", "devsecops", "gitops"
    ],
    "frontend": [
        "angular", "react", "vue", "next.js", "nuxt", "frontend",
        "front-end", "design system", "rxjs", "ngrx", "storybook",
        "cypress", "playwright", "webpack", "vite", "svelte"
    ],
    "data": [
        "spark", "databricks", "hadoop", "snowflake", "airflow", "kafka",
        "data engineer", "pyspark", "dbt", "bigquery", "redshift",
        "hive", "nifi", "flink", "data pipeline", "etl", "elt"
    ],
    "backend": [
        "spring boot", "nestjs", "node.js", "django", "fastapi",
        "java ee", "microservices", "api rest", "quarkus", "laravel",
        "symfony", "express", ".net core", "asp.net"
    ],
    "security": [
        "pentest", "sast", "dast", "owasp", "soc", "siem", "soar",
        "cybersecurite", "cybersecurity", "ethical hacker", "red team",
        "devsecops", "vault", "cnapp", "firewall", "fortinet"
    ],
}

# Poids par domaine — calibrés pour réduire les faux positifs
DOMAIN_WEIGHTS = {
    "devops": {
        "tech_skills": 0.42,   # skills infra = critère principal
        "roles_title": 0.22,   # "DevOps Engineer" dans les rôles
        "summary":     0.18,
        "industry":    0.10,
        "prog_langs":  0.08,   # Python seul ne suffit pas pour DevOps
    },
    "frontend": {
        "tech_skills": 0.40,   # Angular/React = critère principal
        "roles_title": 0.20,   # "Frontend Developer" dans les rôles
        "summary":     0.20,
        "industry":    0.12,
        "prog_langs":  0.08,   # TypeScript seul ne remplace pas Angular
    },
    "data": {
        "tech_skills": 0.35,
        "prog_langs":  0.28,   # Python/PySpark très importants en Data
        "summary":     0.20,
        "industry":    0.10,
        "roles_title": 0.07,
    },
    "backend": {
        "tech_skills": 0.32,
        "prog_langs":  0.25,   # Java/TypeScript importants en Backend
        "summary":     0.20,
        "roles_title": 0.15,
        "industry":    0.08,
    },
    "security": {
        "tech_skills": 0.45,   # outils sécu très spécifiques
        "roles_title": 0.22,
        "summary":     0.18,
        "industry":    0.10,
        "prog_langs":  0.05,
    },
    "general": {
        "tech_skills": 0.27,
        "prog_langs":  0.27,
        "summary":     0.21,
        "industry":    0.15,
        "roles_title": 0.10,
    },
}


def detect_job_domain(job_properties: Dict[str, Any]) -> str:
    """
    Détecte le domaine métier d'une offre à partir de ses skills et titre.
    Retourne : 'devops' | 'frontend' | 'data' | 'backend' | 'security' | 'general'
    """
    skills = job_properties.get("technical_skills", []) or []
    langs  = job_properties.get("programming_languages", []) or []
    title  = job_properties.get("title", "") or ""

    all_text = " ".join(
        [s.lower() for s in skills] +
        [l.lower() for l in langs] +
        [title.lower()]
    )

    domain_scores = {domain: 0 for domain in DOMAIN_KEYWORDS}
    for domain, keywords in DOMAIN_KEYWORDS.items():
        for kw in keywords:
            if kw in all_text:
                domain_scores[domain] += 1

    best_domain = max(domain_scores, key=domain_scores.get)
    if domain_scores[best_domain] == 0:
        return "general"

    print(f"  Domaine détecté : {best_domain} (score={domain_scores[best_domain]})")
    return best_domain


def get_weights_for_job(job_properties: Dict[str, Any]) -> Dict[str, float]:
    """Retourne les poids adaptés au domaine de l'offre."""
    domain = detect_job_domain(job_properties)
    return DOMAIN_WEIGHTS[domain]


# ─── HARD FILTER — SKILLS CRITIQUES ───────────────────────────────────

def extract_critical_skills(job_properties: Dict[str, Any]) -> List[str]:
    """
    Extrait les skills critiques de l'offre (niveau Expert ou Senior requis).
    Ce sont les compétences non-négociables pour le poste.
    """
    senio = job_properties.get("seniority_requirements_technologies", []) or []
    critical = [
        s["technology"].lower()
        for s in senio
        if s.get("level") in ["Expert", "Senior", "Confirmé"]
    ]

    # Fallback : si pas de seniority défini, prendre les 3 premiers skills
    if not critical:
        skills = job_properties.get("technical_skills", []) or []
        critical = [s.lower() for s in skills[:3]]

    return critical


def hard_filter_candidates(
    candidates: List[MatchResult],
    job_properties: Dict[str, Any],
    min_results: int = 5
) -> List[MatchResult]:
    """
    Filtre dur : élimine les candidats sans aucun skill critique de l'offre.
    Garantit un minimum de résultats pour éviter les pages vides.
    """
    critical = extract_critical_skills(job_properties)
    if not critical:
        return candidates

    print(f"  Hard filter — skills critiques : {critical[:5]}")

    filtered = []
    for result in candidates:
        props = result.properties or {}
        c_skills = " ".join([
            s.lower() for s in (props.get("technical_skills") or [])
        ] + [
            l.lower() for l in (props.get("programming_languages") or [])
        ] + [
            r.lower() for r in (props.get("roles_held") or [])
        ] + [
            (props.get("summary") or "").lower()
        ])

        if any(skill in c_skills for skill in critical):
            filtered.append(result)

    # Si le filtre est trop restrictif, retourner les originaux
    if len(filtered) < min_results:
        print(f"  Hard filter trop restrictif ({len(filtered)} résultats) → filtre ignoré")
        return candidates

    print(f"  Hard filter : {len(candidates)} → {len(filtered)} candidats")
    return filtered


# ─── JACCARD FILTER ───────────────────────────────────────────────────

def jaccard_skills(skills_a: List[str], skills_b: List[str]) -> float:
    """Calcule le score Jaccard entre deux listes de skills."""
    if not skills_a or not skills_b:
        return 0.0
    a = set(s.lower().strip() for s in skills_a)
    b = set(s.lower().strip() for s in skills_b)
    intersection = len(a & b)
    union = len(a | b)
    return intersection / union if union > 0 else 0.0


def jaccard_filter_candidates(
    candidates: List[MatchResult],
    job_properties: Dict[str, Any],
    min_jaccard: float = 0.03,
    min_results: int = 5
) -> List[MatchResult]:
    """
    Élimine les candidats dont le Jaccard tech_skills < min_jaccard.
    Évite les faux positifs sémantiques (ex: Data Engineer pour une offre DevOps).
    """
    job_skills = job_properties.get("technical_skills", []) or []
    if not job_skills:
        return candidates

    filtered = []
    for result in candidates:
        props = result.properties or {}
        c_skills = props.get("technical_skills") or []
        score = jaccard_skills(c_skills, job_skills)
        if score >= min_jaccard:
            filtered.append(result)

    if len(filtered) < min_results:
        print(f"  Jaccard filter trop restrictif ({len(filtered)} résultats) → filtre ignoré")
        return candidates

    print(f"  Jaccard filter : {len(candidates)} → {len(filtered)} candidats (min={min_jaccard:.2f})")
    return filtered


# ─── CALCUL DU SCORE FINAL PONDÉRÉ ────────────────────────────────────

def compute_weighted_score(
    candidate_scores: dict,
    weights: Dict[str, float],
    config: SearchConfig
) -> List[MatchResult]:
    """
    Calcule le score final pondéré pour chaque candidat
    à partir des scores par dimension et des poids du domaine.
    """
    match_results = []
    for uuid, scores in candidate_scores.items():
        if scores["properties"] is None or not is_active_properties(scores["properties"]):
            continue
        final_score = sum(
            scores.get(k, 0.0) * weights.get(k, 0.0)
            for k in weights
        )
        if final_score >= config.score_threshold:
            match_results.append(MatchResult(
                id=str(uuid),
                score=final_score,
                properties=scores["properties"],
                individual_scores={
                    k: scores.get(k, 0.0) for k in weights
                },
                search_method=scores.get("search_method", "unknown")
            ))

    match_results.sort(key=lambda x: x.score, reverse=True)
    return match_results


# ─── MODE VECTEUR ─────────────────────────────────────────────────────

def _execute_vector_search(
    client,
    job_properties: Dict[str, Any],
    job_vectors: Dict[str, Any],
    limit: int,
    config: SearchConfig
) -> List[MatchResult]:
    """
    Recherche vectorielle pure — utilise les vecteurs du job
    pour chercher les candidats les plus proches.
    Poids adaptés au domaine de l'offre.
    """
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills":    0.0,
        "summary":        0.0,
        "prog_langs":     0.0,
        "industry":       0.0,
        "roles_title":    0.0,
        "work_experience":0.0,
        "search_method":  "vector_search",
        "properties":     None
    })

    query_limit = limit * 10

    # 1. Technical Skills
    job_tech_skills_vec = job_vectors.get("job_tech_skills_vector")
    if job_tech_skills_vec:
        try:
            print("  Recherche : Technical Skills (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_tech_skills_vec,
                target_vector="tech_skills_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["tech_skills"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills : {e}")

    # 2. Summary
    job_summary_vec = job_vectors.get("job_summary_vector")
    if job_summary_vec:
        try:
            print("  Recherche : Summary (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_summary_vec,
                target_vector="summary_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["summary"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary : {e}")

    # 3. Programming Languages
    job_prog_langs_vec = job_vectors.get("job_prog_langs_vector")
    if job_prog_langs_vec:
        try:
            print("  Recherche : Programming Languages (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_prog_langs_vec,
                target_vector="prog_langs_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["prog_langs"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages : {e}")

    # 4. Industry
    job_industry_vec = job_vectors.get("job_industry_vector")
    if job_industry_vec:
        try:
            print("  Recherche : Industry (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_industry_vec,
                target_vector="industry_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["industry"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Industry : {e}")

    # 5. Job Title vs Roles
    job_title_vec = job_vectors.get("job_title_vector")
    if job_title_vec:
        try:
            print("  Recherche : Job Title vs Roles (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_title_vec,
                target_vector="roles_held_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["roles_title"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Job Title vs Roles : {e}")

    # 6. Job Description vs Work Experience
    job_desc_vec = job_vectors.get("job_description_vector")
    if job_desc_vec:
        try:
            print("  Recherche : Job Description vs Work Experience (Vecteur)...")
            response = candidate_tenant.query.near_vector(
                near_vector=job_desc_vec,
                target_vector="work_experience_vector",
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["work_experience"] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Job Description vs Work Experience : {e}")

    # Poids dynamiques selon le domaine
    weights = get_weights_for_job(job_properties)
    weights["work_experience"] = 0.05  # work_experience toujours faible

    results = list(compute_weighted_score(candidate_scores, weights, config))

    # Filtres post-scoring
    results = hard_filter_candidates(results, job_properties)
    results = jaccard_filter_candidates(results, job_properties)

    return results[:limit]


# ─── MODE TEXTE ───────────────────────────────────────────────────────

def _execute_text_search(
    client,
    job_properties: Dict[str, Any],
    limit: int
) -> List[MatchResult]:
    """
    Recherche BM25 pure sur les proprietes textuelles du job.
    Poids adaptés au domaine de l'offre.
    """
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0,
        "prog_langs":  0.0,
        "summary":     0.0,
        "roles_title": 0.0,
        "search_method": "text_search",
        "properties":  None
    })

    config = SearchConfig()
    query_limit = limit * 10

    # 1. Technical Skills BM25
    tech_skills = job_properties.get("technical_skills", []) or []
    if tech_skills:
        query_text = " ".join(tech_skills) if isinstance(tech_skills, list) else str(tech_skills)
        try:
            print("  Recherche : Technical Skills (BM25)...")
            response = candidate_tenant.query.bm25(
                query=query_text,
                query_properties=["technical_skills"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["tech_skills"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills BM25 : {e}")

    # 2. Programming Languages BM25
    prog_langs = job_properties.get("programming_languages", []) or []
    if prog_langs:
        query_text = " ".join(prog_langs) if isinstance(prog_langs, list) else str(prog_langs)
        try:
            print("  Recherche : Programming Languages (BM25)...")
            response = candidate_tenant.query.bm25(
                query=query_text,
                query_properties=["programming_languages", "projects"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["prog_langs"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages BM25 : {e}")

    # 3. Summary BM25
    summary = job_properties.get("summary", "") or ""
    if summary:
        try:
            print("  Recherche : Summary (BM25)...")
            response = candidate_tenant.query.bm25(
                query=str(summary),
                query_properties=["summary", "work_experience"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["summary"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary BM25 : {e}")

    # 4. Job Title vs Roles BM25
    title = job_properties.get("title", "") or ""
    if title:
        try:
            print("  Recherche : Job Title vs Roles (BM25)...")
            response = candidate_tenant.query.bm25(
                query=str(title),
                query_properties=["roles_held", "work_experience"],
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["roles_title"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Job Title BM25 : {e}")

    # Poids dynamiques selon le domaine
    weights = get_weights_for_job(job_properties)

    results = list(compute_weighted_score(candidate_scores, weights, config))

    # Filtres post-scoring
    results = hard_filter_candidates(results, job_properties)
    results = jaccard_filter_candidates(results, job_properties)

    return results[:limit]


# ─── MODE HYBRIDE ─────────────────────────────────────────────────────

def _execute_hybrid_search(
    client,
    job_properties: Dict[str, Any],
    job_vectors: Dict[str, Any],
    limit: int,
    config: SearchConfig = None
) -> List[MatchResult]:
    """
    Recherche hybride — combine BM25 + vecteurs pour chaque dimension.
    Poids dynamiques selon le domaine du job.
    Hard filter + Jaccard filter pour éliminer les faux positifs.
    """
    if config is None:
        config = SearchConfig()

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0,
        "summary":     0.0,
        "prog_langs":  0.0,
        "industry":    0.0,
        "roles_title": 0.0,
        "search_method": "hybrid_search",
        "properties":  None
    })

    query_limit = limit * 10

    # 1. Technical Skills Hybride
    tech_skills = job_properties.get("technical_skills", []) or []
    job_tech_text = " ".join(tech_skills) if isinstance(tech_skills, list) else str(tech_skills)

    if job_tech_text:
        try:
            print("  Recherche : Technical Skills (Hybride)...")
            response = candidate_tenant.query.hybrid(
                query=job_tech_text,
                target_vector="tech_skills_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["tech_skills"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Technical Skills hybride : {e}")

    # 2. Summary Hybride
    summary = job_properties.get("summary", "") or ""
    if summary:
        try:
            print("  Recherche : Summary (Hybride)...")
            response = candidate_tenant.query.hybrid(
                query=str(summary),
                target_vector="summary_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["summary"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Summary hybride : {e}")

    # 3. Programming Languages Hybride
    prog_langs = job_properties.get("programming_languages", []) or []
    job_prog_text = " ".join(prog_langs) if isinstance(prog_langs, list) else str(prog_langs)

    if job_prog_text:
        try:
            print("  Recherche : Programming Languages (Hybride)...")
            response = candidate_tenant.query.hybrid(
                query=job_prog_text,
                target_vector="prog_langs_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["prog_langs"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Programming Languages hybride : {e}")

    # 4. Industry Hybride
    industry = job_properties.get("industry", "") or ""
    if industry:
        try:
            print("  Recherche : Industry (Hybride)...")
            response = candidate_tenant.query.hybrid(
                query=str(industry),
                target_vector="industry_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["industry"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Industry hybride : {e}")

    # 5. Job Title vs Roles Hybride
    title = job_properties.get("title", "") or ""
    if title:
        try:
            print("  Recherche : Job Title vs Roles (Hybride)...")
            response = candidate_tenant.query.hybrid(
                query=str(title),
                target_vector="roles_held_vector",
                alpha=config.alpha,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            for obj in response.objects:
                candidate_scores[obj.uuid]["roles_title"] = normalize_score(obj.metadata.score)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur Job Title vs Roles hybride : {e}")

    # Poids dynamiques selon le domaine de l'offre
    weights = get_weights_for_job(job_properties)

    results = list(compute_weighted_score(candidate_scores, weights, config))

    # Filtres post-scoring pour éliminer les faux positifs
    results = hard_filter_candidates(results, job_properties)
    results = jaccard_filter_candidates(results, job_properties)

    return results[:limit]


# ─── POINT D'ENTREE PRINCIPAL ─────────────────────────────────────────

def search_candidate_for_job_by_id(
    client,
    job_uuid: str,
    search_mode: Optional[str] = "hybride",
    limit: int = 10
) -> List[MatchResult]:
    """
    Recherche les meilleurs candidats pour une offre donnee (par UUID).
    search_mode : 'vecteur' | 'texte' | 'hybride'

    Pipeline complet :
      1. Weaviate search (vecteur / BM25 / hybride) → top N candidats
      2. Score pondéré avec poids dynamiques selon le domaine du job
      3. Hard filter : au moins 1 skill critique de l'offre obligatoire
      4. Jaccard filter : élimine les candidats hors sujet
      5. Retour des top `limit` candidats
    """
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    vector_names = [
        "job_description_vector",
        "job_industry_vector",
        "job_prog_langs_vector",
        "job_summary_vector",
        "job_tech_skills_vector",
        "job_title_vector"
    ]

    job_obj = job_tenant.query.fetch_object_by_id(job_uuid, include_vector=vector_names)
    if job_obj is None:
        raise ValueError(f"Aucune offre trouvee avec l'UUID {job_uuid}")

    job_properties = job_obj.properties
    if not is_active_properties(job_properties):
        raise ValueError(f"L'offre {job_uuid} est archivee")

    job_vectors    = {
        name: job_obj.vector[name]
        for name in vector_names
        if name in job_obj.vector
    }

    config = SearchConfig()

    print(f"\nOffre UUID : {job_uuid}")
    print(f"Titre      : {job_properties.get('title', 'N/A')}")
    print(f"Mode       : {search_mode}")

    if search_mode == "vecteur":
        return _execute_vector_search(client, job_properties, job_vectors, limit, config)
    elif search_mode == "texte":
        return _execute_text_search(client, job_properties, limit)
    else:
        return _execute_hybrid_search(client, job_properties, job_vectors, limit, config)


# ─── RECHERCHE PAR NOM ────────────────────────────────────────────────

def search_candidate_by_name(client, name: str) -> Any:
    """Recherche un candidat par nom (BM25)."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    response = candidate_tenant.query.bm25(
        query=str(name),
        query_properties=["full_name"],
        limit=5,
        return_metadata=MetadataQuery(distance=True, score=True),
    )
    response.objects = [
        obj for obj in response.objects
        if is_active_properties(obj.properties)
    ][:1]
    return response
