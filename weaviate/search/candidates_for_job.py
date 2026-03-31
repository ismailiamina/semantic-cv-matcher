"""
candidates_for_job.py
Donne un job UUID → retourne les meilleurs candidats correspondants

3 modes disponibles :
  vector  : recherche semantique via les vecteurs Ollama (par defaut)
  text    : recherche par mots-cles BM25
  hybrid  : combine vector + text
"""

import weaviate
import requests
import time
from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from collections import defaultdict
from weaviate.classes.query import MetadataQuery, HybridFusion
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

CANDIDATE_COLLECTION_NAME = "Candidate"
JOB_COLLECTION_NAME       = "Job"
TENANT_ID_FOR_CV          = "cv"
TENANT_ID_FOR_JOBS        = "job"
WEIGHTS = {
    "tech_skills":  0.27,
    "summary":      0.21,
    "prog_langs":   0.27,
    "industry":     0.15,
    "roles_title":  0.10,
}


@dataclass
class MatchResult:
    id:                str
    score:             float
    properties:        Dict[str, Any]
    individual_scores: Optional[Dict[str, float]] = field(default=None)
    search_method:     Optional[str]              = field(default=None)

    def display(self):
        """Affiche le resultat de facon lisible"""
        p = self.properties
        print(f"\n  Score global : {self.score:.3f}")
        print(f"  Nom          : {p.get('full_name', 'N/A')}")
        print(f"  Ville        : {p.get('location', 'N/A')}")
        print(f"  Experience   : {p.get('years_of_experience', 0)} ans")
        print(f"  Roles        : {', '.join(p.get('roles_held', []))}")
        langs  = p.get('programming_languages', [])
        skills = p.get('technical_skills', [])
        print(f"  Langages     : {', '.join(langs[:4])}")
        print(f"  Skills       : {', '.join(skills[:4])}{'...' if len(skills) > 4 else ''}")
        if self.individual_scores:
            s = self.individual_scores
            print(f"  Scores       : skills={s.get('tech_skills',0):.2f}"
                  f" | langs={s.get('prog_langs',0):.2f}"
                  f" | summary={s.get('summary',0):.2f}"
                  f" | industry={s.get('industry',0):.2f}"
                  f" | role={s.get('roles_title',0):.2f}")


def normalize_distance(distance) -> float:
    """Convertit une distance cosine en score 0-1"""
    if distance is None:
        return 0.0
    return 1.0 / (1.0 + float(distance))


def normalize_score(score) -> float:
    """Normalise un score BM25"""
    if score is None:
        return 0.0
    return float(score)


def connect_weaviate():
    """Connexion robuste a Weaviate avec retry"""
    for i in range(10):
        try:
            r = requests.get("http://localhost:8080/v1/.well-known/ready", timeout=3)
            if r.status_code == 200:
                break
        except Exception:
            pass
        print(f"  Weaviate pas encore pret ({i+1}/10)...")
        time.sleep(3)

    client = weaviate.connect_to_local(
        host="localhost",
        port=8080,
        grpc_port=50051,
        skip_init_checks=True
    )
    return client

def candidates_for_job_vector(
    client,
    job_uuid: str,
    limit: int = 10
) -> List[MatchResult]:
    """
    Recherche semantique — identique au projet Go&Dev
    Utilise les vecteurs du job pour trouver les candidats les plus proches

    Mapping vecteurs :
      job_tech_skills_vector  → tech_skills_vector
      job_summary_vector      → summary_vector
      job_prog_langs_vector   → prog_langs_vector
      job_industry_vector     → industry_vector
      job_title_vector        → roles_held_vector
    """

    # 1. Recuperer le job et ses vecteurs
    job_col    = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)

    job_vector_names = [
        "job_tech_skills_vector",
        "job_summary_vector",
        "job_prog_langs_vector",
        "job_industry_vector",
        "job_title_vector",
    ]

    job_obj = job_tenant.query.fetch_object_by_id(
        job_uuid, include_vector=job_vector_names
    )

    if job_obj is None:
        print(f"  Job {job_uuid} non trouve.")
        return []

    job_vectors = job_obj.vector

    # 2. Recherche dans la collection Candidate
    candidate_col    = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant = candidate_col.with_tenant(TENANT_ID_FOR_CV)

    # Mapping : vecteur job → vecteur candidat cible + cle de score
    searches = [
        ("job_tech_skills_vector", "tech_skills_vector",  "tech_skills"),
        ("job_summary_vector",     "summary_vector",      "summary"),
        ("job_prog_langs_vector",  "prog_langs_vector",   "prog_langs"),
        ("job_industry_vector",    "industry_vector",     "industry"),
        ("job_title_vector",       "roles_held_vector",   "roles_title"),
    ]

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0, "properties": None
    })

    for job_vec_name, candidate_vec_name, score_key in searches:
        vec = job_vectors.get(job_vec_name)
        if not vec:
            continue
        try:
            response = candidate_tenant.query.near_vector(
                near_vector=vec,
                target_vector=candidate_vec_name,
                limit=limit * 5,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in response.objects:
                score = normalize_distance(obj.metadata.distance)
                cid   = str(obj.uuid)
                candidate_scores[cid][score_key] = score
                if candidate_scores[cid]["properties"] is None:
                    candidate_scores[cid]["properties"] = obj.properties
        except Exception as e:
            print(f"    Erreur vecteur {score_key} : {e}")

    # 3. Score final pondere
    results = []
    for cid, scores in candidate_scores.items():
        if not scores["properties"]:
            continue
        final_score = (
            scores["tech_skills"]  * WEIGHTS["tech_skills"]  +
            scores["summary"]      * WEIGHTS["summary"]      +
            scores["prog_langs"]   * WEIGHTS["prog_langs"]   +
            scores["industry"]     * WEIGHTS["industry"]     +
            scores["roles_title"]  * WEIGHTS["roles_title"]
        )
        results.append(MatchResult(
            id=cid,
            score=final_score,
            properties=scores["properties"],
            individual_scores={k: scores[k] for k in WEIGHTS.keys()},
            search_method="vector"
        ))

    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]

def candidates_for_job_text(
    client,
    job_uuid: str,
    limit: int = 10
) -> List[MatchResult]:
    """
    Recherche par mots-cles BM25
    Extrait les termes du job et cherche des candidats avec les memes termes
    """

    # Recuperer les proprietes du job
    job_col    = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)

    job_obj = job_tenant.query.fetch_object_by_id(job_uuid)
    if job_obj is None:
        print(f"  Job {job_uuid} non trouve.")
        return []

    job_props = job_obj.properties

    candidate_col    = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant = candidate_col.with_tenant(TENANT_ID_FOR_CV)

    # Mapping : proprietes job → proprietes candidat + poids
    searches = [
        (job_props.get("technical_skills", []),  "technical_skills",  WEIGHTS["tech_skills"]),
        (job_props.get("programming_languages", []), "programming_languages", WEIGHTS["prog_langs"]),
        ([job_props.get("title", "")],            "roles_held",        WEIGHTS["roles_title"]),
        ([job_props.get("industry", "")],         "industry_primary_industries", WEIGHTS["industry"]),
        ([job_props.get("summary", "")],          "summary",           WEIGHTS["summary"]),
    ]

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0, "properties": None
    })

    for query_values, target_prop, weight_key in searches:
        if not query_values or not any(query_values):
            continue

        query_text = " ".join(str(v) for v in query_values if v)
        if not query_text.strip():
            continue

        score_key = {
            "technical_skills": "tech_skills",
            "programming_languages": "prog_langs",
            "roles_held": "roles_title",
            "industry_primary_industries": "industry",
            "summary": "summary"
        }.get(target_prop, "tech_skills")

        try:
            response = candidate_tenant.query.bm25(
                query=query_text,
                query_properties=[target_prop],
                limit=limit * 5,
                return_metadata=MetadataQuery(score=True)
            )
            scores_list = [obj.metadata.score for obj in response.objects if obj.metadata.score]
            max_score   = max(scores_list) if scores_list else 1.0

            for obj in response.objects:
                cid   = str(obj.uuid)
                score = normalize_score(obj.metadata.score) / (max_score or 1.0)
                candidate_scores[cid][score_key] = score
                if candidate_scores[cid]["properties"] is None:
                    candidate_scores[cid]["properties"] = obj.properties
        except Exception as e:
            print(f"    Erreur BM25 {target_prop} : {e}")

    # Score final pondere
    results = []
    for cid, scores in candidate_scores.items():
        if not scores["properties"]:
            continue
        final_score = (
            scores["tech_skills"]  * WEIGHTS["tech_skills"]  +
            scores["summary"]      * WEIGHTS["summary"]      +
            scores["prog_langs"]   * WEIGHTS["prog_langs"]   +
            scores["industry"]     * WEIGHTS["industry"]     +
            scores["roles_title"]  * WEIGHTS["roles_title"]
        )
        results.append(MatchResult(
            id=cid,
            score=final_score,
            properties=scores["properties"],
            individual_scores={k: scores[k] for k in WEIGHTS.keys()},
            search_method="text"
        ))

    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]

def candidates_for_job_hybrid(
    client,
    job_uuid: str,
    limit: int = 10,
    alpha: float = 0.7
) -> List[MatchResult]:
    """
    Recherche hybride — combine semantique (alpha) + BM25 (1-alpha)
    alpha=0.7 : 70% semantique + 30% mots-cles (valeur recommandee)
    alpha=1.0 : 100% semantique (= mode vector)
    alpha=0.0 : 100% mots-cles (= mode text)
    """

    # Recuperer job + vecteurs + proprietes
    job_col    = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)

    job_vector_names = [
        "job_tech_skills_vector",
        "job_summary_vector",
        "job_prog_langs_vector",
        "job_industry_vector",
        "job_title_vector",
    ]

    job_obj = job_tenant.query.fetch_object_by_id(
        job_uuid, include_vector=job_vector_names
    )

    if job_obj is None:
        print(f"  Job {job_uuid} non trouve.")
        return []

    job_vectors = job_obj.vector
    job_props   = job_obj.properties

    candidate_col    = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant = candidate_col.with_tenant(TENANT_ID_FOR_CV)

    # Mapping : (vecteur job, query text, prop candidat, vecteur candidat, cle score)
    searches = [
        (
            "job_tech_skills_vector",
            " ".join(job_props.get("technical_skills", []) or []),
            "technical_skills",
            "tech_skills_vector",
            "tech_skills"
        ),
        (
            "job_prog_langs_vector",
            " ".join(job_props.get("programming_languages", []) or []),
            "programming_languages",
            "prog_langs_vector",
            "prog_langs"
        ),
        (
            "job_summary_vector",
            job_props.get("summary", "") or "",
            "summary",
            "summary_vector",
            "summary"
        ),
        (
            "job_industry_vector",
            job_props.get("industry", "") or "",
            "industry_primary_industries",
            "industry_vector",
            "industry"
        ),
        (
            "job_title_vector",
            job_props.get("title", "") or "",
            "roles_held",
            "roles_held_vector",
            "roles_title"
        ),
    ]

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0, "properties": None
    })

    for job_vec_name, query_text, query_prop, candidate_vec_name, score_key in searches:
        vec = job_vectors.get(job_vec_name)
        if not vec or not query_text.strip():
            continue
        try:
            response = candidate_tenant.query.hybrid(
                query=query_text,
                vector=vec,
                target_vector=candidate_vec_name,
                query_properties=[query_prop],
                alpha=alpha,
                fusion_type=HybridFusion.RELATIVE_SCORE,
                limit=limit * 5,
                return_metadata=MetadataQuery(score=True)
            )
            scores_list = [obj.metadata.score for obj in response.objects if obj.metadata.score]
            max_score   = max(scores_list) if scores_list else 1.0

            for obj in response.objects:
                cid   = str(obj.uuid)
                score = normalize_score(obj.metadata.score) / (max_score or 1.0)
                candidate_scores[cid][score_key] = score
                if candidate_scores[cid]["properties"] is None:
                    candidate_scores[cid]["properties"] = obj.properties
        except Exception as e:
            print(f"    Erreur hybrid {score_key} : {e}")

    # Score final pondere
    results = []
    for cid, scores in candidate_scores.items():
        if not scores["properties"]:
            continue
        final_score = (
            scores["tech_skills"]  * WEIGHTS["tech_skills"]  +
            scores["summary"]      * WEIGHTS["summary"]      +
            scores["prog_langs"]   * WEIGHTS["prog_langs"]   +
            scores["industry"]     * WEIGHTS["industry"]     +
            scores["roles_title"]  * WEIGHTS["roles_title"]
        )
        results.append(MatchResult(
            id=cid,
            score=final_score,
            properties=scores["properties"],
            individual_scores={k: scores[k] for k in WEIGHTS.keys()},
            search_method=f"hybrid(alpha={alpha})"
        ))

    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]

def candidates_for_job(
    client,
    job_uuid: str,
    limit: int = 10,
    mode: str = "vector",
    alpha: float = 0.7
) -> List[MatchResult]:
    """
    Interface principale pour le matching candidats → job

    Args:
        client   : client Weaviate
        job_uuid : UUID du job dans Weaviate
        limit    : nombre de candidats a retourner
        mode     : "vector" | "text" | "hybrid"
        alpha    : poids semantique pour le mode hybrid (0-1)

    Returns:
        Liste de MatchResult tries par score decroissant
    """
    if mode == "vector":
        return candidates_for_job_vector(client, job_uuid, limit)
    elif mode == "text":
        return candidates_for_job_text(client, job_uuid, limit)
    elif mode == "hybrid":
        return candidates_for_job_hybrid(client, job_uuid, limit, alpha)
    else:
        print(f"Mode inconnu : {mode}. Utilisation du mode vector par defaut.")
        return candidates_for_job_vector(client, job_uuid, limit)

def display_candidates(job_props: dict, results: List[MatchResult]):
    """Affiche les resultats de matching de facon lisible"""
    print(f"\n{'=' * 60}")
    print(f"JOB : {job_props.get('title', 'N/A')} — {job_props.get('company', 'N/A')}")
    print(f"      {job_props.get('industry', 'N/A')} | {job_props.get('location', 'N/A')}")
    print(f"      Niveau requis : {job_props.get('experience_level', 'N/A')}")
    req_langs  = job_props.get('programming_languages', [])
    req_skills = job_props.get('technical_skills', [])
    print(f"      Langages      : {', '.join(req_langs[:4])}")
    print(f"      Skills        : {', '.join(req_skills[:4])}{'...' if len(req_skills) > 4 else ''}")
    print(f"{'=' * 60}")
    print(f"TOP {len(results)} CANDIDATS ({results[0].search_method if results else 'N/A'})")
    print(f"{'-' * 60}")

    for i, r in enumerate(results):
        print(f"\n  #{i+1}", end="")
        r.display()


if __name__ == "__main__":

    client = connect_weaviate()

    try:
        # Recuperer tous les jobs
        job_col    = client.collections.get(JOB_COLLECTION_NAME)
        job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)
        jobs       = job_tenant.query.fetch_objects(limit=50).objects

        if not jobs:
            print("Aucun job dans Weaviate. Lance insert_data.py d abord.")
            exit(1)

        print(f"{len(jobs)} jobs disponibles.")

        # Test sur les 3 premiers jobs avec les 3 modes
        for job in jobs[:3]:
            job_uuid  = str(job.uuid)
            job_props = job.properties

            print(f"\n\nJOB : {job_props.get('title')} — {job_props.get('company')}")

            # Mode vector
            print("\n--- MODE VECTOR ---")
            results = candidates_for_job(client, job_uuid, limit=3, mode="vector")
            display_candidates(job_props, results)

            # Mode text
            print("\n--- MODE TEXT (BM25) ---")
            results = candidates_for_job(client, job_uuid, limit=3, mode="text")
            display_candidates(job_props, results)

            # Mode hybrid
            print("\n--- MODE HYBRID (alpha=0.7) ---")
            results = candidates_for_job(client, job_uuid, limit=3, mode="hybrid", alpha=0.7)
            display_candidates(job_props, results)

    finally:
        client.close()