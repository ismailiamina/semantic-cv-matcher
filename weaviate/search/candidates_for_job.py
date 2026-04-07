"""
Architecture :
    - vector  : near_vector (vecteurs du job → vecteurs candidat)
    - text    : near_text  (texte du job → vecteur candidat)
    - hybrid  : hybrid BM25 + vector

  + Mistral Small re-rank sur le top-N retourné par Weaviate
    Mistral comprend les équivalences métier :
    "outillage QA" = Selenium/Cypress, "processus DevOps" = CI/CD/Jenkins

  + Jaccard fusion avec le score Mistral pour skills/langs
"""

import os
import json
import time
import requests
import weaviate
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional
from collections import defaultdict
from weaviate.classes.query import MetadataQuery, HybridFusion
from pathlib import Path
from dotenv import load_dotenv

_p = Path(__file__).resolve()
for _ in range(6):
    _p = _p.parent
    if (_p / ".env").exists():
        load_dotenv(dotenv_path=_p / ".env")
        break

CANDIDATE_COLLECTION_NAME = "Candidate"
JOB_COLLECTION_NAME       = "Job"
TENANT_ID_FOR_CV          = "cv"
TENANT_ID_FOR_JOBS        = "job"

MISTRAL_MODEL   = "mistral-small-latest"
MISTRAL_API_URL = "https://api.mistral.ai/v1/chat/completions"

# Poids modifiables par les sliders Streamlit
WEIGHTS = {
    "tech_skills":  0.25,
    "summary":      0.20,
    "prog_langs":   0.20,
    "industry":     0.15,
    "roles_title":  0.10,
    "work_experience": 0.07,
    "projects":     0.03,
}

SENIORITY_COMPAT = {
    "Junior":   {"Junior":1.0,"Medior":0.6,"Confirmé":0.3,"Senior":0.1,"Expert":0.0},
    "Medior":   {"Junior":0.5,"Medior":1.0,"Confirmé":0.8,"Senior":0.5,"Expert":0.2},
    "Confirmé": {"Junior":0.1,"Medior":0.7,"Confirmé":1.0,"Senior":0.8,"Expert":0.4},
    "Senior":   {"Junior":0.0,"Medior":0.3,"Confirmé":0.6,"Senior":1.0,"Expert":0.8},
    "Expert":   {"Junior":0.0,"Medior":0.1,"Confirmé":0.3,"Senior":0.8,"Expert":1.0},
}


@dataclass
class SearchConfig:
    score_threshold: float = 0.0
    alpha: float = 0.7


@dataclass
class MatchResult:
    id:               str
    score:            float
    properties:       Dict[str, Any]
    individual_scores: Optional[Dict[str, float]] = field(default=None)
    distance:         Optional[float]             = field(default=None)
    search_method:    Optional[str]               = field(default=None)
    rerank_score:     Optional[float]             = field(default=None)
    vector_type:      Optional[str]               = field(default=None)
    search_type:      Optional[str]               = field(default=None)
    explanation:      Optional[str]               = field(default=None)



def connect_weaviate():
    for i in range(10):
        try:
            r = requests.get("http://localhost:8080/v1/.well-known/ready", timeout=3)
            if r.status_code == 200:
                break
        except Exception:
            pass
        print(f"  Weaviate pas encore pret ({i+1}/10)...")
        time.sleep(3)
    return weaviate.connect_to_local(
        host="localhost", port=8080, grpc_port=50051, skip_init_checks=True
    )


def normalize_distance(distance) -> float:
    if distance is None: return 0.0
    return 1.0 / (1.0 + float(distance))

def normalize_score(score) -> float:
    if score is None: return 0.0
    return max(0.0, min(1.0, float(score)))


def jaccard(required: list, candidate: list) -> float:
    """% des éléments requis que le candidat possède (matching partiel)."""
    if not required:  return 1.0
    if not candidate: return 0.0
    req  = [str(x).lower().strip() for x in required  if x]
    cand = [str(x).lower().strip() for x in candidate if x]
    matches = sum(
        1 for r in req
        if any(r in c or c in r or (len(r)>3 and len(c)>3 and r[:5]==c[:5])
               for c in cand)
    )
    return round(matches / len(req), 3)


def get_candidate_level(props: dict) -> str:
    st = props.get("seniority_technologies", [])
    if st:
        f = st[0]
        if isinstance(f, dict):
            lv = f.get("level", "")
            if lv: return lv
        elif isinstance(f, str) and "(" in f:
            return f[f.rfind("(")+1:f.rfind(")")].strip()
    pred = props.get("career_trajectory_predicted_profile", "") or ""
    for l in ["Expert","Senior","Confirmé","Medior","Junior"]:
        if l.lower() in pred.lower(): return l
    y = int(props.get("years_of_experience") or 0)
    if y >= 10: return "Expert"
    if y >= 6:  return "Senior"
    if y >= 4:  return "Confirmé"
    if y >= 2:  return "Medior"
    return "Junior"


def compute_penalty(job_props: dict, cand_props: dict) -> float:
    job_level  = job_props.get("experience_level", "") or ""
    cand_level = get_candidate_level(cand_props)
    s_mult     = SENIORITY_COMPAT.get(job_level, {}).get(cand_level, 0.5) if job_level else 0.8
    yr  = int(job_props.get("years_of_experience_required") or 0)
    yc  = int(cand_props.get("years_of_experience") or 0)
    if yr == 0:
        e_mult = 1.0
    else:
        e_mult = max(0.1, min(1.0, yc / yr))
    return (s_mult * 0.6) + (e_mult * 0.4)



SCORING_PROMPT = """Tu es un recruteur IT expert. Analyse ce candidat pour cette offre et donne des scores precis.

OFFRE: {job_title} | Niveau:{job_level} | {job_years}ans min
Skills requis: {job_skills}
Langages requis: {job_langs}
Description: {job_desc}

CANDIDAT: {cand_years}ans experience | Niveau:{cand_level}
Skills: {cand_skills}
Langages: {cand_langs}
Experience: {cand_exp}
Projets: {cand_proj}
Profil: {cand_summary}

Reponds UNIQUEMENT en JSON valide (scores 0.0 a 1.0) :
{{"skills_match":0.0,"langs_match":0.0,"experience_fit":0.0,"seniority_fit":0.0,"profile_summary":0.0,"explanation":"..."}}

Regles :
- skills_match: % skills requis que le candidat possede (CI/CD=Jenkins/GitLab, cloud=AWS/Azure, outillage QA=Selenium/Cypress/JUnit, DevOps=CI/CD/Docker/K8s)
- langs_match: % langages requis couverts (-1 si aucun langage requis dans l offre)
- experience_fit: adequation experience et parcours (0=insuffisant, 1=parfait)
- seniority_fit: adequation niveau seniorite (0=trop junior, 1=parfait)
- profile_summary: adequation globale profil/contexte metier
- explanation: 1 phrase courte en francais"""


def call_mistral(prompt: str, api_key: str) -> dict:
    """Appel direct HTTP Mistral — sans langchain pour eviter les timeouts."""
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type":  "application/json",
    }
    payload = {
        "model":       MISTRAL_MODEL,
        "temperature": 0.0,
        "max_tokens":  300,
        "messages":    [{"role": "user", "content": prompt}],
    }

    for attempt in range(3):
        try:
            resp = requests.post(MISTRAL_API_URL, headers=headers, json=payload, timeout=60)
            resp.raise_for_status()
            text = resp.json()["choices"][0]["message"]["content"].strip()

            # Nettoyer si Mistral envoie des backticks
            if "```" in text:
                for part in text.split("```"):
                    p = part.strip().lstrip("json").strip()
                    if p.startswith("{"):
                        text = p
                        break

            scores = json.loads(text)

            for k in ["skills_match","experience_fit","seniority_fit","profile_summary"]:
                scores[k] = max(0.0, min(1.0, float(scores.get(k, 0.0))))
            lv = float(scores.get("langs_match", 0.0))
            scores["langs_match"] = lv if lv == -1 else max(0.0, min(1.0, lv))
            return scores

        except requests.exceptions.Timeout:
            print(f"    Timeout Mistral (essai {attempt+1}/3)...")
            time.sleep(3)
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 429:
                print("    Rate limit — attente 20s...")
                time.sleep(20)
            else:
                print(f"    Erreur HTTP {e.response.status_code}")
                break
        except (json.JSONDecodeError, KeyError):
            print("    JSON invalide retourne par Mistral")
            break
        except Exception as e:
            print(f"    Erreur : {str(e)[:60]}")
            break

    return {"skills_match":0.0,"langs_match":0.0,"experience_fit":0.0,
            "seniority_fit":0.0,"profile_summary":0.0,"explanation":"Erreur scoring"}


def rerank_with_mistral(
    job_props: dict,
    match_results: List[MatchResult],
    api_key: str
) -> List[MatchResult]:
    """
    Re-classe le top Weaviate avec Mistral Small.
    Fusionne le score Weaviate (20%) + score Mistral (80%).
    """
    job_skills = job_props.get("technical_skills", []) or []
    job_langs  = job_props.get("programming_languages", []) or []

    reranked = []
    for i, result in enumerate(match_results):
        props      = result.properties
        cand_level = get_candidate_level(props)

        prompt = SCORING_PROMPT.format(
            job_title   = job_props.get("title", ""),
            job_level   = job_props.get("experience_level", "non précisé"),
            job_years   = job_props.get("years_of_experience_required", 0),
            job_skills  = ", ".join(job_skills[:8]) or "—",
            job_langs   = ", ".join(job_langs[:5])  or "aucun",
            job_desc    = (job_props.get("job_description", "") or "")[:400],
            cand_years  = props.get("years_of_experience", 0),
            cand_level  = cand_level,
            cand_skills = ", ".join(props.get("technical_skills", [])[:8] or []) or "—",
            cand_langs  = ", ".join(props.get("programming_languages", [])[:5] or []) or "aucun",
            cand_exp    = (props.get("work_experience", "") or "")[:300],
            cand_proj   = (props.get("projects", "") or "")[:200],
            cand_summary= (props.get("summary", "") or "")[:300],
        )

        mistral = call_mistral(prompt, api_key)

        # Jaccard exact pour skills et langs
        j_skills = jaccard(job_skills, props.get("technical_skills", []) or [])
        if job_langs:
            j_langs = jaccard(job_langs, props.get("programming_languages", []) or [])
        else:
            j_langs = None

        # Fusion Jaccard (précision) + Mistral (compréhension contextuelle)
        final_skills = (j_skills * 0.5 + mistral.get("skills_match", 0) * 0.5)
        if j_langs is not None:
            lv = mistral.get("langs_match", 0)
            final_langs = (j_langs * 0.5 + (lv if lv >= 0 else 0) * 0.5)
        else:
            final_langs = None

        langs_val = final_langs if final_langs is not None else -1
        w_skills  = WEIGHTS["tech_skills"] + WEIGHTS["prog_langs"] if langs_val == -1 else WEIGHTS["tech_skills"]
        w_langs   = 0.0 if langs_val == -1 else WEIGHTS["prog_langs"]

        mistral_score = (
            final_skills                        * w_skills +
            (langs_val if langs_val >= 0 else 0) * w_langs +
            mistral.get("experience_fit", 0)    * WEIGHTS["work_experience"] +
            mistral.get("seniority_fit", 0)     * WEIGHTS["roles_title"] +
            mistral.get("profile_summary", 0)   * WEIGHTS["summary"]
        )

        # Pénalité séniorité/expérience
        penalty = compute_penalty(job_props, props)

        # Score final = Weaviate 20% + Mistral 80%, avec pénalité
        final_score = round(
            (result.score * 0.20 + mistral_score * 0.80) * penalty,
            4
        )

        individual = {
            "tech_skills":  final_skills,
            "prog_langs":   langs_val,
            "summary":      mistral.get("profile_summary", 0.0),
            "roles_title":  mistral.get("seniority_fit",   0.0),
            "industry":     mistral.get("experience_fit",  0.0),
        }

        reranked.append(MatchResult(
            id=result.id,
            score=final_score,
            properties=props,
            individual_scores=individual,
            search_method=result.search_method + "+mistral",
            rerank_score=mistral_score,
            explanation=mistral.get("explanation", "")
        ))

        if i < len(match_results) - 1:
            time.sleep(0.5)

    reranked.sort(key=lambda x: x.score, reverse=True)
    return reranked

def _execute_vector_search(
    client, job_vectors: Dict[str, Any], limit: int, config: SearchConfig
) -> List[MatchResult]:
    """Recherche sémantique via near_vector — identique au fichier référence."""

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0,
        "work_experience": 0.0, "projects": 0.0,
        "final_score": 0.0, "properties": None
    })

    query_limit = limit * 5

    searches = [
        ("job_tech_skills_vector",  "tech_skills_vector",      "tech_skills"),
        ("job_summary_vector",      "summary_vector",          "summary"),
        ("job_prog_langs_vector",   "prog_langs_vector",       "prog_langs"),
        ("job_industry_vector",     "industry_vector",         "industry"),
        ("job_title_vector",        "roles_held_vector",       "roles_title"),
        ("job_description_vector",  "work_experience_vector",  "work_experience"),
        ("job_description_vector",  "projects_vector",         "projects"),
    ]

    for job_vec_name, cand_vec_name, score_key in searches:
        vec = job_vectors.get(job_vec_name)
        if not vec: continue
        try:
            print(f"  Vector search: {score_key}...")
            resp = candidate_tenant.query.near_vector(
                near_vector=vec,
                target_vector=cand_vec_name,
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in resp.objects:
                candidate_scores[obj.uuid][score_key] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur {score_key}: {e}")

    # Score pondéré
    for uuid, sc in candidate_scores.items():
        sc["final_score"] = (
            sc["tech_skills"]    * WEIGHTS["tech_skills"]    +
            sc["summary"]        * WEIGHTS["summary"]        +
            sc["prog_langs"]     * WEIGHTS["prog_langs"]     +
            sc["industry"]       * WEIGHTS["industry"]       +
            sc["roles_title"]    * WEIGHTS["roles_title"]    +
            sc["work_experience"]* WEIGHTS["work_experience"]+
            sc["projects"]       * WEIGHTS["projects"]
        )

    results = [
        MatchResult(
            id=str(uuid), score=sc["final_score"], properties=sc["properties"],
            individual_scores={k: sc[k] for k in WEIGHTS},
            search_method="vector_search"
        )
        for uuid, sc in candidate_scores.items()
        if sc["final_score"] >= config.score_threshold and sc["properties"]
    ]
    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]

def _execute_text_search(
    client, job_properties: Dict[str, Any], limit: int, config: SearchConfig
) -> List[MatchResult]:
    """Recherche sémantique via near_text — identique au fichier référence."""

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0,
        "final_score": 0.0, "properties": None
    })

    query_limit = limit * 5

    def _near_text(query: str, target_vector: str, score_key: str):
        if not query.strip(): return
        try:
            print(f"  Near_text search: {score_key}...")
            resp = candidate_tenant.query.near_text(
                query=query,
                target_vector=target_vector,
                limit=query_limit,
                return_metadata=MetadataQuery(distance=True)
            )
            for obj in resp.objects:
                candidate_scores[obj.uuid][score_key] = normalize_distance(obj.metadata.distance)
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur near_text {score_key}: {e}")

    tech_skills  = " ".join(job_properties.get("technical_skills", []) or [])
    prog_langs   = " ".join(job_properties.get("programming_languages", []) or [])
    summary      = job_properties.get("summary", "") or ""
    industry     = job_properties.get("industry", "") or ""
    title        = job_properties.get("title", "") or ""

    _near_text(tech_skills, "tech_skills_vector",  "tech_skills")
    _near_text(summary,     "summary_vector",       "summary")
    _near_text(prog_langs,  "prog_langs_vector",    "prog_langs")
    _near_text(industry,    "industry_vector",      "industry")
    _near_text(title,       "roles_held_vector",    "roles_title")

    for uuid, sc in candidate_scores.items():
        sc["final_score"] = (
            sc["tech_skills"] * WEIGHTS["tech_skills"] +
            sc["summary"]     * WEIGHTS["summary"]     +
            sc["prog_langs"]  * WEIGHTS["prog_langs"]  +
            sc["industry"]    * WEIGHTS["industry"]    +
            sc["roles_title"] * WEIGHTS["roles_title"]
        )

    results = [
        MatchResult(
            id=str(uuid), score=sc["final_score"], properties=sc["properties"],
            individual_scores={k: sc[k] for k in ["tech_skills","summary","prog_langs","industry","roles_title"]},
            search_method="text_search"
        )
        for uuid, sc in candidate_scores.items()
        if sc["final_score"] >= config.score_threshold and sc["properties"]
    ]
    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]


def _execute_hybrid_search(
    client,
    job_properties: Dict[str, Any],
    job_vectors: Dict[str, Any],
    limit: int,
    config: SearchConfig
) -> List[MatchResult]:
    """Recherche hybride BM25 + vecteurs — identique au fichier référence."""

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant     = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    candidate_scores = defaultdict(lambda: {
        "tech_skills": 0.0, "summary": 0.0, "prog_langs": 0.0,
        "industry": 0.0, "roles_title": 0.0,
        "final_score": 0.0, "properties": None
    })

    query_limit = limit * 5

    searches = [
        (" ".join(job_properties.get("technical_skills", []) or []),
         job_vectors.get("job_tech_skills_vector"),
         "technical_skills", "tech_skills_vector", "tech_skills"),
        (job_properties.get("summary", "") or "",
         job_vectors.get("job_summary_vector"),
         "summary", "summary_vector", "summary"),
        (" ".join(job_properties.get("programming_languages", []) or []),
         job_vectors.get("job_prog_langs_vector"),
         "programming_languages", "prog_langs_vector", "prog_langs"),
        (job_properties.get("industry", "") or "",
         job_vectors.get("job_industry_vector"),
         "industry_primary_industries", "industry_vector", "industry"),
        (job_properties.get("title", "") or "",
         job_vectors.get("job_title_vector"),
         "roles_held", "roles_held_vector", "roles_title"),
    ]

    for query, vec, query_prop, target_vec, score_key in searches:
        if not query.strip() or vec is None: continue
        try:
            print(f"  Hybrid search: {score_key}...")
            resp = candidate_tenant.query.hybrid(
                query=query,
                vector=vec,
                target_vector=target_vec,
                query_properties=[query_prop],
                alpha=config.alpha,
                fusion_type=HybridFusion.RELATIVE_SCORE,
                limit=query_limit,
                return_metadata=MetadataQuery(score=True)
            )
            scores_list = [o.metadata.score for o in resp.objects if o.metadata.score]
            max_score   = max(scores_list) if scores_list else 1.0
            for obj in resp.objects:
                score = normalize_score(obj.metadata.score) / (max_score or 1.0)
                candidate_scores[obj.uuid][score_key] = score
                if candidate_scores[obj.uuid]["properties"] is None:
                    candidate_scores[obj.uuid]["properties"] = obj.properties
        except Exception as e:
            print(f"  Erreur hybrid {score_key}: {e}")

    for uuid, sc in candidate_scores.items():
        sc["final_score"] = (
            sc["tech_skills"] * WEIGHTS["tech_skills"] +
            sc["summary"]     * WEIGHTS["summary"]     +
            sc["prog_langs"]  * WEIGHTS["prog_langs"]  +
            sc["industry"]    * WEIGHTS["industry"]    +
            sc["roles_title"] * WEIGHTS["roles_title"]
        )

    results = [
        MatchResult(
            id=str(uuid), score=sc["final_score"], properties=sc["properties"],
            individual_scores={k: sc[k] for k in ["tech_skills","summary","prog_langs","industry","roles_title"]},
            search_method="hybrid_search"
        )
        for uuid, sc in candidate_scores.items()
        if sc["final_score"] >= config.score_threshold and sc["properties"]
    ]
    results.sort(key=lambda x: x.score, reverse=True)
    return results[:limit]



def search_candidate_for_job_by_id(
    client,
    job_uuid: str,
    search_mode: Optional[str],
    limit: int
) -> List[MatchResult]:
    """
    Identique à search_candidate_for_job_by_id du fichier référence.
    Ajoute Mistral re-rank sur le résultat Weaviate.
    """
    job_collection        = client.collections.get(JOB_COLLECTION_NAME)
    job_collection_tenant = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    vector_names = [
        "job_description_vector", "job_industry_vector", "job_prog_langs_vector",
        "job_summary_vector",     "job_tech_skills_vector", "job_title_vector"
    ]

    job_obj = job_collection_tenant.query.fetch_object_by_id(
        job_uuid, include_vector=vector_names
    )

    if job_obj is None:
        raise ValueError(f"Aucun job avec UUID {job_uuid}")

    job_properties = job_obj.properties
    job_vectors = {
        "job_description_vector":  job_obj.vector["job_description_vector"],
        "job_industry_vector":     job_obj.vector["job_industry_vector"],
        "job_prog_langs_vector":   job_obj.vector["job_prog_langs_vector"],
        "job_summary_vector":      job_obj.vector["job_summary_vector"],
        "job_tech_skills_vector":  job_obj.vector["job_tech_skills_vector"],
        "job_title_vector":        job_obj.vector["job_title_vector"],
    }
    config = SearchConfig()

    # Weaviate search 
    pool_limit = min(limit * 2, 12)  # pool 2x pour Mistral re-rank

    if search_mode == "vecteur":
        weaviate_results = _execute_vector_search(client, job_vectors, pool_limit, config)
    elif search_mode == "texte":
        weaviate_results = _execute_text_search(client, job_properties, pool_limit, config)
    else:  # hybride (défaut)
        weaviate_results = _execute_hybrid_search(
            client, job_properties, job_vectors, pool_limit, config
        )

    # Mistral re-rank
    api_key = os.getenv("MISTRAL_API_KEY")
    if api_key and weaviate_results:
        print(f"\n  Mistral Small : re-classement de {len(weaviate_results)} candidats...")
        return rerank_with_mistral(job_properties, weaviate_results, api_key)[:limit]
    else:
        if not api_key:
            print("  MISTRAL_API_KEY manquante — resultats Weaviate sans re-rank")
        return weaviate_results[:limit]


def search_candidate_by_name(client, name: str):
    """Recherche par nom — identique au fichier référence."""
    candidate_collection        = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_collection_tenant = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    return candidate_collection_tenant.query.bm25(
        query=str(name),
        query_properties=["full_name"],
        limit=1,
        return_metadata=MetadataQuery(distance=True, score=True),
    )


# Alias pour compatibilité Streamlit
def candidates_for_job(client, job_uuid, limit=10, mode="hybride", alpha=0.7):
    return search_candidate_for_job_by_id(client, job_uuid, mode, limit)

def candidates_for_job_vector(client, job_uuid, limit=10):
    return search_candidate_for_job_by_id(client, job_uuid, "vecteur", limit)

def candidates_for_job_text(client, job_uuid, limit=10):
    return search_candidate_for_job_by_id(client, job_uuid, "texte", limit)

def candidates_for_job_hybrid(client, job_uuid, limit=10, alpha=0.7):
    return search_candidate_for_job_by_id(client, job_uuid, "hybride", limit)


def display_results(job_props: dict, results: List[MatchResult]):
    print(f"\n{'='*60}")
    print(f"JOB    : {job_props.get('title')} — {job_props.get('company')}")
    print(f"Niveau : {job_props.get('experience_level')} | {job_props.get('years_of_experience_required',0)} ans min.")
    print(f"Skills : {', '.join((job_props.get('technical_skills') or [])[:5])}")
    print(f"Langs  : {', '.join((job_props.get('programming_languages') or [])[:4]) or '—'}")
    print(f"{'='*60}")
    if not results:
        print("  Aucun candidat trouve.")
        return
    for i, r in enumerate(results):
        p = r.properties
        s = r.individual_scores or {}
        def fmt(v): return "N/A" if v == -1 else f"{v:.0%}"
        print(f"\n  #{i+1} {p.get('full_name','?')} — Score: {r.score:.0%}")
        print(f"     {p.get('years_of_experience',0)} ans | {get_candidate_level(p)}")
        print(f"     Skills: {fmt(s.get('tech_skills',0))} | Langs: {fmt(s.get('prog_langs',0))} | Exp: {fmt(s.get('industry',0))}")
        if r.explanation:
            print(f"     → {r.explanation[:100]}")


if __name__ == "__main__":
    client = connect_weaviate()
    try:
        job_col    = client.collections.get(JOB_COLLECTION_NAME)
        job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)
        jobs       = job_tenant.query.fetch_objects(limit=50).objects
        if not jobs:
            print("Aucun job. Lance insert_data.py d abord.")
        else:
            print(f"{len(jobs)} jobs disponibles.\n")
            for job in jobs[:2]:
                results = search_candidate_for_job_by_id(
                    client, str(job.uuid), "hybride", limit=5
                )
                display_results(job.properties, results)
    finally:
        client.close()