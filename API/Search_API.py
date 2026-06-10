"""
API/Search_API.py
==================
Endpoints FastAPI pour le matching et la recherche.
Architecture identique au projet existant CV-SCANNER-IA.
- candidates for job (3 modes)
- jobs for candidate (3 modes)
- keywords search
- advanced search
- reranker VoyageAI (bouton optionnel)
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import os
import json
import weaviate
import voyageai
from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, List, Dict, Any
from config.settings import *
from weaviate_db.search.candidates_for_job import (
    search_candidate_for_job_by_id,
    search_candidate_by_name,
    MatchResult,
    is_active_properties,
)
from weaviate_db.search.jobs_for_candidate import (
    search_job_for_candidate_by_id,
    search_job_by_title,
)
from weaviate_db.search.keywords import (
    search_candidates_by_tech_skills,
    search_candidates_by_prog_langs,
    search_candidates_by_location,
    search_candidates_by_roles,
    search_candidates_by_experience,
    search_candidates_by_industry,
)
from weaviate_db.search.advanced_search import advanced_search

router = APIRouter()


# ─── CONNEXION ────────────────────────────────────────────────────────

def get_client():
    return weaviate.connect_to_local(
        host=WEAVIATE_HOST,
        port=WEAVIATE_PORT,
        grpc_port=WEAVIATE_GRPC_PORT,
        skip_init_checks=True
    )


# ─── SERIALISATION ────────────────────────────────────────────────────

def serialize_result(r: MatchResult) -> dict:
    return {
        "uuid":              r.id,
        "score":             round(r.score, 4),
        "search_method":     r.search_method,
        "individual_scores": r.individual_scores or {},
        "properties":        r.properties,
    }


# ─── CANDIDATES FOR JOB ───────────────────────────────────────────────

@router.get("/search/candidates-for-job/")
def search_candidates_for_job(
    job_uuid:    str = Query(..., description="UUID de l'offre"),
    mode:        str = Query("hybride", description="vecteur | texte | hybride"),
    limit:       int = Query(10, description="Nombre de candidats a retourner")
):
    """
    Recherche les meilleurs candidats pour une offre donnee.
    3 modes : vecteur / texte / hybride (defaut).
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            results = search_candidate_for_job_by_id(client, job_uuid, mode, limit)
            return {
                "job_uuid": job_uuid,
                "mode":     mode,
                "total":    len(results),
                "results":  [serialize_result(r) for r in results]
            }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── JOBS FOR CANDIDATE ───────────────────────────────────────────────

@router.get("/search/jobs-for-candidate/")
def search_jobs_for_candidate(
    candidate_uuid: str = Query(..., description="UUID du candidat"),
    mode:           str = Query("hybride", description="vecteur | texte | hybride"),
    limit:          int = Query(10, description="Nombre d'offres a retourner")
):
    """
    Recherche les meilleures offres pour un candidat donne.
    3 modes : vecteur / texte / hybride (defaut).
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            results = search_job_for_candidate_by_id(client, candidate_uuid, mode, limit)
            return {
                "candidate_uuid": candidate_uuid,
                "mode":           mode,
                "total":          len(results),
                "results":        [serialize_result(r) for r in results]
            }
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── KEYWORDS SEARCH ──────────────────────────────────────────────────

@router.get("/search/by-skills/")
def search_by_skills(
    skills: List[str] = Query(..., description="Liste de competences"),
    limit:  int       = Query(10)
):
    """Recherche candidats par competences techniques."""
    try:
        with get_client() as client:
            results = search_candidates_by_tech_skills(client, skills, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/by-languages/")
def search_by_languages(
    langs: List[str] = Query(..., description="Liste de langages"),
    limit: int       = Query(10)
):
    """Recherche candidats par langages de programmation."""
    try:
        with get_client() as client:
            results = search_candidates_by_prog_langs(client, langs, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/by-location/")
def search_by_location(
    loc:   List[str] = Query(..., description="Villes ou regions"),
    limit: int       = Query(10)
):
    """Recherche candidats par localisation."""
    try:
        with get_client() as client:
            results = search_candidates_by_location(client, loc, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/by-role/")
def search_by_role(
    role:  str = Query(..., description="Titre du poste"),
    limit: int = Query(10)
):
    """Recherche candidats par role."""
    try:
        with get_client() as client:
            results = search_candidates_by_roles(client, role, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/by-experience/")
def search_by_experience(
    min:   int = Query(..., description="Annees min"),
    max:   int = Query(..., description="Annees max"),
    limit: int = Query(10)
):
    """Recherche candidats par tranche d'experience."""
    try:
        with get_client() as client:
            results = search_candidates_by_experience(client, min, max, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/by-industry/")
def search_by_industry(
    industry: List[str] = Query(..., description="Secteurs d'activite"),
    limit:    int       = Query(10)
):
    """Recherche candidats par industrie."""
    try:
        with get_client() as client:
            results = search_candidates_by_industry(client, industry, limit)
            return {"total": len(results), "results": [serialize_result(r) for r in results]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── ADVANCED SEARCH ──────────────────────────────────────────────────

class AdvancedSearchRequest(BaseModel):
    loc:                    Optional[List[str]]       = []
    exclude:                Optional[str]             = None
    skills:                 Optional[List[str]]       = []
    programming_languages:  Optional[List[str]]       = []
    role:                   Optional[str]             = None
    experience:             Optional[Dict[str, int]]  = {}
    industry:               Optional[List[str]]       = []
    limit:                  Optional[int]             = 20


@router.post("/search/advanced/")
def advanced_search_endpoint(request: AdvancedSearchRequest):
    """
    Recherche avancee multi-criteres ponderee.
    Utilisee par le chatbot de recherche libre dans Streamlit.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            results = advanced_search(client, request.model_dump())
            return {
                "total":   len(results),
                "results": [serialize_result(r) for r in results[:request.limit]]
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur advanced search : {str(e)}")


# ─── RERANKER VOYAGEAI ────────────────────────────────────────────────

class RerankRequest(BaseModel):
    query:      str
    candidates: List[Dict[str, Any]]
    top_k:      Optional[int] = 10


@router.post("/search/rerank/")
def rerank_endpoint(request: RerankRequest):
    api_key = VOYAGEAI_API_KEY
    if not api_key:
        raise HTTPException(status_code=500, detail="VOYAGEAI_APIKEY manquante dans .env")

    try:
        vo = voyageai.Client(api_key=api_key)

        active_candidates = [
            c for c in request.candidates
            if is_active_properties(c.get("properties", {}))
        ]

        documents = []
        for c in active_candidates:
            props       = c.get("properties", {})
            tech_skills = props.get("technical_skills", []) or []
            prog_langs  = props.get("programming_languages", []) or []
            roles       = props.get("roles_held", []) or []
            summary     = props.get("summary", "") or ""
            certs       = props.get("certifications", []) or []
            senio       = props.get("seniority_technologies", []) or []
            years       = props.get("years_of_experience", 0) or 0

            if years >= 10:   level = "Expert"
            elif years >= 6:  level = "Senior"
            elif years >= 4:  level = "Confirme"
            elif years >= 2:  level = "Medior"
            else:             level = "Junior"

            senio_parts = [
                f"{s.get('technology','')} ({s.get('level','')})"
                for s in senio[:8] if isinstance(s, dict)
            ]

            parts = [
                f"Candidat : {props.get('full_name', '')}",
                f"Experience : {years} ans — niveau {level}",
                f"Secteur : conseil et services informatiques, ESN",
                f"Roles : {', '.join(roles[:4]) if roles else 'Non precise'}",
                f"Competences : {', '.join(tech_skills[:12]) if tech_skills else 'Non precise'}",
                f"Langages : {', '.join(prog_langs[:6]) if prog_langs else 'Non precise'}",
            ]
            if certs:
                parts.append(f"Certifications : {', '.join(certs[:4])}")
            if senio_parts:
                parts.append(f"Niveau par technologie : {', '.join(senio_parts)}")
            if summary:
                parts.append(f"Profil : {summary[:500]}")

            documents.append(". ".join(parts))

        if not documents:
            return {"total": 0, "results": []}

        reranking = vo.rerank(
            query=request.query,
            documents=documents,
            model="rerank-2",
            top_k=request.top_k
        )

        reranked_results = []
        for item in reranking.results:
            original = active_candidates[item.index]
            reranked_results.append({
                "uuid":         original.get("uuid", ""),
                "score":        original.get("score", 0),
                "rerank_score": round(item.relevance_score, 4),
                "search_method": "rerank-2",
                "individual_scores": original.get("individual_scores", {}),
                "properties":   original.get("properties", {}),
            })

        return {"total": len(reranked_results), "results": reranked_results}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur reranking : {str(e)}")

# ─── SEARCH BY NAME / TITLE ───────────────────────────────────────────

@router.get("/search/candidate-by-name/")
def search_candidate_name(
    name:  str = Query(..., description="Nom du candidat"),
):
    """Recherche un candidat par nom."""
    try:
        with get_client() as client:
            result = search_candidate_by_name(client, name)
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/search/job-by-title/")
def search_job_title(
    title: str = Query(..., description="Titre de l'offre"),
):
    """Recherche une offre par titre."""
    try:
        with get_client() as client:
            result = search_job_by_title(client, title)
            return result
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# ─── LLM ENDPOINTS ────────────────────────────────────────────────────

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from API.llm_utils import explain_match_llm, analyze_gap_llm

class LLMRequest(BaseModel):
    job_props:  Dict[str, Any]
    cand_props: Dict[str, Any]

@router.post("/llm/explain/")
def llm_explain(request: LLMRequest):
    try:
        result = explain_match_llm(request.job_props, request.cand_props)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.post("/llm/gap/")
def llm_gap(request: LLMRequest):
    try:
        result = analyze_gap_llm(request.cand_props, request.job_props)
        return {"result": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
class ParseQueryRequest(BaseModel):
    query: str

@router.post("/llm/parse-query/")
def parse_query_endpoint(request: ParseQueryRequest):
    import requests as req_lib
    mistral_key = os.getenv("MISTRAL_API_KEY", "")
    if not mistral_key:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY manquante")
    try:
        prompt = f"""Tu es un assistant de recherche RH.
Analyse cette requete et extrais les criteres. Retourne UNIQUEMENT un JSON valide sans backticks.

Champs possibles :
{{"loc": ["ville"], "skills": ["skill1"], "programming_languages": ["lang1"],
 "role": "titre", "experience": {{"min": X, "max": Y}}, "industry": ["secteur"]}}

Requete : {request.query}
JSON :"""
        resp = req_lib.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {mistral_key}"},
            json={"model": "mistral-small-latest",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 300, "temperature": 0.1},
            timeout=30
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        criteria = json.loads(raw)
        return {"criteria": criteria}
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"Impossible de parser la requete : {str(e)}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
