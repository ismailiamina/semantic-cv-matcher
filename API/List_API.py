"""
API/List_API.py
================
Endpoints FastAPI pour lire et lister candidats et offres.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import weaviate
from fastapi import APIRouter, HTTPException, Query
from typing import Optional
from config.settings import *
from weaviate_db.List import (
    get_all_candidates,
    get_all_jobs,
    get_candidate_by_id,
    get_job_by_id,
    get_candidates_names_and_ids,
    get_jobs_titles_and_ids,
    get_candidates_by_company,
    get_jobs_by_company,
    count_candidates,
    count_jobs,
    get_global_stats,
)

router = APIRouter()


# ─── CONNEXION ────────────────────────────────────────────────────────

def get_client():
    return weaviate.connect_to_local(
        host=WEAVIATE_HOST,
        port=WEAVIATE_PORT,
        grpc_port=WEAVIATE_GRPC_PORT,
        skip_init_checks=True
    )


# ─── ENDPOINTS CANDIDATS ──────────────────────────────────────────────

@router.get("/candidates/")
def list_candidates(
    limit: int = Query(500, description="Nombre maximum de candidats"),
    company: Optional[str] = Query(None, description="Filtrer par entreprise source")
):
    """Retourne la liste de tous les candidats."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            if company:
                results = get_candidates_by_company(client, company, limit)
            else:
                results = get_all_candidates(client, limit)
            return {"total": len(results), "candidates": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/candidates/names/")
def list_candidates_names(
    limit: int = Query(500, description="Nombre maximum de candidats")
):
    """
    Retourne uniquement nom + UUID + annees exp + entreprise.
    Utilise pour peupler les selectbox dans Streamlit et le CRM.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            results = get_candidates_names_and_ids(client, limit)
            return {"total": len(results), "candidates": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/candidates/count/")
def count_candidates_endpoint():
    """Retourne le nombre total de candidats en base."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            total = count_candidates(client)
            return {"total": total}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/candidates/{candidate_id}")
def get_candidate(candidate_id: str):
    """Retourne un candidat complet par UUID."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = get_candidate_by_id(client, candidate_id)
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Candidat '{candidate_id}' non trouve"
                )
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── ENDPOINTS OFFRES ─────────────────────────────────────────────────

@router.get("/jobs/")
def list_jobs(
    limit: int = Query(500, description="Nombre maximum d'offres"),
    company: Optional[str] = Query(None, description="Filtrer par entreprise")
):
    """Retourne la liste de toutes les offres."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            if company:
                results = get_jobs_by_company(client, company, limit)
            else:
                results = get_all_jobs(client, limit)
            return {"total": len(results), "jobs": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/titles/")
def list_jobs_titles(
    limit: int = Query(500, description="Nombre maximum d'offres")
):
    """
    Retourne uniquement titre + UUID + entreprise + niveau.
    Utilise pour peupler les selectbox dans Streamlit et le CRM.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            results = get_jobs_titles_and_ids(client, limit)
            return {"total": len(results), "jobs": results}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/count/")
def count_jobs_endpoint():
    """Retourne le nombre total d'offres en base."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            total = count_jobs(client)
            return {"total": total}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/jobs/{job_id}")
def get_job(job_id: str):
    """Retourne une offre complete par UUID."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = get_job_by_id(client, job_id)
            if not result:
                raise HTTPException(
                    status_code=404,
                    detail=f"Offre '{job_id}' non trouvee"
                )
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── ENDPOINT KPIs ────────────────────────────────────────────────────

@router.get("/stats/")
def global_stats():
    """
    Retourne les KPIs globaux pour le dashboard.
    Total candidats, total offres, repartition par entreprise et seniorite.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            stats = get_global_stats(client)
            return stats
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))