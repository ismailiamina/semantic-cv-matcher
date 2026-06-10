"""
API/CRUD_API.py
================
Endpoints FastAPI pour insert / delete candidats et offres.
Architecture identique au projet existant CV-SCANNER-IA.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import weaviate
from fastapi import APIRouter, HTTPException, UploadFile, File
from pydantic import BaseModel
from typing import Optional
from config.settings import *
from weaviate_db.CRUD import (
    add_candidate,
    add_job,
    add_batch_candidates,
    add_batch_jobs,
    delete_candidate_by_id,
    delete_job_by_id,
    transform_cv_data,
    transform_job_data,
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


# ─── MODELES PYDANTIC ─────────────────────────────────────────────────

class CandidatePayload(BaseModel):
    full_name:              Optional[str] = ""
    email:                  Optional[str] = ""
    phone:                  Optional[str] = ""
    location:               Optional[str] = ""
    years_of_experience:    Optional[int] = 0
    linkedin:               Optional[str] = ""
    github:                 Optional[str] = ""
    roles_held:             Optional[list] = []
    programming_languages:  Optional[list] = []
    technical_skills:       Optional[list] = []
    spoken_languages:       Optional[list] = []
    certifications:         Optional[list] = []
    seniority_technologies:          Optional[list] = []
    seniority_programming_languages: Optional[list] = []
    industry:               Optional[dict] = {}
    summary:                Optional[str] = ""
    education_level:        Optional[str] = ""
    field_of_studies:       Optional[str] = ""
    work_experience:        Optional[str] = ""
    projects:               Optional[str] = ""
    experience_timeline:    Optional[list] = []
    career_trajectory:      Optional[dict] = {}
    file_path:              Optional[str] = ""
    parsing_confidence:     Optional[float] = 0.0
    company_source:         Optional[str] = ""


class JobPayload(BaseModel):
    title:              Optional[str] = ""
    company:            Optional[str] = ""
    industry:           Optional[str] = ""
    location:           Optional[str] = ""
    employment_type:    Optional[str] = "not specified"
    job_description:    Optional[str] = ""
    posted:             Optional[str] = ""
    programming_languages: Optional[list] = []
    technical_skills:      Optional[list] = []
    spoken_languages:      Optional[list] = []
    certifications:        Optional[list] = []
    seniority_requirements_technologies:           Optional[list] = []
    seniority_requirements_programming_languages:  Optional[list] = []
    experience_level:             Optional[str] = "not specified"
    salary_range:                 Optional[str] = "not specified"
    education_requirements:       Optional[str] = "not specified"
    years_of_experience_required: Optional[int] = 0
    summary:                      Optional[str] = ""
    file_path:                    Optional[str] = ""


# ─── ENDPOINTS CANDIDATS ──────────────────────────────────────────────

@router.post("/candidates/")
def add_candidate_endpoint(payload: CandidatePayload):
    """Insere un candidat depuis un dict JSON."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = add_candidate(client, payload.model_dump())
            if result.get("status") == "failed":
                raise HTTPException(status_code=500, detail=result.get("error"))
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/candidates/batch/")
def add_batch_candidates_endpoint():
    """
    Insere tous les candidats depuis extracted_cvs_real_final.json.
    Lance l'insertion batch complete.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = add_batch_candidates(client)
            if "error" in result:
                raise HTTPException(status_code=500, detail=result["error"])
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/candidates/{candidate_id}")
def delete_candidate_endpoint(candidate_id: str, confirm: bool = False, scope: str = "archive"):
    """
    Archive un candidat par UUID par defaut.
    confirm=false retourne les donnees sans archiver.
    confirm=true & scope=archive archive.
    confirm=true & scope=hard supprime definitivement.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = delete_candidate_by_id(client, candidate_id, confirm=confirm, scope=scope)
            if result.get("deleted") == 0 and "Aucun" in result.get("message", ""):
                raise HTTPException(status_code=404, detail=result["message"])
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ─── ENDPOINTS OFFRES ─────────────────────────────────────────────────

@router.post("/jobs/")
def add_job_endpoint(payload: JobPayload):
    """Insere une offre depuis un dict JSON."""
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = add_job(client, payload.model_dump())
            if result.get("status") == "failed":
                raise HTTPException(status_code=500, detail=result.get("error"))
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/jobs/batch/")
def add_batch_jobs_endpoint():
    """
    Insere toutes les offres depuis extracted_jobs_real.json.
    Lance l'insertion batch complete.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = add_batch_jobs(client)
            if "error" in result:
                raise HTTPException(status_code=500, detail=result["error"])
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete("/jobs/{job_id}")
def delete_job_endpoint(job_id: str, confirm: bool = False, scope: str = "archive"):
    """
    Archive une offre par UUID par defaut.
    confirm=false retourne les donnees sans archiver.
    confirm=true & scope=archive archive.
    confirm=true & scope=hard supprime definitivement.
    """
    try:
        with get_client() as client:
            if not client.is_ready():
                raise HTTPException(status_code=500, detail="Weaviate non disponible")
            result = delete_job_by_id(client, job_id, confirm=confirm, scope=scope)
            if result.get("deleted") == 0 and "Aucune" in result.get("message", ""):
                raise HTTPException(status_code=404, detail=result["message"])
            return result
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
