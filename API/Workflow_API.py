"""
API endpoints for LangGraph workflows.

These endpoints are additive: they orchestrate existing backend functions
without changing the historical search/upload endpoints.
"""

import sys
from pathlib import Path
from typing import Literal, Optional

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from pydantic import BaseModel, Field

from langgraph_workflows.ingestion_workflow import run_ingestion_workflow
from langgraph_workflows.matching_workflow import run_matching_workflow

router = APIRouter()


class MatchingWorkflowRequest(BaseModel):
    type: Literal["candidates_for_job", "jobs_for_candidate"]
    uuid: str = Field(..., description="UUID de l'offre ou du candidat selon le type")
    mode: str = Field("hybride", description="vecteur | texte | hybride")
    limit: int = Field(10, ge=1, le=50)
    rerank: bool = False
    explain: Optional[bool] = False
    explain_limit: int = Field(3, ge=1, le=5)


class IngestionUrlWorkflowRequest(BaseModel):
    url: str
    target_type: Literal["candidate", "job"] = Field(..., description="candidate | job")
    force_insert: bool = False
    min_quality_score: int = Field(60, ge=0, le=100)


@router.post("/workflows/matching")
def matching_workflow_endpoint(request: MatchingWorkflowRequest):
    """
    Execute the LangGraph matching workflow.
    """
    try:
        return run_matching_workflow(request.model_dump())
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur workflow matching : {str(exc)}")


@router.post("/workflows/ingestion/file")
async def ingestion_file_workflow_endpoint(
    target_type: Literal["candidate", "job"] = Form(..., description="candidate | job"),
    force_insert: bool = Form(False),
    min_quality_score: int = Form(60, ge=0, le=100),
    file: UploadFile = File(...),
):
    """
    Execute the LangGraph ingestion workflow for PDF/DOCX/TXT files.
    """
    try:
        content = await file.read()
        return await run_ingestion_workflow({
            "source_type": "file",
            "target_type": target_type,
            "filename": file.filename or "",
            "file_content": content,
            "force_insert": force_insert,
            "min_quality_score": min_quality_score,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur workflow ingestion fichier : {str(exc)}")


@router.post("/workflows/ingestion/url")
async def ingestion_url_workflow_endpoint(request: IngestionUrlWorkflowRequest):
    """
    Execute the LangGraph ingestion workflow for LinkedIn URLs.
    """
    try:
        return await run_ingestion_workflow({
            "source_type": "url",
            "target_type": request.target_type,
            "url": request.url,
            "force_insert": request.force_insert,
            "min_quality_score": request.min_quality_score,
        })
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Erreur workflow ingestion URL : {str(exc)}")
