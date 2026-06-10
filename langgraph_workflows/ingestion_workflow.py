"""
LangGraph workflow for document and URL ingestion.

The graph wraps the existing upload/parsing functions without replacing the
current /api/upload endpoints. It provides a traceable orchestration layer for
file and URL ingestion.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, TypedDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from langgraph.graph import END, StateGraph
from fastapi import HTTPException

from API.CRUD_API import add_candidate_endpoint, add_job_endpoint, get_client
from API.Upload_API import (
    CV_PROMPT,
    CV_JSON_SCHEMA,
    JOB_PROMPT,
    JOB_JSON_SCHEMA,
    LINKEDIN_CV_PROMPT,
    LINKEDIN_JOB_PROMPT,
    extract_text_from_bytes,
    extract_with_mistral_small,
    is_missing,
    map_cv_to_payload,
    map_job_to_payload,
    normalize_extracted_job,
    retry_linkedin_candidate_extraction,
    retry_linkedin_job_extraction,
    scrape_linkedin_candidate_playwright_text,
    scrape_linkedin_job_guest_text,
    validate_extracted_cv,
    validate_extracted_job,
    validate_linkedin_cv_quality,
    validate_linkedin_job_quality,
)
from weaviate_db.List import get_all_candidates, get_all_jobs

SourceType = Literal["file", "url"]
TargetType = Literal["candidate", "job"]


class IngestionWorkflowState(TypedDict, total=False):
    source_type: SourceType
    target_type: TargetType
    filename: str
    file_content: bytes
    url: str
    force_insert: bool
    min_quality_score: int
    retry_count: int
    route: str
    text: str
    extracted_data: Dict[str, Any]
    payload: Any
    result: Dict[str, Any]
    output: Dict[str, Any]
    text_length: int
    quality: str
    quality_score: int
    quality_status: str
    quality_issues: List[str]
    duplicates: List[Dict[str, Any]]
    insertion_decision: str
    inserted: bool
    warnings: List[str]
    nodes_executed: List[str]


def _append_node(state: IngestionWorkflowState, node_name: str) -> List[str]:
    return [*state.get("nodes_executed", []), node_name]


def _as_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _has_text(value: Any, min_length: int = 1) -> bool:
    return bool(str(value or "").strip()) and len(str(value or "").strip()) >= min_length


def _score_candidate_quality(data: Dict[str, Any]) -> tuple[int, List[str]]:
    score = 0
    issues: List[str] = []

    if not is_missing(data.get("full_name")):
        score += 20
    else:
        issues.append("Nom du candidat manquant.")

    if len(_as_list(data.get("technical_skills"))) >= 3:
        score += 20
    else:
        issues.append("Competences techniques insuffisantes.")

    if len(_as_list(data.get("roles_held"))) >= 1:
        score += 15
    else:
        issues.append("Aucun role professionnel detecte.")

    if _has_text(data.get("summary"), 80):
        score += 15
    else:
        issues.append("Resume trop court ou absent.")

    if len(_as_list(data.get("experience_timeline"))) >= 1:
        score += 15
    else:
        issues.append("Timeline d'experience absente.")

    if len(_as_list(data.get("programming_languages"))) >= 1:
        score += 10
    else:
        issues.append("Langages de programmation absents.")

    try:
        if float(data.get("parsing_confidence") or 0) >= 0.7:
            score += 5
        else:
            issues.append("Confiance de parsing faible.")
    except (TypeError, ValueError):
        issues.append("Confiance de parsing invalide.")

    return min(score, 100), issues


def _score_job_quality(data: Dict[str, Any]) -> tuple[int, List[str]]:
    score = 0
    issues: List[str] = []

    if not is_missing(data.get("title")):
        score += 20
    else:
        issues.append("Titre de l'offre manquant.")

    if not is_missing(data.get("company")):
        score += 15
    else:
        issues.append("Entreprise manquante.")

    if _has_text(data.get("job_description"), 120) or _has_text(data.get("summary"), 80):
        score += 20
    else:
        issues.append("Description ou resume de l'offre insuffisant.")

    if len(_as_list(data.get("technical_skills"))) >= 3:
        score += 20
    else:
        issues.append("Competences techniques requises insuffisantes.")

    if len(_as_list(data.get("programming_languages"))) >= 1:
        score += 10
    else:
        issues.append("Langages de programmation requis absents.")

    if not is_missing(data.get("experience_level")):
        score += 10
    else:
        issues.append("Niveau d'experience non precise.")

    try:
        if int(float(data.get("years_of_experience_required") or 0)) > 0:
            score += 5
        else:
            issues.append("Nombre d'annees d'experience non precise.")
    except (TypeError, ValueError):
        issues.append("Nombre d'annees d'experience invalide.")

    return min(score, 100), issues


def _quality_status(score: int) -> str:
    if score >= 70:
        return "valid"
    if score >= 50:
        return "needs_review"
    return "rejected"


def _build_retry_prompt(target_type: TargetType, issues: List[str], data: Dict[str, Any]) -> str:
    schema = CV_JSON_SCHEMA if target_type == "candidate" else JOB_JSON_SCHEMA
    object_label = "candidat" if target_type == "candidate" else "offre"
    return f"""Tu dois corriger une extraction {object_label} incomplete.
Le JSON precedent manque d'informations importantes.

Problemes detectes :
{chr(10).join(f"- {issue}" for issue in issues) if issues else "- Extraction insuffisante"}

JSON precedent :
{json.dumps(data, ensure_ascii=False)[:4000]}

Regles :
- Utilise uniquement le texte source fourni apres ce prompt.
- N'invente aucune information absente du texte.
- Complete les champs manquants uniquement si l'information est presente.
- Retourne uniquement un JSON valide conforme au schema.

Schema attendu :
{json.dumps(schema, ensure_ascii=False)}

Texte source :
"""


def _extract_with_mistral_resilient(
    text: str,
    prompt: str,
    warnings: List[str],
    retry_prompt: str | None = None,
) -> tuple[Dict[str, Any], List[str]]:
    attempts = [
        ("standard", text),
        ("texte reduit", text[:10000]),
    ]
    last_error: Exception | None = None

    for index, (label, attempt_text) in enumerate(attempts):
        try:
            if index > 0:
                warnings.append("Timeout Mistral detecte : deuxieme tentative avec un texte reduit.")
            return extract_with_mistral_small(attempt_text, retry_prompt or prompt), warnings
        except HTTPException as exc:
            last_error = exc
            detail = str(exc.detail)
            if "timed out" not in detail.lower() and "timeout" not in detail.lower():
                raise
        except Exception as exc:
            last_error = exc
            detail = str(exc)
            if "timed out" not in detail.lower() and "timeout" not in detail.lower():
                raise

    if isinstance(last_error, HTTPException):
        raise last_error
    raise HTTPException(status_code=500, detail=str(last_error or "Erreur Mistral inconnue"))


def _candidate_duplicates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    linkedin = "" if is_missing(data.get("linkedin")) else _norm(data.get("linkedin"))
    email = "" if is_missing(data.get("email")) else _norm(data.get("email"))
    full_name = "" if is_missing(data.get("full_name")) else _norm(data.get("full_name"))
    company = "" if is_missing(data.get("company_source")) else _norm(data.get("company_source"))
    location = "" if is_missing(data.get("location")) else _norm(data.get("location"))

    if not any([linkedin, email, full_name]):
        return []

    matches: List[Dict[str, Any]] = []
    with get_client() as client:
        candidates = get_all_candidates(client, limit=5000)

    for candidate in candidates:
        reasons = []
        if linkedin and linkedin == _norm(candidate.get("linkedin")):
            reasons.append("meme profil LinkedIn")
        if email and email == _norm(candidate.get("email")):
            reasons.append("meme email")
        if full_name and full_name == _norm(candidate.get("full_name")) and company and company == _norm(candidate.get("company_source")):
            reasons.append("meme nom et meme entreprise source")
        elif full_name and full_name == _norm(candidate.get("full_name")) and location and location == _norm(candidate.get("location")):
            reasons.append("meme nom et meme localisation")

        if reasons:
            matches.append({
                "uuid": candidate.get("uuid", ""),
                "label": candidate.get("full_name", ""),
                "reason": ", ".join(reasons),
            })
        if len(matches) >= 5:
            break

    return matches


def _job_duplicates(data: Dict[str, Any]) -> List[Dict[str, Any]]:
    title = "" if is_missing(data.get("title")) else _norm(data.get("title"))
    company = "" if is_missing(data.get("company")) else _norm(data.get("company"))
    location = "" if is_missing(data.get("location")) else _norm(data.get("location"))

    if not title:
        return []

    matches: List[Dict[str, Any]] = []
    with get_client() as client:
        jobs = get_all_jobs(client, limit=5000)

    for job in jobs:
        reasons = []
        if title == _norm(job.get("title")) and company and company == _norm(job.get("company")):
            reasons.append("meme titre et meme entreprise")
        elif title == _norm(job.get("title")) and location and location == _norm(job.get("location")):
            reasons.append("meme titre et meme localisation")

        if reasons:
            matches.append({
                "uuid": job.get("uuid", ""),
                "label": job.get("title", ""),
                "company": job.get("company", ""),
                "reason": ", ".join(reasons),
            })
        if len(matches) >= 5:
            break

    return matches


def route_input_type(state: IngestionWorkflowState) -> Dict[str, Any]:
    source_type = state.get("source_type")
    target_type = state.get("target_type")

    if source_type not in {"file", "url"}:
        raise ValueError("source_type doit etre 'file' ou 'url'")
    if target_type not in {"candidate", "job"}:
        raise ValueError("target_type doit etre 'candidate' ou 'job'")

    return {
        "route": "extract_file_text" if source_type == "file" else "extract_url_text",
        "warnings": state.get("warnings", []),
        "nodes_executed": _append_node(state, "route_input_type"),
    }


def route_to_extraction_node(state: IngestionWorkflowState) -> str:
    return state["route"]


def extract_file_text(state: IngestionWorkflowState) -> Dict[str, Any]:
    text = extract_text_from_bytes(
        state.get("file_content", b""),
        state.get("filename", ""),
    )
    return {
        "text": text,
        "text_length": len(text or ""),
        "nodes_executed": _append_node(state, "extract_file_text"),
    }


async def extract_url_text(state: IngestionWorkflowState) -> Dict[str, Any]:
    url = state.get("url", "")
    if state.get("target_type") == "candidate":
        text = await scrape_linkedin_candidate_playwright_text(url)
    else:
        text = scrape_linkedin_job_guest_text(url)

    return {
        "text": text,
        "text_length": len(text or ""),
        "nodes_executed": _append_node(state, "extract_url_text"),
    }


def parse_with_mistral(state: IngestionWorkflowState) -> Dict[str, Any]:
    source_type = state.get("source_type")
    target_type = state.get("target_type")
    text = state.get("text", "")
    warnings = list(state.get("warnings", []))

    if target_type == "candidate":
        prompt = LINKEDIN_CV_PROMPT if source_type == "url" else CV_PROMPT
    else:
        prompt = LINKEDIN_JOB_PROMPT if source_type == "url" else JOB_PROMPT

    data, warnings = _extract_with_mistral_resilient(text, prompt, warnings)

    return {
        "extracted_data": data,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "parse_with_mistral"),
    }


def evaluate_extraction_quality(state: IngestionWorkflowState) -> Dict[str, Any]:
    data = state.get("extracted_data", {}) or {}
    warnings = list(state.get("warnings", []))

    if state.get("target_type") == "candidate":
        score, issues = _score_candidate_quality(data)
    else:
        score, issues = _score_job_quality(data)

    status = _quality_status(score)
    if status != "valid":
        warnings.append(
            f"Qualite d'extraction {status} ({score}/100) : "
            + "; ".join(issues[:4])
        )

    return {
        "quality": status,
        "quality_score": score,
        "quality_status": status,
        "quality_issues": issues,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "evaluate_extraction_quality"),
    }


def should_retry_quality(state: IngestionWorkflowState) -> str:
    min_score = int(state.get("min_quality_score", 60) or 60)
    retry_count = int(state.get("retry_count", 0) or 0)
    if int(state.get("quality_score", 0) or 0) < min_score and retry_count < 1:
        return "retry_parse_with_mistral"
    return "validate_data"


def retry_parse_with_mistral(state: IngestionWorkflowState) -> Dict[str, Any]:
    warnings = list(state.get("warnings", []))
    retry_prompt = _build_retry_prompt(
        state.get("target_type", "candidate"),
        state.get("quality_issues", []),
        state.get("extracted_data", {}) or {},
    )
    data, warnings = _extract_with_mistral_resilient(
        state.get("text", ""),
        retry_prompt,
        warnings,
        retry_prompt=retry_prompt,
    )

    return {
        "extracted_data": data,
        "retry_count": int(state.get("retry_count", 0) or 0) + 1,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "retry_parse_with_mistral"),
    }


def validate_data(state: IngestionWorkflowState) -> Dict[str, Any]:
    data = dict(state.get("extracted_data", {}) or {})
    text = state.get("text", "")
    source_type = state.get("source_type")
    target_type = state.get("target_type")
    warnings = list(state.get("warnings", []))

    if target_type == "candidate":
        if source_type == "url":
            if is_missing(data.get("linkedin")):
                data["linkedin"] = state.get("url", "")
            data = retry_linkedin_candidate_extraction(data, text)
            if is_missing(data.get("linkedin")):
                data["linkedin"] = state.get("url", "")
        data = validate_extracted_cv(data, text)
        if source_type == "url":
            data = validate_linkedin_cv_quality(data)
    else:
        if source_type == "url":
            data = retry_linkedin_job_extraction(data, text)
        data = validate_extracted_job(data)
        if source_type == "url":
            data = validate_linkedin_job_quality(data)

    return {
        "extracted_data": data,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "validate_data"),
    }


def normalize_payload(state: IngestionWorkflowState) -> Dict[str, Any]:
    data = dict(state.get("extracted_data", {}) or {})

    if state.get("target_type") == "candidate":
        payload = map_cv_to_payload(data)
    else:
        data = normalize_extracted_job(data, state.get("text", ""))
        payload = map_job_to_payload(data)

    return {
        "extracted_data": data,
        "payload": payload,
        "nodes_executed": _append_node(state, "normalize_payload"),
    }


def detect_duplicates(state: IngestionWorkflowState) -> Dict[str, Any]:
    data = state.get("extracted_data", {}) or {}
    warnings = list(state.get("warnings", []))

    if state.get("target_type") == "candidate":
        duplicates = _candidate_duplicates(data)
    else:
        duplicates = _job_duplicates(data)

    if duplicates:
        warnings.append(f"{len(duplicates)} doublon(s) potentiel(s) detecte(s) avant insertion.")

    return {
        "duplicates": duplicates,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "detect_duplicates"),
    }


def decide_insertion(state: IngestionWorkflowState) -> Dict[str, Any]:
    warnings = list(state.get("warnings", []))
    force_insert = bool(state.get("force_insert", False))
    min_score = int(state.get("min_quality_score", 60) or 60)
    quality_score = int(state.get("quality_score", 0) or 0)
    duplicates = state.get("duplicates", [])

    if duplicates and not force_insert:
        decision = "duplicate"
        inserted = False
    elif quality_score < min_score and not force_insert:
        decision = "needs_review"
        inserted = False
    else:
        decision = "insert"
        inserted = True
        if force_insert and (duplicates or quality_score < min_score):
            warnings.append("Insertion forcee malgre les alertes qualite/doublon.")

    return {
        "insertion_decision": decision,
        "inserted": inserted,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "decide_insertion"),
    }


def should_insert(state: IngestionWorkflowState) -> str:
    return "insert_into_weaviate" if state.get("inserted") else "format_response"


def insert_into_weaviate(state: IngestionWorkflowState) -> Dict[str, Any]:
    payload = state.get("payload")

    if state.get("target_type") == "candidate":
        result = add_candidate_endpoint(payload)
    else:
        result = add_job_endpoint(payload)

    return {
        "result": result,
        "nodes_executed": _append_node(state, "insert_into_weaviate"),
    }


def format_response(state: IngestionWorkflowState) -> Dict[str, Any]:
    result = state.get("result", {}) or {}
    data = state.get("extracted_data", {}) or {}
    target_type = state.get("target_type")
    inserted = bool(state.get("inserted", False))
    decision = state.get("insertion_decision", "insert" if inserted else "needs_review")

    label = (
        result.get("name") or data.get("full_name") or ""
        if target_type == "candidate"
        else result.get("title") or data.get("title") or ""
    )
    object_uuid = result.get("id", "")

    if decision == "duplicate":
        status = "duplicate"
        message = "Insertion bloquee : doublon potentiel detecte."
    elif decision == "needs_review":
        status = "needs_review"
        message = "Insertion bloquee : qualite d'extraction insuffisante, verification humaine recommandee."
    else:
        status = "success"
        message = (
            f"Candidat ajoute : {label}"
            if target_type == "candidate"
            else f"Offre ajoutee : {label}"
        )

    output = {
        "status": status,
        "workflow": "ingestion",
        "source_type": state.get("source_type"),
        "target_type": target_type,
        "inserted": inserted,
        "uuid": object_uuid,
        "label": label,
        "message": message,
    }

    return {
        "output": output,
        "nodes_executed": _append_node(state, "format_response"),
    }


def create_file_processing_workflow():
    workflow = StateGraph(IngestionWorkflowState)

    workflow.add_node("route_input_type", route_input_type)
    workflow.add_node("extract_file_text", extract_file_text)
    workflow.add_node("extract_url_text", extract_url_text)
    workflow.add_node("parse_with_mistral", parse_with_mistral)
    workflow.add_node("evaluate_extraction_quality", evaluate_extraction_quality)
    workflow.add_node("retry_parse_with_mistral", retry_parse_with_mistral)
    workflow.add_node("validate_data", validate_data)
    workflow.add_node("normalize_payload", normalize_payload)
    workflow.add_node("detect_duplicates", detect_duplicates)
    workflow.add_node("decide_insertion", decide_insertion)
    workflow.add_node("insert_into_weaviate", insert_into_weaviate)
    workflow.add_node("format_response", format_response)

    workflow.set_entry_point("route_input_type")
    workflow.add_conditional_edges(
        "route_input_type",
        route_to_extraction_node,
        {
            "extract_file_text": "extract_file_text",
            "extract_url_text": "extract_url_text",
        },
    )
    workflow.add_edge("extract_file_text", "parse_with_mistral")
    workflow.add_edge("extract_url_text", "parse_with_mistral")
    workflow.add_edge("parse_with_mistral", "evaluate_extraction_quality")
    workflow.add_conditional_edges(
        "evaluate_extraction_quality",
        should_retry_quality,
        {
            "retry_parse_with_mistral": "retry_parse_with_mistral",
            "validate_data": "validate_data",
        },
    )
    workflow.add_edge("retry_parse_with_mistral", "evaluate_extraction_quality")
    workflow.add_edge("validate_data", "normalize_payload")
    workflow.add_edge("normalize_payload", "detect_duplicates")
    workflow.add_edge("detect_duplicates", "decide_insertion")
    workflow.add_conditional_edges(
        "decide_insertion",
        should_insert,
        {
            "insert_into_weaviate": "insert_into_weaviate",
            "format_response": "format_response",
        },
    )
    workflow.add_edge("insert_into_weaviate", "format_response")
    workflow.add_edge("format_response", END)

    return workflow.compile()


INGESTION_WORKFLOW = create_file_processing_workflow()


async def run_ingestion_workflow(payload: Dict[str, Any]) -> Dict[str, Any]:
    initial_state: IngestionWorkflowState = {
        "source_type": payload["source_type"],
        "target_type": payload["target_type"],
        "filename": payload.get("filename", ""),
        "file_content": payload.get("file_content", b""),
        "url": payload.get("url", ""),
        "force_insert": bool(payload.get("force_insert", False)),
        "min_quality_score": int(payload.get("min_quality_score", 60)),
        "retry_count": 0,
        "warnings": [],
        "nodes_executed": [],
    }

    final_state = await INGESTION_WORKFLOW.ainvoke(initial_state)
    output = dict(final_state.get("output", {}) or {})
    output["metadata"] = {
        "quality": final_state.get("quality_status", final_state.get("quality", "ok")),
        "quality_score": final_state.get("quality_score", 0),
        "quality_issues": final_state.get("quality_issues", []),
        "insertion_decision": final_state.get("insertion_decision", ""),
        "duplicates": final_state.get("duplicates", []),
        "retry_count": final_state.get("retry_count", 0),
        "text_length": final_state.get("text_length", 0),
        "nodes_executed": final_state.get("nodes_executed", []),
        "warnings": final_state.get("warnings", []),
    }
    return output
