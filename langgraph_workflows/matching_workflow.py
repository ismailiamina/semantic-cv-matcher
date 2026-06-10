"""
LangGraph workflow for CV/job matching orchestration.

The graph wraps the existing search and reranking functions without replacing
the current API endpoints. It provides a new orchestration layer that can be
tested independently through /api/workflows/matching.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from fastapi import HTTPException
from langgraph.graph import END, StateGraph

from API.llm_utils import analyze_gap_llm, explain_match_llm
from API.Search_API import RerankRequest, get_client, rerank_endpoint, serialize_result
from weaviate_db.List import get_candidate_by_id, get_job_by_id
from weaviate_db.search.candidates_for_job import search_candidate_for_job_by_id
from weaviate_db.search.jobs_for_candidate import search_job_for_candidate_by_id

MatchingType = Literal["candidates_for_job", "jobs_for_candidate"]


class MatchingWorkflowState(TypedDict, total=False):
    type: MatchingType
    uuid: str
    mode: str
    limit: int
    rerank: bool
    explain: bool
    explain_limit: int
    route: str
    source_context: Dict[str, Any]
    results: List[Dict[str, Any]]
    total: int
    quality: str
    warnings: List[str]
    nodes_executed: List[str]
    used_rerank: bool
    used_explain: bool


def _append_node(state: MatchingWorkflowState, node_name: str) -> List[str]:
    return [*state.get("nodes_executed", []), node_name]


def _as_list(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if item]
    if isinstance(value, str):
        return [value]
    return [str(value)]


def _build_job_query(job: Dict[str, Any]) -> str:
    parts = [
        f"Poste recherche : {job.get('title', '')}",
        f"Entreprise : {job.get('company', '')}",
        f"Niveau requis : {job.get('experience_level', '')}",
        f"Experience requise : {job.get('years_of_experience_required', 0)} ans",
    ]
    skills = _as_list(job.get("technical_skills"))
    languages = _as_list(job.get("programming_languages"))
    if skills:
        parts.append(f"Competences requises : {', '.join(skills[:15])}")
    if languages:
        parts.append(f"Langages requis : {', '.join(languages[:8])}")
    if job.get("summary"):
        parts.append(f"Description : {str(job.get('summary'))[:500]}")
    return ". ".join(part for part in parts if part)


def _build_candidate_query(candidate: Dict[str, Any]) -> str:
    years = candidate.get("years_of_experience", 0) or 0
    parts = [
        f"Candidat : {candidate.get('full_name', '')}",
        f"Experience : {years} ans",
        f"Roles occupes : {', '.join(_as_list(candidate.get('roles_held'))[:5]) or 'Non precise'}",
        f"Competences techniques : {', '.join(_as_list(candidate.get('technical_skills'))[:15]) or 'Non precise'}",
        f"Langages maitrises : {', '.join(_as_list(candidate.get('programming_languages'))[:8]) or 'Non precise'}",
    ]
    certifications = _as_list(candidate.get("certifications"))
    if certifications:
        parts.append(f"Certifications : {', '.join(certifications[:5])}")
    if candidate.get("summary"):
        parts.append(f"Resume : {str(candidate.get('summary'))[:500]}")
    return ". ".join(part for part in parts if part)


def route_matching_type(state: MatchingWorkflowState) -> Dict[str, Any]:
    matching_type = state.get("type")
    if matching_type not in {"candidates_for_job", "jobs_for_candidate"}:
        raise ValueError("type doit etre 'candidates_for_job' ou 'jobs_for_candidate'")

    route = "run_candidates_for_job" if matching_type == "candidates_for_job" else "run_jobs_for_candidate"
    return {
        "route": route,
        "warnings": state.get("warnings", []),
        "nodes_executed": _append_node(state, "route_matching_type"),
    }


def route_to_matching_node(state: MatchingWorkflowState) -> str:
    return state["route"]


def run_candidates_for_job(state: MatchingWorkflowState) -> Dict[str, Any]:
    job_uuid = state["uuid"]
    mode = state.get("mode", "hybride")
    limit = int(state.get("limit", 10))

    with get_client() as client:
        if not client.is_ready():
            raise HTTPException(status_code=500, detail="Weaviate non disponible")
        source_context = get_job_by_id(client, job_uuid)
        if not source_context:
            raise HTTPException(status_code=404, detail=f"Offre '{job_uuid}' non trouvee")
        raw_results = search_candidate_for_job_by_id(client, job_uuid, mode, limit)

    results = [serialize_result(result) for result in raw_results]
    return {
        "source_context": source_context,
        "results": results,
        "total": len(results),
        "used_rerank": False,
        "nodes_executed": _append_node(state, "run_candidates_for_job"),
    }


def run_jobs_for_candidate(state: MatchingWorkflowState) -> Dict[str, Any]:
    candidate_uuid = state["uuid"]
    mode = state.get("mode", "hybride")
    limit = int(state.get("limit", 10))

    with get_client() as client:
        if not client.is_ready():
            raise HTTPException(status_code=500, detail="Weaviate non disponible")
        source_context = get_candidate_by_id(client, candidate_uuid)
        if not source_context:
            raise HTTPException(status_code=404, detail=f"Candidat '{candidate_uuid}' non trouve")
        raw_results = search_job_for_candidate_by_id(client, candidate_uuid, mode, limit)

    results = [serialize_result(result) for result in raw_results]
    return {
        "source_context": source_context,
        "results": results,
        "total": len(results),
        "used_rerank": False,
        "nodes_executed": _append_node(state, "run_jobs_for_candidate"),
    }


def check_results_quality(state: MatchingWorkflowState) -> Dict[str, Any]:
    results = state.get("results", [])
    warnings = list(state.get("warnings", []))

    if not results:
        quality = "empty"
        warnings.append("Aucun resultat retourne par le matching.")
    else:
        top_score = max(float(item.get("score", 0) or 0) for item in results)
        quality = "weak" if top_score < 0.35 else "ok"
        if quality == "weak":
            warnings.append("Les resultats existent mais le meilleur score reste faible.")

    return {
        "quality": quality,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "check_results_quality"),
    }


def should_rerank(state: MatchingWorkflowState) -> str:
    if state.get("rerank") and state.get("results"):
        return "optional_rerank"
    if state.get("explain") and state.get("results"):
        return "optional_explain"
    return "format_results"


def should_explain(state: MatchingWorkflowState) -> str:
    if state.get("explain") and state.get("results"):
        return "optional_explain"
    return "format_results"


def optional_rerank(state: MatchingWorkflowState) -> Dict[str, Any]:
    warnings = list(state.get("warnings", []))
    results = state.get("results", [])
    source_context = state.get("source_context", {})

    query = (
        _build_job_query(source_context)
        if state.get("type") == "candidates_for_job"
        else _build_candidate_query(source_context)
    )

    try:
        reranked = rerank_endpoint(
            RerankRequest(
                query=query,
                candidates=results,
                top_k=int(state.get("limit", 10)),
            )
        )
        return {
            "results": reranked.get("results", []),
            "total": reranked.get("total", 0),
            "used_rerank": True,
            "nodes_executed": _append_node(state, "optional_rerank"),
        }
    except Exception as exc:
        warnings.append(f"Reranking non applique : {str(exc)}")
        return {
            "warnings": warnings,
            "used_rerank": False,
            "nodes_executed": _append_node(state, "optional_rerank"),
        }


def optional_explain(state: MatchingWorkflowState) -> Dict[str, Any]:
    warnings = list(state.get("warnings", []))
    results = list(state.get("results", []))
    source_context = state.get("source_context", {})
    explain_limit = min(max(int(state.get("explain_limit", 3) or 3), 1), 5)

    if not results:
        return {
            "used_explain": False,
            "nodes_executed": _append_node(state, "optional_explain"),
        }

    enriched_results: List[Dict[str, Any]] = []
    for index, item in enumerate(results):
        enriched_item = dict(item)

        if index < explain_limit:
            try:
                props = enriched_item.get("properties", {}) or {}
                if state.get("type") == "candidates_for_job":
                    enriched_item["llm_explanation"] = explain_match_llm(source_context, props)
                else:
                    enriched_item["llm_gap_analysis"] = analyze_gap_llm(source_context, props)
            except Exception as exc:
                warnings.append(f"Analyse LLM non appliquee pour {enriched_item.get('uuid', 'resultat')} : {str(exc)}")

        enriched_results.append(enriched_item)

    if len(results) > explain_limit:
        warnings.append(
            f"Analyse LLM appliquee aux {explain_limit} premiers resultats afin de limiter le temps de traitement."
        )

    return {
        "results": enriched_results,
        "used_explain": True,
        "warnings": warnings,
        "nodes_executed": _append_node(state, "optional_explain"),
    }


def format_results(state: MatchingWorkflowState) -> Dict[str, Any]:
    return {
        "nodes_executed": _append_node(state, "format_results"),
        "total": len(state.get("results", [])),
    }


def create_matching_workflow():
    workflow = StateGraph(MatchingWorkflowState)

    workflow.add_node("route_matching_type", route_matching_type)
    workflow.add_node("run_candidates_for_job", run_candidates_for_job)
    workflow.add_node("run_jobs_for_candidate", run_jobs_for_candidate)
    workflow.add_node("check_results_quality", check_results_quality)
    workflow.add_node("optional_rerank", optional_rerank)
    workflow.add_node("optional_explain", optional_explain)
    workflow.add_node("format_results", format_results)

    workflow.set_entry_point("route_matching_type")
    workflow.add_conditional_edges(
        "route_matching_type",
        route_to_matching_node,
        {
            "run_candidates_for_job": "run_candidates_for_job",
            "run_jobs_for_candidate": "run_jobs_for_candidate",
        },
    )
    workflow.add_edge("run_candidates_for_job", "check_results_quality")
    workflow.add_edge("run_jobs_for_candidate", "check_results_quality")
    workflow.add_conditional_edges(
        "check_results_quality",
        should_rerank,
        {
            "optional_rerank": "optional_rerank",
            "optional_explain": "optional_explain",
            "format_results": "format_results",
        },
    )
    workflow.add_conditional_edges(
        "optional_rerank",
        should_explain,
        {
            "optional_explain": "optional_explain",
            "format_results": "format_results",
        },
    )
    workflow.add_edge("optional_explain", "format_results")
    workflow.add_edge("format_results", END)

    return workflow.compile()


MATCHING_WORKFLOW = create_matching_workflow()


def run_matching_workflow(payload: Dict[str, Any]) -> Dict[str, Any]:
    initial_state: MatchingWorkflowState = {
        "type": payload["type"],
        "uuid": payload["uuid"],
        "mode": payload.get("mode", "hybride"),
        "limit": int(payload.get("limit", 10)),
        "rerank": bool(payload.get("rerank", False)),
        "explain": bool(payload.get("explain", False)),
        "explain_limit": int(payload.get("explain_limit", 3)),
        "warnings": [],
        "nodes_executed": [],
    }

    final_state = MATCHING_WORKFLOW.invoke(initial_state)
    return {
        "status": "success",
        "workflow": "matching",
        "type": final_state.get("type"),
        "uuid": final_state.get("uuid"),
        "mode": final_state.get("mode"),
        "total": final_state.get("total", 0),
        "results": final_state.get("results", []),
        "metadata": {
            "quality": final_state.get("quality"),
            "used_rerank": final_state.get("used_rerank", False),
            "used_explain": final_state.get("used_explain", False),
            "explain_limit": final_state.get("explain_limit", 3),
            "nodes_executed": final_state.get("nodes_executed", []),
            "warnings": final_state.get("warnings", []),
        },
    }
