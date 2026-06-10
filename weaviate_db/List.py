"""
weaviate/List.py
=================
Couche de lecture depuis Weaviate.
Lister / Compter / Fetch par ID pour Candidats et Offres.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
from typing import Optional, List, Dict, Any
from weaviate.classes.query import MetadataQuery, Filter, Sort
from config.settings import *

def is_active(data: Dict[str, Any]) -> bool:
    return not bool(data.get("is_archived", False))


def has_archive_field(collection) -> bool:
    try:
        return any(
            prop.name == "is_archived"
            for prop in collection.config.get().properties
        )
    except Exception:
        return False


# ─── CANDIDATS ────────────────────────────────────────────────────────

def get_all_candidates(client, limit: int = 500) -> List[Dict[str, Any]]:
    """Retourne tous les candidats depuis Weaviate."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    results = tenant_col.query.fetch_objects(
        limit=limit,
        return_metadata=MetadataQuery(creation_time=True)
    )

    candidates = []
    for obj in results.objects:
        data = dict(obj.properties)
        data["uuid"] = str(obj.uuid)
        # Deserialiser la timeline
        if data.get("experience_timeline"):
            try:
                data["experience_timeline"] = json.loads(data["experience_timeline"])
            except Exception:
                data["experience_timeline"] = []
        if is_active(data):
            candidates.append(data)

    return candidates


def get_candidate_by_id(client, candidate_id: str) -> Optional[Dict[str, Any]]:
    """Retourne un candidat par UUID."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    obj = tenant_col.query.fetch_object_by_id(candidate_id)
    if not obj:
        return None

    data = dict(obj.properties)
    data["uuid"] = str(obj.uuid)
    if not is_active(data):
        return None
    if data.get("experience_timeline"):
        try:
            data["experience_timeline"] = json.loads(data["experience_timeline"])
        except Exception:
            data["experience_timeline"] = []
    return data


def get_candidates_by_company(client, company: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Retourne les candidats d'une entreprise specifique."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    results = tenant_col.query.fetch_objects(
        filters=Filter.by_property("company_source").equal(company),
        limit=limit,
        return_metadata=MetadataQuery(creation_time=True)
    )

    candidates = []
    for obj in results.objects:
        data = dict(obj.properties)
        data["uuid"] = str(obj.uuid)
        if is_active(data):
            candidates.append(data)

    return candidates


def count_candidates(client) -> int:
    """Retourne le nombre total de candidats."""
    return len(get_all_candidates(client, limit=5000))


def get_candidates_names_and_ids(client, limit: int = 500) -> List[Dict[str, str]]:
    """
    Retourne uniquement nom + UUID pour peupler les selectbox Streamlit.
    Evite de charger toutes les proprietes.
    """
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    return_properties = [
        "full_name",
        "years_of_experience",
        "company_source",
        "location",
    ]
    if has_archive_field(candidate_collection):
        return_properties.append("is_archived")

    results = tenant_col.query.fetch_objects(
        limit=limit,
        return_properties=return_properties
    )

    return [
        {
            "uuid":               str(obj.uuid),
            "full_name":          obj.properties.get("full_name", "N/A"),
            "years_of_experience": obj.properties.get("years_of_experience", 0),
            "company_source":     obj.properties.get("company_source", ""),
            "location":           obj.properties.get("location", ""),
        }
        for obj in results.objects
        if is_active(obj.properties)
    ]


# ─── OFFRES ───────────────────────────────────────────────────────────

def get_all_jobs(client, limit: int = 500) -> List[Dict[str, Any]]:
    """Retourne toutes les offres depuis Weaviate."""
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    results = tenant_col.query.fetch_objects(
        limit=limit,
        return_metadata=MetadataQuery(creation_time=True)
    )

    jobs = []
    for obj in results.objects:
        data = dict(obj.properties)
        data["uuid"] = str(obj.uuid)
        if is_active(data):
            jobs.append(data)

    return jobs


def get_job_by_id(client, job_id: str) -> Optional[Dict[str, Any]]:
    """Retourne une offre par UUID."""
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    obj = tenant_col.query.fetch_object_by_id(job_id)
    if not obj:
        return None

    data = dict(obj.properties)
    data["uuid"] = str(obj.uuid)
    if not is_active(data):
        return None
    return data


def get_jobs_by_company(client, company: str, limit: int = 100) -> List[Dict[str, Any]]:
    """Retourne les offres d'une entreprise specifique."""
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    results = tenant_col.query.fetch_objects(
        filters=Filter.by_property("company").equal(company),
        limit=limit
    )

    jobs = []
    for obj in results.objects:
        data = dict(obj.properties)
        data["uuid"] = str(obj.uuid)
        if is_active(data):
            jobs.append(data)

    return jobs


def count_jobs(client) -> int:
    """Retourne le nombre total d'offres."""
    return len(get_all_jobs(client, limit=5000))


def get_jobs_titles_and_ids(client, limit: int = 500) -> List[Dict[str, str]]:
    """
    Retourne uniquement titre + UUID pour peupler les selectbox Streamlit.
    """
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    return_properties = [
        "title",
        "company",
        "experience_level",
        "years_of_experience_required",
        "location",
    ]
    if has_archive_field(job_collection):
        return_properties.append("is_archived")

    results = tenant_col.query.fetch_objects(
        limit=limit,
        return_properties=return_properties
    )

    return [
        {
            "uuid":                        str(obj.uuid),
            "title":                       obj.properties.get("title", "N/A"),
            "company":                     obj.properties.get("company", ""),
            "experience_level":            obj.properties.get("experience_level", ""),
            "years_of_experience_required": obj.properties.get("years_of_experience_required", 0),
            "location":                    obj.properties.get("location", ""),
        }
        for obj in results.objects
        if is_active(obj.properties)
    ]


# ─── KPIs GLOBAUX ─────────────────────────────────────────────────────

def get_global_stats(client) -> Dict[str, Any]:
    """
    Retourne les KPIs globaux pour le dashboard Streamlit.
    """
    total_candidates = count_candidates(client)
    total_jobs       = count_jobs(client)

    # Repartition par entreprise (candidats)
    candidates = get_all_candidates(client, limit=1000)
    company_counts = {}
    seniority_counts = {}

    for c in candidates:
        co = c.get("company_source", "inconnu")
        company_counts[co] = company_counts.get(co, 0) + 1

        years = int(c.get("years_of_experience") or 0)
        if years >= 10:   lvl = "Expert"
        elif years >= 6:  lvl = "Senior"
        elif years >= 4:  lvl = "Confirme"
        elif years >= 2:  lvl = "Medior"
        else:             lvl = "Junior"
        seniority_counts[lvl] = seniority_counts.get(lvl, 0) + 1

    # Repartition par entreprise (offres)
    jobs = get_all_jobs(client, limit=1000)
    job_company_counts = {}
    for j in jobs:
        co = j.get("company", "inconnu")
        job_company_counts[co] = job_company_counts.get(co, 0) + 1

    return {
        "total_candidates":  total_candidates,
        "total_jobs":        total_jobs,
        "by_company":        company_counts,
        "by_seniority":      seniority_counts,
        "jobs_by_company":   job_company_counts,
    }
