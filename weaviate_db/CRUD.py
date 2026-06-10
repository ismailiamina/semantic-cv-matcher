"""
Weaviate_DB/CRUD.py
====================
Couche d'acces aux donnees Weaviate.
Insert / Delete / Transform pour Candidats et Offres.
"""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import json
import time
import weaviate
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone
from weaviate.classes.config import Property, DataType
from config.settings import *

CANDIDATE_VECTOR_NAMES = [
    "tech_skills_vector",
    "summary_vector",
    "prog_langs_vector",
    "industry_vector",
    "roles_held_vector",
    "work_experience_vector",
    "location_vector",
    "projects_vector",
]

JOB_VECTOR_NAMES = [
    "job_description_vector",
    "job_industry_vector",
    "job_prog_langs_vector",
    "job_summary_vector",
    "job_tech_skills_vector",
    "job_title_vector",
]


# ─── CONNEXION ────────────────────────────────────────────────────────

def get_client():
    return weaviate.connect_to_local(
        host=WEAVIATE_HOST,
        port=WEAVIATE_PORT,
        grpc_port=WEAVIATE_GRPC_PORT,
        skip_init_checks=True
    )


# ─── TRANSFORMATION ───────────────────────────────────────────────────

def transform_cv_data(data: dict) -> dict:
    """
    Convertit un profil JSON LinkedIn scrappe
    en dict compatible avec le schema Weaviate Candidate.
    """
    if "data" in data:
        data = data.get("data", {})

    industry = data.get("industry", {}) or {}
    seniority_technologies = data.get("seniority_technologies", []) or []
    seniority_programming_languages = data.get("seniority_programming_languages", []) or []
    career = data.get("career_trajectory", {}) or {}

    # Serialiser experience_timeline en JSON string pour Weaviate
    experience_timeline = data.get("experience_timeline", []) or []
    experience_timeline_str = json.dumps(experience_timeline, ensure_ascii=False)

    return {
        # Infos generales
        "full_name":          data.get("full_name") or "not found",
        "email":              data.get("email") or "",
        "phone":              data.get("phone") or "",
        "location":           data.get("location") or "",
        "years_of_experience": int(data.get("years_of_experience") or 0),
        "linkedin":           data.get("linkedin") or "",
        "github":             data.get("github") or "",
        "roles_held":         data.get("roles_held", []) or [],

        # Competences
        "programming_languages": data.get("programming_languages", []) or [],
        "technical_skills":      data.get("technical_skills", []) or [],
        "spoken_languages":      data.get("spoken_languages", []) or [],

        # Certifications
        "certifications": data.get("certifications", []) or [],

        # Seniorite
        "seniority_technologies":           seniority_technologies,
        "seniority_programming_languages":  seniority_programming_languages,

        # Industrie aplatie
        "industry_primary_industries": industry.get("primary_industries", []) or [],

        # Resume et formation
        "summary":          data.get("summary") or "",
        "education_level":  data.get("education_level") or "",
        "field_of_studies": data.get("field_of_studies") or "",
        "work_experience":  data.get("work_experience") or "",
        "projects":         data.get("projects") or "",

        # Timeline serialisee
        "experience_timeline": experience_timeline_str,

        # Trajectoire de carriere aplatie
        "career_trajectory_direction":          career.get("direction") or "",
        "career_trajectory_progression_speed":  career.get("progression_speed") or "",
        "career_trajectory_predicted_profile":  career.get("predicted_profile") or "",
        "career_trajectory_skills_in_progress": career.get("skills_in_progress", []) or [],

        # Metadonnees
        "file_path":          data.get("file_path") or "",
        "parsing_confidence": float(data.get("parsing_confidence") or 0.0),
        "company_source":     data.get("company_source") or "",
        "extraction_method":  data.get("extraction_method") or "",
    }


def transform_job_data(data: dict) -> dict:
    """
    Convertit une offre JSON scrappee depuis LinkedIn
    en dict compatible avec le schema Weaviate Job.
    """
    if "data" in data:
        data = data.get("data", {})

    seniority_req_tech  = data.get("seniority_requirements_technologies", []) or []
    seniority_req_langs = data.get("seniority_requirements_programming_languages", []) or []

    return {
        "title":           data.get("title") or "not specified",
        "company":         data.get("company") or "not specified",
        "industry":        data.get("industry") or "",
        "location":        data.get("location") or "",
        "employment_type": data.get("employment_type") or "not specified",
        "job_description": data.get("job_description") or "",
        "posted":          data.get("posted") or "",

        "programming_languages": data.get("programming_languages", []) or [],
        "technical_skills":      data.get("technical_skills", []) or [],
        "spoken_languages":      data.get("spoken_languages", []) or [],

        "certifications": data.get("certifications", []) or [],

        "seniority_requirements_technologies":           seniority_req_tech,
        "seniority_requirements_programming_languages":  seniority_req_langs,

        "experience_level":             data.get("experience_level") or "not specified",
        "salary_range":                 data.get("salary_range") or "not specified",
        "education_requirements":       data.get("education_requirements") or "not specified",
        "years_of_experience_required": int(data.get("years_of_experience_required") or 0),

        "summary":   data.get("summary") or "",
        "file_path": data.get("file_path") or "",
    }


# ─── INSERT UNITAIRE ──────────────────────────────────────────────────

def add_candidate(client, profile: dict, max_retries: int = 3) -> dict:
    """
    Insere un seul candidat dans Weaviate.
    profile : dict JSON du profil LinkedIn scrappe.
    """
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    json_data = transform_cv_data(profile)
    name      = json_data.get("full_name", "N/A")

    for attempt in range(max_retries):
        try:
            # Verifier doublon par file_path
            file_path = json_data.get("file_path", "")
            if file_path:
                existing = tenant_col.query.bm25(
                    query=file_path,
                    query_properties=["file_path"],
                    limit=1
                )
                if existing.objects:
                    uid = str(existing.objects[0].uuid)
                    print(f"  Candidat existant '{name}' — mise a jour (UUID: {uid})")
                    tenant_col.data.delete_by_id(uid)

            uid = tenant_col.data.insert(properties=json_data)
            print(f"  Candidat ajoute : {name} (UUID: {uid})")
            return {"status": "success", "id": str(uid), "name": name}

        except Exception as e:
            print(f"  Erreur ajout candidat {name} (tentative {attempt+1}) : {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return {"status": "failed", "name": name, "error": str(e)}


def add_job(client, job: dict, max_retries: int = 3) -> dict:
    """
    Insere une seule offre dans Weaviate.
    job : dict JSON de l'offre scrappee.
    """
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    json_data = transform_job_data(job)
    title     = json_data.get("title", "N/A")

    for attempt in range(max_retries):
        try:
            uid = tenant_col.data.insert(properties=json_data)
            print(f"  Offre ajoutee : {title} (UUID: {uid})")
            return {"status": "success", "id": str(uid), "title": title}

        except Exception as e:
            print(f"  Erreur ajout offre {title} (tentative {attempt+1}) : {e}")
            if attempt < max_retries - 1:
                time.sleep(2)
            else:
                return {"status": "failed", "title": title, "error": str(e)}


# ─── INSERT BATCH ─────────────────────────────────────────────────────

def add_batch_candidates(client, candidates_file: Optional[Path] = None) -> dict:
    """
    Insere tous les candidats depuis extracted_cvs_real_final.json en batch.
    """
    path = candidates_file or CANDIDATES_FILE

    if not path.exists():
        return {"error": f"Fichier introuvable : {path}"}

    with open(path, encoding="utf-8") as f:
        candidates = json.load(f)

    valid = [c for c in candidates if "error" not in c]
    print(f"\n  Insertion batch candidats : {len(valid)} profils")

    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    processed = 0
    failed    = 0
    skipped   = 0

    with tenant_col.batch.dynamic() as batch:
        for profile in valid:
            try:
                json_data = transform_cv_data(profile)
                if not json_data.get("full_name") or json_data["full_name"] == "not found":
                    skipped += 1
                    continue
                batch.add_object(properties=json_data)
                processed += 1
            except Exception as e:
                print(f"  Erreur batch candidat : {e}")
                failed += 1

    if batch.number_errors > 0:
        print(f"  Erreurs batch : {batch.number_errors}")

    print(f"  Batch candidats : {processed} inseres | {failed} echecs | {skipped} skips")
    return {"processed": processed, "failed": failed, "skipped": skipped}


def add_batch_jobs(client, jobs_file: Optional[Path] = None) -> dict:
    """
    Insere toutes les offres depuis extracted_jobs_real.json en batch.
    """
    path = jobs_file or JOBS_FILE

    if not path.exists():
        return {"error": f"Fichier introuvable : {path}"}

    with open(path, encoding="utf-8") as f:
        jobs = json.load(f)

    valid = [j for j in jobs if "error" not in j]
    print(f"\n  Insertion batch offres : {len(valid)} offres")

    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    processed = 0
    failed    = 0

    with tenant_col.batch.dynamic() as batch:
        for job in valid:
            try:
                json_data = transform_job_data(job)
                if not json_data.get("title") or json_data["title"] == "not specified":
                    continue
                batch.add_object(properties=json_data)
                processed += 1
            except Exception as e:
                print(f"  Erreur batch offre : {e}")
                failed += 1

    if batch.number_errors > 0:
        print(f"  Erreurs batch : {batch.number_errors}")

    print(f"  Batch offres : {processed} inserees | {failed} echecs")
    return {"processed": processed, "failed": failed}


# ─── DELETE ───────────────────────────────────────────────────────────

def ensure_archive_fields(collection) -> None:
    """Ajoute les champs d'archivage aux collections existantes si necessaire."""
    existing = {p.name for p in collection.config.get().properties}
    fields = [
        ("is_archived", DataType.BOOL),
        ("archived_at", DataType.TEXT),
        ("delete_scope", DataType.TEXT),
    ]
    for name, data_type in fields:
        if name not in existing:
            collection.config.add_property(
                Property(name=name, data_type=data_type, skip_vectorization=True)
            )


def existing_vectors(obj, vector_names: list[str]) -> dict:
    vectors = getattr(obj, "vector", None) or {}
    return {
        name: vectors[name]
        for name in vector_names
        if name in vectors and vectors[name] is not None
    }


def delete_candidate_by_id(client, candidate_id: str, confirm: bool = False, scope: str = "archive") -> dict:
    """Archive un candidat par defaut. scope='hard' supprime definitivement."""
    candidate_collection = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant_col           = candidate_collection.with_tenant(TENANT_ID_FOR_CV)

    obj = tenant_col.query.fetch_object_by_id(
        candidate_id,
        include_vector=CANDIDATE_VECTOR_NAMES,
    )
    if not obj:
        return {"deleted": 0, "message": f"Aucun candidat avec l'ID '{candidate_id}'"}

    data = obj.properties
    if not confirm:
        return {"deleted": 0, "data": data,
                "message": f"Candidat trouve mais suppression non confirmee."}

    if scope == "hard":
        tenant_col.data.delete_by_id(candidate_id)
        return {"deleted": 1, "archived": 0, "data": data,
                "message": f"Candidat '{data.get('full_name')}' supprime definitivement."}

    ensure_archive_fields(candidate_collection)
    tenant_col.data.update(
        uuid=candidate_id,
        properties={
            "is_archived": True,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "delete_scope": "archive",
        },
        vector=existing_vectors(obj, CANDIDATE_VECTOR_NAMES),
    )
    return {"deleted": 0, "archived": 1, "data": data,
            "message": f"Candidat '{data.get('full_name')}' archive."}


def delete_job_by_id(client, job_id: str, confirm: bool = False, scope: str = "archive") -> dict:
    """Archive une offre par defaut. scope='hard' supprime definitivement."""
    job_collection = client.collections.get(JOB_COLLECTION_NAME)
    tenant_col     = job_collection.with_tenant(TENANT_ID_FOR_JOBS)

    obj = tenant_col.query.fetch_object_by_id(
        job_id,
        include_vector=JOB_VECTOR_NAMES,
    )
    if not obj:
        return {"deleted": 0, "message": f"Aucune offre avec l'ID '{job_id}'"}

    data = obj.properties
    if not confirm:
        return {"deleted": 0, "data": data,
                "message": f"Offre trouvee mais suppression non confirmee."}

    if scope == "hard":
        tenant_col.data.delete_by_id(job_id)
        return {"deleted": 1, "archived": 0, "data": data,
                "message": f"Offre '{data.get('title')}' supprimee definitivement."}

    ensure_archive_fields(job_collection)
    tenant_col.data.update(
        uuid=job_id,
        properties={
            "is_archived": True,
            "archived_at": datetime.now(timezone.utc).isoformat(),
            "delete_scope": "archive",
        },
        vector=existing_vectors(obj, JOB_VECTOR_NAMES),
    )
    return {"deleted": 0, "archived": 1, "data": data,
            "message": f"Offre '{data.get('title')}' archivee."}


# ─── SCRIPT D'INSERTION INITIALE ──────────────────────────────────────

if __name__ == "__main__":
    print("=" * 60)
    print("INSERTION INITIALE — Candidats + Offres")
    print("=" * 60)

    client = get_client()
    try:
        print("\n--- CANDIDATS ---")
        result_cv = add_batch_candidates(client)
        print(f"  Resultat : {result_cv}")

        print("\n--- OFFRES ---")
        result_jobs = add_batch_jobs(client)
        print(f"  Resultat : {result_jobs}")

        print("\nInsertion terminee.")
    finally:
        client.close()
