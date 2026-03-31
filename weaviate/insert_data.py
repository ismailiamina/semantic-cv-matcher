"""
Insertion des CVs et Jobs dans Weaviate
Vectorisation automatique par Ollama (nomic-embed-text)
"""

import weaviate
import json
import time
import requests
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

CANDIDATE_COLLECTION_NAME = "Candidate"
JOB_COLLECTION_NAME       = "Job"
TENANT_ID_FOR_CV          = "cv"
TENANT_ID_FOR_JOBS        = "job"

BASE_DIR      = Path(__file__).parent.parent
CV_JSON_PATH  = BASE_DIR / "extraction" / "extracted_cvs.json"
JOB_JSON_PATH = BASE_DIR / "extraction" / "extracted_jobs.json"


def connect_weaviate():
    print("Attente que Weaviate soit pret...")
    for i in range(20):
        try:
            r = requests.get("http://localhost:8080/v1/.well-known/ready", timeout=3)
            if r.status_code == 200:
                print(f"  Weaviate HTTP pret (tentative {i+1})")
                break
        except Exception:
            pass
        print(f"  Tentative {i+1}/20 - attente 3s...")
        time.sleep(3)
    else:
        raise Exception("Weaviate HTTP non disponible apres 60 secondes.")

    time.sleep(3)
    client = weaviate.connect_to_local(
        host="localhost",
        port=8080,
        grpc_port=50051,
        skip_init_checks=True
    )
    print("Weaviate connecte et pret.")
    return client


def transform_cv_data(cv: dict) -> dict:
    industry = cv.get("industry", {})
    if isinstance(industry, dict):
        primary_industries = industry.get("primary_industries", [])
    else:
        primary_industries = []

    timeline = cv.get("experience_timeline", [])
    timeline_companies = [e.get("company", "") for e in timeline if e.get("company")]
    timeline_roles     = [e.get("role", "")    for e in timeline if e.get("role")]

    trajectory = cv.get("career_trajectory", {})
    if not isinstance(trajectory, dict):
        trajectory = {}

    return {
        "full_name":               cv.get("full_name", ""),
        "email":                   cv.get("email", ""),
        "phone":                   cv.get("phone", ""),
        "location":                cv.get("location", ""),
        "years_of_experience":     int(cv.get("years_of_experience") or 0),
        "linkedin":                cv.get("linkedin", ""),
        "github":                  cv.get("github", ""),
        "roles_held":              cv.get("roles_held", []),
        "programming_languages":   cv.get("programming_languages", []),
        "technical_skills":        cv.get("technical_skills", []),
        "spoken_languages":        cv.get("spoken_languages", []),
        "certifications":          cv.get("certifications", []),
        "seniority_technologies":           cv.get("seniority_technologies", []),
        "seniority_programming_languages":  cv.get("seniority_programming_languages", []),
        "industry_primary_industries":      primary_industries,
        "summary":                 cv.get("summary", ""),
        "education_level":         cv.get("education_level", ""),
        "field_of_studies":        cv.get("field_of_studies", ""),
        "work_experience":         cv.get("work_experience", ""),
        "projects":                cv.get("projects", ""),
        "file_path":               cv.get("file_path", ""),
        "parsing_confidence":      float(cv.get("parsing_confidence") or 0),
        "career_trajectory_direction":         trajectory.get("direction", ""),
        "career_trajectory_predicted_profile": trajectory.get("predicted_profile", ""),
        "career_trajectory_progression_speed": trajectory.get("progression_speed", ""),
        "timeline_companies": timeline_companies,
        "timeline_roles":     timeline_roles,
    }


def transform_job_data(job: dict) -> dict:
    return {
        "title":           job.get("title", ""),
        "company":         job.get("company", ""),
        "industry":        job.get("industry", ""),
        "location":        job.get("location", ""),
        "employment_type": job.get("employment_type", ""),
        "job_description": job.get("job_description", ""),
        "posted":          job.get("posted", ""),
        "programming_languages": job.get("programming_languages", []),
        "technical_skills":      job.get("technical_skills", []),
        "spoken_languages":      job.get("spoken_languages", []),
        "certifications":        job.get("certifications", []),
        "seniority_requirements_technologies":
            job.get("seniority_requirements_technologies", []),
        "seniority_requirements_programming_languages":
            job.get("seniority_requirements_programming_languages", []),
        "experience_level":             job.get("experience_level", ""),
        "salary_range":                 job.get("salary_range", ""),
        "education_requirements":       job.get("education_requirements", ""),
        "years_of_experience_required": int(job.get("years_of_experience_required") or 0),
        "summary":                      job.get("summary", ""),
    }


def insert_cvs(client):
    if not CV_JSON_PATH.exists():
        print(f"  ERREUR : {CV_JSON_PATH} non trouve")
        return 0

    with open(CV_JSON_PATH, encoding="utf-8") as f:
        cvs = json.load(f)

    cvs = [cv for cv in cvs if "error" not in cv]
    print(f"\nInsertion de {len(cvs)} CVs...")
    print("Ollama vectorise automatiquement (local, sans limite)...")

    candidate_col    = client.collections.get(CANDIDATE_COLLECTION_NAME)
    candidate_tenant = candidate_col.with_tenant(TENANT_ID_FOR_CV)

    success = 0
    errors  = 0

    with candidate_tenant.batch.dynamic() as batch:
        for i, cv in enumerate(cvs):
            try:
                batch.add_object(properties=transform_cv_data(cv))
                success += 1
                print(f"  [{i+1}/{len(cvs)}] {cv.get('full_name', 'N/A')} - OK")
            except Exception as e:
                errors += 1
                print(f"  [{i+1}/{len(cvs)}] ERREUR : {e}")

    print(f"\n  CVs inseres : {success} / Erreurs : {errors}")
    return success


def insert_jobs(client):
    if not JOB_JSON_PATH.exists():
        print(f"  ERREUR : {JOB_JSON_PATH} non trouve")
        return 0

    with open(JOB_JSON_PATH, encoding="utf-8") as f:
        jobs = json.load(f)

    jobs = [job for job in jobs if "error" not in job]
    print(f"\nInsertion de {len(jobs)} Jobs...")
    print("Ollama vectorise automatiquement (local, sans limite)...")

    job_col    = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)

    success = 0
    errors  = 0

    with job_tenant.batch.dynamic() as batch:
        for i, job in enumerate(jobs):
            try:
                batch.add_object(properties=transform_job_data(job))
                success += 1
                print(f"  [{i+1}/{len(jobs)}] {job.get('title', 'N/A')} - {job.get('company', 'N/A')} - OK")
            except Exception as e:
                errors += 1
                print(f"  [{i+1}/{len(jobs)}] ERREUR : {e}")

    print(f"\n  Jobs inseres : {success} / Erreurs : {errors}")
    return success


def verify_insertion(client):
    print("\n" + "=" * 50)
    print("VERIFICATION")
    print("=" * 50)

    col    = client.collections.get(CANDIDATE_COLLECTION_NAME)
    tenant = col.with_tenant(TENANT_ID_FOR_CV)
    objs   = tenant.query.fetch_objects(limit=500, include_vector=True).objects
    print(f"\n  Candidats inseres : {len(objs)}")
    if objs:
        obj = objs[0]
        print(f"  Exemple : {obj.properties.get('full_name')}")
        print(f"  Vecteurs ({len(obj.vector)}) :")
        for vec_name, vec in obj.vector.items():
            print(f"    {vec_name} : {len(vec)} dimensions")

    col    = client.collections.get(JOB_COLLECTION_NAME)
    tenant = col.with_tenant(TENANT_ID_FOR_JOBS)
    objs   = tenant.query.fetch_objects(limit=500, include_vector=True).objects
    print(f"\n  Jobs inseres : {len(objs)}")
    if objs:
        obj = objs[0]
        print(f"  Exemple : {obj.properties.get('title')} - {obj.properties.get('company')}")
        print(f"  Vecteurs ({len(obj.vector)}) :")
        for vec_name, vec in obj.vector.items():
            print(f"    {vec_name} : {len(vec)} dimensions")

    print("\nTout est pret pour le matching !")


if __name__ == "__main__":
    client = connect_weaviate()
    try:
        insert_cvs(client)
        insert_jobs(client)
        verify_insertion(client)
    finally:
        client.close()