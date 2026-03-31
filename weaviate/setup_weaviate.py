"""
Setup Weaviate — Creation des collections
Vectorisation : Ollama automatique (nomic-embed-text)
"""

import weaviate
import requests
import time
from weaviate.classes.config import (
    Configure, Property, DataType, VectorDistances
)
from weaviate.classes.tenants import Tenant
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

CANDIDATE_COLLECTION_NAME = "Candidate"
JOB_COLLECTION_NAME       = "Job"
TENANT_ID_FOR_CV          = "cv"
TENANT_ID_FOR_JOBS        = "job"
OLLAMA_MODEL              = "nomic-embed-text"
OLLAMA_ENDPOINT           = "http://host.docker.internal:11434"


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


def setup_weaviate():
    client = connect_weaviate()

    try:
        print(f"Embedder : Ollama {OLLAMA_MODEL}")
        print("-" * 50)

        # Suppression
        print("\nSuppression des collections existantes...")
        for name in [CANDIDATE_COLLECTION_NAME, JOB_COLLECTION_NAME]:
            if client.collections.exists(name):
                client.collections.delete(name)
                print(f"  '{name}' supprimee.")
            else:
                print(f"  '{name}' n existait pas.")

        print(f"\nCreation de '{CANDIDATE_COLLECTION_NAME}'...")

        candidate_vectors = [
            Configure.NamedVectors.text2vec_ollama(
                name="tech_skills_vector",
                source_properties=["technical_skills"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="summary_vector",
                source_properties=["summary"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="prog_langs_vector",
                source_properties=["programming_languages"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="industry_vector",
                source_properties=["industry_primary_industries"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="roles_held_vector",
                source_properties=["roles_held"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="work_experience_vector",
                source_properties=["work_experience"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="projects_vector",
                source_properties=["projects"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="location_vector",
                source_properties=["location"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
        ]

        client.collections.create(
            name=CANDIDATE_COLLECTION_NAME,
            multi_tenancy_config=Configure.multi_tenancy(enabled=True),
            vectorizer_config=candidate_vectors,
            properties=[
                Property(name="full_name",           data_type=DataType.TEXT),
                Property(name="email",               data_type=DataType.TEXT),
                Property(name="phone",               data_type=DataType.TEXT),
                Property(name="location",            data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
                Property(name="years_of_experience", data_type=DataType.INT),
                Property(name="linkedin",            data_type=DataType.TEXT),
                Property(name="github",              data_type=DataType.TEXT),
                Property(name="roles_held",          data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
                Property(name="programming_languages", data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0)),
                Property(name="technical_skills",    data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0)),
                Property(name="spoken_languages",    data_type=DataType.TEXT_ARRAY),
                Property(name="certifications",      data_type=DataType.TEXT_ARRAY),
                Property(name="seniority_technologies",
                         data_type=DataType.OBJECT_ARRAY,
                         nested_properties=[
                             Property(name="technology", data_type=DataType.TEXT,
                                      index_searchable=True),
                             Property(name="level",      data_type=DataType.TEXT,
                                      index_searchable=True),
                         ]),
                Property(name="seniority_programming_languages",
                         data_type=DataType.OBJECT_ARRAY,
                         nested_properties=[
                             Property(name="language", data_type=DataType.TEXT,
                                      index_searchable=True),
                             Property(name="level",    data_type=DataType.TEXT,
                                      index_searchable=True),
                         ]),
                Property(name="industry_primary_industries",
                         data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
                Property(name="summary",             data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2), index_searchable=True),
                Property(name="education_level",     data_type=DataType.TEXT),
                Property(name="field_of_studies",    data_type=DataType.TEXT),
                Property(name="work_experience",     data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
                Property(name="projects",            data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
                Property(name="file_path",           data_type=DataType.TEXT),
                Property(name="parsing_confidence",  data_type=DataType.NUMBER),
                Property(name="career_trajectory_direction",         data_type=DataType.TEXT),
                Property(name="career_trajectory_predicted_profile", data_type=DataType.TEXT),
                Property(name="career_trajectory_progression_speed", data_type=DataType.TEXT),
                Property(name="timeline_companies",  data_type=DataType.TEXT_ARRAY),
                Property(name="timeline_roles",      data_type=DataType.TEXT_ARRAY),
            ]
        )

        col = client.collections.get(CANDIDATE_COLLECTION_NAME)
        if not col.tenants.exists(TENANT_ID_FOR_CV):
            col.tenants.create([Tenant(name=TENANT_ID_FOR_CV)])
        print(f"  '{CANDIDATE_COLLECTION_NAME}' cree avec 8 vecteurs Ollama.")

        print(f"\nCreation de '{JOB_COLLECTION_NAME}'...")

        job_vectors = [
            Configure.NamedVectors.text2vec_ollama(
                name="job_tech_skills_vector",
                source_properties=["technical_skills"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="job_summary_vector",
                source_properties=["summary"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="job_prog_langs_vector",
                source_properties=["programming_languages"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="job_industry_vector",
                source_properties=["industry"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="job_title_vector",
                source_properties=["title"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
            Configure.NamedVectors.text2vec_ollama(
                name="job_description_vector",
                source_properties=["job_description"],
                model=OLLAMA_MODEL,
                api_endpoint=OLLAMA_ENDPOINT,
                vector_index_config=Configure.VectorIndex.hnsw(
                    distance_metric=VectorDistances.COSINE)
            ),
        ]

        client.collections.create(
            name=JOB_COLLECTION_NAME,
            multi_tenancy_config=Configure.multi_tenancy(enabled=True),
            vectorizer_config=job_vectors,
            properties=[
                Property(name="title",           data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2), index_searchable=True),
                Property(name="company",         data_type=DataType.TEXT),
                Property(name="industry",        data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2), index_searchable=True),
                Property(name="location",        data_type=DataType.TEXT),
                Property(name="employment_type", data_type=DataType.TEXT),
                Property(name="job_description", data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2), index_searchable=True),
                Property(name="posted",          data_type=DataType.TEXT),
                Property(name="programming_languages", data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2)),
                Property(name="technical_skills",     data_type=DataType.TEXT_ARRAY,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0.75, bm25_k1=1.2)),
                Property(name="spoken_languages",     data_type=DataType.TEXT_ARRAY),
                Property(name="certifications",       data_type=DataType.TEXT_ARRAY),
                Property(name="seniority_requirements_technologies",
                         data_type=DataType.OBJECT_ARRAY,
                         nested_properties=[
                             Property(name="technology", data_type=DataType.TEXT,
                                      index_searchable=True),
                             Property(name="level",      data_type=DataType.TEXT,
                                      index_searchable=True),
                         ]),
                Property(name="seniority_requirements_programming_languages",
                         data_type=DataType.OBJECT_ARRAY,
                         nested_properties=[
                             Property(name="language", data_type=DataType.TEXT,
                                      index_searchable=True),
                             Property(name="level",    data_type=DataType.TEXT,
                                      index_searchable=True),
                         ]),
                Property(name="experience_level",             data_type=DataType.TEXT),
                Property(name="salary_range",                 data_type=DataType.TEXT),
                Property(name="education_requirements",       data_type=DataType.TEXT),
                Property(name="years_of_experience_required", data_type=DataType.INT),
                Property(name="summary",                      data_type=DataType.TEXT,
                         inverted_index_config=Configure.inverted_index(
                             bm25_b=0, bm25_k1=2.0), index_searchable=True),
            ]
        )

        job_col = client.collections.get(JOB_COLLECTION_NAME)
        if not job_col.tenants.exists(TENANT_ID_FOR_JOBS):
            job_col.tenants.create([Tenant(name=TENANT_ID_FOR_JOBS)])
        print(f"  '{JOB_COLLECTION_NAME}' cree avec 6 vecteurs Ollama.")

        # Verification
        print("\n" + "=" * 50)
        print("VERIFICATION")
        print("=" * 50)
        for name in [CANDIDATE_COLLECTION_NAME, JOB_COLLECTION_NAME]:
            col    = client.collections.get(name)
            config = col.config.get()
            vecs   = list(config.vector_config.keys())
            print(f"\n  {name}")
            print(f"    Vecteurs ({len(vecs)}) : {', '.join(vecs)}")

        print("\nSetup termine ! Lance : python insert_data.py")

    finally:
        client.close()


if __name__ == "__main__":
    setup_weaviate()