import os
from pathlib import Path
from dotenv import load_dotenv

# Chemin racine du projet
BASE_DIR = Path(__file__).resolve().parent.parent

# Chargement .env
load_dotenv(dotenv_path=BASE_DIR / ".env")

# ─── WEAVIATE ─────────────────────────────────────────────────────────
WEAVIATE_HOST = "localhost"
WEAVIATE_PORT = 8087
WEAVIATE_GRPC_PORT = 50051

# ─── COLLECTIONS ──────────────────────────────────────────────────────
CANDIDATE_COLLECTION_NAME = "Candidate"
JOB_COLLECTION_NAME       = "Job"

# ─── TENANTS ──────────────────────────────────────────────────────────
TENANT_ID_FOR_CV   = "cv"
TENANT_ID_FOR_JOBS = "job"

# ─── MODELES ──────────────────────────────────────────────────────────
MODEL_NAME      = "mistral-large-latest"
MODEL_PROVIDER  = "mistralai"
EMBEDDER_INFO   = "voyage-3 via text2vec-voyageai"

# ─── CHEMINS DONNEES ──────────────────────────────────────────────────
DATA_DIR        = BASE_DIR / "data" / "data" / "real_data"
CANDIDATES_FILE = DATA_DIR / "extracted_cvs_real_final.json"
JOBS_FILE       = DATA_DIR / "extracted_jobs_real.json"

# ─── API KEYS ─────────────────────────────────────────────────────────
MISTRAL_API_KEY = os.getenv("MISTRAL_API_KEY", "")
VOYAGEAI_API_KEY = os.getenv("VOYAGEAI_APIKEY", "")


