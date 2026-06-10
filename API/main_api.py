"""
API/main_api.py
================
Point d'entree FastAPI — CV-Scanner-IA.
Lance tous les routers : CRUD + List + Search + Upload.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import uvicorn
from fastapi import FastAPI, Request
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from API.Auth_API   import (
    router as auth_router,
    authenticate_request,
    required_permissions_for_request,
    user_has_permission,
)
from API.CRUD_API   import router as crud_router
from API.List_API   import router as list_router
from API.Search_API import router as search_router
from API.Upload_API import router as upload_router
from API.Workflow_API import router as workflow_router

# ─── APPLICATION ──────────────────────────────────────────────────────

app = FastAPI(
    title="CV-Scanner-IA API",
    description="""
API de matching semantique CV / Offres d'emploi.
Developpee dans le cadre du PFE Go & Dev.

## Fonctionnalites
- **CRUD**   : insertion et suppression de candidats et offres
- **List**   : lecture et listing depuis Weaviate
- **Search** : matching candidates_for_job, jobs_for_candidate, advanced search, reranking VoyageAI
- **Upload** : upload fichiers PDF/DOCX/TXT + liens LinkedIn avec extraction Mistral
- **Workflows** : orchestration LangGraph des processus de matching et d'ingestion
    """,
    version="1.0.0",
)

# ─── CORS ─────────────────────────────────────────────────────────────
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def auth_guard(request: Request, call_next):
    """
    Protege les endpoints /api avec un JWT Bearer.
    Les permissions sont verifiees cote backend pour bloquer les appels directs non autorises.
    """
    required_permissions = required_permissions_for_request(request.method, request.url.path)

    if required_permissions is None:
        return await call_next(request)

    try:
        user = authenticate_request(request)
        if not user_has_permission(user, required_permissions):
            return JSONResponse(
                status_code=403,
                content={
                    "detail": "Permission insuffisante",
                    "required_permissions": required_permissions,
                },
            )
        request.state.user = user
    except Exception as exc:
        status_code = getattr(exc, "status_code", 401)
        detail = getattr(exc, "detail", "Authentification requise")
        headers = getattr(exc, "headers", None)
        return JSONResponse(
            status_code=status_code,
            content={"detail": detail},
            headers=headers,
        )

    return await call_next(request)

# ─── ROUTERS ──────────────────────────────────────────────────────────
app.include_router(auth_router,   prefix="/api", tags=["Auth"])
app.include_router(crud_router,   prefix="/api", tags=["CRUD"])
app.include_router(list_router,   prefix="/api", tags=["List"])
app.include_router(search_router, prefix="/api", tags=["Search"])
app.include_router(upload_router, prefix="/api", tags=["Upload"])
app.include_router(workflow_router, prefix="/api", tags=["Workflows"])


def custom_openapi():
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title=app.title,
        version=app.version,
        description=app.description,
        routes=app.routes,
    )
    components = openapi_schema.setdefault("components", {})
    security_schemes = components.setdefault("securitySchemes", {})
    security_schemes["BearerAuth"] = {
        "type": "http",
        "scheme": "bearer",
        "bearerFormat": "JWT",
    }

    for path, operations in openapi_schema.get("paths", {}).items():
        if path.startswith("/api/auth/") or path in {"/", "/health"}:
            continue
        for operation in operations.values():
            if isinstance(operation, dict):
                operation.setdefault("security", [{"BearerAuth": []}])

    app.openapi_schema = openapi_schema
    return app.openapi_schema


app.openapi = custom_openapi

# ─── HEALTH CHECK ─────────────────────────────────────────────────────

@app.get("/", tags=["Health"])
def root():
    return {
        "status":  "ok",
        "message": "CV-Scanner-IA API is running",
        "docs":    "/docs",
    }


@app.get("/health", tags=["Health"])
def health():
    return {"status": "healthy"}


# ─── POINT D'ENTREE ───────────────────────────────────────────────────

if __name__ == "__main__":
    uvicorn.run(
        "API.main_api:app",
        host="0.0.0.0",
        port=8005,
        reload=True
    )
