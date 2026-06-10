"""
API/Auth_API.py
================
Authentification backend pour CV-Scanner-IA.

- Login avec email + mot de passe.
- Mots de passe stockes sous forme de hash PBKDF2-SHA256.
- Jeton JWT HS256 signe cote backend.
- Permissions verifiees cote FastAPI avant l'acces aux endpoints /api.
"""

import base64
import hashlib
import hmac
import json
import os
import time
import uuid
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

load_dotenv()

router = APIRouter()

AUTH_SECRET_KEY = os.getenv("AUTH_SECRET_KEY", "cv-scanner-local-dev-change-me")
AUTH_TOKEN_TTL_SECONDS = int(os.getenv("AUTH_TOKEN_TTL_SECONDS", "28800"))
JWT_ALGORITHM = "HS256"

Permission = str
Role = str


ROLE_PERMISSIONS: Dict[Role, List[Permission]] = {
    "admin": [
        "viewCandidates",
        "manageCandidates",
        "viewOffers",
        "manageOffers",
        "matchCandidates",
        "matchJobs",
        "freeSearch",
        "analytics",
        "profile",
        "activity",
        "rerank",
        "llm",
    ],
    "recruiter": [
        "viewCandidates",
        "manageCandidates",
        "viewOffers",
        "manageOffers",
        "matchCandidates",
        "matchJobs",
        "freeSearch",
        "analytics",
        "profile",
        "activity",
        "rerank",
        "llm",
    ],
    "manager": [
        "viewCandidates",
        "viewOffers",
        "matchCandidates",
        "matchJobs",
        "freeSearch",
        "analytics",
        "profile",
        "activity",
        "rerank",
        "llm",
    ],
    "reader": [
        "viewCandidates",
        "viewOffers",
        "analytics",
        "profile",
    ],
}


AUTH_USERS: Dict[str, Dict[str, str]] = {
    "amina.ismaili@gmail.com": {
        "name": "Amina Ismaili",
        "role": "admin",
        "password_hash": "pbkdf2_sha256$210000$dQ64-ajQx_4GZDsf$dxZBdt-WhWG5RJO48KhfLQVkj1j8BS1dsHW4Q-Dtbx4",
    },
    "recrutement.godev@gmail.com": {
        "name": "Equipe Recrutement",
        "role": "recruiter",
        "password_hash": "pbkdf2_sha256$210000$IKfqrp9u9dJqB9E7$vaf86Wn-iyyNwa_wnY3STYOIZzL2fHMuzSovU3j5RUA",
    },
    "manager.tech.godev@gmail.com": {
        "name": "Manager Technique",
        "role": "manager",
        "password_hash": "pbkdf2_sha256$210000$ilfFtLnOWozFJkCU$_fQH4-5K8iLDEa7Z6P5OprjP_HhC_TFb39Yn-0PfYwU",
    },
    "viewer.godev@gmail.com": {
        "name": "Utilisateur Lecture",
        "role": "reader",
        "password_hash": "pbkdf2_sha256$210000$t76p1DYTYmhEWi9M$xWOkc1Dry8KlTZ4cbHPB4IC7eZUb3LuQM1TSd6Tn4Y4",
    },
}


class LoginRequest(BaseModel):
    email: str
    password: str


class AuthUser(BaseModel):
    email: str
    name: str
    role: Role
    permissions: List[Permission]
    lastLoginAt: str


class LoginResponse(BaseModel):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: AuthUser


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode("ascii")


def _b64url_decode(value: str) -> bytes:
    padding = "=" * (-len(value) % 4)
    return base64.urlsafe_b64decode((value + padding).encode("ascii"))


def verify_password(password: str, stored_hash: str) -> bool:
    try:
        algorithm, iterations, salt, expected_hash = stored_hash.split("$", 3)
        if algorithm != "pbkdf2_sha256":
            return False
        digest = hashlib.pbkdf2_hmac(
            "sha256",
            password.encode("utf-8"),
            salt.encode("utf-8"),
            int(iterations),
        )
        calculated_hash = _b64url_encode(digest)
        return hmac.compare_digest(calculated_hash, expected_hash)
    except Exception:
        return False


def create_jwt(payload: Dict[str, Any]) -> str:
    header = {"alg": JWT_ALGORITHM, "typ": "JWT"}
    encoded_header = _b64url_encode(json.dumps(header, separators=(",", ":")).encode("utf-8"))
    encoded_payload = _b64url_encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))
    signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
    signature = hmac.new(AUTH_SECRET_KEY.encode("utf-8"), signing_input, hashlib.sha256).digest()
    return f"{encoded_header}.{encoded_payload}.{_b64url_encode(signature)}"


def decode_jwt(token: str) -> Dict[str, Any]:
    try:
        encoded_header, encoded_payload, encoded_signature = token.split(".", 2)
        signing_input = f"{encoded_header}.{encoded_payload}".encode("ascii")
        expected_signature = hmac.new(
            AUTH_SECRET_KEY.encode("utf-8"),
            signing_input,
            hashlib.sha256,
        ).digest()
        received_signature = _b64url_decode(encoded_signature)
        if not hmac.compare_digest(received_signature, expected_signature):
            raise ValueError("signature invalide")

        header = json.loads(_b64url_decode(encoded_header))
        if header.get("alg") != JWT_ALGORITHM:
            raise ValueError("algorithme JWT invalide")

        payload = json.loads(_b64url_decode(encoded_payload))
        if int(payload.get("exp", 0)) < int(time.time()):
            raise ValueError("token expire")
        return payload
    except Exception:
        raise HTTPException(
            status_code=401,
            detail="Token invalide ou expire",
            headers={"WWW-Authenticate": "Bearer"},
        )


def build_user(email: str, user_record: Dict[str, str], last_login_at: Optional[str] = None) -> Dict[str, Any]:
    role = user_record["role"]
    return {
        "email": email,
        "name": user_record["name"],
        "role": role,
        "permissions": ROLE_PERMISSIONS.get(role, []),
        "lastLoginAt": last_login_at or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def authenticate_token(token: str) -> Dict[str, Any]:
    payload = decode_jwt(token)
    email = str(payload.get("sub", "")).lower()
    user_record = AUTH_USERS.get(email)
    if not user_record:
        raise HTTPException(
            status_code=401,
            detail="Utilisateur inconnu",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return build_user(email, user_record, payload.get("lastLoginAt"))


def extract_bearer_token(request: Request) -> str:
    authorization = request.headers.get("Authorization", "")
    scheme, _, token = authorization.partition(" ")
    if scheme.lower() != "bearer" or not token:
        raise HTTPException(
            status_code=401,
            detail="Authentification requise",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return token.strip()


def authenticate_request(request: Request) -> Dict[str, Any]:
    token = extract_bearer_token(request)
    return authenticate_token(token)


def user_has_permission(user: Dict[str, Any], permissions: List[Permission]) -> bool:
    if not permissions:
        return True
    current_permissions = set(user.get("permissions", []))
    return any(permission in current_permissions for permission in permissions)


def required_permissions_for_request(method: str, path: str) -> Optional[List[Permission]]:
    if method.upper() == "OPTIONS":
        return None
    if not path.startswith("/api"):
        return None
    if path.startswith("/api/auth/"):
        return None

    method = method.upper()

    if path.startswith("/api/upload/"):
        return ["manageCandidates", "manageOffers"]

    if path.startswith("/api/stats/"):
        return ["analytics"]

    if path.startswith("/api/llm/"):
        return ["llm"]

    if path.startswith("/api/workflows/ingestion/"):
        return ["manageCandidates", "manageOffers"]
    if path.startswith("/api/workflows/matching"):
        return ["matchCandidates", "matchJobs"]

    if path.startswith("/api/search/rerank/"):
        return ["rerank"]
    if path.startswith("/api/search/candidates-for-job/"):
        return ["matchCandidates"]
    if path.startswith("/api/search/jobs-for-candidate/"):
        return ["matchJobs"]
    if path.startswith("/api/search/"):
        return ["freeSearch"]

    if path.startswith("/api/candidates"):
        return ["viewCandidates"] if method == "GET" else ["manageCandidates"]

    if path.startswith("/api/jobs"):
        return ["viewOffers"] if method == "GET" else ["manageOffers"]

    return []


@router.post("/auth/login", response_model=LoginResponse)
def login(payload: LoginRequest):
    email = payload.email.lower().strip()
    if "@" not in email or "." not in email:
        raise HTTPException(status_code=422, detail="Adresse email invalide")
    user_record = AUTH_USERS.get(email)
    if not user_record or not verify_password(payload.password, user_record["password_hash"]):
        raise HTTPException(status_code=401, detail="Email ou mot de passe incorrect")

    issued_at = int(time.time())
    expires_at = issued_at + AUTH_TOKEN_TTL_SECONDS
    last_login_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(issued_at))
    user = build_user(email, user_record, last_login_at)
    token = create_jwt({
        "iss": "cv-scanner-ia",
        "sub": email,
        "name": user["name"],
        "role": user["role"],
        "permissions": user["permissions"],
        "lastLoginAt": last_login_at,
        "iat": issued_at,
        "exp": expires_at,
        "jti": str(uuid.uuid4()),
    })

    return {
        "access_token": token,
        "token_type": "bearer",
        "expires_in": AUTH_TOKEN_TTL_SECONDS,
        "user": user,
    }


@router.get("/auth/me", response_model=AuthUser)
def me(request: Request):
    return authenticate_request(request)
