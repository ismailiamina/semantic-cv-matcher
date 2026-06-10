"""
API/Upload_API.py
==================
Upload fichiers (PDF/DOCX/TXT) et liens LinkedIn.
- Réutilise CandidatePayload, JobPayload, add_candidate_endpoint, add_job_endpoint depuis CRUD_API.py
- Utilise les schemas JSON et prompts exacts définis dans le projet
- Mistral Small pour extraction (cohérent avec le reste du projet)
"""
import sys, os, json, tempfile, re, asyncio, unicodedata
from pathlib import Path
from datetime import datetime
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import requests as req_lib
from fastapi import APIRouter, HTTPException, UploadFile, File, Form
from pydantic import BaseModel
from dotenv import load_dotenv

# ── Réutilisation directe des endpoints et modèles CRUD existants ─────
from API.CRUD_API import (
    add_candidate_endpoint,
    add_job_endpoint,
    CandidatePayload,
    JobPayload,
    get_client,
)
from weaviate_db.List import get_all_candidates, get_all_jobs

_p = Path(__file__).resolve().parent
for _ in range(4):
    if (_p / ".env").exists():
        load_dotenv(dotenv_path=_p / ".env"); break
    _p = _p.parent

router    = APIRouter()
CURRENT_YEAR = datetime.now().year


# ─── SCHEMAS JSON EXACTS DU PROJET ────────────────────────────────────

CV_JSON_SCHEMA = {
    "title": "CandidateProfile",
    "type": "object",
    "properties": {
        "full_name":             {"type": "string"},
        "email":                 {"type": "string"},
        "phone":                 {"type": "string"},
        "location":              {"type": "string"},
        "years_of_experience":   {"type": "number"},
        "linkedin":              {"type": "string"},
        "github":                {"type": "string"},
        "roles_held":            {"type": "array", "items": {"type": "string"}},
        "programming_languages": {"type": "array", "items": {"type": "string"}},
        "technical_skills":      {"type": "array", "items": {"type": "string"}},
        "spoken_languages":      {"type": "array", "items": {"type": "string"}},
        "certifications":        {"type": "array", "items": {"type": "string"}},
        "seniority_technologies": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "technology": {"type": "string"},
                    "level":      {"type": "string",
                                   "enum": ["Junior","Medior","Confirmé","Senior","Expert"]}
                },
                "required": ["technology","level"]
            }
        },
        "seniority_programming_languages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {"type": "string"},
                    "level":    {"type": "string",
                                 "enum": ["Junior","Medior","Confirmé","Senior","Expert"]}
                },
                "required": ["language","level"]
            }
        },
        "industry": {
            "type": "object",
            "properties": {
                "primary_industries": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["primary_industries"]
        },
        "summary":            {"type": "string"},
        "education_level": {
            "type": "string",
            "enum": ["Bac + 2","Bac + 3","Bac + 4","Bac + 5","Bac + 6 et plus"]
        },
        "field_of_studies":   {"type": "string"},
        "work_experience":    {"type": "string"},
        "projects":           {"type": "string"},
        "parsing_confidence": {"type": "number"},
        "experience_timeline": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "year_start":      {"type": "number"},
                    "year_end":        {"type": "number"},
                    "company":         {"type": "string"},
                    "role":            {"type": "string"},
                    "skills_acquired": {"type": "array","items": {"type": "string"}},
                    "languages_used":  {"type": "array","items": {"type": "string"}},
                    "description":     {"type": "string"}
                },
                "required": ["year_start","year_end","company","role"]
            }
        },
        "career_trajectory": {
            "type": "object",
            "properties": {
                "direction":          {"type": "string"},
                "progression_speed":  {"type": "string",
                                       "enum": ["Rapide","Normale","Lente"]},
                "predicted_profile":  {"type": "string"},
                "skills_in_progress": {"type": "array","items": {"type": "string"}}
            },
            "required": ["direction","progression_speed","predicted_profile"]
        }
    },
    "required": [
        "full_name","roles_held","programming_languages","technical_skills",
        "industry","summary","experience_timeline","career_trajectory"
    ]
}

JOB_JSON_SCHEMA = {
    "title": "JobProfile",
    "type": "object",
    "properties": {
        "title":           {"type": "string"},
        "company":         {"type": "string"},
        "industry":        {"type": "string"},
        "location":        {"type": "string"},
        "employment_type": {
            "type": "string",
            "enum": ["Full-time","Part-time","Fixed-term",
                     "Casual","Temporary","Internship","not specified"]
        },
        "job_description": {"type": "string"},
        "posted":          {"type": "string"},
        "programming_languages": {"type": "array", "items": {"type": "string"}},
        "technical_skills":      {"type": "array", "items": {"type": "string"}},
        "spoken_languages":      {"type": "array", "items": {"type": "string"}},
        "certifications":        {"type": "array", "items": {"type": "string"}},
        "seniority_requirements_technologies": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "technology": {"type": "string"},
                    "level":      {"type": "string",
                                   "enum": ["Junior","Medior","Confirme","Senior","Expert"]}
                },
                "required": ["technology","level"]
            }
        },
        "seniority_requirements_programming_languages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {"type": "string"},
                    "level":    {"type": "string",
                                 "enum": ["Junior","Medior","Confirme","Senior","Expert"]}
                },
                "required": ["language","level"]
            }
        },
        "experience_level": {
            "type": "string",
            "enum": ["Junior","Medior","Confirme","Senior","Expert"]
        },
        "salary_range":                 {"type": "string"},
        "education_requirements":       {"type": "string"},
        "years_of_experience_required": {"type": "number"},
        "summary":                      {"type": "string"}
    },
    "required": [
        "title","company","job_description",
        "programming_languages","technical_skills",
        "certifications","summary"
    ]
}


# ─── PROMPTS EXACTS DU PROJET ─────────────────────────────────────────

CV_PROMPT = f"""Tu reçois le contenu texte COMPLET d'un profil LinkedIn organisé par sections.
Structure ces informations en JSON.

════════════════════════════════════════
RÈGLES ABSOLUES — NE JAMAIS VIOLER
════════════════════════════════════════

RÈGLE 1 — ANTI-INVENTION (CRITIQUE) :
Tu n'as le droit d'ajouter UNE compétence, UN langage, UNE certification, UNE expérience
QUE SI ce mot apparaît EXPLICITEMENT dans le texte fourni.
INTERDIT ABSOLU :
  ✗ Déduire qu'il connaît Python parce qu'il fait du ML
  ✗ Ajouter Docker parce qu'il fait du DevOps
  ✗ Supposer Java parce qu'il travaille chez une ESN
  ✗ Inventer un poste manquant pour combler un trou
Si tu n'es pas certain à 100% qu'un élément est écrit dans le texte → NE L'AJOUTE PAS.
Si une personne ne contient pas de compétences techniques ou de langage de programmation écrit null.

RÈGLE 2 — ANTI-OUBLI (CRITIQUE) :
Avant de retourner le JSON, effectue ces vérifications :
  ✓ Relis la section COMPÉTENCES du texte → chaque compétence listée doit être dans technical_skills ou programming_languages
  ✓ Relis la section EXPÉRIENCES du texte → chaque entreprise et poste doit être dans experience_timeline
  ✓ Relis la section CERTIFICATIONS → chaque certification doit être dans certifications
  ✓ Relis la section FORMATIONS → l'école et le diplôme doivent être dans education_level et field_of_studies
  ✓ Relis la section PROJETS → les projets doivent être dans projects

RÈGLE 3 — EXPERIENCE_TIMELINE (CRITIQUE) :
Extrais CHAQUE poste comme une entrée séparée avec year_start ET year_end OBLIGATOIRES.
Interprétation des dates LinkedIn :
  "janv. 2018 - Présent"         → year_start=2018, year_end={CURRENT_YEAR}
  "mars 2020 - août 2022"        → year_start=2020, year_end=2022
  "Présent" / "aujourd'hui"      → year_end={CURRENT_YEAR}

RÈGLE 4 — YEARS_OF_EXPERIENCE :
Un poste est un STAGE uniquement si : "Internship", "Stage", "Stagiaire", "Alternance", "PFE", "PFA"
years_of_experience = CURRENT_YEAR - year_start_du_premier_poste_reel

RÈGLE 5 — SÉNIORITÉ :
  0-2 ans → Junior | 2-4 ans → Medior | 4-6 ans → Confirmé
  6-10 ans → Senior | 10+ ans → Expert

RÈGLE 6 — PROGRAMMING_LANGUAGES vs TECHNICAL_SKILLS :
  programming_languages = langages de code UNIQUEMENT : Python, Java, JavaScript, TypeScript, SQL, C, C++, C#, Go, Ruby, PHP...
  technical_skills = outils, frameworks : Docker, Kubernetes, AWS, Spring, React, Angular, Git...

Retourne UNIQUEMENT un JSON valide sans backticks ni commentaires.
Schema attendu : {json.dumps(CV_JSON_SCHEMA, ensure_ascii=False)}

CV :"""

# Override volontaire: prompt CV court pour stabiliser l'extraction des fichiers uploades.
CV_PROMPT = """Tu recois le texte brut d'un CV candidat.
Extrais uniquement les champs suivants dans un JSON :
full_name, roles_held, programming_languages, technical_skills, summary,
experience_timeline, career_trajectory, years_of_experience, location, company_source.

Regles :
- full_name est obligatoire : cherche le nom complet du candidat, souvent en haut du CV.
- programming_languages contient uniquement des langages de code.
- technical_skills contient frameworks, outils, plateformes et methodes.
- experience_timeline contient les experiences clairement presentes dans le texte.
- company_source est l'entreprise actuelle ou la derniere entreprise professionnelle du candidat.
- Pour company_source, ne prends jamais l'ecole, la formation, les profils recommandes LinkedIn ou les entreprises citees dans les posts.
- Si le texte contient une section "Experience", company_source doit venir de la premiere entreprise sous cette section, avant "Formation" ou "Benevolat".
- years_of_experience est un nombre entier estime depuis les experiences reelles.
- N'invente aucune information absente du texte.
- Si une information est absente, mets "not found".

Retourne UNIQUEMENT un JSON valide sans backticks ni commentaires.

CV :"""

JOB_PROMPT = f"""Tu reçois le texte d'une offre d'emploi IT.
Structure ces informations en JSON.

RÈGLES ABSOLUES :
1. Utilise UNIQUEMENT les informations présentes dans le texte
2. N'invente RIEN — si absent : "not specified" ou [] ou 0
3. title : titre exact du poste
4. company : nom exact de l'entreprise qui publie l'offre
5. employment_type : Full-time / Part-time / Fixed-term / Internship / not specified
6. programming_languages : langages de code uniquement (Python, Java, SQL, JavaScript...)
7. technical_skills : outils, frameworks, plateformes, méthodes et compétences opérationnelles IT
8. experience_level : Junior(0-2ans) / Medior(2-4ans) / Confirme(4-6ans) / Senior(6-10ans) / Expert(10+ans)
9. seniority_requirements_technologies : top 3 technologies avec niveau attendu
10. seniority_requirements_programming_languages : tous les langages explicitement requis avec niveau attendu
11. salary_range : si mentionné, sinon "not specified"
12. education_requirements : Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus / not specified
13. years_of_experience_required : nombre entier, 0 si non mentionné
14. summary : 2-3 phrases résumant le poste, l'entreprise et les compétences clés
15. posted : date si mentionnée (YYYY-MM-DD), sinon date du jour

ANTI-INVENTION :
Ne jamais ajouter une compétence ou un langage qui n'est pas explicitement écrit dans le texte.

ANTI-OUBLI :
- Relis les sections "missions", "profil", "compétences", "environnement" et "prérequis".
- Chaque technologie, langage, outil, méthode ou compétence IT explicitement citée doit être présente dans programming_languages ou technical_skills.
- Respecte les nuances du texte : "excellente maîtrise" indique un niveau fort, alors que "bonne connaissance" indique un niveau attendu moins élevé. Ne transforme pas "bonne connaissance en Java" en "maîtrise de Java".
- Si un contrat CDI est mentionné, employment_type doit être "Full-time".
- Si ASAP est mentionné, ne l'utilise pas comme date de publication ; conserve posted à "not specified".

Retourne UNIQUEMENT un JSON valide sans backticks ni commentaires.
Schema attendu : {json.dumps(JOB_JSON_SCHEMA, ensure_ascii=False)}

Offre :"""

LINKEDIN_CV_PROMPT = f"""Tu recois le texte NETTOYE d'un profil LinkedIn candidat, organise par sections.
Ta mission est d'extraire un profil candidat fiable en respectant strictement le schema JSON.

Regles critiques :
- Retourne UNIQUEMENT un JSON valide sans backticks ni commentaires.
- N'invente jamais une information absente du texte.
- N'ajoute une competence, un langage, une certification ou une experience QUE si elle apparait explicitement.
- Ignore les boutons, menus, suggestions, profils recommandes, posts, commentaires, reactions et personnes similaires.
- Les posts LinkedIn ne sont PAS des experiences professionnelles.
- La section EXPERIENCE est prioritaire pour company_source, roles_held, work_experience et experience_timeline.
- La section SKILLS/COMPETENCES est prioritaire pour programming_languages et technical_skills.
- programming_languages contient uniquement les langages de code : Python, Java, JavaScript, TypeScript, SQL, C#, PHP, etc.
- technical_skills contient frameworks, outils, plateformes, methodes et technologies : React, Spring Boot, Docker, AWS, SQL Server, etc.
- Si une information est absente, mets "not found" pour les champs texte ou [] pour les listes.
- full_name est obligatoire et doit venir de l'en-tete du profil.
- company_source est l'entreprise actuelle ou la derniere entreprise professionnelle dans EXPERIENCE.
- N'utilise jamais une ecole, une formation, un profil recommande ou une entreprise citee dans un post comme company_source.
- years_of_experience est estime depuis les experiences professionnelles reelles.

Schema attendu : {json.dumps(CV_JSON_SCHEMA, ensure_ascii=False)}

Profil LinkedIn nettoye :
"""

LINKEDIN_JOB_PROMPT = f"""Tu recois le texte NETTOYE d'une offre LinkedIn.
Structure cette offre en JSON fiable en respectant strictement le schema.

Regles critiques :
- Retourne UNIQUEMENT un JSON valide sans backticks ni commentaires.
- N'invente jamais une information absente du texte.
- Ignore les boutons, menus, recommandations, offres similaires et contenus hors offre.
- title est obligatoire et doit correspondre au titre exact de l'offre.
- company est le nom de l'entreprise qui publie l'offre.
- programming_languages contient uniquement les langages de code.
- technical_skills contient frameworks, outils, plateformes et methodes.
- Si une information est absente : "not specified", [] ou 0 selon le champ.

Schema attendu : {json.dumps(JOB_JSON_SCHEMA, ensure_ascii=False)}

Offre LinkedIn nettoyee :
"""


# ─── EXTRACTION TEXTE ─────────────────────────────────────────────────

def extract_text_from_bytes(content: bytes, filename: str) -> str:
    """Extrait le texte brut depuis PDF, DOCX ou TXT."""
    filename = filename.lower()
    if filename.endswith(".pdf"):
        try:
            import fitz
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                tmp.write(content); tmp_path = tmp.name
            doc  = fitz.open(tmp_path)
            text = "\n".join(page.get_text() for page in doc)
            doc.close(); os.unlink(tmp_path)
            return text
        except ImportError:
            raise HTTPException(status_code=500, detail="pymupdf non installé")
    elif filename.endswith(".docx"):
        try:
            import docx
            with tempfile.NamedTemporaryFile(suffix=".docx", delete=False) as tmp:
                tmp.write(content); tmp_path = tmp.name
            doc  = docx.Document(tmp_path)
            text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
            os.unlink(tmp_path)
            return text
        except ImportError:
            raise HTTPException(status_code=500, detail="python-docx non installé")
    elif filename.endswith(".txt"):
        try:    return content.decode("utf-8")
        except: return content.decode("latin-1", errors="ignore")
    else:
        raise HTTPException(status_code=400, detail="Format non supporté. Utilisez PDF, DOCX ou TXT.")


# ─── EXTRACTION LINKEDIN ──────────────────────────────────────────────

def scrape_linkedin_text(url: str) -> str:
    """Scrape une page LinkedIn publique via Selenium."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        import time
        opts = Options()
        opts.add_argument("--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
        driver = webdriver.Chrome(options=opts)
        driver.get(url)
        time.sleep(3)
        text = driver.find_element(By.TAG_NAME, "body").text
        driver.quit()
        return text[:8000]
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping LinkedIn échoué : {e}")


# ─── EXTRACTION MISTRAL SMALL ─────────────────────────────────────────

LINKEDIN_BLOCKERS = (
    "sign in to view",
    "join linkedin",
    "authwall",
    "checkpoint",
    "security verification",
    "unusual traffic",
    "something went wrong",
    "page not found",
)

LINKEDIN_NOISE_PREFIXES = (
    "voir le profil",
    "suivre",
    "message",
    "plus",
    "j'aime",
    "commenter",
    "republier",
    "envoyer",
    "afficher plus",
    "tout afficher",
    "s'identifier",
    "inscrivez-vous",
    "rejoignez linkedin",
    "les personnes qui suivent",
    "people also viewed",
    "publicite",
    "promoted",
)

LINKEDIN_TECHNICAL_NOISE_TOKENS = (
    "como-t:",
    "como-err:",
    "trackingid",
    "trackingId",
    "pageforestid",
    "pageForestId",
    "applicationurn",
    "applicationUrn",
    "apptrackingid",
    "appTrackingId",
    "serviceversion",
    "serviceVersion",
    "treeid",
    "treeId",
    "viewport:",
    "csrf",
    "clientapplicationinstance",
    "clientPageInstanceId",
    "voyager-web",
    "flagship-web",
)

LINKEDIN_SECTION_ALIASES = {
    "infos": "ABOUT",
    "about": "ABOUT",
    "experience": "EXPERIENCE",
    "expérience": "EXPERIENCE",
    "expÃ©rience": "EXPERIENCE",
    "formation": "EDUCATION",
    "education": "EDUCATION",
    "compétences": "SKILLS",
    "compÃ©tences": "SKILLS",
    "competences": "SKILLS",
    "skills": "SKILLS",
    "licences et certifications": "CERTIFICATIONS",
    "certifications": "CERTIFICATIONS",
    "projets": "PROJECTS",
    "projects": "PROJECTS",
    "bénévolat": "VOLUNTEERING",
    "bÃ©nÃ©volat": "VOLUNTEERING",
    "benevolat": "VOLUNTEERING",
    "activité": "ACTIVITY",
    "activitÃ©": "ACTIVITY",
    "activite": "ACTIVITY",
    "activity": "ACTIVITY",
}

LINKEDIN_CANDIDATE_AUTHWALL_TOKENS = (
    "passer au contenu principal",
    "s'identifier",
    "s’identifier",
    "mot de passe",
    "mot de passe oublie",
    "mot de passe oublié",
    "nouveau sur linkedin",
    "e-mail ou telephone",
    "e-mail ou téléphone",
    "email or phone",
)


def env_flag(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    if value is None:
        return default
    return value.strip().lower() in {"1", "true", "yes", "on", "oui"}


def get_linkedin_chrome_user_data_dir() -> str:
    return os.getenv("LINKEDIN_CHROME_USER_DATA_DIR", "").strip().strip('"')


def get_linkedin_chrome_profile() -> str:
    return os.getenv("LINKEDIN_CHROME_PROFILE", "").strip()


def get_linkedin_playwright_user_data_dir() -> str:
    return (
        os.getenv("LINKEDIN_PLAYWRIGHT_USER_DATA_DIR", "").strip().strip('"')
        or get_linkedin_chrome_user_data_dir()
    )


def looks_like_candidate_authwall(raw_text: str) -> bool:
    lowered = (raw_text or "").lower()
    login_hits = sum(1 for token in LINKEDIN_CANDIDATE_AUTHWALL_TOKENS if token in lowered)
    useful_profile_markers = (
        "experience" in lowered or
        "expérience" in lowered or
        "expÃ©rience" in lowered or
        "formation" in lowered or
        "compétences" in lowered or
        "compÃ©tences" in lowered or
        "skills" in lowered
    )
    return login_hits >= 2 and not useful_profile_markers


def normalize_linkedin_line(line: str) -> str:
    line = re.sub(r"\s+", " ", line or "").strip()
    return line.strip("•*-| ")


def is_linkedin_noise(line: str) -> bool:
    lowered = normalize_linkedin_line(line).lower()
    if not lowered or len(lowered) <= 1:
        return True
    if lowered in {"voir plus", "show more", "see more", "like", "comment", "share"}:
        return True
    if any(token.lower() in lowered for token in LINKEDIN_TECHNICAL_NOISE_TOKENS):
        return True
    return any(lowered.startswith(prefix) for prefix in LINKEDIN_NOISE_PREFIXES)


def clean_linkedin_text(raw_text: str, url: str, page_type: str) -> str:
    """
    Nettoie le texte LinkedIn avant l'envoi au LLM.
    Objectif: garder les informations du profil/offre et retirer le bruit UI.
    """
    if not raw_text or len(raw_text.strip()) < 300:
        raise HTTPException(
            status_code=422,
            detail="LinkedIn n'a pas retourne assez de contenu exploitable. Utilisez un profil public ou uploadez un fichier."
        )

    lowered_raw = raw_text.lower()
    if any(token in lowered_raw for token in LINKEDIN_BLOCKERS):
        raise HTTPException(
            status_code=422,
            detail="LinkedIn bloque ou masque ce contenu. Connectez-vous, utilisez un profil public ou uploadez un fichier."
        )

    lines = []
    seen = set()
    for raw_line in raw_text.splitlines():
        line = normalize_linkedin_line(raw_line)
        if is_linkedin_noise(line):
            continue
        key = line.lower()
        if key in seen:
            continue
        seen.add(key)
        lines.append(line)

    if len(lines) < 8:
        raise HTTPException(
            status_code=422,
            detail="Le scraping LinkedIn n'a pas retourne assez de lignes fiables pour l'extraction."
        )

    sections = {"HEADER": []}
    current = "HEADER"
    for line in lines:
        alias = LINKEDIN_SECTION_ALIASES.get(line.lower())
        if alias:
            current = alias
            sections.setdefault(current, [])
            continue
        sections.setdefault(current, []).append(line)

    ordered_sections = [
        "HEADER",
        "ABOUT",
        "EXPERIENCE",
        "EDUCATION",
        "SKILLS",
        "CERTIFICATIONS",
        "PROJECTS",
        "VOLUNTEERING",
    ]
    if page_type == "job":
        ordered_sections = ["HEADER", "ABOUT", "EXPERIENCE", "SKILLS", "CERTIFICATIONS"]

    output = [
        f"URL_SOURCE: {url}",
        f"TYPE_SOURCE: LINKEDIN_{page_type.upper()}",
        "CONSIGNE_SOURCE: utiliser uniquement les informations ci-dessous.",
    ]
    for section in ordered_sections:
        values = sections.get(section) or []
        if not values:
            continue
        output.append(f"\nSECTION: {section}")
        output.extend(values[:120])

    cleaned = "\n".join(output).strip()
    print(f"[Upload] LinkedIn lignes utiles : {len(lines)}")
    print(f"[Upload] LinkedIn texte nettoye : {len(cleaned)} caracteres")
    print(f"[Upload] LinkedIn apercu : {cleaned[:700].replace(chr(10), ' | ')}")
    return cleaned[:24000]


def extract_linkedin_job_id(url: str) -> str:
    """Extrait le job_id depuis les formats d'URL LinkedIn Jobs les plus courants."""
    patterns = [
        r"/jobs/view/(\d+)",
        r"[?&]currentJobId=(\d+)",
        r"[?&]jobId=(\d+)",
        r"/jobs/collections/[^?]+[?&]currentJobId=(\d+)",
    ]
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    return ""


def html_to_text(html: str) -> str:
    """Convertit du HTML LinkedIn en texte propre, avec fallback si bs4 n'est pas disponible."""
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg"]):
            tag.decompose()
        text = soup.get_text("\n")
    except Exception:
        text = re.sub(r"(?is)<(script|style|noscript).*?>.*?</\1>", "\n", html)
        text = re.sub(r"(?s)<[^>]+>", "\n", text)

    text = re.sub(r"&nbsp;?", " ", text)
    text = re.sub(r"&amp;?", "&", text)
    text = re.sub(r"&lt;?", "<", text)
    text = re.sub(r"&gt;?", ">", text)
    lines = [normalize_linkedin_line(line) for line in text.splitlines()]
    return "\n".join(line for line in lines if line)


def scrape_linkedin_job_guest_text(url: str) -> str:
    """
    Recupere une offre LinkedIn via l'endpoint public jobs-guest.
    Ce chemin evite Selenium et ne necessite pas de session LinkedIn.
    """
    job_id = extract_linkedin_job_id(url)
    if not job_id:
        raise HTTPException(
            status_code=422,
            detail="Impossible d'extraire le job_id depuis l'URL LinkedIn."
        )

    guest_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
    print(f"[Upload] LinkedIn job_id : {job_id}")
    print(f"[Upload] LinkedIn jobs-guest : {guest_url}")

    try:
        response = req_lib.get(
            guest_url,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
                ),
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "Accept-Language": "fr-FR,fr;q=0.9,en;q=0.8",
            },
            timeout=30,
        )
        if response.status_code == 404:
            raise HTTPException(status_code=404, detail="Offre LinkedIn introuvable via jobs-guest.")
        response.raise_for_status()
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Recuperation LinkedIn jobs-guest echouee : {e}")

    raw_text = html_to_text(response.text)
    enriched_text = (
        f"URL_SOURCE: {url}\n"
        f"JOB_ID_SOURCE: {job_id}\n"
        f"TYPE_SOURCE: LINKEDIN_JOB_GUEST\n"
        "CONSIGNE_SOURCE: utiliser uniquement les informations ci-dessous.\n\n"
        f"{raw_text}"
    )
    cleaned = clean_linkedin_text(enriched_text, url, "job")
    if len(cleaned) < 500:
        raise HTTPException(
            status_code=422,
            detail="L'offre LinkedIn jobs-guest ne contient pas assez de texte exploitable."
        )
    return cleaned


LINKEDIN_CANDIDATE_DETAIL_SECTIONS = (
    ("EXPERIENCE_DETAIL", "details/experience/"),
    ("EDUCATION_DETAIL", "details/education/"),
    ("SKILLS_DETAIL", "details/skills/"),
    ("CERTIFICATIONS_DETAIL", "details/certifications/"),
    ("PROJECTS_DETAIL", "details/projects/"),
    ("VOLUNTEERING_DETAIL", "details/volunteering-experiences/"),
)


def normalize_linkedin_profile_url(url: str) -> str:
    base = url.split("?")[0].split("#")[0].rstrip("/")
    return base + "/"


def extract_playwright_page_text(page) -> str:
    return page.evaluate(
        """
        () => {
          const meta = Array.from(document.querySelectorAll('meta[name], meta[property]'))
            .map(m => `${m.getAttribute('name') || m.getAttribute('property')}: ${m.getAttribute('content') || ''}`)
            .filter(Boolean)
            .join('\\n');
          const jsonLd = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
            .map(s => s.innerText || s.textContent || '')
            .join('\\n');
          const body = document.body.innerText || document.body.textContent || '';
          return `PAGE_TITLE: ${document.title}\\n${meta}\\n${jsonLd}\\n${body}`;
        }
        """
    )


def expand_linkedin_visible_sections(page) -> None:
    for _ in range(5):
        clicked = page.locator(
            "button:has-text('Voir plus'), "
            "button:has-text('voir plus'), "
            "button:has-text('Tout afficher'), "
            "button:has-text('tout afficher'), "
            "button:has-text('Show more'), "
            "button:has-text('See more')"
        )
        count = min(clicked.count(), 16)
        if count == 0:
            break
        did_click = False
        for index in range(count):
            try:
                clicked.nth(index).click(timeout=1500, force=True)
                page.wait_for_timeout(350)
                did_click = True
            except Exception:
                continue
        if not did_click:
            break


def scroll_linkedin_page(page, steps: int = 7) -> None:
    last_height = 0
    for _ in range(steps):
        page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
        page.wait_for_timeout(900)
        height = page.evaluate("document.body.scrollHeight")
        if height == last_height:
            break
        last_height = height


def collect_linkedin_candidate_detail_pages(page, profile_url: str, timeout_error_cls) -> str:
    base_url = normalize_linkedin_profile_url(profile_url)
    parts = []
    for section_name, suffix in LINKEDIN_CANDIDATE_DETAIL_SECTIONS:
        detail_url = base_url + suffix
        try:
            page.goto(detail_url, wait_until="domcontentloaded", timeout=35000)
            try:
                page.wait_for_load_state("networkidle", timeout=8000)
            except timeout_error_cls:
                pass
            expand_linkedin_visible_sections(page)
            scroll_linkedin_page(page, steps=5)
            section_text = extract_playwright_page_text(page)
            if section_text and not looks_like_candidate_authwall(section_text):
                parts.append(f"\n=== {section_name} ===\n{section_text}")
                print(f"[Upload] LinkedIn detail collectee : {section_name}")
        except Exception as e:
            print(f"[Upload] LinkedIn detail ignoree {section_name} : {str(e)[:180]}")
            continue
    return "\n".join(parts)


def scrape_linkedin_candidate_playwright_text_sync(url: str) -> str:
    """
    Recupere un profil candidat LinkedIn avec Playwright et un profil persistant.
    Le profil doit deja etre connecte a LinkedIn.
    """
    user_data_dir = get_linkedin_playwright_user_data_dir()
    if not user_data_dir:
        raise HTTPException(
            status_code=422,
            detail=(
                "Configuration Playwright LinkedIn manquante. "
                "Ajoutez LINKEDIN_PLAYWRIGHT_USER_DATA_DIR ou LINKEDIN_CHROME_USER_DATA_DIR dans .env."
            )
        )

    headless = env_flag("LINKEDIN_HEADLESS", False)
    browser_context = None
    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except ImportError:
        raise HTTPException(
            status_code=500,
            detail=(
                "Playwright n'est pas installe. Executez : "
                "pip install playwright puis python -m playwright install chromium"
            )
        )

    try:
        with sync_playwright() as p:
            browser_context = p.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=headless,
                locale="fr-FR",
                viewport={"width": 1440, "height": 2200},
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            page = browser_context.pages[0] if browser_context.pages else browser_context.new_page()
            page.set_default_timeout(20000)
            page.goto(url, wait_until="domcontentloaded", timeout=45000)

            try:
                page.wait_for_load_state("networkidle", timeout=12000)
            except PlaywrightTimeoutError:
                pass

            expand_linkedin_visible_sections(page)
            scroll_linkedin_page(page, steps=7)
            main_text = extract_playwright_page_text(page)

            if looks_like_candidate_authwall(main_text):
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "LinkedIn masque le profil candidat pour le profil Playwright. "
                        "Ouvrez ce profil navigateur, connectez-vous a LinkedIn, puis retestez."
                    )
                )

            detail_text = collect_linkedin_candidate_detail_pages(
                page,
                url,
                PlaywrightTimeoutError,
            )
            raw_text = (
                f"=== PROFILE_MAIN ===\n{main_text}\n"
                f"{detail_text}"
            )
            return clean_linkedin_text(raw_text, url, "candidate")
    except HTTPException:
        raise
    except Exception as e:
        error_type = type(e).__name__
        message = str(e) or repr(e) or "Erreur inconnue"
        print(f"[Upload] Playwright error type : {error_type}")
        print(f"[Upload] Playwright error detail : {message[:1200]}")
        if "Executable doesn't exist" in message or "playwright install" in message:
            raise HTTPException(
                status_code=500,
                detail="Chromium Playwright manquant. Executez : python -m playwright install chromium"
            )
        if "Target page, context or browser has been closed" in message:
            raise HTTPException(
                status_code=500,
                detail=(
                    "Playwright a ferme le navigateur pendant l'extraction. "
                    "Verifiez que Chromium Playwright est installe et que le profil n'est pas bloque."
                )
            )
        if "user data directory is already in use" in message.lower():
            raise HTTPException(
                status_code=422,
                detail="Le profil Playwright LinkedIn est deja ouvert. Fermez cette fenetre navigateur puis retestez."
            )
        raise HTTPException(
            status_code=500,
            detail=f"Scraping LinkedIn Playwright echoue ({error_type}) : {message[:500]}"
        )
    finally:
        if browser_context is not None:
            try:
                browser_context.close()
            except Exception:
                pass


async def scrape_linkedin_candidate_playwright_text(url: str) -> str:
    """
    Execute Playwright dans un thread pour eviter le probleme subprocess asyncio
    de Uvicorn sous Windows.
    """
    return await asyncio.to_thread(scrape_linkedin_candidate_playwright_text_sync, url)


def scrape_linkedin_text_v2(url: str, page_type: str = "candidate", authenticated: bool = False) -> str:
    """Scrape une page LinkedIn avec scroll, attente explicite et nettoyage anti-bruit."""
    driver = None
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support.ui import WebDriverWait
        from selenium.webdriver.support import expected_conditions as EC
        import time

        opts = Options()
        headless_default = not authenticated
        if env_flag("LINKEDIN_HEADLESS", headless_default):
            opts.add_argument("--headless=new")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1440,2200")
        opts.add_argument("--lang=fr-FR")
        opts.add_argument("--disable-blink-features=AutomationControlled")
        opts.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"
        )

        if authenticated:
            user_data_dir = get_linkedin_chrome_user_data_dir()
            profile_dir = get_linkedin_chrome_profile()
            if not user_data_dir:
                raise HTTPException(
                    status_code=422,
                    detail=(
                        "Configuration LinkedIn candidat manquante. "
                        "Ajoutez LINKEDIN_CHROME_USER_DATA_DIR dans .env avec un profil Chrome connecte a LinkedIn."
                    )
                )
            opts.add_argument(f"--user-data-dir={user_data_dir}")
            if profile_dir:
                opts.add_argument(f"--profile-directory={profile_dir}")

        driver = webdriver.Chrome(options=opts)
        driver.set_page_load_timeout(45)
        driver.get(url)
        WebDriverWait(driver, 18).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )

        for _ in range(4):
            buttons = driver.find_elements(
                By.XPATH,
                "//button[contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'voir plus') "
                "or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'show more') "
                "or contains(translate(., 'ABCDEFGHIJKLMNOPQRSTUVWXYZ', 'abcdefghijklmnopqrstuvwxyz'), 'see more')]"
            )
            clicked = False
            for button in buttons[:10]:
                try:
                    driver.execute_script("arguments[0].click();", button)
                    clicked = True
                    time.sleep(0.4)
                except Exception:
                    continue
            if not clicked:
                break

        last_height = 0
        for _ in range(6):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(1.2)
            height = driver.execute_script("return document.body.scrollHeight")
            if height == last_height:
                break
            last_height = height

        raw_text = driver.execute_script(
            """
            const meta = Array.from(document.querySelectorAll('meta[name], meta[property]'))
              .map(m => `${m.getAttribute('name') || m.getAttribute('property')}: ${m.getAttribute('content') || ''}`)
              .filter(Boolean)
              .join('\\n');
            const jsonLd = Array.from(document.querySelectorAll('script[type="application/ld+json"]'))
              .map(s => s.innerText || s.textContent || '')
              .join('\\n');
            const body = document.body.innerText || document.body.textContent || '';
            return `PAGE_TITLE: ${document.title}\\n${meta}\\n${jsonLd}\\n${body}`;
            """
        )
        if page_type == "candidate" and looks_like_candidate_authwall(raw_text):
            raise HTTPException(
                status_code=422,
                detail=(
                    "LinkedIn masque le profil candidat pour ce navigateur. "
                    "Connectez le profil Chrome configure dans LINKEDIN_CHROME_USER_DATA_DIR puis relancez l'upload."
                )
            )
        return clean_linkedin_text(raw_text, url, page_type)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Scraping LinkedIn echoue : {e}")
    finally:
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass


MISSING_VALUES = {"", "not found", "not specified", "none", "null", "n/a", "na"}
GENERIC_NAME_LINES = {
    "cv", "curriculum vitae", "resume", "profil", "profile", "contact",
    "candidat", "candidate", "competences", "skills", "experience",
    "accueil", "mon reseau", "mon réseau", "emplois", "notifications",
    "vous", "coordonnees", "coordonnées", "pour les entreprises",
    "essayer premium", "se connecter",
}
GENERIC_NAME_LINES.update({
    "passer au contenu principal",
    "skip to main content",
})
GENERIC_NAME_TOKENS = (
    "contenu principal",
    "s'identifier",
    "identifier",
    "mot de passe",
    "linkedin",
    "postuler",
    "e-mail",
    "email",
)
NAME_PATTERN = re.compile(r"^[A-Za-zÀ-ÿ' -]{3,60}$")


def is_missing(value) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        return value.strip().lower() in MISSING_VALUES
    return False


def as_list(value) -> list:
    if is_missing(value):
        return []
    if isinstance(value, list):
        return value
    return [value]


def as_dict(value) -> dict:
    if isinstance(value, dict):
        return value
    return {}


def as_int(value, default: int = 0) -> int:
    if is_missing(value):
        return default
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return default


def as_float(value, default: float = 0.0) -> float:
    if is_missing(value):
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def is_plausible_person_name(value: str) -> bool:
    line = (value or "").strip().strip(":-|â€¢*")
    compact = line.lower()
    if not line or compact in GENERIC_NAME_LINES:
        return False
    if any(token in compact for token in GENERIC_NAME_TOKENS):
        return False
    if any(token in compact for token in ("@", "http", "www.", "linkedin", "github")):
        return False
    if any(ch.isdigit() for ch in line):
        return False
    if not NAME_PATTERN.fullmatch(line):
        return False
    parts = [p for p in line.replace("-", " ").replace("'", " ").split() if p]
    return 2 <= len(parts) <= 4


def extract_full_name_fallback(text: str) -> str:
    """Extrait un nom plausible depuis l'en-tete du CV sans inventer."""
    for raw_line in text.splitlines()[:80]:
        line = raw_line.strip().strip(":-|•*")
        page_title = re.match(r"^PAGE_TITLE:\s*(.+?)(?:\s*\|\s*LinkedIn.*)?$", line, flags=re.IGNORECASE)
        if page_title:
            candidate = page_title.group(1).strip()
            if is_plausible_person_name(candidate):
                return candidate

        if is_plausible_person_name(line):
            return line

        compact = line.lower()
        if not line or compact in GENERIC_NAME_LINES:
            continue
        if any(token in compact for token in GENERIC_NAME_TOKENS):
            continue
        if any(token in compact for token in ("@", "http", "www.", "linkedin", "github")):
            continue
        if any(ch.isdigit() for ch in line):
            continue
        if not NAME_PATTERN.fullmatch(line):
            continue
        parts = [p for p in line.replace("-", " ").replace("'", " ").split() if p]
        if len(parts) >= 2:
            return line
    return ""


def merge_extraction_data(primary: dict, fallback: dict) -> dict:
    """Conserve les champs utiles du JSON precedent quand le retry les perd."""
    if not isinstance(primary, dict):
        primary = {}
    if not isinstance(fallback, dict):
        fallback = {}

    merged = dict(primary)
    for key, value in fallback.items():
        current = merged.get(key)
        if is_missing(current):
            merged[key] = value
        elif isinstance(current, list) and not current and isinstance(value, list) and value:
            merged[key] = value
        elif isinstance(current, dict) and not current and isinstance(value, dict) and value:
            merged[key] = value
    return merged


def extract_company_source_fallback(data: dict) -> str:
    """Utilise la premiere entreprise professionnelle extraite dans l'experience."""
    for item in as_list(data.get("experience_timeline")):
        if not isinstance(item, dict):
            continue
        company = item.get("company")
        if is_missing(company):
            continue
        company_text = str(company).strip()
        lowered = company_text.lower()
        if lowered in {"stage", "temps plein", "freelance", "not found", "not specified"}:
            continue
        return company_text
    return ""


def clean_company_line(line: str) -> str:
    company = re.split(r"\s+[·•]\s+|\s+-\s+", line, maxsplit=1)[0].strip()
    return company.strip(":-|â€¢* ")


def is_company_candidate(line: str) -> bool:
    if is_missing(line):
        return False
    lowered = line.strip().lower()
    rejected_exact = {
        "experience", "expérience", "formation", "bénévolat", "benevolat",
        "compétences", "competences", "infos", "activité", "activite",
        "temps plein", "stage", "sur site", "hybride", "remote",
    }
    if lowered in rejected_exact:
        return False
    if any(token in lowered for token in ("@", "http", "voir le profil", "j'aime", "commenter", "reposter")):
        return False
    if len(line.strip()) < 2 or len(line.strip()) > 80:
        return False
    return True


def extract_company_source_from_text(text: str) -> str:
    """
    Lit la section Experience du texte brut LinkedIn/CV.
    Exemple attendu:
    Experience
    Developpeur
    Operation media sarl · Temps plein
    """
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    start = None
    for i, line in enumerate(lines):
        if line.lower() in {"experience", "expérience"}:
            start = i + 1
            break
    if start is None:
        for i, line in enumerate(lines):
            if line.lower() == "section: experience":
                start = i + 1
                break
    if start is None:
        return ""

    stop_headers = {
        "formation", "bénévolat", "benevolat", "compétences", "competences",
        "licences et certifications", "certifications", "recommandations",
    }
    role_seen = False
    for line in lines[start:start + 40]:
        lowered = line.lower()
        if lowered.startswith("section:") and lowered != "section: experience":
            break
        if lowered in stop_headers:
            break
        if not is_company_candidate(line):
            continue
        if "·" in line or "•" in line:
            company = clean_company_line(line)
            if is_company_candidate(company):
                return company
        if not role_seen:
            role_seen = True
            continue
        company = clean_company_line(line)
        if is_company_candidate(company):
            return company
    return ""


def validate_extracted_cv(data: dict, text: str) -> dict:
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="Extraction CV invalide : JSON objet attendu")

    print(f"[Upload] full_name Mistral : {data.get('full_name')}")
    if is_missing(data.get("full_name")):
        fallback_name = extract_full_name_fallback(text)
        if fallback_name:
            data["full_name"] = fallback_name
            print(f"[Upload] full_name fallback : {fallback_name}")

    if is_missing(data.get("full_name")):
        raise HTTPException(
            status_code=422,
            detail="Nom du candidat introuvable apres extraction. Aucun candidat n'a ete insere."
        )

    print(f"[Upload] company_source Mistral : {data.get('company_source')}")
    text_company = extract_company_source_from_text(text)
    if text_company:
        if data.get("company_source") != text_company:
            data["company_source"] = text_company
            print(f"[Upload] company_source section Experience : {text_company}")
    elif is_missing(data.get("company_source")):
        fallback_company = extract_company_source_fallback(data)
        if fallback_company:
            data["company_source"] = fallback_company
            print(f"[Upload] company_source fallback : {fallback_company}")

    return data


def validate_extracted_job(data: dict) -> dict:
    if not isinstance(data, dict):
        raise HTTPException(status_code=422, detail="Extraction offre invalide : JSON objet attendu")
    print(f"[Upload] job title Mistral : {data.get('title')}")
    if is_missing(data.get("title")):
        raise HTTPException(
            status_code=422,
            detail="Titre de l'offre introuvable apres extraction. Aucune offre n'a ete inseree."
        )
    return data


JOB_LEVEL_ALIASES = {
    "junior": "Junior",
    "debutant": "Junior",
    "débutant": "Junior",
    "medior": "Medior",
    "intermediaire": "Medior",
    "intermédiaire": "Medior",
    "confirme": "Confirme",
    "confirmé": "Confirme",
    "senior": "Senior",
    "expert": "Expert",
}

JOB_LEVEL_MIN_YEARS = {
    "Junior": 0,
    "Medior": 2,
    "Confirme": 4,
    "Senior": 6,
    "Expert": 10,
}


def normalize_job_level(value) -> str:
    if is_missing(value):
        return "not specified"
    normalized = str(value).strip()
    key = normalized.lower()
    key = (
        key.replace("Ã©", "e")
           .replace("é", "e")
           .replace("è", "e")
           .replace("ê", "e")
           .replace("à", "a")
    )
    return JOB_LEVEL_ALIASES.get(key, normalized)


def infer_years_from_text(text: str) -> int:
    if is_missing(text):
        return 0
    patterns = [
        r"(\d{1,2})\s*\+?\s*(?:ans|années|annees|years)\s+(?:d['’ ]?)?(?:expérience|experience)",
        r"(?:minimum|min\.?|au moins)\s+(\d{1,2})\s*(?:ans|années|annees|years)",
        r"(\d{1,2})\s*(?:ans|années|annees|years)\s+(?:minimum|min\.?|requis|required)",
    ]
    for pattern in patterns:
        match = re.search(pattern, str(text), flags=re.IGNORECASE)
        if match:
            return as_int(match.group(1))
    return 0


def normalize_job_seniority_lists(data: dict) -> None:
    for field in (
        "seniority_requirements_technologies",
        "seniority_requirements_programming_languages",
    ):
        normalized_items = []
        for item in as_list(data.get(field)):
            if not isinstance(item, dict):
                continue
            item = dict(item)
            if "level" in item:
                item["level"] = normalize_job_level(item.get("level"))
            normalized_items.append(item)
        data[field] = normalized_items


def _lookup_text(value: str) -> str:
    text = str(value or "").lower()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(ch for ch in text if not unicodedata.combining(ch))
    replacements = {
        "à": "a", "â": "a",
        "é": "e", "è": "e", "ê": "e", "ë": "e",
        "î": "i", "ï": "i",
        "ô": "o",
        "ù": "u", "û": "u",
        "ç": "c",
        "’": "'", " ": " ",
    }
    for src, dst in replacements.items():
        text = text.replace(src, dst)
    return re.sub(r"\s+", " ", text).strip()


def _contains_any(text: str, variants: list[str]) -> bool:
    normalized_text = _lookup_text(text)
    return any(_lookup_text(variant) in normalized_text for variant in variants)


def _append_unique(values: list, value: str) -> None:
    if not value:
        return
    existing = {_lookup_text(v) for v in values if isinstance(v, str)}
    if _lookup_text(value) not in existing:
        values.append(value)


def _dedupe_labels(values: list) -> list:
    normalized_to_label = {}
    for value in as_list(values):
        if not isinstance(value, str) or is_missing(value):
            continue
        key = _lookup_text(value)
        if key not in normalized_to_label:
            normalized_to_label[key] = value.strip()
    return list(normalized_to_label.values())


def _dedupe_seniority_items(values: list, key_name: str) -> list:
    normalized_to_item = {}
    for value in as_list(values):
        if not isinstance(value, dict):
            continue
        label = value.get(key_name)
        if is_missing(label):
            continue
        item = dict(value)
        if not is_missing(item.get("level")):
            item["level"] = normalize_job_level(item.get("level"))
        key = _lookup_text(label)
        if key not in normalized_to_item:
            normalized_to_item[key] = item
    return list(normalized_to_item.values())


def finalize_job_extraction(data: dict, source_text: str = "") -> dict:
    data["programming_languages"] = _dedupe_labels(data.get("programming_languages"))
    data["technical_skills"] = _dedupe_labels(data.get("technical_skills"))
    data["spoken_languages"] = _dedupe_labels(data.get("spoken_languages"))
    data["certifications"] = _dedupe_labels(data.get("certifications"))
    data["seniority_requirements_technologies"] = _dedupe_seniority_items(
        data.get("seniority_requirements_technologies"),
        "technology",
    )
    data["seniority_requirements_programming_languages"] = _dedupe_seniority_items(
        data.get("seniority_requirements_programming_languages"),
        "language",
    )

    if _contains_any(source_text, ["type de contrat : cdi", "contrat : cdi", " cdi "]):
        data["employment_type"] = "Full-time"
    if _lookup_text(data.get("posted")) in {"asap", "des que possible", "des que possible"}:
        data["posted"] = "not specified"

    return data


def normalize_extracted_job(data: dict, source_text: str = "") -> dict:
    if not isinstance(data, dict):
        return data

    data["experience_level"] = normalize_job_level(data.get("experience_level"))
    normalize_job_seniority_lists(data)
    data = finalize_job_extraction(data, source_text)

    years = as_int(data.get("years_of_experience_required"))
    text_years = infer_years_from_text(source_text)
    level_years = JOB_LEVEL_MIN_YEARS.get(data.get("experience_level"), 0)

    if text_years > 0 and (years == 0 or years != text_years):
        data["years_of_experience_required"] = text_years
        print(f"[Upload] job years fallback texte : {text_years}")
    elif years == 0 and level_years > 0:
        data["years_of_experience_required"] = level_years
        print(
            "[Upload] job years fallback niveau : "
            f"{data.get('experience_level')} -> {level_years}"
        )

    if is_missing(data.get("employment_type")):
        data["employment_type"] = "not specified"
    if is_missing(data.get("salary_range")):
        data["salary_range"] = "not specified"
    if is_missing(data.get("education_requirements")):
        data["education_requirements"] = "not specified"

    return data


def has_useful_linkedin_cv_data(data: dict) -> bool:
    return bool(
        as_list(data.get("roles_held")) or
        as_list(data.get("programming_languages")) or
        as_list(data.get("technical_skills")) or
        as_list(data.get("experience_timeline")) or
        not is_missing(data.get("summary"))
    )


def has_useful_linkedin_job_data(data: dict) -> bool:
    return bool(
        as_list(data.get("programming_languages")) or
        as_list(data.get("technical_skills")) or
        not is_missing(data.get("job_description")) or
        not is_missing(data.get("summary"))
    )


def retry_linkedin_candidate_extraction(data: dict, text: str) -> dict:
    if has_useful_linkedin_cv_data(data):
        return data

    print("[Upload] LinkedIn CV extraction pauvre : deuxieme passage Mistral")
    retry_prompt = f"""Tu dois corriger une extraction LinkedIn candidat trop pauvre.
Le JSON precedent ne contient pas assez d'informations utiles.

Regles :
- Utilise uniquement le texte LinkedIn nettoye fourni.
- Extrais toutes les competences, langages, experiences et le resume visibles.
- N'invente rien.
- Retourne UNIQUEMENT un JSON valide conforme au schema.
- Si une information est absente, mets "not found" ou [].

Schema attendu : {json.dumps(CV_JSON_SCHEMA, ensure_ascii=False)}

Texte LinkedIn nettoye :
"""
    retry_data = extract_with_mistral_small(text, retry_prompt)
    if not is_missing(data.get("linkedin")) and is_missing(retry_data.get("linkedin")):
        retry_data["linkedin"] = data.get("linkedin")
    return merge_extraction_data(retry_data, data)


def retry_linkedin_job_extraction(data: dict, text: str) -> dict:
    if has_useful_linkedin_job_data(data):
        return data

    print("[Upload] LinkedIn job extraction pauvre : deuxieme passage Mistral")
    retry_prompt = f"""Tu dois corriger une extraction LinkedIn offre trop pauvre.
Le JSON precedent ne contient pas assez d'informations utiles.

Regles :
- Utilise uniquement le texte LinkedIn nettoye fourni.
- Extrais le titre, l'entreprise, la description, les competences, langages, localisation et niveau visibles.
- N'invente rien.
- Retourne UNIQUEMENT un JSON valide conforme au schema.
- Si une information est absente, mets "not specified" ou [].

Schema attendu : {json.dumps(JOB_JSON_SCHEMA, ensure_ascii=False)}

Texte LinkedIn nettoye :
"""
    return extract_with_mistral_small(text, retry_prompt)


def audit_job_extraction_with_mistral(data: dict, text: str) -> dict:
    """
    Second passage LLM generaliste : le modele compare le JSON extrait avec le
    texte source et corrige les oublis sans regles metier specifiques au domaine.
    """
    if not isinstance(data, dict) or is_missing(text):
        return data

    audit_prompt = f"""Tu es un auditeur d'extraction d'offres d'emploi IT.
Compare le JSON extrait avec le texte source complet.

Objectif :
- corriger les omissions ;
- corriger les mauvaises classifications ;
- supprimer les informations inventees ;
- retourner un JSON complet et fiable conforme au schema.

Regles generales :
- Utilise uniquement les informations explicitement presentes dans le texte source.
- N'ajoute aucune information absente du texte.
- Ne cree aucune regle specifique a un domaine, une entreprise ou une offre.
- Ajoute toutes les competences, technologies, outils, frameworks, methodes, responsabilites techniques et prerequis cites dans l'offre.
- programming_languages contient uniquement les langages de programmation, scripting ou requete.
- technical_skills contient les technologies, outils, frameworks, plateformes, methodes et competences operationnelles demandees.
- Les listes seniority_requirements_programming_languages et seniority_requirements_technologies doivent contenir les elements explicitement requis avec un niveau coherent.
- Respecte les nuances du texte : une bonne connaissance ne doit pas devenir une maitrise.
- CDI doit etre traduit par employment_type = "Full-time".
- ASAP ne doit jamais etre considere comme une date de publication.
- Evite les doublons semantiques.

Schema attendu : {json.dumps(JOB_JSON_SCHEMA, ensure_ascii=False)}

JSON extrait a auditer :
{json.dumps(data, ensure_ascii=False)}

Texte source :
"""
    try:
        audited_data = extract_with_mistral_small(text, audit_prompt)
        if isinstance(audited_data, dict) and not is_missing(audited_data.get("title")):
            return audited_data
    except Exception as exc:
        print(f"[Upload] audit job Mistral ignore : {exc}")
    return data


def validate_linkedin_cv_quality(data: dict) -> dict:
    useful_content = has_useful_linkedin_cv_data(data)
    if not useful_content:
        print(f"[Upload] LinkedIn CV JSON insuffisant : {json.dumps(data, ensure_ascii=False)[:800]}")
        raise HTTPException(
            status_code=422,
            detail="Extraction LinkedIn insuffisante : aucune experience, competence ou synthese exploitable. Aucun candidat n'a ete insere."
        )
    return data


def validate_linkedin_job_quality(data: dict) -> dict:
    useful_content = has_useful_linkedin_job_data(data)
    if not useful_content:
        print(f"[Upload] LinkedIn job JSON insuffisant : {json.dumps(data, ensure_ascii=False)[:800]}")
        raise HTTPException(
            status_code=422,
            detail="Extraction LinkedIn insuffisante : offre trop incomplete. Aucune offre n'a ete inseree."
        )
    return data


def extract_with_mistral_small(text: str, prompt: str, model: str = "mistral-small-latest") -> dict:
    """
    Appelle Mistral Small avec JSON mode pour respecter le schema exact.
    Mistral Small est cohérent avec le reste du projet (LLM explain/gap).
    """
    mistral_key = os.getenv("MISTRAL_API_KEY", "")
    if not mistral_key:
        raise HTTPException(status_code=500, detail="MISTRAL_API_KEY manquante")
    try:
        resp = req_lib.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={
                "Content-Type":  "application/json",
                "Authorization": f"Bearer {mistral_key}"
            },
            json={
                "model":    model,
                "messages": [{"role": "user", "content": prompt + text[:18000]}],
                "max_tokens":   3500,
                "temperature":  0.1,
                "response_format": {"type": "json_object"}   # JSON mode Mistral
            },
            timeout=60
        )
        resp.raise_for_status()
        raw = resp.json()["choices"][0]["message"]["content"].strip()
        raw = raw.replace("```json", "").replace("```", "").strip()
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise HTTPException(status_code=422, detail=f"JSON invalide retourné par Mistral : {e}")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erreur Mistral : {e}")


# ─── MAPPING JSON → PYDANTIC ─────────────────────────────────────────

def map_cv_to_payload(data: dict) -> CandidatePayload:
    """
    Mappe le JSON extrait par Mistral vers CandidatePayload (CRUD_API).
    Respecte les champs de CandidatePayload existants.
    """
    career = as_dict(data.get("career_trajectory"))
    return CandidatePayload(
        full_name              = data.get("full_name", ""),
        email                  = data.get("email", ""),
        phone                  = data.get("phone", ""),
        location               = data.get("location", ""),
        years_of_experience    = as_int(data.get("years_of_experience")),
        linkedin               = data.get("linkedin", ""),
        github                 = data.get("github", ""),
        roles_held             = as_list(data.get("roles_held")),
        programming_languages  = as_list(data.get("programming_languages")),
        technical_skills       = as_list(data.get("technical_skills")),
        spoken_languages       = as_list(data.get("spoken_languages")),
        certifications         = as_list(data.get("certifications")),
        seniority_technologies = as_list(data.get("seniority_technologies")),
        seniority_programming_languages = as_list(data.get("seniority_programming_languages")),
        industry               = as_dict(data.get("industry")),
        summary                = data.get("summary", ""),
        education_level        = data.get("education_level", ""),
        field_of_studies       = data.get("field_of_studies", ""),
        work_experience        = data.get("work_experience", ""),
        projects               = data.get("projects", ""),
        experience_timeline    = as_list(data.get("experience_timeline")),
        career_trajectory      = career,
        parsing_confidence     = as_float(data.get("parsing_confidence")),
        company_source         = data.get("company_source", ""),
    )


def map_job_to_payload(data: dict) -> JobPayload:
    """
    Mappe le JSON extrait par Mistral vers JobPayload (CRUD_API).
    Respecte les champs de JobPayload existants.
    """
    return JobPayload(
        title              = data.get("title", ""),
        company            = data.get("company", ""),
        industry           = data.get("industry", ""),
        location           = data.get("location", ""),
        employment_type    = data.get("employment_type", "not specified"),
        job_description    = data.get("job_description", ""),
        posted             = data.get("posted", ""),
        programming_languages = as_list(data.get("programming_languages")),
        technical_skills      = as_list(data.get("technical_skills")),
        spoken_languages      = as_list(data.get("spoken_languages")),
        certifications        = as_list(data.get("certifications")),
        seniority_requirements_technologies = (
            as_list(data.get("seniority_requirements_technologies"))
        ),
        seniority_requirements_programming_languages = (
            as_list(data.get("seniority_requirements_programming_languages"))
        ),
        experience_level             = data.get("experience_level", "not specified"),
        salary_range                 = data.get("salary_range", "not specified"),
        education_requirements       = data.get("education_requirements", "not specified"),
        years_of_experience_required = as_int(data.get("years_of_experience_required")),
        summary                      = data.get("summary", ""),
    )


# ─── ENDPOINTS UPLOAD ─────────────────────────────────────────────────

def _dup_norm(value) -> str:
    if is_missing(value):
        return ""
    value = str(value).lower()
    value = re.sub(r"[^\w\s]", " ", value, flags=re.UNICODE)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _find_candidate_duplicates(data: dict) -> list:
    linkedin = _dup_norm(data.get("linkedin"))
    email = _dup_norm(data.get("email"))
    full_name = _dup_norm(data.get("full_name"))
    company = _dup_norm(data.get("company_source"))
    location = _dup_norm(data.get("location"))

    if not any([linkedin, email, full_name]):
        return []

    matches = []
    with get_client() as client:
        candidates = get_all_candidates(client, limit=5000)

    for candidate in candidates:
        reasons = []
        if linkedin and linkedin == _dup_norm(candidate.get("linkedin")):
            reasons.append("meme profil LinkedIn")
        if email and email == _dup_norm(candidate.get("email")):
            reasons.append("meme email")
        if full_name and full_name == _dup_norm(candidate.get("full_name")) and company and company == _dup_norm(candidate.get("company_source")):
            reasons.append("meme nom et meme entreprise source")
        elif full_name and full_name == _dup_norm(candidate.get("full_name")) and location and location == _dup_norm(candidate.get("location")):
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


def _find_job_duplicates(data: dict) -> list:
    title = _dup_norm(data.get("title"))
    company = _dup_norm(data.get("company"))
    location = _dup_norm(data.get("location"))

    if not title:
        return []

    matches = []
    with get_client() as client:
        jobs = get_all_jobs(client, limit=5000)

    for job in jobs:
        reasons = []
        if title == _dup_norm(job.get("title")) and company and company == _dup_norm(job.get("company")):
            reasons.append("meme titre et meme entreprise")
        elif title == _dup_norm(job.get("title")) and location and location == _dup_norm(job.get("location")):
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


def _duplicate_upload_response(target_type: str, label: str, duplicates: list) -> dict:
    label_key = "title" if target_type == "job" else "name"
    return {
        "status": "duplicate",
        "inserted": False,
        "target_type": target_type,
        label_key: label,
        "uuid": "",
        "message": "Insertion bloquee : doublon potentiel detecte.",
        "duplicates": duplicates,
    }


@router.post("/upload/candidate/file")
async def upload_candidate_file(
    file: UploadFile = File(...),
    force_insert: bool = Form(False),
):
    """
    Upload un CV (PDF/DOCX/TXT).
    Pipeline : texte → Mistral Small (schema CV exact) → CandidatePayload → add_candidate_endpoint()
    """
    try:
        content = await file.read()
        text    = extract_text_from_bytes(content, file.filename or "")
        data    = extract_with_mistral_small(text, CV_PROMPT)
        data    = validate_extracted_cv(data, text)
        duplicates = _find_candidate_duplicates(data)
        if duplicates and not force_insert:
            return _duplicate_upload_response(
                "candidate",
                data.get("full_name", ""),
                duplicates,
            )
        payload = map_cv_to_payload(data)

        # Appel direct à l'endpoint CRUD existant — zéro duplication
        result  = add_candidate_endpoint(payload)

        name = result.get("name", data.get("full_name", ""))
        return {
            "status":   "success",
            "name":     name,
            "uuid":     result.get("id", ""),
            "message":  f"Candidat ajouté : {name}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/upload/job/file")
async def upload_job_file(
    file: UploadFile = File(...),
    force_insert: bool = Form(False),
):
    """
    Upload une offre (PDF/DOCX/TXT).
    Pipeline : texte → Mistral Small (schema Job exact) → JobPayload → add_job_endpoint()
    """
    try:
        content = await file.read()
        text    = extract_text_from_bytes(content, file.filename or "")
        data    = extract_with_mistral_small(text, JOB_PROMPT)
        data    = audit_job_extraction_with_mistral(data, text)
        data    = validate_extracted_job(data)
        data    = normalize_extracted_job(data, text)
        duplicates = _find_job_duplicates(data)
        if duplicates and not force_insert:
            return _duplicate_upload_response(
                "job",
                data.get("title", ""),
                duplicates,
            )
        payload = map_job_to_payload(data)

        # Appel direct à l'endpoint CRUD existant — zéro duplication
        result  = add_job_endpoint(payload)

        title = result.get("title", data.get("title", ""))
        return {
            "status":  "success",
            "title":   title,
            "uuid":    result.get("id", ""),
            "message": f"Offre ajoutée : {title}"
        }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


class LinkRequest(BaseModel):
    url:  str
    type: str  # "candidate" ou "job"
    force_insert: bool = False

@router.post("/upload/from-url")
async def upload_from_url(req: LinkRequest):
    """
    Upload depuis un lien LinkedIn public.
    Candidat : Playwright persistant → Mistral Small → CRUD endpoint existant.
    Offre    : LinkedIn jobs-guest → Mistral Small → CRUD endpoint existant.
    """
    if req.type not in ("candidate", "job"):
        raise HTTPException(status_code=400, detail="type doit être 'candidate' ou 'job'")

    try:
        if req.type == "candidate":
            text = await scrape_linkedin_candidate_playwright_text(req.url)
            data    = extract_with_mistral_small(text, LINKEDIN_CV_PROMPT)
            if is_missing(data.get("linkedin")):
                data["linkedin"] = req.url
            data    = retry_linkedin_candidate_extraction(data, text)
            if is_missing(data.get("linkedin")):
                data["linkedin"] = req.url
            data    = validate_extracted_cv(data, text)
            data    = validate_linkedin_cv_quality(data)
            duplicates = _find_candidate_duplicates(data)
            if duplicates and not req.force_insert:
                return _duplicate_upload_response(
                    "candidate",
                    data.get("full_name", ""),
                    duplicates,
                )
            payload = map_cv_to_payload(data)
            result  = add_candidate_endpoint(payload)
            name    = result.get("name", data.get("full_name", ""))
            return {
                "status":  "success",
                "name":    name,
                "uuid":    result.get("id", ""),
                "message": f"Candidat ajouté : {name}"
            }
        else:
            text    = scrape_linkedin_job_guest_text(req.url)
            data    = extract_with_mistral_small(text, LINKEDIN_JOB_PROMPT)
            data    = retry_linkedin_job_extraction(data, text)
            data    = audit_job_extraction_with_mistral(data, text)
            data    = validate_extracted_job(data)
            data    = validate_linkedin_job_quality(data)
            data    = normalize_extracted_job(data, text)
            duplicates = _find_job_duplicates(data)
            if duplicates and not req.force_insert:
                return _duplicate_upload_response(
                    "job",
                    data.get("title", ""),
                    duplicates,
                )
            payload = map_job_to_payload(data)
            result  = add_job_endpoint(payload)
            title   = result.get("title", data.get("title", ""))
            return {
                "status":  "success",
                "title":   title,
                "uuid":    result.get("id", ""),
                "message": f"Offre ajoutée : {title}"
            }
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
