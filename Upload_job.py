import os
import json
import time
import requests
import tempfile
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv

_p = Path(__file__).resolve()
for _ in range(6):
    _p = _p.parent
    if (_p / ".env").exists():
        load_dotenv(dotenv_path=_p / ".env")
        break

JOB_COLLECTION_NAME = "Job"
TENANT_ID_FOR_JOBS  = "job"

JSON_SCHEMA = {
    "title": "JobProfile",
    "type": "object",
    "properties": {
        "title":           {"type": "string"},
        "company":         {"type": "string"},
        "industry":        {"type": "string"},
        "location":        {"type": "string"},
        "employment_type": {
            "type": "string",
            "enum": ["Full-time","Part-time","Fixed-term","Casual","Temporary","Internship","not specified"]
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
                    "level": {"type": "string",
                              "enum": ["Junior","Medior","Confirmé","Senior","Expert"]}
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
                                 "enum": ["Junior","Medior","Confirmé","Senior","Expert"]}
                },
                "required": ["language","level"]
            }
        },
        "experience_level":             {"type": "string",
                                         "enum": ["Junior","Medior","Confirmé","Senior","Expert"]},
        "salary_range":                 {"type": "string"},
        "education_requirements":       {"type": "string"},
        "years_of_experience_required": {"type": "number"},
        "summary":                      {"type": "string"}
    },
    "required": ["title","company","job_description",
                 "programming_languages","technical_skills","certifications","summary"]
}

PROMPT = """Tu reçois le texte d'une offre d'emploi IT. Structure ces informations en JSON.

REGLES ABSOLUES :
1. Utilise UNIQUEMENT les informations présentes dans le texte
2. N'invente RIEN — si absent : "not specified" ou [] ou 0
3. title : titre exact du poste
4. company : nom exact de l'entreprise
5. employment_type : Full-time / Part-time / Fixed-term / Internship / not specified
6. programming_languages : langages de code uniquement (Python, Java, SQL, JavaScript...)
7. technical_skills : outils, frameworks, plateformes
8. experience_level : Junior(0-2ans) / Medior(2-4ans) / Confirmé(4-6ans) / Senior(6-10ans) / Expert(10+ans)
9. seniority_requirements_technologies : top 3 technologies avec niveau attendu
10. seniority_requirements_programming_languages : top 2 langages avec niveau attendu
11. salary_range : si mentionné, sinon "not specified"
12. education_requirements : Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus / not specified
13. years_of_experience_required : nombre entier, 0 si non mentionné
14. summary : 2-3 phrases résumant le poste, l'entreprise et les compétences clés
15. posted : date si mentionnée (YYYY-MM-DD), sinon date du jour
"""



def extract_text_from_docx(file_bytes: bytes) -> str:
    import docx, io
    doc  = docx.Document(io.BytesIO(file_bytes))
    text = "\n".join(p.text for p in doc.paragraphs if p.text.strip())
    return text[:8000]


def extract_text_from_pdf(file_bytes: bytes) -> str:
    try:
        import fitz, io
        doc  = fitz.open(stream=io.BytesIO(file_bytes), filetype="pdf")
        text = "\n".join(page.get_text() for page in doc)
        return text[:8000]
    except ImportError:
        return ""


def extract_text_from_txt(file_bytes: bytes) -> str:
    return file_bytes.decode("utf-8", errors="ignore")[:8000]


def extract_text_from_url(url: str) -> str:
    from bs4 import BeautifulSoup
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/121.0.0.0 Safari/537.36"
        ),
        "Accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection":      "keep-alive",
        "Cache-Control":   "max-age=0",
        "Referer":         "https://www.linkedin.com/",
    }

    job_id = ""
    if "/jobs/view/" in url:
        job_id = url.rstrip("/").split("/jobs/view/")[-1].split("/")[0].split("?")[0]
    if job_id and job_id.isdigit():
        api_url = f"https://www.linkedin.com/jobs-guest/jobs/api/jobPosting/{job_id}"
        try:
            r0 = requests.get(api_url, headers=headers, timeout=15)
            if r0.status_code == 200 and len(r0.text) > 500:
                soup0  = BeautifulSoup(r0.text, "html.parser")
                parts0 = [f"URL : {url}"]
                for script in soup0.find_all("script", {"type": "application/ld+json"}):
                    try:
                        data = json.loads(script.string or "")
                        if data.get("@type") == "JobPosting":
                            if data.get("title"):
                                parts0.append(f"TITRE : {data['title']}")
                            org = data.get("hiringOrganization", {})
                            if org.get("name"):
                                parts0.append(f"ENTREPRISE : {org['name']}")
                            loc = data.get("jobLocation", {})
                            if isinstance(loc, dict):
                                addr = loc.get("address", {})
                                parts0.append(f"LOCALISATION : {addr.get('addressLocality','')}, {addr.get('addressCountry','')}")
                            if data.get("employmentType"):
                                parts0.append(f"TYPE : {data['employmentType']}")
                            if data.get("description"):
                                desc_soup = BeautifulSoup(data["description"], "html.parser")
                                parts0.append("\nDESCRIPTION COMPLETE :")
                                parts0.append(desc_soup.get_text(separator="\n"))
                            if data.get("datePosted"):
                                parts0.append(f"DATE : {data['datePosted']}")
                            result0 = "\n".join(parts0)
                            if len(result0) > 300:
                                return result0[:8000]
                    except Exception:
                        continue
                for tag in soup0(["script","style","nav","header","footer","meta","noscript","svg"]):
                    tag.decompose()
                text0  = "\n".join(parts0) + "\n" + soup0.get_text(separator="\n")
                lines0 = [l.strip() for l in text0.splitlines() if l.strip() and len(l.strip()) > 2]
                result0 = "\n".join(lines0)
                if len(result0) > 300:
                    return result0[:8000]
        except Exception:
            pass

    try:
        resp = requests.get(url, headers=headers, timeout=20)
        if resp.status_code != 200:
            return ""
        html = resp.text
        soup = BeautifulSoup(html, "html.parser")
        parts = [f"URL : {url}"]
        json_ld_scripts = soup.find_all("script", {"type": "application/ld+json"})
        for script in json_ld_scripts:
            try:
                data = json.loads(script.string or "")
                if data.get("@type") == "JobPosting":
                    if data.get("title"):
                        parts.append(f"TITRE : {data['title']}")
                    org = data.get("hiringOrganization", {})
                    if org.get("name"):
                        parts.append(f"ENTREPRISE : {org['name']}")
                    loc = data.get("jobLocation", {})
                    if isinstance(loc, dict):
                        addr = loc.get("address", {})
                        city = addr.get("addressLocality", "")
                        country = addr.get("addressCountry", "")
                        if city or country:
                            parts.append(f"LOCALISATION : {city}, {country}")
                    elif isinstance(loc, list) and loc:
                        addr = loc[0].get("address", {})
                        parts.append(f"LOCALISATION : {addr.get('addressLocality','')}")
                    if data.get("employmentType"):
                        parts.append(f"TYPE : {data['employmentType']}")
                    if data.get("description"):
                        desc_soup = BeautifulSoup(data["description"], "html.parser")
                        desc_text = desc_soup.get_text(separator="\n")
                        desc_lines = [l.strip() for l in desc_text.splitlines() if l.strip()]
                        parts.append("\nDESCRIPTION COMPLETE :")
                        parts.append("\n".join(desc_lines))
                    if data.get("datePosted"):
                        parts.append(f"DATE : {data['datePosted']}")
                    result = "\n".join(parts)
                    if len(result) > 200:
                        return result[:8000]
            except (json.JSONDecodeError, AttributeError):
                continue
        import re
        title_match = re.search(r'"jobTitle"\s*:\s*"([^"]+)"', html)
        if title_match:
            parts.append(f"TITRE : {title_match.group(1)}")
        desc_match = re.search(r'"description"\s*:\s*\{[^}]*"text"\s*:\s*"((?:[^"\\]|\\.)*)"', html)
        if desc_match:
            desc = desc_match.group(1).replace("\\n", "\n").replace("\\/", "/")
            parts.append(f"\nDESCRIPTION :\n{desc[:3000]}")
        company_match = re.search(r'"companyName"\s*:\s*"([^"]+)"', html)
        if company_match:
            parts.append(f"ENTREPRISE : {company_match.group(1)}")
        if len("\n".join(parts)) > 200:
            return "\n".join(parts)[:8000]
        for tag in soup(["script","style","nav","footer","header","meta","noscript","svg","button"]):
            tag.decompose()
        text  = soup.get_text(separator="\n")
        lines = [l.strip() for l in text.splitlines() if l.strip() and len(l.strip()) > 3]
        parts.append("\n".join(lines))
        result = "\n".join(parts)
        return result[:8000] if len(result) > 100 else ""
    except Exception:
        return ""


def structure_with_mistral(text: str, source: str) -> dict:
    from langchain_mistralai import ChatMistralAI
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        return {"error": "MISTRAL_API_KEY manquante"}
    llm = ChatMistralAI(model="mistral-large-latest", api_key=api_key, timeout=90, max_retries=2)
    structured_llm = llm.with_structured_output(schema=JSON_SCHEMA)
    for attempt in range(3):
        try:
            result = structured_llm.invoke(PROMPT + f"\n\nSOURCE : {source}\n\nCONTENU :\n{text}")
            if hasattr(result, "model_dump"): return result.model_dump()
            if hasattr(result, "dict"):       return result.dict()
            return dict(result)
        except Exception as e:
            err = str(e)
            if "429" in err or "rate" in err.lower():
                time.sleep((attempt + 1) * 30)
            elif attempt < 2:
                time.sleep(10)
            else:
                return {"error": err}
    return {"error": "Echec Mistral"}


def normalize_seniority(items: list, key_name: str) -> list:
    if not items: return []
    if isinstance(items[0], dict): return items
    result = []
    for item in items:
        if "(" in item and ")" in item:
            name  = item[:item.rfind("(")].strip()
            level = item[item.rfind("(")+1:item.rfind(")")].strip()
        else:
            name, level = item, "not specified"
        result.append({key_name: name, "level": level})
    return result


def insert_job_weaviate(client, job: dict) -> str:
    job_col    = client.collections.get(JOB_COLLECTION_NAME)
    job_tenant = job_col.with_tenant(TENANT_ID_FOR_JOBS)
    properties = {
        "title":           job.get("title", ""),
        "company":         job.get("company", ""),
        "industry":        job.get("industry", ""),
        "location":        job.get("location", ""),
        "employment_type": job.get("employment_type", ""),
        "job_description": job.get("job_description", ""),
        "posted":          job.get("posted", datetime.now().strftime("%Y-%m-%d")),
        "programming_languages": job.get("programming_languages", []),
        "technical_skills":      job.get("technical_skills", []),
        "spoken_languages":      job.get("spoken_languages", []),
        "certifications":        job.get("certifications", []),
        "seniority_requirements_technologies": normalize_seniority(
            job.get("seniority_requirements_technologies", []), "technology"
        ),
        "seniority_requirements_programming_languages": normalize_seniority(
            job.get("seniority_requirements_programming_languages", []), "language"
        ),
        "experience_level":             job.get("experience_level", ""),
        "salary_range":                 job.get("salary_range", ""),
        "education_requirements":       job.get("education_requirements", ""),
        "years_of_experience_required": int(job.get("years_of_experience_required") or 0),
        "summary":                      job.get("summary", ""),
    }
    uuid = job_tenant.data.insert(properties=properties)
    return str(uuid)



# Palette identique aux classes CSS de app.py
_S = "font-size:12px;font-family:Inter,sans-serif;border-radius:4px;padding:7px 12px;margin:4px 0;"
MSG = {
    "ok":   f"background:#ECFDF5;color:#059669;border:1px solid #A7F3D0;{_S}",
    "err":  f"background:#FEF2F2;color:#DC2626;border:1px solid #FECACA;{_S}",
    "warn": f"background:#FFFBEB;color:#D97706;border:1px solid #FDE68A;{_S}",
    "info": f"background:#F9FAFB;color:#6B7280;border:1px solid #E8EAED;{_S}",
}

def _msg(st, kind, text):
    st.markdown(f'<div style="{MSG[kind]}">{text}</div>', unsafe_allow_html=True)


def render_upload_job_section(client) -> dict:
    import streamlit as st

    st.markdown('<div class="sb-lbl">Ajouter une nouvelle offre</div>', unsafe_allow_html=True)

    input_mode = st.radio(
        "Source de l'offre",
        ["Fichier (.docx / .pdf / .txt)", "URL LinkedIn"],
        horizontal=True,
        key="upload_mode"
    )

    text_extracted = ""
    source_label   = ""

    # ── Mode fichier ──────────────────────────────────────
    if input_mode == "Fichier (.docx / .pdf / .txt)":
        uploaded_file = st.file_uploader(
            "Fichier de l'offre",
            type=["docx", "pdf", "txt"],
            key="job_file_upload"
        )

        if uploaded_file:
            file_bytes   = uploaded_file.read()
            ext          = uploaded_file.name.lower().split(".")[-1]
            source_label = uploaded_file.name

            if ext == "docx":
                text_extracted = extract_text_from_docx(file_bytes)
            elif ext == "pdf":
                text_extracted = extract_text_from_pdf(file_bytes)
            elif ext == "txt":
                text_extracted = extract_text_from_txt(file_bytes)

            if text_extracted:
                _msg(st, "ok", f"Fichier lu — {len(text_extracted)} caractères extraits")
                st.markdown(
    f'<div style="background:#F9FAFB;border:1px solid #E8EAED;border-radius:4px;'
    f'padding:10px 12px;font-size:11px;color:#374151;font-family:JetBrains Mono,monospace;'
    f'white-space:pre-wrap;max-height:120px;overflow-y:auto;">'
    f'{(text_extracted[:500] + "...") if len(text_extracted) > 500 else text_extracted}'
    f'</div>',
    unsafe_allow_html=True
)
            else:
                _msg(st, "err", "Impossible d'extraire le texte du fichier.")

    # ── Mode URL ──────────────────────────────────────────
    else:
        url_input = st.text_input(
            "URL de l'offre LinkedIn",
            placeholder="https://www.linkedin.com/jobs/view/4194104278/",
            key="job_url_input"
        )

        if url_input and st.button("Charger l'offre", key="load_url_btn"):
            if "linkedin.com/jobs/view/" not in url_input:
                _msg(st, "warn", "L'URL doit être une offre LinkedIn (/jobs/view/...)")
            else:
                st.markdown(
    f'<div style="background:#F9FAFB;border:1px solid #E8EAED;border-radius:4px;'
    f'padding:10px 12px;font-size:11px;color:#374151;font-family:JetBrains Mono,monospace;'
    f'white-space:pre-wrap;max-height:120px;overflow-y:auto;">'
    f'{text_extracted[:500] + "..."}'
    f'</div>',
    unsafe_allow_html=True
)
                if text_extracted and len(text_extracted) > 100:
                    st.session_state["job_text_from_url"]   = text_extracted
                    st.session_state["job_source_from_url"] = source_label
                    _msg(st, "ok", f"Offre chargée — {len(text_extracted)} caractères")
                    with st.container(border=True):
                        st.caption("Aperçu du texte extrait")
                        st.text(text_extracted[:500] + "...")
                else:
                    _msg(st, "err", "Impossible de charger cette offre (offre privée ou URL invalide).")

        if not text_extracted and "job_text_from_url" in st.session_state:
            text_extracted = st.session_state.get("job_text_from_url", "")
            source_label   = st.session_state.get("job_source_from_url", "")

    # ── Bouton d'ajout ────────────────────────────────────
    if text_extracted and len(text_extracted) > 50:
        st.markdown("<hr>", unsafe_allow_html=True)

        col1, col2 = st.columns([3, 1])
        with col1:
            _msg(st, "info", "L'offre sera structurée par Mistral Large, vectorisée par Ollama, puis insérée dans Weaviate.")
        with col2:
            add_btn = st.button("Ajouter et rechercher", type="primary", key="add_job_btn")

        if add_btn:
            st.markdown('<div style="font-size:12px;color:#6B7280;font-family:Inter,sans-serif;padding:6px 0;">Mistral structure l\'offre...</div>', unsafe_allow_html=True)
            with st.spinner(""):
                job_data = structure_with_mistral(text_extracted, source_label)

            if "error" in job_data:
                _msg(st, "err", f"Erreur Mistral : {job_data['error']}")
                return None

            _msg(st, "ok", "Offre structurée avec succès")

            with st.container(border=True):
                st.caption("Aperçu de l'offre structurée")
                col_a, col_b = st.columns(2)
                with col_a:
                    st.write(f"**Titre** : {job_data.get('title', '—')}")
                    st.write(f"**Entreprise** : {job_data.get('company', '—')}")
                    st.write(f"**Niveau** : {job_data.get('experience_level', '—')}")
                    st.write(f"**Expérience min** : {job_data.get('years_of_experience_required', 0)} ans")
                with col_b:
                    st.write(f"**Skills** : {', '.join(job_data.get('technical_skills', [])[:5])}")
                    st.write(f"**Langages** : {', '.join(job_data.get('programming_languages', [])[:4]) or '—'}")
                    st.write(f"**Localisation** : {job_data.get('location', '—')}")

            st.markdown('<div style="font-size:12px;color:#6B7280;font-family:Inter,sans-serif;padding:6px 0;">Vectorisation et insertion dans Weaviate...</div>', unsafe_allow_html=True)
            with st.spinner(""):
                try:
                    job_data["file_path"]         = source_label
                    job_data["extraction_method"] = "upload_streamlit + mistral-large"
                    job_data["extracted_at"]       = datetime.now().isoformat()

                    uuid = insert_job_weaviate(client, job_data)
                    job_data["uuid"] = uuid

                    _msg(st, "ok", "Offre insérée dans Weaviate")

                    for key in ["job_text_from_url", "job_source_from_url"]:
                        if key in st.session_state:
                            del st.session_state[key]

                    return job_data

                except Exception as e:
                    _msg(st, "err", f"Erreur insertion Weaviate : {str(e)[:120]}")
                    return None

    return None