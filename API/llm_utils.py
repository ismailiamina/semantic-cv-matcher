"""
API/llm_utils.py
================
Fonctions LLM isolees — sans dependance Streamlit.
"""
import os
import requests
from dotenv import load_dotenv
from pathlib import Path

# Charge .env
_p = Path(__file__).resolve().parent
for _ in range(4):
    if (_p / ".env").exists():
        load_dotenv(dotenv_path=_p / ".env")
        break
    _p = _p.parent


def explain_match_llm(job_props: dict, cand_props: dict) -> str:
    mistral_key = os.getenv("MISTRAL_API_KEY", "")
    if not mistral_key:
        return "MISTRAL_API_KEY manquante"

    company = job_props.get('company', '') or 'l entreprise cliente'


    prompt = f"""Tu es un expert RH specialise dans le placement de consultants IT au Maroc.
Tu analyses des profils pour un recrutement chez : {company}.

OFFRE :
- Poste : {job_props.get('title','')}
- Niveau requis : {job_props.get('experience_level','')} — {job_props.get('years_of_experience_required',0)} ans minimum
- Competences requises : {', '.join(job_props.get('technical_skills',[]) or [])}
- Langages requis : {', '.join(job_props.get('programming_languages',[]) or [])}

CANDIDAT : {cand_props.get('full_name','')}
- Experience : {cand_props.get('years_of_experience',0)} ans
- Roles : {', '.join(cand_props.get('roles_held',[]) or [])}
- Competences : {', '.join(cand_props.get('technical_skills',[]) or [])}
- Langages : {', '.join(cand_props.get('programming_languages',[]) or [])}
- Certifications : {', '.join(cand_props.get('certifications',[]) or []) or 'Aucune'}
- Profil : {(cand_props.get('summary','') or '')[:300]}

Reponds en 3 points courts et factuels. N utilise PAS de titres markdown (###). Utilise uniquement ** pour le gras :
1. Points forts : pourquoi ce candidat correspond
2. Points faibles : ce qui manque ou ne correspond pas
3. Verdict final en une phrase

Sois concis, factuel et professionnel. Maximum 150 mots."""
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {mistral_key}"},
            json={"model": "mistral-small-latest",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 300, "temperature": 0.3},
            timeout=30
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Erreur : {e}"




def analyze_gap_llm(cand_props, job_props):
    mistral_key = os.getenv("MISTRAL_API_KEY", "")
    if not mistral_key:
        return "MISTRAL_API_KEY manquante"
    job_skills   = job_props.get("technical_skills", []) or []
    job_langs    = job_props.get("programming_languages", []) or []
    job_certs    = job_props.get("certifications", []) or []
    cand_skills  = cand_props.get("technical_skills", []) or []
    cand_langs   = cand_props.get("programming_languages", []) or []
    cand_certs   = cand_props.get("certifications", []) or []
    cand_senio   = cand_props.get("seniority_technologies", []) or []
    missing_skills = [s for s in job_skills if s not in cand_skills]
    missing_langs  = [l for l in job_langs  if l not in cand_langs]
    missing_certs  = [c for c in job_certs  if c not in cand_certs]
    years_gap      = max(0, (job_props.get("years_of_experience_required", 0) or 0) - (cand_props.get("years_of_experience", 0) or 0))
    prompt = f"""Tu es un expert en formation et developpement des competences IT.
Analyse le gap entre ce candidat et l offre cible, puis propose un plan de formation personnalise.

CANDIDAT : {cand_props.get('full_name','')}
- Experience actuelle : {cand_props.get('years_of_experience',0)} ans
- Competences actuelles : {', '.join(cand_skills[:15]) if cand_skills else 'Non precise'}
- Langages actuels : {', '.join(cand_langs) if cand_langs else 'Non precise'}
- Certifications : {', '.join(cand_certs[:4]) if cand_certs else 'Aucune'}
- Niveau par techno : {', '.join([f"{s.get('technology','')} ({s.get('level','')})" for s in cand_senio[:6] if isinstance(s, dict)])}

OFFRE CIBLEE : {job_props.get('title','')}
- Niveau requis : {job_props.get('experience_level','')} — {job_props.get('years_of_experience_required',0)} ans minimum
- Competences requises : {', '.join(job_skills) if job_skills else 'Non precise'}
- Langages requis : {', '.join(job_langs) if job_langs else 'Non precise'}
- Certifications souhaitees : {', '.join(job_certs) if job_certs else 'Aucune'}

GAPS DETECTES :
- Competences manquantes : {', '.join(missing_skills) if missing_skills else 'Aucun gap majeur'}
- Langages manquants : {', '.join(missing_langs) if missing_langs else 'Aucun'}
- Certifications manquantes : {', '.join(missing_certs) if missing_certs else 'Aucune'}
- Gap experience : {years_gap} ans

Produis un plan de formation structure. N utilise PAS de titres markdown (###). Utilise uniquement ** pour le gras et des listes numerotees. Pour chaque formation cite le lien URL reel sous forme [Nom du cours](https://url) :
1. GAPS PRIORITAIRES (les 3 plus importants pour ce poste)
2. FORMATIONS RECOMMANDEES (pour chaque gap : plateforme + [nom du cours](url) 
3. CONSEIL FINAL en une phrase

Cite des plateformes reelles (Udemy, Coursera, OpenClassrooms, Pluralsight). Maximum 250 mots."""
    try:
        resp = requests.post(
            "https://api.mistral.ai/v1/chat/completions",
            headers={"Content-Type": "application/json", "Authorization": f"Bearer {mistral_key}"},
            json={"model": "mistral-small-latest",
                  "messages": [{"role": "user", "content": prompt}],
                  "max_tokens": 500, "temperature": 0.3},
            timeout=45
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        return f"Erreur : {e}"