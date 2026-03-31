"""
Pipeline d'extraction de CVs PDF
Etape 1 : PyMuPDF (fitz)   -> extrait le texte brut du PDF
Etape 2 : Mistral Large     -> structure le texte en JSON enrichi
"""
from langchain_mistralai import ChatMistralAI
import fitz
import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

JSON_SCHEMA = {
    "title": "CandidateProfile",
    "description": "Extracted profile information from a CV",
    "type": "object",
    "properties": {
        "full_name": {"type": "string"},
        "email": {"type": "string"},
        "phone": {"type": "string"},
        "location": {"type": "string"},
        "years_of_experience": {"type": "number"},
        "linkedin": {"type": "string"},
        "github": {"type": "string"},
        "roles_held": {"type": "array", "items": {"type": "string"}},
        "programming_languages": {"type": "array", "items": {"type": "string"}},
        "technical_skills": {"type": "array", "items": {"type": "string"}},
        "spoken_languages": {"type": "array", "items": {"type": "string"}},
        "certifications": {"type": "array", "items": {"type": "string"}},
        "seniority_technologies": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "technology": {"type": "string"},
                    "level": {"type": "string", "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
                },
                "required": ["technology", "level"]
            }
        },
        "seniority_programming_languages": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "language": {"type": "string"},
                    "level": {"type": "string", "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
                },
                "required": ["language", "level"]
            }
        },
        "industry": {
            "type": "object",
            "properties": {
                "primary_industries": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["primary_industries"]
        },
        "summary": {"type": "string"},
        "education_level": {
            "type": "string",
            "enum": ["Bac + 2", "Bac + 3", "Bac + 4", "Bac + 5", "Bac + 6 et plus"]
        },
        "field_of_studies": {"type": "string"},
        "work_experience": {"type": "string"},
        "projects": {"type": "string"},
        "file_path": {"type": "string"},
        "parsing_confidence": {"type": "number"},
        "experience_timeline": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "year_start": {"type": "number"},
                    "year_end": {"type": "number"},
                    "company": {"type": "string"},
                    "role": {"type": "string"},
                    "skills_acquired": {"type": "array", "items": {"type": "string"}},
                    "languages_used": {"type": "array", "items": {"type": "string"}},
                    "description": {"type": "string"}
                },
                "required": ["year_start", "year_end", "company", "role"]
            }
        },
        "career_trajectory": {
            "type": "object",
            "properties": {
                "direction": {"type": "string"},
                "progression_speed": {"type": "string", "enum": ["Rapide", "Normale", "Lente"]},
                "predicted_profile": {"type": "string"},
                "skills_in_progress": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["direction", "progression_speed", "predicted_profile"]
        }
    },
    "required": [
        "full_name", "email", "roles_held",
        "programming_languages", "technical_skills",
        "industry", "summary",
        "experience_timeline", "career_trajectory"
    ]
}

PROMPT = """Extract information from this CV text and return ONLY a JSON object ALL IN ENGLISH.

STANDARD FIELDS:
- full_name: found at the top of the CV, no CV without a name
- email: if missing use "not found"
- phone: if missing use "not found"
- location: city or region, if missing use "not found"
- years_of_experience: calculate from earliest work experience year to now, if not found use 0
- linkedin: profile URL, if missing use "not found"
- github: profile URL, if missing use "not found"
- roles_held: maximum 3 representative normalized roles, no duplicates
- technical_skills: tools, frameworks, domain expertise — NOT programming languages
- programming_languages: coding languages only (Python, Java, SQL, etc.)
- spoken_languages: all spoken languages mentioned
- certifications: only recognized IT certifications (AWS, GCP, Azure, Cisco, Scrum, Oracle, IBM, CompTIA). If none use []
- seniority_technologies: top 3 technologies with level (0-2=Junior, 2-4=Medior, 4-6=Confirmé, 6-10=Senior, 10+=Expert)
- seniority_programming_languages: top 2 languages with level
- industry.primary_industries: sectors deduced ONLY from project descriptions, not company names
- summary: 3-line professional summary focused on skills and experience
- education_level: choose exactly from Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus
- field_of_studies: main academic field
- work_experience: all experiences merged into one coherent paragraph
- projects: all projects described with objectives and technologies used
- parsing_confidence: your confidence score between 0.0 and 1.0

ADDITIONAL FIELDS - TIMELINE:
experience_timeline: Extract EACH work experience as a SEPARATE entry in CHRONOLOGICAL order (oldest first).
For each entry:
  - year_start: start year as integer
  - year_end: end year as integer (use 2025 if still ongoing)
  - company: exact company name
  - role: exact job title
  - skills_acquired: tools and technologies used in this specific position
  - languages_used: programming languages used in this specific position
  - description: 1-2 sentences on responsibilities and achievements

ADDITIONAL FIELDS - CAREER TRAJECTORY:
career_trajectory: Analyze the chronological progression of skills and roles.
  - direction: one sentence describing the career evolution detected
  - progression_speed: Rapide / Normale / Lente based on skill acquisition speed
  - predicted_profile: what this person will become in 12-18 months if current trend continues
  - skills_in_progress: skills currently being acquired based on recent trajectory

Try to extract all information even if the CV is not perfectly structured.
"""

def get_llm():
    return ChatMistralAI(
        model="mistral-large-latest",
        api_key=os.getenv("MISTRAL_API_KEY"),
        timeout=90,
        max_retries=3
    )


def structure_with_llm(text: str, max_retries: int = 3) -> dict:
    """
    Structure le texte avec Mistral Large
    Retry automatique avec backoff en cas d erreur
    """
    llm = get_llm()
    structured_llm = llm.with_structured_output(schema=JSON_SCHEMA)

    for attempt in range(max_retries):
        try:
            result = structured_llm.invoke(PROMPT + "\n\nCV TEXT:\n" + text)
            return result
        except Exception as e:
            error_msg = str(e)
            if "429" in error_msg or "rate" in error_msg.lower():
                wait = (attempt + 1) * 30
                print(f"    Rate limit - attente {wait}s... (tentative {attempt+1}/{max_retries})")
                time.sleep(wait)
            elif "timeout" in error_msg.lower():
                wait = 15
                print(f"    Timeout - attente {wait}s... (tentative {attempt+1}/{max_retries})")
                time.sleep(wait)
            else:
                print(f"    Erreur LLM : {error_msg[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    raise

    raise Exception("Echec apres tous les essais")



def extract_text_fitz(pdf_path: str) -> str:
    text = ""
    try:
        doc = fitz.open(pdf_path)
        print(f"  Pages : {len(doc)}")
        for page_num in range(len(doc)):
            text += doc[page_num].get_text()
        doc.close()
        print(f"  Texte extrait : {len(text)} caracteres")
    except Exception as e:
        print(f"  Erreur fitz : {e}")
        text = "NO TEXT EXTRACTED"
    return text


def get_seniority(years) -> str:
    years = int(years or 0)
    if years <= 2:    return "Junior"
    elif years <= 4:  return "Medior"
    elif years <= 6:  return "Confirme"
    elif years <= 10: return "Senior"
    else:             return "Expert"


def result_to_dict(result) -> dict:
    if isinstance(result, dict):
        return result
    if hasattr(result, 'model_dump'):
        return result.model_dump()
    if hasattr(result, 'dict'):
        return result.dict()
    return dict(result)


def display_result(result: dict):
    if not result or "error" in result:
        print(f"  ERREUR : {result}")
        return

    print("\n  Resultat :")
    print(f"    Nom          : {result.get('full_name', 'N/A')}")
    print(f"    Email        : {result.get('email', 'N/A')}")
    print(f"    Ville        : {result.get('location', 'N/A')}")
    years = result.get('years_of_experience', 0) or 0
    print(f"    Experience   : {years} ans ({get_seniority(years)})")
    print(f"    Formation    : {result.get('education_level', 'N/A')} - {result.get('field_of_studies', 'N/A')}")
    print(f"    Roles        : {', '.join(result.get('roles_held', []))}")
    print(f"    Langages     : {', '.join(result.get('programming_languages', []))}")
    skills = result.get('technical_skills', [])
    print(f"    Skills       : {', '.join(skills[:5])}{'...' if len(skills) > 5 else ''}")

    timeline = result.get("experience_timeline", [])
    if timeline:
        print(f"\n    Timeline ({len(timeline)} poste(s)) :")
        for exp in timeline:
            print(f"      {exp.get('year_start')}-{exp.get('year_end')} | {exp.get('company', 'N/A')} | {exp.get('role', 'N/A')}")

    traj = result.get("career_trajectory", {})
    if traj:
        print(f"\n    Trajectoire :")
        print(f"      Direction  : {traj.get('direction', 'N/A')}")
        print(f"      Prediction : {traj.get('predicted_profile', 'N/A')}")

    print(f"\n    Confiance    : {result.get('parsing_confidence', 'N/A')}")


def process_cv(pdf_path: str) -> dict:
    print(f"\nTraitement : {Path(pdf_path).name}")
    print("-" * 50)

    print("  Etape 1 : Extraction texte (fitz)...")
    text = extract_text_fitz(pdf_path)

    if not text or text == "NO TEXT EXTRACTED" or len(text.strip()) < 50:
        print("  ERREUR : Texte insuffisant")
        return {"error": "insufficient_text", "file": str(pdf_path)}

    print("  Etape 2 : Structuration avec Mistral Large...")
    try:
        result = structure_with_llm(text)
        result = result_to_dict(result)
        result["file_path"] = str(pdf_path)
        result["extraction_method"] = "fitz + mistral-large"
        display_result(result)
        return result
    except Exception as e:
        print(f"  Erreur finale : {e}")
        return {"error": str(e), "file": str(pdf_path)}


def process_folder(
    folder_path: str,
    output_path: str = "extracted_cvs.json",
    limit: int = None,
    delay: float = 3.0,
    resume: bool = True
):
    """
    Traite tous les PDFs d'un dossier
    resume=True : reprend depuis le dernier CV traite (evite de recommencer)
    """
    folder = Path(folder_path)
    pdf_files = list(folder.glob("*.pdf"))
    if limit:
        pdf_files = pdf_files[:limit]

    if not pdf_files:
        print(f"Aucun PDF dans {folder_path}")
        return []

    # Reprise depuis le fichier existant
    results = []
    processed_files = set()
    output = Path(output_path)

    if resume and output.exists():
        with open(output, encoding="utf-8") as f:
            results = json.load(f)
        for r in results:
            if "file_path" in r or "file" in r:
                fname = Path(r.get("file_path", r.get("file", ""))).name
                processed_files.add(fname)
        print(f"Reprise : {len(processed_files)} CVs deja traites")

    # Filtrer les fichiers non encore traites
    remaining = [p for p in pdf_files if p.name not in processed_files]
    print(f"\n{len(remaining)} CVs restants a traiter (sur {len(pdf_files)} total)")
    print("=" * 50)

    success = sum(1 for r in results if "error" not in r)
    errors  = sum(1 for r in results if "error" in r)

    for i, pdf_path in enumerate(remaining):
        print(f"\n[{i+1}/{len(remaining)}]")
        result = process_cv(str(pdf_path))
        results.append(result)

        if "error" not in result:
            success += 1
        else:
            errors += 1

        # Sauvegarde incrementale — en cas d interruption
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        if i < len(remaining) - 1:
            print(f"  Pause {delay}s...")
            time.sleep(delay)

    print("\n" + "=" * 50)
    print("EXTRACTION TERMINEE")
    print("=" * 50)
    print(f"  Total    : {len(pdf_files)}")
    print(f"  Succes   : {success}")
    print(f"  Erreurs  : {errors}")
    print(f"  Fichier  : {output_path}")

    return results


def compare_with_ground_truth(extracted_path: str, ground_truth_path: str):
    with open(extracted_path, encoding="utf-8") as f:
        extracted = json.load(f)
    with open(ground_truth_path, encoding="utf-8") as f:
        ground_truth = json.load(f)

    print("\nCOMPARAISON EXTRACTION vs VERITE TERRAIN")
    print("=" * 55)

    count = min(len(extracted), len(ground_truth))
    scores = {
        "full_name": [], "email": [], "location": [],
        "years_of_experience": [], "programming_languages": [], "technical_skills": [],
    }

    for i in range(count):
        ext = extracted[i]
        gt  = ground_truth[i]
        if "error" in ext:
            continue
        for field in ["full_name", "email", "location"]:
            match = (str(ext.get(field, "")).lower().strip() == str(gt.get(field, "")).lower().strip())
            scores[field].append(1 if match else 0)
        ext_y = int(ext.get("years_of_experience") or 0)
        gt_y  = int(gt.get("years_of_experience") or 0)
        scores["years_of_experience"].append(1 if abs(ext_y - gt_y) <= 1 else 0)
        for field in ["programming_languages", "technical_skills"]:
            s1 = set(s.lower() for s in (ext.get(field) or []))
            s2 = set(s.lower() for s in (gt.get(field) or []))
            if s1 or s2:
                jaccard = len(s1 & s2) / len(s1 | s2)
                scores[field].append(jaccard)

    print(f"\n  Sur {count} CVs compares :\n")
    for field, vals in scores.items():
        if not vals:
            continue
        avg = sum(vals) / len(vals) * 100
        bar = chr(9608) * int(avg / 5)
        print(f"  {field:30} : {avg:5.1f}%  {bar}")


if __name__ == "__main__":

    base_dir     = Path(__file__).parent.parent
    pdf_folder   = base_dir / "data" / "synthetic_data_cv" / "pdf"
    ground_truth = base_dir / "data" / "synthetic_data_cv" / "dataset_complet.json"
    output_file  = Path(__file__).parent / "extracted_cvs.json"

    if pdf_folder.exists():
        process_folder(
            folder_path=str(pdf_folder),
            output_path=str(output_file),
            limit=None,      # None = tous les fichiers
            delay=3.0,       # 3 secondes entre chaque CV
            resume=True      # Reprend depuis ou on s est arrete
        )
        if ground_truth.exists():
            compare_with_ground_truth(str(output_file), str(ground_truth))
    else:
        print(f"Dossier non trouve : {pdf_folder}")