"""
Pipeline d'extraction de Fiches de Poste DOCX
Etape 1 : python-docx  -> extrait le texte brut du DOCX
"""

from langchain_mistralai import ChatMistralAI
from docx import Document
import json
import os
import time
from pathlib import Path
from dotenv import load_dotenv

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


JSON_SCHEMA = {
    "title": "JobProfile",
    "description": "Structured job offer information",
    "type": "object",
    "properties": {
        "title":           {"type": "string"},
        "company":         {"type": "string"},
        "industry":        {"type": "string"},
        "location":        {"type": "string"},
        "employment_type": {
            "type": "string",
            "enum": ["Full-time", "Part-time", "Fixed-term", "Casual", "Temporary", "Internship", "not specified"]
        },
        "job_description":  {"type": "string"},
        "posted":           {"type": "string"},
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
                    "level": {"type": "string", "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
                },
                "required": ["technology", "level"]
            }
        },
        "seniority_requirements_programming_languages": {
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
        "experience_level": {
            "type": "string",
            "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]
        },
        "salary_range":              {"type": "string"},
        "education_requirements":    {"type": "string"},
        "years_of_experience_required": {"type": "number"},
        "summary":                   {"type": "string"}
    },
    "required": [
        "title", "job_description", "company",
        "programming_languages", "technical_skills",
        "certifications", "summary"
    ]
}

PROMPT = """Extract information from this job description text and return ONLY a JSON object.

1. JOB INFO:
   - title: The role or job title being hired for
   - company: The name of the company offering the job
   - industry: The domain/sector (e.g., IT, Finance, Healthcare, Education)
   - location: Where the job is based. If remote, specify "Remote"
   - employment_type: Choose from Full-time / Part-time / Fixed-term / Casual / Temporary / Internship / not specified
   - job_description: Extract the full job description as a string
   - posted: Date posted in YYYY-MM-DD format, if not found use 0000-00-00

2. SKILLS:
   - programming_languages: all programming languages required
   - technical_skills: tools, frameworks, technologies, platforms
     NOTE: Bash and Shell are BOTH a language AND a tool - include in BOTH lists
   - spoken_languages: spoken languages required or preferred

3. CERTIFICATIONS:
   - Only recognized IT certifications (AWS, GCP, Azure, Cisco, Scrum Alliance, Oracle, IBM, CompTIA)
   - If none mentioned, use []

4. SENIORITY REQUIREMENTS:
   - seniority_requirements_technologies: up to 3 technologies with expected level
   - seniority_requirements_programming_languages: up to 2 languages with expected level
   - Levels: Junior(0-2 ans) / Medior(2-4 ans) / Confirme(4-6 ans) / Senior(6-10 ans) / Expert(10+)

5. experience_level: Junior / Medior / Confirme / Senior / Expert

6. salary_range: if mentioned extract it, otherwise "non specified"

7. education_requirements: Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus, or "non specified"

8. years_of_experience_required: number of years required

9. summary: 3-line paragraph describing the job, required skills, company, experience required

Try to infer correct values even if described indirectly.
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
            result = structured_llm.invoke(PROMPT + "\n\nJOB DESCRIPTION TEXT:\n" + text)
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


def extract_text_docx(docx_path: str) -> str:
    text = ""
    try:
        doc = Document(docx_path)
        for para in doc.paragraphs:
            if para.text.strip():
                text += para.text + "\n"
        print(f"  Texte extrait : {len(text)} caracteres")
    except Exception as e:
        print(f"  Erreur python-docx : {e}")
        text = "NO TEXT EXTRACTED"
    return text

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
    print(f"    Titre          : {result.get('title', 'N/A')}")
    print(f"    Entreprise     : {result.get('company', 'N/A')}")
    print(f"    Secteur        : {result.get('industry', 'N/A')}")
    print(f"    Ville          : {result.get('location', 'N/A')}")
    print(f"    Niveau         : {result.get('experience_level', 'N/A')}")
    print(f"    Exp. requise   : {result.get('years_of_experience_required', 'N/A')} ans")
    print(f"    Salaire        : {result.get('salary_range', 'N/A')}")
    skills = result.get('technical_skills', [])
    print(f"    Skills         : {', '.join(skills[:5])}{'...' if len(skills) > 5 else ''}")


def process_job(docx_path: str) -> dict:
    print(f"\nTraitement : {Path(docx_path).name}")
    print("-" * 50)

    print("  Etape 1 : Extraction texte (python-docx)...")
    text = extract_text_docx(docx_path)

    if not text or text == "NO TEXT EXTRACTED" or len(text.strip()) < 30:
        print("  ERREUR : Texte insuffisant")
        return {"error": "insufficient_text", "file": str(docx_path)}

    print("  Etape 2 : Structuration avec Mistral Large...")
    try:
        result = structure_with_llm(text)
        result = result_to_dict(result)
        result["file_path"] = str(docx_path)
        result["extraction_method"] = "python-docx + mistral-large"
        display_result(result)
        return result
    except Exception as e:
        print(f"  Erreur finale : {e}")
        return {"error": str(e), "file": str(docx_path)}

def process_folder(
    folder_path: str,
    output_path: str = "extracted_jobs.json",
    limit: int = None,
    delay: float = 3.0,
    resume: bool = True
):
    """
    Traite tous les fichiers DOCX d'un dossier
    resume=True : reprend depuis le dernier job traite
    """
    folder = Path(folder_path)
    docx_files = list(folder.glob("*.docx"))
    if limit:
        docx_files = docx_files[:limit]

    if not docx_files:
        print(f"Aucun DOCX dans {folder_path}")
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
        print(f"Reprise : {len(processed_files)} jobs deja traites")

    remaining = [p for p in docx_files if p.name not in processed_files]
    print(f"\n{len(remaining)} jobs restants a traiter (sur {len(docx_files)} total)")
    print("=" * 50)

    success = sum(1 for r in results if "error" not in r)
    errors  = sum(1 for r in results if "error" in r)

    for i, docx_path in enumerate(remaining):
        print(f"\n[{i+1}/{len(remaining)}]")
        result = process_job(str(docx_path))
        results.append(result)

        if "error" not in result:
            success += 1
        else:
            errors += 1

        # Sauvegarde incrementale
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(results, f, ensure_ascii=False, indent=2)

        if i < len(remaining) - 1:
            print(f"  Pause {delay}s...")
            time.sleep(delay)

    print("\n" + "=" * 50)
    print("EXTRACTION TERMINEE")
    print("=" * 50)
    print(f"  Total    : {len(docx_files)}")
    print(f"  Succes   : {success}")
    print(f"  Erreurs  : {errors}")
    print(f"  Fichier  : {output_path}")

    return results


if __name__ == "__main__":

    base_dir    = Path(__file__).parent.parent
    docx_folder = base_dir / "data" / "synthetic_data" / "jobs" / "docx"
    output_file = Path(__file__).parent / "extracted_jobs.json"

    if docx_folder.exists():
        process_folder(
            folder_path=str(docx_folder),
            output_path=str(output_file),
            limit=None,     # None = tous les fichiers
            delay=3.0,      # 3 secondes entre chaque job
            resume=True     # Reprend depuis ou on s est arrete
        )
    else:
        print(f"Dossier non trouve : {docx_folder}")
        print("Lance d'abord : python data/generate_synthetic_jobs.py")