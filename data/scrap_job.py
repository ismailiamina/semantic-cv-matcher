
import json
import time
import random
import os
import re
from pathlib import Path
from datetime import datetime
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


GODEV_JOBS_URL = "https://www.linkedin.com/company/goanddev/jobs/"

# Clients directs de Go & Dev — ajouter autant de liens que necessaire
CLIENT_JOBS_URLS = [
    "https://www.linkedin.com/company/clevasolutions/jobs/",
    "https://www.linkedin.com/company/centreonsoftware/jobs/",
    "https://www.linkedin.com/company/ascentiel-groupe/jobs/",
]

OUTPUT_DIR       = Path("data/real_data")
OUTPUT_JOBS_RAW  = OUTPUT_DIR / "godev_jobs_raw.json"
OUTPUT_JOBS_FILE = OUTPUT_DIR / "extracted_jobs_real.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_DELAY = 4
MAX_DELAY = 8



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
            "enum": ["Full-time", "Part-time", "Fixed-term",
                     "Casual", "Temporary", "Internship", "not specified"]
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
                              "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
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
                    "level": {"type": "string",
                              "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
                },
                "required": ["language", "level"]
            }
        },
        "experience_level": {
            "type": "string",
            "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]
        },
        "salary_range":                 {"type": "string"},
        "education_requirements":       {"type": "string"},
        "years_of_experience_required": {"type": "number"},
        "summary":                      {"type": "string"}
    },
    "required": [
        "title", "company", "job_description",
        "programming_languages", "technical_skills",
        "certifications", "summary"
    ]
}

PROMPT = """Tu reçois le texte d'une offre d'emploi IT.
Structure ces informations en JSON.

REGLES ABSOLUES :
1. Utilise UNIQUEMENT les informations présentes dans le texte
2. N'invente RIEN — si absent : "not specified" ou [] ou 0
3. title : titre exact du poste
4. company : nom exact de l'entreprise qui publie l'offre
5. employment_type : Full-time / Part-time / Fixed-term / Internship / not specified
6. programming_languages : langages de code uniquement (Python, Java, SQL, JavaScript...)
7. technical_skills : outils, frameworks, plateformes (Docker, AWS, React, Angular...)
   NOTE : Bash est à la fois langage ET outil → inclure dans les deux listes
8. experience_level : Junior(0-2ans) / Medior(2-4ans) / Confirmé(4-6ans) / Senior(6-10ans) / Expert(10+ans)
9. seniority_requirements_technologies : top 3 technologies avec niveau attendu
10. seniority_requirements_programming_languages : top 2 langages avec niveau attendu
11. salary_range : si mentionné, sinon "non specified"
12. education_requirements : Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus / non specified
13. years_of_experience_required : nombre entier, 0 si non mentionné
14. summary : 2-3 phrases résumant le poste, l'entreprise et les compétences clés
15. posted : date de publication si mentionnée (format YYYY-MM-DD), sinon date du jour
"""


def setup_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)
    options.add_argument("--start-maximized")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    service = Service(ChromeDriverManager().install())
    driver  = webdriver.Chrome(service=service, options=options)
    driver.execute_script(
        "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
    )
    return driver


def linkedin_login(driver) -> bool:
    email    = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")

    if not email or not password:
        raise ValueError("LINKEDIN_EMAIL et LINKEDIN_PASSWORD manquants dans .env")

    print(f"Connexion LinkedIn avec {email}...")
    driver.get("https://www.linkedin.com/login")
    time.sleep(3)

    for selector in ["#username", "input[name='session_key']"]:
        try:
            field = driver.find_element(By.CSS_SELECTOR, selector)
            field.clear()
            field.send_keys(email)
            break
        except Exception:
            continue

    time.sleep(1)

    for selector in ["#password", "input[type='password']"]:
        try:
            field = driver.find_element(By.CSS_SELECTOR, selector)
            field.clear()
            field.send_keys(password)
            break
        except Exception:
            continue

    time.sleep(1)

    for selector in ["button[type='submit']", "button.btn__primary--large"]:
        try:
            driver.find_element(By.CSS_SELECTOR, selector).click()
            break
        except Exception:
            continue

    time.sleep(5)

    if "checkpoint" in driver.current_url:
        print("Verification manuelle requise. Completez puis appuyez sur Entree.")
        input()
        driver.get("https://www.linkedin.com/feed/")
        time.sleep(3)

    connected = "feed" in driver.current_url or "mynetwork" in driver.current_url
    print("Connecte." if connected else "Connexion echouee.")
    return connected


def extract_job_urls_from_page(driver, company_jobs_url: str) -> set:
    """
    Extrait toutes les URLs d'offres depuis une page /jobs/ LinkedIn
    Gere le carousel et le chargement dynamique
    """
    print(f"  Chargement : {company_jobs_url}")
    driver.get(company_jobs_url)
    time.sleep(5)

    job_urls = set()

    # Parcourir le carousel avec le bouton Suivant
    for click in range(15):
        page_source = driver.page_source
        soup        = BeautifulSoup(page_source, "html.parser")

        # Chercher les liens /jobs/view/
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            if "/jobs/view/" in href:
                clean = href.split("?")[0]
                if not clean.startswith("http"):
                    clean = "https://www.linkedin.com" + clean
                job_urls.add(clean)

        # Chercher les data-entity-urn pour reconstruire les URLs
        for tag in soup.find_all(attrs={"data-entity-urn": True}):
            urn = tag.get("data-entity-urn", "")
            if "jobPosting" in urn:
                job_id = urn.split(":")[-1]
                job_urls.add(f"https://www.linkedin.com/jobs/view/{job_id}/")

        print(f"    Click {click+1} : {len(job_urls)} offres")

        # Cliquer sur Suivant
        clicked = False
        for xpath in [
            "//button[@aria-label='Suivant']",
            "//button[@aria-label='Next']",
            "//button[contains(@class, 'artdeco-carousel__next-button')]",
            "//button[contains(@aria-label, 'suivant')]",
        ]:
            try:
                btn = driver.find_element(By.XPATH, xpath)
                if btn.is_enabled():
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(2)
                    clicked = True
                    break
            except Exception:
                continue

        if not clicked:
            break

    # Scroll pour charger d'eventuelles offres supplementaires
    for step in range(0, 4000, 500):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.4)
    time.sleep(2)

    # Re-extraire apres scroll
    soup = BeautifulSoup(driver.page_source, "html.parser")
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        if "/jobs/view/" in href:
            clean = href.split("?")[0]
            if not clean.startswith("http"):
                clean = "https://www.linkedin.com" + clean
            job_urls.add(clean)

    print(f"    Total : {len(job_urls)} offres trouvees")
    return job_urls


def scrape_job_page(driver, url: str) -> str:
    """
    Scrape le contenu d'une page d'offre LinkedIn
    """
    driver.get(url)
    time.sleep(4)

    # Depliage de la description
    for xpath in [
        "//button[contains(., 'Voir plus')]",
        "//button[contains(., 'Show more')]",
        "//button[contains(@class, 'show-more-less-html__button')]"
    ]:
        try:
            btn = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
        except Exception:
            pass

    soup = BeautifulSoup(driver.page_source, "html.parser")
    for tag in soup(["script", "style", "nav", "footer", "header",
                     "meta", "noscript", "svg", "button", "aside"]):
        tag.decompose()

    text  = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip() and len(l.strip()) > 2]
    return "\n".join(lines)[:8000]



def structure_with_mistral(job_text: str, job_url: str,
                            max_retries: int = 3) -> dict:
    from langchain_mistralai import ChatMistralAI

    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        return {"error": "MISTRAL_API_KEY manquante"}

    llm = ChatMistralAI(
        model="mistral-large-latest",
        api_key=api_key,
        timeout=90,
        max_retries=2
    )
    structured_llm = llm.with_structured_output(schema=JSON_SCHEMA)

    for attempt in range(max_retries):
        try:
            result = structured_llm.invoke(
                PROMPT + f"\n\nURL : {job_url}\n\nCONTENU DE L'OFFRE :\n{job_text}"
            )
            if hasattr(result, "model_dump"):
                return result.model_dump()
            if hasattr(result, "dict"):
                return result.dict()
            return dict(result)

        except Exception as e:
            error = str(e)
            if "429" in error or "rate" in error.lower():
                wait = (attempt + 1) * 30
                print(f"    Rate limit — attente {wait}s...")
                time.sleep(wait)
            elif "timeout" in error.lower():
                time.sleep(15)
            else:
                print(f"    Erreur Mistral : {error[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(10)
                else:
                    return {"error": error}

    return {"error": "Echec Mistral"}



def run_jobs_scraping(max_per_source: int = 30):
    print("=" * 60)
    print("SCRAPING OFFRES D'EMPLOI — GO & DEV + CLIENTS")
    print("=" * 60)

    # Reprise
    jobs_results   = []
    processed_urls = set()

    if OUTPUT_JOBS_FILE.exists():
        with open(OUTPUT_JOBS_FILE, encoding="utf-8") as f:
            jobs_results = json.load(f)
        for r in jobs_results:
            fp = r.get("file_path", "")
            if fp:
                processed_urls.add(fp.split("?")[0].lower())
        print(f"Reprise : {len(processed_urls)} offres deja traitees.")

    driver = None
    all_jobs_text = []

    try:
        driver = setup_driver()

        if not linkedin_login(driver):
            print("Connexion LinkedIn echouee.")
            return

        # ── Option 1 — Go & Dev Jobs ──────────────
        print(f"\nOption 1 — Go & Dev ({GODEV_JOBS_URL})")
        godev_urls = extract_job_urls_from_page(driver, GODEV_JOBS_URL)
        for url in list(godev_urls)[:max_per_source]:
            text = scrape_job_page(driver, url)
            if len(text) > 100:
                all_jobs_text.append({"url": url, "text": text, "source": "goanddev"})
            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
        print(f"  Go & Dev : {len(godev_urls)} offres collectees")

        # ── Option 2 — Clients directs ────────────
        for client_url in CLIENT_JOBS_URLS:
            company_name = client_url.split("/company/")[1].split("/")[0]
            print(f"\nOption 2 — Client : {company_name} ({client_url})")

            client_urls = extract_job_urls_from_page(driver, client_url)
            for url in list(client_urls)[:max_per_source]:
                text = scrape_job_page(driver, url)
                if len(text) > 100:
                    all_jobs_text.append({"url": url, "text": text, "source": company_name})
                time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
            print(f"  {company_name} : {len(client_urls)} offres collectees")

        # Sauvegarder les textes bruts
        with open(OUTPUT_JOBS_RAW, "w", encoding="utf-8") as f:
            json.dump(all_jobs_text, f, ensure_ascii=False, indent=2)

        # Filtrer les deja traites
        remaining = [
            j for j in all_jobs_text
            if j["url"].split("?")[0].lower() not in processed_urls
        ]

        print(f"\n{len(remaining)} offres a structurer avec Mistral")
        print("=" * 60)

        success = sum(1 for r in jobs_results if "error" not in r)
        errors  = sum(1 for r in jobs_results if "error" in r)

        for i, job in enumerate(remaining):
            url    = job["url"]
            text   = job["text"]
            source = job["source"]

            print(f"\n[{i+1}/{len(remaining)}] {source.upper()} — {url.split('/')[-2]}")

            result = structure_with_mistral(text, url)

            if "error" in result:
                errors += 1
                print(f"  Erreur : {result['error'][:80]}")
                jobs_results.append({"error": result["error"], "file_path": url})
            else:
                result["file_path"]         = url
                result["extraction_method"] = f"{source} + mistral-large"
                result["extracted_at"]      = datetime.now().isoformat()
                jobs_results.append(result)
                success += 1
                print(f"  OK — {result.get('title')} | "
                      f"{result.get('company')} | "
                      f"{result.get('experience_level', 'N/A')}")

            # Sauvegarde incrementale
            with open(OUTPUT_JOBS_FILE, "w", encoding="utf-8") as f:
                json.dump(jobs_results, f, ensure_ascii=False, indent=2)

            time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

        print("\n" + "=" * 60)
        print("SCRAPING OFFRES TERMINE")
        print("=" * 60)
        print(f"  Succes    : {success}")
        print(f"  Erreurs   : {errors}")
        print(f"  Fichier   : {OUTPUT_JOBS_FILE}")
        print(f"\n  Prochaine etape :")
        print(f"    Dans insert_data.py :")
        print(f"    JOB_JSON_PATH = BASE_DIR / 'data' / 'real_data' / 'extracted_jobs_real.json'")

    except KeyboardInterrupt:
        print("\nArret manuel.")
    except Exception as e:
        print(f"\nErreur : {e}")
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        with open(OUTPUT_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs_results, f, ensure_ascii=False, indent=2)
        print("Fichier sauvegarde.")


if __name__ == "__main__":
    run_jobs_scraping(max_per_source=30)