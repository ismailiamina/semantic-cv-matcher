
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
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

OUTPUT_DIR       = Path("data/real_data")
URLS_FILE        = OUTPUT_DIR / "jobs_urls.json"
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
7. technical_skills : outils, frameworks, plateformes
   NOTE : Bash est à la fois langage ET outil → inclure dans les deux listes
8. experience_level : Junior(0-2ans) / Medior(2-4ans) / Confirmé(4-6ans) / Senior(6-10ans) / Expert(10+ans)
9. seniority_requirements_technologies : top 3 technologies avec niveau attendu
10. seniority_requirements_programming_languages : top 2 langages avec niveau attendu
11. salary_range : si mentionné, sinon "non specified"
12. education_requirements : Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus / non specified
13. years_of_experience_required : nombre entier, 0 si non mentionné
14. summary : 2-3 phrases résumant le poste, l'entreprise et les compétences clés
15. posted : date si mentionnée (YYYY-MM-DD), sinon date du jour
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

    print(f"Connexion avec {email}...")
    driver.get("https://www.linkedin.com/login")
    time.sleep(3)

    for sel in ["#username", "input[name='session_key']"]:
        try:
            f = driver.find_element(By.CSS_SELECTOR, sel)
            f.clear(); f.send_keys(email); break
        except Exception: continue

    time.sleep(1)
    for sel in ["#password", "input[type='password']"]:
        try:
            f = driver.find_element(By.CSS_SELECTOR, sel)
            f.clear(); f.send_keys(password); break
        except Exception: continue

    time.sleep(1)
    for sel in ["button[type='submit']", "button.btn__primary--large"]:
        try:
            driver.find_element(By.CSS_SELECTOR, sel).click(); break
        except Exception: continue

    time.sleep(5)
    if "checkpoint" in driver.current_url:
        print("Verification manuelle requise. Completez puis appuyez sur Entree.")
        input()
        driver.get("https://www.linkedin.com/feed/")
        time.sleep(3)

    ok = "feed" in driver.current_url or "mynetwork" in driver.current_url
    print("Connecte." if ok else "Connexion echouee.")
    return ok



def scrape_job_page(driver, url: str) -> str:
    """Scrape le contenu d'une page offre LinkedIn"""
    driver.get(url)
    time.sleep(5)

    # Depliage description
    for xpath in [
        "//button[contains(., 'Voir plus')]",
        "//button[contains(., 'Show more')]",
        "//button[contains(@class,'show-more-less-html__button')]"
    ]:
        try:
            btn = driver.find_element(By.XPATH, xpath)
            driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
        except Exception:
            pass

    # Scroll pour charger le contenu
    for step in range(0, 4000, 400):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.3)
    time.sleep(1)

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
                PROMPT + f"\n\nURL : {job_url}\n\nCONTENU :\n{job_text}"
            )
            if hasattr(result, "model_dump"): return result.model_dump()
            if hasattr(result, "dict"):       return result.dict()
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



def create_urls_template():
    """Cree un fichier template pour que l'utilisateur colle ses URLs"""
    template = [
        "# INSTRUCTIONS :",
        "# 1. Va sur LinkedIn et ouvre chaque offre de Go & Dev et ses clients",
        "# 2. Copie l URL de chaque offre (ex: https://www.linkedin.com/jobs/view/4194104278/)",
        "# 3. Colle-les ici en remplacant les exemples ci-dessous",
        "# 4. Supprime ces lignes de commentaires (commencant par #)",
        "# 5. Lance : python jobs_scraper.py",
        "",
        "Sources :",
        "  Go & Dev    : https://www.linkedin.com/company/goanddev/jobs/",
        "  Cleva       : https://www.linkedin.com/company/clevasolutions/jobs/",
        "  Centreon    : https://www.linkedin.com/company/centreonsoftware/jobs/",
        "  Ascentiel   : https://www.linkedin.com/company/ascentiel-groupe/jobs/",
    ]

    print("\n" + "=" * 60)
    print("FICHIER URLs MANQUANT OU VIDE")
    print("=" * 60)
    print(f"\nFichier a remplir : {URLS_FILE}")
    print("\nSteps :")
    print("  1. Va sur LinkedIn dans ton navigateur")
    print("  2. Ouvre la page Jobs de Go & Dev :")
    print("     https://www.linkedin.com/company/goanddev/jobs/")
    print("  3. Clique sur chaque offre et copie l URL")
    print("  4. Fais pareil pour Cleva, Centreon, Ascentiel")
    print(f"  5. Colle toutes les URLs dans : {URLS_FILE}")
    print("\nFormat attendu dans le fichier JSON :")
    print('  [')
    print('    "https://www.linkedin.com/jobs/view/4194104278/",')
    print('    "https://www.linkedin.com/jobs/view/4194104279/"')
    print('  ]')

    # Creer le fichier vide avec des exemples commentes
    with open(URLS_FILE, "w", encoding="utf-8") as f:
        json.dump([
            "REMPLACE_CETTE_URL_PAR_UNE_VRAIE_URL_LINKEDIN_JOBS",
            "https://www.linkedin.com/jobs/view/EXEMPLE/"
        ], f, ensure_ascii=False, indent=2)

    print(f"\nFichier cree : {URLS_FILE}")
    print("Remplis-le avec les vraies URLs et relance le script.")



def run_jobs_scraping():
    print("=" * 60)
    print("SCRAPING OFFRES D EMPLOI — GO & DEV + CLIENTS")
    print("=" * 60)

    # Verifier le fichier URLs
    if not URLS_FILE.exists():
        create_urls_template()
        return

    with open(URLS_FILE, encoding="utf-8") as f:
        all_urls = json.load(f)

    # Filtrer les URLs valides — accepte /jobs/view/ ET /company/.../jobs/
    valid_urls = [
        u for u in all_urls
        if isinstance(u, str)
        and "linkedin.com" in u
        and not u.startswith("REMPLACE")
        and ("/jobs/view/" in u or "/company/" in u)
    ]

    if not valid_urls:
        print("\nAucune URL valide dans le fichier.")
        create_urls_template()
        return

    print(f"{len(valid_urls)} URLs a traiter")

    # Séparer les URLs de pages entreprise et les URLs d'offres individuelles
    company_pages = [u for u in valid_urls if "/company/" in u]
    job_pages     = [u for u in valid_urls if "/jobs/view/" in u]

    print(f"  Pages entreprise : {len(company_pages)}")
    print(f"  Offres directes  : {len(job_pages)}")

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
    success = sum(1 for r in jobs_results if "error" not in r)
    errors  = sum(1 for r in jobs_results if "error" in r)

    try:
        driver = setup_driver()

        if not linkedin_login(driver):
            print("Connexion impossible.")
            return

        # Extraire les offres depuis les pages entreprise
        if company_pages:
            print(f"\nExtraction des offres depuis {len(company_pages)} pages entreprise...")
            for company_url in company_pages:
                print(f"  {company_url}")
                driver.get(company_url)
                time.sleep(8)

                # Scroll pour charger les offres
                for step in range(0, 6000, 400):
                    driver.execute_script(f"window.scrollTo(0, {step})")
                    time.sleep(0.4)
                time.sleep(3)

                # Extraire les URLs /jobs/view/ depuis le DOM
                links = driver.find_elements(By.XPATH, "//a[contains(@href, '/jobs/view/')]")
                for link in links:
                    try:
                        href = link.get_attribute("href")
                        if href and "/jobs/view/" in href:
                            job_pages.append(href.split("?")[0])
                    except Exception:
                        continue

                # Regex sur le source
                source = driver.page_source
                for jid in re.findall(r'/jobs/view/(\d+)/', source):
                    job_pages.append(f"https://www.linkedin.com/jobs/view/{jid}/")

                # Parcourir le carousel
                for _ in range(10):
                    clicked = False
                    for xpath in [
                        "//button[@aria-label='Suivant']",
                        "//button[@aria-label='Next']",
                        "//button[contains(@class,'artdeco-carousel__next-button')]",
                    ]:
                        try:
                            btn = driver.find_element(By.XPATH, xpath)
                            if btn.is_enabled():
                                driver.execute_script("arguments[0].click();", btn)
                                time.sleep(3)
                                links = driver.find_elements(By.XPATH, "//a[contains(@href, '/jobs/view/')]")
                                for link in links:
                                    try:
                                        href = link.get_attribute("href")
                                        if href and "/jobs/view/" in href:
                                            job_pages.append(href.split("?")[0])
                                    except Exception:
                                        continue
                                clicked = True
                                break
                        except Exception:
                            continue
                    if not clicked:
                        break

                print(f"    Offres trouvees sur cette page : {len(job_pages)}")

            # Dedupliquer
            job_pages = list(set(job_pages))
            print(f"  Total offres a scraper : {len(job_pages)}")

        # Filtrer les deja traites
        remaining = [u for u in job_pages
                     if u.split("?")[0].lower() not in processed_urls]

        if not remaining:
            print("Toutes les offres sont deja traitees.")
            return

        print(f"\n{len(remaining)} offres restantes a scraper")
        print("=" * 60)

        for i, url in enumerate(remaining):
            print(f"\n[{i+1}/{len(remaining)}] {url}")

            # Scraper le contenu de l offre
            print("  Scraping du contenu...")
            text = scrape_job_page(driver, url)

            if not text or len(text.strip()) < 100:
                print("  Contenu insuffisant — offre supprimee ou privee.")
                errors += 1
                jobs_results.append({"error": "insufficient_content", "file_path": url})
            else:
                print(f"  Texte : {len(text)} caracteres")
                print("  Structuration avec Mistral Large...")

                result = structure_with_mistral(text, url)

                if "error" in result:
                    errors += 1
                    print(f"  Erreur : {result['error'][:80]}")
                    jobs_results.append({"error": result["error"], "file_path": url})
                else:
                    result["file_path"]         = url
                    result["extraction_method"] = "selenium + mistral-large"
                    result["extracted_at"]      = datetime.now().isoformat()
                    jobs_results.append(result)
                    success += 1
                    print(f"  OK — {result.get('title')} | "
                          f"{result.get('company')} | "
                          f"{result.get('experience_level', 'N/A')}")

            # Sauvegarde incrementale
            with open(OUTPUT_JOBS_FILE, "w", encoding="utf-8") as f:
                json.dump(jobs_results, f, ensure_ascii=False, indent=2)

            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            print(f"  Pause {delay:.1f}s...")
            time.sleep(delay)

        print("\n" + "=" * 60)
        print("SCRAPING TERMINE")
        print("=" * 60)
        print(f"  Succes    : {success}")
        print(f"  Erreurs   : {errors}")
        print(f"  Fichier   : {OUTPUT_JOBS_FILE}")
        print(f"\n  Prochaine etape :")
        print(f"    Dans insert_data.py :")
        print(f"    JOB_JSON_PATH = BASE_DIR / 'data' / 'real_data' / 'extracted_jobs_reals.json'")

    except KeyboardInterrupt:
        print("\nArret manuel.")
    except Exception as e:
        print(f"\nErreur : {e}")
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
        with open(OUTPUT_JOBS_FILE, "w", encoding="utf-8") as f:
            json.dump(jobs_results, f, ensure_ascii=False, indent=2)
        print("Fichier sauvegarde.")


if __name__ == "__main__":
    run_jobs_scraping()