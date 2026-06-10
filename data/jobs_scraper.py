"""
jobs_scraper.py
================
Scraping automatique des offres d'emploi LinkedIn.
- Collecte automatique des URLs depuis les pages Jobs de chaque entreprise
- Cible : 40 offres par entreprise
- Anti-doublons
- Sauvegarde incrémentale avec os.fsync
"""

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

# ─── CONFIGURATION ────────────────────────────────────────────────────

COMPANY_JOBS_URLS = [
    ("goanddev",      "https://www.linkedin.com/company/goanddev/jobs/"),
    ("cleva",         "https://www.linkedin.com/company/clevasolutions/jobs/"),
    ("centreon",      "https://www.linkedin.com/company/centreonsoftware/jobs/"),
    ("devoteam",      "https://www.linkedin.com/company/devoteam/jobs/"),
    ("sqli",          "https://www.linkedin.com/company/sqli/jobs/"),
    ("capgemini",     "https://www.linkedin.com/company/capgemini/jobs/"),
    ("dxcmorocco",    "https://www.linkedin.com/company/dxcmorocco/jobs/"),
]

TARGET_JOBS_PER_COMPANY = 40

OUTPUT_DIR       = Path("data/real_data")
URLS_FILE        = OUTPUT_DIR / "jobs_urls.json"
OUTPUT_JOBS_FILE = OUTPUT_DIR / "extracted_jobs_real.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_DELAY = 3
MAX_DELAY = 6


# ─── SCHEMA JSON ──────────────────────────────────────────────────────

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


# ─── PROMPT ───────────────────────────────────────────────────────────

PROMPT = """Tu recois le texte d'une offre d'emploi IT.
Structure ces informations en JSON.

REGLES ABSOLUES :
1. Utilise UNIQUEMENT les informations presentes dans le texte
2. N'invente RIEN — si absent : "not specified" ou [] ou 0
3. title : titre exact du poste
4. company : nom exact de l'entreprise qui publie l'offre
5. employment_type : Full-time / Part-time / Fixed-term / Internship / not specified
6. programming_languages : langages de code uniquement (Python, Java, SQL, JavaScript...)
7. technical_skills : outils, frameworks, plateformes
8. experience_level : Junior(0-2ans) / Medior(2-4ans) / Confirme(4-6ans) / Senior(6-10ans) / Expert(10+ans)
9. seniority_requirements_technologies : top 3 technologies avec niveau attendu
10. seniority_requirements_programming_languages : top 2 langages avec niveau attendu
11. salary_range : si mentionne, sinon "not specified"
12. education_requirements : Bac+2 / Bac+3 / Bac+4 / Bac+5 / Bac+6 et plus / not specified
13. years_of_experience_required : nombre entier, 0 si non mentionne
14. summary : 2-3 phrases resumant le poste, l'entreprise et les competences cles
15. posted : date si mentionnee (YYYY-MM-DD), sinon date du jour

ANTI-INVENTION :
Ne jamais ajouter une competence ou un langage qui n'est pas explicitement ecrit dans le texte.
"""


# ─── DRIVER ───────────────────────────────────────────────────────────

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


# ─── CONNEXION ────────────────────────────────────────────────────────

def linkedin_login(driver) -> bool:
    email    = os.getenv("LINKEDIN_EMAIL")
    password = os.getenv("LINKEDIN_PASSWORD")
    if not email or not password:
        raise ValueError("LINKEDIN_EMAIL et LINKEDIN_PASSWORD manquants dans .env")

    print(f"Connexion avec {email}...")
    driver.get("https://www.linkedin.com/login")
    time.sleep(3)

    for sel in ["#username","input[name='session_key']"]:
        try:
            f = driver.find_element(By.CSS_SELECTOR, sel)
            f.clear(); f.send_keys(email); break
        except Exception: continue

    time.sleep(1)
    for sel in ["#password","input[type='password']"]:
        try:
            f = driver.find_element(By.CSS_SELECTOR, sel)
            f.clear(); f.send_keys(password); break
        except Exception: continue

    time.sleep(1)
    for sel in ["button[type='submit']","button.btn__primary--large"]:
        try: driver.find_element(By.CSS_SELECTOR, sel).click(); break
        except Exception: continue

    time.sleep(5)

    print()
    print("  +-----------------------------------------------------+")
    print("  |  VERIFICATION CONNEXION LINKEDIN                    |")
    print("  |  Si verification requise -> complete dans navigateur|")
    print("  |  Appuie sur Entree quand tu es connecte ->          |")
    print("  +-----------------------------------------------------+")
    input()

    driver.get("https://www.linkedin.com/feed/")
    time.sleep(3)

    if "feed" in driver.current_url or "mynetwork" in driver.current_url:
        print("  Connecte avec succes."); return True
    print("  Connexion echouee."); return False


# ─── NORMALISATION NOM ENTREPRISE ─────────────────────────────────────

def normalize_company_name(name: str) -> str:
    """
    Normalise le nom de l'entreprise pour eviter les doublons de filiales.
    Exemple : Devoteam Cyber Trust -> Devoteam
    """
    name_lower = (name or "").lower().strip()
    mapping = [
        ("devoteam",    "Devoteam"),
        ("capgemini",   "Capgemini"),
        ("sqli",        "SQLI"),
        ("alten",       "Alten"),
        ("sopra",       "Sopra Steria"),
        ("go & dev",    "GO & DEV"),
        ("go&dev",      "GO & DEV"),
        ("goanddev",    "GO & DEV"),
        ("cleva",       "Cleva"),
        ("centreon",    "Centreon"),
        ("dxc",         "DXC Technology Morocco"),
        ("inetum",      "Inetum"),
    ]
    for key, canonical in mapping:
        if key in name_lower:
            return canonical
    return name


# ─── COLLECTE URLS ────────────────────────────────────────────────────

def collect_job_urls_from_company(driver, company_url: str,
                                   company_name: str, target: int = 40) -> list:
    print(f"\n  Collecte offres : {company_name}")
    driver.get(company_url)
    time.sleep(6)

    job_urls  = set()
    no_change = 0
    last_count = 0

    # Cliquer sur "Voir toutes les offres"
    clicked_see_all = False
    see_all_selectors = [
        "//a[contains(., 'Voir toutes les')]",
        "//a[contains(., 'See all')]",
        "//a[contains(., \"offres d'emploi\")]",
        "//a[contains(., 'emplois')]",
        "//a[contains(@href, '/jobs/search')]",
        "//a[contains(@href, 'f_C=')]",
    ]
    for sel in see_all_selectors:
        try:
            elems = driver.find_elements(By.XPATH, sel)
            for elem in elems:
                if not elem.is_displayed():
                    continue
                text = elem.text.strip()
                href = elem.get_attribute("href") or ""
                if (
                    any(w in text.lower() for w in ["voir", "all", "offres", "emplois"])
                    or "jobs/search" in href
                    or "f_C=" in href
                ):
                    print(f"    Clic sur : '{text[:60]}'")
                    driver.execute_script("arguments[0].click();", elem)
                    time.sleep(5)
                    clicked_see_all = True
                    break
            if clicked_see_all:
                break
        except Exception:
            continue

    if not clicked_see_all:
        print(f"    Lien voir toutes les offres non trouve — scroll page actuelle")

    # Scroll + pagination
    for scroll in range(80):
        driver.execute_script("window.scrollBy(0, 500);")
        time.sleep(1.5)

        # Methode 1 — liens directs
        for link in driver.find_elements(By.XPATH, "//a[contains(@href, '/jobs/view/')]"):
            try:
                href = link.get_attribute("href")
                if href and "/jobs/view/" in href:
                    job_urls.add(href.split("?")[0].rstrip("/"))
            except Exception:
                continue

        # Methode 2 — data-job-id
        for card in driver.find_elements(By.XPATH, "//*[@data-job-id]"):
            try:
                jid = card.get_attribute("data-job-id")
                if jid and jid.isdigit():
                    job_urls.add(f"https://www.linkedin.com/jobs/view/{jid}/")
            except Exception:
                continue

        # Methode 3 — data-entity-urn
        for urn in driver.find_elements(
            By.XPATH, "//*[contains(@data-entity-urn, 'jobPosting')]"
        ):
            try:
                val = urn.get_attribute("data-entity-urn")
                if val:
                    jid = val.split(":")[-1]
                    if jid.isdigit():
                        job_urls.add(f"https://www.linkedin.com/jobs/view/{jid}/")
            except Exception:
                continue

        # Methode 4 — regex JSON interne strict
        source = driver.page_source
        for jid in re.findall(r'"jobPostingId"\s*:\s*"?(\d{7,})"?', source):
            job_urls.add(f"https://www.linkedin.com/jobs/view/{jid}/")
        for jid in re.findall(r'"entityUrn"\s*:\s*"urn:li:jobPosting:(\d{7,})"', source):
            job_urls.add(f"https://www.linkedin.com/jobs/view/{jid}/")

        if len(job_urls) != last_count:
            no_change = 0
            print(f"    Scroll {scroll+1} : {len(job_urls)} offres trouvees")
        else:
            no_change += 1

            if no_change % 3 == 0:
                count_before = len(job_urls)
                clicked = False

                for sel in [
                    "//button[contains(., 'Afficher plus')]",
                    "//button[contains(., 'Voir plus')]",
                    "//button[contains(., 'Show more')]",
                    "//button[contains(., 'Load more')]",
                    "//button[@aria-label='Afficher plus de resultats']",
                    "//button[@aria-label='Show more results']",
                    "//button[contains(@class,'infinite-scroller__show-more')]",
                    "//button[contains(@class,'scaffold-finite-scroll__load-button')]",
                ]:
                    try:
                        btns = driver.find_elements(By.XPATH, sel)
                        for btn in btns:
                            if btn.is_displayed() and btn.is_enabled():
                                driver.execute_script(
                                    "arguments[0].scrollIntoView({block:'center'});", btn
                                )
                                time.sleep(1)
                                driver.execute_script("arguments[0].click();", btn)
                                time.sleep(5)
                                if len(job_urls) > count_before:
                                    clicked   = True
                                    no_change = 0
                                    print(f"    Bouton Afficher plus clique")
                                break
                        if clicked:
                            break
                    except Exception:
                        continue

                if not clicked:
                    try:
                        next_btn = driver.find_element(
                            By.XPATH,
                            "//button[@aria-label='Page suivante']"
                            " | //button[@aria-label='Next']"
                            " | //li[contains(@class,'artdeco-pagination__indicator')]"
                            "//button[not(@disabled)]"
                        )
                        if next_btn.is_enabled():
                            driver.execute_script(
                                "arguments[0].scrollIntoView({block:'center'});", next_btn
                            )
                            time.sleep(1)
                            driver.execute_script("arguments[0].click();", next_btn)
                            time.sleep(5)
                            no_change = 0
                            print(f"    Page suivante cliquee")
                    except Exception:
                        pass

            if no_change >= 9:
                print(f"    Fin du scroll — {len(job_urls)} offres trouvees")
                break

        last_count = len(job_urls)

        if len(job_urls) >= target:
            print(f"    Cible atteinte : {len(job_urls)} offres")
            break

    result = list(job_urls)[:target]
    print(f"  {len(result)} offres collectees pour {company_name}")
    return result


# ─── SCRAPING CONTENU OFFRE ───────────────────────────────────────────

def scrape_job_page(driver, url: str) -> str:
    driver.get(url)
    time.sleep(5)

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

    for step in range(0, 4000, 400):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.3)
    time.sleep(1)

    soup = BeautifulSoup(driver.page_source, "html.parser")
    for tag in soup(["script","style","nav","footer","header",
                     "meta","noscript","svg","button","aside"]):
        tag.decompose()

    text  = soup.get_text(separator="\n")
    lines = [l.strip() for l in text.splitlines() if l.strip() and len(l.strip()) > 2]

    excluded = [
        "s'identifier","sign in","voir des offres similaires",
        "personnes ont postule","signaler","copier le lien",
        "vous connaissez peut-etre","people also viewed"
    ]
    lines = [l for l in lines
             if not any(e in l.lower() for e in excluded)]

    return "\n".join(lines)[:8000]


# ─── STRUCTURATION MISTRAL ────────────────────────────────────────────

def structure_with_mistral(job_text: str, job_url: str, max_retries: int = 3) -> dict:
    from langchain_mistralai import ChatMistralAI

    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        return {"error": "MISTRAL_API_KEY manquante"}

    llm = ChatMistralAI(
        model="mistral-small-latest",
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
                if attempt < max_retries - 1: time.sleep(10)
                else: return {"error": error}

    return {"error": "Echec Mistral"}


# ─── SAUVEGARDE SECURISEE ─────────────────────────────────────────────

def save_jobs(jobs: list):
    with open(OUTPUT_JOBS_FILE, "w", encoding="utf-8") as f:
        json.dump(jobs, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())


# ─── DEDUPLICATION ET LIMITE PAR ENTREPRISE ───────────────────────────

def deduplicate_and_limit(jobs_results: list) -> list:
    """
    - Supprime les doublons d'URL
    - Normalise les noms d'entreprise (Devoteam Cyber Trust -> Devoteam)
    - Limite a TARGET_JOBS_PER_COMPANY offres par entreprise
    """
    print("\n  Deduplication et limite par entreprise...")

    company_counts = {}
    final_results  = []
    seen_urls      = set()

    for r in jobs_results:
        url      = r.get("file_path","") or ""
        norm_url = url.split("?")[0].lower()

        # Supprimer doublons URL
        if norm_url in seen_urls:
            continue
        seen_urls.add(norm_url)

        if "error" in r:
            final_results.append(r)
            continue

        # Normaliser le nom d'entreprise
        raw_company      = r.get("company","") or ""
        canonical        = normalize_company_name(raw_company)
        r["company"]     = canonical

        # Appliquer la limite
        count = company_counts.get(canonical, 0)
        if count >= TARGET_JOBS_PER_COMPANY:
            continue

        company_counts[canonical] = count + 1
        final_results.append(r)

    # Rapport
    print(f"  Avant : {len(jobs_results)} | Apres : {len([r for r in final_results if 'error' not in r])}")
    for co, cnt in sorted(company_counts.items(), key=lambda x: -x[1]):
        print(f"    {co:<35} : {cnt} offres")

    return final_results


# ─── SCRAPING PRINCIPAL ───────────────────────────────────────────────

def run_jobs_scraping():
    print("=" * 60)
    print("SCRAPING OFFRES D'EMPLOI — AUTO COLLECTE URLs")
    print(f"Cible : {TARGET_JOBS_PER_COMPANY} offres par entreprise")
    print("=" * 60)

    # Charger les offres deja scrapees
    jobs_results   = []
    processed_urls = set()

    if OUTPUT_JOBS_FILE.exists():
        with open(OUTPUT_JOBS_FILE, encoding="utf-8") as f:
            jobs_results = json.load(f)
        for r in jobs_results:
            fp = r.get("file_path","") or ""
            if fp: processed_urls.add(fp.split("?")[0].lower())
        valid = len([r for r in jobs_results if "error" not in r])
        print(f"Offres deja scrapees : {valid} valides ({len(processed_urls)} total)")

    success = sum(1 for r in jobs_results if "error" not in r)
    errors  = sum(1 for r in jobs_results if "error" in r)

    driver = None
    try:
        driver = setup_driver()
        if not linkedin_login(driver):
            print("Connexion impossible."); return

        # ETAPE 1 : Collecte automatique des URLs
        print(f"\n{'-'*60}")
        print("ETAPE 1 — COLLECTE AUTOMATIQUE DES URLs")
        print(f"{'-'*60}")

        saved_urls = []
        if URLS_FILE.exists():
            with open(URLS_FILE, encoding="utf-8") as f:
                saved = json.load(f)
            saved_urls = [u for u in saved
                          if isinstance(u, str) and "/jobs/view/" in u]
            print(f"URLs deja collectees : {len(saved_urls)}")

        all_new_urls = []
        for company_name, company_url in COMPANY_JOBS_URLS:
            urls = collect_job_urls_from_company(
                driver, company_url, company_name, TARGET_JOBS_PER_COMPANY
            )
            all_new_urls.extend(urls)

        all_urls_set = set(saved_urls) | set(all_new_urls)
        all_job_urls = list(all_urls_set)

        with open(URLS_FILE, "w", encoding="utf-8") as f:
            json.dump(all_job_urls, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())

        print(f"\n  Total URLs collectees : {len(all_job_urls)}")
        print(f"  Sauvegardees dans : {URLS_FILE}")

        # ETAPE 2 : Scraping du contenu des offres
        remaining = [u for u in all_job_urls
                     if u.split("?")[0].lower() not in processed_urls]

        print(f"\n{'-'*60}")
        print("ETAPE 2 — SCRAPING DU CONTENU DES OFFRES")
        print(f"  Offres a scraper : {len(remaining)}")
        print(f"{'-'*60}")

        if not remaining:
            print("Toutes les offres sont deja traitees.")
        else:
            for i, url in enumerate(remaining):
                print(f"\n[{i+1}/{len(remaining)}] {url}")

                norm = url.split("?")[0].lower()
                if norm in processed_urls:
                    print("  Deja traite — skip")
                    continue

                print("  Scraping du contenu...")
                text = scrape_job_page(driver, url)

                if not text or len(text.strip()) < 100:
                    print("  Contenu insuffisant — offre expiree ou privee")
                    errors += 1
                    jobs_results.append({"error":"insufficient_content","file_path":url})
                    processed_urls.add(norm)
                    save_jobs(jobs_results)
                    continue

                print(f"  Texte : {len(text)} caracteres")
                print("  Structuration avec Mistral Small...")

                result = structure_with_mistral(text, url)

                if "error" in result:
                    errors += 1
                    print(f"  Erreur : {result['error'][:80]}")
                    result["file_path"] = url
                    jobs_results.append(result)
                else:
                    result["file_path"]         = url
                    result["extraction_method"] = "selenium + mistral-small"
                    result["extracted_at"]      = datetime.now().isoformat()
                    jobs_results.append(result)
                    success += 1
                    processed_urls.add(norm)
                    print(f"  OK {result.get('title','?')} — "
                          f"{result.get('company','?')} — "
                          f"{result.get('experience_level','?')} — "
                          f"{result.get('years_of_experience_required',0)} ans")

                save_jobs(jobs_results)

                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"  Pause {delay:.1f}s...")
                time.sleep(delay)

        # ETAPE 3 : Deduplication et limite par entreprise
        print(f"\n{'-'*60}")
        print("ETAPE 3 — DEDUPLICATION ET LIMITE PAR ENTREPRISE")
        print(f"{'-'*60}")

        jobs_results = deduplicate_and_limit(jobs_results)
        save_jobs(jobs_results)

        # RESUME FINAL
        valid = [r for r in jobs_results if "error" not in r]
        print(f"\n{'='*60}")
        print("SCRAPING TERMINE")
        print(f"{'='*60}")
        print(f"  Succes  : {len(valid)}")
        print(f"  Erreurs : {len([r for r in jobs_results if 'error' in r])}")
        print(f"  Fichier : {OUTPUT_JOBS_FILE}")
        print(f"\n  Prochaine etape :")
        print(f"    python weaviate/setup_weaviate.py")
        print(f"    python weaviate/insert_data.py")

    except KeyboardInterrupt:
        print("\nArret manuel — donnees sauvegardees.")
    except Exception as e:
        print(f"\nErreur : {e}")
        import traceback; traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
            print("Navigateur ferme.")
        save_jobs(jobs_results)
        print("Fichier sauvegarde.")


if __name__ == "__main__":
    run_jobs_scraping()