"""
linkedin_scraper.py
Scraping des profils LinkedIn des employes GO & DEV

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
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")


GODEV_COMPANY_URL = "https://www.linkedin.com/company/goanddev/people/"
OUTPUT_DIR        = Path("data/real_data")
OUTPUT_PROFILES   = OUTPUT_DIR / "godev_profiles_raw.json"
OUTPUT_CVS_FORMAT = OUTPUT_DIR / "extracted_cvs_real.json"
OUTPUT_URLS       = OUTPUT_DIR / "godev_linkedin_urls.json"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

MIN_DELAY = 4
MAX_DELAY = 8



JSON_SCHEMA = {
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
                    "level": {"type": "string",
                              "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
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
                    "level": {"type": "string",
                              "enum": ["Junior", "Medior", "Confirmé", "Senior", "Expert"]}
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
        "summary":            {"type": "string"},
        "education_level": {
            "type": "string",
            "enum": ["Bac + 2", "Bac + 3", "Bac + 4", "Bac + 5", "Bac + 6 et plus"]
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
                    "skills_acquired": {"type": "array", "items": {"type": "string"}},
                    "languages_used":  {"type": "array", "items": {"type": "string"}},
                    "description":     {"type": "string"}
                },
                "required": ["year_start", "year_end", "company", "role"]
            }
        },
        "career_trajectory": {
            "type": "object",
            "properties": {
                "direction":          {"type": "string"},
                "progression_speed":  {"type": "string",
                                       "enum": ["Rapide", "Normale", "Lente"]},
                "predicted_profile":  {"type": "string"},
                "skills_in_progress": {"type": "array", "items": {"type": "string"}}
            },
            "required": ["direction", "progression_speed", "predicted_profile"]
        }
    },
    "required": [
        "full_name", "roles_held",
        "programming_languages", "technical_skills",
        "industry", "summary",
        "experience_timeline", "career_trajectory"
    ]
}

PROMPT = """Tu reçois le contenu texte d'une page profil LinkedIn.
Structure ces informations en JSON.

REGLES ABSOLUES :
1. Utilise UNIQUEMENT les informations explicitement présentes dans le texte
2. N'invente RIEN — si absent : "not found" ou [] ou 0
3. email / phone : rarement présents sur profil public → "not found"
4. experience_timeline : liste CHAQUE poste séparément dans l'ordre chronologique
5. career_trajectory : SEULEMENT si 2+ postes réels présents
   - Moins de 2 postes → direction: "Données insuffisantes"
                          predicted_profile: "Données insuffisantes"
                          skills_in_progress: []
6. programming_languages : langages de code uniquement (Python, Java, SQL...)
7. technical_skills : outils, frameworks, plateformes (Docker, AWS, Kafka...)
8. years_of_experience : calcule depuis les dates des expériences
9. parsing_confidence :
   - 0.85 si profil complet (expériences + skills + formation)
   - 0.60 si profil partiel
   - 0.30 si profil minimal (nom + titre seulement)
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

    # Email
    for selector in ["#username", "input[name='session_key']", "input[type='text']"]:
        try:
            field = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector))
            )
            field.clear()
            field.send_keys(email)
            print(f"  Email saisi via : {selector}")
            break
        except Exception:
            continue

    time.sleep(1)

    # Mot de passe
    for selector in ["#password", "input[name='session_password']", "input[type='password']"]:
        try:
            field = driver.find_element(By.CSS_SELECTOR, selector)
            field.clear()
            field.send_keys(password)
            break
        except Exception:
            continue

    time.sleep(1)

    # Bouton connexion
    for selector in ["button[type='submit']", "button.btn__primary--large"]:
        try:
            driver.find_element(By.CSS_SELECTOR, selector).click()
            break
        except Exception:
            continue

    time.sleep(5)

    # Verification manuelle si necessaire
    if "checkpoint" in driver.current_url:
        print("LinkedIn demande une verification manuelle.")
        print("Completez la verification dans le navigateur, puis appuyez sur Entree.")
        input()
        driver.get("https://www.linkedin.com/feed/")
        time.sleep(3)

    if "feed" in driver.current_url or "linkedin.com/in" in driver.current_url:
        print("Connecte avec succes.")
        return True

    print("Connexion echouee — verifiez les identifiants.")
    return False


def get_profile_urls(driver) -> list:
    print(f"\nAcces a la page employes : {GODEV_COMPANY_URL}")
    driver.get(GODEV_COMPANY_URL)
    time.sleep(5)

    profile_urls = set()
    no_change    = 0
    last_count   = 0

    for scroll in range(50):  # plus de scrolls
        # Scroll progressif
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        # Extraire toutes les URLs /in/ visibles
        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/in/')]")
        for link in links:
            href = link.get_attribute("href")
            if href and "/in/" in href:
                clean = href.split("?")[0].rstrip("/")
                # Exclure les URLs qui ne sont pas des profils individuels
                if clean.count("/") >= 4:
                    profile_urls.add(clean)

        if len(profile_urls) == last_count:
            no_change += 1

            # Essayer plusieurs variantes du bouton "Voir plus"
            clicked = False
            voir_plus_selectors = [
                "//button[contains(., 'Voir plus')]",
                "//button[contains(., 'Show more')]",
                "//button[contains(., 'Afficher plus')]",
                "//button[contains(@class, 'scaffold-finite-scroll__load-button')]",
                "//button[contains(@aria-label, 'Voir plus')]",
            ]
            for selector in voir_plus_selectors:
                try:
                    btn = driver.find_element(By.XPATH, selector)
                    driver.execute_script("arguments[0].scrollIntoView();", btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(3)
                    clicked = True
                    no_change = 0
                    print(f"  Bouton 'Voir plus' clique")
                    break
                except Exception:
                    continue

            if not clicked and no_change >= 5:
                print(f"  Fin de la page apres {scroll} scrolls")
                break
        else:
            no_change = 0
            print(f"  Scroll {scroll+1} : {len(profile_urls)} profils")

        last_count = len(profile_urls)

    urls = list(profile_urls)
    print(f"  Total : {len(urls)} URLs de profils trouvees")

    # Sauvegarder les URLs trouvees
    with open(OUTPUT_URLS if hasattr(OUTPUT_URLS, 'exists') else Path("data/real_data/godev_linkedin_urls.json"),
              "w", encoding="utf-8") as f:
        json.dump(urls, f, ensure_ascii=False, indent=2)

    return urls


def extract_visible_text(driver, url: str) -> str:
    """
    Extrait tout le texte visible de la page profil
    Methode robuste — independante des selecteurs CSS de LinkedIn
    Laisse Mistral structurer le texte
    """
    print(f"  Chargement : {url.split('/in/')[-1]}")
    driver.get(url)
    time.sleep(4)

    # Scroll progressif pour charger le contenu lazy
    total_height = driver.execute_script("return document.body.scrollHeight")
    for step in range(0, min(total_height, 5000), 500):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.4)
    driver.execute_script("window.scrollTo(0, 0)")
    time.sleep(1)

    # Extraction via BeautifulSoup — propre et robuste
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Supprimer les elements inutiles
    for tag in soup(["script", "style", "nav", "footer",
                     "header", "meta", "noscript", "svg", "button"]):
        tag.decompose()

    # Extraire le texte visible
    text = soup.get_text(separator="\n")

    # Nettoyage
    lines = [line.strip() for line in text.splitlines() if line.strip()]
    lines = [line for line in lines if len(line) > 2]
    text  = "\n".join(lines)

    # Ajouter l'URL pour que Mistral sache de quel profil il s'agit
    text = f"URL DU PROFIL : {url}\n\n" + text

    # Limiter pour Mistral (max 8000 caracteres)
    return text[:8000]


def structure_with_mistral(profile_text: str, max_retries: int = 3) -> dict:
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
            result = structured_llm.invoke(PROMPT + "\n\nCONTENU DU PROFIL :\n" + profile_text)
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

    return {"error": "Echec Mistral apres tous les essais"}


def run_scraping(max_profiles: int = 30):
    print("=" * 60)
    print("SCRAPING LINKEDIN - GO & DEV")
    print("=" * 60)

    driver = None

    try:
        driver = setup_driver()

        if not linkedin_login(driver):
            print("Connexion impossible.")
            return

        # Recuperer les URLs — recharge depuis fichier si deja collectees
        if OUTPUT_URLS.exists():
            with open(OUTPUT_URLS, encoding="utf-8") as f:
                existing_urls = json.load(f)
            if existing_urls:
                print(f"URLs existantes chargees : {len(existing_urls)} URLs")
                profile_urls = existing_urls
            else:
                profile_urls = get_profile_urls(driver)
        else:
            profile_urls = get_profile_urls(driver)

        if not profile_urls:
            print("Aucun profil trouve.")
            return

        profile_urls = profile_urls[:max_profiles]

        # Reprise depuis fichier existant — deduplication robuste
        cv_results     = []
        processed_urls = set()

        if Path(OUTPUT_CVS_FORMAT).exists():
            with open(OUTPUT_CVS_FORMAT, encoding="utf-8") as f:
                cv_results = json.load(f)
            for r in cv_results:
                fp = r.get("file_path", "")
                if fp:
                    # Normaliser l'URL pour eviter les doublons de format
                    normalized = fp.split("?")[0].rstrip("/").lower()
                    processed_urls.add(normalized)
            print(f"Reprise : {len(processed_urls)} profils deja traites.")

        # Filtrer les URLs deja traitees (comparaison normalisee)
        remaining = []
        for u in profile_urls:
            normalized = u.split("?")[0].rstrip("/").lower()
            if normalized not in processed_urls:
                remaining.append(u)
        success   = sum(1 for r in cv_results if "error" not in r)
        errors    = sum(1 for r in cv_results if "error" in r)

        print(f"\n{len(remaining)} profils a traiter")
        print("=" * 60)

        for i, url in enumerate(remaining):
            print(f"\n[{i+1}/{len(remaining)}] {url.split('/in/')[-1]}")

            # Extraction texte visible
            text = extract_visible_text(driver, url)

            if not text or len(text.strip()) < 150:
                print("  Contenu insuffisant — profil prive.")
                errors += 1
                cv_results.append({"error": "profil_prive", "file_path": url})

            else:
                print(f"  Texte extrait : {len(text)} caracteres")
                print("  Structuration avec Mistral Large...")

                result = structure_with_mistral(text)

                if "error" in result:
                    errors += 1
                    print(f"  Erreur : {result['error'][:80]}")
                    cv_results.append({"error": result["error"], "file_path": url})
                else:
                    result["file_path"]         = url
                    result["extraction_method"] = "selenium + mistral-large"
                    result["extracted_at"]      = datetime.now().isoformat()
                    cv_results.append(result)
                    success += 1
                    print(f"  OK — {result.get('full_name', 'N/A')} | "
                          f"{result.get('years_of_experience', 0)} ans | "
                          f"confiance : {result.get('parsing_confidence', 0):.2f}")

            # Sauvegarde incrementale
            with open(OUTPUT_CVS_FORMAT, "w", encoding="utf-8") as f:
                json.dump(cv_results, f, ensure_ascii=False, indent=2)

            delay = random.uniform(MIN_DELAY, MAX_DELAY)
            print(f"  Pause {delay:.1f}s...")
            time.sleep(delay)

        print("\n" + "=" * 60)
        print("SCRAPING TERMINE")
        print("=" * 60)
        print(f"  Succes    : {success}")
        print(f"  Erreurs   : {errors}")
        print(f"  Fichier   : {OUTPUT_CVS_FORMAT}")
        print(f"\n  Prochaine etape :")
        print(f"    Modifier CV_JSON_PATH dans insert_data.py :")
        print(f"    CV_JSON_PATH = BASE_DIR / 'data' / 'real_data' / 'extracted_cvs_real.json'")

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
            print("Navigateur ferme.")


if __name__ == "__main__":
    run_scraping(max_profiles=50)