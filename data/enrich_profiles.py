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
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup

load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

INPUT_FILE  = Path("data/real_data/extracted_cvs_real.json")
OUTPUT_FILE = Path("data/real_data/extracted_cvs_real_enriched.json")  # nouveau fichier
BACKUP_FILE = Path("data/real_data/extracted_cvs_real_backup.json")


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

REGLES ABSOLUES — NE JAMAIS VIOLER :

1. IDENTIFICATION DU CANDIDAT
   - Le profil appartient UNIQUEMENT à la personne dont le nom apparaît en premier
   - N'extrais QUE les informations de cette personne
   - Ignore toutes les recommandations, suggestions et profils similaires en bas de page
   - Si tu vois d'autres noms dans la page → ce sont des contacts, ignore-les

2. YEARS_OF_EXPERIENCE — TRAVAIL UNIQUEMENT, PAS LES STAGES
   - Compte UNIQUEMENT les postes de type CDI, CDD, Freelance, Temps plein, Part-time
   - NE PAS compter : Stage, Internship, Stage de fin d'études, Alternance
   - Calcule en années décimales depuis les dates réelles
     Ex : "mars 2022 - mars 2024" = 2.0 ans
     Ex : "juil. 2023 - aujourd'hui (avril 2026)" = 2.75 ans
   - Si TOUS les postes sont des stages → years_of_experience = 0
   - Arrondis à 1 décimale

3. EXPERIENCE_TIMELINE — TOUS LES POSTES SANS EXCEPTION
   - Inclure TOUS les postes : stages ET travail
   - Chaque poste = une entrée séparée dans l'ordre chronologique
   - Mettre dans description : le type "Stage" ou "Emploi" ou "Alternance"
   - Dates : extraire les années exactes depuis le texte
     "aujourd'hui" ou "present" → 2026
   - year_start et year_end doivent être des entiers (jamais null)
   - Si durée en mois seulement (ex: "2 mois depuis juil. 2025") → year_end = year_start ou +1
   - skills_acquired : compétences mentionnées dans la description de CE poste uniquement

4. TECHNICAL_SKILLS ET PROGRAMMING_LANGUAGES
   - Extraire TOUTES les compétences mentionnées dans la section Compétences, les postes, les projets
   - programming_languages : Python, Java, SQL, JavaScript, R, TypeScript, Scala, etc.
   - technical_skills : Power BI, Docker, FastAPI, scikit-learn, TensorFlow, Streamlit, etc.
   - Ne pas dupliquer entre les deux listes

5. PROJETS
   - Extraire TOUS les projets de la section Projets
   - Format : "Nom du projet : description courte. Technologies : X, Y, Z"
   - Séparer par " | "

6. CERTIFICATIONS
   - Titre COMPLET + organisme
   - Ex : "Scrum Master Certified — Udemy", "Time Management — Udemy"
   - Ignorer les organismes sans titre de certification

7. EMAIL ET PHONE
   - Chercher dans tout le texte
   - Si vraiment absent → "not found"

8. CAREER_TRAJECTORY — BASÉ SUR LES VRAIS POSTES
   - Analyser la progression réelle de experience_timeline
   - Si 0 postes → direction: "Début de carrière", predicted_profile: "Junior en formation"
   - Si 1 poste → direction: basé sur ce poste, predicted_profile: évolution logique
   - Si 2+ postes → analyser la vraie évolution des rôles et compétences

9. PARSING_CONFIDENCE
   - 0.90 si profil très complet (2+ postes avec dates + skills + formation + projets)
   - 0.75 si profil complet (1+ poste + skills ou formation)
   - 0.50 si profil partiel
   - 0.30 si profil minimal
"""


def is_incomplete(profile: dict) -> bool:
    """
    Retourne True si le profil manque d'informations importantes
    """
    if "error" in profile:
        return True

    checks = [
        profile.get("years_of_experience", 0) == 0,
        len(profile.get("experience_timeline", [])) == 0,
        profile.get("work_experience", "not found") in ["not found", "", None],
        profile.get("parsing_confidence", 0) < 0.65,
        len(profile.get("technical_skills", [])) == 0,
        len(profile.get("programming_languages", [])) == 0,
    ]

    # Incomplet si au moins 2 criteres sont vrais
    return sum(checks) >= 2


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
    if connected:
        print("Connecte avec succes.")
    else:
        print("Connexion echouee.")
    return connected


def extract_profile_sections(driver, url: str) -> str:
    """
    Extrait le profil en plusieurs passes :
    1. Charge la page
    2. Deplie toutes les sections
    3. Extrait chaque section separement
    4. Concatene le tout pour Mistral
    """
    print(f"  Chargement : {url.split('/in/')[-1]}")
    driver.get(url)
    time.sleep(5)

    # Scroll initial complet
    for step in range(0, 5000, 600):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.4)
    time.sleep(2)

    # Cliquer sur TOUS les boutons d'expansion
    expansion_xpaths = [
        # Boutons "Voir plus" texte
        "//button[contains(., 'Voir plus')]",
        "//button[contains(., 'voir plus')]",
        "//button[contains(., 'Show more')]",
        "//button[contains(., 'Afficher plus')]",
        # Boutons de section specifiques LinkedIn
        "//button[contains(@class, 'pvs-list__footer-wrapper')]",
        "//button[contains(@class, 'inline-show-more-text__button')]",
        "//button[contains(@class, 'scaffold-finite-scroll__load-button')]",
        # Liens "Tout afficher"
        "//a[contains(., 'Tout afficher')]",
        "//a[contains(., 'See all')]",
        # Boutons par aria-label
        "//button[contains(@aria-label, 'expérience')]",
        "//button[contains(@aria-label, 'compétence')]",
        "//button[contains(@aria-label, 'formation')]",
        "//button[contains(@aria-label, 'projet')]",
    ]

    clicked = 0
    for xpath in expansion_xpaths:
        try:
            elements = driver.find_elements(By.XPATH, xpath)
            for el in elements:
                try:
                    if el.is_displayed() and el.is_enabled():
                        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", el)
                        time.sleep(0.3)
                        driver.execute_script("arguments[0].click();", el)
                        time.sleep(1)
                        clicked += 1
                except Exception:
                    continue
        except Exception:
            continue

    if clicked > 0:
        print(f"  {clicked} bouton(s) d'expansion clique(s)")
        time.sleep(2)

    # Scroll final apres expansion
    for step in range(0, 8000, 400):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.3)
    time.sleep(2)

    # Extraction via BeautifulSoup avec sections separees
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Supprimer les elements de navigation et publicite
    for tag in soup(["script", "style", "nav", "footer",
                     "header", "meta", "noscript", "svg",
                     "aside", "iframe"]):
        tag.decompose()

    # Extraire le texte complet
    full_text = soup.get_text(separator="\n")

    # Nettoyage des lignes
    lines = full_text.splitlines()
    clean_lines = []
    for line in lines:
        line = line.strip()
        if len(line) < 2:
            continue
        # Ignorer les lignes de navigation LinkedIn
        nav_keywords = [
            "S'inscrire", "Se connecter", "Passer à Premium",
            "Connexion", "Sign in", "Join now",
            "Mes offres d'emploi", "Mon réseau",
            "Abonnement Premium", "Essayer Premium",
        ]
        if any(kw.lower() == line.lower() for kw in nav_keywords):
            continue
        clean_lines.append(line)

    # Supprimer les doublons consecutifs
    deduped = []
    prev = None
    for line in clean_lines:
        if line != prev:
            deduped.append(line)
        prev = line

    clean_text = "\n".join(deduped)

    # Ajouter l'URL en tete pour que Mistral sache quel profil c'est
    result = f"URL DU PROFIL : {url}\n\n"
    result += clean_text

    # Augmenter la limite a 12000 caracteres pour capturer plus de contenu
    return result[:12000]


def structure_with_mistral(profile_text: str, candidate_name: str,
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

    # Ajouter le nom du candidat dans le prompt pour eviter les confusions
    prompt_with_context = (
        PROMPT +
        f"\n\nNOM DU CANDIDAT ATTENDU : {candidate_name}\n"
        f"N'extrais QUE les informations de cette personne.\n\n"
        f"CONTENU DU PROFIL :\n{profile_text}"
    )

    for attempt in range(max_retries):
        try:
            result = structured_llm.invoke(prompt_with_context)
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


def merge_profiles(old: dict, new: dict) -> dict:
    """
    Fusionne l'ancien et le nouveau profil
    Garde le meilleur de chaque champ
    Priorité au nouveau sauf si vide
    """
    if "error" in new:
        return old

    merged = dict(old)  # partir de l'ancien

    def is_empty(val):
        if val is None: return True
        if val == "not found": return True
        if val == "": return True
        if isinstance(val, list) and len(val) == 0: return True
        if isinstance(val, dict) and not val: return True
        if isinstance(val, (int, float)) and val == 0: return True
        return False

    # Pour chaque champ du nouveau profil
    fields = [
        "full_name", "email", "phone", "location", "years_of_experience",
        "linkedin", "github", "roles_held", "programming_languages",
        "technical_skills", "spoken_languages", "certifications",
        "seniority_technologies", "seniority_programming_languages",
        "industry", "summary", "education_level", "field_of_studies",
        "work_experience", "projects", "experience_timeline", "career_trajectory",
        "parsing_confidence"
    ]

    for field in fields:
        new_val = new.get(field)
        old_val = old.get(field)

        if not is_empty(new_val):
            # Le nouveau a une valeur — comparer avec l'ancien
            if is_empty(old_val):
                # L'ancien est vide — prendre le nouveau
                merged[field] = new_val
            elif isinstance(new_val, list) and len(new_val) > len(old_val or []):
                # Le nouveau a plus d'elements — prendre le nouveau
                merged[field] = new_val
            elif field == "years_of_experience" and new_val > (old_val or 0):
                # Le nouveau a plus d'annees — prendre le nouveau
                merged[field] = new_val
            elif field == "parsing_confidence" and new_val > (old_val or 0):
                merged[field] = new_val
            elif field in ["experience_timeline", "career_trajectory",
                           "work_experience", "projects", "summary"]:
                # Toujours preferer le nouveau pour ces champs narratifs
                merged[field] = new_val

    # Conserver les metadonnees de l'original
    merged["file_path"]         = old.get("file_path", new.get("file_path", ""))
    merged["extraction_method"] = "selenium + mistral-large (enriched)"
    merged["extracted_at"]      = datetime.now().isoformat()

    return merged

def run_enrichment():
    print("=" * 60)
    print("ENRICHISSEMENT DES PROFILS INCOMPLETS")
    print("=" * 60)

    # Charger le fichier existant
    if not INPUT_FILE.exists():
        print(f"Fichier introuvable : {INPUT_FILE}")
        return

    with open(INPUT_FILE, encoding="utf-8") as f:
        profiles = json.load(f)

    print(f"Profils charges : {len(profiles)}")

    # Backup
    with open(BACKUP_FILE, "w", encoding="utf-8") as f:
        json.dump(profiles, f, ensure_ascii=False, indent=2)
    print(f"Backup cree : {BACKUP_FILE}")

    # Identifier les profils incomplets
    to_enrich = []
    for i, p in enumerate(profiles):
        if is_incomplete(p):
            url  = p.get("file_path", p.get("linkedin", ""))
            name = p.get("full_name", "Inconnu")
            if url and "linkedin.com/in/" in url:
                to_enrich.append((i, url, name))
                print(f"  A enrichir [{i}] : {name} — confiance {p.get('parsing_confidence', 0):.2f}")

    if not to_enrich:
        print("\nTous les profils sont complets. Rien a enrichir.")
        return

    print(f"\n{len(to_enrich)} profil(s) a enrichir")
    print("=" * 60)

    # Connexion Selenium
    driver = None
    try:
        driver = setup_driver()
        if not linkedin_login(driver):
            print("Connexion impossible.")
            return

        enriched_count = 0
        failed_count   = 0

        for idx, (profile_idx, url, name) in enumerate(to_enrich):
            print(f"\n[{idx+1}/{len(to_enrich)}] {name}")
            print(f"  URL : {url}")

            # Extraction du texte du profil
            text = extract_profile_sections(driver, url)

            if not text or len(text.strip()) < 200:
                print("  Contenu insuffisant — profil prive ou inaccessible.")
                failed_count += 1
                continue

            print(f"  Texte extrait : {len(text)} caracteres")
            print(f"  Structuration avec Mistral Large...")

            # Structuration avec Mistral
            new_data = structure_with_mistral(text, name)

            if "error" in new_data:
                print(f"  Erreur Mistral : {new_data['error'][:80]}")
                failed_count += 1
                continue

            # Fusion avec l'ancien profil
            old_data      = profiles[profile_idx]
            merged        = merge_profiles(old_data, new_data)
            profiles[profile_idx] = merged

            enriched_count += 1
            print(f"  OK — {merged.get('full_name')} | "
                  f"{merged.get('years_of_experience', 0)} ans | "
                  f"confiance : {merged.get('parsing_confidence', 0):.2f} | "
                  f"{len(merged.get('experience_timeline', []))} poste(s)")

            # Sauvegarde apres chaque profil enrichi
            with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
                json.dump(profiles, f, ensure_ascii=False, indent=2)

            delay = random.uniform(5, 9)
            print(f"  Pause {delay:.1f}s...")
            time.sleep(delay)

        print("\n" + "=" * 60)
        print("ENRICHISSEMENT TERMINE")
        print("=" * 60)
        print(f"  Enrichis  : {enriched_count}")
        print(f"  Echecs    : {failed_count}")
        print(f"  Fichier   : {OUTPUT_FILE}")
        print(f"  Backup    : {BACKUP_FILE}")

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
        # Sauvegarde finale
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(profiles, f, ensure_ascii=False, indent=2)
        print("Fichier sauvegarde.")


if __name__ == "__main__":
    run_enrichment()