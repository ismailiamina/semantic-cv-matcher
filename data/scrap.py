"""
linkedin_scraper_v3.py
=======================
Scraping LinkedIn SEMI-AUTOMATIQUE — Alten + Devoteam + SQLI

- Cible : 20 profils IT valides par entreprise
- Le compteur n'avance QUE si le profil est validé (pas skippé)
- Pause de validation tous les 10 profils validés
- Fichier séparé par entreprise dans data/real_data/new_candidates/
"""

import json
import time
import random
import os
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

# ─── CONFIGURATION ────────────────────────────────────────────────────
COMPANY_URLS = [
    ("alten",           "https://www.linkedin.com/company/alten/people/"),
    ("devoteam",        "https://www.linkedin.com/company/devoteam/people/"),
    ("sqli",            "https://www.linkedin.com/company/sqli/people/"),
    ("oracle",          "https://www.linkedin.com/company/oracle/people/"),
    ("atos",            "https://www.linkedin.com/company/atos/people/"),
    ("matious",         "https://www.linkedin.com/company/matious/people/"),
    ("inetum",          "https://www.linkedin.com/company/inetum/people/"),
    ("capgemini",       "https://www.linkedin.com/company/capgemini/people/"),
    ("GO & DEV",       "https://www.linkedin.com/company/goanddev/people/"),
    ("DXC Technology Morocco", "https://www.linkedin.com/company/dxcmorocco/people/"),
]
TARGET_PER_COMPANY = 20   # profils IT valides souhaités par entreprise
PAUSE_EVERY        = 10   # pause de validation tous les N profils validés

OUTPUT_DIR    = Path("data/real_data")
NEW_CANDS_DIR = OUTPUT_DIR / "new_candidates"
ENRICHED_FILE = OUTPUT_DIR / "extracted_cvs_real_enriched.json"
URLS_DIR      = OUTPUT_DIR / "urls"

NEW_CANDS_DIR.mkdir(parents=True, exist_ok=True)
URLS_DIR.mkdir(parents=True, exist_ok=True)

MIN_DELAY    = 3
MAX_DELAY    = 5
CURRENT_YEAR = datetime.now().year

STAGE_KEYWORDS = [
    "stage", "intern", "stagiaire", "alternance",
    "apprenti", "pfe", "pfa", "internship"
]


# ─── SCHÉMA JSON ──────────────────────────────────────────────────────

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


# ─── PROMPT ───────────────────────────────────────────────────────────

PROMPT = f"""Tu reçois le contenu texte COMPLET d'un profil LinkedIn organisé par sections.
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
Si une personne ne contient pas de competeneces techniques ou de langage de programmation écrit null.
RÈGLE 2 — ANTI-OUBLI (CRITIQUE) :
Avant de retourner le JSON, effectue ces vérifications :
  ✓ Relis la section COMPÉTENCES du texte → chaque compétence listée doit être dans technical_skills ou programming_languages
  ✓ Relis la section EXPÉRIENCES du texte → chaque entreprise et poste doit être dans experience_timeline
  ✓ Relis la section CERTIFICATIONS → chaque certification doit être dans certifications
  ✓ Relis la section FORMATIONS → l'école et le diplôme doivent être dans education_level et field_of_studies
  ✓ Relis la section PROJETS → les projets doivent être dans projects
Si tu trouves quelque chose de manquant → ajoute-le AVANT de retourner le JSON.

RÈGLE 3 — EXPERIENCE_TIMELINE (CRITIQUE) :
Extrais CHAQUE poste comme une entrée séparée avec year_start ET year_end OBLIGATOIRES.

Interprétation des dates LinkedIn :
  "janv. 2018 - Présent"         → year_start=2018, year_end={CURRENT_YEAR}
  "mars 2020 - août 2022"        → year_start=2020, year_end=2022
  "2019 - aujourd'hui"           → year_start=2019, year_end={CURRENT_YEAR}
  "juil. 2021 · 1 an 3 mois"    → year_start=2021, year_end=2022
  "Présent" / "aujourd'hui"      → year_end={CURRENT_YEAR}

RÈGLE 4 — YEARS_OF_EXPERIENCE & CLASSIFICATION STAGES :

Un poste est un STAGE uniquement si l'un de ces labels apparaît EXPLICITEMENT :
  "Internship", "Stage", "Stagiaire", "Alternance", "Apprentissage", "PFE", "PFA"

Un poste est RÉEL même si :
  ✗ Sa durée est courte (< 6 mois)
  ✗ C'est une première expérience
  ✗ C'est chez une petite structure
  ✗ Le titre semble junior

INTERDIT ABSOLU :
  ✗ Exclure un poste basé sur sa durée
  ✗ Supposer qu'un poste est un stage parce qu'il est court
  ✗ Exclure "Mobile Application Developer" parce que 5 mois

Calcul years_of_experience :
  → Prend la date de début du poste RÉEL le plus ancien (hors stages)
  → years_of_experience = CURRENT_YEAR - year_start_du_premier_poste_reel
  → Ne pas additionner les durées, ne pas déduire les chevauchements

RÈGLE 5 — SÉNIORITÉ :
  0-2 ans → Junior | 2-4 ans → Medior | 4-6 ans → Confirmé
  6-10 ans → Senior | 10+ ans → Expert
  3 premières technologies = niveau global.
  Autres = un niveau en dessous.
  JAMAIS tous à Junior si years_of_experience > 2.
  Si un langage apparaît dans une seule expérience courte (ex: stage 5 mois),
lui attribuer le niveau "Junior" par défaut, sauf contexte contraire.

RÈGLE 6 — DOUBLONS :
Chaque skill / langage / certification / poste : UNE SEULE FOIS.

RÈGLE 7 — PROGRAMMING_LANGUAGES vs TECHNICAL_SKILLS :
  programming_languages = langages de code UNIQUEMENT écrits dans le texte :
    Python, Java, JavaScript, TypeScript, SQL, C, C++, C#, Go, Ruby, PHP, Kotlin, Swift, R...
  technical_skills = outils, frameworks, plateformes écrits dans le texte :
    Docker, Kubernetes, AWS, Azure, Spring, React, Angular, Selenium, Git, Jenkins, JIRA...
  JAMAIS ajouter un outil ou langage non mentionné explicitement.
Pour le champ "programming_languages", tu dois :
- Scanner TOUTES les sections : résumé, expériences, compétences, projets
- Inclure tous les langages explicitement mentionnés, même une seule fois
- Distinguer C# et C++ comme deux entrées séparées
- Ne pas ignorer les langages trouvés uniquement dans experience_timeline

RÈGLE 8 — PARSING_CONFIDENCE :
  0.85 = profil complet (toutes sections présentes avec détails)
  0.60 = profil partiel (certaines sections manquantes ou sans dates)
  0.30 = profil minimal (nom + titre seulement)

════════════════════════════════════════
EXEMPLES FEW-SHOT
════════════════════════════════════════

EXEMPLE 1 — Dates :
  "Software Engineer · Capgemini · janv. 2016 - Présent
   Développeur · Sopra · mars 2014 - déc. 2015"
  → timeline[0] : year_start=2014, year_end=2015, company="Sopra"
  → timeline[1] : year_start=2016, year_end={CURRENT_YEAR}, company="Capgemini"

EXEMPLE 2 — Anti-invention :
  Texte : "Compétences : Java, Spring, PostgreSQL"
  → programming_languages = ["Java"]            ← seulement ce qui est écrit
  → technical_skills = ["Spring", "PostgreSQL"] ← pas Docker, pas Git, pas Maven
                                                   même s'ils sont courants avec Java

EXEMPLE 3 — Anti-oubli :
  Texte section compétences : "Java · Python · Docker · Kubernetes · AWS · Terraform"
  → programming_languages DOIT contenir : ["Java", "Python"]
  → technical_skills DOIT contenir : ["Docker", "Kubernetes", "AWS", "Terraform"]
  Si l'un manque → c'est une erreur, ajoute-le.

EXEMPLE 4 — Séparation langages/outils :
  "Python, TensorFlow, SQL, Docker, Git"
  → programming_languages = ["Python", "SQL"]
  → technical_skills = ["TensorFlow", "Docker", "Git"]
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

    for selector in ["#username","input[name='session_key']","input[type='text']"]:
        try:
            f = WebDriverWait(driver, 5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
            f.clear(); f.send_keys(email); break
        except Exception: continue

    time.sleep(1)
    for selector in ["#password","input[name='session_password']","input[type='password']"]:
        try:
            f = driver.find_element(By.CSS_SELECTOR, selector)
            f.clear(); f.send_keys(password); break
        except Exception: continue

    time.sleep(1)
    for selector in ["button[type='submit']","button.btn__primary--large"]:
        try: driver.find_element(By.CSS_SELECTOR, selector).click(); break
        except Exception: continue

    time.sleep(5)

    print()
    print("  ┌─────────────────────────────────────────────────────┐")
    print("  │  VÉRIFICATION CONNEXION LINKEDIN                    │")
    print("  │                                                     │")
    print("  │  Si LinkedIn demande une vérification :             │")
    print("  │  → Complète-la dans le navigateur                   │")
    print("  │  → Assure-toi d'être sur la page d'accueil          │")
    print("  │                                                     │")
    print("  │  Appuie sur Entrée quand tu es connecté →           │")
    print("  └─────────────────────────────────────────────────────┘")
    input()

    driver.get("https://www.linkedin.com/feed/")
    time.sleep(3)

    if "feed" in driver.current_url or "mynetwork" in driver.current_url:
        print("  Connecté avec succès."); return True

    print("  Connexion échouée — vérifie les identifiants dans .env")
    return False


# ─── COLLECTE URLS ────────────────────────────────────────────────────

def get_profile_urls_from_company(driver, company_url: str, company_key: str) -> list:
    urls_file = URLS_DIR / f"{company_key}_urls.json"

    if urls_file.exists():
        with open(urls_file, encoding="utf-8") as f:
            existing = json.load(f)
        if existing:
            print(f"  URLs déjà collectées : {len(existing)} pour {company_key}")
            return existing

    print(f"  Collecte URLs : {company_url}")
    driver.get(company_url)
    time.sleep(5)

    profile_urls = set()
    no_change    = 0
    last_count   = 0

    for scroll in range(50):
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)
        links = driver.find_elements(By.XPATH, "//a[contains(@href, '/in/')]")
        for link in links:
            href = link.get_attribute("href")
            if href and "/in/" in href:
                clean = href.split("?")[0].rstrip("/")
                if clean.count("/") >= 4:
                    profile_urls.add(clean)

        if len(profile_urls) == last_count:
            no_change += 1
            clicked = False
            for sel in [
                "//button[contains(., 'Voir plus')]",
                "//button[contains(., 'Show more')]",
                "//button[contains(@class,'scaffold-finite-scroll__load-button')]"
            ]:
                try:
                    btn = driver.find_element(By.XPATH, sel)
                    driver.execute_script("arguments[0].scrollIntoView();", btn)
                    time.sleep(1)
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(3); clicked = True; no_change = 0; break
                except Exception: continue
            if not clicked and no_change >= 5: break
        else:
            no_change = 0
            print(f"  Scroll {scroll+1} : {len(profile_urls)} profils")
        last_count = len(profile_urls)

    urls = list(profile_urls)
    with open(urls_file, "w", encoding="utf-8") as f:
        json.dump(urls, f, ensure_ascii=False, indent=2)
    print(f"  {len(urls)} URLs collectées pour {company_key}")
    return urls


# ─── EXTRACTION TEXTE — SEMI-AUTOMATIQUE ──────────────────────────────

EXCLUDED_LINES = [
    "s'identifier","sign in","voir le profil complet",
    "vous aimerez peut-être","personnes également consultées",
    "suivre","se connecter","message","ignorer",
    "j'accepte","politique de confidentialité",
    "cookie","paramètres","toute l'activité","toute l activite",
    "vous connaissez peut-être","people also viewed",
    "add profile section","open to","more profiles for you"
]


def clean_lines(text: str) -> list:
    """Nettoie le texte et filtre les lignes parasites."""
    lines = [l.strip() for l in text.splitlines() if l.strip() and len(l.strip()) > 2]
    return [l for l in lines
            if not any(e in l.lower() for e in EXCLUDED_LINES)
            and len(l) > 2]


def capture_modal_text(driver) -> str:
    """
    Capture le texte de la modale ouverte uniquement.
    Cherche le div de la modale LinkedIn par ses classes connues.
    Si modale non trouvée, capture la section ciblée par ID.
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Essayer de trouver la modale ouverte
    modal = (
        soup.find("div", {"class": lambda c: c and "artdeco-modal__content" in c})
        or soup.find("div", {"role": "dialog"})
        or soup.find("div", {"class": lambda c: c and "scaffold-layout__detail" in c})
        or soup.find("div", {"class": lambda c: c and "pvs-list" in str(c) and "modal" in str(c)})
    )

    if modal:
        for tag in modal(["script","style","svg","button","nav"]):
            tag.decompose()
        return "\n".join(clean_lines(modal.get_text(separator="\n")))

    # Fallback : capturer toute la page proprement
    for tag in soup(["script","style","nav","footer","header","meta","noscript","svg"]):
        tag.decompose()
    return "\n".join(clean_lines(soup.get_text(separator="\n")))


def capture_section_from_dom(driver, section_id: str) -> str:
    """
    Capture uniquement le contenu d'une section spécifique via son ID LinkedIn.
    Utilisé quand "Tout afficher" n'existe pas — les éléments sont directement visibles.

    IDs LinkedIn : #experience, #education, #skills, #licenses_and_certifications, #projects
    """
    soup = BeautifulSoup(driver.page_source, "html.parser")

    # Chercher la section par son id
    section = soup.find(id=section_id)

    if not section:
        # Chercher le parent de la section via data-generated-suggestion-target
        section = soup.find("section", {"id": section_id})

    if not section:
        # Chercher via aria-labelledby
        heading = soup.find(id=f"{section_id}-title")
        if heading:
            section = heading.find_parent("section")

    if section:
        for tag in section(["script","style","svg","button"]):
            tag.decompose()
        text = "\n".join(clean_lines(section.get_text(separator="\n")))
        return text

    return ""


def extract_text_after_manual_clicks(driver, url: str, valid_count: int) -> str:
    """
    Extraction section par section — INTELLIGENTE :

    Pour chaque section :
    - Si "Tout afficher" EXISTE → tu cliques, modale s'ouvre → script capture la MODALE
    - Si "Tout afficher" N'EXISTE PAS → script capture directement la SECTION depuis le DOM
    → Dans les deux cas, seul le contenu pertinent est capturé, pas toute la page
    """
    print(f"\n  Chargement : {url.split('/in/')[-1]}")
    driver.get(url)
    time.sleep(4)

    # Scroll pour charger le contenu lazy
    total_height = driver.execute_script("return document.body.scrollHeight")
    for step in range(0, min(total_height, 6000), 300):
        driver.execute_script(f"window.scrollTo(0, {step})")
        time.sleep(0.2)
    driver.execute_script("window.scrollTo(0, 0)")
    time.sleep(1)

    all_texts = {}

    # ══════════════════════════════════════════════════════════════
    # SECTION 1 — Informations générales (header profil)
    # Capture uniquement le header — pas toute la page
    # ══════════════════════════════════════════════════════════════
    print()
    print("  ┌─────────────────────────────────────────────────────┐")
    print(f"  │  [{valid_count}/{TARGET_PER_COMPANY}] SECTION 1/6 — Informations générales    │")
    print("  │                                                     │")
    print("  │  Déjà visible — Appuie sur Entrée pour capturer →   │")
    print("  └─────────────────────────────────────────────────────┘")
    input()

    # Capturer uniquement le header LinkedIn (nom, titre, localisation, résumé)
    soup_gen = BeautifulSoup(driver.page_source, "html.parser")
    header_text = ""
    for sel in [
        ".pv-text-details__left-panel",
        ".pv-top-card",
        ".ph5.pb5",
        "section.pv-profile-section",
        "#about",
    ]:
        elem = soup_gen.select_one(sel)
        if elem:
            for tag in elem(["script","style","svg","button"]):
                tag.decompose()
            header_text += elem.get_text(separator="\n") + "\n"

    if not header_text.strip():
        # Fallback : top 50 lignes de la page
        for tag in soup_gen(["script","style","nav","footer","header","meta","noscript","svg"]):
            tag.decompose()
        all_lines = clean_lines(soup_gen.get_text(separator="\n"))
        header_text = "\n".join(all_lines[:50])

    all_texts["general"] = "\n".join(clean_lines(header_text))
    print(f"  ✅ Informations générales — {len(all_texts['general'])} caractères")

    # ══════════════════════════════════════════════════════════════
    # SECTIONS 2-6 — Sections avec ou sans "Tout afficher"
    # ══════════════════════════════════════════════════════════════
    # (section_num, nom, id_dom, instruction_tout_afficher)
    sections = [
        ("2", "Expériences",    "experience",                  "Tout afficher les X expériences"),
        ("3", "Formations",     "education",                   "Tout afficher les X formations"),
        ("4", "Compétences",    "skills",                      "Tout afficher les X compétences"),
        ("5", "Certifications", "licenses_and_certifications", "Tout afficher les certifications"),
        ("6", "Projets",        "projects",                    "Tout afficher les projets"),
    ]

    for num, name, dom_id, instruction in sections:
        print()
        print("  ┌─────────────────────────────────────────────────────┐")
        print(f"  │  [{valid_count}/{TARGET_PER_COMPANY}] SECTION {num}/6 — {name:<35} │")
        print("  │                                                     │")
        print(f"  │  Si le bouton 'Tout afficher' EXISTE :             │")
        print(f"  │    → Ferme la modale précédente (Echap)            │")
        print(f"  │    → Clique sur '{instruction[:30]}'      │")
        print(f"  │    → Modale OUVERTE → Entrée                       │")
        print("  │                                                     │")
        print(f"  │  Si 'Tout afficher' N'EXISTE PAS :                 │")
        print(f"  │    → Appuie sur [d] pour capture directe           │")
        print("  │                                                     │")
        print("  │  [Entrée] = Modale ouverte, capture modale          │")
        print("  │  [d]      = Pas de bouton, capture section directe  │")
        print("  │  [s]      = Section absente du profil               │")
        print("  └─────────────────────────────────────────────────────┘")
        response = input("  → ").strip().lower()

        if response == "s":
            print(f"  ⏭️  Section {name} absente — skippée")
            continue

        elif response == "d":
            # Capture directe depuis le DOM — section visible dans la page
            section_text = capture_section_from_dom(driver, dom_id)
            if section_text:
                all_texts[name.lower()] = section_text
                print(f"  ✅ {name} capturée depuis DOM — {len(section_text)} caractères")
            else:
                print(f"  ⚠️  Section {name} introuvable dans le DOM — skippée")

        else:
            # Entrée — capture la modale ouverte
            section_text = capture_modal_text(driver)
            if section_text:
                all_texts[name.lower()] = section_text
                print(f"  ✅ {name} capturée depuis modale — {len(section_text)} caractères")
            else:
                print(f"  ⚠️  Modale vide — essai capture DOM comme fallback")
                section_text = capture_section_from_dom(driver, dom_id)
                if section_text:
                    all_texts[name.lower()] = section_text
                    print(f"  ✅ {name} capturée depuis DOM (fallback) — {len(section_text)} caractères")
                else:
                    print(f"  ❌ {name} non capturée")

    # Combiner toutes les sections
    combined = f"URL DU PROFIL : {url}\n\n"
    section_labels = {
        "general":       "=== INFORMATIONS GÉNÉRALES ===",
        "expériences":   "=== EXPÉRIENCES ===",
        "formations":    "=== FORMATIONS ===",
        "compétences":   "=== COMPÉTENCES ===",
        "certifications":"=== CERTIFICATIONS ===",
        "projets":       "=== PROJETS ===",
    }
    for key, text in all_texts.items():
        label = section_labels.get(key, f"=== {key.upper()} ===")
        combined += f"\n{label}\n{text}\n"

    print(f"\n  Texte total combiné : {len(combined)} caractères")
    return combined[:18000]


# ─── CALCUL ANNÉES DEPUIS TIMELINE ────────────────────────────────────

def calc_years_from_timeline(timeline: list) -> tuple:
    if not timeline:
        return 0, [], []

    real_experiences = []
    stages_list      = []

    for t in timeline:
        if not isinstance(t, dict): continue

        role = (t.get("role","") or "").lower()
        desc = (t.get("description","") or "").lower()
        is_stage = any(kw in role or kw in desc for kw in STAGE_KEYWORDS)

        year_start = t.get("year_start")
        year_end   = t.get("year_end") or CURRENT_YEAR

        if not year_start or not isinstance(year_start, (int, float)): continue
        if year_start < 1990 or year_start > CURRENT_YEAR: continue

        entry = {
            "year_start": int(year_start),
            "year_end":   min(int(year_end), CURRENT_YEAR),
            "role":       t.get("role",""),
            "company":    t.get("company","")
        }

        if is_stage:
            stages_list.append(entry)
        else:
            duration = entry["year_end"] - entry["year_start"]
            if duration < 1 and entry["year_end"] != CURRENT_YEAR:
                stages_list.append(entry)
            else:
                real_experiences.append(entry)

    if not real_experiences:
        return 0, [], stages_list

    min_start = min(e["year_start"] for e in real_experiences)
    max_end   = max(e["year_end"]   for e in real_experiences)
    years     = max(0, max_end - min_start)

    return years, real_experiences, stages_list


# ─── VALIDATION MANUELLE ANNÉES ───────────────────────────────────────

def validate_years_of_experience(result: dict, valid_count: int) -> dict | None:
    name     = result.get("full_name","N/A")
    timeline = result.get("experience_timeline",[])

    calculated, real_exps, stages_list = calc_years_from_timeline(timeline)

    old_years = int(result.get("years_of_experience") or 0)
    if calculated > 0:
        result["years_of_experience"] = calculated
        if old_years != calculated:
            print(f"  Post-traitement : years_of_experience {old_years} → {calculated}")

    final_years = int(result.get("years_of_experience") or 0)
    level       = years_to_level(final_years)

    print()
    print("  ┌─────────────────────────────────────────────────────────┐")
    print(f"  │  [{valid_count}/{TARGET_PER_COMPANY}] VALIDATION — {name[:36]:<36} │")
    print("  ├─────────────────────────────────────────────────────────┤")
    print(f"  │  Années d'expérience : {final_years} ans → {level:<27}│")
    print("  │                                                         │")

    if real_exps:
        print("  │  ✅ Expériences réelles (comptées) :                    │")
        for e in real_exps:
            company = (e.get("company","") or "")[:16]
            role    = (e.get("role","")    or "")[:16]
            y_s     = e.get("year_start","?")
            y_e     = e.get("year_end","?")
            print(f"  │     · {y_s}-{y_e}  {company:<16}  {role:<15} │")
    else:
        print("  │  ⚠️  Aucune expérience réelle trouvée                   │")

    if stages_list:
        print("  │                                                         │")
        print("  │  ✗ Stages exclus :                                      │")
        for e in stages_list:
            company = (e.get("company","") or "")[:16]
            role    = (e.get("role","")    or "")[:16]
            y_s     = e.get("year_start","?")
            y_e     = e.get("year_end","?")
            print(f"  │     · {y_s}-{y_e}  {company:<16}  {role:<15} │")

    print("  ├─────────────────────────────────────────────────────────┤")
    print("  │  [Entrée] = OK  |  [nombre] = corriger  |  [s] = skip   │")
    print("  └─────────────────────────────────────────────────────────┘")
    response = input("  → ").strip().lower()

    if response == "s":
        print(f"  ⏭️  Skippé — compteur reste à {valid_count}/{TARGET_PER_COMPANY}")
        return None

    elif response == "":
        print(f"  ✅ Validé : {final_years} ans ({level}) — {valid_count+1}/{TARGET_PER_COMPANY}")
        return result

    else:
        try:
            new_years = int(response)
            result["years_of_experience"] = new_years
            new_level = years_to_level(new_years)
            levels = ["Junior","Medior","Confirmé","Senior","Expert"]
            lower  = levels[max(0, levels.index(new_level) - 1)]
            for key in ["seniority_technologies","seniority_programming_languages"]:
                items = result.get(key,[])
                if items:
                    result[key] = [
                        {**t, "level": new_level if i < 3 else lower}
                        if isinstance(t, dict) else t
                        for i, t in enumerate(items)
                    ]
            print(f"  ✅ Corrigé : {final_years} → {new_years} ans ({new_level}) — {valid_count+1}/{TARGET_PER_COMPANY}")
            return result
        except ValueError:
            print("  Valeur invalide — années non modifiées")
            return result


# ─── UTILITAIRES ──────────────────────────────────────────────────────

def deduplicate_list(items: list) -> list:
    if not items: return []
    seen, result = set(), []
    for item in items:
        norm = str(item).lower().strip()
        if norm and norm not in seen:
            seen.add(norm); result.append(item)
    return result


def years_to_level(y: int) -> str:
    if y >= 10: return "Expert"
    if y >= 6:  return "Senior"
    if y >= 4:  return "Confirmé"
    if y >= 2:  return "Medior"
    return "Junior"


def post_process(profile: dict) -> dict:
    for key in ["technical_skills","programming_languages","certifications","roles_held"]:
        profile[key] = deduplicate_list(profile.get(key,[]))

    final_years  = int(profile.get("years_of_experience") or 0)
    global_level = years_to_level(final_years)

    for key in ["seniority_technologies","seniority_programming_languages"]:
        items = profile.get(key,[])
        if not items or global_level == "Junior": continue
        all_junior = all(t.get("level") == "Junior" for t in items if isinstance(t, dict))
        if all_junior:
            levels = ["Junior","Medior","Confirmé","Senior","Expert"]
            lower  = levels[max(0, levels.index(global_level) - 1)]
            profile[key] = [
                {**t, "level": global_level if i < 3 else lower}
                if isinstance(t, dict) else t
                for i, t in enumerate(items)
            ]
            print(f"  Post-traitement : {key} tous Junior → {global_level}/{lower}")
    return profile


# ─── STRUCTURATION MISTRAL ────────────────────────────────────────────

def structure_with_mistral(text: str, max_retries: int = 3) -> dict:
    from langchain_mistralai import ChatMistralAI
    api_key = os.getenv("MISTRAL_API_KEY")
    if not api_key:
        return {"error": "MISTRAL_API_KEY manquante"}

    llm = ChatMistralAI(
        model="mistral-small-latest", api_key=api_key, timeout=90, max_retries=2
    )
    structured_llm = llm.with_structured_output(schema=JSON_SCHEMA)

    for attempt in range(max_retries):
        try:
            result = structured_llm.invoke(PROMPT + "\n\nCONTENU DU PROFIL :\n" + text)
            if hasattr(result, "model_dump"): return result.model_dump()
            if hasattr(result, "dict"):       return result.dict()
            return dict(result)
        except Exception as e:
            err = str(e)
            if "429" in err or "rate" in err.lower():
                wait = (attempt + 1) * 30
                print(f"    Rate limit — attente {wait}s..."); time.sleep(wait)
            elif "timeout" in err.lower():
                time.sleep(15)
            else:
                print(f"    Erreur Mistral : {err[:100]}")
                if attempt < max_retries - 1: time.sleep(10)
                else: return {"error": err}
    return {"error": "Echec Mistral après tous les essais"}


# ─── ANTI-DOUBLONS ────────────────────────────────────────────────────

def build_existing_index(company_key: str) -> tuple:
    processed_urls     = set()
    processed_names    = set()
    processed_linkedin = set()

    for source in [NEW_CANDS_DIR / f"{company_key}_profiles.json", ENRICHED_FILE]:
        if not source.exists(): continue
        try:
            with open(source, encoding="utf-8") as f:
                profiles = json.load(f)
            for p in profiles:
                fp = p.get("file_path","") or ""
                if fp: processed_urls.add(fp.split("?")[0].rstrip("/").lower())
                name = (p.get("full_name","") or "").lower().strip()
                if name and name not in ("not found",""): processed_names.add(name)
                li = (p.get("linkedin","") or "").lower().strip()
                if li and "/in/" in li: processed_linkedin.add(li.split("?")[0].rstrip("/"))
        except Exception: continue
    return processed_urls, processed_names, processed_linkedin


def is_duplicate(url, profile, processed_urls, processed_names, processed_linkedin) -> str:
    norm_url = url.split("?")[0].rstrip("/").lower()
    if norm_url in processed_urls: return "URL déjà traitée"
    name = (profile.get("full_name","") or "").lower().strip()
    if name and name not in ("not found","") and name in processed_names:
        return f"Nom déjà présent : {profile.get('full_name')}"
    li = (profile.get("linkedin","") or "").lower().strip()
    if li and "/in/" in li:
        if li.split("?")[0].rstrip("/") in processed_linkedin:
            return "LinkedIn URL déjà présente"
    return ""


# ─── RAPPORT INTERMÉDIAIRE ────────────────────────────────────────────

def print_progress_report(validated: list, skipped: int, private: int,
                          company: str, valid_count: int):
    print(f"\n{'═'*60}")
    print(f"  RAPPORT INTERMÉDIAIRE — {company.upper()}")
    print(f"  Validés : {valid_count}/{TARGET_PER_COMPANY}")
    print(f"{'═'*60}")
    for r in validated[-10:]:
        name  = r.get("full_name","N/A")
        years = r.get("years_of_experience",0)
        lvl   = years_to_level(int(years))
        conf  = r.get("parsing_confidence",0)
        sk    = len(r.get("technical_skills",[]))
        print(f"  ✅ {name} — {years} ans — {lvl} — {sk} skills — {conf:.0%}")
    if skipped > 0:  print(f"  ⏭️  Skippés  : {skipped}")
    if private > 0:  print(f"  ⚠️  Privés   : {private}")
    print(f"{'═'*60}")


# ─── SCRAPING PRINCIPAL ───────────────────────────────────────────────

def run_scraping():
    print("=" * 60)
    print("CV-SCANNER-IA — SCRAPING SEMI-AUTOMATIQUE")
    print(f"Cible : {TARGET_PER_COMPANY} profils IT valides par entreprise")
    print(f"Pause tous les {PAUSE_EVERY} profils validés")
    print("=" * 60)

    driver = None
    try:
        driver = setup_driver()
        if not linkedin_login(driver):
            print("Connexion impossible."); return

        for company_key, company_url in COMPANY_URLS:
            output_file = NEW_CANDS_DIR / f"{company_key}_profiles.json"

            print(f"\n{'─'*60}")
            print(f"ENTREPRISE : {company_key.upper()}")
            print(f"Cible : {TARGET_PER_COMPANY} profils IT valides")
            print(f"{'─'*60}")

            # Charger les profils déjà validés pour cette entreprise
            existing = []
            if output_file.exists():
                with open(output_file, encoding="utf-8") as f:
                    existing = json.load(f)

            # Compter les profils déjà validés (pas les erreurs/privés)
            already_valid = [r for r in existing if "error" not in r]
            valid_count   = len(already_valid)

            if valid_count >= TARGET_PER_COMPANY:
                print(f"  Déjà {valid_count} profils valides — cible atteinte ✅")
                continue

            print(f"  Déjà {valid_count}/{TARGET_PER_COMPANY} profils valides")

            # Index anti-doublons
            processed_urls, processed_names, processed_linkedin = \
                build_existing_index(company_key)

            # Récupérer les URLs
            all_urls = get_profile_urls_from_company(driver, company_url, company_key)

            # Filtrer les déjà traités
            remaining = [
                u for u in all_urls
                if u.split("?")[0].rstrip("/").lower() not in processed_urls
            ]
            print(f"  {len(remaining)} URLs restantes à parcourir")

            if not remaining:
                print("  Aucune URL disponible — collecte épuisée.")
                continue

            # Stats pour le rapport
            skipped_count = 0
            private_count = 0
            last_pause_at = valid_count  # dernier palier où on a fait une pause

            # Parcourir les URLs jusqu'à atteindre la cible
            for url_idx, url in enumerate(remaining):

                # Cible atteinte → arrêter pour cette entreprise
                if valid_count >= TARGET_PER_COMPANY:
                    print(f"\n  🎯 Cible atteinte : {valid_count}/{TARGET_PER_COMPANY} profils validés !")
                    break

                norm_url = url.split("?")[0].rstrip("/").lower()
                if norm_url in processed_urls:
                    continue

                print(f"\n  [{url_idx+1}/{len(remaining)}] — "
                      f"Validés : {valid_count}/{TARGET_PER_COMPANY} — "
                      f"{url.split('/in/')[-1]}")

                # Extraction semi-automatique
                text = extract_text_after_manual_clicks(driver, url, valid_count)

                if not text or len(text.strip()) < 150:
                    print("  Contenu insuffisant — profil privé")
                    existing.append({"error":"profil_prive","file_path":url})
                    processed_urls.add(norm_url)
                    private_count += 1

                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(existing, f, ensure_ascii=False, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    continue

                print("  Structuration avec Mistral small...")
                result = structure_with_mistral(text)

                if "error" in result:
                    print(f"  Erreur : {result['error'][:80]}")
                    existing.append({"error":result["error"],"file_path":url})
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(existing, f, ensure_ascii=False, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    continue

                # Vérifier doublon
                dup = is_duplicate(url, result,
                                   processed_urls, processed_names, processed_linkedin)
                if dup:
                    print(f"  DOUBLON : {dup} — skip automatique")
                    processed_urls.add(norm_url)
                    continue

                # Post-traitement
                result = post_process(result)

                # ── VALIDATION MANUELLE ──────────────────────────────────
                validated_result = validate_years_of_experience(result, valid_count)
                # ────────────────────────────────────────────────────────

                if validated_result is None:
                    # Profil skippé — le compteur NE change PAS
                    processed_urls.add(norm_url)
                    skipped_count += 1
                    # Pas de sauvegarde pour les skippés
                    continue

                # Profil validé — sauvegarder et incrémenter
                validated_result["file_path"]         = url
                validated_result["extraction_method"] = f"semi-auto + mistral-small ({company_key})"
                validated_result["extracted_at"]      = datetime.now().isoformat()
                validated_result["company_source"]    = company_key
                validated_result["raw_text"] = text[:3000]  # texte brut limité à 3000 chars
                existing.append(validated_result)
                processed_urls.add(norm_url)

                name = (validated_result.get("full_name","") or "").lower().strip()
                if name: processed_names.add(name)
                li = (validated_result.get("linkedin","") or "").lower().strip()
                if li and "/in/" in li:
                    processed_linkedin.add(li.split("?")[0].rstrip("/"))

                valid_count += 1

                # Sauvegarde incrémentale avec vérification
                try:
                    with open(output_file, "w", encoding="utf-8") as f:
                        json.dump(existing, f, ensure_ascii=False, indent=2)
                        f.flush()
                        os.fsync(f.fileno())
                    # Vérification que le fichier est bien écrit
                    with open(output_file, "r", encoding="utf-8") as f:
                        saved = json.load(f)
                    real_saved = len([r for r in saved if "error" not in r])
                    print(f"  💾 Vérifié sur disque : {real_saved}/{TARGET_PER_COMPANY} profils valides")
                except Exception as e:
                    print(f"  ❌ ERREUR SAUVEGARDE : {e}")
                    print(f"  ⚠️  Rollback — profil retiré de la liste")
                    existing.pop()
                    valid_count -= 1
                    continue

                print(f"  ✅ Sauvegardé — {valid_count}/{TARGET_PER_COMPANY}")

                # Pause tous les PAUSE_EVERY profils validés
                if (valid_count - last_pause_at) >= PAUSE_EVERY and valid_count < TARGET_PER_COMPANY:
                    valid_profiles = [r for r in existing if "error" not in r]
                    print_progress_report(valid_profiles, skipped_count,
                                          private_count, company_key, valid_count)
                    print(f"\n  Appuie sur Entrée pour continuer vers {TARGET_PER_COMPANY}")
                    print(f"  ou Ctrl+C pour arrêter et reprendre plus tard.")
                    input()
                    last_pause_at = valid_count

                # Délai entre profils
                delay = random.uniform(MIN_DELAY, MAX_DELAY)
                print(f"  Pause {delay:.1f}s...")
                time.sleep(delay)

            # Résumé final entreprise
            final_valid = len([r for r in existing if "error" not in r])
            print(f"\n{'═'*60}")
            print(f"  {company_key.upper()} — RÉSUMÉ FINAL")
            print(f"  ✅ Profils valides    : {final_valid}/{TARGET_PER_COMPANY}")
            print(f"  ⏭️  Skippés            : {skipped_count}")
            print(f"  ⚠️  Privés             : {private_count}")
            print(f"  Fichier              : {output_file}")
            print(f"{'═'*60}")

            if final_valid < TARGET_PER_COMPANY:
                print(f"  ⚠️  Cible non atteinte ({final_valid}/{TARGET_PER_COMPANY})")
                print(f"  Cause : URLs épuisées. Relance le script pour collecter plus d'URLs.")

            # Pause entre entreprises
            if (company_key, company_url) != COMPANY_URLS[-1]:
                print(f"\n  Appuie sur Entrée pour l'entreprise suivante")
                print(f"  ou Ctrl+C pour arrêter.")
                input()

        # Résumé global
        print(f"\n{'='*60}")
        print("SCRAPING TERMINÉ — RÉSUMÉ GLOBAL")
        print(f"{'='*60}")
        total = 0
        for company_key, _ in COMPANY_URLS:
            f = NEW_CANDS_DIR / f"{company_key}_profiles.json"
            if f.exists():
                with open(f, encoding="utf-8") as fp:
                    data = json.load(fp)
                valid = len([r for r in data if "error" not in r])
                total += valid
                status = "✅" if valid >= TARGET_PER_COMPANY else "⚠️ "
                print(f"  {status} {company_key:<10} : {valid}/{TARGET_PER_COMPANY} profils valides")
        print(f"  {'─'*30}")
        print(f"  TOTAL : {total} nouveaux profils")
        print(f"\nPour fusionner :")
        print(f"  python merge_profiles.py")

    except KeyboardInterrupt:
        print("\nArrêt manuel — données sauvegardées.")
    except Exception as e:
        print(f"\nErreur : {e}")
        import traceback; traceback.print_exc()
    finally:
        if driver:
            try: driver.quit()
            except Exception: pass
            print("Navigateur fermé.")


if __name__ == "__main__":
    run_scraping()