"""
Générateur de CVs Synthétiques — Profils IT

"""

import json
import random
import os
from datetime import datetime, timedelta
from fpdf import FPDF  # pip install fpdf2


PRENOMS = [
    "Amine", "Youssef", "Mehdi", "Omar", "Karim", "Soufiane", "Hamza",
    "Ilias", "Rachid", "Tariq", "Saad", "Othmane", "Zakaria", "Bilal",
    "Salma", "Fatima", "Zineb", "Nadia", "Sara", "Khadija", "Hajar",
    "Meryem", "Imane", "Loubna", "Sanae", "Amina", "Houda", "Ghizlane",
    "Adam", "Reda", "Badr", "Walid", "Hicham", "Adil", "Mourad"
]

NOMS = [
    "Benali", "Alaoui", "El Idrissi", "Benkirane", "Chraibi", "Tahiri",
    "Bennani", "El Amrani", "Berrada", "Bensaid", "Lahlou", "Filali",
    "Tazi", "Kettani", "Naciri", "El Fassi", "Bakkali", "Rahimi",
    "Ouazzani", "El Mansouri", "Hajji", "Ziani", "Bouazza", "Skalli",
    "El Khatib", "Moussaoui", "Bencheikh", "Boutaleb", "Essalhi"
]

VILLES = [
    "Casablanca", "Rabat", "Marrakech", "Fès", "Tanger",
    "Agadir", "Meknès", "Oujda", "Kenitra", "Tétouan"
]

UNIVERSITES = [
    "ENSIAS", "INPT", "ENSA Rabat", "ENSEM", "ENSA Casablanca",
    "EMI", "ENSET", "FST Mohammedia", "ENSA Marrakech", "INSEA",
    "UIR", "ESGI", "EMSI", "HEM", "ENSAM Meknès"
]

DOMAINES_ETUDES = [
    "Génie Informatique", "Science des Données", "Génie Logiciel",
    "Intelligence Artificielle", "Réseaux et Télécommunications",
    "Cybersécurité", "Systèmes Embarqués", "Big Data",
    "Développement Web", "Cloud Computing"
]

ROLES = [
    ["Développeur Full Stack", "Ingénieur Backend", "Développeur Frontend"],
    ["Data Engineer", "Data Scientist", "ML Engineer"],
    ["DevOps Engineer", "SRE Engineer", "Cloud Architect"],
    ["Développeur Backend", "Ingénieur API", "Développeur Python"],
    ["Data Analyst", "Business Intelligence Developer", "Data Scientist"],
    ["Développeur Mobile", "iOS Developer", "Android Developer"],
    ["Ingénieur Cybersécurité", "Pentester", "Security Analyst"],
    ["Architecte Solutions", "Tech Lead", "CTO"],
    ["Scrum Master", "Product Owner", "Agile Coach"],
    ["Ingénieur QA", "Test Automation Engineer", "QA Lead"],
]

TECH_SKILLS_PAR_ROLE = {
    "data": ["Machine Learning", "Deep Learning", "TensorFlow", "PyTorch",
             "Scikit-learn", "Pandas", "NumPy", "Apache Spark", "Kafka",
             "Airflow", "MLflow", "Docker", "Kubernetes", "AWS SageMaker",
             "Power BI", "Tableau", "Elasticsearch", "Redis", "FastAPI"],
    "web": ["React", "Angular", "Vue.js", "Node.js", "Django", "FastAPI",
            "Spring Boot", "Docker", "Kubernetes", "REST API", "GraphQL",
            "PostgreSQL", "MongoDB", "Redis", "Nginx", "Jenkins", "Git",
            "TypeScript", "HTML/CSS", "Tailwind CSS"],
    "devops": ["Docker", "Kubernetes", "Terraform", "Ansible", "Jenkins",
               "GitLab CI/CD", "AWS", "Azure", "GCP", "Prometheus",
               "Grafana", "ELK Stack", "Helm", "ArgoCD", "Linux", "Bash"],
    "mobile": ["React Native", "Flutter", "SwiftUI", "Jetpack Compose",
               "Firebase", "REST API", "GraphQL", "Redux", "SQLite",
               "Fastlane", "TestFlight", "Android Studio", "Xcode"],
    "security": ["Penetration Testing", "SIEM", "SOC", "Splunk", "Wireshark",
                 "Metasploit", "Burp Suite", "OWASP", "ISO 27001",
                 "Firewall", "IDS/IPS", "PKI", "Kali Linux"],
}

PROG_LANGS_PAR_ROLE = {
    "data": ["Python", "R", "SQL", "Scala", "Julia"],
    "web": ["JavaScript", "TypeScript", "Python", "Java", "PHP", "Go"],
    "devops": ["Python", "Bash", "Go", "YAML", "HCL"],
    "mobile": ["Swift", "Kotlin", "JavaScript", "Dart"],
    "security": ["Python", "Bash", "C", "PowerShell", "Ruby"],
}

INDUSTRIES = [
    "Finance", "Banque", "Assurance", "E-commerce", "Santé",
    "Télécommunications", "Énergie", "Logistique", "Éducation",
    "Immobilier", "Transport", "Retail", "Manufacturing"
]

ENTREPRISES = [
    "OCP", "Maroc Telecom", "BMCE Bank", "Attijariwafa Bank",
    "CIH Bank", "TotalEnergies Maroc", "Renault Maroc", "Société Générale",
    "BNP Paribas Maroc", "Orange Maroc", "Inwi", "HPS",
    "CGI Maroc", "Capgemini Maroc", "Accenture Maroc", "Deloitte Maroc",
    "Ernst & Young", "Leyton", "Alten", "Sopra Steria",
    "Atos Maroc", "IBM Maroc", "Microsoft Maroc", "Oracle Maroc"
]

CERTIFICATIONS = [
    "AWS Solutions Architect", "AWS Cloud Practitioner",
    "Google Cloud Professional", "Azure Administrator",
    "Kubernetes CKA", "Terraform Associate",
    "Scrum Master PSM I", "PMP", "CISSP", "CEH",
    "Microsoft Azure Developer", "Google Associate Cloud Engineer"
]

LANGUES = ["Français", "Anglais", "Arabe", "Espagnol", "Allemand"]

NIVEAUX_SENIORITE = ["Junior", "Medior", "Confirmé", "Senior", "Expert"]
NIVEAUX_EDUCATION = ["Bac + 2", "Bac + 3", "Bac + 4", "Bac + 5", "Bac + 6 et plus"]


def get_role_type(roles):
    """Détermine le type de rôle pour choisir les bonnes compétences"""
    roles_str = " ".join(roles).lower()
    if any(k in roles_str for k in ["data", "ml", "machine", "science", "analyst"]):
        return "data"
    elif any(k in roles_str for k in ["devops", "sre", "cloud", "infra"]):
        return "devops"
    elif any(k in roles_str for k in ["mobile", "ios", "android", "flutter"]):
        return "mobile"
    elif any(k in roles_str for k in ["security", "cybersec", "pentest"]):
        return "security"
    else:
        return "web"


def generate_years_of_experience():
    """Génère des années d'expérience avec distribution réaliste"""
    weights = [20, 30, 25, 15, 7, 3]  # Junior à Expert
    ranges = [(0, 1), (1, 3), (3, 5), (5, 8), (8, 12), (12, 20)]
    chosen_range = random.choices(ranges, weights=weights)[0]
    return random.randint(chosen_range[0], chosen_range[1])


def get_seniority_level(years):
    """Retourne le niveau de séniorité selon les années"""
    if years <= 2:
        return "Junior"
    elif years <= 4:
        return "Medior"
    elif years <= 6:
        return "Confirmé"
    elif years <= 10:
        return "Senior"
    else:
        return "Expert"


def generate_experience_timeline(years_of_experience, role_type, entreprises_pool):
    """Génère une timeline d'expérience chronologique réaliste"""
    timeline = []
    current_year = datetime.now().year
    remaining_years = years_of_experience
    current_date = current_year

    tech_pool = TECH_SKILLS_PAR_ROLE.get(role_type, TECH_SKILLS_PAR_ROLE["web"])
    lang_pool = PROG_LANGS_PAR_ROLE.get(role_type, PROG_LANGS_PAR_ROLE["web"])

    num_experiences = min(max(1, years_of_experience // 2), 4)

    for i in range(num_experiences):
        duration = random.randint(1, max(1, remaining_years - (num_experiences - i - 1)))
        if i == num_experiences - 1:
            duration = remaining_years

        start_year = current_date - duration
        end_year = current_date if i == 0 else current_date

        skills_this_period = random.sample(tech_pool, min(random.randint(3, 6), len(tech_pool)))
        langs_this_period = random.sample(lang_pool, min(random.randint(1, 3), len(lang_pool)))

        timeline.append({
            "year_start": start_year,
            "year_end": end_year,
            "company": random.choice(entreprises_pool),
            "skills_acquired": skills_this_period,
            "languages_used": langs_this_period
        })

        current_date = start_year
        remaining_years -= duration

        if remaining_years <= 0:
            break

    timeline.reverse()
    return timeline


def generate_work_experience_text(timeline, roles):
    """Génère un texte de description d'expérience professionnelle"""
    texts = []
    for exp in timeline:
        skills_str = ", ".join(exp["skills_acquired"])
        langs_str = ", ".join(exp["languages_used"])
        text = (
            f"Chez {exp['company']} ({exp['year_start']}-{exp['year_end']}), "
            f"j'ai travaillé en tant que {random.choice(roles)} "
            f"en utilisant {skills_str} et {langs_str}."
        )
        texts.append(text)
    return " ".join(texts)


def generate_projects_text(role_type, industries):
    """Génère des descriptions de projets réalistes"""
    projects = []
    num_projects = random.randint(2, 4)
    tech_pool = TECH_SKILLS_PAR_ROLE.get(role_type, TECH_SKILLS_PAR_ROLE["web"])

    project_types = {
        "data": [
            "Système de recommandation produits",
            "Pipeline de données en temps réel",
            "Modèle de prédiction du churn client",
            "Dashboard analytique Business Intelligence",
            "Détection de fraude par Machine Learning",
            "Chatbot intelligent basé sur NLP"
        ],
        "web": [
            "Plateforme e-commerce full stack",
            "API REST microservices",
            "Application de gestion RH",
            "Portail client bancaire",
            "Système de réservation en ligne",
            "Dashboard de reporting temps réel"
        ],
        "devops": [
            "Migration infrastructure vers le Cloud",
            "Pipeline CI/CD automatisé",
            "Cluster Kubernetes haute disponibilité",
            "Monitoring et alerting centralisé",
            "Infrastructure as Code avec Terraform"
        ],
        "mobile": [
            "Application mobile bancaire",
            "Application e-commerce mobile",
            "Application de livraison en temps réel",
            "Application santé et fitness"
        ],
        "security": [
            "Audit de sécurité applicatif",
            "Mise en place SOC",
            "Pentest infrastructure réseau",
            "Implémentation ISO 27001"
        ]
    }

    project_list = project_types.get(role_type, project_types["web"])
    selected_projects = random.sample(project_list, min(num_projects, len(project_list)))

    for project in selected_projects:
        techs = random.sample(tech_pool, min(3, len(tech_pool)))
        industry = random.choice(industries)
        projects.append(
            f"Projet '{project}' dans le secteur {industry} "
            f"en utilisant {', '.join(techs)}."
        )

    return " ".join(projects)


def generate_summary(name, roles, years, skills, langs):
    """Génère un résumé professionnel"""
    role = roles[0]
    top_skills = ", ".join(skills[:3])
    top_langs = ", ".join(langs[:2])
    seniority = get_seniority_level(years)

    summaries = [
        f"Ingénieur {seniority} en {role} avec {years} ans d'expérience. "
        f"Spécialisé en {top_skills}. "
        f"Maîtrise des langages {top_langs}. "
        f"Passionné par l'innovation technologique et le développement de solutions performantes.",

        f"Professionnel {role} avec {years} années d'expérience dans le développement "
        f"de solutions techniques. Expert en {top_skills}. "
        f"Forte maîtrise de {top_langs} avec une approche orientée qualité et performance.",

        f"{years} ans d'expérience en tant que {role}. "
        f"Compétences avancées en {top_skills}. "
        f"Rigoureux, autonome et orienté résultats. "
        f"Expérience dans des environnements Agile/Scrum."
    ]

    return random.choice(summaries)


def generate_synthetic_cv(index):
    """Génère un CV synthétique complet"""

    # Informations personnelles
    prenom = random.choice(PRENOMS)
    nom = random.choice(NOMS)
    full_name = f"{prenom} {nom}"
    ville = random.choice(VILLES)
    nom_email = nom.lower().replace(' ', '').replace("'", '')
    email = f"{prenom.lower()}.{nom_email}@gmail.com"
    phone = f"+212 6{random.randint(10, 99)} {random.randint(100, 999)} {random.randint(100, 999)}"
    nom_clean = nom.lower().replace(' ', '-').replace("'", '')
    linkedin = f"linkedin.com/in/{prenom.lower()}-{nom_clean}"
    github = f"github.com/{prenom.lower()}{random.randint(10, 99)}"

    # Expérience et rôles
    years = generate_years_of_experience()
    roles_group = random.choice(ROLES)
    role_type = get_role_type(roles_group)
    roles = roles_group[:random.randint(1, 3)]

    # Compétences
    tech_pool = TECH_SKILLS_PAR_ROLE.get(role_type, TECH_SKILLS_PAR_ROLE["web"])
    lang_pool = PROG_LANGS_PAR_ROLE.get(role_type, PROG_LANGS_PAR_ROLE["web"])

    num_skills = min(random.randint(5, 12), len(tech_pool))
    num_langs = min(random.randint(2, 4), len(lang_pool))

    technical_skills = random.sample(tech_pool, num_skills)
    programming_languages = random.sample(lang_pool, num_langs)
    spoken_languages = random.sample(LANGUES, random.randint(2, 3))

    # Industries
    industries = random.sample(INDUSTRIES, random.randint(1, 3))

    # Certifications (pas toujours)
    certifications = []
    if random.random() > 0.5:
        certifications = random.sample(CERTIFICATIONS, random.randint(1, 2))

    # Séniorité
    seniority_level = get_seniority_level(years)
    top_techs = technical_skills[:min(3, len(technical_skills))]
    top_langs = programming_languages[:min(2, len(programming_languages))]

    seniority_technologies = [
        {"technology": tech, "level": seniority_level}
        for tech in top_techs
    ]
    seniority_programming_languages = [
        {"language": lang, "level": seniority_level}
        for lang in top_langs
    ]

    # Timeline d'expérience (pour la trajectoire)
    timeline = generate_experience_timeline(
        years, role_type,
        random.sample(ENTREPRISES, min(5, len(ENTREPRISES)))
    )

    # Textes
    work_experience = generate_work_experience_text(timeline, roles)
    projects = generate_projects_text(role_type, industries)
    summary = generate_summary(full_name, roles, years, technical_skills, programming_languages)

    # Education
    education_level = random.choice(NIVEAUX_EDUCATION)
    field_of_studies = random.choice(DOMAINES_ETUDES)
    universite = random.choice(UNIVERSITES)

    # Parsing confidence simulé
    parsing_confidence = round(random.uniform(0.75, 0.99), 2)

    cv_data = {
        "full_name": full_name,
        "email": email,
        "phone": phone,
        "location": ville,
        "years_of_experience": years,
        "linkedin": linkedin,
        "github": github,
        "roles_held": roles,
        "programming_languages": programming_languages,
        "technical_skills": technical_skills,
        "spoken_languages": spoken_languages,
        "certifications": certifications,
        "seniority_technologies": seniority_technologies,
        "seniority_programming_languages": seniority_programming_languages,
        "industry": {
            "primary_industries": industries
        },
        "summary": summary,
        "education_level": education_level,
        "field_of_studies": field_of_studies,
        "work_experience": work_experience,
        "projects": projects,
        "file_path": f"synthetic_cvs/cv_{index:04d}_{prenom.lower()}_{nom.lower().replace(' ', '_')}.json",
        "parsing_confidence": parsing_confidence,

        # Champs supplémentaires pour la trajectoire de carrière
        "experience_timeline": timeline,
        "universite": universite,
        "seniority_level_global": seniority_level
    }

    return cv_data


def save_as_json(cv_data, output_dir):
    """Sauvegarde le CV en JSON"""
    filename = os.path.basename(cv_data["file_path"]).replace(".json", "")
    filepath = os.path.join(output_dir, "json", f"{filename}.json")
    os.makedirs(os.path.dirname(filepath), exist_ok=True)

    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(cv_data, f, ensure_ascii=False, indent=2)

    return filepath


def save_as_pdf(cv_data, output_dir):
    """Génère un PDF réaliste à partir des données du CV"""
    try:
        pdf = FPDF()
        pdf.add_page()
        pdf.set_auto_page_break(auto=True, margin=15)

        # En-tête
        pdf.set_font("Helvetica", "B", 20)
        pdf.set_fill_color(30, 90, 160)
        pdf.set_text_color(255, 255, 255)
        pdf.rect(0, 0, 210, 35, "F")
        pdf.set_xy(10, 8)
        pdf.cell(0, 10, cv_data["full_name"], ln=True)

        pdf.set_font("Helvetica", "", 10)
        pdf.set_xy(10, 20)
        contact = f"{cv_data['email']}  |  {cv_data['phone']}  |  {cv_data['location']}"
        pdf.cell(0, 8, contact, ln=True)

        pdf.set_text_color(0, 0, 0)
        pdf.set_xy(10, 40)

        def section_title(title):
            pdf.set_font("Helvetica", "B", 13)
            pdf.set_fill_color(240, 245, 255)
            pdf.set_text_color(30, 90, 160)
            pdf.cell(0, 8, title, ln=True, fill=True)
            pdf.set_text_color(0, 0, 0)
            pdf.set_font("Helvetica", "", 10)
            pdf.ln(2)

        def body_text(text, max_chars=200):
            pdf.set_font("Helvetica", "", 10)
            pdf.multi_cell(0, 6, text[:max_chars] + "..." if len(text) > max_chars else text)
            pdf.ln(2)

        # Résumé
        section_title("PROFIL PROFESSIONNEL")
        body_text(cv_data["summary"])

        # Informations clés
        section_title("INFORMATIONS")
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 6, f"Roles : {', '.join(cv_data['roles_held'])}", ln=True)
        pdf.cell(0, 6, f"Experience : {cv_data['years_of_experience']} ans", ln=True)
        pdf.cell(0, 6, f"Formation : {cv_data['education_level']} en {cv_data['field_of_studies']}", ln=True)
        pdf.cell(0, 6, f"LinkedIn : {cv_data['linkedin']}", ln=True)
        pdf.cell(0, 6, f"GitHub : {cv_data['github']}", ln=True)
        pdf.ln(3)

        # Compétences techniques
        section_title("COMPETENCES TECHNIQUES")
        pdf.cell(0, 6, f"Langages : {', '.join(cv_data['programming_languages'])}", ln=True)
        pdf.cell(0, 6, f"Technologies : {', '.join(cv_data['technical_skills'])}", ln=True)
        pdf.cell(0, 6, f"Langues : {', '.join(cv_data['spoken_languages'])}", ln=True)
        if cv_data["certifications"]:
            pdf.cell(0, 6, f"Certifications : {', '.join(cv_data['certifications'])}", ln=True)
        pdf.ln(3)

        # Expérience
        section_title("EXPERIENCE PROFESSIONNELLE")
        body_text(cv_data["work_experience"], max_chars=500)

        # Projets
        section_title("PROJETS")
        body_text(cv_data["projects"], max_chars=400)

        # Sauvegarde
        filename = os.path.basename(cv_data["file_path"]).replace(".json", "")
        pdf_path = os.path.join(output_dir, "pdf", f"{filename}.pdf")
        os.makedirs(os.path.dirname(pdf_path), exist_ok=True)
        pdf.output(pdf_path)

        return pdf_path

    except Exception as e:
        print(f"Erreur PDF pour {cv_data['full_name']} : {e}")
        return None


def generate_dataset(num_cvs=100, output_dir="synthetic_data"):
    """
    Génère un dataset complet de CVs synthétiques
    
    Args:
        num_cvs    : nombre de CVs à générer
        output_dir : dossier de sortie
    """

    print(f"\n Génération de {num_cvs} CVs synthétiques...")
    print(f" Dossier de sortie : {output_dir}/")
    print("-" * 50)

    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(os.path.join(output_dir, "json"), exist_ok=True)
    os.makedirs(os.path.join(output_dir, "pdf"), exist_ok=True)

    all_cvs = []
    stats = {
        "total": num_cvs,
        "junior": 0,
        "medior": 0,
        "confirme": 0,
        "senior": 0,
        "expert": 0,
        "roles": {},
        "villes": {}
    }

    for i in range(num_cvs):
        cv = generate_synthetic_cv(i + 1)
        all_cvs.append(cv)

        # JSON
        json_path = save_as_json(cv, output_dir)

        # PDF
        pdf_path = save_as_pdf(cv, output_dir)

        # Statistiques
        level = cv["seniority_level_global"].lower().replace("é", "e").replace("ó", "o")
        if level in stats:
            stats[level] += 1

        role = cv["roles_held"][0]
        stats["roles"][role] = stats["roles"].get(role, 0) + 1

        ville = cv["location"]
        stats["villes"][ville] = stats["villes"].get(ville, 0) + 1

        if (i + 1) % 10 == 0:
            print(f" {i + 1}/{num_cvs} CVs générés...")

    # Sauvegarde dataset complet
    dataset_path = os.path.join(output_dir, "dataset_complet.json")
    with open(dataset_path, "w", encoding="utf-8") as f:
        json.dump(all_cvs, f, ensure_ascii=False, indent=2)

    # Rapport final
    rapport_path = os.path.join(output_dir, "rapport_generation.json")
    with open(rapport_path, "w", encoding="utf-8") as f:
        json.dump(stats, f, ensure_ascii=False, indent=2)

    print("\n" + "=" * 50)
    print(" GÉNÉRATION TERMINÉE")
    print("=" * 50)
    print(f" Total CVs générés : {num_cvs}")
    print(f" Dossier JSON      : {output_dir}/json/")
    print(f" Dossier PDF       : {output_dir}/pdf/")
    print(f" Dataset complet   : {dataset_path}")
    print(f" Rapport           : {rapport_path}")
    print("\n Distribution par niveau :")
    for level in ["junior", "medior", "confirme", "senior", "expert"]:
        count = stats.get(level, 0)
        bar = "█" * count
        print(f"  {level.capitalize():12} : {count:3} {bar}")
    print("\n Top 5 villes :")
    top_villes = sorted(stats["villes"].items(), key=lambda x: x[1], reverse=True)[:5]
    for ville, count in top_villes:
        print(f"  {ville:15} : {count}")

    return all_cvs, stats


if __name__ == "__main__":
    
    cvs, stats = generate_dataset(
        num_cvs=200,
        output_dir="synthetic_data"
    )

    print("\n Exemple de CV généré :")
    example = cvs[0]
    print(f"  Nom        : {example['full_name']}")
    print(f"  Ville      : {example['location']}")
    print(f"  Experience : {example['years_of_experience']} ans")
    print(f"  Niveau     : {example['seniority_level_global']}")
    print(f"  Roles      : {', '.join(example['roles_held'])}")
    print(f"  Skills     : {', '.join(example['technical_skills'][:3])}...")
    print(f"  Timeline   : {len(example['experience_timeline'])} postes")