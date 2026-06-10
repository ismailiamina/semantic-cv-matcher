"""
merge_profiles.py
==================
Fusionne tous les profils de new_candidates/*.json
en un seul fichier propre sans doublons.

Source  : data/real_data/new_candidates/*.json
Output  : data/real_data/extracted_cvs_real_final.json
"""

import json
import os
from pathlib import Path
from datetime import datetime

NEW_CANDS_DIR = Path("data/real_data/new_candidates")
OUTPUT_FILE   = Path("data/real_data/extracted_cvs_real_final.json")


def normalize_name(name: str) -> str:
    if not name: return ""
    return name.lower().strip().replace("  ", " ")


def normalize_url(url: str) -> str:
    if not url: return ""
    return url.split("?")[0].rstrip("/").lower()


def is_valid_profile(profile: dict) -> bool:
    if "error" in profile:
        return False
    name = (profile.get("full_name") or "").strip()
    if not name or name.lower() in ("not found", "", "n/a"):
        return False
    return True


def merge_profiles():
    print("=" * 60)
    print("FUSION DES PROFILS — new_candidates -> extracted_cvs_real_final")
    print("=" * 60)

    all_profiles  = []
    seen_urls     = set()
    seen_names    = set()
    stats         = {}

    # Lire tous les fichiers *_profiles.json
    json_files = sorted(NEW_CANDS_DIR.glob("*_profiles.json"))

    if not json_files:
        print(f"Aucun fichier trouve dans {NEW_CANDS_DIR}")
        return

    print(f"\nFichiers trouves : {len(json_files)}")

    for json_file in json_files:
        company_name = json_file.stem.replace("_profiles", "")
        try:
            with open(json_file, encoding="utf-8") as f:
                profiles = json.load(f)
        except Exception as e:
            print(f"  ERREUR lecture {json_file.name} : {e}")
            continue

        valid   = [p for p in profiles if is_valid_profile(p)]
        invalid = len(profiles) - len(valid)

        print(f"\n  {json_file.name}")
        print(f"    Total    : {len(profiles)}")
        print(f"    Valides  : {len(valid)}")
        print(f"    Invalides (erreurs/prives) : {invalid}")

        added   = 0
        skipped = 0

        for profile in valid:
            # Normaliser URL et nom pour deduplication
            url  = normalize_url(profile.get("file_path") or profile.get("linkedin") or "")
            name = normalize_name(profile.get("full_name") or "")

            # Verifier doublon
            is_dup = False
            if url and url in seen_urls:
                is_dup = True
            if name and name in seen_names:
                is_dup = True

            if is_dup:
                skipped += 1
                continue

            # Ajouter
            if url:  seen_urls.add(url)
            if name: seen_names.add(name)

            # Ajouter champ company_source si absent
            if not profile.get("company_source"):
                profile["company_source"] = company_name

            all_profiles.append(profile)
            added += 1

        stats[company_name] = {"added": added, "skipped": skipped}
        print(f"    Ajoutes  : {added}")
        print(f"    Doublons : {skipped}")

    # Sauvegarder
    print(f"\n{'='*60}")
    print(f"RESULTAT FINAL")
    print(f"{'='*60}")
    print(f"  Total profils uniques : {len(all_profiles)}")

    # Rapport par entreprise
    print(f"\n  Repartition par entreprise :")
    company_counts = {}
    for p in all_profiles:
        co = p.get("company_source", "inconnu")
        company_counts[co] = company_counts.get(co, 0) + 1
    for co, count in sorted(company_counts.items(), key=lambda x: -x[1]):
        print(f"    {co:<35} : {count} profils")

    # Sauvegarder avec fsync
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_profiles, f, ensure_ascii=False, indent=2)
        f.flush()
        os.fsync(f.fileno())

    # Verification
    with open(OUTPUT_FILE, encoding="utf-8") as f:
        check = json.load(f)

    print(f"\n  Sauvegarde : {OUTPUT_FILE}")
    print(f"  Verification : {len(check)} profils ecrits sur disque")
    print(f"\n  Prochaine etape :")
    print(f"    python Weaviate_DB/SetupDB.py")
    print(f"    python Weaviate_DB/insert_data.py")


if __name__ == "__main__":
    merge_profiles()