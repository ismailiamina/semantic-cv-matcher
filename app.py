import streamlit as st
import sys
import requests
import time
from pathlib import Path

sys.path.append(str(Path(__file__).parent / "weaviate"))

sys.path.append(str(Path(__file__).parent / "weaviate" / "search"))

from candidates_for_job import (
    candidates_for_job,
    connect_weaviate,
    CANDIDATE_COLLECTION_NAME,
    JOB_COLLECTION_NAME,
    TENANT_ID_FOR_CV,
    TENANT_ID_FOR_JOBS,
    WEIGHTS,
)

st.set_page_config(page_title="Nexus — Semantic Talent Matching", layout="wide")

st.markdown("""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600&family=JetBrains+Mono:wght@400;500&display=swap');

*, html, body { font-family: 'Inter', sans-serif; }
.stApp { background: #FAFBFC; }
[data-testid="stSidebar"] { background: #FFFFFF; border-right: 1px solid #E8EAED; }

.header { padding: 28px 0 20px; border-bottom: 1px solid #E8EAED; margin-bottom: 24px; }
.header-title { font-size: 22px; font-weight: 600; color: #111827; letter-spacing: -0.4px; }
.header-sub { font-size: 13px; color: #6B7280; margin-top: 3px; }

.kpi-row { display: flex; gap: 10px; margin-bottom: 24px; }
.kpi { flex: 1; background: #fff; border: 1px solid #E8EAED; border-radius: 6px; padding: 14px 18px; }
.kpi-val { font-family: 'JetBrains Mono', monospace; font-size: 22px; font-weight: 600; color: #111827; }
.kpi-lbl { font-size: 10px; text-transform: uppercase; letter-spacing: 1px; color: #9CA3AF; margin-top: 2px; }

.job-card { background: #fff; border: 1px solid #E8EAED; border-left: 3px solid #111827; border-radius: 6px; padding: 18px 22px; margin-bottom: 20px; }
.job-name { font-size: 16px; font-weight: 600; color: #111827; }
.job-meta { font-size: 12px; color: #6B7280; margin-top: 4px; margin-bottom: 14px; }
.job-meta b { color: #374151; }

.lbl { font-size: 10px; font-weight: 600; letter-spacing: 1px; text-transform: uppercase; color: #9CA3AF; margin-bottom: 5px; }
.tag { display: inline-block; font-family: 'JetBrains Mono', monospace; font-size: 11px; background: #F3F4F6; color: #374151; border: 1px solid #E5E7EB; border-radius: 3px; padding: 2px 8px; margin: 2px 2px 2px 0; }
.tag-l { background: #EEF2FF; color: #4338CA; border-color: #C7D2FE; }
.tag-c { background: #ECFDF5; color: #065F46; border-color: #6EE7B7; }

.cand-card { background: #fff; border: 1px solid #E8EAED; border-radius: 6px; padding: 18px 22px; margin-bottom: 10px; }
.cand-header { display: flex; justify-content: space-between; align-items: flex-start; margin-bottom: 10px; }
.cand-rank { font-size: 10px; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; color: #9CA3AF; }
.cand-name { font-size: 15px; font-weight: 600; color: #111827; }
.cand-role { font-size: 12px; color: #6B7280; margin-top: 1px; }
.cand-meta { font-size: 11px; color: #9CA3AF; margin-bottom: 10px; }
.cand-meta span { margin-right: 14px; }
.cand-divider { border: none; border-top: 1px solid #F3F4F6; margin: 10px 0; }

.score-pill { font-family: 'JetBrains Mono', monospace; font-size: 18px; font-weight: 600; padding: 5px 14px; border-radius: 4px; }
.s-hi { background: #ECFDF5; color: #059669; border: 1px solid #A7F3D0; }
.s-md { background: #FFFBEB; color: #D97706; border: 1px solid #FDE68A; }
.s-lo { background: #FEF2F2; color: #DC2626; border: 1px solid #FECACA; }

.breakdown { background: #F9FAFB; border: 1px solid #E8EAED; border-radius: 5px; padding: 12px 14px; }
.br-row { display: flex; align-items: center; gap: 8px; margin-bottom: 5px; }
.br-lbl { font-size: 11px; color: #6B7280; width: 72px; flex-shrink: 0; }
.br-track { flex: 1; height: 4px; background: #E5E7EB; border-radius: 2px; overflow: hidden; }
.br-fill { height: 100%; background: #374151; border-radius: 2px; }
.br-pct { font-family: 'JetBrains Mono', monospace; font-size: 11px; color: #374151; width: 32px; text-align: right; }

.traj { background: #EFF6FF; border: 1px solid #BFDBFE; border-radius: 5px; padding: 10px 14px; margin-top: 10px; font-size: 12px; color: #1E40AF; }
.traj-lbl { font-size: 10px; font-weight: 600; letter-spacing: 0.8px; text-transform: uppercase; color: #3B82F6; margin-bottom: 4px; }
.traj-pred { font-size: 11px; color: #2563EB; margin-top: 5px; }

.timeline { font-size: 11px; color: #6B7280; background: #F9FAFB; border: 1px solid #E8EAED; border-radius: 4px; padding: 8px 12px; margin-top: 8px; }

.empty { text-align: center; padding: 50px; color: #9CA3AF; }
.empty-title { font-size: 14px; font-weight: 500; color: #6B7280; margin-bottom: 6px; }

.sb-lbl { font-size: 10px; font-weight: 600; letter-spacing: 1px; text-transform: uppercase; color: #9CA3AF; margin: 16px 0 8px 0; }
.sb-brand { font-size: 15px; font-weight: 600; color: #111827; }
.sb-ver { font-size: 11px; color: #9CA3AF; font-family: 'JetBrains Mono', monospace; }
.status { font-size: 12px; color: #374151; }
.dot { display: inline-block; width: 7px; height: 7px; border-radius: 50%; margin-right: 5px; vertical-align: middle; }
.dot-on { background: #10B981; }
.dot-off { background: #EF4444; }
hr { border: none; border-top: 1px solid #E8EAED; margin: 14px 0; }

.stButton > button { background: #111827 !important; color: #fff !important; border: none !important; border-radius: 5px !important; font-family: 'Inter', sans-serif !important; font-weight: 500 !important; font-size: 13px !important; padding: 9px 0 !important; width: 100% !important; }
.stButton > button:hover { background: #1F2937 !important; }
.stSelectbox label, .stSlider label, .stRadio label, .stMultiSelect label, .stTextInput label { font-size: 11px !important; color: #6B7280 !important; font-weight: 500 !important; }
</style>
""", unsafe_allow_html=True)


def weaviate_ready():
    try:
        return requests.get("http://localhost:8080/v1/.well-known/ready", timeout=2).status_code == 200
    except Exception:
        return False


@st.cache_resource(show_spinner=False)
def get_client():
    try:
        return connect_weaviate()
    except Exception:
        return None


@st.cache_data(ttl=60, show_spinner=False)
def load_jobs(_client):
    try:
        col = _client.collections.get(JOB_COLLECTION_NAME).with_tenant(TENANT_ID_FOR_JOBS)
        return [{"uuid": str(o.uuid), "props": o.properties}
                for o in col.query.fetch_objects(limit=500).objects]
    except Exception:
        return []


@st.cache_data(ttl=60, show_spinner=False)
def load_candidates(_client):
    try:
        col = _client.collections.get(CANDIDATE_COLLECTION_NAME).with_tenant(TENANT_ID_FOR_CV)
        return [{"uuid": str(o.uuid), "props": o.properties}
                for o in col.query.fetch_objects(limit=500).objects]
    except Exception:
        return []


def badge(score):
    p = round(score * 100)
    c = "s-hi" if p >= 70 else ("s-md" if p >= 45 else "s-lo")
    return f'<span class="score-pill {c}">{p}%</span>'


def bar(label, val):
    p = int(val * 100)
    st.markdown(f"""
    <div class="br-row">
        <span class="br-lbl">{label}</span>
        <div class="br-track"><div class="br-fill" style="width:{p}%;"></div></div>
        <span class="br-pct">{p}%</span>
    </div>""", unsafe_allow_html=True)


def tags(items, cls=""):
    if not items:
        return "<span style='color:#9CA3AF;font-size:11px;'>—</span>"
    return "".join(f'<span class="tag {cls}">{i}</span>' for i in (items or [])[:7])


def level(years):
    y = int(years or 0)
    if y <= 2:    return "Junior", y
    elif y <= 4:  return "Medior", y
    elif y <= 6:  return "Confirmé", y
    elif y <= 10: return "Senior", y
    else:         return "Expert", y


with st.sidebar:
    st.markdown("""
    <div style="padding:18px 0 14px;border-bottom:1px solid #E8EAED;margin-bottom:14px;">
        <div class="sb-brand">Nexus — Semantic Talent Matching</div>
        <div class="sb-ver">candidates_for_job </div>
    </div>""", unsafe_allow_html=True)

    st.markdown('<div class="sb-lbl">Recherche</div>', unsafe_allow_html=True)
    mode  = st.radio("Mode", ["hybrid", "vector"],
                     format_func=lambda x: "Hybrid (70% vecteur + 30% BM25)" if x == "hybrid" else "Vector (similarité cosine)")
    limit = st.slider("Résultats", 3, 20, 5)

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<div class="sb-lbl">Filtres</div>', unsafe_allow_html=True)
    f_levels   = st.multiselect("Séniorité", ["Junior", "Medior", "Confirmé", "Senior", "Expert"])
    f_location = st.text_input("Localisation", placeholder="Casablanca, Rabat...")
    f_exp      = st.slider("Expérience (ans)", 0, 20, (0, 20))

    st.markdown("<hr>", unsafe_allow_html=True)
    st.markdown('<div class="sb-lbl">Poids du scoring</div>', unsafe_allow_html=True)
    ws = st.slider("Skills techniques",   0.0, 1.0, WEIGHTS["tech_skills"],  0.01)
    wl = st.slider("Langages",            0.0, 1.0, WEIGHTS["prog_langs"],   0.01)
    wu = st.slider("Résumé",              0.0, 1.0, WEIGHTS["summary"],      0.01)
    wi = st.slider("Industrie",           0.0, 1.0, WEIGHTS["industry"],     0.01)
    wr = st.slider("Rôle",                0.0, 1.0, WEIGHTS["roles_title"],  0.01)
    tot = ws + wl + wu + wi + wr or 1.0
    custom_weights = {
        "tech_skills":  ws / tot,
        "prog_langs":   wl / tot,
        "summary":      wu / tot,
        "industry":     wi / tot,
        "roles_title":  wr / tot,
    }

    st.markdown("<hr>", unsafe_allow_html=True)
    ready = weaviate_ready()
    dot   = "dot-on" if ready else "dot-off"
    msg   = "Weaviate connecté" if ready else "Weaviate hors ligne"
    st.markdown(f'<span class="dot {dot}"></span><span class="status">{msg}</span>',
                unsafe_allow_html=True)

if not weaviate_ready():
    st.error("Weaviate n'est pas accessible. Lance docker-compose up -d puis relance l'application.")
    st.stop()

client = get_client()
if client is None:
    st.error("Connexion à Weaviate impossible.")
    st.stop()

jobs       = load_jobs(client)
candidates = load_candidates(client)

if not jobs:
    st.error("Aucune offre dans Weaviate. Lance insert_data.py.")
    st.stop()
if not candidates:
    st.error("Aucun candidat dans Weaviate. Lance insert_data.py.")
    st.stop()

st.markdown("""
<div class="header">
    <div class="header-title">Matching Sémantique — Candidats pour un Poste</div>
    <div class="header-sub">Sélectionnez une offre pour identifier les profils les plus compatibles</div>
</div>""", unsafe_allow_html=True)

c1, c2, c3, c4 = st.columns(4)
for col, val, lbl in [
    (c1, len(candidates), "Candidats"),
    (c2, len(jobs),       "Offres"),
    (c3, len(candidates) * 8 + len(jobs) * 6, "Vecteurs"),
    (c4, mode.upper(),    "Mode actif"),
]:
    with col:
        st.markdown(f"""
        <div class="kpi">
            <div class="kpi-val">{val}</div>
            <div class="kpi-lbl">{lbl}</div>
        </div>""", unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

job_map = {}
for j in sorted(jobs, key=lambda x: x["props"].get("title", "")):
    p = j["props"]
    key = f"{p.get('title','N/A')}  —  {p.get('company','N/A')}  |  {p.get('experience_level','')}  |  {p.get('location','')}"
    job_map[key] = j

selected_key  = st.selectbox("Offre d'emploi", list(job_map.keys()), label_visibility="collapsed")
selected      = job_map[selected_key]
selected_uuid = selected["uuid"]
jp            = selected["props"]

st.markdown(f"""
<div class="job-card">
    <div class="job-name">{jp.get('title','N/A')}</div>
    <div class="job-meta">
        <b>{jp.get('company','')}</b> &nbsp;·&nbsp; {jp.get('location','')} &nbsp;·&nbsp;
        {jp.get('industry','')} &nbsp;·&nbsp; {jp.get('employment_type','')} &nbsp;·&nbsp;
        Niveau <b>{jp.get('experience_level','')}</b> &nbsp;·&nbsp;
        <b>{jp.get('years_of_experience_required',0)} ans</b> min.
        {'&nbsp;·&nbsp; ' + str(jp.get('salary_range','')) if jp.get('salary_range') else ''}
    </div>
    <div class="lbl">Compétences requises</div>
    <div style="margin-bottom:10px;">{tags(jp.get('technical_skills',[]))}</div>
    <div class="lbl">Langages requis</div>
    <div style="margin-bottom:10px;">{tags(jp.get('programming_languages',[]), 'tag-l')}</div>
    {'<div class="lbl">Certifications</div><div>' + tags(jp.get('certifications',[]), 'tag-c') + '</div>' if jp.get('certifications') else ''}
</div>""", unsafe_allow_html=True)

run = st.button("Lancer la recherche de candidats")
st.markdown("<br>", unsafe_allow_html=True)

if run:
    with st.spinner("Analyse sémantique en cours..."):
        raw_results = candidates_for_job(
            client=client,
            job_uuid=selected_uuid,
            limit=limit * 3,
            mode=mode,
        )

    filtered = []
    for r in raw_results:
        p   = r.properties
        exp = int(p.get("years_of_experience", 0) or 0)
        lvl, _ = level(exp)

        if exp < f_exp[0] or exp > f_exp[1]:
            continue
        if f_levels and lvl not in f_levels:
            continue
        if f_location and f_location.lower() not in (p.get("location", "") or "").lower():
            continue

        filtered.append(r)
        if len(filtered) >= limit:
            break

    if not filtered:
        st.markdown("""
        <div class="empty">
            <div class="empty-title">Aucun candidat trouvé</div>
            <div>Essayez d'élargir les filtres ou de changer le mode de recherche.</div>
        </div>""", unsafe_allow_html=True)
    else:
        st.markdown(
            f'<div class="lbl">{len(filtered)} candidat(s) — mode {mode.upper()}</div>',
            unsafe_allow_html=True
        )
        st.markdown("<hr>", unsafe_allow_html=True)

        for i, r in enumerate(filtered):
            p    = r.properties
            ind  = r.individual_scores or {}
            exp  = int(p.get("years_of_experience", 0) or 0)
            lvl, yrs = level(exp)

            roles  = p.get("roles_held", []) or []
            skills = p.get("technical_skills", []) or []
            langs  = p.get("programming_languages", []) or []
            certs  = p.get("certifications", []) or []

            traj_dir   = p.get("career_trajectory_direction", "") or ""
            traj_pred  = p.get("career_trajectory_predicted_profile", "") or ""
            traj_speed = p.get("career_trajectory_progression_speed", "") or ""
            companies  = p.get("timeline_companies", []) or []
            tl_roles   = p.get("timeline_roles", []) or []

            col_l, col_r = st.columns([3, 2])

            with col_l:
                st.markdown(f"""
                <div class="cand-card">
                    <div class="cand-header">
                        <div>
                            <div class="cand-rank">Rang #{i+1}</div>
                            <div class="cand-name">{p.get('full_name','N/A')}</div>
                            <div class="cand-role">{" · ".join(roles[:2]) if roles else "—"}</div>
                        </div>
                        {badge(r.score)}
                    </div>
                    <div class="cand-meta">
                        <span>{p.get('location','N/A')}</span>
                        <span>{yrs} ans · {lvl}</span>
                        <span>{p.get('education_level','N/A')}</span>
                        <span>{p.get('email','')}</span>
                    </div>
                    <hr class="cand-divider">
                    <div class="lbl">Compétences</div>
                    <div style="margin-bottom:8px;">{tags(skills)}</div>
                    <div class="lbl">Langages</div>
                    <div style="margin-bottom:8px;">{tags(langs, 'tag-l')}</div>
                    {'<div class="lbl">Certifications</div><div>' + tags(certs, 'tag-c') + '</div>' if certs else ''}
                </div>""", unsafe_allow_html=True)

                if traj_dir:
                    speed_label = f" · <b>{traj_speed}</b>" if traj_speed else ""
                    st.markdown(f"""
                    <div class="traj">
                        <div class="traj-lbl">Trajectoire de carrière{speed_label}</div>
                        <div>{traj_dir}</div>
                        {'<div class="traj-pred">Prédiction 12-18 mois : ' + traj_pred + '</div>' if traj_pred else ''}
                    </div>""", unsafe_allow_html=True)

                if companies:
                    tl = " &rarr; ".join(
                        f"<b>{r2}</b> @ {c}"
                        for r2, c in zip(tl_roles, companies)
                    )
                    st.markdown(f'<div class="timeline">{tl}</div>', unsafe_allow_html=True)

            with col_r:
                st.markdown('<div class="breakdown"><div class="lbl" style="margin-bottom:10px;">Score détaillé</div>',
                            unsafe_allow_html=True)
                bar("Skills",    ind.get("tech_skills", 0))
                bar("Langages",  ind.get("prog_langs", 0))
                bar("Résumé",    ind.get("summary", 0))
                bar("Industrie", ind.get("industry", 0))
                bar("Rôle",      ind.get("roles_title", 0))
                st.markdown("</div>", unsafe_allow_html=True)

            st.markdown("<hr>", unsafe_allow_html=True)

else:
    st.markdown("""
    <div class="empty">
        <div class="empty-title">Sélectionnez une offre et lancez la recherche</div>
        <div>Choisissez un poste, ajustez les filtres dans la barre latérale, puis cliquez sur le bouton.</div>
    </div>""", unsafe_allow_html=True)