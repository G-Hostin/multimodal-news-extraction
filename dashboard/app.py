"""Tableau de bord des KPI du pipeline ETL. Lancement : uv run streamlit run dashboard/app.py"""

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# Les seuils d'alerte sont dans src/config.py, partagés avec la tâche controle_qualite du DAG
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT_DIR / "src"))
import config  # noqa: E402

st.set_page_config(page_title="Pipeline CheckIt.AI", layout="wide")

# Noms lisibles pour un public non technique
NOMS_CANAUX = {"newsdata": "NewsData.io", "rss": "Flux RSS", "legorafi": "Le Gorafi"}
NOMS_TACHES = {
    "extract_newsdata": "Collecte NewsData",
    "extract_rss": "Collecte flux RSS",
    "extract_legorafi": "Collecte Le Gorafi",
    "transform": "Nettoyage",
    "load": "Rangement en base",
}
PERIODES = {"7 derniers jours": 7, "30 derniers jours": 30, "Tout l'historique": None}


# ---------- Chargement des données ----------

# Connexion avec l'utilisateur analyst_reader (lecture seule), paramètres dans .streamlit/secrets.toml
conn = st.connection("checkit_db", type="sql")

metrics = conn.query("SELECT * FROM pipeline_metrics ORDER BY date_debut", ttl="5m")
articles = conn.query("SELECT source, nom_source, a_image, date_extraction FROM articles", ttl="5m")
taille_base = conn.query("SELECT pg_database_size('checkit') AS taille", ttl="5m")["taille"].iloc[0]

if metrics.empty:
    st.warning("Aucune exécution enregistrée pour l'instant. Lancez le DAG dans Airflow.")
    st.stop()

# Heures affichées à l'heure française
maintenant = pd.Timestamp.now(tz="Europe/Paris")
metrics["date_debut"] = metrics["date_debut"].dt.tz_convert("Europe/Paris")
metrics["date_fin"] = metrics["date_debut"] + pd.to_timedelta(metrics["duree_secondes"], unit="s")
articles["date_extraction"] = articles["date_extraction"].dt.tz_convert("Europe/Paris")
# Canal de collecte de chaque article : les 4 flux RSS sont regroupés
articles["canal"] = articles["source"].where(articles["source"].isin(["newsdata", "legorafi"]), "rss")


def resume_executions(metrics):
    """Une ligne par exécution du DAG : début, durée totale et nombre d'articles."""
    extractions = metrics[metrics["tache"].str.startswith("extract_")]
    executions = metrics.groupby("run_id").agg(debut=("date_debut", "min"), fin=("date_fin", "max"))
    executions["duree"] = (executions["fin"] - executions["debut"]).dt.total_seconds()
    executions["recuperes"] = extractions.groupby("run_id")["nb_entree"].sum()
    executions["gardes"] = extractions.groupby("run_id")["nb_sortie"].sum()
    executions["pct_valides"] = 100 * executions["gardes"] / executions["recuperes"]
    # Total en base après le chargement : la différence avec l'exécution précédente
    # donne le nombre de nouveaux articles (les autres étaient déjà en base)
    executions["total_base"] = metrics[metrics["tache"] == "load"].groupby("run_id")["nb_sortie"].max()
    executions = executions.sort_values("debut")
    executions["nouveaux"] = executions["total_base"].diff()
    return executions


executions = resume_executions(metrics)
derniere = executions.index[-1]
derniere_exec = executions.loc[derniere]
extractions_derniere = metrics[(metrics["run_id"] == derniere) & metrics["tache"].str.startswith("extract_")]
credits_jour = metrics.loc[metrics["date_debut"].dt.date == maintenant.date(), "credits_api"].sum()
heures_depuis = (maintenant - derniere_exec["fin"]).total_seconds() / 3600
pct_image = 100 * articles["a_image"].mean()


# ---------- En-tête ----------

st.title("Suivi du pipeline d'articles")
st.markdown(
    "Chaque nuit, le pipeline récupère des articles d'actualité (texte et image) pour le futur "
    "détecteur de fake news de CheckIt.AI, les nettoie et les range dans la base. "
    "Cette page indique si tout s'est bien passé."
)
st.caption(f"Dernière exécution : {derniere_exec['debut']:%d/%m/%Y à %H:%M}")
# Les requêtes sont gardées en mémoire 5 minutes : le bouton vide ce cache et relit la base
st.button("Actualiser les données", icon=":material/refresh:", on_click=st.cache_data.clear)


# ---------- 1. État du pipeline (dernière exécution) ----------

st.header("1. Est-ce que tout va bien ?")

problemes = []  # rouge
alertes = []    # orange
taches_manquantes = {"extract_newsdata", "extract_rss", "extract_legorafi"} - set(extractions_derniere["tache"])
for tache in sorted(taches_manquantes):
    problemes.append(f"Échec de l'étape « {NOMS_TACHES[tache]} » lors de la dernière exécution.")
if heures_depuis > config.SEUIL_FRAICHEUR_HEURES:
    problemes.append(f"Le pipeline n'a pas tourné depuis {heures_depuis:.0f} heures.")
for _, ligne in extractions_derniere.iterrows():
    pct = 100 * ligne["nb_sortie"] / ligne["nb_entree"] if ligne["nb_entree"] else 0
    if pct < config.SEUIL_ARTICLES_VALIDES:
        alertes.append(f"{NOMS_CANAUX[ligne['source']]} : seulement {pct:.0f} % d'articles utilisables "
                       f"(minimum attendu : {config.SEUIL_ARTICLES_VALIDES} %).")
if pct_image < config.SEUIL_ARTICLES_AVEC_IMAGE:
    alertes.append(f"Seulement {pct_image:.0f} % des articles ont une image "
                   f"(minimum attendu : {config.SEUIL_ARTICLES_AVEC_IMAGE} %).")
if derniere_exec["duree"] > config.SEUIL_DUREE_EXECUTION:
    alertes.append(f"La dernière exécution a pris {derniere_exec['duree']:.0f} secondes "
                   f"(maximum attendu : {config.SEUIL_DUREE_EXECUTION} s).")
if credits_jour > config.SEUIL_CREDITS_JOUR:
    alertes.append(f"{credits_jour:.0f} crédits NewsData utilisés aujourd'hui "
                   f"(maximum conseillé : {config.SEUIL_CREDITS_JOUR} sur {config.NEWSDATA_QUOTA_JOUR}).")

for probleme in problemes:
    st.error(probleme, icon=":material/error:")
for alerte in alertes:
    st.warning(alerte, icon=":material/warning:")
if not problemes and not alertes:
    st.success("Tout va bien : la dernière exécution s'est déroulée normalement.", icon=":material/check_circle:")

nouveaux = derniere_exec["nouveaux"]
col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Articles collectés", f"{derniere_exec['gardes']:.0f}", border=True,
            help="Articles utilisables récupérés lors de la dernière exécution")
col2.metric("Nouveaux articles", "–" if pd.isna(nouveaux) else f"{nouveaux:.0f}", border=True,
            help="Articles qui n'étaient pas encore en base. Les autres avaient déjà été collectés avant")
col3.metric("Articles utilisables", f"{derniere_exec['pct_valides']:.0f} %", border=True,
            help="Part des articles récupérés qui ont un titre et un texte")
col4.metric("Durée", f"{derniere_exec['duree']:.0f} s", border=True,
            help="Temps total de la dernière exécution")
col5.metric("Crédits NewsData aujourd'hui", f"{credits_jour:.0f} / {config.NEWSDATA_QUOTA_JOUR}", border=True,
            help="Quota de l'offre gratuite, 5 crédits par exécution")


# ---------- 2. Historique, avec filtres ----------

st.header("2. Historique")
col_periode, col_canaux = st.columns(2)
periode = col_periode.selectbox("Période", list(PERIODES))
canaux = col_canaux.multiselect("Sources", list(NOMS_CANAUX), default=list(NOMS_CANAUX),
                                format_func=NOMS_CANAUX.get)

# Application des filtres
nb_jours = PERIODES[periode]
debut_periode = maintenant - pd.Timedelta(days=nb_jours) if nb_jours else metrics["date_debut"].min()
metrics_f = metrics[metrics["date_debut"] >= debut_periode]
executions_f = executions[executions["debut"] >= debut_periode]
extractions_f = metrics_f[metrics_f["tache"].str.startswith("extract_") & metrics_f["source"].isin(canaux)]
articles_f = articles[(articles["date_extraction"] >= debut_periode) & articles["canal"].isin(canaux)]

if extractions_f.empty:
    st.info("Aucune donnée pour ces filtres.")
    st.stop()

onglet_qualite, onglet_rapidite, onglet_cout, onglet_detail = st.tabs(
    ["Qualité des données", "Rapidité", "Coût et ressources", "Détail des exécutions"]
)

with onglet_qualite:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Articles utilisables par source")
        st.caption(f"Part des articles récupérés qui ont un titre et un texte, en moyenne sur la période. "
                   f"En dessous de {config.SEUIL_ARTICLES_VALIDES} %, une alerte est affichée.")
        par_canal = extractions_f.groupby(extractions_f["source"].map(NOMS_CANAUX))[["nb_entree", "nb_sortie"]].sum()
        pct_par_canal = (100 * par_canal["nb_sortie"] / par_canal["nb_entree"]).rename("Articles utilisables")
        st.bar_chart(pct_par_canal, x_label="Source", y_label="Articles utilisables (%)")
    with col2:
        st.subheader("Articles avec et sans image")
        st.caption("Nombre d'articles collectés sur la période, selon qu'ils ont une image ou non.")
        images = pd.crosstab(articles_f["canal"].map(NOMS_CANAUX),
                             articles_f["a_image"].map({True: "Avec image", False: "Sans image"}))
        st.bar_chart(images, x_label="Source", y_label="Nombre d'articles")

with onglet_rapidite:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("Durée de chaque exécution")
        st.caption(f"Temps total du pipeline, en secondes. Au-delà de {config.SEUIL_DUREE_EXECUTION} s, "
                   "une alerte est affichée.")
        durees = executions_f.set_index(executions_f["debut"].dt.strftime("%d/%m %H:%M"))["duree"].rename("Durée")
        st.bar_chart(durees, x_label="Exécution", y_label="Durée (s)")
    with col2:
        st.subheader("Temps passé par étape")
        st.caption("Durée moyenne de chaque étape. La collecte du Gorafi est la plus lente "
                   "car le script attend 2 secondes entre deux pages.")
        etapes = metrics_f[metrics_f["tache"].isin(NOMS_TACHES)]
        duree_etapes = etapes.groupby(etapes["tache"].map(NOMS_TACHES))["duree_secondes"].mean().rename("Durée")
        st.bar_chart(duree_etapes, x_label="Étape", y_label="Durée moyenne (s)", horizontal=True)

with onglet_cout:
    taille_images = sum(f.stat().st_size for f in config.IMAGES_DIR.glob("*")) / 1e6
    col1, col2, col3 = st.columns(3)
    col1.metric("Crédits NewsData aujourd'hui", f"{credits_jour:.0f} / {config.NEWSDATA_QUOTA_JOUR}", border=True)
    col2.metric("Espace disque des images", f"{taille_images:.0f} Mo", border=True)
    col3.metric("Taille de la base", f"{taille_base / 1e6:.0f} Mo", border=True)

    st.subheader("Crédits NewsData utilisés par jour")
    st.caption(f"L'offre gratuite donne {config.NEWSDATA_QUOTA_JOUR} crédits par jour. "
               f"Au-delà de {config.SEUIL_CREDITS_JOUR}, une alerte est affichée.")
    credits_par_jour = metrics_f.groupby(metrics_f["date_debut"].dt.strftime("%d/%m"))["credits_api"].sum()
    st.bar_chart(credits_par_jour.rename("Crédits"), x_label="Jour", y_label="Crédits utilisés")

with onglet_detail:
    st.caption("Une ligne par exécution du pipeline, de la plus récente à la plus ancienne.")
    tableau = executions_f.sort_values("debut", ascending=False).reset_index(drop=True)
    tableau = pd.DataFrame({
        "Date": tableau["debut"].dt.strftime("%d/%m/%Y %H:%M"),
        "Articles récupérés": tableau["recuperes"],
        "Articles utilisables": tableau["gardes"],
        # Pas de valeur pour la première exécution enregistrée (rien à comparer)
        "Nouveaux en base": tableau["nouveaux"].map(lambda v: "–" if pd.isna(v) else f"{v:.0f}"),
        "Utilisables (%)": tableau["pct_valides"].round(0),
        "Durée (s)": tableau["duree"].round(0),
    })
    st.dataframe(tableau, hide_index=True, width="stretch")
