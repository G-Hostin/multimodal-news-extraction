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
NOMS_SOURCES = {"newsdata": "NewsData.io", "rss": "Flux RSS", "legorafi": "Le Gorafi"}
NOMS_TACHES = {
    "extract_newsdata": "NewsData.io",
    "extract_rss": "Flux RSS",
    "extract_legorafi": "Le Gorafi",
    "transform": "Transformation",
    "load": "Chargement",
}


# ---------- Chargement des données ----------

# Connexion avec l'utilisateur analyst_reader (lecture seule), paramètres dans .streamlit/secrets.toml
conn = st.connection("checkit_db", type="sql")

metrics = conn.query("SELECT * FROM pipeline_metrics ORDER BY date_debut", ttl="5m")
articles = conn.query("SELECT nom_source, a_image FROM articles", ttl="5m")
taille_base = conn.query("SELECT pg_database_size('checkit') AS taille", ttl="5m")["taille"].iloc[0]

if metrics.empty:
    st.warning("Aucune exécution enregistrée pour l'instant. Lancez le DAG dans Airflow.")
    st.stop()

# Heures affichées à l'heure française
metrics["date_debut"] = metrics["date_debut"].dt.tz_convert("Europe/Paris")
metrics["date_fin"] = metrics["date_debut"] + pd.to_timedelta(metrics["duree_secondes"], unit="s")

# Une ligne par exécution du DAG
executions = metrics.groupby("run_id").agg(debut=("date_debut", "min"), fin=("date_fin", "max"))
executions["duree"] = (executions["fin"] - executions["debut"]).dt.total_seconds()
executions = executions.sort_values("debut")
derniere = executions.index[-1]

extractions = metrics[metrics["tache"].str.startswith("extract_")]
extractions_derniere = extractions[extractions["run_id"] == derniere]

# ---------- Calcul des KPI ----------

pct_valides = 100 * extractions_derniere["nb_sortie"].sum() / extractions_derniere["nb_entree"].sum()
pct_image = 100 * articles["a_image"].mean()
duree_derniere = executions.loc[derniere, "duree"]
aujourd_hui = pd.Timestamp.now(tz="Europe/Paris").date()
credits_jour = metrics.loc[metrics["date_debut"].dt.date == aujourd_hui, "credits_api"].sum()
heures_depuis = (pd.Timestamp.now(tz="Europe/Paris") - executions.loc[derniere, "fin"]).total_seconds() / 3600
taille_images = sum(f.stat().st_size for f in config.IMAGES_DIR.glob("*")) / 1e6

# ---------- En-tête et chiffres clés ----------

st.title("Pipeline d'articles CheckIt.AI")
st.caption(f"Dernière exécution le {executions.loc[derniere, 'debut']:%d/%m/%Y à %H:%M}. "
           "Le pipeline récupère chaque jour des articles d'actualité (texte et image) et les range dans la base.")

col1, col2, col3, col4, col5 = st.columns(5)
col1.metric("Articles en base", len(articles), border=True)
col2.metric("Articles valides", f"{pct_valides:.0f} %", border=True,
            help="Part des articles récupérés qui ont un titre et un texte (dernière exécution)")
col3.metric("Articles avec image", f"{pct_image:.0f} %", border=True,
            help="Part des articles en base qui ont une image exploitable")
col4.metric("Durée de la dernière exécution", f"{duree_derniere:.0f} s", border=True)
col5.metric("Crédits NewsData aujourd'hui", f"{credits_jour:.0f} / {config.NEWSDATA_QUOTA_JOUR}", border=True)

# ---------- État du pipeline : mêmes seuils que le plan de monitoring ----------

st.subheader("État du pipeline")

sources_manquantes = {"extract_newsdata", "extract_rss", "extract_legorafi"} - set(extractions_derniere["tache"])
if sources_manquantes:
    noms = ", ".join(NOMS_TACHES[tache] for tache in sorted(sources_manquantes))
    st.error(f"Extraction en échec lors de la dernière exécution : {noms}")
if heures_depuis > config.SEUIL_FRAICHEUR_HEURES:
    st.error(f"Le pipeline n'a pas tourné depuis {heures_depuis:.0f} heures")

alertes = []
for _, ligne in extractions_derniere.iterrows():
    pct = 100 * ligne["nb_sortie"] / ligne["nb_entree"] if ligne["nb_entree"] else 0
    if pct < config.SEUIL_ARTICLES_VALIDES:
        alertes.append(f"{NOMS_SOURCES[ligne['source']]} : seulement {pct:.0f} % d'articles valides "
                       f"(seuil : {config.SEUIL_ARTICLES_VALIDES} %)")
if pct_image < config.SEUIL_ARTICLES_AVEC_IMAGE:
    alertes.append(f"Seulement {pct_image:.0f} % d'articles avec image (seuil : {config.SEUIL_ARTICLES_AVEC_IMAGE} %)")
if duree_derniere > config.SEUIL_DUREE_EXECUTION:
    alertes.append(f"Exécution lente : {duree_derniere:.0f} s (seuil : {config.SEUIL_DUREE_EXECUTION} s)")
if credits_jour > config.SEUIL_CREDITS_JOUR:
    alertes.append(f"{credits_jour:.0f} crédits NewsData utilisés aujourd'hui (seuil : {config.SEUIL_CREDITS_JOUR})")

for alerte in alertes:
    st.warning(alerte)
if not alertes and not sources_manquantes and heures_depuis <= config.SEUIL_FRAICHEUR_HEURES:
    st.success("Tout fonctionne normalement : aucune alerte sur la dernière exécution.")

# ---------- Qualité ----------

st.header("Qualité des données")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Articles gardés par source")
    st.caption("Sur la dernière exécution, part des articles récupérés qui ont un titre et un texte.")
    valides = extractions_derniere.set_index(extractions_derniere["source"].map(NOMS_SOURCES))
    valides = (100 * valides["nb_sortie"] / valides["nb_entree"]).astype(float).rename("Articles gardés")
    st.bar_chart(valides, x_label="Source", y_label="Articles gardés (%)")

with col2:
    st.subheader("Articles avec et sans image")
    st.caption("Tous les articles de la base, par source.")
    images = pd.crosstab(articles["nom_source"], articles["a_image"].map({True: "Avec image", False: "Sans image"}))
    st.bar_chart(images, x_label="Source", y_label="Nombre d'articles")

# ---------- Rapidité ----------

st.header("Rapidité")
col1, col2 = st.columns(2)

with col1:
    st.subheader("Durée de chaque exécution")
    st.caption(f"Temps total du pipeline, en secondes. Seuil d'alerte : {config.SEUIL_DUREE_EXECUTION} s.")
    durees = executions.set_index(executions["debut"].dt.strftime("%Y-%m-%d %H:%M"))["duree"].rename("Durée")
    st.line_chart(durees, x_label="Exécution", y_label="Durée (s)")

with col2:
    st.subheader("Durée moyenne par tâche")
    st.caption("Le Gorafi est le plus lent à cause de la pause de 2 secondes entre deux pages.")
    duree_taches = metrics.groupby(metrics["tache"].map(NOMS_TACHES))["duree_secondes"].mean().rename("Durée moyenne")
    # Avec horizontal=True, x_label correspond à l'axe des catégories (vertical)
    st.bar_chart(duree_taches, x_label="Tâche", y_label="Durée moyenne (s)", horizontal=True)

# ---------- Coût ----------

st.header("Coût et ressources")
col1, col2, col3 = st.columns(3)
col1.metric("Crédits NewsData utilisés aujourd'hui", f"{credits_jour:.0f}",
            help=f"Quota gratuit : {config.NEWSDATA_QUOTA_JOUR} crédits par jour, 5 par exécution")
col2.metric("Espace disque des images", f"{taille_images:.0f} Mo")
col3.metric("Taille de la base", f"{taille_base / 1e6:.0f} Mo")

st.subheader("Crédits NewsData par jour")
credits_par_jour = metrics.groupby(metrics["date_debut"].dt.strftime("%Y-%m-%d"))["credits_api"].sum().rename("Crédits")
st.bar_chart(credits_par_jour, x_label="Jour", y_label="Crédits utilisés")

# ---------- Détail ----------

with st.expander("Voir le détail des dernières tâches"):
    st.dataframe(metrics.sort_values("date_debut", ascending=False).head(30), hide_index=True)
