"""DAG ETL : extraction des articles (NewsData, RSS, Le Gorafi), transformation, chargement dans PostgreSQL."""

import logging
from datetime import timedelta

import pandas as pd
import pendulum
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG

# Scripts des étapes 2 et 3 (dossier src/, ajouté au PYTHONPATH dans docker-compose.yaml)
import config
from extract_gorafi import extract_gorafi
from extract_newsdata import extract_newsdata
from extract_rss import extract_rss
from transform import transforme
from utils import add_images, clean_articles, save_articles

logger = logging.getLogger(__name__)

EXTRACTORS = {
    "newsdata": extract_newsdata,
    "rss": extract_rss,
    "legorafi": extract_gorafi,
}

METRICS_COLUMNS = [
    "run_id", "tache", "source", "date_debut", "duree_secondes",
    "nb_entree", "nb_sortie", "nb_avec_image", "nb_images_partagees", "credits_api",
]


def save_metrics(run_id, tache, debut, source=None, nb_entree=None, nb_sortie=None,
                 nb_avec_image=None, nb_images_partagees=None, credits_api=None):
    """Enregistre les chiffres d'une tâche dans la table pipeline_metrics (pour le tableau de bord)."""
    duree = (pendulum.now("UTC") - debut).total_seconds()
    row = (run_id, tache, source, debut, duree, nb_entree, nb_sortie,
           nb_avec_image, nb_images_partagees, credits_api)
    hook = PostgresHook(postgres_conn_id="checkit_db")
    hook.insert_rows(table="pipeline_metrics", rows=[row], target_fields=METRICS_COLUMNS)


def extract(source_name, **context):
    """Extrait une source, nettoie les articles, télécharge leurs images et les sauvegarde."""
    debut = pendulum.now("UTC")
    articles_bruts = EXTRACTORS[source_name]()
    articles = clean_articles(articles_bruts)
    articles = add_images(articles)
    save_articles(articles, source_name)

    nb_avec_image = sum(1 for article in articles if article["image_path"])
    # NewsData : 1 crédit par page demandée
    credits = config.NEWSDATA_MAX_PAGES if source_name == "newsdata" else None
    save_metrics(context["run_id"], f"extract_{source_name}", debut, source=source_name,
                 nb_entree=len(articles_bruts), nb_sortie=len(articles),
                 nb_avec_image=nb_avec_image, credits_api=credits)
    return len(articles)


def transform(**context):
    """Lance le pipeline de transformation de l'étape 3."""
    debut = pendulum.now("UTC")
    df = transforme()
    save_metrics(context["run_id"], "transform", debut, nb_sortie=len(df),
                 nb_avec_image=int(df["a_image"].sum()),
                 nb_images_partagees=int(df["image_partagee"].sum()))
    return len(df)


def load(**context):
    """Charge le fichier Parquet dans la table articles (ajout ou mise à jour selon l'id)."""
    debut = pendulum.now("UTC")
    df = pd.read_parquet(config.PROCESSED_DIR / "articles.parquet")
    # Les valeurs manquantes de pandas deviennent NULL en base
    rows = df.astype(object).where(df.notna(), None).values.tolist()

    hook = PostgresHook(postgres_conn_id="checkit_db")
    hook.insert_rows(
        table="articles",
        rows=rows,
        target_fields=list(df.columns),
        replace=True,  # INSERT ... ON CONFLICT (id) DO UPDATE
        replace_index="id",
    )

    total = hook.get_first("SELECT COUNT(*) FROM articles")[0]
    logger.info("Chargement : %d articles envoyés, %d articles au total en base", len(rows), total)
    save_metrics(context["run_id"], "load", debut, nb_entree=len(rows), nb_sortie=total)
    return total


def controle_qualite(**context):
    """Compare les chiffres de l'exécution aux seuils d'alerte. Échoue si un problème est critique."""
    hook = PostgresHook(postgres_conn_id="checkit_db")
    records = hook.get_records(
        "SELECT tache, nb_entree, nb_sortie, nb_avec_image, date_debut "
        "FROM pipeline_metrics WHERE run_id = %s",
        parameters=(context["run_id"],),
    )
    metrics = {tache: {"entree": entree, "sortie": sortie, "avec_image": avec_image, "debut": debut}
               for tache, entree, sortie, avec_image, debut in records}
    critiques = []
    nb_alertes = 0

    # Une extraction sans chiffres veut dire que la tâche a échoué
    for source_name in EXTRACTORS:
        tache = f"extract_{source_name}"
        if tache not in metrics:
            critiques.append(f"{tache} a échoué")
            continue
        entree, sortie = metrics[tache]["entree"], metrics[tache]["sortie"]
        if entree and 100 * sortie / entree < config.SEUIL_ARTICLES_VALIDES:
            logger.warning("Alerte : %s n'a gardé que %d %% des articles", tache, 100 * sortie / entree)
            nb_alertes += 1

    if metrics.get("load", {}).get("entree", 0) == 0:
        critiques.append("aucun article chargé en base")

    transform_metrics = metrics.get("transform")
    if transform_metrics and transform_metrics["sortie"]:
        pourcentage_image = 100 * transform_metrics["avec_image"] / transform_metrics["sortie"]
        if pourcentage_image < config.SEUIL_ARTICLES_AVEC_IMAGE:
            logger.warning("Alerte : seulement %d %% des articles ont une image", pourcentage_image)
            nb_alertes += 1

    if metrics:
        duree = (pendulum.now("UTC") - min(m["debut"] for m in metrics.values())).total_seconds()
        if duree > config.SEUIL_DUREE_EXECUTION:
            logger.warning("Alerte : l'exécution a duré %d secondes", duree)
            nb_alertes += 1

    credits_jour = hook.get_first(
        "SELECT COALESCE(SUM(credits_api), 0) FROM pipeline_metrics WHERE date_debut::date = CURRENT_DATE"
    )[0]
    if credits_jour > config.SEUIL_CREDITS_JOUR:
        logger.warning("Alerte : %d crédits NewsData utilisés aujourd'hui (quota : %d)",
                       credits_jour, config.NEWSDATA_QUOTA_JOUR)
        nb_alertes += 1

    if critiques:
        raise ValueError("Problème critique : " + ", ".join(critiques))
    logger.info("Contrôle qualité : aucun problème critique, %d alerte(s)", nb_alertes)


with DAG(
    dag_id="etl_multimodal_news",
    description="Extraction, transformation et chargement d'articles d'actualité (texte et image)",
    start_date=pendulum.datetime(2026, 9, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["checkit", "etl"],
) as dag:

    extract_newsdata_task = PythonOperator(
        task_id="extract_newsdata",
        python_callable=extract,
        op_kwargs={"source_name": "newsdata"},
    )
    extract_rss_task = PythonOperator(
        task_id="extract_rss",
        python_callable=extract,
        op_kwargs={"source_name": "rss"},
    )
    extract_gorafi_task = PythonOperator(
        task_id="extract_legorafi",
        python_callable=extract,
        op_kwargs={"source_name": "legorafi"},
    )
    # all_done : la transformation tourne même si une extraction a échoué (avec les sources qui ont marché)
    transform_task = PythonOperator(task_id="transform", python_callable=transform, trigger_rule="all_done")
    load_task = PythonOperator(task_id="load", python_callable=load)
    # Pas de nouvel essai : relancer le contrôle ne changerait pas le résultat
    controle_task = PythonOperator(task_id="controle_qualite", python_callable=controle_qualite, retries=0)

    # Les 3 extractions tournent en parallèle, puis la transformation, le chargement et le contrôle
    [extract_newsdata_task, extract_rss_task, extract_gorafi_task] >> transform_task >> load_task >> controle_task
