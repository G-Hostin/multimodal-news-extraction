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


def extract(source_name):
    """Extrait une source, garde les articles complets avec une image valide, et les sauvegarde."""
    articles = EXTRACTORS[source_name]()
    articles = clean_articles(articles)
    articles = add_images(articles)
    save_articles(articles, source_name)
    return len(articles)


def transform():
    """Lance le pipeline de transformation de l'étape 3."""
    df = transforme()
    return len(df)


def load():
    """Charge le fichier Parquet dans la table articles (ajout ou mise à jour selon l'id)."""
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
    return total


with DAG(
    dag_id="etl_multimodal_news",
    description="Extraction, transformation et chargement d'articles avec texte et image",
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
    transform_task = PythonOperator(task_id="transform", python_callable=transform)
    load_task = PythonOperator(task_id="load", python_callable=load)

    # Les 3 extractions tournent en parallèle, puis la transformation, puis le chargement
    [extract_newsdata_task, extract_rss_task, extract_gorafi_task] >> transform_task >> load_task
