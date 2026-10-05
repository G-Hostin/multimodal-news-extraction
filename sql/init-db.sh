#!/bin/bash
# Exécuté une seule fois, au premier démarrage du conteneur checkit-db
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Personne n'a de droits sur la base sans qu'on les lui donne
    REVOKE ALL ON DATABASE $POSTGRES_DB FROM PUBLIC;
    REVOKE ALL ON SCHEMA public FROM PUBLIC;

    CREATE TABLE articles (
        id                TEXT PRIMARY KEY,
        url               TEXT NOT NULL,
        titre             TEXT NOT NULL,
        texte             TEXT NOT NULL,
        nb_mots_titre     INTEGER,
        nb_mots_texte     INTEGER,
        date_publication  TIMESTAMPTZ NOT NULL,
        langue            VARCHAR(2),
        auteur            TEXT,
        label             VARCHAR(20),
        source            VARCHAR(30),
        nom_source        TEXT,
        type_source       VARCHAR(10),
        est_fact_checking BOOLEAN,
        domaine           TEXT,
        rang_fiabilite    INTEGER,
        a_image           BOOLEAN NOT NULL,
        image_partagee    BOOLEAN NOT NULL,
        image_url         TEXT,
        image_path        TEXT,
        image_largeur     INTEGER,
        image_hauteur     INTEGER,
        image_format      VARCHAR(10),
        date_extraction   TIMESTAMPTZ
    );

    -- Une ligne par tâche et par exécution du DAG, pour le tableau de bord
    CREATE TABLE pipeline_metrics (
        id              SERIAL PRIMARY KEY,
        run_id          TEXT NOT NULL,
        tache           TEXT NOT NULL,
        source          TEXT,
        date_debut      TIMESTAMPTZ NOT NULL,
        duree_secondes  REAL NOT NULL,
        nb_entree       INTEGER,
        nb_sortie       INTEGER,
        nb_avec_image   INTEGER,
        nb_images_partagees INTEGER,
        credits_api     INTEGER
    );

    -- Utilisé par le DAG : lire, ajouter et mettre à jour les articles
    CREATE ROLE etl_writer LOGIN PASSWORD '$ETL_WRITER_PASSWORD';
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO etl_writer;
    GRANT USAGE ON SCHEMA public TO etl_writer;
    GRANT SELECT, INSERT, UPDATE ON articles TO etl_writer;
    GRANT SELECT, INSERT ON pipeline_metrics TO etl_writer;
    GRANT USAGE ON SEQUENCE pipeline_metrics_id_seq TO etl_writer;

    -- Utilisé pour l'analyse : lecture seule
    CREATE ROLE analyst_reader LOGIN PASSWORD '$ANALYST_READER_PASSWORD';
    GRANT CONNECT ON DATABASE $POSTGRES_DB TO analyst_reader;
    GRANT USAGE ON SCHEMA public TO analyst_reader;
    GRANT SELECT ON articles, pipeline_metrics TO analyst_reader;
EOSQL
