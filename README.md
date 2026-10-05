# Extraction de données multimodales d'actualité

Projet réalisé pour CheckIt.AI : récupérer automatiquement des articles d'actualité avec leur texte et leur image, pour alimenter un futur détecteur de fake news.

Sources utilisées :

- l'API NewsData.io
- les flux RSS du Monde, de franceinfo, des Décodeurs et de 20 Minutes (Fake Off)
- le site satirique Le Gorafi, par scraping (requests + BeautifulSoup)

## Installation

Le projet utilise [uv](https://docs.astral.sh/uv/).

```bash
uv sync
```

Copier `.env.example` en `.env` et y mettre sa clé NewsData.io (compte gratuit sur https://newsdata.io) :

```
NEWSDATA_API_KEY=ta_cle_ici
```

## Lancer l'extraction

```bash
uv run python src/main.py
```

Le script tourne sans intervention. Si une source ne répond pas, l'erreur est notée dans les logs et les autres sources continuent.

## Résultats

- `data/extracted/articles_<date>.json` et `.csv` : un article par ligne, au même format pour toutes les sources
- `data/images/` : l'image de chaque article qui en a une, nommée avec l'id de l'article
- `logs/extraction.log` : le déroulement de chaque exécution (nombre d'articles, articles sans image, erreurs)

Chaque article contient : id, source, domaine, url, titre, texte, image_url, image_path, date_publication, langue, auteur, fiabilite_source, label.

Les articles sans titre ou sans texte sont écartés, ceux sans image sont gardés. Les images sont téléchargées pour vérifier que le lien fonctionne et que le fichier est bien une image.

## Transformer les données

```bash
uv run python src/transform.py
```

Les dossiers d'entrée et de sortie peuvent être changés au lancement (par défaut, ceux de `src/config.py`) :

```bash
uv run python src/transform.py --entree data/extracted --sortie data/processed
```

Le pipeline lit tous les fichiers de `data/extracted/`, puis nettoie les URL et les textes, convertit les types, supprime les doublons, vérifie les champs et les images (image valide avec `a_image`, image partagée avec `image_partagee`), ajoute des colonnes et exporte le résultat :

- `data/processed/articles.parquet` : le jeu de données final, avec les types conservés
- `data/processed/articles.csv` : la même chose, lisible dans un tableur
- `logs/transformation.log` : le nombre d'articles modifiés ou écartés à chaque étape

Le pipeline repart toujours de tous les fichiers extraits, donc deux lancements sur les mêmes données donnent le même résultat. Le schéma des données finales est dans `livrables/etape3_schema_donnees.mmd` (source Mermaid) et `livrables/etape3_schema_donnees.pdf`.

## Orchestration avec Airflow

Le DAG `etl_multimodal_news` (`dags/etl_multimodal_news.py`) enchaîne tout le flux une fois par jour :

```
extract_newsdata ─┐
extract_rss ──────┼──> transform ──> load
extract_legorafi ─┘
```

Les trois extractions tournent en parallèle. `transform` lance le pipeline de l'étape 3 et `load` charge le résultat dans la table `articles` d'une base PostgreSQL (ajout ou mise à jour selon l'id, donc pas de doublons d'un jour à l'autre). Chaque tâche est un `PythonOperator` qui appelle les fonctions de `src/`.

Lancement (Docker Desktop nécessaire) :

```bash
docker compose build
docker compose up airflow-init
docker compose up -d
```

L'interface est sur http://localhost:8080 (identifiant `airflow`, mot de passe `airflow`).

Le fichier `.env` doit contenir les variables de `.env.example`. La clé Fernet se génère avec :

```bash
python -c "import base64, os; print(base64.urlsafe_b64encode(os.urandom(32)).decode())"
```

### Base de données

PostgreSQL, parce que les données transformées ont un schéma fixe et qu'une clé primaire sur l'id évite les doublons au chargement. Les images restent des fichiers dans `data/images/`, la base stocke leur chemin.

La base tourne dans son propre conteneur (`checkit-db`), séparé de la base interne d'Airflow, et n'est accessible que depuis la machine locale (port 5433). Au premier démarrage, `sql/init-db.sh` crée la table et deux utilisateurs avec mot de passe : `etl_writer` (utilisé par le DAG, lecture et écriture de la table) et `analyst_reader` (lecture seule). Les mots de passe et la clé API sont dans le `.env`, et la clé Fernet fait chiffrer par Airflow les connexions et variables qu'il stocke.

## Tableau de bord et monitoring

Chaque tâche du DAG enregistre ses chiffres (articles, durée, crédits NewsData) dans la table `pipeline_metrics`. La dernière tâche, `controle_qualite`, compare ces chiffres aux seuils d'alerte définis dans `src/config.py` et échoue en cas de problème critique.

Le tableau de bord Streamlit affiche les KPI (qualité, rapidité, coût) en lisant la base avec l'utilisateur `analyst_reader` :

```bash
uv run streamlit run dashboard/app.py
```

Il faut d'abord copier `.streamlit/secrets.toml.example` en `.streamlit/secrets.toml` et y mettre le mot de passe `ANALYST_READER_PASSWORD` du `.env`. Le plan de monitoring est dans `livrables/etape5_plan_monitoring.pdf`.

## Organisation du code

```
dags/
└── etl_multimodal_news.py   # DAG Airflow
dashboard/
└── app.py                   # tableau de bord Streamlit des KPI
sql/
└── init-db.sh               # création de la table et des rôles PostgreSQL
src/
├── config.py            # paramètres : sources, nombre d'articles, dossiers
├── utils.py             # logs, nettoyage, téléchargement des images, sauvegarde
├── extract_newsdata.py  # API NewsData.io
├── extract_rss.py       # flux RSS (feedparser)
├── extract_gorafi.py    # scraping du Gorafi
├── main.py              # lance toutes les extractions
└── transform.py         # pipeline de transformation (lecture, traitement, export)
notebooks/
├── exploration.ipynb        # tests des réponses de chaque source avant d'écrire les scripts
└── diagnostic_donnees.ipynb # diagnostic des données extraites avant la transformation
```

Les paramètres (nombre de pages NewsData, nombre d'articles du Gorafi, pause entre deux pages...) se modifient dans `src/config.py`.

## Limites respectées

- NewsData.io gratuit : 200 crédits par jour. Le script utilise 5 crédits par exécution.
- Le Gorafi : le robots.txt est vérifié avant chaque page, avec une pause de 2 secondes entre deux pages et 10 articles maximum.
