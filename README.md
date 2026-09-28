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
- `data/images/` : l'image de chaque article, nommée avec l'id de l'article
- `logs/extraction.log` : le déroulement de chaque exécution (nombre d'articles, articles écartés, erreurs)

Chaque article contient : id, source, domaine, url, titre, texte, image_url, image_path, date_publication, langue, auteur, fiabilite_source, label.

Les articles sans texte ou sans image sont écartés. Les images sont téléchargées pour vérifier que le lien fonctionne et que le fichier est bien une image.

## Transformer les données

```bash
uv run python src/transform.py
```

Le pipeline lit tous les fichiers de `data/extracted/`, puis nettoie les URL et les textes, convertit les types, supprime les doublons, vérifie les champs et les images (image valide, pas partagée entre plusieurs articles), ajoute des colonnes et exporte le résultat :

- `data/processed/articles.parquet` : le jeu de données final, avec les types conservés
- `data/processed/articles.csv` : la même chose, lisible dans un tableur
- `logs/transformation.log` : le nombre d'articles modifiés ou écartés à chaque étape

Le pipeline repart toujours de tous les fichiers extraits, donc deux lancements sur les mêmes données donnent le même résultat. Le schéma des données finales est dans `livrables/etape3_schema_donnees.mmd` (source Mermaid) et `livrables/etape3_schema_donnees.pdf`.

## Organisation du code

```
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
