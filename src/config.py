"""Paramètres de l'extraction (sources, volumes, dossiers)."""

import os
from pathlib import Path

from dotenv import load_dotenv

# Dossier racine du projet (celui qui contient src/)
ROOT_DIR = Path(__file__).resolve().parent.parent

# La clé API est lue dans le fichier .env (jamais écrite dans le code)
load_dotenv(ROOT_DIR / ".env")
NEWSDATA_API_KEY = os.getenv("NEWSDATA_API_KEY")

# --- NewsData.io ---
NEWSDATA_URL = "https://newsdata.io/api/1/latest"
NEWSDATA_PARAMS = {"language": "fr", "country": "fr"}
# 1 page = 10 articles = 1 crédit (200 crédits par jour en offre gratuite)
NEWSDATA_MAX_PAGES = 5

# --- Flux RSS ---
RSS_FEEDS = {
    "lemonde": "https://www.lemonde.fr/rss/une.xml",
    "franceinfo": "https://www.franceinfo.fr/titres.rss",
    "lesdecodeurs": "https://www.lemonde.fr/les-decodeurs/rss_full.xml",
    "20minutes_fakeoff": "https://www.20minutes.fr/feeds/rss-fake-off.xml",
}

# --- Le Gorafi (scraping) ---
GORAFI_URL = "https://www.legorafi.fr/"
GORAFI_MAX_ARTICLES = 10
GORAFI_PAUSE = 2  # secondes entre deux pages, pour ne pas surcharger le site

# --- Requêtes HTTP ---
REQUEST_TIMEOUT = 15  # secondes
HEADERS = {"User-Agent": "Mozilla/5.0 (projet etudiant multimodal-news-extraction)"}

# Toutes nos sources sont en français
LANGUAGE = "fr"

# --- Dossiers de sortie ---
DATA_DIR = ROOT_DIR / "data"
IMAGES_DIR = DATA_DIR / "images"
EXTRACTED_DIR = DATA_DIR / "extracted"
LOGS_DIR = ROOT_DIR / "logs"
