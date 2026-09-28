"""Extraction des articles depuis l'API NewsData.io."""

import logging
from datetime import datetime, timezone

import requests

import config
from utils import build_article

logger = logging.getLogger(__name__)


def fetch_newsdata_page(page_token=None):
    """Appelle l'API et renvoie la réponse JSON d'une page de résultats."""
    params = dict(config.NEWSDATA_PARAMS)
    if page_token:
        params["page"] = page_token

    # La clé passe dans l'en-tête plutôt que dans l'URL, pour ne pas apparaître dans les logs
    headers = {"X-ACCESS-KEY": config.NEWSDATA_API_KEY}
    response = requests.get(config.NEWSDATA_URL, params=params, headers=headers,
                            timeout=config.REQUEST_TIMEOUT)

    if response.status_code == 429:
        raise RuntimeError("Quota NewsData atteint, réessayer plus tard")
    response.raise_for_status()
    return response.json()


def parse_newsdata_article(item):
    """Transforme un article de l'API au format commun."""
    # pubDate est en UTC, au format "2026-09-27 22:49:46"
    date = datetime.strptime(item["pubDate"], "%Y-%m-%d %H:%M:%S")
    date = date.replace(tzinfo=timezone.utc).isoformat()

    auteur = ", ".join(item["creator"]) if item.get("creator") else None

    return build_article(
        source="newsdata",
        url=item["link"],
        titre=item.get("title"),
        # Le contenu complet est réservé aux offres payantes, on garde la description
        texte=item.get("description"),
        image_url=item.get("image_url"),
        date_publication=date,
        auteur=auteur,
        fiabilite_source=item.get("source_priority"),
    )


def extract_newsdata():
    """Récupère plusieurs pages de résultats (dans la limite de NEWSDATA_MAX_PAGES)."""
    if not config.NEWSDATA_API_KEY:
        raise ValueError("Clé NEWSDATA_API_KEY absente du fichier .env")

    articles = []
    page_token = None
    nb_duplicates = 0
    nb_pages = 0

    for _ in range(config.NEWSDATA_MAX_PAGES):
        data = fetch_newsdata_page(page_token)
        nb_pages += 1

        for item in data.get("results", []):
            # NewsData signale lui-même les articles repris d'une autre source
            if item.get("duplicate"):
                nb_duplicates += 1
                continue
            try:
                articles.append(parse_newsdata_article(item))
            except (KeyError, ValueError) as error:
                logger.warning("Article NewsData ignoré (champ manquant ou invalide) : %s", error)

        page_token = data.get("nextPage")
        if not page_token:
            break

    logger.info("NewsData : %d articles récupérés sur %d page(s), %d doublons signalés par l'API",
                len(articles), nb_pages, nb_duplicates)
    return articles
