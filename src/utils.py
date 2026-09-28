"""Fonctions communes à toutes les sources : logs, nettoyage, images, sauvegarde."""

import hashlib
import json
import logging
from datetime import datetime, timezone
from urllib.parse import urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup

import config

logger = logging.getLogger(__name__)

# Extension du fichier selon le type d'image renvoyé par le serveur
IMAGE_EXTENSIONS = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "image/webp": ".webp",
    "image/gif": ".gif",
}


def setup_logging():
    """Affiche les logs dans la console et les écrit dans logs/extraction.log."""
    config.LOGS_DIR.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.FileHandler(config.LOGS_DIR / "extraction.log", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def make_id(url):
    """Crée un identifiant unique à partir de l'URL de l'article."""
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def get_domain(url):
    """Renvoie le nom de domaine d'une URL, sans le 'www.'."""
    return urlparse(url).netloc.removeprefix("www.")


def clean_text(text):
    """Retire les balises HTML et les espaces en trop."""
    if not text:
        return ""
    text = BeautifulSoup(text, "html.parser").get_text(" ")
    return " ".join(text.split())


def build_article(source, url, titre, texte, image_url, date_publication,
                  auteur=None, fiabilite_source=None, label=None):
    """Construit un article au format commun défini à l'étape 1."""
    return {
        "id": make_id(url),
        "source": source,
        "domaine": get_domain(url),
        "url": url,
        "titre": titre,
        "texte": texte,
        "image_url": image_url,
        "image_path": None,  # rempli après le téléchargement de l'image
        "date_publication": date_publication,
        "langue": config.LANGUAGE,
        "auteur": auteur,
        "fiabilite_source": fiabilite_source,
        "label": label,
    }


def clean_articles(articles):
    """Nettoie les textes et garde seulement les articles complets (texte + image)."""
    cleaned = []
    seen_ids = set()
    nb_incomplete = 0
    nb_duplicates = 0

    for article in articles:
        article["titre"] = clean_text(article["titre"])
        article["texte"] = clean_text(article["texte"])

        if not article["titre"] or not article["texte"] or not article["image_url"]:
            nb_incomplete += 1
            continue
        if article["id"] in seen_ids:
            nb_duplicates += 1
            continue

        seen_ids.add(article["id"])
        cleaned.append(article)

    logger.info("Nettoyage : %d articles gardés, %d incomplets, %d doublons",
                len(cleaned), nb_incomplete, nb_duplicates)
    return cleaned


def download_image(image_url, article_id):
    """Télécharge l'image si le lien fonctionne. Renvoie le chemin du fichier, ou None."""
    try:
        response = requests.get(image_url, headers=config.HEADERS, timeout=config.REQUEST_TIMEOUT)
        response.raise_for_status()
    except requests.RequestException as error:
        logger.warning("Image inaccessible (%s) : %s", image_url, error)
        return None

    # On vérifie que le lien renvoie bien une image dans un format connu
    content_type = response.headers.get("Content-Type", "").split(";")[0].strip()
    extension = IMAGE_EXTENSIONS.get(content_type)
    if extension is None:
        logger.warning("Format d'image non pris en charge (%s) : %s", content_type, image_url)
        return None

    config.IMAGES_DIR.mkdir(parents=True, exist_ok=True)
    image_path = config.IMAGES_DIR / f"{article_id}{extension}"
    image_path.write_bytes(response.content)
    return image_path.relative_to(config.ROOT_DIR).as_posix()


def add_images(articles):
    """Télécharge l'image de chaque article et écarte ceux dont l'image ne fonctionne pas."""
    articles_with_image = []
    for article in articles:
        image_path = download_image(article["image_url"], article["id"])
        if image_path:
            article["image_path"] = image_path
            articles_with_image.append(article)

    nb_rejected = len(articles) - len(articles_with_image)
    logger.info("Images : %d téléchargées, %d articles écartés (image inaccessible)",
                len(articles_with_image), nb_rejected)
    return articles_with_image


def save_articles(articles):
    """Sauvegarde les articles en JSON et en CSV, dans un fichier daté."""
    config.EXTRACTED_DIR.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    json_path = config.EXTRACTED_DIR / f"articles_{timestamp}.json"
    csv_path = config.EXTRACTED_DIR / f"articles_{timestamp}.csv"

    with open(json_path, "w", encoding="utf-8") as file:
        json.dump(articles, file, ensure_ascii=False, indent=2)

    # utf-8-sig pour que les accents s'affichent bien dans Excel
    pd.DataFrame(articles).to_csv(csv_path, index=False, encoding="utf-8-sig")

    logger.info("Sauvegarde : %d articles dans %s", len(articles), json_path.name)
    return json_path
