"""Extraction des articles depuis les flux RSS des médias."""

import logging
from datetime import datetime, timezone

import feedparser
import requests

import config
from utils import build_article

logger = logging.getLogger(__name__)


def fetch_feed(url):
    """Télécharge le flux RSS et le lit avec feedparser."""
    response = requests.get(url, headers=config.HEADERS, timeout=config.REQUEST_TIMEOUT)
    response.raise_for_status()
    return feedparser.parse(response.content)


def get_image_url(entry):
    """Cherche l'image de l'article : balise media:content ou enclosure selon le média."""
    if entry.get("media_content"):
        return entry.media_content[0].get("url")
    for enclosure in entry.get("enclosures", []):
        if enclosure.get("type", "").startswith("image/"):
            return enclosure.get("href")
    return None


def parse_entry(entry, feed_name):
    """Transforme un article du flux au format commun."""
    # feedparser donne la date déjà convertie en UTC dans published_parsed
    date = None
    if entry.get("published_parsed"):
        date = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc).isoformat()

    return build_article(
        source=feed_name,
        url=entry.link,
        titre=entry.get("title"),
        texte=entry.get("summary"),
        image_url=get_image_url(entry),
        date_publication=date,
        auteur=entry.get("author"),
    )


def extract_rss():
    """Lit tous les flux de la configuration. Un flux en erreur n'arrête pas les autres."""
    articles = []

    for feed_name, url in config.RSS_FEEDS.items():
        try:
            feed = fetch_feed(url)
        except requests.RequestException as error:
            logger.error("Flux %s inaccessible : %s", feed_name, error)
            continue

        for entry in feed.entries:
            articles.append(parse_entry(entry, feed_name))
        logger.info("RSS %s : %d articles", feed_name, len(feed.entries))

    return articles
