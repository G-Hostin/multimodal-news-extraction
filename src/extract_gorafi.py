"""Extraction des articles du Gorafi par scraping (requests + BeautifulSoup)."""

import logging
import time
from datetime import datetime, timezone
from urllib.robotparser import RobotFileParser

import requests
from bs4 import BeautifulSoup

import config
from utils import build_article

logger = logging.getLogger(__name__)


def load_robots():
    """Lit le robots.txt du site, qui indique les pages autorisées aux robots."""
    # On télécharge le fichier avec requests (robots.read() utilise un autre User-Agent,
    # refusé par le site, ce qui ferait croire que tout est interdit)
    response = requests.get(config.GORAFI_URL + "robots.txt", headers=config.HEADERS,
                            timeout=config.REQUEST_TIMEOUT)
    response.raise_for_status()
    robots = RobotFileParser()
    robots.parse(response.text.splitlines())
    return robots


def fetch_page(url):
    """Télécharge une page HTML et la transforme en objet BeautifulSoup."""
    response = requests.get(url, headers=config.HEADERS, timeout=config.REQUEST_TIMEOUT)
    response.raise_for_status()
    return BeautifulSoup(response.content, "html.parser")


def get_article_links(soup):
    """Récupère les liens des articles sur la page d'accueil (sans doublons)."""
    links = []
    for link in soup.find_all("a", href=True):
        href = link["href"]
        # Les URL d'articles ont la forme https://www.legorafi.fr/2026/09/28/titre/
        if href.startswith(config.GORAFI_URL + "20") and href not in links:
            links.append(href)
    return links


def get_meta(soup, property_name):
    """Renvoie le contenu d'une balise <meta property="..."> ou None."""
    tag = soup.find("meta", property=property_name)
    return tag["content"] if tag else None


def parse_article(soup, url):
    """Extrait le titre, le texte, l'image et la date d'une page d'article."""
    titre = soup.find("h1").get_text(strip=True)

    # Le texte de l'article est dans les <p> de la div "mvp-content-main"
    paragraphs = []
    content = soup.find("div", id="mvp-content-main")
    if content:
        for p in content.find_all("p"):
            text = p.get_text(" ", strip=True)
            # L'encart newsletter marque la fin de l'article, la suite n'en fait pas partie
            if "newsletter" in text.lower():
                break
            if text:
                paragraphs.append(text)

    date = get_meta(soup, "article:published_time")
    if date:
        date = datetime.fromisoformat(date).astimezone(timezone.utc).isoformat()

    auteur = soup.find("meta", attrs={"name": "author"})

    return build_article(
        source="legorafi",
        url=url,
        titre=titre,
        texte=" ".join(paragraphs),
        # L'image principale est dans la balise og:image
        image_url=get_meta(soup, "og:image"),
        date_publication=date,
        auteur=auteur["content"] if auteur else None,
        # Le Gorafi est un site satirique : tous ses articles sont de la satire
        label="satire",
    )


def extract_gorafi():
    """Récupère les derniers articles du Gorafi (dans la limite de GORAFI_MAX_ARTICLES)."""
    robots = load_robots()
    home = fetch_page(config.GORAFI_URL)
    links = get_article_links(home)[:config.GORAFI_MAX_ARTICLES]

    articles = []
    for url in links:
        if not robots.can_fetch(config.HEADERS["User-Agent"], url):
            logger.warning("Page interdite par robots.txt, ignorée : %s", url)
            continue

        time.sleep(config.GORAFI_PAUSE)
        try:
            soup = fetch_page(url)
            articles.append(parse_article(soup, url))
        except (requests.RequestException, AttributeError) as error:
            logger.warning("Article du Gorafi ignoré (%s) : %s", url, error)

    logger.info("Le Gorafi : %d articles récupérés", len(articles))
    return articles
