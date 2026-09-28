"""Lance l'extraction complète : NewsData, flux RSS et Le Gorafi."""

import logging

from extract_gorafi import extract_gorafi
from extract_newsdata import extract_newsdata
from extract_rss import extract_rss
from utils import add_images, clean_articles, save_articles, setup_logging

logger = logging.getLogger(__name__)

SOURCES = {
    "NewsData": extract_newsdata,
    "RSS": extract_rss,
    "Le Gorafi": extract_gorafi,
}


def main():
    setup_logging()
    logger.info("Début de l'extraction")

    articles = []
    for name, extract_function in SOURCES.items():
        # Si une source plante, on passe à la suivante
        try:
            articles.extend(extract_function())
        except Exception as error:
            logger.error("Échec de l'extraction %s : %s", name, error)

    logger.info("Total brut : %d articles", len(articles))
    articles = clean_articles(articles)
    articles = add_images(articles)
    save_articles(articles)
    logger.info("Fin de l'extraction")


if __name__ == "__main__":
    main()
