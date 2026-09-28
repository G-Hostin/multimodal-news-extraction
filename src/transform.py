"""Pipeline de transformation : lecture des données extraites, traitement, export."""

import json
import logging
import re
from pathlib import Path

import pandas as pd
from PIL import Image, UnidentifiedImageError

import config
from utils import setup_logging

logger = logging.getLogger(__name__)

# Mapping du code de la source vers un nom lisible et le type de collecte
SOURCE_NAMES = {
    "newsdata": "NewsData.io",
    "lemonde": "Le Monde",
    "franceinfo": "franceinfo",
    "lesdecodeurs": "Les Décodeurs",
    "20minutes_fakeoff": "20 Minutes Fake Off",
    "legorafi": "Le Gorafi",
}
SOURCE_TYPES = {
    "newsdata": "api",
    "lemonde": "rss",
    "franceinfo": "rss",
    "lesdecodeurs": "rss",
    "20minutes_fakeoff": "rss",
    "legorafi": "scraping",
}
FACT_CHECKING_SOURCES = ["lesdecodeurs", "20minutes_fakeoff"]

REQUIRED_COLUMNS = ["url", "titre", "texte", "image_url", "image_path", "date_publication"]

# Ordre des colonnes dans le fichier final
FINAL_COLUMNS = [
    "id", "url", "titre", "texte", "nb_mots_titre", "nb_mots_texte",
    "date_publication", "langue", "auteur", "label",
    "source", "nom_source", "type_source", "est_fact_checking", "domaine", "rang_fiabilite",
    "image_url", "image_path", "image_largeur", "image_hauteur", "image_format",
    "date_extraction",
]


# ---------- 1. Lecture ----------

def lit_donnees():
    """Charge tous les fichiers JSON produits par l'extraction dans un seul DataFrame."""
    files = sorted(config.EXTRACTED_DIR.glob("articles_*.json"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier extrait dans {config.EXTRACTED_DIR}")

    dataframes = []
    for file in files:
        with open(file, encoding="utf-8") as f:
            df_file = pd.DataFrame(json.load(f))
        # La date d'extraction est dans le nom du fichier : articles_20260928_110726.json
        timestamp = file.stem.removeprefix("articles_")
        df_file["date_extraction"] = pd.to_datetime(timestamp, format="%Y%m%d_%H%M%S", utc=True)
        dataframes.append(df_file)

    df = pd.concat(dataframes, ignore_index=True)
    logger.info("Lecture : %d articles dans %d fichier(s)", len(df), len(files))
    return df


# ---------- 2. Traitement ----------

def nettoie_url(df):
    """Retire les paramètres de suivi des URL (?at_medium=..., #xtor=...)."""
    urls_before = df["url"].copy()
    df["url"] = df["url"].str.split("#").str[0].str.split("?").str[0]
    logger.info("URL : %d URL nettoyées", (urls_before != df["url"]).sum())
    return df


def nettoie_texte(texte):
    """Uniformise un texte : apostrophes, espaces insécables, points de suspension de fin."""
    texte = texte.replace("’", "'").replace("\xa0", " ")
    # Les descriptions coupées par NewsData se terminent par "..." ou "…"
    texte = re.sub(r"(\.\.\.|…)$", "", texte.strip())
    return " ".join(texte.split())


def nettoie_textes(df):
    """Applique nettoie_texte() au titre et au texte."""
    for column in ["titre", "texte"]:
        before = df[column].copy()
        df[column] = df[column].fillna("").apply(nettoie_texte)
        logger.info("Texte : %d valeurs modifiées dans '%s'", (before != df[column]).sum(), column)
    return df


def convertit_types(df):
    """Convertit les dates en datetime UTC et le rang de fiabilité en entier."""
    df["date_publication"] = pd.to_datetime(df["date_publication"], utc=True, errors="coerce")
    df["fiabilite_source"] = pd.to_numeric(df["fiabilite_source"], errors="coerce").astype("Int64")
    logger.info("Types : %d dates invalides", df["date_publication"].isnull().sum())
    return df


def supprime_doublons(df):
    """Supprime les articles présents plusieurs fois (même URL), en gardant la première extraction."""
    before = len(df)
    # Tri "stable" : à date égale, l'ordre de lecture est gardé (résultat reproductible)
    df = df.sort_values("date_extraction", kind="stable").drop_duplicates(subset="url", keep="first")
    logger.info("Doublons : %d supprimés, %d articles restants", before - len(df), len(df))
    return df


def verifie_champs(df):
    """Écarte les articles avec un champ obligatoire vide, une autre langue ou une date dans le futur."""
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLUMNS)
    df = df[(df["titre"] != "") & (df["texte"] != "")]
    logger.info("Champs obligatoires : %d articles écartés", before - len(df))

    before = len(df)
    df = df[df["langue"].isin([config.LANGUAGE])]
    logger.info("Langue : %d articles écartés (langue différente de '%s')", before - len(df), config.LANGUAGE)

    before = len(df)
    df = df[df["date_publication"] <= pd.Timestamp.now(tz="UTC")]
    logger.info("Dates : %d articles écartés (date dans le futur)", before - len(df))
    return df


def valide_image(image_path):
    """Vérifie qu'une image existe, s'ouvre et est assez grande. Renvoie (largeur, hauteur, format) ou None."""
    try:
        with Image.open(config.ROOT_DIR / image_path) as img:
            img.load()  # lit l'image en entier : échoue si le fichier est corrompu
            width, height = img.size
            image_format = img.format
    except (FileNotFoundError, UnidentifiedImageError, OSError) as error:
        logger.warning("Image invalide (%s) : %s", image_path, error)
        return None

    if width < config.IMAGE_MIN_WIDTH:
        logger.warning("Image trop petite (%d px de large) : %s", width, image_path)
        return None
    return width, height, image_format


def valide_images(df):
    """Applique valide_image() à chaque article et ajoute les dimensions et le format de l'image."""
    results = df["image_path"].apply(valide_image)
    before = len(df)
    df = df[results.notnull()].copy()
    results = results[results.notnull()]

    df["image_largeur"] = results.str[0].astype(int)
    df["image_hauteur"] = results.str[1].astype(int)
    df["image_format"] = results.str[2]
    logger.info("Images : %d articles écartés (image absente, corrompue ou trop petite)", before - len(df))
    return df


def verifie_association(df):
    """Vérifie que chaque image correspond bien à son article et n'est pas partagée."""
    # L'image est nommée avec l'id de l'article à l'extraction
    same_name = df["image_path"].apply(lambda path: Path(path).stem) == df["id"]
    logger.info("Association : %d images dont le nom ne correspond pas à l'id", (~same_name).sum())
    df = df[same_name]

    # Une même image utilisée par plusieurs articles différents est une image par défaut (logo...)
    shared = df["image_url"].duplicated(keep=False)
    for domain, count in df[shared]["domaine"].value_counts().items():
        logger.warning("Association : %d articles de %s partagent la même image, écartés", count, domain)
    df = df[~shared]
    logger.info("Association : %d articles écartés (image partagée)", shared.sum())
    return df


def mappe_sources(df):
    """Ajoute le nom lisible de la source, le type de collecte et l'indicateur fact-checking."""
    df["nom_source"] = df["source"].map(SOURCE_NAMES)
    df["type_source"] = df["source"].map(SOURCE_TYPES)
    df["est_fact_checking"] = df["source"].isin(FACT_CHECKING_SOURCES)
    logger.info("Mapping : répartition par type %s", df["type_source"].value_counts().to_dict())
    return df


def remplit_manquants(df):
    """Remplace les valeurs manquantes quand on connaît la bonne valeur."""
    logger.info("Valeurs manquantes : %d auteurs, %d labels", df["auteur"].isnull().sum(), df["label"].isnull().sum())
    df["auteur"] = df["auteur"].fillna("inconnu")
    # Personne n'a vérifié ces articles : on le dit plutôt que de laisser vide
    df["label"] = df["label"].fillna("non_verifie")
    # Le rang de fiabilité n'existe que pour NewsData : on le laisse vide pour les autres
    return df


def renomme_colonnes(df):
    """Renomme fiabilite_source : c'est un rang, plus il est bas, plus la source est fiable."""
    return df.rename(columns={"fiabilite_source": "rang_fiabilite"})


def ajoute_colonnes(df):
    """Ajoute le nombre de mots du titre et du texte."""
    df["nb_mots_titre"] = df["titre"].str.split().str.len()
    df["nb_mots_texte"] = df["texte"].str.split().str.len()
    return df


# ---------- 3. Export ----------

def exporte(df):
    """Écrit le jeu de données final en Parquet (types conservés) et en CSV (lecture humaine)."""
    df = df[FINAL_COLUMNS].sort_values(["date_publication", "id"], ascending=[False, True])
    df = df.reset_index(drop=True)

    config.PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(config.PROCESSED_DIR / "articles.parquet", index=False)
    df.to_csv(config.PROCESSED_DIR / "articles.csv", index=False, encoding="utf-8-sig")
    logger.info("Export : %d articles dans %s", len(df), config.PROCESSED_DIR)
    return df


def main():
    setup_logging("transformation.log")
    logger.info("Début de la transformation")

    df = lit_donnees()
    df = nettoie_url(df)
    df = nettoie_textes(df)
    df = convertit_types(df)
    df = supprime_doublons(df)
    df = verifie_champs(df)
    df = valide_images(df)
    df = verifie_association(df)
    df = mappe_sources(df)
    df = remplit_manquants(df)
    df = renomme_colonnes(df)
    df = ajoute_colonnes(df)
    exporte(df)

    logger.info("Fin de la transformation")


if __name__ == "__main__":
    main()
