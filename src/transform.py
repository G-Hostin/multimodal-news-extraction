"""Pipeline de transformation : lecture des données extraites, traitement, export."""

import argparse
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

# L'image n'est pas obligatoire : les articles sans image sont gardés et signalés par a_image
REQUIRED_COLUMNS = ["url", "titre", "texte", "date_publication"]

# Ordre des colonnes dans le fichier final
FINAL_COLUMNS = [
    "id", "url", "titre", "texte", "nb_mots_titre", "nb_mots_texte",
    "date_publication", "langue", "auteur", "label",
    "source", "nom_source", "type_source", "est_fact_checking", "domaine", "rang_fiabilite",
    "a_image", "image_partagee",
    "image_url", "image_path", "image_largeur", "image_hauteur", "image_format",
    "date_extraction",
]


# ---------- 1. Lecture ----------

def lit_donnees(dossier_entree=config.EXTRACTED_DIR):
    """Charge tous les fichiers JSON produits par l'extraction dans un seul DataFrame."""
    files = sorted(Path(dossier_entree).glob("articles_*.json"))
    if not files:
        raise FileNotFoundError(f"Aucun fichier extrait dans {dossier_entree}")

    dataframes = []
    for file in files:
        with open(file, encoding="utf-8") as f:
            df_file = pd.DataFrame(json.load(f))
        # La date d'extraction est dans le nom du fichier : articles_20260928_110726.json
        # (ou articles_20260928_110726_newsdata.json quand l'extraction vient d'Airflow)
        timestamp = "_".join(file.stem.split("_")[1:3])
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
    """Vérifie qu'une image existe et s'ouvre. Renvoie (largeur, hauteur, format) ou None."""
    if pd.isna(image_path):
        return None  # article sans image
    try:
        with Image.open(config.ROOT_DIR / image_path) as img:
            img.load()  # lit l'image en entier : échoue si le fichier est corrompu
            return img.width, img.height, img.format
    except (FileNotFoundError, UnidentifiedImageError, OSError) as error:
        logger.warning("Image invalide (%s) : %s", image_path, error)
        return None


def valide_images(df):
    """Applique valide_image() à chaque article et ajoute a_image, les dimensions et le format."""
    results = df["image_path"].apply(valide_image)
    df["a_image"] = results.notnull()
    # Si l'image n'est pas exploitable, on ne garde pas de chemin vers elle
    df.loc[~df["a_image"], "image_path"] = None

    # Colonnes vides (NA) pour les articles sans image
    df["image_largeur"] = results.apply(lambda r: r[0] if r else None).astype("Int64")
    df["image_hauteur"] = results.apply(lambda r: r[1] if r else None).astype("Int64")
    df["image_format"] = results.apply(lambda r: r[2] if r else None)
    logger.info("Images : %d articles avec une image valide, %d sans image",
                df["a_image"].sum(), (~df["a_image"]).sum())
    return df


def verifie_association(df):
    """Vérifie que chaque image correspond à son article et signale les images partagées."""
    # L'image est nommée avec l'id de l'article à l'extraction
    with_image = df["a_image"]
    same_name = df.loc[with_image, "image_path"].apply(lambda path: Path(path).stem) == df.loc[with_image, "id"]
    logger.info("Association : %d images dont le nom ne correspond pas à l'id", (~same_name).sum())

    # Une même image utilisée par plusieurs articles différents est souvent une image par défaut (logo...)
    df["image_partagee"] = with_image & df["image_url"].duplicated(keep=False)
    for domain, count in df.loc[df["image_partagee"], "domaine"].value_counts().items():
        logger.warning("Association : %d articles de %s partagent la même image", count, domain)
    logger.info("Association : %d articles avec une image partagée (signalés)", df["image_partagee"].sum())
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

def exporte(df, dossier_sortie=config.PROCESSED_DIR):
    """Écrit le jeu de données final en Parquet (types conservés) et en CSV (lecture humaine)."""
    df = df[FINAL_COLUMNS].sort_values(["date_publication", "id"], ascending=[False, True])
    df = df.reset_index(drop=True)

    dossier_sortie = Path(dossier_sortie)
    dossier_sortie.mkdir(parents=True, exist_ok=True)
    df.to_parquet(dossier_sortie / "articles.parquet", index=False)
    df.to_csv(dossier_sortie / "articles.csv", index=False, encoding="utf-8-sig")
    logger.info("Export : %d articles dans %s", len(df), dossier_sortie)
    return df


def transforme(dossier_entree=config.EXTRACTED_DIR, dossier_sortie=config.PROCESSED_DIR):
    """Enchaîne toutes les étapes : lecture, traitement, export."""
    logger.info("Début de la transformation")
    df = lit_donnees(dossier_entree)
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
    df = exporte(df, dossier_sortie)
    logger.info("Fin de la transformation")
    return df


def main():
    # Les dossiers peuvent être changés au lancement, par défaut ce sont ceux de config.py
    parser = argparse.ArgumentParser(description="Transforme les articles extraits en jeu de données final")
    parser.add_argument("--entree", default=config.EXTRACTED_DIR, help="dossier des fichiers JSON extraits")
    parser.add_argument("--sortie", default=config.PROCESSED_DIR, help="dossier du Parquet et du CSV")
    args = parser.parse_args()

    setup_logging("transformation.log")
    transforme(args.entree, args.sortie)


if __name__ == "__main__":
    main()
