"""
Module Extract — Lecture et validation des fichiers CSV bruts
"""
import pandas as pd
import numpy as np
import os
import logging

logger = logging.getLogger(__name__)

RAW_PATH = os.environ.get('ANIDATA_RAW_PATH', '/opt/airflow/data/raw')

SCHEMAS = {
    'anime': {
        'required_cols': ['MAL_ID', 'Name', 'Score', 'Genres', 'Type', 'Episodes', 'Members'],
        'file': 'anime.csv'
    },
    'ratings': {
        'required_cols': ['user_id', 'anime_id', 'rating'],
        'file': 'rating_complete.csv'
    },
    'synopsis': {
        'required_cols': ['MAL_ID', 'sypnopsis'],
        'file': 'anime_with_synopsis.csv'
    }
}


def extract_anime():
    """Charge anime.csv et valide le schéma."""
    path = os.path.join(RAW_PATH, SCHEMAS['anime']['file'])
    logger.info(f"Extraction anime depuis {path}")
    df = pd.read_csv(path)
    _validate_schema(df, SCHEMAS['anime']['required_cols'], 'anime')
    logger.info(f"anime.csv chargé : {len(df):,} lignes")
    return df


def extract_ratings(nrows=None):
    """Charge rating_complete.csv (optionnellement limité à nrows pour les tests)."""
    path = os.path.join(RAW_PATH, SCHEMAS['ratings']['file'])
    logger.info(f"Extraction ratings depuis {path}")
    df = pd.read_csv(path, nrows=nrows)
    _validate_schema(df, SCHEMAS['ratings']['required_cols'], 'ratings')
    logger.info(f"rating_complete.csv chargé : {len(df):,} lignes")
    return df


def extract_synopsis():
    """Charge anime_with_synopsis.csv et valide le schéma."""
    path = os.path.join(RAW_PATH, SCHEMAS['synopsis']['file'])
    logger.info(f"Extraction synopsis depuis {path}")
    df = pd.read_csv(path)
    _validate_schema(df, SCHEMAS['synopsis']['required_cols'], 'synopsis')
    logger.info(f"synopsis chargé : {len(df):,} lignes")
    return df


def _validate_schema(df, required_cols, name):
    """Vérifie que les colonnes obligatoires sont présentes."""
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"[{name}] Colonnes manquantes : {missing}")
    logger.info(f"[{name}] Schéma validé — {len(df.columns)} colonnes OK")
