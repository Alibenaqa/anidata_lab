"""
Module Transform — Nettoyage, enrichissement et feature engineering
"""
import pandas as pd
import numpy as np
import logging

logger = logging.getLogger(__name__)

TOP_STUDIOS = [
    'Toei Animation', 'Sunrise', 'J.C.Staff', 'Madhouse', 'Bones',
    'A-1 Pictures', 'Production I.G', 'Kyoto Animation', 'Gainax', 'Shaft'
]


def clean_anime(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie le dataframe anime brut."""
    logger.info("Nettoyage anime...")
    df = df.copy()

    # Supprimer doublons
    before = len(df)
    df = df.drop_duplicates(subset=['MAL_ID'])
    logger.info(f"Doublons supprimés : {before - len(df)}")

    # Remplacer 'Unknown' par NaN
    df.replace('Unknown', np.nan, inplace=True)

    # Conversion types numériques
    numeric_cols = ['Score', 'Episodes', 'Members', 'Favorites', 'Ranked', 'Popularity',
                    'Watching', 'Completed', 'Dropped', 'Plan to Watch',
                    'Score-10','Score-9','Score-8','Score-7','Score-6',
                    'Score-5','Score-4','Score-3','Score-2','Score-1']
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Calculer 'Scored By' depuis les colonnes de distribution de votes
    score_cols = [f'Score-{i}' for i in range(1, 11) if f'Score-{i}' in df.columns]
    if score_cols:
        df['Scored By'] = df[score_cols].sum(axis=1)

    logger.info("Nettoyage terminé")
    return df


def enrich_anime(df: pd.DataFrame, synopsis_df: pd.DataFrame = None) -> pd.DataFrame:
    """Ajoute les features métier et fusionne le synopsis."""
    logger.info("Enrichissement anime...")
    df = df.copy()

    # Parser genres et studios
    df['genres_list'] = df['Genres'].apply(_parse_list)
    df['studios_list'] = df['Studios'].apply(_parse_list)
    df['producers_list'] = df['Producers'].apply(_parse_list) if 'Producers' in df.columns else [[]] * len(df)

    # Score pondéré (Bayesian average)
    C = df['Score'].mean()
    m = 5000
    df['weighted_score'] = (
        (df['Scored By'] / (df['Scored By'] + m)) * df['Score'] +
        (m / (df['Scored By'] + m)) * C
    ).round(4)

    # Taux de complétion et abandon
    total = df.get('Watching', 0) + df.get('Completed', 0) + df.get('Dropped', 0)
    if 'Completed' in df.columns:
        df['completion_rate'] = (df['Completed'] / total.replace(0, np.nan)).round(4)
    if 'Dropped' in df.columns:
        df['drop_rate'] = (df['Dropped'] / total.replace(0, np.nan)).round(4)

    # Classification studio
    df['studio_category'] = df['studios_list'].apply(_classify_studio)

    # Fusion synopsis
    if synopsis_df is not None:
        synopsis_clean = synopsis_df[['MAL_ID', 'sypnopsis']].rename(columns={'sypnopsis': 'synopsis'})
        df = df.merge(synopsis_clean, on='MAL_ID', how='left')

    logger.info(f"Enrichissement terminé : {df.shape[1]} colonnes")
    return df


def _parse_list(val):
    if pd.isna(val):
        return []
    return [x.strip() for x in str(val).split(',') if x.strip()]


def _classify_studio(studios):
    if not studios:
        return 'Inconnu'
    for s in studios:
        if s in TOP_STUDIOS:
            return 'Grand Studio'
    return 'Indépendant'
