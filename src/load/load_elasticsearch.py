"""
Module Load — Indexation du dataset gold dans Elasticsearch
"""
import pandas as pd
import numpy as np
import json
import logging
import os
from elasticsearch import Elasticsearch, helpers

logger = logging.getLogger(__name__)

ES_HOST = os.environ.get('ELASTICSEARCH_HOST', 'http://localhost:9200')
INDEX_NAME = 'anime-gold'

ANIME_MAPPING = {
    "mappings": {
        "properties": {
            "MAL_ID":          {"type": "integer"},
            "Name":            {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "Score":           {"type": "float"},
            "weighted_score":  {"type": "float"},
            "Genres":          {"type": "keyword"},
            "genres_list":     {"type": "keyword"},
            "Type":            {"type": "keyword"},
            "Episodes":        {"type": "integer"},
            "Status":          {"type": "keyword"},
            "Studios":         {"type": "keyword"},
            "studio_category": {"type": "keyword"},
            "Members":         {"type": "integer"},
            "Favorites":       {"type": "integer"},
            "Rank":            {"type": "integer"},
            "Popularity":      {"type": "integer"},
            "completion_rate": {"type": "float"},
            "drop_rate":       {"type": "float"},
            "synopsis":        {"type": "text"}
        }
    }
}


def get_es_client():
    """Retourne un client Elasticsearch connecté."""
    es = Elasticsearch(ES_HOST)
    if not es.ping():
        raise ConnectionError(f"Impossible de joindre Elasticsearch à {ES_HOST}")
    logger.info(f"Connecté à Elasticsearch : {ES_HOST}")
    return es


def create_index(es: Elasticsearch, index: str = INDEX_NAME):
    """Crée l'index avec le mapping si il n'existe pas."""
    if es.indices.exists(index=index):
        logger.info(f"Index '{index}' existe déjà")
        return
    es.indices.create(index=index, body=ANIME_MAPPING)
    logger.info(f"Index '{index}' créé")


def load_to_elasticsearch(df: pd.DataFrame, index: str = INDEX_NAME):
    """Indexe le dataframe dans Elasticsearch en bulk."""
    es = get_es_client()
    create_index(es, index)

    # Préparer les documents
    def generate_docs(df):
        for _, row in df.iterrows():
            doc = row.to_dict()
            # Nettoyer les NaN (non sérialisables en JSON)
            doc = {k: (None if isinstance(v, float) and np.isnan(v) else v)
                   for k, v in doc.items()}
            yield {
                "_index": index,
                "_id": doc.get("MAL_ID"),
                "_source": doc
            }

    success, errors = helpers.bulk(es, generate_docs(df), raise_on_error=False)
    logger.info(f"Indexés : {success:,} documents, erreurs : {len(errors)}")
    return success, errors
