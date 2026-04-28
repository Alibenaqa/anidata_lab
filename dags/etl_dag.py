from datetime import datetime, timedelta
from pathlib import Path
import json
import os
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "sakura-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def load_to_elasticsearch(**context):
    from elasticsearch import Elasticsearch, helpers

    raw_dir = Path("/opt/airflow/data/raw")
    files = sorted(raw_dir.glob("anime_*.json"), reverse=True)
    if not files:
        raise FileNotFoundError("Aucun fichier scraper trouvé dans /opt/airflow/data/raw")

    latest = files[0]
    print(f"Chargement du fichier : {latest}")
    data = json.loads(latest.read_text(encoding="utf-8"))
    animes = data.get("animes", [])

    es_host = os.getenv("ELASTICSEARCH_HOST", "http://host.docker.internal:9200")
    es = Elasticsearch(es_host)
    if not es.ping():
        raise ConnectionError(f"Impossible de se connecter à Elasticsearch ({es_host})")

    def gen_docs():
        for anime in animes:
            yield {
                "_index": "anidex-animes",
                "_id": anime.get("id"),
                "_source": {
                    **anime,
                    "@timestamp": datetime.utcnow().isoformat(),
                },
            }

    success, failed = helpers.bulk(es, gen_docs(), raise_on_error=False)
    print(f"Indexés : {success} | Erreurs : {len(failed)}")

with DAG(
    dag_id="etl_dag",
    description="Transforme et indexe les données scrapées dans Elasticsearch",
    default_args=default_args,
    start_date=datetime(2026, 4, 27),
    schedule=None,
    catchup=False,
    tags=["etl", "elasticsearch", "anidata"],
) as dag:

    load = PythonOperator(
        task_id="load_to_elasticsearch",
        python_callable=load_to_elasticsearch,
    )
