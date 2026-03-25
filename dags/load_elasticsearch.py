"""
DAG load_elasticsearch — Séance 7
Indexation du dataset gold dans Elasticsearch + monitoring.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'sakura-analytics',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='load_elasticsearch',
    default_args=default_args,
    description='Chargement du dataset gold dans Elasticsearch',
    schedule=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['load', 'elasticsearch', 'anidata'],
) as dag:

    def check_es_connection(**context):
        from load.load_elasticsearch import get_es_client
        es = get_es_client()
        info = es.info()
        print(f"Elasticsearch {info['version']['number']} — OK")
        context['ti'].xcom_push(key='es_version', value=info['version']['number'])

    def load_gold(**context):
        import pandas as pd
        from load.load_elasticsearch import load_to_elasticsearch

        gold_path = '/opt/airflow/data/gold/anime_gold.json'
        if not os.path.exists(gold_path):
            raise FileNotFoundError(f"Dataset gold introuvable : {gold_path}")

        gold_df = pd.read_json(gold_path)
        print(f"Chargement de {len(gold_df):,} documents dans Elasticsearch...")

        success, errors = load_to_elasticsearch(gold_df)
        context['ti'].xcom_push(key='indexed_count', value=success)

        if errors:
            print(f"ATTENTION : {len(errors)} erreurs d'indexation")

    def verify_indexation(**context):
        from load.load_elasticsearch import get_es_client
        ti = context['ti']
        indexed_count = ti.xcom_pull(task_ids='load_gold', key='indexed_count')

        es = get_es_client()
        es.indices.refresh(index='anime-gold')
        count = es.count(index='anime-gold')['count']

        print(f"Documents dans Elasticsearch : {count:,}")
        print(f"Documents indexés ce run : {indexed_count:,}")

        if count < 1000:
            raise ValueError(f"Trop peu de documents indexés : {count}")

        print("Vérification OK !")

    def run_sample_query(**context):
        from load.load_elasticsearch import get_es_client
        es = get_es_client()

        # Test requête full-text sur synopsis
        result = es.search(
            index='anime-gold',
            body={
                "query": {"match": {"synopsis": "friendship adventure"}},
                "sort": [{"weighted_score": {"order": "desc"}}],
                "size": 5
            }
        )
        hits = result['hits']['hits']
        print(f"Top 5 animes 'friendship adventure' :")
        for hit in hits:
            src = hit['_source']
            print(f"  - {src.get('Name')} (score: {src.get('weighted_score')})")

    t_check = PythonOperator(task_id='check_es_connection', python_callable=check_es_connection)
    t_load = PythonOperator(task_id='load_gold', python_callable=load_gold)
    t_verify = PythonOperator(task_id='verify_indexation', python_callable=verify_indexation)
    t_query = PythonOperator(task_id='run_sample_query', python_callable=run_sample_query)

    t_check >> t_load >> t_verify >> t_query
