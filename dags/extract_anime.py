"""
DAG extract_anime — Séances 4 & 5
Extraction multi-fichiers avec validation de schéma, retries et XComs.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import sys

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'sakura-analytics',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
    'email_on_failure': False,
}

with DAG(
    dag_id='extract_anime',
    default_args=default_args,
    description='Extraction et validation des fichiers CSV bruts',
    schedule=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['extract', 'anidata'],
) as dag:

    def extract_anime_task(**context):
        from extract.extract_anime import extract_anime
        df = extract_anime()
        context['ti'].xcom_push(key='anime_shape', value=list(df.shape))
        print(f"anime.csv extrait : {df.shape}")

    def extract_ratings_task(**context):
        from extract.extract_anime import extract_ratings
        # Limité à 1M pour les tests (retirer nrows en prod)
        df = extract_ratings(nrows=1_000_000)
        context['ti'].xcom_push(key='ratings_shape', value=list(df.shape))
        print(f"ratings extrait : {df.shape}")

    def extract_synopsis_task(**context):
        from extract.extract_anime import extract_synopsis
        df = extract_synopsis()
        context['ti'].xcom_push(key='synopsis_shape', value=list(df.shape))
        print(f"synopsis extrait : {df.shape}")

    def validate_extraction(**context):
        ti = context['ti']
        anime_shape = ti.xcom_pull(task_ids='extract_anime_csv', key='anime_shape')
        ratings_shape = ti.xcom_pull(task_ids='extract_ratings_csv', key='ratings_shape')
        synopsis_shape = ti.xcom_pull(task_ids='extract_synopsis_csv', key='synopsis_shape')

        print("=== RAPPORT D'EXTRACTION ===")
        print(f"anime.csv       : {anime_shape[0]:,} lignes x {anime_shape[1]} colonnes")
        print(f"ratings         : {ratings_shape[0]:,} lignes x {ratings_shape[1]} colonnes")
        print(f"synopsis        : {synopsis_shape[0]:,} lignes x {synopsis_shape[1]} colonnes")

        if anime_shape[0] < 1000:
            raise ValueError(f"anime.csv trop petit : {anime_shape[0]} lignes (attendu > 1000)")

        print("Validation réussie !")

    t_anime = PythonOperator(
        task_id='extract_anime_csv',
        python_callable=extract_anime_task,
    )

    t_ratings = PythonOperator(
        task_id='extract_ratings_csv',
        python_callable=extract_ratings_task,
    )

    t_synopsis = PythonOperator(
        task_id='extract_synopsis_csv',
        python_callable=extract_synopsis_task,
    )

    t_validate = PythonOperator(
        task_id='validate_extraction',
        python_callable=validate_extraction,
    )

    # Extraction parallèle, puis validation
    [t_anime, t_ratings, t_synopsis] >> t_validate
