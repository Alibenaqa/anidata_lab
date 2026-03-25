"""
DAG hello_anidata — Séance 4
Premier DAG de découverte : log, branchement simple, paramètres.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    'owner': 'sakura-analytics',
    'retries': 1,
}

with DAG(
    dag_id='hello_anidata',
    default_args=default_args,
    description='Premier DAG AniData Lab',
    schedule=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['intro', 'anidata'],
) as dag:

    def say_hello(**context):
        print("Bienvenue sur AniData Lab — Sakura Analytics !")
        print(f"DAG run ID : {context['run_id']}")
        print(f"Exécution : {context['execution_date']}")

    def check_data_exists(**context):
        import os
        raw_path = '/opt/airflow/data/raw'
        files = ['anime.csv', 'rating_complete.csv', 'anime_with_synopsis.csv']
        all_exist = all(os.path.exists(os.path.join(raw_path, f)) for f in files)
        return 'data_ready' if all_exist else 'data_missing'

    def data_ready(**context):
        print("Tous les fichiers CSV sont présents. Prêt pour l'ETL !")

    def data_missing(**context):
        print("ATTENTION : Fichiers CSV manquants dans /opt/airflow/data/raw")

    task_hello = PythonOperator(
        task_id='say_hello',
        python_callable=say_hello,
    )

    task_check = BranchPythonOperator(
        task_id='check_data',
        python_callable=check_data_exists,
    )

    task_ready = PythonOperator(
        task_id='data_ready',
        python_callable=data_ready,
    )

    task_missing = PythonOperator(
        task_id='data_missing',
        python_callable=data_missing,
    )

    task_end = EmptyOperator(task_id='end', trigger_rule='none_failed_min_one_success')

    task_hello >> task_check >> [task_ready, task_missing] >> task_end
