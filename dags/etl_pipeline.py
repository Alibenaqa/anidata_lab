"""
DAG etl_pipeline — Pipeline ETL complet (Extract → Transform → Load)
Déclenche les 3 DAGs dans l'ordre via TriggerDagRunOperator.
"""
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from datetime import datetime, timedelta

default_args = {
    'owner': 'sakura-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL complet AniData Lab — Extract → Transform → Load',
    schedule='0 2 * * *',  # Tous les jours à 2h du matin
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['etl', 'pipeline', 'anidata'],
) as dag:

    def pipeline_start(**context):
        print("="*50)
        print("PIPELINE ETL ANIDATA LAB — DÉMARRAGE")
        print(f"Run ID : {context['run_id']}")
        print(f"Date   : {context['execution_date']}")
        print("="*50)

    def pipeline_end(**context):
        print("="*50)
        print("PIPELINE ETL ANIDATA LAB — TERMINÉ")
        print("Dashboard Grafana mis à jour !")
        print("="*50)

    t_start = PythonOperator(task_id='pipeline_start', python_callable=pipeline_start)

    t_extract = TriggerDagRunOperator(
        task_id='trigger_extract',
        trigger_dag_id='extract_anime',
        wait_for_completion=True,
        poke_interval=30,
    )

    t_transform = TriggerDagRunOperator(
        task_id='trigger_transform',
        trigger_dag_id='transform_anime',
        wait_for_completion=True,
        poke_interval=30,
    )

    t_load = TriggerDagRunOperator(
        task_id='trigger_load',
        trigger_dag_id='load_elasticsearch',
        wait_for_completion=True,
        poke_interval=30,
    )

    t_end = PythonOperator(task_id='pipeline_end', python_callable=pipeline_end)

    t_start >> t_extract >> t_transform >> t_load >> t_end
