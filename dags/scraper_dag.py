from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys
sys.path.insert(0, "/opt/airflow/src")

default_args = {
    "owner": "sakura-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}#demo CI cdj

def run_scraper(**context):
    from anidata_scraper import scrape_to_file
    filepath = scrape_to_file(
        output_dir="/opt/airflow/data/raw",
        base_url="http://mock-site",
        enrich=True,
    )
    context["ti"].xcom_push(key="scraped_file", value=filepath)
    print(f"Fichier produit : {filepath}")
    return filepath

with DAG(
    dag_id="scraper_dag",
    description="Scrape le mock-site AniDex et déclenche le pipeline ETL",
    default_args=default_args,
    start_date=datetime(2026, 4, 27),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["scraping", "anidata"],
) as dag:

    scrape = PythonOperator(
        task_id="scrape_anidex",
        python_callable=run_scraper,
    )

    trigger_etl = TriggerDagRunOperator(
        task_id="trigger_etl_dag",
        trigger_dag_id="etl_dag",
        wait_for_completion=False,
    )

    scrape >> trigger_etl
