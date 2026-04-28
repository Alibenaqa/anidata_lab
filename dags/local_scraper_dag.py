from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import sys
sys.path.insert(0, "/opt/airflow/src")

default_args = {
    "owner": "sakura-analytics",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

def run_local_scraper(**context):
    from anidata_scraper import scrape_to_file
    filepath = scrape_to_file(
        output_dir="/opt/airflow/data/raw",
        base_url="http://host.docker.internal:8088",
        enrich=True,
    )
    context["ti"].xcom_push(key="scraped_file", value=filepath)
    print(f"Fichier produit (scraping local) : {filepath}")
    return filepath

with DAG(
    dag_id="local_scraper_dag",
    description="Scrape le mock-site via le port local (host.docker.internal:8088)",
    default_args=default_args,
    start_date=datetime(2026, 4, 28),
    schedule="*/5 * * * *",
    catchup=False,
    tags=["scraping", "local", "anidata"],
) as dag:

    scrape = PythonOperator(
        task_id="scrape_local",
        python_callable=run_local_scraper,
    )
