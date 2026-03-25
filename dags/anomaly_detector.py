"""
DAG anomaly_detector — Séance 8
Détection d'anomalies dans les ratings (spam, notes suspectes).
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'sakura-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='anomaly_detector',
    default_args=default_args,
    description='Détection d\'anomalies dans les ratings MyAnimeList',
    schedule='0 3 * * *',  # Après le pipeline ETL
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['anomalies', 'qualite', 'anidata'],
) as dag:

    def detect_rating_spam(**context):
        """Détecte les utilisateurs avec un volume anormal de ratings."""
        import pandas as pd
        import numpy as np

        ratings_path = '/opt/airflow/data/raw/rating_complete.csv'
        df = pd.read_csv(ratings_path, nrows=500_000)

        # Règle 1 : utilisateurs avec > 1000 ratings (potentiel spam)
        user_counts = df.groupby('user_id')['rating'].count()
        spam_users = user_counts[user_counts > 1000]
        print(f"Utilisateurs suspects (>1000 ratings) : {len(spam_users)}")

        # Règle 2 : utilisateurs avec 100% de la même note
        user_rating_std = df.groupby('user_id')['rating'].std()
        zero_std_users = user_rating_std[user_rating_std == 0]
        print(f"Utilisateurs avec note unique (std=0) : {len(zero_std_users)}")

        anomalies = {
            'spam_users_count': len(spam_users),
            'zero_variance_users': len(zero_std_users),
            'total_users': df['user_id'].nunique(),
        }
        context['ti'].xcom_push(key='rating_anomalies', value=anomalies)
        print(f"Anomalies ratings : {anomalies}")

    def detect_score_anomalies(**context):
        """Détecte les animes avec des scores suspects."""
        import pandas as pd
        import numpy as np

        gold_path = '/opt/airflow/data/gold/anime_gold.json'
        df = pd.read_json(gold_path)

        # Règle 3 : animes avec score très élevé mais peu de votes
        suspicious = df[
            (df['Score'] >= 9.0) & (df['Scored By'] < 100)
        ]
        print(f"Animes score >= 9.0 avec < 100 votes : {len(suspicious)}")
        if not suspicious.empty:
            print(suspicious[['Name', 'Score', 'Scored By']].to_string())

        # Règle 4 : animes avec completion_rate anormalement bas
        if 'completion_rate' in df.columns:
            low_completion = df[df['completion_rate'] < 0.05]
            print(f"Animes avec completion_rate < 5% : {len(low_completion)}")

        anomalies = {
            'suspicious_high_score': len(suspicious),
        }
        context['ti'].xcom_push(key='score_anomalies', value=anomalies)

    def generate_anomaly_report(**context):
        """Génère un rapport consolidé des anomalies."""
        ti = context['ti']
        rating_anomalies = ti.xcom_pull(task_ids='detect_rating_spam', key='rating_anomalies')
        score_anomalies = ti.xcom_pull(task_ids='detect_score_anomalies', key='score_anomalies')

        print("\n" + "="*50)
        print("RAPPORT D'ANOMALIES — AniData Lab")
        print("="*50)
        print(f"Date : {context['execution_date']}")
        print()
        print("RATINGS :")
        for k, v in (rating_anomalies or {}).items():
            print(f"  {k} : {v}")
        print()
        print("SCORES :")
        for k, v in (score_anomalies or {}).items():
            print(f"  {k} : {v}")
        print("="*50)

    def index_anomalies(**context):
        """Indexe le rapport d'anomalies dans Elasticsearch."""
        from load.load_elasticsearch import get_es_client
        import json
        from datetime import datetime as dt

        ti = context['ti']
        rating_anomalies = ti.xcom_pull(task_ids='detect_rating_spam', key='rating_anomalies') or {}
        score_anomalies = ti.xcom_pull(task_ids='detect_score_anomalies', key='score_anomalies') or {}

        es = get_es_client()
        doc = {
            'timestamp': dt.utcnow().isoformat(),
            'run_id': context['run_id'],
            **rating_anomalies,
            **score_anomalies
        }
        es.index(index='anidata-anomalies', body=doc)
        print(f"Rapport d'anomalies indexé dans 'anidata-anomalies'")

    t_ratings = PythonOperator(
        task_id='detect_rating_spam',
        python_callable=detect_rating_spam,
    )

    t_scores = PythonOperator(
        task_id='detect_score_anomalies',
        python_callable=detect_score_anomalies,
    )

    t_report = PythonOperator(
        task_id='generate_anomaly_report',
        python_callable=generate_anomaly_report,
    )

    t_index = PythonOperator(
        task_id='index_anomalies',
        python_callable=index_anomalies,
    )

    # Détection en parallèle, puis rapport
    [t_ratings, t_scores] >> t_report >> t_index
