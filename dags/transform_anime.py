"""
DAG transform_anime — Séance 6
Nettoyage et feature engineering avec BranchPythonOperator et versioning.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.insert(0, '/opt/airflow/src')

default_args = {
    'owner': 'sakura-analytics',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

GOLD_PATH = '/opt/airflow/data/gold'


with DAG(
    dag_id='transform_anime',
    default_args=default_args,
    description='Nettoyage, enrichissement et feature engineering',
    schedule=None,
    start_date=datetime(2026, 3, 23),
    catchup=False,
    tags=['transform', 'anidata'],
) as dag:

    def load_raw(**context):
        from extract.extract_anime import extract_anime, extract_synopsis
        import pandas as pd

        anime_df = extract_anime()
        synopsis_df = extract_synopsis()

        context['ti'].xcom_push(key='anime_rows', value=len(anime_df))
        # Sauvegarder en parquet temporaire pour la task suivante
        os.makedirs('/tmp/anidata', exist_ok=True)
        anime_df.to_parquet('/tmp/anidata/anime_raw.parquet', index=False)
        synopsis_df.to_parquet('/tmp/anidata/synopsis_raw.parquet', index=False)
        print(f"Données brutes chargées : {len(anime_df):,} animes")

    def check_data_quality(**context):
        import pandas as pd
        df = pd.read_parquet('/tmp/anidata/anime_raw.parquet')
        missing_score_pct = df['Score'].isna().mean()
        print(f"% scores manquants : {missing_score_pct:.1%}")
        # Si trop de données manquantes → nettoyage renforcé
        if missing_score_pct > 0.3:
            return 'clean_heavy'
        return 'clean_standard'

    def clean_standard(**context):
        import pandas as pd
        from transform.transform_anime import clean_anime
        df = pd.read_parquet('/tmp/anidata/anime_raw.parquet')
        df_clean = clean_anime(df)
        df_clean.to_parquet('/tmp/anidata/anime_clean.parquet', index=False)
        print(f"Nettoyage standard terminé : {len(df_clean):,} lignes")

    def clean_heavy(**context):
        import pandas as pd
        from transform.transform_anime import clean_anime
        df = pd.read_parquet('/tmp/anidata/anime_raw.parquet')
        df_clean = clean_anime(df)
        # Nettoyage renforcé : supprimer les lignes sans score ET sans membres
        df_clean = df_clean.dropna(subset=['Score', 'Members'], how='all')
        df_clean.to_parquet('/tmp/anidata/anime_clean.parquet', index=False)
        print(f"Nettoyage renforcé terminé : {len(df_clean):,} lignes")

    def enrich(**context):
        import pandas as pd
        from transform.transform_anime import enrich_anime
        anime_df = pd.read_parquet('/tmp/anidata/anime_clean.parquet')
        synopsis_df = pd.read_parquet('/tmp/anidata/synopsis_raw.parquet')
        gold_df = enrich_anime(anime_df, synopsis_df)
        gold_df.to_parquet('/tmp/anidata/anime_gold.parquet', index=False)
        context['ti'].xcom_push(key='gold_rows', value=len(gold_df))
        print(f"Enrichissement terminé : {len(gold_df):,} animes, {gold_df.shape[1]} colonnes")

    def export_gold(**context):
        import pandas as pd
        gold_df = pd.read_parquet('/tmp/anidata/anime_gold.parquet')
        os.makedirs(GOLD_PATH, exist_ok=True)

        # Versioning
        version = context['execution_date'].strftime('%Y%m%d_%H%M')
        csv_path = os.path.join(GOLD_PATH, f'anime_gold.csv')
        json_path = os.path.join(GOLD_PATH, f'anime_gold.json')

        # Convertir les listes en strings pour CSV
        export_df = gold_df.copy()
        for col in ['genres_list', 'studios_list', 'producers_list']:
            if col in export_df.columns:
                export_df[col] = export_df[col].apply(
                    lambda x: ','.join(x) if isinstance(x, list) else x
                )

        export_df.to_csv(csv_path, index=False, encoding='utf-8')
        gold_df.to_json(json_path, orient='records', force_ascii=False)

        print(f"Dataset gold exporté : {csv_path}")
        print(f"Version : {version}")
        print(f"Lignes : {len(gold_df):,}")

    t_load_raw = PythonOperator(task_id='load_raw', python_callable=load_raw)

    t_check_quality = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=check_data_quality,
    )

    t_clean_standard = PythonOperator(task_id='clean_standard', python_callable=clean_standard)
    t_clean_heavy = PythonOperator(task_id='clean_heavy', python_callable=clean_heavy)

    t_enrich = PythonOperator(
        task_id='enrich',
        python_callable=enrich,
        trigger_rule='none_failed_min_one_success',
    )

    t_export = PythonOperator(task_id='export_gold', python_callable=export_gold)

    t_load_raw >> t_check_quality >> [t_clean_standard, t_clean_heavy] >> t_enrich >> t_export
