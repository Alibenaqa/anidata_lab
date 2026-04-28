FROM apache/airflow:2.10.4-python3.10

LABEL org.opencontainers.image.title="AniData Lab Airflow"
LABEL org.opencontainers.image.description="Airflow custom pour AniData Lab — scraper + DAGs"
LABEL org.opencontainers.image.source="https://github.com/Alibenaqa/anidata_lab"

USER airflow

# Dépendances Python
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# Code source (scraper + modules ETL)
COPY --chown=airflow:root src/ /opt/airflow/src/

# DAGs
COPY --chown=airflow:root dags/ /opt/airflow/dags/

# Dossier de données partagé avec le volume
RUN mkdir -p /opt/airflow/data/raw

# Vérification
RUN python -c "import sys; sys.path.insert(0, '/opt/airflow/src'); import anidata_scraper; print(f'AniData Scraper {anidata_scraper.__version__} OK')"
