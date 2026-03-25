.PHONY: elk airflow stop-elk stop-airflow logs-airflow status

# Lancer la stack ELK (Elasticsearch + Logstash + Grafana)
elk:
	cd docker/elk && docker compose up -d
	@echo "ELK démarré !"
	@echo "  Elasticsearch : http://localhost:9200"
	@echo "  Grafana        : http://localhost:3000  (admin / anidata123)"

# Lancer Airflow
airflow:
	cd docker/airflow && docker compose up airflow-init && docker compose up -d airflow-webserver airflow-scheduler
	@echo "Airflow démarré !"
	@echo "  UI : http://localhost:8080  (admin / admin123)"

# Lancer tout
all: elk airflow

# Arrêter ELK
stop-elk:
	cd docker/elk && docker compose down

# Arrêter Airflow
stop-airflow:
	cd docker/airflow && docker compose down

# Arrêter tout
stop:
	cd docker/elk && docker compose down
	cd docker/airflow && docker compose down

# Voir les logs Airflow
logs-airflow:
	cd docker/airflow && docker compose logs -f airflow-scheduler

# Status des containers
status:
	docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
