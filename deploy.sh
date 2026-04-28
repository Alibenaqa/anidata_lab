#!/bin/bash
set -e

echo "==> Pull de la dernière image depuis GHCR..."
docker pull ghcr.io/alibenaqa/anidata_lab-airflow:latest

echo "==> Redémarrage du stack Airflow..."
cd "$(dirname "$0")/docker/airflow"
docker compose down
docker compose up -d

echo "==> Stack relancé avec la dernière image !"
docker ps --format "table {{.Names}}\t{{.Status}}"
