#!/usr/bin/env bash
# Lab 10 Trino, parties 2 et 3 (apres bash lab-10/lab-trino-deploy.sh)
# Lancement depuis ~/work/bigdata : bash lab-10/lab-trino.sh > lab-10/sortie-trino.txt 2>&1
cd "$(dirname "$0")/.."
eval "$(aws configure export-credentials --profile default --format env)"
export AWS_PAGER=""
export AWS_REGION="us-east-1"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export TRINO_HOST=trino
export TRINO_PORT=8080
export TRINO_USER="$KUBERNETES_NAMESPACE"
set -a; . /home/onyxia/.polaris-credentials.env; set +a

echo "################ prep : tables silver visibles dans Polaris (Spark)"
python3 -u lab-10/spark_polaris.py prep 2>/dev/null
echo "################ dependances"
uv add trino pandas
echo "################ labs 2 et 3 + exercices 1 a 4 (Trino)"
uv run python -u lab-10/lab_trino.py
echo "################ exercice 5 (Spark)"
python3 -u lab-10/spark_polaris.py exo5 2>/dev/null
echo "################ fin"
