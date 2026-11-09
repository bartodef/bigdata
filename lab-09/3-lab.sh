#!/usr/bin/env bash
# Lab Polaris : partie Spark (a lancer depuis ~/work/bigdata apres 2-deploiement.sh)
# Lancement : bash lab-09/3-lab.sh > sortie-polaris.txt 2>/dev/null
cd "$(dirname "$0")/.."
eval "$(aws configure export-credentials --profile default --format env)"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export AWS_REGION="us-east-1"
export AWS_PAGER=""
set -a; . /home/onyxia/.polaris-credentials.env; set +a
for etape in dev analyst verif exo2 nettoyage; do
  echo "################ $etape"
  python3 -u lab-09/lab_polaris.py $etape
done
echo "################ fin"
