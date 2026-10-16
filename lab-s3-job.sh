#!/usr/bin/env bash
# Upload des datasets dans bronze/ depuis un Job Kubernetes
set -e
export AWS_PAGER=""
LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")

kubectl auth can-i create jobs
kubectl auth can-i create secrets

uv run dataset-users -o csv > users.csv
uv run dataset-orders -o csv > orders.csv
ls -lh users.csv orders.csv

# Relance possible : on supprime les objets d'une exécution précédente
kubectl delete -f job-upload-bronze.yaml --ignore-not-found
kubectl delete configmap datasets s3-config --ignore-not-found
kubectl delete secret s3-credentials --ignore-not-found

kubectl create configmap datasets --from-file=users.csv --from-file=orders.csv
kubectl create configmap s3-config \
  --from-literal=AWS_ENDPOINT_URL="$S3_ENDPOINT_URL" \
  --from-literal=AWS_DEFAULT_REGION="$(aws configure get region --profile 'default')" \
  --from-literal=LAB_BUCKET_NAME="$LAB_BUCKET_NAME"
# Identifiants exportés dans un fichier temporaire, jamais affichés
aws configure export-credentials --profile 'default' --format env-no-export \
  | grep -E '^AWS_(ACCESS_KEY_ID|SECRET_ACCESS_KEY|SESSION_TOKEN)=' > s3.env
kubectl create secret generic s3-credentials --from-env-file=s3.env
rm s3.env

kubectl apply -f job-upload-bronze.yaml
kubectl wait --for=condition=complete job/upload-bronze --timeout=120s
kubectl logs job/upload-bronze

echo "Vérification depuis le terminal :"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/" --recursive

# Les objets Kubernetes sont supprimés, les fichiers de bronze/ sont gardés pour les modules suivants
kubectl delete -f job-upload-bronze.yaml
kubectl delete configmap datasets s3-config
kubectl delete secret s3-credentials
