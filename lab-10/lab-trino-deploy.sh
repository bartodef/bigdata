#!/usr/bin/env bash
# Lab 10 Trino, partie 1 : deploiement de Trino avec Helm (k8s)
# Prerequis : Polaris deploye (bash lab-09/2-deploiement.sh) et ~/.polaris-credentials.env present
# Lancement depuis ~/work/bigdata : bash lab-10/lab-trino-deploy.sh > lab-10/sortie-trino-deploy.txt 2>&1
set -euo pipefail
cd "$(dirname "$0")/.."
export AWS_PAGER=""

echo "=== Variables"
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
eval "$(aws configure export-credentials --profile default --format env)"
echo "$LAB_BUCKET_NAME"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/"

echo "=== Identifiants Polaris"
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo "dev: ${POLARIS_DEV_CREDENTIAL%%:*}:..."

echo "=== Fichiers du chart"
mkdir -p lab-10/deployment
cat <<'YAML' >lab-10/deployment/Chart.yaml
apiVersion: v2
name: trino
version: 1.45.0
dependencies:
  - name: trino
    version: 1.45.0
    repository: https://trinodb.github.io/charts
YAML
cat <<'YAML' >lab-10/deployment/values.yaml
trino:
  image:
    tag: "480"

  server:
    workers: 0 # Coordinator-only; enables node-scheduler.include-coordinator.
    config:
      query:
        maxMemory: "400MB"

  coordinator:
    jvm:
      maxHeapSize: "700M"
    resources:
      requests:
        cpu: 250m
        memory: 224Mi
      limits:
        cpu: 1000m
        memory: 1Gi
    config:
      nodeScheduler:
        includeCoordinator: true
      query:
        maxMemoryPerNode: "250MB"

  envFrom:
    - secretRef:
        name: trino-polaris-oauth

  env:
    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_ACCESS_KEY_ID
    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SECRET_ACCESS_KEY
    - name: AWS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SESSION_TOKEN

    - name: POLARIS_CATALOG
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: POLARIS_CATALOG
    - name: POLARIS_URI
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: POLARIS_URI
    - name: S3_ENDPOINT
      valueFrom:
        configMapKeyRef:
          name: polaris-config
          key: S3_ENDPOINT

  additionalCatalogs:
    lakehouse: |-
      connector.name=iceberg
      iceberg.catalog.type=rest
      iceberg.rest-catalog.uri=${ENV:POLARIS_URI}/api/catalog
      iceberg.rest-catalog.warehouse=${ENV:POLARIS_CATALOG}
      iceberg.rest-catalog.security=OAUTH2
      iceberg.rest-catalog.oauth2.server-uri=${ENV:POLARIS_URI}/api/catalog/v1/oauth/tokens
      iceberg.rest-catalog.oauth2.credential=${ENV:POLARIS_DEV_CREDENTIAL}
      iceberg.rest-catalog.oauth2.scope=PRINCIPAL_ROLE:ALL
      fs.native-s3.enabled=true
      s3.endpoint=${ENV:S3_ENDPOINT}
      s3.region=us-east-1
      s3.path-style-access=true

  service:
    type: ClusterIP

  ingress:
    enabled: false # Trino is unauthenticated here; use port-forward.
YAML

echo "=== Secret et ConfigMap"
kubectl create secret generic trino-polaris-oauth \
  --from-literal=POLARIS_DEV_CREDENTIAL="$POLARIS_DEV_CREDENTIAL" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create configmap trino-polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== Deploiement (2 a 3 minutes)"
helm repo add trino https://trinodb.github.io/charts || true
helm repo update
helm dependency update ./lab-10/deployment
helm upgrade --install trino ./lab-10/deployment --wait --timeout 10m

echo "=== Verification"
kubectl get pods -l app.kubernetes.io/name=trino
curl -s -i http://trino:8080/v1/info
echo
echo "=== Fin"
