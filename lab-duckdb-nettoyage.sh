#!/usr/bin/env bash
# Suppression des objets du lab DuckDB, la couche bronze est gardée
set -e
cd "$(dirname "$0")"
export AWS_PAGER=""
LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/analytics/" --recursive
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/large/" --recursive
rm -f orders_large.csv
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
