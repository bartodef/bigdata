#!/usr/bin/env bash
# Lab DuckDB complet sur Onyxia
# Lancement : bash lab-duckdb.sh 2>&1 | tee sortie-duckdb.txt
set -e
cd "$(dirname "$0")"
export AWS_PAGER=""
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
echo "$S3_ENDPOINT_URL $LAB_BUCKET_NAME"
command -v duckdb
duckdb --version
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"

cat > init.sql <<SQL
SET VARIABLE bucket = 's3://$LAB_BUCKET_NAME';
SET TimeZone = 'UTC';
SQL
cat init.sql

echo "=== Secret S3"
duckdb -c "SELECT name, type, provider, persistent, storage FROM duckdb_secrets();"
duckdb -c "SELECT secret_string FROM duckdb_secrets();" | grep -oE "endpoint=[^;]*|url_style=[^;]*" || true
# Les identifiants du secret Onyxia expirent : on le recrée avec ceux du profil default
if ! duckdb -init init.sql -c "SELECT count(*) AS users FROM read_csv(getvariable('bucket') || '/bronze/users.csv')"; then
  echo "Secret S3 expiré, recréation depuis le profil default"
  duckdb -c "
    DROP PERSISTENT SECRET IF EXISTS s3_onyxia_connection;
    CREATE PERSISTENT SECRET s3_onyxia_connection (
      TYPE s3,
      KEY_ID '$(aws configure get aws_access_key_id --profile 'default')',
      SECRET '$(aws configure get aws_secret_access_key --profile 'default')',
      SESSION_TOKEN '$(aws configure get aws_session_token --profile 'default')',
      ENDPOINT '${S3_ENDPOINT_URL#https://}',
      REGION '$(aws configure get region --profile 'default' || echo us-east-1)',
      URL_STYLE 'path');"
  duckdb -init init.sql -c "SELECT count(*) AS users FROM read_csv(getvariable('bucket') || '/bronze/users.csv')"
fi

echo "=== Requêtes du lab"
duckdb analytics.duckdb -init init.sql < sql/lab-duckdb.sql
echo "=== Exercices"
duckdb analytics.duckdb -init init.sql < sql/exercices.sql

echo "=== CSV vs Parquet à grande échelle"
time uv run dataset-orders -u 5000 -o csv > orders_large.csv
ls -lh orders_large.csv
aws s3 --profile 'default' cp orders_large.csv "s3://$LAB_BUCKET_NAME/large/orders.csv"
duckdb analytics.duckdb -init init.sql < sql/large.sql
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/large/"
for query in \
  "FROM read_csv(getvariable('bucket') || '/large/orders.csv', strict_mode=false) SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT product, sum(quantity) GROUP BY product" \
  "FROM read_parquet(getvariable('bucket') || '/large/orders.parquet') SELECT count(*) WHERE date >= '2100-01-01'"
do
  echo "$query"
  duckdb -init init.sql -c "SET enable_external_file_cache = false; EXPLAIN ANALYZE $query" 2>/dev/null \
  | grep -E 'in:|#GET|Total Time|Scanning'
done

echo "=== API Python"
uv run orders-report
uv run orders-report -p cookie
echo "=== Terminé"
