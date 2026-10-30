#!/usr/bin/env bash
# Lab dbt complet sur Onyxia (sources bronze, silver, tests, docs, export Parquet, exercices)
# Lancement : bash lab-dbt.sh 2>&1 | tee sortie-dbt.txt
# Pas de set -e : un test en échec ne doit pas arrêter la suite
cd "$(dirname "$0")"
export AWS_PAGER=""
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
echo "$LAB_BUCKET_NAME"
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/bronze/" || exit 1

echo "=== Installation"
uv add dbt-duckdb
uv run dbt --version

cd lab_medallion
echo "=== dbt debug"
uv run dbt debug

echo "=== Sources bronze"
uv run dbt show -q --limit 3 \
  --inline "select uuid, user_uuid, date, quantity, product from {{ source('bronze', 'orders') }}"
uv run dbt show -q --limit 3 --inline "select address from {{ source('bronze', 'users') }}" --output json

echo "=== Seeds (référentiels states et zip_ranges)"
uv run dbt seed

echo "=== dbt run silver"
uv run dbt run --select silver
echo "--- compilé"
cat target/compiled/lab_medallion/models/silver/stg_users.sql
echo "--- exécuté"
cat target/run/lab_medallion/models/silver/stg_users.sql

duckdb -readonly lab_medallion.duckdb <<'SQL'
SELECT table_schema, table_name, table_type FROM information_schema.tables ORDER BY ALL;
SELECT username, username_normalized, street, city, state, zip_code FROM main_silver.stg_users LIMIT 3;
DESCRIBE main_silver.stg_orders;
DESCRIBE main_silver.stg_users;
-- Exercice 3 : dates de naissance invalides remplacées par NULL
SELECT birthdate_is_valid, count(*) AS users FROM main_silver.stg_users GROUP BY ALL;
-- Commandes qui auraient échoué au test avec les dates brutes
SELECT count(*) AS orders_before_birth
FROM main_silver.stg_orders o
JOIN read_csv('s3://' || getenv('LAB_BUCKET_NAME') || '/bronze/users.csv') u ON o.user_id = cast(u.uuid AS uuid)
WHERE o.ordered_at < u.birthdate;
-- Taille d'un UUID en VARCHAR
SELECT octet_length(cast(order_id AS varchar)) AS varchar_bytes FROM main_silver.stg_orders LIMIT 1;
SQL

echo "=== dbt test silver (avec stockage des échecs)"
uv run dbt test --select silver --store-failures
duckdb -readonly lab_medallion.duckdb <<'SQL'
SELECT table_name FROM information_schema.tables WHERE table_schema = 'main_dbt_test__audit' ORDER BY ALL;
SELECT * FROM main_dbt_test__audit.assert_users_zip_matches_state LIMIT 5;
SQL

echo "=== Pipeline complet"
rm -f lab_medallion.duckdb
uv run dbt seed
uv run dbt build --select silver

echo "=== Lignage"
uv run dbt ls -q --resource-type model
uv run dbt ls -q --resource-type source model seed --select +stg_orders
uv run dbt ls -q --resource-type source model seed --select +stg_users

echo "=== Documentation"
uv run dbt docs generate --static
ls -lh target/static_index.html

echo "=== Export Parquet silver"
for model in users orders; do
  duckdb lab_medallion.duckdb -c "
    COPY (SELECT * FROM main_silver.stg_$model)
    TO 's3://$LAB_BUCKET_NAME/silver/dbt/$model.parquet' (FORMAT parquet);"
done
aws s3 --profile default ls "s3://$LAB_BUCKET_NAME/silver/dbt/"
duckdb -readonly lab_medallion.duckdb -c "
  SELECT sex, count(*) AS users
  FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/users.parquet')
  GROUP BY sex ORDER BY sex;"
# Exercice : le Parquet et la vue doivent contenir exactement les mêmes lignes
duckdb -readonly lab_medallion.duckdb -c "
  SELECT
    (SELECT count(*) FROM main_silver.stg_orders) AS view_rows,
    (SELECT count(*) FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet')) AS parquet_rows,
    (SELECT count(*) FROM (
      FROM main_silver.stg_orders
      EXCEPT ALL
      FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet'))) AS only_in_view,
    (SELECT count(*) FROM (
      FROM read_parquet('s3://$LAB_BUCKET_NAME/silver/dbt/orders.parquet')
      EXCEPT ALL
      FROM main_silver.stg_orders)) AS only_in_parquet;"

echo "=== Exercice 5 : matérialisation en table"
ls -lh lab_medallion.duckdb
duckdb -readonly lab_medallion.duckdb -cmd ".timer on" -c "SELECT count(*) FROM main_silver.stg_orders;"
sed -i 's/+materialized: view/+materialized: table/' dbt_project.yml
uv run dbt build --select silver
duckdb -readonly lab_medallion.duckdb -c "
  SELECT table_schema, table_name, table_type FROM information_schema.tables
  WHERE table_schema = 'main_silver' ORDER BY ALL;"
ls -lh lab_medallion.duckdb
duckdb -readonly lab_medallion.duckdb -cmd ".timer on" -c "SELECT count(*) FROM main_silver.stg_orders;"
# Restauration de la configuration d'origine
sed -i 's/+materialized: table/+materialized: view/' dbt_project.yml
uv run dbt build --select silver
grep materialized dbt_project.yml

echo "=== Nettoyage (couche gold, la bronze est conservée)"
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/gold/" --recursive
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
echo "=== Terminé"
