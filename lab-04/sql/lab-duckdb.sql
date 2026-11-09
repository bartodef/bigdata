-- Lab DuckDB : requêtes de l'énoncé, dans l'ordre
-- Lancement : duckdb analytics.duckdb -init init.sql < sql/lab-duckdb.sql
.echo on

SELECT getvariable('bucket') AS bucket;

-- Couche bronze
SELECT uuid, username, name, birthdate
FROM read_csv(getvariable('bucket') || '/bronze/users.csv', header = true, strict_mode = false)
LIMIT 3;
SELECT Delimiter, Quote, HasHeader
FROM sniff_csv(getvariable('bucket') || '/bronze/users.csv');
DESCRIBE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
SELECT column_name, column_type, approx_unique, null_percentage
FROM (SUMMARIZE FROM read_csv(getvariable('bucket') || '/bronze/orders.csv'));

-- Chargement des tables
CREATE OR REPLACE TABLE users AS
FROM read_csv(getvariable('bucket') || '/bronze/users.csv');
CREATE OR REPLACE TABLE orders AS
FROM read_csv(getvariable('bucket') || '/bronze/orders.csv');
.tables
SELECT
  (SELECT count(*) FROM users) AS users,
  (SELECT count(*) FROM orders) AS orders;
SELECT min(date) AS first_order, max(date) AS last_order FROM orders;

-- Qualité
SELECT count(*) AS orphan_orders FROM orders o ANTI JOIN users u ON o.user_uuid = u.uuid;
SELECT count(*) AS inactive_users FROM users u ANTI JOIN orders o ON o.user_uuid = u.uuid;
SELECT uuid, count(*) AS occurrences FROM orders GROUP BY uuid HAVING count(*) > 1;

-- Agrégations
SELECT
  product,
  count(*) AS orders,
  sum(quantity) AS quantity,
  round(100 * sum(quantity) / sum(sum(quantity)) OVER (), 1) AS share_pct
FROM orders
GROUP BY product
ORDER BY quantity DESC;
SELECT date_trunc('month', date) AS month, count(*) AS orders, sum(quantity) AS quantity
FROM orders
GROUP BY month
ORDER BY month;

-- Jointures
SELECT u.username, u.name, count(*) AS orders, sum(o.quantity) AS quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY quantity DESC
LIMIT 5;
SELECT
  (date_diff('year', u.birthdate, DATE '2020-01-01') // 20) * 20 AS age_group,
  count(DISTINCT u.uuid) AS users,
  count(*) AS orders,
  round(avg(o.quantity), 2) AS avg_quantity
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY age_group
ORDER BY age_group;

-- Fonctions de fenêtrage
WITH daily AS (
  SELECT date::DATE AS day, sum(quantity) AS quantity
  FROM orders
  GROUP BY day
)
SELECT
  day,
  quantity,
  sum(quantity) OVER (ORDER BY day) AS cumulative,
  round(avg(quantity) OVER (ORDER BY day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 1) AS avg_7d
FROM daily
ORDER BY day
LIMIT 10;
SELECT strftime(date, '%Y-%m') AS month, product, sum(quantity) AS quantity
FROM orders
GROUP BY month, product
QUALIFY rank() OVER (PARTITION BY month ORDER BY sum(quantity) DESC) = 1
ORDER BY month;

-- Pivot
PIVOT (SELECT strftime(date, '%Y-%m') AS month, product, quantity FROM orders)
ON product
USING sum(quantity)
ORDER BY month;

-- Export Parquet
COPY orders TO (getvariable('bucket') || '/analytics/orders.parquet') (FORMAT parquet);
SELECT path_in_schema, type, compression, total_compressed_size, total_uncompressed_size
FROM parquet_metadata(getvariable('bucket') || '/analytics/orders.parquet');

-- Partitionnement Hive
COPY orders TO (getvariable('bucket') || '/analytics/orders_by_product') (FORMAT parquet, PARTITION_BY (product), OVERWRITE_OR_IGNORE);
SELECT file FROM glob(getvariable('bucket') || '/analytics/orders_by_product/**');
EXPLAIN ANALYZE
SELECT count(*)
FROM read_parquet(getvariable('bucket') || '/analytics/orders_by_product/*/*.parquet')
WHERE product = 'cookie';
