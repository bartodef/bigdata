-- Lab DuckDB : exercices (tables users et orders de analytics.duckdb)
.echo on

-- 1. Nombre moyen de commandes par utilisateur, quantité moyenne par commande
SELECT
  round(count(*) / count(DISTINCT user_uuid), 2) AS avg_orders_per_user,
  round(avg(quantity), 2) AS avg_quantity_per_order
FROM orders;

-- 2. Première et dernière commande de chaque utilisateur, et nombre de jours entre les deux
SELECT
  u.username,
  min(o.date) AS first_order,
  max(o.date) AS last_order,
  date_diff('day', min(o.date), max(o.date)) AS days_between
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
ORDER BY first_order
LIMIT 10;

-- 3. Heure de la journée avec la plus grande quantité vendue
SELECT hour(date) AS hour, sum(quantity) AS quantity
FROM orders
GROUP BY hour
ORDER BY quantity DESC
LIMIT 3;

-- 4. Variation mensuelle de la quantité vendue par produit, en pourcentage
WITH monthly AS (
  SELECT product, strftime(date, '%Y-%m') AS month, sum(quantity) AS quantity
  FROM orders
  GROUP BY ALL
)
SELECT
  product,
  month,
  quantity,
  round(100 * (quantity - lag(quantity) OVER w) / lag(quantity) OVER w, 1) AS variation_pct
FROM monthly
WINDOW w AS (PARTITION BY product ORDER BY month)
ORDER BY product, month;

-- 5. Utilisateurs ayant commandé chaque produit au moins une fois
SELECT u.username, count(DISTINCT o.product) AS products
FROM orders o
JOIN users u ON o.user_uuid = u.uuid
GROUP BY ALL
HAVING count(DISTINCT o.product) = (SELECT count(DISTINCT product) FROM orders)
ORDER BY u.username;
SELECT count(*) AS users_all_products FROM (
  SELECT user_uuid FROM orders GROUP BY user_uuid
  HAVING count(DISTINCT product) = (SELECT count(DISTINCT product) FROM orders)
);
