# Lab 10 Trino : labs 2 (silver -> gold) et 3 (star schema) + exercices
# Lancement depuis ~/work/bigdata : uv run python -u lab-10/lab_trino.py > lab-10/sortie-trino.txt 2>&1
# Les tables gold sont supprimees puis recreees : le script peut etre relance.
import os
import subprocess

import pandas as pd
import trino

pd.set_option("display.width", 250)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 90)

CATALOG = "lakehouse"
BUCKET = os.environ.get("LAB_BUCKET_NAME", os.environ.get("KUBERNETES_NAMESPACE"))

conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "onyxia")),
    http_scheme="http",
    catalog=CATALOG,
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()  # la requete n'est terminee qu'une fois les resultats lus
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(titre, statement):
    print(f"\n--- {titre}")
    try:
        df = sql(statement)
        if df.shape == (1, 1) and isinstance(df.iloc[0, 0], str) and "\n" in df.iloc[0, 0]:
            print(df.iloc[0, 0])  # plans EXPLAIN
        else:
            print(df.to_string())
        return df
    except Exception as e:
        print("ERREUR :", str(e)[:600])
        return None


def section(titre):
    print("\n" + "=" * 100 + f"\n=== {titre}\n" + "=" * 100)


def s3_ls(prefix):
    out = subprocess.run(["aws", "s3", "--profile", "default", "ls", "--recursive", f"s3://{BUCKET}/{prefix}"],
                         capture_output=True, text=True).stdout
    print(out if out else f"(rien sous {prefix})")


GOLD_TABLES = ["orders_by_state_month", "orders_by_state_month_part", "top_products", "user_activity",
               "cohorts", "dim_user", "dim_product", "dim_date", "fact_orders"]

# ---------------------------------------------------------------------------------------------
section("LAB 2 / Environnement")
show("version", "SELECT version()")
show("catalogue et schema de la session", "SELECT current_catalog, current_schema, current_user")

section("LAB 2 / Catalogs, schemas and tables")
show("SHOW CATALOGS", "SHOW CATALOGS")
show("SHOW SCHEMAS FROM lakehouse", "SHOW SCHEMAS FROM lakehouse")
show("SHOW TABLES FROM lakehouse.silver", "SHOW TABLES FROM lakehouse.silver")
show("DESCRIBE silver.orders", "DESCRIBE lakehouse.silver.orders")
show("DESCRIBE silver.users", "DESCRIBE lakehouse.silver.users")
show("SHOW CREATE TABLE silver.orders", "SHOW CREATE TABLE lakehouse.silver.orders")

section("LAB 2 / Query the silver layer")
show("count orders", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
show("count users", "SELECT count(*) AS users FROM lakehouse.silver.users")
show("adresses par state", """
    SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
    FROM lakehouse.silver.users
    GROUP BY state ORDER BY users DESC LIMIT 10""")
show("orphan orders", """
    SELECT count(*) AS orphan_orders
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
    WHERE u.user_id IS NULL""")

section("LAB 2 / Iceberg metadata")
show("orders$snapshots", """
    SELECT snapshot_id, committed_at, operation, summary['engine-name'] AS engine,
           summary['added-records'] AS added_records
    FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at""")
show("orders$files", """
    SELECT file_path, file_format, record_count, file_size_in_bytes
    FROM lakehouse.silver."orders$files\"""")
show("orders$history", 'SELECT * FROM lakehouse.silver."orders$history"')
show("orders$manifests", """
    SELECT path, added_data_files_count, existing_data_files_count
    FROM lakehouse.silver."orders$manifests\"""")
print("\n--- aws s3 ls warehouse/silver/orders/")
s3_ls("warehouse/silver/orders/")

section("LAB 2 / Query plan and execution")
JOIN_STATE = """
    SELECT u.state, count(*) AS orders
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY u.state"""
show("EXPLAIN (TYPE DISTRIBUTED)", "EXPLAIN (TYPE DISTRIBUTED)" + JOIN_STATE)
show("EXPLAIN ANALYZE", "EXPLAIN ANALYZE" + JOIN_STATE)
show("system.runtime.queries", """
    SELECT query_id, state,
           date_diff('millisecond', created, coalesce("end", current_timestamp)) AS elapsed_ms,
           substr(query, 1, 70) AS query
    FROM system.runtime.queries ORDER BY created DESC LIMIT 5""")

section("LAB 2 / Gold layer")
show("CREATE SCHEMA gold", "CREATE SCHEMA IF NOT EXISTS lakehouse.gold")
# Les tables gold d'une execution precedente sont supprimees par lab-10/spark_polaris.py prep :
# le DROP TABLE de Trino demande la purge des fichiers, que Polaris refuse par defaut (403).
show("SHOW SCHEMAS", "SHOW SCHEMAS FROM lakehouse")

show("CTAS orders_by_state_month", """
    CREATE TABLE lakehouse.gold.orders_by_state_month AS
    SELECT u.state,
           date_trunc('month', o.ordered_at) AS month,
           count(*)                          AS orders,
           sum(o.quantity)                   AS units,
           count(DISTINCT o.user_id)         AS buyers
    FROM lakehouse.silver.orders AS o
    JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
    GROUP BY 1, 2""")
show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 10")

TOP_PRODUCTS = """
    SELECT product, count(*) AS orders, sum(quantity) AS units,
           rank() OVER (ORDER BY sum(quantity) DESC) AS units_rank
    FROM lakehouse.silver.orders
    GROUP BY product"""
show("CTAS top_products", "CREATE TABLE lakehouse.gold.top_products AS" + TOP_PRODUCTS)
show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank LIMIT 10")

show("CTAS user_activity", """
    CREATE TABLE lakehouse.gold.user_activity AS
    SELECT u.user_id, u.state,
           date_diff('year', u.birthdate, current_date) AS age,
           count(o.order_id)            AS orders,
           coalesce(sum(o.quantity), 0) AS units,
           min(o.ordered_at)            AS first_order_at,
           max(o.ordered_at)            AS last_order_at
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY u.user_id, u.state, u.birthdate""")
show("user_activity", "SELECT * FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 10")
show("users sans commande", "SELECT count(*) AS users_sans_commande FROM lakehouse.gold.user_activity WHERE orders = 0")

show("SHOW TABLES gold", "SHOW TABLES FROM lakehouse.gold")
show("top_products$files", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')

section("LAB 2 / Validation")
silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
print(f"units  silver={silver_units} gold_state_month={gold_units} top_products={product_units}")
print(f"users  silver={silver_users} user_activity={gold_users}")
print(f"orders silver={silver_orders} user_activity={gold_orders}")
for nom, ok in [("silver_units == gold_units", silver_units == gold_units),
                ("silver_units == product_units", silver_units == product_units),
                ("silver_users == gold_users", silver_users == gold_users),
                ("silver_orders == gold_orders", silver_orders == gold_orders)]:
    print(("OK      " if ok else "ECHEC   ") + nom)

section("LAB 2 / Refresh a gold table")
show("DELETE top_products", "DELETE FROM lakehouse.gold.top_products")
show("INSERT top_products", """
    INSERT INTO lakehouse.gold.top_products
    SELECT product, count(*), sum(quantity), rank() OVER (ORDER BY sum(quantity) DESC)
    FROM lakehouse.silver.orders GROUP BY product""")
show("top_products$snapshots", """
    SELECT snapshot_id, committed_at, operation
    FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
first = sql("""SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots"
               ORDER BY committed_at LIMIT 1""").iloc[0, 0]
snaps = sql("""SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
show("time travel (premier snapshot)", f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {first}")
if len(snaps) >= 2:
    show("time travel (apres DELETE)",
         f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {snaps.iloc[1, 0]}")
show("version courante", "SELECT count(*) AS products FROM lakehouse.gold.top_products")

# ---------------------------------------------------------------------------------------------
section("LAB 3 / Star schema : dimensions")
show("CTAS dim_user", """
    CREATE TABLE lakehouse.gold.dim_user AS
    SELECT row_number() OVER (ORDER BY user_id) AS user_key,
           user_id, username, name, sex, birthdate, city, state, zip_code
    FROM lakehouse.silver.users""")
show("CTAS dim_product", """
    CREATE TABLE lakehouse.gold.dim_product AS
    SELECT row_number() OVER (ORDER BY product) AS product_key, product
    FROM (SELECT DISTINCT product FROM lakehouse.silver.orders)""")
first_day, last_day = sql("""
    SELECT CAST(min(ordered_at) AS date) AS first_day, CAST(max(ordered_at) AS date) AS last_day
    FROM lakehouse.silver.orders""").iloc[0]
print("periode :", first_day, "->", last_day)
show("CTAS dim_date", f"""
    CREATE TABLE lakehouse.gold.dim_date AS
    SELECT CAST(date_format(CAST(d AS timestamp), '%Y%m%d') AS integer) AS date_key,
           d AS full_date, year(d) AS year, quarter(d) AS quarter, month(d) AS month,
           date_format(CAST(d AS timestamp), '%M') AS month_name,
           day_of_week(d) AS day_of_week, day_of_week(d) IN (6, 7) AS is_weekend
    FROM UNNEST(sequence(DATE '{first_day}', DATE '{last_day}', INTERVAL '1' DAY)) AS t(d)""")
show("dim_date", "SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key")
show("dim_product", "SELECT * FROM lakehouse.gold.dim_product ORDER BY product_key")

section("LAB 3 / Star schema : fact table")
show("CTAS fact_orders", """
    CREATE TABLE lakehouse.gold.fact_orders AS
    SELECT o.order_id, u.user_key, p.product_key,
           CAST(date_format(o.ordered_at, '%Y%m%d') AS integer) AS date_key,
           o.ordered_at, o.quantity
    FROM lakehouse.silver.orders AS o
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_id = o.user_id
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product = o.product""")
show("nombre de lignes", """
    SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
    UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
    UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
    UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders""")

section("LAB 3 / Query the star")
show("year x state x product", """
    SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    GROUP BY d.year, u.state, p.product ORDER BY units DESC LIMIT 10""")
show("orders_by_state_month depuis l'etoile", """
    SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units,
           count(DISTINCT f.user_key) AS buyers
    FROM lakehouse.gold.fact_orders AS f
    JOIN lakehouse.gold.dim_date    AS d ON d.date_key = f.date_key
    JOIN lakehouse.gold.dim_user    AS u ON u.user_key = f.user_key
    GROUP BY u.state, d.year, d.month ORDER BY d.year, d.month, u.state LIMIT 10""")
show("difference etoile vs orders_by_state_month (doit etre vide)", """
    (SELECT u.state, d.year, d.month, count(*) AS orders, sum(f.quantity) AS units,
            count(DISTINCT f.user_key) AS buyers
     FROM lakehouse.gold.fact_orders AS f
     JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
     JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
     GROUP BY u.state, d.year, d.month)
    EXCEPT
    (SELECT state, year(month), month(month), orders, units, buyers
     FROM lakehouse.gold.orders_by_state_month)""")

section("LAB 3 / Validation")
fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
unmatched = sql("""
    SELECT count(*) AS v FROM lakehouse.gold.fact_orders AS f
    LEFT JOIN lakehouse.gold.dim_user    AS u ON u.user_key    = f.user_key
    LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
    LEFT JOIN lakehouse.gold.dim_date    AS d ON d.date_key    = f.date_key
    WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL""").iloc[0, 0]
print(f"orders silver={silver_orders} fact={fact_orders} | units silver={silver_units} fact={fact_units} | unmatched={unmatched}")
for nom, ok in [("silver_orders == fact_orders", silver_orders == fact_orders),
                ("silver_units == fact_units", silver_units == fact_units),
                ("unmatched == 0", unmatched == 0)]:
    print(("OK      " if ok else "ECHEC   ") + nom)
show("controles supplementaires", """
    SELECT
      (SELECT count(*) - count(DISTINCT order_id) FROM lakehouse.gold.fact_orders) AS order_id_en_double,
      (SELECT count(*) - count(DISTINCT user_id) FROM lakehouse.gold.dim_user)    AS user_id_en_double,
      (SELECT count(*) FROM lakehouse.gold.fact_orders f
         JOIN lakehouse.gold.dim_user u ON u.user_key = f.user_key
         JOIN lakehouse.silver.orders o ON o.order_id = f.order_id
        WHERE u.user_id <> o.user_id)                                              AS mauvais_user_key""")

# ---------------------------------------------------------------------------------------------
section("EXERCICE 1 / table partitionnee")
show("CTAS orders_by_state_month_part", """
    CREATE TABLE lakehouse.gold.orders_by_state_month_part
    WITH (partitioning = ARRAY['month(month)']) AS
    SELECT * FROM lakehouse.gold.orders_by_state_month""")
show("fichiers non partitionnee", 'SELECT file_path, record_count FROM lakehouse.gold."orders_by_state_month$files"')
show("fichiers partitionnee", 'SELECT file_path, partition, record_count FROM lakehouse.gold."orders_by_state_month_part$files"')
mois = sql("SELECT min(month) AS m FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
filtre = f"WHERE month = from_iso8601_timestamp('{pd.Timestamp(mois).isoformat()}')"
show("EXPLAIN ANALYZE non partitionnee", f"EXPLAIN ANALYZE SELECT sum(units) FROM lakehouse.gold.orders_by_state_month {filtre}")
show("EXPLAIN ANALYZE partitionnee", f"EXPLAIN ANALYZE SELECT sum(units) FROM lakehouse.gold.orders_by_state_month_part {filtre}")

section("EXERCICE 2 / cohorts")
show("CTAS cohorts", """
    CREATE TABLE lakehouse.gold.cohorts AS
    SELECT (year(u.birthdate) / 10) * 10                        AS birth_decade,
           count(DISTINCT u.user_id)                            AS users,
           count(o.order_id)                                    AS orders,
           round(CAST(sum(o.quantity) AS double) / nullif(count(o.order_id), 0), 2) AS avg_units_per_order
    FROM lakehouse.silver.users AS u
    LEFT JOIN lakehouse.silver.orders AS o ON o.user_id = u.user_id
    GROUP BY 1""")
show("cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")

section("EXERCICE 3 / INSERT dans silver puis MERGE dans gold")
MERGE = """
    MERGE INTO lakehouse.gold.orders_by_state_month AS t
    USING (
        SELECT u.state, date_trunc('month', o.ordered_at) AS month, count(*) AS orders,
               sum(o.quantity) AS units, count(DISTINCT o.user_id) AS buyers
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    ) AS s
    ON t.state = s.state AND t.month = s.month
    WHEN MATCHED AND (t.orders <> s.orders OR t.units <> s.units OR t.buyers <> s.buyers)
        THEN UPDATE SET orders = s.orders, units = s.units, buyers = s.buyers
    WHEN NOT MATCHED THEN INSERT (state, month, orders, units, buyers)
        VALUES (s.state, s.month, s.orders, s.units, s.buyers)"""
DELETE_STALE = """
    DELETE FROM lakehouse.gold.orders_by_state_month
    WHERE (state, month) NOT IN (
        SELECT DISTINCT u.state, date_trunc('month', o.ordered_at)
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id)"""
avant = sql("""SELECT snapshot_id FROM lakehouse.gold."orders_by_state_month$snapshots"
               ORDER BY committed_at DESC LIMIT 1""").iloc[0, 0]
show("INSERT d'une commande test", """
    INSERT INTO lakehouse.silver.orders (order_id, user_id, ordered_at, quantity, product)
    SELECT 'test-lab-trino-0001', user_id, TIMESTAMP '2020-01-03 12:00:00 UTC', 7, 'croissant'
    FROM lakehouse.silver.users ORDER BY user_id LIMIT 1""")
show("MERGE gold.orders_by_state_month", MERGE)
cle = """(SELECT u.state FROM lakehouse.silver.users u ORDER BY u.user_id LIMIT 1)"""
show("ligne concernee, version courante",
     f"SELECT * FROM lakehouse.gold.orders_by_state_month WHERE state = {cle} AND month = TIMESTAMP '2020-01-01 00:00:00 UTC'")
show("ligne concernee, version precedente (FOR VERSION AS OF)",
     f"SELECT * FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {avant} WHERE state = {cle} AND month = TIMESTAMP '2020-01-01 00:00:00 UTC'")
show("snapshots gold.orders_by_state_month", """
    SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at""")
# remise en etat : on retire la commande test et on resynchronise le gold
show("DELETE commande test", "DELETE FROM lakehouse.silver.orders WHERE order_id = 'test-lab-trino-0001'")
show("MERGE de remise en etat", MERGE)
# le MERGE de Trino n'a pas de clause WHEN NOT MATCHED BY SOURCE : on supprime a part
# les lignes gold qui n'existent plus dans la source (ici la ligne creee par la commande test)
show("DELETE des lignes gold sans source", DELETE_STALE)
show("controle", """SELECT (SELECT count(*) FROM lakehouse.silver.orders) AS silver_orders,
                           (SELECT sum(orders) FROM lakehouse.gold.orders_by_state_month) AS gold_orders""")

section("EXERCICE 4 / optimize")
show("snapshots avant", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
show("fichiers avant", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')
show("optimize", "ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
show("snapshots apres", 'SELECT snapshot_id, committed_at, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
show("fichiers apres", 'SELECT file_path, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')
print("\n--- aws s3 ls warehouse/gold/top_products/")
s3_ls("warehouse/gold/top_products/")

section("FIN")
show("SHOW TABLES gold", "SHOW TABLES FROM lakehouse.gold")
conn.close()
