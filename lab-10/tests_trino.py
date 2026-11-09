"""Tests du lab 10 (lecture seule) : python tests_trino.py <numero>"""
import os, sys
import pandas as pd
import trino

pd.set_option("display.width", 110)
pd.set_option("display.max_columns", 20)
pd.set_option("display.max_colwidth", 60)

conn = trino.dbapi.connect(
    host=os.environ.get("TRINO_HOST", "trino"),
    port=int(os.environ.get("TRINO_PORT", "8080")),
    user=os.environ.get("TRINO_USER", os.environ.get("KUBERNETES_NAMESPACE", "user")),
    http_scheme="http",
    catalog="lakehouse",
)


def sql(statement):
    cur = conn.cursor()
    cur.execute(statement)
    rows = cur.fetchall()
    columns = [c[0] for c in cur.description] if cur.description else []
    return pd.DataFrame(rows, columns=columns)


def show(title, statement):
    print(f"\n>>> {' '.join(statement.split())}")
    print(sql(statement).to_string())


step = sys.argv[1] if len(sys.argv) > 1 else "1"

if step == "1":
    show("version de Trino", "SELECT version()")
    show("catalogues", "SHOW CATALOGS")
    show("schemas du catalogue lakehouse", "SHOW SCHEMAS FROM lakehouse")
    show("tables silver", "SHOW TABLES FROM lakehouse.silver")
    show("types de silver.orders", "DESCRIBE lakehouse.silver.orders")

elif step == "2":
    show("nombre de commandes", "SELECT count(*) AS orders FROM lakehouse.silver.orders")
    show("nombre d'utilisateurs", "SELECT count(*) AS users FROM lakehouse.silver.users")
    show("villes manquantes par etat", """
        SELECT state, count(*) AS users, count_if(city IS NULL) AS missing_city
        FROM lakehouse.silver.users GROUP BY state ORDER BY users DESC LIMIT 5""")
    show("commandes orphelines (sans utilisateur)", """
        SELECT count(*) AS orphan_orders
        FROM lakehouse.silver.orders AS o
        LEFT JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
        WHERE u.user_id IS NULL""")

elif step == "3":
    show("snapshots", """
        SELECT snapshot_id, committed_at, operation, element_at(summary, 'spark.app.id') IS NOT NULL AS written_by_spark
        FROM lakehouse.silver."orders$snapshots" ORDER BY committed_at""")
    show("fichiers de donnees", """
        SELECT regexp_extract(file_path, '[^/]+$') AS file, file_format, record_count, file_size_in_bytes
        FROM lakehouse.silver."orders$files\"""")
    show("historique", 'SELECT * FROM lakehouse.silver."orders$history"')

elif step == "4":
    plan = sql("""
        EXPLAIN (TYPE DISTRIBUTED)
        SELECT u.state, count(*) AS orders
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
        GROUP BY u.state""").iloc[0, 0]
    keep = ("Fragment", "Output partitioning", "TableScan", "ScanFilter", "InnerJoin",
            "Distribution", "RemoteSource", "Aggregate", "LocalExchange")
    for line in plan.splitlines():
        if any(k in line for k in keep):
            print(line.rstrip()[:105])

elif step == "5":
    stats = sql("""
        EXPLAIN ANALYZE
        SELECT u.state, count(*) AS orders
        FROM lakehouse.silver.orders AS o
        JOIN lakehouse.silver.users AS u ON u.user_id = o.user_id
        GROUP BY u.state""").iloc[0, 0]
    for line in stats.splitlines():
        s = line.strip()
        if s.startswith(("Queued", "Fragment", "CPU:", "Peak Memory")):
            print(s[:105])
    show("dernieres requetes du cluster", """
        SELECT query_id, state, date_diff('millisecond', created, coalesce("end", current_timestamp)) AS ms,
               substr(query, 1, 40) AS query
        FROM system.runtime.queries ORDER BY created DESC LIMIT 4""")

elif step == "6":
    show("tables du schema gold", "SHOW TABLES FROM lakehouse.gold")
    show("orders_by_state_month", "SELECT * FROM lakehouse.gold.orders_by_state_month ORDER BY month, state LIMIT 6")
    show("top_products", "SELECT * FROM lakehouse.gold.top_products ORDER BY units_rank")
    show("user_activity", """
        SELECT user_id, state, age, orders, units FROM lakehouse.gold.user_activity ORDER BY orders DESC LIMIT 5""")

elif step == "7":
    silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    gold_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.orders_by_state_month").iloc[0, 0]
    product_units = sql("SELECT sum(units) AS v FROM lakehouse.gold.top_products").iloc[0, 0]
    silver_users = sql("SELECT count(*) AS v FROM lakehouse.silver.users").iloc[0, 0]
    gold_users = sql("SELECT count(*) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
    silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    gold_orders = sql("SELECT sum(orders) AS v FROM lakehouse.gold.user_activity").iloc[0, 0]
    print(f"unites   : silver={silver_units}  orders_by_state_month={gold_units}  top_products={product_units}")
    print(f"users    : silver={silver_users}  user_activity={gold_users}")
    print(f"commandes: silver={silver_orders}  user_activity={gold_orders}")
    assert silver_units == gold_units
    assert silver_units == product_units
    assert silver_users == gold_users
    assert silver_orders == gold_orders
    print("Les 4 assert passent : OK")

elif step == "8":
    show("snapshots (creation, DELETE, INSERT)", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at""")
    snaps = sql('SELECT snapshot_id FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
    for i, label in enumerate(["apres creation", "apres DELETE", "apres INSERT (courant)"]):
        if i < len(snaps):
            sid = snaps.iloc[i, 0]
            show("", f"SELECT count(*) AS products FROM lakehouse.gold.top_products FOR VERSION AS OF {sid}")

elif step == "9":
    show("nombre de lignes", """
        SELECT 'dim_user' AS table_name, count(*) AS row_count FROM lakehouse.gold.dim_user
        UNION ALL SELECT 'dim_product', count(*) FROM lakehouse.gold.dim_product
        UNION ALL SELECT 'dim_date', count(*) FROM lakehouse.gold.dim_date
        UNION ALL SELECT 'fact_orders', count(*) FROM lakehouse.gold.fact_orders""")
    show("dim_date (extrait)", "SELECT * FROM lakehouse.gold.dim_date ORDER BY date_key LIMIT 4")
    show("requete sur l'etoile : annee x etat x produit", """
        SELECT d.year, u.state, p.product, count(*) AS orders, sum(f.quantity) AS units
        FROM lakehouse.gold.fact_orders AS f
        JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
        JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
        JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
        GROUP BY d.year, u.state, p.product ORDER BY units DESC LIMIT 5""")

elif step == "10":
    silver_orders = sql("SELECT count(*) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    fact_orders = sql("SELECT count(*) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
    silver_units = sql("SELECT sum(quantity) AS v FROM lakehouse.silver.orders").iloc[0, 0]
    fact_units = sql("SELECT sum(quantity) AS v FROM lakehouse.gold.fact_orders").iloc[0, 0]
    unmatched = sql("""
        SELECT count(*) AS v FROM lakehouse.gold.fact_orders AS f
        LEFT JOIN lakehouse.gold.dim_user AS u ON u.user_key = f.user_key
        LEFT JOIN lakehouse.gold.dim_product AS p ON p.product_key = f.product_key
        LEFT JOIN lakehouse.gold.dim_date AS d ON d.date_key = f.date_key
        WHERE u.user_key IS NULL OR p.product_key IS NULL OR d.date_key IS NULL""").iloc[0, 0]
    print(f"commandes : silver={silver_orders}  fact_orders={fact_orders}")
    print(f"unites    : silver={silver_units}  fact_orders={fact_units}")
    print(f"commandes sans dimension complete : {unmatched}")
    assert silver_orders == fact_orders
    assert silver_units == fact_units
    assert unmatched == 0
    show("difference etoile vs orders_by_state_month (doit etre vide)", """
        WITH star AS (
          SELECT u.state, date_trunc('month', f.ordered_at) AS month, count(*) AS orders, sum(f.quantity) AS units
          FROM lakehouse.gold.fact_orders f JOIN lakehouse.gold.dim_user u ON u.user_key = f.user_key
          GROUP BY 1, 2)
        SELECT count(*) AS lignes_differentes FROM (
          SELECT state, month, orders, units FROM star
          EXCEPT SELECT state, month, orders, units FROM lakehouse.gold.orders_by_state_month)""")
    print("Les 3 assert passent : OK")

elif step == "11":
    show("fichiers de la table non partitionnee", """
        SELECT regexp_extract(file_path, '[^/]+$') AS file, record_count
        FROM lakehouse.gold."orders_by_state_month$files\"""")
    show("fichiers de la table partitionnee", """
        SELECT regexp_extract(file_path, 'month_month=[^/]+') AS partition_dir, record_count
        FROM lakehouse.gold."orders_by_state_month_part$files" ORDER BY 1""")
    for name in ("orders_by_state_month", "orders_by_state_month_part"):
        stats = sql(f"""
            EXPLAIN ANALYZE SELECT state, orders FROM lakehouse.gold.{name}
            WHERE month = TIMESTAMP '2020-02-01 00:00:00 UTC'""").iloc[0, 0]
        for line in stats.splitlines():
            s = line.strip()
            if s.startswith("Input:") and "Filtered" in s:
                print(f"{name:28s} -> {s.split(', Physical')[0]}")

elif step == "12":
    show("gold.cohorts", "SELECT * FROM lakehouse.gold.cohorts ORDER BY birth_decade")

elif step == "13":
    show("snapshots (creation puis MERGE)", """
        SELECT snapshot_id, committed_at, operation
        FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at""")
    snaps = sql('SELECT snapshot_id, operation FROM lakehouse.gold."orders_by_state_month$snapshots" ORDER BY committed_at')
    for sid, op in snaps.itertuples(index=False):
        show(f"ligne VT / 2020-01 a la version {sid} ({op})", f"""
            SELECT state, month, orders, units, buyers
            FROM lakehouse.gold.orders_by_state_month FOR VERSION AS OF {sid}
            WHERE state = 'VT' AND month = TIMESTAMP '2020-01-01 00:00:00 UTC'""")

elif step == "14":
    show("snapshots avant", 'SELECT snapshot_id, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
    show("fichiers avant", 'SELECT regexp_extract(file_path, \'[^/]+$\') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')
    sql("ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
    print("\n>>> ALTER TABLE lakehouse.gold.top_products EXECUTE optimize")
    show("snapshots apres", 'SELECT snapshot_id, operation FROM lakehouse.gold."top_products$snapshots" ORDER BY committed_at')
    show("fichiers apres", 'SELECT regexp_extract(file_path, \'[^/]+$\') AS file, record_count, file_size_in_bytes FROM lakehouse.gold."top_products$files"')

conn.close()
