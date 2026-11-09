# Comparaison des sorties dbt et Spark avec DuckDB
# Lancement : uv run python lab-07/compare_dbt.py
import os

import duckdb

b = "s3://" + (os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"])
con = duckdb.connect()
con.sql(f"""CREATE SECRET lab_s3 (TYPE s3, PROVIDER credential_chain, PROFILE 'default', URL_STYLE 'path',
          ENDPOINT '{os.environ["AWS_S3_ENDPOINT"]}', SCOPE '{b}')""")
con.sql(f"""
  SELECT 'dbt' AS engine, count(*) AS orders, sum(quantity) AS quantity FROM '{b}/silver/dbt/orders.parquet'
  UNION ALL
  SELECT 'spark', count(*), sum(quantity) FROM '{b}/silver/spark/orders.parquet/*.parquet'
""").show()
con.sql(f"""
  SELECT 'dbt' AS engine, count(*) AS users FROM '{b}/silver/dbt/users.parquet'
  UNION ALL
  SELECT 'spark', count(*) FROM '{b}/silver/spark/users.parquet/*.parquet'
""").show()
con.sql(f"DESCRIBE SELECT * FROM '{b}/silver/dbt/orders.parquet'").show()
con.sql(f"DESCRIBE SELECT * FROM '{b}/silver/spark/orders.parquet/*.parquet'").show()
con.sql(f"DESCRIBE SELECT * FROM '{b}/silver/dbt/users.parquet'").show()
con.sql(f"DESCRIBE SELECT * FROM '{b}/silver/spark/users.parquet/*.parquet'").show()
