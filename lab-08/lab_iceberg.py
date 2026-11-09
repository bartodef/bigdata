# Lab Iceberg : explorer les tables Iceberg avec Spark
# Il faut avoir fait le lab Spark DataFrames avant (tables lab.silver.orders et lab.silver.users)
# Lancement : python3 lab-08/lab_iceberg.py 2>/dev/null | tee lab-08/sortie-iceberg.txt
import subprocess
import sys

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
from pyspark.sql import functions as F

from session import get_spark

spark, BUCKET = get_spark("lab-iceberg")


def ls(path):
    out = subprocess.run(["aws", "s3", "--profile", "default", "ls", f"s3://{BUCKET}/{path}", "--recursive"],
                         capture_output=True, text=True).stdout
    print(out.replace(BUCKET, "<bucket>"))


def snapshots(table):
    spark.sql(f"SELECT snapshot_id, parent_id, operation, committed_at, summary['added-records'] AS added, "
              f"summary['total-records'] AS total, summary['added-data-files'] AS added_files "
              f"FROM {table}.snapshots ORDER BY committed_at").show(truncate=False)


def files(table):
    spark.sql(f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {table}.files") \
        .withColumn("file_path", F.regexp_replace("file_path", ".*/data/", "data/")).show(truncate=False)


spark.sql("SHOW NAMESPACES IN lab").show()
spark.sql("SHOW TABLES IN lab.silver").show()

print("=== Table orders")
spark.sql("SELECT count(*) AS orders, sum(quantity) AS quantity FROM lab.silver.orders").show()
ls("warehouse/silver/orders/")

print("=== Metadata")
snapshots("lab.silver.orders")
files("lab.silver.orders")
spark.sql("SELECT made_current_at, snapshot_id, is_current_ancestor FROM lab.silver.orders.history").show(truncate=False)

print("=== Deuxieme snapshot")
stg_orders = spark.table("lab.silver.orders")
nb_avant = stg_orders.count()
new_orders = stg_orders.limit(3).withColumn("order_id", F.expr("uuid()"))
new_orders.writeTo("lab.silver.orders").append()
snapshots("lab.silver.orders")
spark.sql("SELECT count(*) FROM lab.silver.orders").show()
files("lab.silver.orders")
ls("warehouse/silver/orders/")

print("=== Time travel")
snaps = spark.sql("SELECT snapshot_id FROM lab.silver.orders.snapshots ORDER BY committed_at").collect()
print(snaps)
first_snapshot = snaps[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS premier_snapshot FROM lab.silver.orders VERSION AS OF {first_snapshot}").show()
spark.sql("SELECT count(*) AS maintenant FROM lab.silver.orders").show()

print("=== Schema evolution")
spark.sql("ALTER TABLE lab.silver.orders ADD COLUMN source STRING")
spark.sql("DESCRIBE lab.silver.orders").show()
spark.sql("""
    INSERT INTO lab.silver.orders
    SELECT order_id, user_id, ordered_at, quantity, product, 'spark'
    FROM lab.silver.orders
    LIMIT 1
""")
spark.sql("SELECT source, count(*) FROM lab.silver.orders GROUP BY source").show()
files("lab.silver.orders")
snapshots("lab.silver.orders")
ls("warehouse/silver/orders/metadata/")

print("=== Parquet vs Iceberg")
ls("silver/spark/orders.parquet/")

print("=== Exercice 1 table users")
if not spark.catalog.tableExists("lab.silver.users"):
    users = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
    users.writeTo("lab.silver.users").createOrReplace()
spark.sql("SELECT count(*) FROM lab.silver.users").show()
snapshots("lab.silver.users")

print("=== Exercice 2 nouveau batch de users")
users = spark.table("lab.silver.users")
batch = users.limit(2).withColumn("user_id", F.expr("uuid()")).withColumn("username", F.concat("username", F.lit("_new")))
batch.writeTo("lab.silver.users").append()
snapshots("lab.silver.users")

print("=== Exercice 3 time travel users")
us = spark.sql("SELECT snapshot_id FROM lab.silver.users.snapshots ORDER BY committed_at").collect()
spark.sql(f"SELECT count(*) AS avant FROM lab.silver.users VERSION AS OF {us[0]['snapshot_id']}").show()
spark.sql("SELECT count(*) AS apres FROM lab.silver.users").show()

print("=== Exercice 4 colonne country")
spark.sql("ALTER TABLE lab.silver.users ADD COLUMN country STRING")
spark.sql("""
    INSERT INTO lab.silver.users
    SELECT uuid(), 'jdupont', 'Jean Dupont', 'M', 'jean.dupont@example.com', DATE'1990-05-12',
           '10 rue de Rivoli', 'Paris', NULL, '75001', 'FR'
""")
spark.sql("SELECT country, count(*) FROM lab.silver.users GROUP BY country").show()
spark.sql("DESCRIBE lab.silver.users").show()

print("=== Exercice 5 layout users")
ls("warehouse/silver/users/")

spark.stop()
print("=== fin")
