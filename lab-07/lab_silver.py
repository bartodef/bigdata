# Lab Spark DataFrames : bronze vers silver
# Lancement : python3 lab-07/lab_silver.py 2>/dev/null | tee lab-07/sortie-spark-silver.txt
import sys
import time

sys.path.insert(0, __import__("os").path.dirname(__import__("os").path.abspath(__file__)))
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from pyspark.sql.window import Window

from session import get_spark

t_start = time.perf_counter()
spark, BUCKET = get_spark("lab-silver-pyspark")
sc = spark.sparkContext
print("spark", spark.version, "scala", sc._jvm.scala.util.Properties.versionNumberString(),
      "defaultParallelism", sc.defaultParallelism)

print("=== Bronze")
users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True,
                              samplingRatio=0.01, multiLine=True)
orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True,
                               samplingRatio=0.01, multiLine=True)
users_bronze.printSchema()
orders_bronze.printSchema()
users_bronze.show(3, truncate=False)
users_bronze.select("address").show(3, truncate=False)
print("users", users_bronze.count(), "orders", orders_bronze.count())

print("=== Silver users")
users_typed = (
    users_bronze.select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)
stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
)
stg_users = stg_users.withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))
stg_users.show(5, truncate=False)

print("=== Silver orders")
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)
print("Original:", orders_typed.count())
duplicates = orders_typed.limit(3)
orders_with_duplicates = orders_typed.unionByName(duplicates)
print("With duplicates:", orders_with_duplicates.count())
orders_with_duplicates.groupBy("order_id").count().filter(F.col("count") > 1).show()
dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
stg_orders = (
    orders_with_duplicates
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)
print("After deduplication:", stg_orders.count())
stg_orders.groupBy("order_id").count().filter(F.col("count") > 1).show()

print("=== Spark SQL")
orders_typed.createOrReplaceTempView("orders_typed")
stg_orders_sql = spark.sql("""
    SELECT order_id, user_id, ordered_at, quantity, product
    FROM (
        SELECT *, row_number() OVER (PARTITION BY order_id ORDER BY ordered_at DESC) AS rn
        FROM orders_typed
    )
    WHERE rn = 1
""")
stg_orders_sql.show(5)
assert stg_orders.count() == stg_orders_sql.count()
assert set(stg_orders.columns) == set(stg_orders_sql.columns)
print("asserts ok")
stg_users.createOrReplaceTempView("stg_users")
spark.sql("""
    SELECT state, count(*) AS users, sum(CASE WHEN city IS NULL THEN 1 ELSE 0 END) AS missing_city
    FROM stg_users GROUP BY state ORDER BY users DESC
""").show(10)
spark.sql("SELECT state, count(*) AS users FROM stg_users WHERE city IS NULL GROUP BY state").show()
print("zip vide", stg_users.filter("zip_code = ''").count(), "state vide", stg_users.filter("state = ''").count())

print("=== Parquet")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("partitions users", stg_users.rdd.getNumPartitions(), "orders", stg_orders.rdd.getNumPartitions())
users_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
orders_silver = spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
assert users_silver.count() == users_bronze.count()
assert orders_silver.count() == stg_orders.count()
print("relecture ok", users_silver.count(), orders_silver.count())
print("temps total silver", round(time.perf_counter() - t_start, 1), "s")

print("=== Iceberg")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots").show(truncate=False)
spark.sql("SELECT file_path, file_format, record_count, file_size_in_bytes FROM lab.silver.orders.files").show(truncate=False)
assert spark.table("lab.silver.users").count() == users_bronze.count()
assert spark.table("lab.silver.orders").count() == stg_orders.count()
print("iceberg ok")

print("=== Exercice 1 birthdate_is_valid")
first = stg_orders.groupBy("user_id").agg(F.min("ordered_at").alias("first_ordered_at"))
users_valid_df = (
    stg_users.join(first, "user_id", "left")
    .withColumn("birthdate_is_valid",
                F.coalesce(F.col("birthdate") <= F.to_date("first_ordered_at"), F.lit(True)))
    .withColumn("birthdate", F.when(F.col("birthdate_is_valid"), F.col("birthdate")))
    .drop("first_ordered_at")
)
stg_orders.createOrReplaceTempView("stg_orders")
users_valid_sql = spark.sql("""
    SELECT u.user_id, u.username, u.name, u.sex, u.email,
           CASE WHEN coalesce(u.birthdate <= to_date(f.first_ordered_at), true) THEN u.birthdate END AS birthdate,
           u.street, u.city, u.state, u.zip_code,
           coalesce(u.birthdate <= to_date(f.first_ordered_at), true) AS birthdate_is_valid
    FROM stg_users u
    LEFT JOIN (SELECT user_id, min(ordered_at) AS first_ordered_at FROM stg_orders GROUP BY user_id) f
      ON u.user_id = f.user_id
""")
users_valid_df.groupBy("birthdate_is_valid").count().show()
diff = users_valid_df.select(users_valid_sql.columns).exceptAll(users_valid_sql).count() + \
    users_valid_sql.exceptAll(users_valid_df.select(users_valid_sql.columns)).count()
print("difference DataFrame / SQL", diff)

print("=== Exercice 2 UDF")


def zip_python(address):
    import re
    m = re.search(r"([A-Z]{2}) ([0-9]{5})$", address or "")
    return m.group(2) if m else ""


zip_udf = F.udf(zip_python, StringType())


@F.pandas_udf(StringType())
def zip_pandas(address: pd.Series) -> pd.Series:
    return address.str.extract(r"([A-Z]{2}) ([0-9]{5})$")[1].fillna("")


def bench(col):
    best = None
    for _ in range(5):
        t = time.perf_counter()
        users_bronze.select(col.alias("z")).agg(F.count("z")).collect()
        d = time.perf_counter() - t
        best = d if best is None or d < best else best
    return round(best * 1000, 1)


users_bronze.cache().count()
orders_bronze.cache().count()
print("regexp_extract ms", bench(F.regexp_extract("address", "([A-Z]{2}) ([0-9]{5})$", 2)))
print("udf python ms", bench(zip_udf("address")))
print("pandas_udf ms", bench(zip_pandas("address")))
same = users_bronze.select(F.regexp_extract("address", "([A-Z]{2}) ([0-9]{5})$", 2).alias("a"),
                           zip_udf("address").alias("b"), zip_pandas("address").alias("c")) \
    .filter("a != b or a != c").count()
print("resultats differents", same)

print("=== Exercice 3 cache")
t = time.perf_counter(); orders_bronze.groupBy("product").count().collect(); print("apres cache ms", round((time.perf_counter() - t) * 1000))
print("plan", [l for l in orders_bronze.groupBy("product").count()._jdf.queryExecution().executedPlan().toString().split("\n") if "InMemory" in l][:1])

print("=== Exercice 4 append et time travel")
new_order = stg_orders.limit(1).withColumn("order_id", F.expr("uuid()"))
new_order.writeTo("lab.silver.orders").append()
snaps = spark.sql("SELECT snapshot_id, operation, committed_at FROM lab.silver.orders.snapshots ORDER BY committed_at")
snaps.show(truncate=False)
first_snap = snaps.collect()[0]["snapshot_id"]
spark.sql(f"SELECT count(*) AS avant FROM lab.silver.orders VERSION AS OF {first_snap}").show()
spark.sql("SELECT count(*) AS apres FROM lab.silver.orders").show()
new_order.write.mode("append").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("parquet apres append", spark.read.parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet").count())
# on remet le Parquet comme avant pour la comparaison avec dbt
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")

spark.stop()
print("=== fin")
