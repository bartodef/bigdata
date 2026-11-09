# Lab 10 Trino : partie Spark (preparation des tables silver dans Polaris + exercice 5)
# Usage : python3 -u lab-10/spark_polaris.py prep|exo5
import os
import sys

from pyspark.sql import SparkSession

ETAPE = sys.argv[1]
BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"

spark = (
    SparkSession.builder
    .appName("lab-trino-spark")
    .master("local[*]")
    .config("spark.driver.memory", "1g")
    .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ['AWS_S3_ENDPOINT']}")
    .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("spark.hadoop.fs.s3a.session.token", os.environ.get("AWS_SESSION_TOKEN", ""))
    .config("spark.hadoop.fs.s3a.endpoint.region", os.environ.get("AWS_REGION"))
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.jars.packages", ICEBERG_PACKAGE)
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.polaris.type", "rest")
    .config("spark.sql.catalog.polaris.uri", f"{POLARIS_URI}/api/catalog")
    .config("spark.sql.catalog.polaris.warehouse", POLARIS_CATALOG)
    .config("spark.sql.catalog.polaris.credential", os.environ["POLARIS_DEV_CREDENTIAL"])
    .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
    .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")


def tables():
    return [r.tableName for r in spark.sql("SHOW TABLES IN polaris.silver").collect()]


if ETAPE == "prep":
    # Bronze du bucket (users.csv et orders.csv), memes transformations que lab-07/lab_silver.py
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    users_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/users.csv", header=True, inferSchema=True,
                                  samplingRatio=0.01, multiLine=True)
    orders_bronze = spark.read.csv(f"s3a://{BUCKET}/bronze/orders.csv", header=True, inferSchema=True,
                                   samplingRatio=0.01, multiLine=True)
    print("bronze : users", users_bronze.count(), "orders", orders_bronze.count())

    # memes transformations que le lab 7 (lab-07/lab_silver.py)
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
    orders_typed = orders_bronze.select(
        F.col("uuid").cast("string").alias("order_id"),
        F.col("user_uuid").cast("string").alias("user_id"),
        F.col("date").cast("timestamp").alias("ordered_at"),
        F.col("quantity").cast("int").alias("quantity"),
        F.lower(F.trim("product")).alias("product"),
    )
    dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())
    stg_orders = (
        orders_typed.withColumn("row_number", F.row_number().over(dedup_window))
        .filter(F.col("row_number") == 1)
        .drop("row_number")
    )

    # Tables silver de Polaris recreees (DROP sans PURGE : les fichiers des labs 7 et 8 restent)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.orders")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    stg_users.writeTo("polaris.silver.users").create()
    stg_orders.writeTo("polaris.silver.orders").create()
    # gold d'une execution precedente : DROP sans purge (Polaris refuse la purge par defaut)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    for r in spark.sql("SHOW TABLES IN polaris.gold").collect():
        spark.sql(f"DROP TABLE polaris.gold.{r.tableName}")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT count(*) AS orders, sum(quantity) AS units FROM polaris.silver.orders").show()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    spark.sql("DESCRIBE polaris.silver.orders").show()

elif ETAPE == "exo5":
    # Exercice 5 : meme agregation que gold.orders_by_state_month, calculee par Spark
    spark_df = spark.sql("""
        SELECT u.state,
               date_trunc('month', o.ordered_at) AS month,
               count(*)                          AS orders,
               sum(o.quantity)                   AS units,
               count(DISTINCT o.user_id)         AS buyers
        FROM polaris.silver.orders AS o
        JOIN polaris.silver.users  AS u ON u.user_id = o.user_id
        GROUP BY 1, 2
    """)
    trino_df = spark.table("polaris.gold.orders_by_state_month")
    n_spark, n_trino = spark_df.count(), trino_df.count()
    print("lignes Spark :", n_spark, "| lignes Trino (gold) :", n_trino)
    print("unites Spark :", spark_df.agg({"units": "sum"}).first()[0],
          "| unites Trino :", trino_df.agg({"units": "sum"}).first()[0])
    diff = (spark_df.select("state", "orders", "units", "buyers")
            .exceptAll(trino_df.select("state", "orders", "units", "buyers")))
    print("lignes differentes (hors colonne month) :", diff.count())
    spark_df.orderBy("month", "state").show(5, truncate=False)

spark.stop()
