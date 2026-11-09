# Lab 10 Trino : partie Spark (tables silver dans Polaris + exercice 5)
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
    # silver.orders est la table enregistree dans Polaris par le lab Polaris : on n'y touche pas.
    # silver.users a ete supprimee au nettoyage du lab Polaris : on la recree comme dans ce lab.
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    if "users" not in tables():
        spark.table("lab.silver.users").writeTo("polaris.silver.users").createOrReplace()
    # tables gold d'une execution precedente : DROP sans purge (Polaris refuse la purge par defaut)
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
