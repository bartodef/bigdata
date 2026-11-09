# Creation de la SparkSession avec S3 et le catalogue Iceberg "lab" (comme dans l'enonce)
import configparser
import os

from pyspark.sql import SparkSession

ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def credentials():
    # On prend le profil "default" de ~/.aws/credentials (mis a jour depuis Onyxia),
    # les variables d'environnement du service sont perimees au bout de quelques heures
    conf = configparser.ConfigParser()
    conf.read(os.path.expanduser("~/.aws/credentials"))
    if conf.has_section("default"):
        d = conf["default"]
        return d["aws_access_key_id"], d["aws_secret_access_key"], d.get("aws_session_token", "")
    return (os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"],
            os.environ.get("AWS_SESSION_TOKEN", ""))


def get_spark(app_name):
    access_key, secret_key, token = credentials()
    bucket = os.environ.get("LAB_BUCKET_NAME") or os.environ["KUBERNETES_NAMESPACE"]
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[2]")
        # limite memoire pour ne pas depasser la limite du pod Onyxia (OOMKilled)
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.hadoop.fs.s3a.endpoint", f"https://{os.environ.get('AWS_S3_ENDPOINT', 's3.seaweedfs.adm.adaltas.cloud')}")
        .config("spark.hadoop.fs.s3a.access.key", access_key)
        .config("spark.hadoop.fs.s3a.secret.key", secret_key)
        .config("spark.hadoop.fs.s3a.session.token", token)
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.jars.packages", ICEBERG_PACKAGE)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lab.type", "hadoop")
        .config("spark.sql.catalog.lab.warehouse", f"s3a://{bucket}/warehouse")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark, bucket
