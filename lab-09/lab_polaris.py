# Lab Polaris : gouverner les tables Iceberg avec Polaris
# Lancement depuis ~/work/bigdata (apres lab-09/2-deploiement.sh) : bash lab-09/3-lab.sh
# Usage : python3 -u lab-09/lab_polaris.py <etape>
#   dev       : connexion, register_table, tables et schema en dev
#   analyst   : lecture et ecritures refusees en analyst
#   verif     : snapshots en dev apres les essais de l'analyst + exercice 1
#   exo2      : exercice 2 (role silver_reader, namespace gold)
#   nettoyage : DROP de polaris.silver.users et polaris.gold
#   reset     : (hors lab) supprime de Polaris les tables creees par le lab Trino pour rejouer ce lab
import json
import os
import subprocess
import sys
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession

ETAPE = sys.argv[1]
BUCKET = os.environ["LAB_BUCKET_NAME"]
POLARIS_URI = os.environ["POLARIS_URI"]
POLARIS_CATALOG = os.environ["POLARIS_CATALOG"]
ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.11.0"


def session(principal, credential=None):
    credential = credential or os.environ[f"POLARIS_{principal.upper()}_CREDENTIAL"]
    spark = (
        SparkSession.builder
        .appName("lab-polaris")
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
        .config("spark.sql.catalog.polaris.credential", credential)
        .config("spark.sql.catalog.polaris.scope", "PRINCIPAL_ROLE:ALL")
        .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.hadoop.HadoopFileIO")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    print("Polaris principal:", principal)
    print("fs.s3.impl:", spark.sparkContext.getConf().get("spark.hadoop.fs.s3.impl", None))
    return spark


def ls(prefix):
    out = subprocess.run(["aws", "s3", "--profile", "default", "ls", f"s3://{BUCKET}/{prefix}", "--recursive"],
                         capture_output=True, text=True).stdout
    lines = [l.split()[-1] for l in out.splitlines() if l.strip()]
    print(f"{len(lines)} objets sous {prefix}")
    for l in lines:
        print("  ", l)
    return lines


def essai(spark, sql):
    print(">", sql)
    try:
        spark.sql(sql).show(truncate=False)
        print("OK")
    except Exception as e:
        lines = [l.strip() for l in str(e).split("\n") if l.strip()]
        utiles = [l for l in lines if "Forbidden" in l or "NotAuthorized" in l or "403" in l]
        msg = utiles[0] if utiles else lines[0]
        print("ERREUR :", msg[:400])


def snapshots(spark, table):
    spark.sql(f"SELECT snapshot_id, operation, committed_at FROM {table}.snapshots ORDER BY committed_at") \
        .show(truncate=False)


# --- API de management de Polaris (en root) pour l'exercice 2
def api(method, path, body=None, token=None, form=None):
    data = None
    headers = {}
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(POLARIS_URI + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req) as r:
            txt = r.read().decode()
            return r.status, (json.loads(txt) if txt else None)
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


def root_token():
    _, rep = api("POST", "/api/catalog/v1/oauth/tokens", form={
        "grant_type": "client_credentials", "client_id": "root",
        "client_secret": os.environ.get("POLARIS_ROOT_SECRET", "s3cr3t"), "scope": "PRINCIPAL_ROLE:ALL"})
    return rep["access_token"]


if ETAPE == "dev":
    spark = session("dev")
    print("=== SHOW NAMESPACES")
    spark.sql("SHOW NAMESPACES IN lab").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()

    print("=== Register de silver.orders")
    hint = subprocess.run(["aws", "s3", "--profile", "default", "cp",
                           f"s3://{BUCKET}/warehouse/silver/orders/metadata/version-hint.text", "-"],
                          capture_output=True, text=True).stdout.strip()
    print("version-hint.text =", hint)
    avant = ls("warehouse/silver/orders/")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.silver")
    spark.sql(f"""
        CALL polaris.system.register_table(
          table => 'silver.orders',
          metadata_file => 's3a://{BUCKET}/warehouse/silver/orders/metadata/v{hint}.metadata.json'
        )
    """).show()
    spark.sql("SELECT count(*) AS orders FROM polaris.silver.orders").show()
    spark.sql("SELECT count(*) AS orders FROM lab.silver.orders").show()
    apres = ls("warehouse/silver/orders/")
    print("nouveaux objets apres register :", sorted(set(apres) - set(avant)))

    print("=== Tables en dev")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    snapshots(spark, "polaris.silver.orders")
    stg_users = spark.table("lab.silver.users")
    stg_users.writeTo("polaris.silver.users").createOrReplace()
    spark.sql("SELECT count(*) AS users FROM polaris.silver.users").show()
    ls("warehouse/silver/")
    spark.sql("SHOW TBLPROPERTIES polaris.silver.users").show(truncate=False)
    spark.sql("SELECT file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)

    print("=== Schema en dev")
    try:
        spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN country STRING")
    except Exception as e:
        # la colonne country existe deja : elle a ete ajoutee a lab.silver.users par l'exercice 4 du lab Iceberg
        print("ALTER ADD COLUMN country :", str(e).split("\n")[0][:160])
        print("-> la colonne existe deja, j'ajoute une colonne region a la place")
        spark.sql("ALTER TABLE polaris.silver.users ADD COLUMN region STRING")
    spark.sql("DESCRIBE polaris.silver.users").show()
    spark.sql("SELECT file FROM polaris.silver.users.metadata_log_entries").show(truncate=False)
    spark.sql("SELECT file FROM polaris.silver.orders.metadata_log_entries").show(truncate=False)
    spark.stop()

elif ETAPE == "analyst":
    spark = session("analyst")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SELECT product, count(*) FROM polaris.silver.orders GROUP BY product").show()
    essai(spark, "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    essai(spark, "ALTER TABLE polaris.silver.users ADD COLUMN phone STRING")
    spark.stop()

elif ETAPE == "verif":
    spark = session("dev")
    print("=== Snapshots de silver.orders apres les essais de l'analyst")
    snapshots(spark, "polaris.silver.orders")
    print("=== Exercice 1 : suppression de country")
    snapshots(spark, "polaris.silver.users")
    spark.sql("ALTER TABLE polaris.silver.users DROP COLUMN country")
    spark.sql("DESCRIBE polaris.silver.users").show()
    snapshots(spark, "polaris.silver.users")
    spark.sql("SELECT timestamp, file, latest_snapshot_id FROM polaris.silver.users.metadata_log_entries") \
        .show(truncate=False)
    ls("warehouse/silver/users/")
    spark.stop()

elif ETAPE == "exo2":
    token = root_token()
    cat = POLARIS_CATALOG
    print("=== Creation du role silver_reader, du role principal et du principal reader_silver")
    print(api("POST", f"/api/management/v1/catalogs/{cat}/catalog-roles", {"catalogRole": {"name": "silver_reader"}}, token)[0])
    for priv in ["NAMESPACE_LIST", "TABLE_LIST", "NAMESPACE_READ_PROPERTIES", "TABLE_READ_PROPERTIES", "TABLE_READ_DATA"]:
        st, rep = api("PUT", f"/api/management/v1/catalogs/{cat}/catalog-roles/silver_reader/grants",
                      {"grant": {"type": "namespace", "namespace": ["silver"], "privilege": priv}}, token)
        print("grant", priv, "sur silver :", st)
    print(api("POST", "/api/management/v1/principal-roles", {"principalRole": {"name": "silver_analyst"}}, token)[0])
    print(api("PUT", "/api/management/v1/principal-roles/silver_analyst/catalog-roles/" + cat,
              {"catalogRole": {"name": "silver_reader"}}, token)[0])
    st, rep = api("POST", "/api/management/v1/principals", {"principal": {"name": "reader_silver"}}, token)
    if st != 201:
        st, rep = api("POST", "/api/management/v1/principals/reader_silver/reset", None, token)
    cred = f"{rep['credentials']['clientId']}:{rep['credentials']['clientSecret']}"
    print(api("PUT", "/api/management/v1/principals/reader_silver/principal-roles",
              {"principalRole": {"name": "silver_analyst"}}, token)[0])
    st, rep = api("GET", f"/api/management/v1/catalogs/{cat}/catalog-roles/silver_reader/grants", None, token)
    print("grants de silver_reader :", json.dumps(rep))

    print("=== Namespace gold en dev")
    spark = session("dev")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.gold")
    spark.sql("""CREATE OR REPLACE TABLE polaris.gold.orders_by_product AS
                 SELECT product, count(*) AS orders, sum(quantity) AS quantity
                 FROM polaris.silver.orders GROUP BY product""")
    spark.sql("SELECT * FROM polaris.gold.orders_by_product").show()
    spark.stop()

    print("=== Tests avec reader_silver")
    SparkSession._instantiatedSession = None
    spark = session("reader_silver", cred)
    essai(spark, "SHOW TABLES IN polaris.silver")
    essai(spark, "SELECT count(*) FROM polaris.silver.orders")
    essai(spark, "SHOW TABLES IN polaris.gold")
    essai(spark, "SELECT * FROM polaris.gold.orders_by_product")
    essai(spark, "INSERT INTO polaris.silver.orders SELECT * FROM polaris.silver.orders LIMIT 1")
    spark.stop()

elif ETAPE == "reset":
    spark = session("dev")
    for ns in ["gold", "silver"]:
        try:
            for r in spark.sql(f"SHOW TABLES IN polaris.{ns}").collect():
                print("DROP TABLE polaris." + ns + "." + r.tableName)
                spark.sql(f"DROP TABLE polaris.{ns}.{r.tableName}")
        except Exception as e:
            print("namespace", ns, ":", str(e).split("\n")[0][:120])
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    spark.sql("SHOW NAMESPACES IN polaris").show()
    spark.stop()

elif ETAPE == "nettoyage":
    spark = session("dev")
    spark.sql("DROP TABLE IF EXISTS polaris.gold.orders_by_product")
    spark.sql("DROP TABLE IF EXISTS polaris.silver.users")
    spark.sql("DROP NAMESPACE IF EXISTS polaris.gold")
    spark.sql("SHOW TABLES IN polaris.silver").show()
    spark.sql("SHOW NAMESPACES IN polaris").show()
    spark.stop()
