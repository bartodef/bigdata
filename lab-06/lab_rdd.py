# Lab Spark RDD
# Avant : aws s3 cp des fichiers bronze/users.csv et bronze/orders.csv dans le dossier courant
# Lancement : python3 lab-06/lab_rdd.py 2>/dev/null | tee lab-06/sortie-rdd.txt
import json
import re
import time
import urllib.request
from operator import add

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("lab-rdd").master("local[*]").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("ERROR")
print("defaultParallelism", sc.defaultParallelism, "defaultMinPartitions", sc.defaultMinPartitions)


def stages():
    # infos de la Spark UI (onglet Stages)
    app = sc.applicationId
    url = f"{sc.uiWebUrl}/api/v1/applications/{app}/stages"
    return json.load(urllib.request.urlopen(url))


print("=== Partitions")
users_lines = sc.textFile("users.csv")
orders_lines = sc.textFile("orders.csv")
print("users", users_lines.getNumPartitions())
print("orders", orders_lines.getNumPartitions())
print("repartition(8)", orders_lines.repartition(8).getNumPartitions())
print("coalesce(8)", orders_lines.coalesce(8).getNumPartitions())
print("coalesce(1)", orders_lines.coalesce(1).getNumPartitions())

print("=== Lazy evaluation")
header = orders_lines.first()
orders_data = orders_lines.filter(lambda line: line != header and line.strip() != "")
print(orders_data.toDebugString().decode())
nb = len(stages())
print("count 1", orders_data.count())
print("count 2", orders_data.count())
print("nouveaux stages", len(stages()) - nb)

print("=== Adresse sur 2 lignes")
for line in users_lines.take(6):
    print(repr(line))
print("lignes users", users_lines.count())
UUID_AT_START = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
users_records = users_lines.filter(lambda line: UUID_AT_START.match(line) is not None)
print("vrais users", users_records.count())


def parse_order(line):
    order_id, user_id, date, quantity, product = line.split(",")
    return {
        "order_id": order_id,
        "user_id": user_id,
        "date": date,
        "quantity": int(quantity),
        "product": product.strip().lower(),
    }


orders_rdd = orders_data.map(parse_order).cache()
print(orders_rdd.take(3))

print("=== Virgule en trop dans product")
bad = sc.parallelize([header, "a,b,2020-01-01 00:00:00,2,pain,complet"]).filter(lambda l: l != header)
bad_rdd = bad.map(parse_order)
print("map ok, rien ne plante encore")
try:
    bad_rdd.collect()
except Exception as e:
    print("erreur a l'action :", str(e).split("\n")[-1][:200] if "ValueError" not in str(e) else "ValueError: too many values to unpack")

print("=== reduceByKey vs groupByKey")
s0 = {s["stageId"] for s in stages()}
user_quantity = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
print(user_quantity.take(5))
s1 = {s["stageId"] for s in stages()}
user_quantity_grouped = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).groupByKey().mapValues(sum)
print(user_quantity_grouped.take(5))
s2 = {s["stageId"] for s in stages()}
for name, ids in (("reduceByKey", s1 - s0), ("groupByKey", s2 - s1)):
    for s in stages():
        if s["stageId"] in ids:
            print(name, "stage", s["stageId"], "shuffleWrite", s["shuffleWriteBytes"], "shuffleRead", s["shuffleReadBytes"])
print("memes resultats", sorted(user_quantity.collect()) == sorted(user_quantity_grouped.collect()))

print("=== Dedoublonnage")


def most_recent(order_a, order_b):
    return order_a if order_a["date"] > order_b["date"] else order_b


orders_dedup = orders_rdd.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).values()
print(orders_rdd.count(), orders_dedup.count())
with_dup = orders_rdd.union(sc.parallelize(orders_rdd.take(3)))
print("avec 3 doublons", with_dup.count(), "apres", with_dup.map(lambda o: (o["order_id"], o)).reduceByKey(most_recent).count())

print("=== Jointure")


def parse_user_for_join(line):
    fields = line.split(",")
    return fields[0], fields[1]


users_kv = users_records.map(parse_user_for_join)
orders_kv = orders_rdd.map(lambda o: (o["user_id"], o))
enriched = orders_kv.join(users_kv)
print(enriched.take(1))
print("join", enriched.count())
print("leftOuterJoin", orders_kv.leftOuterJoin(users_kv).count())

print("=== Exercice 1 aggregateByKey")
count_total = orders_rdd.map(lambda o: (o["user_id"], o["quantity"])).aggregateByKey(
    (0, 0),
    lambda acc, q: (acc[0] + 1, acc[1] + q),
    lambda a, b: (a[0] + b[0], a[1] + b[1]),
)
avg = count_total.mapValues(lambda c: round(c[1] / c[0], 2))
print(count_total.take(3))
print(avg.take(3))

print("=== Exercice 2 user inconnu")
fake = dict(orders_rdd.first())
fake["order_id"] = "00000000-0000-0000-0000-000000000000"
fake["user_id"] = "ffffffff-ffff-ffff-ffff-ffffffffffff"
orders_fake = orders_rdd.union(sc.parallelize([fake]))
left = orders_fake.map(lambda o: (o["user_id"], o)).leftOuterJoin(users_kv)
missing = left.filter(lambda kv: kv[1][1] is None)
print("sans user", missing.count(), missing.map(lambda kv: kv[0]).collect())
print("join normal", orders_fake.map(lambda o: (o["user_id"], o)).join(users_kv).count())

print("=== Exercice 3 DataFrame")
df = spark.read.csv("orders.csv", header=True, inferSchema=True)
by_user = df.groupBy("user_uuid").sum("quantity")
by_user.explain()
print(user_quantity.toDebugString().decode())
a = {r[0]: r[1] for r in by_user.collect()}
b = dict(user_quantity.collect())
print("DataFrame = RDD", a == b)

print("=== Exercice 4 cache")


def timing(rdd_orders):
    uq = rdd_orders.map(lambda o: (o["user_id"], o["quantity"])).reduceByKey(add)
    res = []
    for _ in range(3):
        t = time.perf_counter()
        rdd_orders.count()
        t1 = time.perf_counter()
        uq.count()
        t2 = time.perf_counter()
        res.append((round(t1 - t, 3), round(t2 - t1, 3)))
    return res


print("avec cache", timing(orders_rdd))
print("onglet Storage", [(r.name(), r.numCachedPartitions(), r.memSize()) for r in sc._jsc.sc().getRDDStorageInfo()])
orders_rdd.unpersist()
no_cache = orders_data.map(parse_order)
print("sans cache", timing(no_cache))

spark.stop()
