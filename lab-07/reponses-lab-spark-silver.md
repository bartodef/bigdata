# Lab 7 Bronze vers Silver avec Spark DataFrames

Le code est dans `lab-07/lab_silver.py` (la création de la session est dans `lab-07/session.py`). Lancé sur Onyxia avec `python3 -u lab-07/lab_silver.py > lab-07/sortie-spark-silver.txt`. Spark 4.1.1 avec Scala 2.13 et `hadoop-aws-3.4.2`. Iceberg `iceberg-spark-runtime-4.1_2.13:1.11.0`.

Au début mon service avait une limite de 2 Go de mémoire et le pod a été tué (OOMKilled) pendant l'écriture Iceberg. J'ai passé le service à 4 CPU et 4000Mi comme demandé dans les prérequis et j'ai mis `spark.driver.memory` à 1g.

## SparkSession avec S3

**Les identifiants sont temporaires. Que se passe-t-il pour un job Spark si le service redémarre ?**

Le job est perdu. En mode `local[*]` tout Spark tourne dans le pod Onyxia donc si le pod redémarre tout ce qui n'est pas écrit sur S3 disparaît (DataFrames, vues, cache). Ça m'est arrivé : en plus le redémarrage a effacé `~/.aws` et j'ai dû remettre mes clés. Et même sans redémarrage le token finit par expirer et les appels S3 échouent.

**Pourquoi `SimpleAWSCredentialsProvider` ne marche pas ici ?**

Onyxia donne des identifiants temporaires : access key, secret key et session token. `SimpleAWSCredentialsProvider` envoie seulement l'access key et la secret key sans le token donc S3 refuse. `TemporaryAWSCredentialsProvider` envoie aussi le token. Avec des clés permanentes il n'y a pas de token donc le simple marche.

## Bronze

J'ai bien 50 users et 2829 commandes. Grâce à `multiLine=True` l'adresse sur 2 lignes reste dans une seule ligne (dans le lab RDD j'avais 101 lignes). `birthdate` est lu en `date` et `date` en `timestamp`. `uuid` reste en `string`.

## Silver

Avec 3 doublons ajoutés on passe à 2832 lignes et après le dédoublonnage on revient à 2829. La version SQL donne le même nombre et les mêmes colonnes.

**Expliquer le résultat de la requête par état.**

Elle compte les users par état et combien n'ont pas de ville. Les seuls sans ville sont AE (3 sur 3), AP (2 sur 2) et AA (1). Ce sont des adresses militaires comme `DPO AP 09617` qui n'ont pas de ville. Pour tous les vrais états `missing_city` vaut 0 donc le découpage de l'adresse marche. On voit aussi que `state` contient des territoires comme FM et VI. Avec 50 users il y a au max 2 ou 3 users par état.

**DataFrame ou SQL : lequel pour une exploration rapide et lequel pour un modèle utilisé par plusieurs collègues ?**

Les deux donnent le même plan donc la performance est la même. Pour une exploration rapide je prendrais SQL, c'est plus court et tout le monde le lit. Pour un modèle partagé je prendrais l'API DataFrame : on peut faire des fonctions réutilisables, tester chaque étape et les erreurs de colonnes se voient plus tôt.

**Où ailleurs `regexp_extract()` qui renvoie une chaîne vide peut donner un faux résultat ?**

Dans `state` et `zip_code`. Si l'adresse ne correspond pas au format on aurait `''` au lieu de NULL. Un `count(state)` les compterait et un `groupBy("state")` ferait un groupe vide. Un test `not_null` ne verrait rien. Ici j'ai vérifié : 0 state vide et 0 zip vide, mais il faudrait faire comme pour `city` et transformer `''` en NULL.

## Parquet

**Combien de part-files dans `users.parquet/` et pourquoi ?**

Il y a un seul fichier `part-00000-...snappy.parquet` plus un fichier `_SUCCESS`. Il y a un fichier par partition du DataFrame au moment de l'écriture et ici les DataFrames n'ont qu'une partition (1 seule petite lecture de CSV). Pour `orders` la fenêtre fait un shuffle mais Spark (AQE) regroupe les petites partitions donc il reste 1 fichier.

**Que perd-on avec `coalesce(1)` sur un gros dataset ?**

Le parallélisme. Tout est écrit par une seule tâche donc c'est lent et un seul executor doit tout avoir en mémoire. On ne peut plus lire le fichier en parallèle non plus. Et `coalesce(1)` peut même réduire le parallélisme des étapes d'avant.

La relecture donne bien 50 et 2829.

## Iceberg

**Comparer `silver/spark/orders.parquet/` et `warehouse/silver/orders/`.**

Dans le dossier Parquet il y a seulement les données (`part-00000...parquet`) et un `_SUCCESS`. Dans le dossier Iceberg les données sont dans `data/` (`00000-77-...parquet`, 2829 lignes). Ce qui décrit la table est dans `metadata/` : `v1.metadata.json` (schéma, partitions, snapshots), le manifest list `snap-....avro`, le manifest `...-m0.avro` (liste des fichiers de données avec leurs stats) et `version-hint.text` (la version actuelle).

**Un job réécrit le dossier Parquet pendant qu'une requête le lit. Que voit la requête ? Et avec Iceberg ?**

Avec Parquet le lecteur peut voir un mélange : des anciens fichiers déjà supprimés (erreur fichier introuvable), des nouveaux fichiers à moitié écrits, ou un dossier vide. Avec Iceberg le lecteur lit le snapshot qui était actif quand il a commencé. Les nouveaux fichiers ne sont visibles qu'au commit (changement du fichier de métadonnées) donc il voit soit l'ancienne version soit la nouvelle, jamais un mélange.

**Le pointeur vers la version actuelle est un fichier du bucket. Que se passe-t-il si deux jobs commitent en même temps ?**

Les deux peuvent créer le même `vN.metadata.json` ou écrire `version-hint.text` en même temps. S3 n'a pas de rename atomique ni de verrou donc un commit peut écraser l'autre et on perd des données. Un service de catalogue (Polaris, Nessie, Hive Metastore...) fait le changement de pointeur de façon atomique : si quelqu'un a commité avant, le deuxième commit est refusé et il réessaie. Le catalogue gère aussi les droits d'accès.

## Comparaison avec dbt

| | Commandes | Quantité | Users |
| --- | --- | --- | --- |
| dbt | 2829 | 8615 | 50 |
| Spark | 2829 | 8615 | 50 |

Les deux couches silver ont les mêmes lignes.

**Qu'est-ce qui change entre les deux schémas ?**

Avec dbt `order_id` et `user_id` sont en `UUID` et `ordered_at` en `TIMESTAMP WITH TIME ZONE`. Avec Spark ils sont en `VARCHAR` et `ordered_at` en `TIMESTAMP` sans fuseau. Spark n'a pas de type UUID donc j'ai gardé des chaînes de caractères. Pour ceux qui lisent la silver ça prend plus de place (36 octets au lieu de 16), rien ne vérifie que c'est un vrai UUID et pour faire une jointure avec la sortie dbt il faut caster.

**Un fichier par modèle avec dbt et un dossier de part-files avec Spark : lequel est plus simple et lequel passe à l'échelle ?**

Le fichier unique de dbt est plus simple à lire (un seul chemin). Le dossier de Spark passe mieux à l'échelle car chaque tâche écrit son fichier en parallèle et on peut lire en parallèle aussi.

**Qu'est-ce qui remplace les tests dbt dans ce lab ?**

Juste des `assert` sur le nombre de lignes et les colonnes, et la vérification à la main des doublons. Pour avoir les mêmes garanties il faudrait écrire des contrôles : `count` des NULL sur les clés (not_null), `groupBy().count() > 1` (unique), `isin()` sur la liste des produits (accepted_values) et une jointure `left_anti` entre commandes et users (relationships). On pourrait aussi utiliser une librairie comme Great Expectations.

**Qu'est-ce qui remplace `ref()` et `source()` ?**

Les variables Python et l'ordre des cellules. `stg_orders` dépend de `orders_bronze` parce qu'on l'a créé à partir de lui. Les vues temporaires (`createOrReplaceTempView`) servent aussi de noms. Mais il n'y a pas de graphe de dépendances : si on lance les cellules dans le désordre ça casse.

**Comparer les temps de dbt build et de Spark. Pourquoi ce n'est pas comparable ?**

`dbt build --select silver` prend environ 5 s (avec les tests). Ma partie silver Spark prend environ 39 s. Mais ce n'est pas la même chose : Spark doit démarrer une JVM et télécharger le jar Iceberg, dbt crée seulement des vues sans rien calculer (le calcul se fait à la lecture) alors que Spark lit, calcule et écrit vraiment les Parquet. Et Spark est fait pour des gros volumes, sur 300 Ko son temps de démarrage domine tout.

**dbt + DuckDB ou Spark pour : 10 Go par jour, 10 To par jour, une équipe qui ne connaît que SQL ?**

- 10 Go par jour : dbt avec DuckDB, ça tient sur une seule machine et c'est plus simple.
- 10 To par jour : Spark sur un cluster, une seule machine ne suffit plus.
- Des analystes qui ne connaissent que SQL : dbt (on peut aussi utiliser dbt avec Spark comme moteur si les données sont grosses).

## Exercices

### 1. birthdate_is_valid

Je fais la jointure entre `stg_users` et la première commande de chaque user, une fois avec l'API DataFrame et une fois en Spark SQL. Résultat : 45 valides et 5 invalides, pareil que dans le lab dbt. Les deux versions donnent exactement les mêmes lignes (0 différence avec `exceptAll`).

### 2. UDF

Temps sur `users_bronze` (meilleur de 5 essais) :

| Méthode | Temps |
| --- | --- |
| `regexp_extract()` | 113 ms |
| UDF Python | 155 ms |
| `pandas_udf` | 185 ms |

Les trois donnent le même résultat. `regexp_extract()` est le plus rapide car il reste dans la JVM. Les UDF doivent envoyer les données vers un process Python. Ici avec 50 lignes la `pandas_udf` est la plus lente à cause du coût de démarrage (Arrow). Sur beaucoup de lignes elle serait plus rapide que l'UDF Python normale car elle traite les données par paquets.

### 3. Cache

Après `.cache()` le plan montre `InMemoryTableScan` : les étapes suivantes lisent la mémoire au lieu de relire le CSV sur S3. Dans la Spark UI les DataFrames apparaissent dans l'onglet Storage.

### 4. Append et time travel

J'ai ajouté une commande avec `writeTo(...).append()`. La table a maintenant 2 snapshots (overwrite puis append). Avec `VERSION AS OF` sur le premier snapshot j'ai 2829 lignes et la table actuelle en a 2830.

Avec le dossier Parquet j'ai fait la même chose (`mode("append")`) : on passe aussi à 2830 mais il n'y a aucun moyen de revenir à l'ancienne version, il n'y a pas d'historique. Il faudrait garder une copie à la main. Après le test j'ai remis le Parquet comme avant pour que la comparaison avec dbt reste juste.
