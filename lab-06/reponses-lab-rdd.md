# Lab 6 Introduction aux RDD de Spark

Le code est dans `lab-06/lab_rdd.py`. Je l'ai lancé sur Onyxia (Spark 4.1.1 en `local[*]`) avec `python3 lab-06/lab_rdd.py 2>/dev/null > lab-06/sortie-rdd.txt`. Les fichiers `users.csv` et `orders.csv` sont en local dans le projet.

## Partitions

Résultats : `users.csv` donne 2 partitions et `orders.csv` aussi 2. Avec `repartition(8)` on passe à 8.

**Qu'est-ce qui décide du nombre de partitions de `textFile()` pour un fichier de cette taille ?**

Normalement Spark coupe le fichier en blocs de 128 Mo. Nos fichiers font 7 Ko et 330 Ko donc ça ferait 1 seule partition. Mais `textFile()` a un minimum `minPartitions` qui vaut par défaut `min(defaultParallelism, 2)` donc 2. Pour des petits fichiers c'est ce minimum qui décide. Pour un gros fichier ce serait la taille des blocs.

**`repartition()` et `coalesce()` : lequel fait un shuffle ?**

`repartition()` fait un shuffle complet : toutes les lignes sont redistribuées. Il peut augmenter ou diminuer le nombre de partitions. `coalesce()` ne fait pas de shuffle, il regroupe juste des partitions. Il est moins cher mais il peut seulement diminuer. J'ai testé : `coalesce(8)` reste à 2 partitions et `coalesce(1)` donne 1.

## Lazy evaluation

`toDebugString()` montre la lignée : `HadoopRDD` (lecture) puis `MapPartitionsRDD` puis `PythonRDD` (le filtre). Rien n'est encore calculé. `count()` donne 2829.

**On refait `count()`. Est-ce qu'il y a un nouveau stage ?**

Oui. Les deux `count()` ont créé 2 nouveaux stages (un par count). Spark ne garde pas le résultat. Une transformation c'est juste un plan donc à chaque action Spark relit le fichier et refait le filtre.

**Où est-ce que `.cache()` changerait ça ?**

Il faut appeler `orders_data.cache()` avant la première action, juste après le `filter`. Le premier `count()` calcule et garde les données en mémoire. Le deuxième fait toujours un job mais il lit la mémoire au lieu du fichier.

## L'adresse sur 2 lignes

`users_lines.count()` donne 101 au lieu de 50 (50 users sur 2 lignes chacun et l'en-tête). Une ligne sur deux c'est la fin de l'adresse (`Robinsonshire, KY 01352",...`) sans uuid. Après le filtre sur l'uuid au début on a bien 50.

**Pourquoi `spark.read.csv(multiLine=True)` n'a pas ce problème ?**

Le lecteur CSV comprend les guillemets. Quand un champ est entre guillemets il continue à lire même s'il y a un retour à la ligne. `textFile()` coupe à chaque retour à la ligne sans rien comprendre au CSV.

**Quand est-ce qu'un lecteur ligne par ligne est sûr ?**

Quand chaque enregistrement tient sur une seule ligne. Il ne faut aucun retour à la ligne dans les valeurs (comme le JSON lines ou un CSV sans champs multilignes).

## Parsing des commandes

**Que se passe-t-il s'il y a une virgule en trop dans `product` ?**

`split(",")` donne 6 morceaux au lieu de 5 et Python plante avec `ValueError: too many values to unpack`. J'ai testé avec une fausse ligne : le `map` ne plante pas tout de suite car il est lazy. L'erreur arrive seulement au `collect()` et c'est tout le job qui échoue.

**Où l'API DataFrame fait ce contrôle et quand ?**

Le lecteur CSV des DataFrames gère les guillemets et connaît le schéma (avec `inferSchema` ou un schéma donné). Il vérifie les colonnes et les types au moment de la lecture. Une ligne mauvaise donne des NULL (mode PERMISSIVE) ou une erreur (mode FAILFAST) au lieu de planter n'importe où plus tard.

## reduceByKey ou groupByKey

Les deux donnent le même résultat. Dans la Spark UI (je l'ai lu avec l'API REST de la UI) :

| | Shuffle write | Shuffle read |
| --- | --- | --- |
| reduceByKey | 2 513 octets | 1 065 octets |
| groupByKey | 6 230 octets | 2 643 octets |

**Qu'est-ce que `groupByKey` envoie dans le shuffle et pourquoi c'est plus cher ?**

`groupByKey` envoie toutes les quantités une par une (2829 valeurs) vers la partition du user. `reduceByKey` fait d'abord la somme dans chaque partition et envoie seulement une somme par user et par partition. Ici c'est 2,5 fois plus de données avec `groupByKey`. Sur un gros dataset ça fait beaucoup de réseau et de mémoire.

**Quand `groupByKey` est obligatoire ?**

Quand on a besoin de toutes les valeurs ensemble. Par exemple la médiane exacte ou trier les commandes de chaque user. On ne peut pas calculer une médiane en combinant des morceaux comme une somme.

## Dédoublonnage

Il n'y a pas de doublon dans les données (2829 avant et après). J'ai ajouté 3 doublons pour tester : 2832 lignes puis 2829 après le `reduceByKey`.

**Qu'est-ce qui joue le rôle du `partitionBy()` de la fenêtre ?**

C'est le `reduceByKey` sur la clé `order_id`. Il fait un shuffle pour mettre toutes les lignes d'une même commande sur la même partition et ensuite il en garde une avec `most_recent`.

**Quelle version montre le mieux l'intention ?**

La fonction de fenêtre. On voit directement la clé (`order_id`) et le tri (date décroissante). Avec le `reduceByKey` il faut lire la fonction `most_recent` pour comprendre quelle ligne est gardée.

## Jointure

La jointure donne 2829 lignes, chaque commande avec son username.

**Comment Spark pourrait éviter le shuffle ?**

Il faudrait que les deux RDD soient déjà partitionnés de la même façon sur `user_id` (même partitioner et même nombre de partitions, par exemple avec `partitionBy()` avant). Comme ça les mêmes clés sont déjà sur les mêmes partitions.

**Avec `leftOuterJoin()` qu'est-ce qui change si un `user_id` n'a pas de user ?**

La commande est gardée avec `None` à la place du username. Avec `join()` elle disparaît. Ici ça donne aussi 2829 car toutes les commandes ont un user.

**Quel risque de faire `.collect()` sur `enriched` ?**

`collect()` ramène tout dans la mémoire du driver. Sur un gros dataset le driver n'a pas assez de mémoire et plante (OutOfMemory). Il faut filtrer ou agréger avant, ou utiliser `take()` ou écrire le résultat dans des fichiers.

## Exercices

### 1. aggregateByKey

J'ai fait `aggregateByKey((0, 0), ...)` qui donne (nombre de commandes, quantité totale) par user en un seul passage. Ensuite la moyenne c'est total / nombre. Par exemple un user a 4 commandes et 12 produits donc 3,0 de moyenne. Un autre a 85 commandes pour 253 donc 2,98.

### 2. user inconnu

J'ai ajouté une commande avec un `user_id` qui n'existe pas. Avec `leftOuterJoin()` puis un filtre sur `None` je trouve 1 commande sans user. Avec `join()` normal elle disparaît (2829).

### 3. Avec un DataFrame

`df.groupBy("user_uuid").sum("quantity")` tient en 1 ligne contre 3 lignes pour le RDD (map, reduceByKey et le parsing à écrire avant). Les résultats sont les mêmes.

`.explain()` montre un `HashAggregate` partiel, un `Exchange hashpartitioning` (le shuffle) puis un `HashAggregate` final. Spark lit seulement les 2 colonnes utiles (`ReadSchema: user_uuid, quantity`). C'est la même idée que `reduceByKey` mais Spark l'a optimisé tout seul. Le `toDebugString()` du RDD montre juste la suite des RDD avec le `ShuffledRDD`.

### 4. Cache

Temps en secondes (count de orders_rdd puis count de user_quantity, 3 essais) :

| | Essai 1 | Essai 2 | Essai 3 |
| --- | --- | --- | --- |
| avec cache | 0,092 / 0,476 | 0,155 / 0,159 | 0,14 / 0,162 |
| sans cache | 0,147 / 0,311 | 0,164 / 0,188 | 0,321 / 0,284 |

Avec le cache l'onglet Storage montre le `PythonRDD` avec 2 partitions en mémoire (172 Ko). Sans cache il n'y a rien. Le cache va un peu plus vite mais la différence est petite car le fichier est petit et en local. Sur un gros fichier sur S3 la différence serait beaucoup plus grande.
