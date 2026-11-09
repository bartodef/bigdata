# Lab 10 Trino

Tout se lance depuis la racine du projet (`~/work/bigdata`) :

- `lab-10/lab-trino-deploy.sh` : partie 1, déploiement de Trino avec Helm (chart dans `lab-10/deployment/`), sortie dans `lab-10/sortie-trino-deploy.txt`.
- `lab-10/lab-trino.sh` : parties 2 et 3. Il prépare d'abord les tables silver dans Polaris avec Spark (`lab-10/spark_polaris.py prep`), puis lance toutes les requêtes Trino (`lab-10/lab_trino.py`), puis l'exercice 5 côté Spark. Sortie dans `lab-10/sortie-trino.txt`.

Le `kubectl patch` du service vscode-pyspark (limites 2000m / 2000Mi) est obligatoire : le namespace a un quota `onyxia-quota` de 4 CPU / 4Gi en limites, et le pod vscode prenait déjà tout (4 / 4000Mi). Sans le patch, `helm install polaris` échoue (`exceeded quota`, puis `context deadline exceeded`) parce que les pods Polaris et PostgreSQL ne peuvent pas être créés. Après le redémarrage du pod il a fallu remettre le profil S3 `default` (script d'Onyxia), et supprimer le PVC PostgreSQL du premier essai raté (il gardait un ancien mot de passe : `password authentication failed for user "polaris"`).

Les tables gold d'une exécution précédente sont supprimées au début (par Spark, voir la partie Gold), comme ça on peut relancer le lab.

**Données : les mêmes que l'énoncé, 5000 commandes et 50 utilisateurs.** L'énoncé part de 5000 commandes, soit exactement 100 commandes par utilisateur : c'est le générateur du lab 3 lancé avec `-C 100 -c 100`. Le bronze de mon bucket a été généré avec les options par défaut (entre 0 et 100 commandes par utilisateur), d'où seulement 2829 commandes dans les labs 7 et 8. Pour ce lab, `spark_polaris.py prep` régénère les données avec `uv run dataset-orders -C 100 -c 100` (les 50 utilisateurs sont les mêmes, le générateur a une graine fixe), applique les mêmes transformations que le lab 7 (`lab-07/lab_silver.py`) et recrée `silver.orders` et `silver.users` dans Polaris, avec exactement les colonnes de l'énoncé. Les tables du lab 7 et 8 (catalogue `lab`) ne sont pas touchées : le `DROP TABLE` de Spark ne purge pas les fichiers.

Il reste de petites différences avec les chiffres affichés dans l'énoncé (par exemple 15016 unités au lieu de 14976, croissant 870 commandes au lieu de 883) : les exemples de l'énoncé incluent les commandes ajoutées pendant les exercices des labs 7 et 8 (d'où un utilisateur à 102 commandes et `AA` à 101), et la version de Faker peut changer quelques valeurs.

## Partie 1 : déploiement

Trino est déployé en mode coordinateur seul (`workers: 0`, `includeCoordinator: true`) : le coordinateur fait aussi le travail des workers, ce qui suffit pour un lab avec 1 Go de mémoire. Le catalogue `lakehouse` est un connecteur Iceberg de type REST qui pointe sur Polaris. Trino s'authentifie à Polaris en OAuth2 avec le principal `dev` (secret `trino-polaris-oauth`) et lit les fichiers sur S3 directement avec les clés du secret `polaris-s3`. `curl http://trino:8080/v1/info` répond `200 OK`.

## Environnement

**Que change le paramètre `catalog=` de `connect()` ?**

C'est le catalogue par défaut de la session. Les noms de tables qui ne sont pas complets sont cherchés dans `lakehouse`. On pourrait écrire `silver.orders` au lieu de `lakehouse.silver.orders`. Ça ne limite pas l'accès aux autres catalogues : `system.runtime.queries` marche quand même avec le nom complet. Avec `schema=` en plus, on pourrait écrire juste `orders`.

## Catalogs, schemas and tables

**Quel type Trino pour `ordered_at`, avec fuseau horaire ?**

Trino affiche `timestamp(6) with time zone`. C'est le type Iceberg `timestamptz` : précision à la microseconde, et la valeur est un instant absolu stocké en UTC. Trino l'affiche avec `+00:00`. Côté Spark c'était un `timestamp` (qui est aussi « avec fuseau », mais affiché dans le fuseau de la session).

**`order_id` en Spark était un string. Que rapporte Trino ?**

`varchar`. Trino a un type `uuid`, mais il lit le type du schéma Iceberg, et Spark a écrit une colonne `string`. Le type vient de la table, pas du moteur. Pour avoir un `uuid` il faudrait que la table soit créée avec ce type dans Iceberg.

## Query the silver layer

**Expliquer la sortie des requêtes.**

- `count(*)` donne 5000 commandes et 50 utilisateurs, comme dans l'énoncé (Trino lit les fichiers Parquet écrits par Spark, au même snapshot).
- La requête par `state` donne la même chose que l'énoncé : `FM` 3, `AE` 3 (3 sans ville), puis des états à 2 dont `AP` (2 sans ville). `missing_city` compte les adresses où le parsing du lab Spark n'a pas trouvé de ville : ce sont les adresses militaires (`AE`, `AP`, format « Unit / PSC / Box ») qui n'ont pas de ville au sens classique.
- La requête des orphelins donne 0 : toutes les commandes ont un utilisateur, donc une jointure interne ne perdra aucune ligne.

**Quel test du lab dbt remplace la requête des orphelins ? Comment la lancer à chaque refresh ?**

Le test `relationships` de dbt (`user_id` de `orders` doit exister dans `users`). Pour le faire à chaque refresh, on le met dans le pipeline avant de construire le gold, et on arrête si le résultat n'est pas 0 (un `assert` dans le script Python, ou un test dbt avec l'adaptateur `dbt-trino` qui lance la même requête).

**Pas de job ni de session à créer : quand choisir Trino plutôt que Spark ?**

Trino est un service qui tourne déjà : une requête part tout de suite et la réponse arrive en quelques secondes. Spark doit démarrer une JVM, télécharger les packages Iceberg, créer une session. Donc Trino pour les requêtes interactives, l'exploration, la BI, les petites transformations SQL. Spark pour les gros traitements batch, le code Python et le ML.

## Iceberg metadata

**Combien de snapshots a `silver.orders` ? Quel moteur les a écrits ?**

1 seul snapshot (`append`, 5000 lignes), écrit par Spark (`engine-name = spark` dans `summary`) au moment de `writeTo(...).create()`. L'énoncé en montre 3 (`overwrite` puis 2 `append`) parce que sa table a reçu les appends des exercices des labs 7 et 8. Trino n'a rien écrit dans silver, il lit seulement (sauf l'exercice 3, voir plus bas).

**`aws s3 ls` comparé à `$files` : lequel n'a pas besoin du bucket ?**

`$files` donne 1 fichier Parquet (5000 lignes, 146 Ko). `aws s3 ls warehouse/silver/orders/` en montre beaucoup plus (une dizaine de fichiers dans `data/` et autant de `metadata.json`), parce que le dossier contient aussi les fichiers de l'ancienne table du lab 7 et 8 et de la table supprimée (le `DROP` sans purge laisse les fichiers). `aws s3 ls` liste tous les objets du dossier, y compris les fichiers qui ne font plus partie du snapshot courant (anciens fichiers, métadonnées). `$files` donne seulement les fichiers de données du snapshot courant, avec le nombre de lignes. `$files` n'a pas besoin de lister le bucket : Trino lit les manifests (fichiers `.avro` référencés par les métadonnées que Polaris lui indique), qui contiennent déjà la liste des fichiers et leurs stats. C'est tout l'intérêt d'Iceberg : pas de listing de répertoire, qui est lent sur S3.

## Query plan and execution

**Quelle table est diffusée (broadcast) dans la jointure, et pourquoi ?**

`users`. Dans le plan, la jointure est `InnerJoin[..., distribution = REPLICATED]` : le Fragment 2 lit `users` (estimé à 50 lignes, 4.02kB) et l'envoie entier au Fragment 1, qui lit `orders` (5000 lignes), fait la jointure et une agrégation `PARTIAL`. Le Fragment 0 `[HASH]` fait l'agrégation `FINAL`. `users` a 50 lignes contre 5000 pour `orders` : il est beaucoup moins cher d'envoyer la petite table entière à chaque tâche que de répartir (hasher) les deux tables sur le réseau. Trino le choisit avec les statistiques Iceberg (nombre de lignes dans les manifests).

**Où est l'équivalent du shuffle dans le plan Trino ?**

Dans les `RemoteExchange` / `RemoteSource` entre les fragments, en particulier l'échange `REPARTITION` sur `state` avant l'agrégation finale (agrégation `PARTIAL` puis `FINAL`). Les lignes sont redistribuées par hash de la clé de regroupement, comme le shuffle de Spark, mais en flux en mémoire et sans écriture sur disque.

**Quelles statistiques d'`EXPLAIN ANALYZE` regarder en premier ?**

Dans mon `EXPLAIN ANALYZE`, le fragment qui lit les tables reçoit 5050 lignes (5000 + 50) et le temps est surtout du « Blocked » (attente des données S3 et des autres fragments), le CPU est de quelques dizaines de ms. `system.runtime.queries` marche (l'`EXPLAIN ANALYZE` a pris 1,2 s). À regarder en premier : le temps CPU et le temps écoulé (« wall time ») de chaque fragment, pour trouver l'étape lente. Puis le nombre de lignes en entrée des `TableScan` (si on lit beaucoup plus que nécessaire, le filtre n'est pas poussé ou il n'y a pas de partitionnement), la mémoire max (peak memory), et les lignes en sortie de la jointure (une jointure qui explose le nombre de lignes). Les écarts entre tâches (skew) aussi.

## Gold layer

**`LEFT JOIN` pour `user_activity`, jointure interne pour `orders_by_state_month` : lequel dépend de la requête des orphelins ?**

`user_activity` part des utilisateurs et doit garder ceux qui n'ont jamais commandé (0 commande), d'où le `LEFT JOIN` depuis `users`. `orders_by_state_month` a besoin du `state` de l'utilisateur, donc une jointure interne. C'est elle qui dépend des orphelins : une commande sans utilisateur disparaîtrait sans erreur et le total serait faux. Avec 0 orphelin ça ne change rien.

**`current_date` fait changer `age` tous les jours : conséquence pour le refresh ?**

La table est juste le jour où elle est construite, ensuite elle vieillit même si silver ne change pas. Il faut la reconstruire tous les jours (au moins), ou ne pas stocker l'âge mais la date de naissance et calculer l'âge au moment de la requête (dans une vue).

**Où sont les fichiers des tables gold, et qui a décidé de l'emplacement ?**

Dans `s3://user-b-defelice-ece/warehouse/gold/top_products-<uuid>/data/` (voir `top_products$files`). La base vient de Polaris : le catalogue a un `default-base-location` `s3://<bucket>/warehouse/`, et une table va dans `base/namespace/table`. Le suffixe `-<uuid>` après le nom de table est ajouté par Trino (option `iceberg.unique-table-location`, activée par défaut) pour qu'une table recréée avec le même nom n'écrase pas les fichiers de l'ancienne. C'est pour ça que `aws s3 ls warehouse/gold/top_products/` ne trouve rien à l'exercice 4.

Pour relancer le lab il faut supprimer les tables gold, mais le `DROP TABLE` de Trino demande aussi la purge des fichiers, et Polaris la refuse par défaut (`403`, `Unable to purge entity`, option `DROP_WITH_PURGE_ENABLED`). Je les supprime donc avec Spark (`DROP TABLE` sans purge) dans `spark_polaris.py prep`.

Résultats : `orders_by_state_month` 53 lignes (en janvier 2020, `AP`, `AS`, `GA`... ont chacun 100 commandes et 1 acheteur), `top_products` 6 produits (croissant 1er avec 870 commandes et 2620 unités, puis drink 2533, brioche 2513, bread 2502, donut 2461, cookie 2387), `user_activity` 50 lignes. Ici aucun utilisateur n'est sans commande (ils en ont tous 100), mais le `LEFT JOIN` les garderait.

## Validation

Les 4 assertions passent : 15016 unités partout, 50 utilisateurs, 5000 commandes.

**Quel défaut du silver une assertion attraperait, et lequel passerait ?**

Attrapé : une commande sans utilisateur (le total d'`orders_by_state_month` et de `user_activity` serait plus petit que silver), ou un `user_id` en double dans `users` (la jointure doublerait des commandes, total trop grand). Pas attrapé : tout ce qui garde les totaux. Une commande en double dans silver (elle est comptée partout pareil), une quantité fausse, un mauvais `state` sur un utilisateur, une date décalée : les sommes restent égales.

**La première assertion échoue si une commande n'a pas d'utilisateur : bug du gold ou du silver ?**

Du silver. Le gold fait ce qu'on lui demande (jointure interne). C'est la donnée silver qui n'est pas cohérente (intégrité référentielle). Il faut corriger en amont ou tester avant de construire le gold.

## Refresh a gold table

**Combien de snapshots après le refresh ? Quelle `operation` ?**

3 snapshots : `append` (le CTAS), `delete` (le `DELETE FROM`, qui supprime des fichiers entiers), `append` (l'`INSERT`). Le time travel donne 6 produits sur le premier snapshot, 0 sur celui du `DELETE`, et 6 sur la version courante.

**Entre le `DELETE` et l'`INSERT`, que voit un tableau de bord ? Et avec un `MERGE` ?**

Une table vide : chaque instruction est commitée séparément, donc le snapshot vide est visible entre les deux. Avec un `MERGE` (ou `CREATE OR REPLACE TABLE`), tout est fait dans un seul commit : le tableau de bord voit l'ancienne version puis la nouvelle, jamais un état intermédiaire.

**Comparer avec le dossier Parquet du lab Spark écrasé pendant une lecture.**

Avec un dossier Parquet en `overwrite`, Spark supprime les anciens fichiers puis écrit les nouveaux : un lecteur peut voir un dossier vide, un mélange, ou planter parce qu'un fichier a disparu. Avec Iceberg, les anciens fichiers restent tant que le snapshot existe et un lecteur lit toujours un snapshot complet (isolation par snapshot).

## Compare with Spark

**Ce dont chaque moteur a besoin pour `top_products`, en plus de la requête.**

Trino : un client (`trino` en Python) et l'adresse du serveur. Le catalogue, les accès Polaris et S3 sont dans la config du serveur. Spark : une session à créer, le package `iceberg-spark-runtime` à télécharger, la config du catalogue REST, les identifiants Polaris et les clés S3 dans le code ou l'environnement, et un job qui tourne pendant la requête.

**Ce que les deux moteurs partagent et ce qu'ils ne savent pas l'un de l'autre.**

Ils partagent le catalogue Polaris, le format de table Iceberg (métadonnées, manifests) et les fichiers Parquet sur S3. Ils ne savent rien l'un de l'autre : pas de mémoire, pas de cache, pas de session en commun. Chacun voit seulement les snapshots commités par l'autre, avec son propre système de types.

**Trino ou Spark ?**

- Tableau de bord rafraîchi toutes les minutes sur le gold : Trino (faible latence, toujours démarré, SQL pour les outils BI).
- Transformation de nuit de 10 To de fichiers bronze : Spark (gros volume, tolérance aux pannes, peut écrire sur disque).
- Data scientist qui explore silver avec une requête : Trino pour explorer vite en SQL. Spark seulement s'il continue en Python / ML ensuite.

**Pourquoi l'échec de toute la requête est acceptable ici et pas pour 10 To ?**

Ici les requêtes durent quelques secondes : si un worker tombe on relance, ça ne coûte presque rien. Une transformation de 10 To dure des heures sur beaucoup de machines, la probabilité de perdre un nœud devient forte, et tout recommencer coûte des heures. Il faut alors `retry-policy=TASK` (avec un exchange manager sur S3) ou Spark, qui recalcule seulement les partitions perdues.

# Partie 3 : star schema

Comme dans l'énoncé : `dim_user` 50 lignes, `dim_product` 6 (bread, brioche, cookie, croissant, donut, drink), `fact_orders` 5000 lignes comme `silver.orders`. `dim_date` a 209 jours (du 2020-01-01 au 2020-07-27 : 5000 commandes, une par heure). La requête qui reconstruit `orders_by_state_month` depuis l'étoile donne les mêmes lignes : le `EXCEPT` avec la table gold est vide. Les 3 assertions passent (5000 commandes, 15016 unités, 0 ligne sans dimension).

**Ces contrôles suffisent-ils ? Exemple de défaut non détecté et contrôles en plus.**

Non. Ils vérifient le nombre de lignes, la somme des quantités et que chaque clé existe dans sa dimension. Mais une commande peut être rattachée au mauvais utilisateur (clé qui existe mais fausse) sans changer aucun total. Pareil pour une date décalée d'un jour à cause du fuseau horaire. Avec les données des labs 7 et 8, le contrôle en plus du script trouvait d'ailleurs 1 `order_id` en double alors que les 3 assertions passaient. Avec les données de l'énoncé, il trouve 0. Contrôles en plus (dans le script) : `order_id` unique dans `fact_orders`, `user_id` unique dans `dim_user`, et comparaison ligne à ligne : en rejoignant `fact_orders` à `dim_user` et à `silver.orders` sur `order_id`, le `user_id` doit être le même (0 ligne différente). On peut aussi comparer les agrégats par dimension (par state, par produit, par jour) avec le silver.

**Grain de `fact_orders` ? de `dim_date` ?**

`fact_orders` : une ligne par commande. `dim_date` : une ligne par jour calendaire.

**Quelle assertion échoue si `user_id` est en double dans `silver.users` ? Et si une commande n'a pas de produit ?**

`user_id` en double : `dim_user` a deux lignes pour le même `user_id`, la jointure de `fact_orders` duplique les commandes de cet utilisateur, donc `silver_orders == fact_orders` et `silver_units == fact_units` échouent. Produit `NULL` : `p.product = NULL` ne trouve rien (le `DISTINCT` crée bien une ligne `NULL` dans `dim_product`, mais l'égalité avec `NULL` n'est jamais vraie), donc `product_key` est `NULL` et c'est l'assertion `unmatched == 0` qui échoue.

**`row_number()` pour les clés : que devient `user_key` si on reconstruit `dim_user` après l'ajout d'un utilisateur ?**

Les clés sont recalculées dans l'ordre de `user_id`. Un nouvel utilisateur qui se range au milieu décale toutes les clés suivantes. Si `fact_orders` n'est pas reconstruite en même temps, ses clés pointent vers les mauvais utilisateurs, sans erreur. Il faut tout reconstruire ensemble, ou garder les clés existantes et ne donner une nouvelle clé qu'aux nouveaux (`max(user_key) + row_number()` avec un `MERGE`), ou utiliser une clé stable (hash du `user_id`).

**Si un utilisateur change d'état, quel état voient les anciennes commandes ? Que faudrait-il pour garder l'historique ?**

Le nouvel état : `dim_user` est écrasée (SCD de type 1), donc toutes les commandes passées sont comptées dans le nouvel état. Pour garder l'historique, il faut une dimension à évolution lente de type 2 : une nouvelle ligne avec une nouvelle clé à chaque changement, des colonnes `valid_from`, `valid_to` et `is_current`, et la fact qui prend la clé valide à la date de la commande.

**`orders_by_state_month` vs la requête sur l'étoile.**

`orders_by_state_month` répond à une seule question (état × mois) et c'est très rapide : la table est déjà agrégée et petite. Toute nouvelle question (par produit, par trimestre, par sexe, week-end ou non) demande une nouvelle table. L'étoile répond à toutes ces combinaisons avec juste des jointures, sans nouvelle table. Le prix c'est au moment de la requête : lire la fact entière et faire plusieurs jointures et l'agrégation à chaque fois. En pratique on garde l'étoile comme base, et on crée des agrégats pour les tableaux de bord les plus utilisés.

# Exercices

**1. Table partitionnée par mois.** La table partitionnée a 7 fichiers, un par mois (dossiers `data/month_month=2020-01/` à `2020-07/`, colonne `partition` dans `$files`), la table normale un seul fichier (sa table `$files` n'a pas de colonne `partition`). Avec un filtre sur janvier 2020, `EXPLAIN ANALYZE` lit les 53 lignes de la table normale (85 % filtrées après lecture) et seulement 8 sur la table partitionnée (0 % filtré) : Trino ne lit que le fichier du bon mois grâce aux partitions (partition pruning).

**2. `gold.cohorts`.** Décennie = `(year(birthdate) / 10) * 10` (division entière). `LEFT JOIN` depuis `users` pour compter aussi ceux qui n'ont pas commandé, `count(DISTINCT user_id)` parce que la jointure répète l'utilisateur sur chaque commande, et `sum(quantity) / count(order_id)` en `double` pour la moyenne. Résultat :

| birth_decade | users | orders | avg_units_per_order |
| --- | --- | --- | --- |
| 1910 | 1 | 100 | 3.05 |
| 1930 | 5 | 500 | 2.93 |
| 1940 | 4 | 400 | 2.91 |
| 1950 | 6 | 600 | 3.05 |
| 1960 | 8 | 800 | 2.94 |
| 1970 | 5 | 500 | 3.04 |
| 1980 | 2 | 200 | 3.12 |
| 1990 | 5 | 500 | 3.09 |
| 2000 | 4 | 400 | 3.03 |
| 2010 | 5 | 500 | 3.04 |
| 2020 | 5 | 500 | 2.96 |

Chaque utilisateur a 100 commandes, donc `orders` = 100 × `users`. La moyenne est autour de 3 unités par commande pour toutes les décennies (la quantité est tirée au hasard entre 1 et 5).

**3. INSERT + MERGE + time travel.** J'ajoute une commande test (`test-lab-trino-0001`, 7 croissants le 2020-01-03) au premier utilisateur, qui est dans le `VT`. Le `MERGE` ne touche qu'une ligne : `VT` n'avait pas de commande en janvier, donc il insère la ligne (`VT`, janvier, 1 commande, 7 unités, 1 acheteur) et commit un snapshot `overwrite`. Avec `FOR VERSION AS OF <snapshot d'avant>` la ligne n'existe pas (résultat vide), c'est bien l'état précédent.

Pour remettre en état, je supprime la commande test et je relance le `MERGE`... qui ne change rien (0 ligne) : le `MERGE` de Trino n'a pas de clause `WHEN NOT MATCHED BY SOURCE`, il ne peut pas supprimer une ligne gold qui n'existe plus dans la source (à mon premier essai, le gold gardait 1 commande de trop et l'exercice 5 trouvait une ligne de plus côté Trino). D'où le `DELETE ... WHERE (state, month) NOT IN (source)` après le `MERGE` : 1 ligne supprimée, gold et silver reviennent à 5000. Un refresh incrémental par `MERGE` doit donc aussi gérer les suppressions.

**4. `optimize`.** Avant : 3 snapshots (`append`, `delete`, `append`) et un seul fichier de données (celui de l'`INSERT`, 6 lignes, 797 octets, celui du CTAS a été retiré par le `DELETE`). Après : exactement pareil, pas de nouveau snapshot. `optimize` réécrit les petits fichiers en un plus gros (compaction) et crée alors un snapshot `replace`, mais avec un seul fichier il n'y a rien à fusionner, donc il ne fait rien. Les anciens fichiers restent sur S3 tant que les anciens snapshots existent : il faut `expire_snapshots` puis `remove_orphan_files` pour libérer la place. (`aws s3 ls warehouse/gold/top_products/` ne trouve rien à cause du suffixe `-<uuid>` du dossier, voir la partie Gold.)

**5. Même agrégation dans Spark.** Spark lit les mêmes tables silver par Polaris et calcule `orders_by_state_month` : 53 lignes et 15016 unités, comme la table gold écrite par Trino, et 0 ligne différente. Spark lit aussi directement `polaris.gold.orders_by_state_month`, créée par Trino : les deux moteurs voient la même table grâce au catalogue commun. (C'est cette comparaison qui avait montré la ligne en trop de l'exercice 3 avant l'ajout du `DELETE`.)
