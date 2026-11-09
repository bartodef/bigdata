# Rendu Lab 10 : Trino

Namespace / bucket : `user-b-defelice-ece`. Trino version 480, Polaris 1.8.0.

## Fichiers du rendu

- `lab-10/lab-trino-deploy.sh` : partie 1 (déploiement Helm), sortie dans `lab-10/sortie-trino-deploy.txt`
- `lab-10/lab-trino.sh` : parties 2 et 3 et exercices, sortie dans `lab-10/sortie-trino.txt`
- `lab-10/lab_trino.py` : toutes les requêtes Trino (client Python `trino`)
- `lab-10/spark_polaris.py` : préparation des tables silver dans Polaris et exercice 5 (Spark)
- `lab-10/deployment/` : chart Helm (`Chart.yaml`, `values.yaml`)
- `lab_trino/gold/` : métadonnées Iceberg des tables gold (synchro S3 sans les fichiers `data/`)

Remarque : mon bronze avait été généré avec les options par défaut (2829 commandes). Pour avoir les mêmes chiffres que l'énoncé, j'ai régénéré les données avec `dataset-orders -C 100 -c 100` : 50 utilisateurs et 5000 commandes (100 par utilisateur).

## Partie 1 : déploiement de Trino

1. Patch du statefulset `vscode-pyspark` (limites 2000m / 2000Mi), sinon le quota du namespace (4 CPU / 4 Gi) bloque la création des pods.
2. Création de `Chart.yaml` et `values.yaml` (coordinator seul, catalogue `lakehouse` de type iceberg branché sur Polaris en REST avec OAuth2).
3. Secret `trino-polaris-oauth` (credential du principal `dev`), secret `polaris-s3`, configmap `trino-polaris-config`.
4. `helm upgrade --install trino ./lab-10/deployment --wait`

Résultat : pod `trino-coordinator` en Running et `curl http://trino:8080/v1/info` répond `HTTP/1.1 200 OK` avec `"version":"480"`.

## Partie 2 : silver vers gold

### Connexion et catalogues

- `SHOW CATALOGS` : `lakehouse`, `system`, `tpcds`, `tpch`
- `SHOW SCHEMAS FROM lakehouse` : `gold`, `information_schema`, `silver`, `system`
- `SHOW TABLES FROM lakehouse.silver` : `orders`, `users`

Question : le paramètre `catalog=` de `connect()` fixe le catalogue par défaut de la session. On peut écrire `silver.orders` au lieu de `lakehouse.silver.orders`. Les noms complets marchent toujours.

Types : `ordered_at` est en `timestamp(6) with time zone` dans Trino (Spark affichait juste `timestamp`). Trino garde donc le fuseau (UTC). `order_id` est en `varchar` : Trino a un type `uuid` mais la table a été écrite par Spark en string, donc Trino lit un string.

### Requêtes sur le silver

- `count(*)` orders = 5000, users = 50 (même chose que dans le lab Spark)
- Adresses par état : AE a 3 users et 3 villes manquantes, AP 2 users et 2 villes manquantes. Ce sont des adresses militaires (APO/DPO), sans vraie ville, donc le parsing du lab 7 renvoie NULL. Les autres états ont 0 ville manquante.
- Commandes orphelines (sans user) : 0. Un inner join ne perdra aucune commande.

Questions :

- La requête orpheline remplace le test `relationships` de dbt (clé étrangère `user_id` de orders vers users). Pour la relancer à chaque rafraîchissement, je la mettrais dans un script avec un `assert orphan_orders == 0` avant de construire le gold.
- Trino répond directement, sans créer de session Spark ni soumettre de job. Il est donc mieux pour les requêtes interactives et les outils de BI. Spark reste mieux pour les gros traitements batch.

### Métadonnées Iceberg

- `orders$snapshots` : 1 snapshot, opération `append`, écrit par Spark, 5000 lignes ajoutées
- `orders$files` : 1 fichier Parquet de 5000 lignes et 146358 octets
- `orders$history` : 1 ligne, snapshot courant

Avec `aws s3 ls` on voit les mêmes fichiers dans `warehouse/silver/orders/data/` plus les fichiers `metadata/` (json, avro). La table `$files` n'a pas besoin d'accès au bucket côté client : c'est Trino qui lit les manifestes Iceberg, le client fait juste du SQL.

### Plan d'exécution

`EXPLAIN (TYPE DISTRIBUTED)` sur la jointure orders / users groupée par état :

- 3 fragments : Fragment 2 scanne `users` et l'envoie en BROADCAST, Fragment 1 scanne `orders`, fait la jointure et l'agrégation partielle, Fragment 0 fait l'agrégation finale (SINGLE)
- 2 `TableScan`, un par table
- échanges `REMOTE` entre les fragments
- jointure `REPLICATED` (broadcast) : la table users ne fait que 50 lignes, donc elle est copiée sur tous les workers au lieu de partitionner les deux côtés

`EXPLAIN ANALYZE` : exécution 1.88 s, 5050 lignes en entrée (5000 commandes + 50 users), 37 lignes en sortie (37 états), mémoire très faible (quelques Ko). `system.runtime.queries` donne l'historique des requêtes avec leur durée.

### Couche gold

- `gold.orders_by_state_month` : 53 lignes (état x mois)
- `gold.top_products` : 6 produits, le croissant est premier avec 2620 unités (870 commandes), puis drink 2533, brioche 2513, bread 2502, donut 2461, cookie 2387
- `gold.user_activity` : 50 lignes, 100 commandes par user, 0 user sans commande

Questions :

- `user_activity` utilise un LEFT JOIN pour garder les users qui n'ont jamais commandé. `orders_by_state_month` utilise un inner join, c'est celle qui dépend de la requête orpheline : si une commande n'avait pas de user, elle disparaîtrait de l'agrégat.
- `age` est calculé avec `current_date`, donc la table devient fausse avec le temps. Il faut la recalculer régulièrement, ou stocker la date de naissance et calculer l'âge au moment de la requête.
- Les fichiers gold sont dans `s3://user-b-defelice-ece/warehouse/gold/<table>-<uuid>/data/`. C'est Polaris qui a choisi l'emplacement (base location du catalogue + namespace + table), Trino n'a rien précisé.

### Validation

- unités : silver 15016 = gold orders_by_state_month 15016 = top_products 15016
- users : silver 50 = user_activity 50
- commandes : silver 5000 = somme des orders de user_activity 5000

Les 4 `assert` passent.

### Rafraîchissement de top_products

`DELETE` (6 lignes) puis `INSERT` (6 lignes). La table a 3 snapshots : `append` (création), `delete`, `append`. Time travel avec `FOR VERSION AS OF` : 6 produits au premier snapshot, 0 après le DELETE, 6 en version courante.

Entre le DELETE et l'INSERT, un dashboard voit une table vide. Avec un `MERGE`, tout se fait en un seul snapshot : le dashboard voit soit l'ancienne version soit la nouvelle, jamais une table vide.

### Comparaison avec Spark

- Trino : juste une connexion HTTP au coordinator et du SQL. Les credentials Polaris et S3 sont côté serveur.
- Spark : une session à créer, les packages iceberg-spark-runtime et le bundle AWS à télécharger, les credentials Polaris et S3 dans la config de session, puis un job.
- Cas d'usage : dashboard rafraîchi chaque minute = Trino, transformation nocturne de 10 To = Spark, data scientist qui explore le silver = Trino.
- Perte d'un worker : ici les requêtes durent quelques secondes, relancer coûte rien. Sur 10 To une requête de plusieurs heures qui repart de zéro coûte très cher, donc il faut soit le mode fault tolerant de Trino soit Spark qui recalcule seulement les partitions perdues.

## Partie 3 : schéma en étoile

Tables créées :

- `dim_user` : 50 lignes, clé `user_key` par `row_number()`
- `dim_product` : 6 lignes
- `dim_date` : 209 lignes, un jour du 2020-01-01 au 2020-07-27, clé `YYYYMMDD`
- `fact_orders` : 5000 lignes

La requête année x état x produit sur l'étoile donne par exemple FM / drink en tête avec 181 unités. La reconstruction de `orders_by_state_month` depuis l'étoile donne exactement les mêmes lignes (différence vide).

Validation : 5000 commandes et 15016 unités dans le fait comme dans le silver, 0 commande sans dimension. En plus j'ai vérifié : 0 `order_id` en double, 0 `user_id` en double dans dim_user, 0 mauvaise clé user.

Questions :

- Les checks ne suffisent pas : si deux commandes échangent leur user ou leur produit, les totaux restent bons. Il faudrait comparer des agrégats par user et par produit entre silver et étoile, et vérifier l'unicité de `order_id` dans le fait.
- Grain de `fact_orders` : une commande. Grain de `dim_date` : un jour.
- Si `silver.users` a un `user_id` en double, le LEFT JOIN duplique les commandes : `silver_orders == fact_orders` échoue. Si une commande a un produit NULL, `product_key` est NULL : `unmatched == 0` échoue.
- Si on reconstruit `dim_user` après l'ajout d'un user, `row_number()` peut redistribuer les numéros, donc les `user_key` changent et `fact_orders` pointe sur les mauvais users. Il faut reconstruire le fait aussi, ou garder des clés stables.
- Si un user déménage, `dim_user` est écrasée et toutes ses commandes passées montrent le nouvel état. Pour garder l'historique il faut une dimension à évolution lente (SCD type 2) : plusieurs lignes par user avec dates de début et de fin de validité.
- `orders_by_state_month` répond à une seule question très vite. L'étoile répond à n'importe quelle combinaison (année, produit, jour de la semaine...) sans nouvelle table, mais chaque requête fait 2 ou 3 jointures.

## Exercices

### 1. Table partitionnée

`gold.orders_by_state_month_part` avec `partitioning = ARRAY['month(month)']`, 53 lignes.

- non partitionnée : 1 fichier Parquet de 53 lignes
- partitionnée : 1 fichier par mois (dossiers `month_month=2020-01`, `2020-02`...), par exemple 8 lignes pour février

Requête filtrée sur un mois avec `EXPLAIN ANALYZE` :

- non partitionnée : 53 lignes lues, 84.91 % filtrées après lecture
- partitionnée : 8 lignes lues, 0 % filtrées

Trino ne lit que la partition utile (partition pruning).

### 2. Table cohorts

`gold.cohorts` : 11 décennies de naissance (1910 à 2020). Par exemple 1960 : 8 users, 800 commandes, 2.94 unités par commande en moyenne. Chaque user ayant 100 commandes, le nombre de commandes vaut toujours 100 x users.

### 3. INSERT puis MERGE puis time travel

- `INSERT INTO silver.orders` d'une commande test (état VT, janvier 2020, 7 unités)
- `MERGE INTO gold.orders_by_state_month` : 1 ligne touchée, une nouvelle ligne VT / 2020-01 avec 1 commande et 7 unités
- `FOR VERSION AS OF` sur le snapshot précédent : la ligne VT / 2020-01 n'existe pas
- snapshots de la table gold : `append` (création) puis `overwrite` (le MERGE)

J'ai ensuite supprimé la commande test et remis la table gold dans son état initial.

### 4. ALTER TABLE EXECUTE optimize

Sur `gold.top_products` : avant comme après, 3 snapshots et 1 seul fichier Parquet de 797 octets. Rien n'a changé car la table n'a qu'un seul fichier déjà petit, il n'y a rien à compacter. `optimize` ne crée un nouveau snapshot que s'il réécrit des fichiers (par exemple beaucoup de petits fichiers après plusieurs INSERT).

### 5. Même agrégation en Spark

Depuis la session Spark connectée à Polaris : 53 lignes et 15016 unités, identique à Trino, 0 ligne différente.

## Conclusion

Trino lit les tables Iceberg créées par Spark grâce à Polaris, construit la couche gold en SQL simple (CTAS, INSERT, MERGE), et les deux moteurs voient exactement les mêmes données. Trino est le plus pratique pour les requêtes interactives et la BI, Spark reste le choix pour les gros batchs.
