# Lab 9 : Polaris

Fichiers : `1-ressources.sh` (patch du service vscode), `2-deploiement.sh` (déploiement Helm de Polaris, sortie dans `sortie-polaris-deploiement.txt`), `3-lab.sh` et `lab_polaris.py` (toute la partie Spark, sortie dans `sortie-polaris.txt`). Les métadonnées Iceberg sont commitées dans `warehouse/silver/orders/metadata` et `warehouse/silver/users/metadata`.

## SparkSession avec le catalogue Polaris

`SHOW NAMESPACES IN lab` renvoie `gold` et `silver`, `SHOW NAMESPACES IN polaris` renvoie seulement `silver`.

**Qui répond à chaque commande ?** La première est traitée par Spark lui même, qui liste les dossiers du bucket sous `warehouse/` (catalogue hadoop). La deuxième est une requête HTTP vers Polaris (`/api/catalog`), c'est lui qui renvoie la liste. On le voit parce que les deux listes ne sont pas les mêmes alors que c'est le même bucket : Polaris ne connaît que ce qu'on a déclaré chez lui.

**Où est utilisé le credential ?** Dans la requête vers Polaris. Spark l'échange contre un token OAuth2 et l'envoie dans chaque appel à l'API. Le bucket, lui, est lu avec les clés S3 de la session (pas de credential vending sur Ceph).

## Enregistrer la table silver dans Polaris

`version-hint.text` vaut 5, j'ai donc enregistré `v5.metadata.json`. `polaris.silver.orders` et `lab.silver.orders` donnent toutes les deux 2834 commandes (2829 au départ plus les lignes ajoutées par les exercices du lab Iceberg).

**Qu'a écrit `register_table` ?** Rien dans le bucket : il y a 75 objets sous `warehouse/silver/orders/` avant et après. Polaris a juste ajouté dans sa base PostgreSQL une entrée `silver.orders` qui pointe vers `v5.metadata.json`.

**Pourquoi c'est dangereux de continuer à écrire via `lab` ?** Les deux catalogues ont chacun leur pointeur. Un commit via `lab` écrit `v6.metadata.json` et met à jour `version-hint.text`, mais Polaris reste sur `v5`. Les deux catalogues ne voient plus la même table et une écriture via Polaris ensuite créerait une deuxième branche de métadonnées.

## Créer des tables en dev

`polaris.silver.users` est créée depuis `lab.silver.users` : 53 utilisateurs (50 au départ plus ceux ajoutés dans les exercices du lab Iceberg).

**Où Polaris a stocké les fichiers de `silver.users` ?** Dans `s3://user-b-defelice-ece/warehouse/silver/users/`, c'est à dire base location du catalogue (`warehouse/`) + namespace + nom de table. C'est Polaris qui choisit, on ne donne pas de chemin. Le dossier est le même que celui de la table hadoop du lab Spark, les deux tables s'y retrouvent donc mélangées (chacune avec ses propres fichiers de métadonnées).

**Différence entre enregistrée et créée via Polaris ?** Pour `orders` (enregistrée) les fichiers de données existaient déjà et Polaris n'a fait que les référencer. Pour `users` (créée) c'est Spark via Polaris qui a écrit les Parquet et le premier fichier `00000-....metadata.json`. Dans les deux cas les données sont dans mon bucket, Polaris ne stocke que des pointeurs.

## Changement de schéma en dev

`ALTER TABLE polaris.silver.users ADD COLUMN country` a échoué avec `FIELD_ALREADY_EXISTS` : la colonne `country` existait déjà parce que l'exercice 4 du lab Iceberg l'avait ajoutée à `lab.silver.users`. J'ai donc ajouté une colonne `region` à la place, et `DESCRIBE` montre bien les 12 colonnes.

**Quel fichier de métadonnées enregistre le changement ?** Un nouveau fichier `00001-....metadata.json` dans `warehouse/silver/users/metadata/`. Ce n'est pas le fichier pointé par `register_table` : celui là concerne `orders` (`v5.metadata.json`) et il n'a pas bougé (`metadata_log_entries` d'`orders` montre toujours v1 à v5).

**Quel privilège Polaris vérifie ?** `TABLE_WRITE_PROPERTIES` (opération `ADD_TABLE_SCHEMA`), inclus dans `CATALOG_MANAGE_CONTENT` du rôle `catalog_contributor` de `dev`.

## Lecture en analyst

En `analyst`, `SHOW TABLES` et le `GROUP BY product` marchent (croissant 491, drink 484, cookie 470, brioche 468, donut 462, bread 459).

L'`INSERT` échoue avec : `ForbiddenException: Forbidden: Principal 'analyst' with activated PrincipalRoles '[data_analyst]' and activated grants via '[data_analyst, catalog_reader]' is not authorized for op ADD_TABLE_SNAPSHOT`. L'`ALTER TABLE ... ADD COLUMN phone` échoue pareil avec `op ADD_TABLE_SCHEMA`.

**Quel privilège manque pour l'INSERT ?** `TABLE_WRITE_DATA`. `catalog_reader` ne donne que `TABLE_READ_DATA`, `TABLE_READ_PROPERTIES` et `CATALOG_READ_PROPERTIES`.

**L'INSERT raté a t il créé un snapshot ?** Non. En `dev`, `silver.orders` a toujours ses 4 snapshots (overwrite puis 3 append, tous du 4 novembre). Polaris a refusé le commit avant qu'il soit écrit, Spark a annulé l'écriture.

## Exercice 1 : suppression de country

`ALTER TABLE polaris.silver.users DROP COLUMN country` passe. La table n'a toujours qu'un seul snapshot (celui de la création), mais `metadata_log_entries` montre un fichier de métadonnées en plus. C'est une opération de métadonnées : Iceberg écrit juste un nouveau schéma dans un nouveau fichier `.metadata.json`, les fichiers Parquet ne sont pas réécrits (la colonne est simplement ignorée à la lecture). C'est pour ça qu'il n'y a pas de nouveau snapshot.

## Exercice 2 : rôle silver_reader

Avec l'API de management (en root) j'ai créé le catalog role `silver_reader` avec les privilèges `NAMESPACE_LIST`, `TABLE_LIST`, `NAMESPACE_READ_PROPERTIES`, `TABLE_READ_PROPERTIES` et `TABLE_READ_DATA` sur le namespace `silver` seulement, un principal role `silver_analyst` qui reçoit ce catalog role, et un principal `reader_silver`. En `dev` j'ai créé le namespace `gold` et une table `gold.orders_by_product`.

Avec `reader_silver` : `SHOW TABLES IN polaris.silver` et `SELECT count(*) FROM polaris.silver.orders` (2834) marchent. `SHOW TABLES IN polaris.gold` est refusé (`not authorized for op LIST_TABLES`), `SELECT * FROM polaris.gold.orders_by_product` est refusé (`LOAD_TABLE`) et l'`INSERT` dans silver est refusé (`ADD_TABLE_SNAPSHOT`). Le rôle lit bien silver et rien d'autre.

## Nettoyage

`polaris.silver.users` et le namespace `gold` sont supprimés, `silver.orders` reste enregistrée pour le lab Trino. `SHOW TABLES IN polaris.silver` ne montre plus que `orders`.

## Ce que Polaris contrôle et ce qu'il ne contrôle pas

Polaris contrôle les opérations qui passent par son API : lister, charger une table, commiter un snapshot ou un schéma. Il ne contrôle pas l'accès aux fichiers eux mêmes : comme on lit le bucket avec nos clés S3, quelqu'un qui a les clés peut lire les Parquet directement, ou écrire via le catalogue hadoop `lab`, sans que Polaris le voie.
