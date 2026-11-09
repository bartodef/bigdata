# Lab 8 Explorer les tables Iceberg avec Spark

Le code est dans `lab-08/lab_iceberg.py`, lancé sur Onyxia avec `python3 -u lab-08/lab_iceberg.py > lab-08/sortie-iceberg.txt`. Catalogue Hadoop `lab` dans `s3a://<bucket>/warehouse`.

Les tables `lab.silver.orders` et `lab.silver.users` viennent du lab 7. Je ne les ai pas recréées avec `createOrReplace()` sinon je perdais l'historique. `orders` avait déjà 2 snapshots (création + l'append de l'exercice 4 du lab 7) donc 2830 lignes et pas 5000 comme dans l'énoncé.

## Créer une table

`SHOW TABLES IN lab.silver` montre `orders` et `users`. La requête donne 2830 commandes et une quantité de 8617. On utilise le nom de la table et pas un chemin de fichier comme avec Parquet.

## Les fichiers

**Quels fichiers contiennent les lignes ?**

Les fichiers Parquet dans `data/` : `00000-77-...parquet` (84 Ko, 2829 lignes, écrit à la création) et `00000-160-...parquet` (1,9 Ko, 1 ligne, écrit par l'append).

**Quels fichiers décrivent la table ?**

Ceux dans `metadata/` :

- `v1.metadata.json`, `v2.metadata.json` : un par commit avec le schéma, les partitions, la liste des snapshots et le snapshot actuel
- `version-hint.text` : le numéro de la version actuelle (utilisé par le catalogue Hadoop)
- `snap-....avro` : le manifest list, un par snapshot, qui liste les manifests
- `...-m0.avro` : les manifests qui listent les fichiers de données avec le nombre de lignes et les min/max des colonnes

**Pourquoi Iceberg a besoin de plus que les fichiers Parquet ?**

Des fichiers Parquet dans un dossier ne disent pas lesquels font partie de la table ni quelle version est la bonne. Les métadonnées donnent la liste exacte des fichiers de chaque version. Ça permet les commits atomiques, l'historique et le time travel, l'évolution du schéma, et de ne pas lister tout le dossier S3 pour trouver les fichiers (on peut même ignorer des fichiers grâce aux min/max).

## Métadonnées

| snapshot_id | parent | opération | lignes ajoutées | total |
| --- | --- | --- | --- | --- |
| 964221037480772361 | aucun | overwrite | 2829 | 2829 |
| 4316733754762097419 | 964221037480772361 | append | 1 | 2830 |

**Combien de snapshots ?** 2.

**Combien de fichiers de données dans la table ?** 2 (2829 lignes et 1 ligne).

**Qu'est-ce que la table `snapshots` donne de plus que les fichiers Parquet ?**

L'historique de la table : l'id de chaque version, son parent, la date du commit, le type d'opération (overwrite, append...) et un résumé (lignes et fichiers ajoutés, total). Les fichiers Parquet ne savent rien de tout ça.

## Deuxième snapshot

J'ai ajouté 3 commandes avec de nouveaux `order_id`.

**Combien de snapshots maintenant ?** 3. La table a 2833 lignes.

**Les anciens fichiers Parquet ont disparu ?** Non. Il y a maintenant 3 fichiers dans `data/` : les 2 anciens et un nouveau de 3 lignes. Iceberg a juste ajouté un fichier et de nouvelles métadonnées (`v3.metadata.json`, un nouveau manifest et un nouveau `snap-....avro`).

**Pourquoi un nouveau snapshot au lieu de remplacer la table ?**

Les fichiers ne sont jamais modifiés. Chaque écriture crée une nouvelle version qui pointe vers les anciens fichiers + les nouveaux. Le changement de version est atomique donc les lecteurs ne voient jamais une table à moitié écrite. Et on garde l'historique pour revenir en arrière.

## Time travel

Avec `VERSION AS OF` sur le premier snapshot j'ai 2829 lignes. La table actuelle en a 2833.

**Que lit le time travel ?**

Il lit les métadonnées de l'ancien snapshot donc seulement les fichiers de données qui faisaient partie de la table à ce moment (ici le fichier de 2829 lignes).

**Pourquoi Iceberg peut lire une ancienne version sans copier la table ?**

Parce que les fichiers ne sont jamais réécrits. Chaque snapshot est juste une liste de fichiers. Les nouveaux snapshots réutilisent les mêmes fichiers et en ajoutent. Il n'y a rien à copier.

**Et si on supprime les fichiers de l'ancien snapshot ?**

Le time travel vers ce snapshot ne marche plus (erreur fichier introuvable). C'est ce que fait `expire_snapshots` : il supprime les vieux snapshots et les fichiers qui ne servent plus à aucun snapshot gardé. Si on supprime à la main un fichier encore utilisé par la version actuelle la table est cassée.

## Évolution du schéma

J'ai ajouté la colonne `source STRING` puis inséré 1 ligne avec `source = 'spark'`. Résultat : 1 ligne `spark` et 2833 lignes à NULL.

**Est-ce que les anciens Parquet ont été réécrits ?** Non. Ils sont toujours là avec les mêmes tailles. Le nouveau fichier de 1 ligne est le seul à avoir la colonne `source`. Pour les anciens fichiers Iceberg renvoie NULL.

**Où est enregistré le nouveau schéma ?** Dans un nouveau fichier de métadonnées. Le `ALTER TABLE` a créé `v4.metadata.json` sans nouveau snapshot puis l'`INSERT` a créé `v5.metadata.json`. Les colonnes ont un id dans les métadonnées, c'est comme ça qu'Iceberg fait le lien avec les vieux fichiers.

**Pourquoi c'est une opération sur la table et pas sur un fichier Parquet ?**

Une table c'est plein de fichiers écrits à des moments différents. On ne peut pas tous les modifier. Iceberg garde le schéma dans les métadonnées de la table et fait correspondre les colonnes par id. Comme ça les vieux fichiers restent valides et tous les lecteurs voient le même schéma.

## Iceberg ou Parquet

**Lequel est juste un ensemble de fichiers Parquet ?** `silver/spark/orders.parquet/` (un `part-00000...parquet` et un `_SUCCESS`).

**Lequel a des métadonnées et des snapshots ?** `warehouse/silver/orders/`.

**Comment remplacer un dossier Parquet sans gêner une requête qui le lit ?**

Écrire la nouvelle version dans un autre dossier (par exemple avec un numéro de version) puis faire pointer les lecteurs vers ce nouveau dossier (une vue ou une table dans un metastore). On supprime l'ancien dossier seulement quand plus personne ne le lit.

**Comment Iceberg rend l'état visible de façon atomique ?**

Il écrit d'abord les nouveaux fichiers de données et de métadonnées. Rien n'est visible tant que le pointeur vers la version actuelle n'a pas changé. Le commit c'est juste ce changement de pointeur, en une seule opération.

**Pourquoi il faut un catalogue quand plusieurs moteurs utilisent la même table ?**

Il faut que tout le monde soit d'accord sur la version actuelle et que deux commits en même temps ne s'écrasent pas. Avec le catalogue Hadoop c'est juste un fichier dans le bucket, sans verrou. Un catalogue (Polaris, Nessie, Glue...) fait le commit de façon atomique, donne les noms des tables à tous les moteurs (Spark, Trino, DuckDB...) et gère les droits.

## Exercices

### 1. Table users

`lab.silver.users` existait déjà depuis le lab 7 avec 50 lignes et 1 snapshot.

### 2. Nouveau batch de users

J'ai ajouté 2 users (copiés avec un nouvel `user_id` et `_new` à la fin du username). Il y a maintenant 2 snapshots : overwrite (50) puis append (2 ajoutés, total 52).

### 3. Time travel

Avant l'append : 50 users. Après : 52.

### 4. Colonne country

`ALTER TABLE lab.silver.users ADD COLUMN country STRING` puis j'ai inséré un user avec `country = 'FR'`. Résultat : 1 ligne FR et les 52 autres à NULL. Le `DESCRIBE` montre la colonne `country` à la fin.

### 5. Layout S3 de users et orders

C'est la même organisation : `data/` avec les Parquet et `metadata/` avec les `vN.metadata.json`, les manifests, les manifest lists et `version-hint.text`. Seul le nombre de fichiers change selon le nombre de commits : orders a 5 versions de métadonnées et 4 fichiers de données, users a 4 versions (`v1` à `v4`), 3 snapshots et 3 fichiers de données. Les fichiers de users sont plus petits (7 Ko pour les 50 users).

### 6. Parquet, Iceberg et catalogue Iceberg

- Parquet : un format de fichier en colonnes. Un fichier ne sait pas qu'il fait partie d'une table.
- Iceberg : un format de table. Il ajoute des métadonnées au-dessus des fichiers Parquet pour avoir une vraie table avec un schéma, des snapshots, des commits atomiques et le time travel.
- Catalogue Iceberg : le service qui connaît le nom des tables et le pointeur vers leur version actuelle. Il fait les commits de façon sûre quand plusieurs jobs ou moteurs écrivent, et il gère les droits.

## Nettoyage

L'énoncé demande de supprimer le warehouse à la fin. Je l'ai gardé pour l'instant car le lab Polaris enregistre la table silver qui est dedans.
