# Lab 5 Bronze vers Silver avec dbt et DuckDB

Le projet est dans `lab-05/lab_medallion/`. J'ai tout lancé avec `bash lab-05/lab-dbt.sh 2>&1 | tee lab-05/sortie-dbt.txt` sur Onyxia (dbt 1.12.6 et dbt-duckdb 1.11.1).

Le CLI duckdb d'Onyxia (1.5.5) est plus vieux que la librairie installée avec dbt et n'arrive pas à ouvrir `lab_medallion.duckdb`. Dans le script j'ai donc fait les requêtes avec le DuckDB Python du projet.

## Couche bronze

**On ne touche pas aux CSV bronze. À quoi ça sert si on trouve un bug dans une transformation 6 mois plus tard ?**

Les données brutes sont toujours là comme elles sont arrivées. On corrige le modèle SQL et on relance `dbt build`. Toute la couche silver est recalculée correctement depuis le début. Si on avait modifié les CSV directement on ne pourrait plus revenir en arrière.

**La source relit tout le fichier à chaque requête. Quel format si les commandes arrivent toutes les heures pendant des années ?**

Je prendrais du Parquet partitionné par date (par exemple `date=2026-11-04/`). Chaque heure on ajoute un fichier sans réécrire les autres. Les requêtes lisent seulement les dates dont elles ont besoin et seulement les colonnes utiles. On pourrait aussi utiliser un format de table comme Iceberg.

## Couche silver

**Les modèles silver sont des vues. Que se passe-t-il quand on interroge une vue et quand c'est mieux qu'une table ?**

Une vue garde juste la requête SQL. À chaque fois qu'on l'interroge DuckDB relit les CSV sur S3 et refait tout le calcul. C'est mieux qu'une table quand les données sont petites ou changent souvent car on a toujours le résultat à jour et ça ne prend pas de place. Quand on interroge souvent ou que les données sont grosses une table est mieux.

**Un UUID prend 16 octets. Combien en VARCHAR ?**

36 octets (32 caractères hexa et 4 tirets). J'ai vérifié avec `octet_length(cast(order_id AS varchar))` qui donne 36. C'est plus de 2 fois plus gros.

## Tests

Avec `dbt test --select silver` le test `assert_orders_after_user_birth` trouvait 288 commandes passées avant la naissance du client. Les dates de naissance de Faker sont calculées par rapport à aujourd'hui alors que les commandes commencent en 2020. Certains clients sont donc "nés" après leur première commande.

Avec `--store-failures` les lignes en erreur sont enregistrées dans le schéma `main_dbt_test__audit` et on peut les regarder avec DuckDB.

**Quelle stratégie pour un dashboard sur l'âge des clients ? Et pour les ventes par région ?**

Pour l'âge il faut que le test bloque (error) ou mettre les dates fausses à NULL. Sinon le dashboard affiche des âges faux. C'est ce que j'ai fait dans l'exercice 3.

Pour les ventes par région la date de naissance ne sert à rien. Un simple warning suffit et on ne bloque pas le pipeline pour ça.

**Les adresses ont des codes postaux qui ne correspondent pas à l'état (par exemple `KY 01352`). Quel test le verrait ?**

Un test qui vérifie que les 3 premiers chiffres du code postal sont dans les plages autorisées pour l'état. Il faut une table de référence avec les préfixes de code postal par état (publiée par USPS). Je l'ai fait dans l'exercice 4.

## Pipeline complet

`dbt build --select silver` fait les modèles et les tests dans le bon ordre : `PASS=27 WARN=1 ERROR=0`. Le seul warning c'est le test des codes postaux.

Lignage avec `dbt ls` :

- `stg_orders` dépend de `source:bronze.orders`
- `stg_users` dépend de `source:bronze.users` et de `stg_orders` (à cause de l'exercice 3)

`dbt docs generate --static` crée un seul fichier `target/static_index.html` (2,5 Mo) avec la doc et le graphe.

## Export Parquet

J'ai exporté les deux modèles dans `silver/dbt/` : `users.parquet` (7 956 octets) et `orders.parquet` (72 532 octets).

En lisant le Parquet directement on trouve 30 F et 20 M.

Exercice : j'ai comparé la vue `stg_orders` et le fichier `orders.parquet` avec `EXCEPT ALL` dans les deux sens. Les deux ont 2829 lignes et 0 ligne de différence.

## Exercices

### 1. Colonne nettoyée

J'ai ajouté `username_normalized = lower(trim(username))` dans `stg_users`. Elle est documentée dans `silver.yml` avec les tests `unique` et `not_null`.

### 2. Tests des commandes

- `product` : test `accepted_values` avec les 6 produits
- `user_id` : test `relationships` vers `stg_users`
- `quantity` positive : il n'y a pas de test générique pour "plus grand que 0" sans le package dbt_utils. J'ai donc fait un test singulier `assert_orders_quantity_positive.sql`. J'ai aussi mis `accepted_values` 1 à 5 sur la quantité.

### 3. Dates de naissance invalides

Dans `stg_users` je calcule la première commande de chaque client. Si la date de naissance est après cette commande je la remplace par NULL et `birthdate_is_valid` vaut false.

Résultat : 45 clients valides et 5 invalides. Le test `assert_orders_after_user_birth` passe maintenant (avant il trouvait 288 lignes).

### 4. Codes postaux invalides

J'ai mis deux seeds : `states.csv` (les codes des états) et `zip_ranges.csv` (les préfixes de code postal autorisés par état). Le test `assert_users_zip_matches_state` trouve 49 utilisateurs sur 50 avec un code postal qui ne correspond pas. C'est normal car Faker tire le code postal au hasard. Je l'ai mis en warning pour ne pas bloquer le pipeline.

- D'où viennent les données de référence ? De la source officielle (USPS).
- En seed ? Oui car c'est petit et ça change rarement. C'est versionné dans git avec le projet.
- Valider dans la silver ? Oui car c'est là qu'on nettoie et qu'on vérifie la qualité.
- Si la référence change ? On met à jour le CSV et on relance `dbt seed` puis `dbt build`. Le résultat du test peut changer.

### 5. Vue ou table

| | Vue | Table |
| --- | --- | --- |
| Type dans DuckDB | VIEW | BASE TABLE |
| Taille de `lab_medallion.duckdb` | 780 Ko | 1,3 Mo |
| `count(*)` sur `stg_orders` | 0,377 s | 0,008 s |

- Où sont les données ? Avec une vue elles restent dans les CSV sur S3. Avec une table elles sont copiées dans le fichier DuckDB.
- Quand le calcul est fait ? Pour une vue à chaque requête. Pour une table une seule fois pendant `dbt build`.
- Si le CSV change ? La vue voit tout de suite les changements. La table garde l'ancien résultat jusqu'au prochain `dbt build`.
- Qui prend plus de place ? La table.
- Qui évite de relire les fichiers ? La table, c'est pour ça qu'elle est presque 50 fois plus rapide ici.

J'ai remis `+materialized: view` à la fin.
