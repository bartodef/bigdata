# Lab 4 SQL analytics avec DuckDB

Toute la sortie est dans `lab-04/sortie-duckdb.txt` (lancé avec `bash lab-04/lab-duckdb.sh 2>&1 | tee lab-04/sortie-duckdb.txt` sur Onyxia le 30/10).

## Configuration S3

**Le secret est stocké dans un fichier du home. Quels risques ?**

Le fichier `~/.duckdb/stored_secrets/` contient les clés en clair. N'importe quel programme lancé par mon utilisateur peut le lire. On peut aussi le commit dans git par erreur et il reste là après le lab.

Sur Onyxia c'est moins grave. Le service est un pod juste pour moi dans mon namespace et les clés sont temporaires (environ 12h). D'ailleurs le secret créé au début ne marchait plus (`InvalidAccessKeyId`) et j'ai dû le recréer.

**Comment limiter l'accès S3 d'un Job Kubernetes ?**

Il faut donner au Job son propre ServiceAccount avec des droits minimum. Par exemple seulement lire et écrire dans `bronze/`. Si on utilise un Secret Kubernetes il faut que seul ce Job puisse le lire et le monter seulement dans son conteneur.

## Couche bronze

**Pourquoi `approx_unique` donne 40 et 3167 au lieu de 50 et 2829 ?**

`approx_unique` fait une estimation (algorithme HyperLogLog) pour aller vite et utiliser peu de mémoire. L'erreur peut aller de 10 à 20 % dans les deux sens. Ici 40 au lieu de 50 (trop bas) et 3167 au lieu de 2829 (trop haut). Pour avoir le bon nombre il faut `count(DISTINCT ...)` qui est plus lent.

**Les types sont devinés sur un échantillon. Quel problème avec un gros fichier ?**

Si le début du fichier n'est pas représentatif le type peut être faux. Par exemple une colonne vue comme un entier contient plus loin un nombre à virgule ou du texte. La lecture plante au milieu du fichier. Ou alors avec `ignore_errors` des lignes sont perdues sans qu'on le voie. Il faut soit lire tout le fichier (`sample_size = -1`) soit donner les types à la main.

## Export Parquet

**Pourquoi la colonne `uuid` est presque pas compressée ?** (113 189 vers 102 191 octets)

Les UUID sont aléatoires et tous différents. Il n'y a pas de répétition donc la compression ne gagne presque rien. Si on les stockait en type `UUID` (16 octets) au lieu de texte (36 caractères) ce serait 2 fois plus petit.

**Pourquoi certaines colonnes sont plus grosses après compression ?** (`date` 22 667 contre 22 661 et `product` 1 271 contre 1 266)

Ces colonnes sont déjà très compactes grâce à l'encodage (dictionnaire pour `product` et dates très proches pour `date`). La compression ne trouve plus rien à gagner et ajoute juste ses en-têtes. Le résultat fait quelques octets de plus.

## Partitionnement

**Pourquoi partitionner par `uuid` est une mauvaise idée ?**

Ça ferait 2829 dossiers avec chacun un tout petit fichier. Sur S3 chaque fichier demande au moins une requête donc c'est lent. Et chaque fichier Parquet a son en-tête qui est parfois plus gros que les données. En plus personne ne filtre par `uuid` donc le partitionnement ne sert à rien. Par `product` il y a 6 fichiers et la requête sur `cookie` en lit un seul (`Scanning Files: 1/6`).

**Quelle colonne de partition pour des commandes qui arrivent chaque jour ?**

La date de commande (par jour ou par mois s'il y a peu de données par jour). Chaque jour on écrit une nouvelle partition sans toucher aux anciennes. Et les requêtes filtrent souvent sur une période donc on lit moins de fichiers.

## CSV ou Parquet sur un gros fichier

Taille : CSV 29,6 Mo et Parquet 11,5 Mo (252 416 commandes).

| Requête | Données lues | GET | Temps |
| --- | --- | --- | --- |
| CSV somme par produit | 28,2 MiB | 1 | 1,23 s |
| Parquet somme par produit | 203,5 KiB | 4 | 0,310 s |
| Parquet `date >= '2100-01-01'` | 16,0 KiB | 1 | 0,136 s |

**Combien de row groups et combien sont ignorés par le filtre sur 2100 ?**

Il y a 3 row groups (123 573, 124 276 et 4 567 lignes). Leurs dates max sont 2034, 2048 et 2048. Aucun ne peut avoir une date après 2100 donc les 3 sont ignorés. DuckDB lit seulement le footer (16 KiB).

**Que se passe-t-il si les commandes ne sont pas triées par date ?**

Chaque row group aurait des dates de toute la période. Les min et max ne permettraient plus d'ignorer un row group. Par exemple `date >= '2040-01-01'` lit aujourd'hui 1 row group sur 3 mais il devrait tous les lire. Trier les données sur la colonne qu'on filtre rend les statistiques utiles.

**Quelle part du temps vient du réseau et quelle part du parsing du CSV ?**

Le CSV prend 1,23 s et le Parquet 0,31 s donc 4 fois plus.

- Réseau : pour le CSV il faut télécharger tout le fichier (28,2 MiB) même pour 2 colonnes. Le Parquet lit seulement les 2 colonnes utiles (203,5 KiB) soit 140 fois moins.
- Parsing : le CSV doit être découpé et converti en texte vers nombre ou date pour toutes les colonnes. Le Parquet est déjà en binaire avec les bons types.

Sur Onyxia le S3 est à distance donc c'est surtout le réseau qui compte. Dans l'énoncé le S3 est sur la même machine et l'écart est plus petit (0,28 s contre 0,14 s). Cet écart là vient surtout du parsing.
