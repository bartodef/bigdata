# Réponses - Lab SQL analytics avec DuckDB

Sortie complète : `bash lab-duckdb.sh 2>&1 | tee sortie-duckdb.txt` (exécuté sur Onyxia le 2026-10-30).

## Configuration S3

**Le secret persistant est stocké dans un fichier du home. Quels risques, et pourquoi sont-ils limités sur Onyxia ?**
Le fichier (`~/.duckdb/stored_secrets/`) contient les clés en clair : n'importe quel processus de l'utilisateur peut le
lire, il peut être commité par erreur dans git, copié dans une sauvegarde ou une image, et il reste après la fin du
travail. Sur Onyxia le risque est limité : le service est un pod mono-utilisateur dans un namespace personnel, le volume
du home n'est monté que dans ce pod, et surtout les identifiants sont temporaires (token STS valable ~12 h). On l'a
d'ailleurs constaté : le secret créé au démarrage du service renvoyait `InvalidAccessKeyId` et a dû être recréé
depuis le profil `default` rafraîchi.

**Le secret est visible par tout processus de l'utilisateur. Comment restreindre l'accès S3 d'un Job Kubernetes ?**
Donner au Job son propre ServiceAccount et une identité dédiée (fédération / IRSA, ou rôle STS) avec une policy
minimale : seulement les actions nécessaires (`s3:GetObject`, `s3:PutObject`) sur le seul préfixe utile
(`bronze/*` par exemple). Les identifiants sont temporaires et renouvelés automatiquement ; à défaut, un Secret
Kubernetes dédié, lisible uniquement par ce ServiceAccount via le RBAC, monté seulement dans le conteneur qui en a besoin.

## Couche bronze

**50 utilisateurs ayant tous commandé et 2829 commandes uniques : pourquoi `approx_unique` renvoie 40 et 3167 ?**
`approx_unique` est une estimation par HyperLogLog : il hache les valeurs et déduit la cardinalité du nombre de zéros
en tête des hash, avec peu de registres pour rester rapide et léger en mémoire. L'erreur relative est de l'ordre de
10 à 20 %, dans les deux sens : 40 au lieu de 50 pour `user_uuid` (sous-estimation), 3167 au lieu de 2829 pour `uuid`
(surestimation). Pour une valeur exacte il faut `count(DISTINCT ...)`, plus coûteux.

**Les types sont inférés sur un échantillon. Quel problème avec un gros fichier dont les premières lignes ne sont pas représentatives ?**
Le type choisi peut être faux pour la suite du fichier : une colonne vue comme `BIGINT` contient plus loin un décimal
ou du texte, une date a un autre format, une colonne vide au début devient `VARCHAR`. La lecture échoue alors au milieu
du fichier (erreur de conversion) ou, avec `strict_mode = false` / `ignore_errors`, des lignes sont rejetées ou des
valeurs mises à NULL silencieusement. Solution : `sample_size = -1` (tout lire, plus lent) ou déclarer le schéma avec
`columns = {...}` / `types = {...}`.

## Export Parquet

**Pourquoi la colonne `uuid` est-elle à peine compressée ?** (113 189 → 102 191 octets)
Ce sont des UUID v4 aléatoires, tous distincts : aucune répétition à exploiter, donc pas de dictionnaire utile et
Snappy ne trouve presque aucune séquence commune. Il ne reste que le gain sur l'alphabet hexadécimal et les tirets.
Les stocker en type `UUID` (16 octets binaires) au lieu de chaînes de 36 caractères réduirait la taille de moitié.

**Pourquoi la taille compressée de certaines colonnes est-elle plus grande que la taille non compressée ?**
(`date` 22 667 > 22 661, `product` 1 271 > 1 266)
Ces colonnes sont déjà encodées de façon compacte avant compression (dictionnaire + RLE / bit-packing pour `product`,
timestamps int64 très proches pour `date`). Snappy ne trouve plus rien à gagner et ajoute ses en-têtes de bloc : le
résultat est quelques octets plus gros. Le writer ne revient pas à `UNCOMPRESSED` pour ces pages.

## Partitionnement Hive

**Pourquoi partitionner par `uuid` est une mauvaise idée ?**
Une partition par commande = 2829 préfixes avec chacun un fichier minuscule (« small files problem »). Sur un object
storage chaque fichier coûte au moins une requête HTTP (latence de dizaines de ms, coût par requête), le LIST est
paginé par 1000 clés, et chaque fichier Parquet a son propre footer, souvent plus gros que les données. Aucune requête
analytique ne filtre par `uuid` : le pruning ne sert jamais, on paie seulement le surcoût. À comparer avec le
partitionnement par `product` : 6 fichiers, et la requête sur `cookie` n'en lit qu'un (`Scanning Files: 1/6`, 3 GET).

**Quelle colonne de partition pour des commandes qui arrivent chaque jour ?**
La date de commande, au jour (`date=2026-10-30/`) ou au mois si le volume quotidien est faible pour éviter les petits
fichiers. Chaque ingestion écrit une nouvelle partition sans réécrire les anciennes, on peut retraiter ou supprimer
un jour isolément, et les requêtes filtrent presque toujours sur une période, ce qui élague les autres partitions.

## CSV vs Parquet à grande échelle

Mesures : CSV 29,6 Mo / Parquet 11,5 Mo (252 416 commandes).

| Requête | Données lues | GET | Temps |
| --- | --- | --- | --- |
| CSV, somme par produit | 28,2 MiB | 1 | 1,23 s |
| Parquet, somme par produit | 203,5 KiB | 4 | 0,310 s |
| Parquet, `date >= '2100-01-01'` | 16,0 KiB | 1 | 0,136 s |

**Combien de row groups, et combien sont ignorés par le filtre `date >= '2100-01-01'` ?**
3 row groups (123 573, 124 276 et 4 567 lignes). Leurs `stats_max` sur `date` sont 2034, 2048 et 2048 : aucun ne peut
contenir de date ≥ 2100, les 3 sont ignorés. La requête ne lit que le footer (16 KiB, 1 GET) et aucune page de données.

**Les commandes sont triées par date. Que se passerait-il si elles étaient mélangées ?**
Chaque row group contiendrait des dates de toute la période : ses min/max couvriraient 2020 → 2048 et ne permettraient
plus d'en exclure aucun. Un filtre dans la plage, par exemple `date >= '2040-01-01'`, qui aujourd'hui ne lit qu'un row
group sur 3, devrait les lire tous. (Le filtre ≥ 2100 resterait élagué car il est hors de toute la plage, mais c'est un
cas particulier.) Trier ou clusteriser sur la colonne filtrée est ce qui rend les statistiques efficaces.

**Comparer les temps CSV et Parquet : quelle part est due au réseau, quelle part au parsing du CSV ?**
CSV : 1,23 s contre 0,31 s en Parquet, soit 4 fois plus.
- Réseau : le CSV oblige à télécharger tout le fichier (28,2 MiB) même pour 2 colonnes ; Parquet ne lit que le footer
  puis les colonnes `product` et `quantity`, très compressées (dictionnaire, petits entiers) : 203,5 KiB, 140 fois moins.
- Parsing : chaque octet du CSV doit être découpé en lignes et champs puis converti (texte → entier, texte → timestamp
  pour les 5 colonnes, y compris celles inutiles) ; Parquet lit des valeurs binaires déjà typées, seulement pour les
  colonnes demandées.
Sur Onyxia, avec le S3 distant, le réseau domine : 28 MiB à transférer pèsent plus que le parsing, que DuckDB
parallélise. L'énoncé (S3 sur la même machine, réseau quasi gratuit) n'obtient que 0,28 s contre 0,14 s : c'est
l'écart dû au parsing seul. L'écart grandit ici avec la latence et le débit du réseau.
