# Lab 3 Object storage avec S3

## Pourquoi les identifiants sont dans un Secret et pas dans la ConfigMap ?

Une ConfigMap est lisible en clair par tous ceux qui ont accès au namespace et elle s'affiche avec `kubectl describe`. Un Secret est caché dans l'affichage et on peut limiter qui le lit avec le RBAC. Il peut aussi être chiffré dans etcd. Donc on met la config normale (endpoint, bucket) dans la ConfigMap et les clés dans le Secret.

## Les identifiants Onyxia sont temporaires. Que se passe-t-il si le Job tourne demain ?

Le token a expiré donc le Job échoue avec `ExpiredToken` ou `AccessDenied`. Dans une vraie plateforme on ne copie pas les clés à la main. Le pod récupère ses propres identifiants grâce à son ServiceAccount et ils sont renouvelés tout seuls. On peut aussi utiliser un outil comme Vault qui change les clés régulièrement.

## Comment faire une ingestion tous les jours ?

Avec un CronJob. On met le template du Job dans `spec.jobTemplate` et on ajoute `schedule: "0 2 * * *"` pour tous les jours à 2h. On ajoute `concurrencyPolicy: Forbid` pour ne pas avoir deux Jobs en même temps. Et il faudrait lire les données depuis la vraie source et pas depuis une ConfigMap.

## La bucket policy bloque-t-elle la suppression ?

Oui. Après avoir mis la policy `Deny s3:DeleteObject` le `rm` de `bronze/protected.csv` renvoie `AccessDenied`. Même le propriétaire du bucket ne peut pas supprimer. L'ACL de l'objet montre un seul droit `FULL_CONTROL` pour `admin`.

## Observations

- L'ETag d'un upload simple c'est le md5 du fichier. Pour un upload multipart de 200 Mo l'ETag finit par `-25` (25 morceaux de 8 Mo).
- Le versioning marche. L'objet envoyé avant d'activer le versioning a la version `null`. Un `rm` ajoute un delete marker mais on peut toujours récupérer les anciennes versions.
- Le Job de l'énoncé a été refusé par le quota du namespace (`must specify limits.cpu`). Le quota oblige chaque conteneur à donner une limite de CPU. J'ai ajouté `cpu: 500m` dans les limits du Job.

## Observations en plus

Lancé avec `bash lab-03/lab-s3-observations.sh`, la sortie est dans `lab-03/sortie-s3-observations.txt`.

- L'ETag `4b1324359d7e5872c9640c29821963d9` est le même que le `md5sum` de `users.csv`.
- On peut ajouter des métadonnées avec `--metadata`. `head-object` renvoie `{"rows": "50", "source": "dataset_users.py", "version": "1"}`.
- Une URL presignée en GET permet de télécharger le fichier avec `curl` sans identifiants. Une URL valable 5 s utilisée après 7 s renvoie une erreur 403.
- Une URL presignée en PUT faite avec boto3 est refusée (403) et le fichier n'est pas créé. Je pense que SeaweedFS n'accepte pas la signature des versions récentes de boto3.
- La règle de lifecycle est acceptée et on peut la relire. Mais je n'ai pas pu vérifier que les fichiers sont vraiment supprimés après 30 jours pendant le lab.
- Le GET juste après le PUT marche tout de suite. La lecture après écriture est donc immédiate.
