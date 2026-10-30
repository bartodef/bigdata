# Réponses - Lab Object storage avec S3

**Pourquoi les identifiants sont dans un Secret et pas dans la ConfigMap ?**
Une ConfigMap est lisible en clair par quiconque peut lire les ressources du namespace et s'affiche avec
`kubectl describe`. Un Secret est masqué dans l'affichage, son accès est contrôlé séparément par le RBAC
(`get secrets`) et il peut être chiffré au repos dans etcd. On sépare donc la configuration (endpoint, bucket)
des données sensibles (clés d'accès).

**Les identifiants Onyxia sont temporaires. Que se passe-t-il si le Job tourne demain ? Comment fait une vraie plateforme ?**
Le token de session a expiré : le Job échoue avec `ExpiredToken` / `AccessDenied`. En production, on n'injecte pas
d'identifiants copiés à la main : le pod obtient ses propres identifiants via son ServiceAccount (fédération
d'identité / IRSA, échange d'un token Kubernetes contre des identifiants S3 temporaires renouvelés automatiquement),
ou via un gestionnaire de secrets (Vault, External Secrets Operator) qui fait tourner les clés d'un compte de service
dédié.

**Comment transformer ce Job en ingestion quotidienne ?**
Avec un `CronJob` : on reprend le `template` du Job dans `spec.jobTemplate` et on ajoute
`schedule: "0 2 * * *"` (tous les jours à 2h). On y ajoute `concurrencyPolicy: Forbid` pour éviter deux exécutions
en parallèle, et la source de données doit être lue depuis le système source plutôt qu'une ConfigMap figée.

**La bucket policy bloque-t-elle la suppression ?**
Oui : sur SeaweedFS, le `rm` de `bronze/protected.csv` renvoie `AccessDenied` une fois la policy `Deny s3:DeleteObject`
appliquée, même pour l'utilisateur authentifié propriétaire du bucket. L'ACL de l'objet montre un seul grant
`FULL_CONTROL` pour `admin`.

**Observations**
- ETag d'un upload simple = md5 du fichier ; upload multipart de 200 Mo = ETag suffixé `-25` (25 parts de 8 Mo).
- Versioning supporté : l'objet uploadé avant l'activation a la version `null` ; un `rm` pose un delete marker et
  toutes les versions restent récupérables.
- Le Job de l'énoncé est refusé par le quota du namespace Onyxia (`failed quota: onyxia-quota: must specify
  limits.cpu`) : un `ResourceQuota` qui limite le CPU impose que chaque conteneur déclare `limits.cpu`. Ajout de
  `cpu: 500m` dans les limits du Job.

**Observations complémentaires** (`bash lab-s3-observations.sh`, sortie dans `sortie-s3-observations.txt`)
- Métadonnées système : l'ETag `4b1324359d7e5872c9640c29821963d9` est identique au `md5sum` de `users.csv`. SeaweedFS
  renvoie aussi `ContentEncoding: aws-chunked`, reliquat de l'upload en streaming de l'AWS CLI récent.
- Métadonnées utilisateur : `--metadata` est supporté, `head-object` renvoie
  `{"rows": "50", "source": "dataset_users.py", "version": "1"}`.
- Presigned URL GET : `curl` télécharge le fichier sans identifiants ; une URL de 5 s utilisée après 7 s renvoie
  `HTTP 403`.
- Presigned URL PUT générée avec boto3 : refusée, `curl -T` renvoie `HTTP 403` et `bronze/upload.csv` n'est pas créé.
  Contrairement au GET signé par l'AWS CLI, la signature boto3 n'est pas acceptée par SeaweedFS (différences probables :
  style d'adressage ou en-têtes de checksum ajoutés par les versions récentes de botocore).
- Lifecycle : `put-bucket-lifecycle-configuration` est accepté sans erreur et `get-bucket-lifecycle-configuration`
  relit les 3 règles. L'application effective (expiration après 30 jours) n'est pas vérifiable pendant le lab.
- Consistency : le `GET` qui suit immédiatement le `PUT` réussit (« immediately available ») : lecture après écriture
  forte. Le warning sur `/dev/null` vient seulement de l'impossibilité de modifier sa date, pas de S3.
