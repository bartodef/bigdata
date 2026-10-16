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
