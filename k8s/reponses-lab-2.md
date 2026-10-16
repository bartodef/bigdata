# Réponses - Lab 2 Kubernetes : ConfigMap et Secret

Manifests et fichiers utilisés : `lab-2/`. Les identifiants sont les clés d'exemple de la documentation AWS.

## ConfigMap vs Secret

- ConfigMap : configuration non sensible (chemins, niveau de log, fichiers de config), stockée en clair.
- Secret : données sensibles (clés, mots de passe). Les valeurs sont seulement encodées en base64, pas chiffrées :
  `kubectl get secret -o jsonpath='{.data.secret-key}' | base64 -d` les relit en clair. La protection vient du RBAC
  (droit `get secrets` séparé) et du chiffrement au repos d'etcd s'il est activé.

## Création

- 3 méthodes pour les ConfigMaps : `--from-file` (une clé = le nom du fichier), `--from-literal` (clé=valeur),
  manifest YAML (versionnable, méthode GitOps). Lecture d'une clé avec `-o jsonpath='{.data.log_level}'` → `INFO`.
- 3 méthodes pour les Secrets : `--from-literal`, `--from-file`, manifest avec `stringData` (encodé en base64 à
  l'application).
- Observation : avec `kubectl apply` d'un Secret en `stringData`, les valeurs sont recopiées **en clair** dans
  l'annotation `kubectl.kubernetes.io/last-applied-configuration`. Ne jamais versionner ce manifest tel quel :
  utiliser Sealed Secrets, External Secrets Operator ou Vault.

## Utilisation dans les pods

- Variables d'environnement : `valueFrom.configMapKeyRef` / `secretKeyRef` pour une clé (`LOG_LEVEL=INFO`,
  `SPARK_WORKERS=4`), `envFrom` pour toutes les clés (`log_level`, `spark_workers`, `batch_size`...).
  Le pod `ceph-connector` affiche `Access Key: AKIAIOSFODNN7EXAMPLE` dans ses logs : un secret en variable
  d'environnement peut fuiter dans les logs.
- `ceph-connector` redémarre plusieurs fois : c'est un Pod (restartPolicy `Always` par défaut) dont la commande se
  termine, Kubernetes le relance. Pour une tâche ponctuelle, il faut un Job.
- Volumes : la ConfigMap montée dans `/etc/config` donne un fichier par clé sélectionnée (`application.yaml`,
  `tables.csv` renommé via `items.path`), sous forme de liens vers `..data` : la mise à jour de la ConfigMap est
  propagée dans le pod sans redémarrage (contrairement aux variables d'environnement).
- Le Secret monté dans `/etc/ceph-credentials` est en lecture seule, sur un tmpfs (en mémoire, pas sur disque), avec
  les droits `-rw-------` grâce à `defaultMode: 0600`. C'est la méthode à préférer aux variables d'environnement.

## Bonnes pratiques

- Ne jamais committer de secret en clair, chiffrer etcd au repos, limiter `get secrets` par RBAC.
- Préférer le montage en volume en lecture seule avec des droits restreints.
- Ne pas afficher les secrets dans les logs.

Remarque : le téléchargement des images `python:3.9` et `apache/spark` a d'abord échoué (`ImagePullBackOff`) à
cause d'une coupure réseau ; les pods ont démarré une fois les images récupérées.
