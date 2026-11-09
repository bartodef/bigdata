# Lab 2.2 Kubernetes : ConfigMap et Secret

Les fichiers sont dans `lab-2/`. Pour les identifiants j'ai pris les clés d'exemple de la doc AWS.

## ConfigMap ou Secret

Une ConfigMap sert pour la config pas sensible (chemins, niveau de log, fichiers de config). Elle est en clair.

Un Secret sert pour les mots de passe et les clés. Mais les valeurs sont juste en base64 et pas chiffrées. Avec `kubectl get secret -o jsonpath='{.data.secret-key}' | base64 -d` on relit la valeur en clair. La vraie protection c'est le RBAC (qui a le droit de lire les secrets) et le chiffrement de etcd s'il est activé.

## Création

Il y a 3 façons de créer une ConfigMap :

- `--from-file` (le nom du fichier devient la clé)
- `--from-literal` (cle=valeur)
- un fichier YAML (pratique pour le mettre dans git)

Pour lire une seule clé : `-o jsonpath='{.data.log_level}'` donne `INFO`.

Pour les Secrets c'est pareil : `--from-literal`, `--from-file` ou un YAML avec `stringData`.

J'ai remarqué que quand on fait `kubectl apply` d'un Secret avec `stringData` les valeurs sont copiées en clair dans l'annotation `last-applied-configuration`. Il ne faut donc pas mettre ce fichier dans git. Il vaut mieux utiliser Sealed Secrets ou Vault.

## Utilisation dans les pods

En variables d'environnement :

- `configMapKeyRef` ou `secretKeyRef` pour une seule clé (j'ai `LOG_LEVEL=INFO` et `SPARK_WORKERS=4`)
- `envFrom` pour prendre toutes les clés d'un coup

Le pod `ceph-connector` affiche `Access Key: AKIAIOSFODNN7EXAMPLE` dans ses logs. Donc un secret en variable d'environnement peut finir dans les logs.

`ceph-connector` redémarre plusieurs fois. C'est un Pod avec `restartPolicy: Always` par défaut et sa commande se termine alors Kubernetes le relance. Pour une tâche qui doit juste tourner une fois il faut un Job.

En volume :

- la ConfigMap montée dans `/etc/config` donne un fichier par clé (`application.yaml` et `tables.csv` renommé avec `items.path`)
- si on modifie la ConfigMap le fichier est mis à jour dans le pod sans redémarrer (ce n'est pas le cas avec les variables d'environnement)
- le Secret monté dans `/etc/ceph-credentials` est en lecture seule et stocké en mémoire (tmpfs)
- avec `defaultMode: 0600` les droits sont `-rw-------`

Pour un secret c'est mieux de le monter en volume plutôt qu'en variable d'environnement.

## Bonnes pratiques

- ne jamais mettre un secret en clair dans git
- chiffrer etcd et limiter qui peut lire les secrets avec le RBAC
- monter les secrets en volume en lecture seule
- ne pas afficher les secrets dans les logs

Remarque : au début les images `python:3.9` et `apache/spark` étaient en `ImagePullBackOff` à cause d'une coupure réseau. Ça a marché une fois les images téléchargées.
