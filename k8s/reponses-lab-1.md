# Réponses - Lab 1 Kubernetes (minikube)

Cluster : minikube v1.39.0, pilote docker (Docker Desktop), Kubernetes v1.37.0, 1 nœud `minikube` (12 CPU, 8 Go).

## 1. minikube

- `kubectl top pods -A --sort-by cpu --sum=true` : consommation CPU/mémoire de tous les pods de tous les namespaces
  (`-A`), triés par CPU décroissant, avec une ligne de total en bas (`--sum`). Les chiffres viennent de metrics-server.
  Ici : 345m CPU et 402 Mi au total, kube-apiserver en tête (130m).
- `kube-system` : `kube-apiserver` (API, point d'entrée de toutes les commandes), `etcd` (base clé-valeur qui stocke
  l'état du cluster), `kube-scheduler` (choisit le nœud de chaque pod), `kube-controller-manager` (boucles de contrôle :
  replicas, nodes, jobs...), `kube-proxy` (règles réseau des Services sur chaque nœud), `coredns` (DNS interne),
  `kindnet` (plugin réseau CNI), `storage-provisioner` (volumes minikube), `metrics-server` (métriques de `top`).

## 2. kubectl

- Le conteneur tourne sous Debian GNU/Linux 8 (jessie).
- Le code est dans `/server.js` : serveur HTTP Node.js qui écoute sur le port **8080**.
- `curl localhost:8080` dans le pod : `Hello Kubernetes bootcamp! | Running on: <pod> | v=1`.
- Depuis la machine locale, l'appli n'est pas joignable : l'IP du pod (10.244.x.x) n'existe que dans le réseau du
  cluster, il faut un Service pour l'exposer.

## 3. Service

- `kubectl expose deployments/kubernetes-bootcamp --type="NodePort" --port 8080` → port `8080:31463/TCP`.
- `minikube ip` = 192.168.49.2. Avec le pilote docker sous Windows cette IP n'est pas routable depuis l'hôte :
  `minikube service kubernetes-bootcamp` crée un tunnel vers localhost et ouvre le navigateur.

## 4. Scale

- `kubectl scale deployments/kubernetes-bootcamp --replicas=5`, vérifié avec `kubectl get pods` (5 pods Running).
- En rafraîchissant, le nom du pod change : le Service répartit les requêtes entre les replicas (kube-proxy).
  Avec `curl` en boucle, chaque requête tombe sur un pod différent. Dans le navigateur, on reste souvent sur le
  même pod : le navigateur réutilise la même connexion TCP (keep-alive) et la répartition se fait par connexion,
  pas par requête.
- Scale à 2 : les 3 pods en trop passent en `Terminating` puis disparaissent.

## 5. Mise à jour

- v2 : rolling update, les pods sont remplacés un par un. Pendant la mise à jour la page alterne entre `v=1` et
  `v=2` sans coupure, puis n'affiche plus que `v=2`.
- v3 : le nouveau pod reste en `ErrImagePull` / `ImagePullBackOff` car l'image `jocatalin/kubernetes-bootcamp:v3`
  n'existe pas. Les anciens pods v2 continuent de tourner : le rolling update ne supprime les anciens pods que
  quand les nouveaux sont prêts, donc pas d'interruption.
- `kubectl rollout undo` revient à la v2. Retour à la v1 avec
  `kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=gcr.io/google-samples/kubernetes-bootcamp:v1`.

## 6. Manifests

- `lab-1/deployment.yaml` et `lab-1/service.yaml` complétés (image + port 8080, `replicas: 3`, selector
  `app: kubernetes-bootcamp`, port 8080).
- Les 3 pods tournent et le Service `kubernetes-bootcamp-service` (NodePort) répartit bien les requêtes entre eux.
