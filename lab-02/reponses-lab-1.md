# Lab 2.1 Kubernetes (minikube)

J'ai utilisé minikube v1.39.0 avec le pilote docker et Kubernetes v1.37.0. Il y a un seul nœud qui s'appelle minikube.

## 1. minikube

`kubectl top pods -A --sort-by cpu --sum=true` affiche le CPU et la mémoire de tous les pods de tous les namespaces. Les pods sont triés par CPU et il y a une ligne de total à la fin. Chez moi le total fait 345m de CPU et 402 Mi de mémoire. kube-apiserver est le pod qui consomme le plus (130m).

Les pods dans kube-system :

- kube-apiserver : c'est l'API qui reçoit toutes les commandes kubectl
- etcd : la base qui garde l'état du cluster
- kube-scheduler : il choisit sur quel nœud va chaque pod
- kube-controller-manager : il gère les boucles de contrôle (replicas, jobs...)
- kube-proxy : il gère le réseau des Services
- coredns : le DNS interne du cluster
- kindnet : le plugin réseau
- storage-provisioner : les volumes de minikube
- metrics-server : il donne les chiffres de `kubectl top`

## 2. kubectl

Le conteneur tourne sous Debian 8 (jessie).

Le code est dans `/server.js`. C'est un petit serveur Node.js qui écoute sur le port 8080.

Avec `curl localhost:8080` dans le pod j'obtiens `Hello Kubernetes bootcamp! | Running on: <pod> | v=1`.

Depuis ma machine l'appli n'est pas accessible. L'IP du pod (10.244.x.x) existe seulement dans le réseau du cluster donc il faut un Service.

## 3. Service

`kubectl expose deployments/kubernetes-bootcamp --type="NodePort" --port 8080` donne le port `8080:31463/TCP`.

`minikube ip` donne 192.168.49.2. Sous Windows avec docker cette IP ne marche pas depuis ma machine. J'ai utilisé `minikube service kubernetes-bootcamp` qui fait un tunnel et ouvre le navigateur.

## 4. Scale

Avec `kubectl scale deployments/kubernetes-bootcamp --replicas=5` j'ai bien 5 pods Running.

Quand je fais des `curl` en boucle le nom du pod change à chaque fois. Le Service répartit les requêtes entre les pods. Dans le navigateur je tombe souvent sur le même pod car il garde la même connexion ouverte.

Quand je repasse à 2 replicas les 3 pods en trop passent en Terminating puis disparaissent.

## 5. Mise à jour

v2 : les pods sont remplacés un par un (rolling update). Pendant la mise à jour la page montre parfois v=1 et parfois v=2. Il n'y a pas de coupure.

v3 : le nouveau pod reste en `ErrImagePull` puis `ImagePullBackOff` parce que l'image v3 n'existe pas. Les anciens pods v2 continuent de tourner donc l'appli marche toujours.

`kubectl rollout undo` revient à la v2. Pour revenir à la v1 j'ai fait `kubectl set image deployments/kubernetes-bootcamp kubernetes-bootcamp=gcr.io/google-samples/kubernetes-bootcamp:v1`.

## 6. Manifests

J'ai complété `lab-1/deployment.yaml` (image, port 8080, 3 replicas, label `app: kubernetes-bootcamp`) et `lab-1/service.yaml` (NodePort sur le port 8080).

Les 3 pods tournent et le Service répartit bien les requêtes entre eux.
