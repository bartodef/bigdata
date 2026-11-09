#!/usr/bin/env bash
cd "$(dirname "$0")/.."  # les chemins sont relatifs a la racine du projet
# Lab Polaris, etape 1 : ressources du service vscode-pyspark (2000m / 2500Mi) comme dans l'enonce
# Le service redemarre : il faut ensuite remettre les cles S3 dans ~/.aws
STATEFULSET_NAME="$(kubectl get statefulsets.apps -o=jsonpath='{.items[0].metadata.name}' -l app.kubernetes.io/name=vscode-pyspark)"
kubectl patch statefulset $STATEFULSET_NAME \
  -n $KUBERNETES_NAMESPACE \
  --type='strategic' \
  -p='{
    "spec": {
      "template": {
        "spec": {
          "containers": [{
            "name": "vscode-pyspark",
            "resources": {
              "requests": {
                "cpu": "50m",
                "memory": "200Mi"
              },
              "limits": {
                "cpu": "2000m"
                ,"memory": "2500Mi"
              }
            }
          }]
        }
      }
    }
  }'
kubectl rollout status statefulset/$STATEFULSET_NAME -n $KUBERNETES_NAMESPACE
