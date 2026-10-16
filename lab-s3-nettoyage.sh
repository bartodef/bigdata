#!/usr/bin/env bash
# Nettoyage de fin de lab S3 : versions, delete markers, versioning, lifecycle, fichiers locaux
set -e
export AWS_PAGER=""
LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"

for prefix in bronze/ large/; do
  aws s3api --profile 'default' list-object-versions --bucket "$LAB_BUCKET_NAME" --prefix "$prefix" --output json \
  | jq -c '{Objects: ([.Versions[]?, .DeleteMarkers[]?] | map({Key, VersionId})), Quiet: true}' \
  > delete.json
  if [ "$(jq '.Objects | length' delete.json)" -gt 0 ]; then
    aws s3api --profile 'default' delete-objects --bucket "$LAB_BUCKET_NAME" --delete file://delete.json
  fi
done
# Le versioning ne peut pas être désactivé, seulement suspendu
aws s3api --profile 'default' put-bucket-versioning \
  --bucket "$LAB_BUCKET_NAME" --versioning-configuration Status=Suspended
aws s3api --profile 'default' delete-bucket-lifecycle --bucket "$LAB_BUCKET_NAME"
rm -f large_dataset.bin users_downloaded.csv users_v1.csv users_v2.csv delete.json lifecycle.json policy.json
echo "Contenu du bucket après nettoyage :"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/" --recursive
