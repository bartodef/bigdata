#!/usr/bin/env bash
# Lab S3 : rejoue métadonnées, presigned URL, lifecycle et consistency pour noter les résultats
# Lancement : bash lab-s3-observations.sh 2>&1 | tee sortie-s3-observations.txt
# Pas de set -e : on veut voir les erreurs éventuelles de SeaweedFS
cd "$(dirname "$0")"
export AWS_PAGER=""
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export S3_ENDPOINT_URL=$(aws configure get endpoint_url --profile 'default' || echo "https://$AWS_S3_ENDPOINT")
echo "$S3_ENDPOINT_URL $LAB_BUCKET_NAME"
uv run dataset-users -o csv > users.csv

echo "=== Métadonnées système : ETag vs md5"
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv
md5sum users.csv

echo "=== Métadonnées utilisateur"
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv" \
  --metadata "source=dataset_users.py,version=1,rows=50"
aws s3api --profile 'default' head-object --bucket "$LAB_BUCKET_NAME" --key bronze/users.csv | jq '.Metadata'

echo "=== Presigned URL GET"
URL=$(aws s3 --profile 'default' presign "s3://$LAB_BUCKET_NAME/bronze/users.csv" --expires-in 300)
echo "${URL%%\?*}?<signature masquée>"
curl -s "$URL" | head -n 3
echo "=== Presigned URL GET expirée (5 s)"
URL=$(aws s3 --profile 'default' presign "s3://$LAB_BUCKET_NAME/bronze/users.csv" --expires-in 5)
sleep 7
curl -s -o /dev/null -w "HTTP %{http_code}\n" "$URL"

echo "=== Presigned URL PUT (boto3)"
PUT_URL=$(uv run --with boto3 python - <<'PY'
import os
import boto3

session = boto3.Session(profile_name="default")
s3 = session.client("s3", endpoint_url=os.environ["S3_ENDPOINT_URL"])
print(s3.generate_presigned_url(
    "put_object",
    Params={"Bucket": os.environ["LAB_BUCKET_NAME"], "Key": "bronze/upload.csv"},
    ExpiresIn=300,
))
PY
)
curl -s -o /dev/null -w "HTTP %{http_code}\n" -T users.csv "$PUT_URL"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/upload.csv"

echo "=== Lifecycle"
cat > lifecycle.json <<'EOF'
{
  "Rules": [
    {
      "ID": "expire-large-after-30-days",
      "Status": "Enabled",
      "Filter": { "Prefix": "large/" },
      "Expiration": { "Days": 30 }
    },
    {
      "ID": "delete-noncurrent-versions",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "NoncurrentVersionExpiration": { "NoncurrentDays": 7 }
    },
    {
      "ID": "abort-incomplete-multipart",
      "Status": "Enabled",
      "Filter": { "Prefix": "" },
      "AbortIncompleteMultipartUpload": { "DaysAfterInitiation": 7 }
    }
  ]
}
EOF
aws s3api --profile 'default' put-bucket-lifecycle-configuration \
  --bucket "$LAB_BUCKET_NAME" --lifecycle-configuration file://lifecycle.json
aws s3api --profile 'default' get-bucket-lifecycle-configuration --bucket "$LAB_BUCKET_NAME"

echo "=== Consistency"
aws s3 --profile 'default' cp users.csv "s3://$LAB_BUCKET_NAME/bronze/users.csv"
aws s3 --profile 'default' cp "s3://$LAB_BUCKET_NAME/bronze/users.csv" /dev/null && echo "immediately available"

echo "=== Nettoyage"
aws s3 --profile 'default' rm "s3://$LAB_BUCKET_NAME/bronze/upload.csv"
aws s3api --profile 'default' delete-bucket-lifecycle --bucket "$LAB_BUCKET_NAME"
rm -f lifecycle.json
echo "=== Terminé"
