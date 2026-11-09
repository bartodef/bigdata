#!/usr/bin/env bash
# Lab Polaris, etape 2 : fichiers du chart, ConfigMap, Secrets et deploiement Helm
# Lancement depuis ~/work/bigdata : bash lab-09/2-deploiement.sh 2>&1 | tee lab-09/sortie-polaris-deploiement.txt
cd "$(dirname "$0")/.."
export AWS_PAGER=""
# les variables AWS_* du service sont perimees : on reprend celles du profil default
export AWS_ACCESS_KEY_ID="$(aws configure get aws_access_key_id --profile default)"
export AWS_SECRET_ACCESS_KEY="$(aws configure get aws_secret_access_key --profile default)"
export AWS_SESSION_TOKEN="$(aws configure get aws_session_token --profile default)"

echo "=== Fichiers du chart"
mkdir -p lab-09/deployment/templates
# Chart.yaml
cat <<'YAML' >lab-09/deployment/Chart.yaml
apiVersion: v2
name: polaris
version: 1.8.0
dependencies:
  - name: polaris
    version: 1.8.0
    repository: https://downloads.apache.org/polaris/helm-chart
  - name: postgresql
    version: 18.12.15
    repository: oci://registry-1.docker.io/bitnamicharts
YAML
# values.yaml
cat <<'YAML' >lab-09/deployment/values.yaml
postgresql:
  auth:
    username: polaris
    database: polaris
    existingSecret: polaris-db
  primary:
    initdb:
      scripts:
        01-create-polaris-schema.sql: |
          CREATE SCHEMA IF NOT EXISTS polaris_schema AUTHORIZATION polaris;
    persistence:
      size: 1Gi
    resources:
      requests:
        cpu: 50m
        memory: 128Mi
      limits:
        cpu: 250m
        memory: 256Mi

polaris:
  persistence:
    type: relational-jdbc
    relationalJdbc:
      secret:
        name: polaris-db
        username: username
        password: password
        jdbcUrl: jdbcUrl

  authentication:
    type: internal

  resources:
    requests:
      cpu: 100m
      memory: 256Mi
    limits:
      cpu: 500m
      memory: 512Mi

  # Load Polaris configuration from the existing ConfigMap.
  extraEnvFrom:
    - configMapRef:
        name: polaris-config

  # Load S3 credentials from the existing Secret.
  extraEnv:
    - name: POLARIS_BOOTSTRAP_CREDENTIALS
      value: "POLARIS,root,s3cr3t"

    - name: AWS_ACCESS_KEY_ID
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_ACCESS_KEY_ID

    - name: AWS_SECRET_ACCESS_KEY
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SECRET_ACCESS_KEY

    - name: AWS_SESSION_TOKEN
      valueFrom:
        secretKeyRef:
          name: polaris-s3
          key: AWS_SESSION_TOKEN

    - name: AWS_REGION
      value: us-east-1

  extraInitContainers:
    - name: polaris-bootstrap
      image: apache/polaris-admin-tool:1.8.0
      resources:
        requests:
          cpu: 50m
          memory: 128Mi
        limits:
          cpu: 500m
          memory: 512Mi

      env:
        - name: QUARKUS_DATASOURCE_USERNAME
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: username
        - name: QUARKUS_DATASOURCE_PASSWORD
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: password
        - name: QUARKUS_DATASOURCE_JDBC_URL
          valueFrom:
            secretKeyRef:
              name: polaris-db
              key: jdbcUrl
        - name: POLARIS_PERSISTENCE_TYPE
          value: relational-jdbc
        - name: ROOT_PASSWORD
          valueFrom:
            secretKeyRef:
              name: polaris-credential
              key: POLARIS_ROOT_SECRET

      command: ["/bin/sh", "-c"]
      args:
        - |
          until (echo > /dev/tcp/polaris-postgresql/5432) 2>/dev/null; do
            echo "Waiting for PostgreSQL..."
            sleep 3
          done
          echo "PostgreSQL is ready."
          out="$(java -jar /deployments/polaris-admin-tool.jar bootstrap \
            -r POLARIS \
            -c "POLARIS,root,$ROOT_PASSWORD" 2>&1)"; rc=$?
          echo "$out"
          [ "$rc" -eq 0 ] && exit 0
          echo "$out" | grep -q "already been bootstrapped" && exit 0
          exit 1

  livenessProbe:
    initialDelaySeconds: 90
    failureThreshold: 6
    periodSeconds: 10
    timeoutSeconds: 10
    successThreshold: 1
    terminationGracePeriodSeconds: 30
YAML
# bootstrap-job.yaml
cat <<'YAML' >lab-09/deployment/templates/bootstrap-job.yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: polaris-bootstrap
  namespace: {{ .Release.Namespace }}
  labels:
    app.kubernetes.io/name: polaris-bootstrap
    app.kubernetes.io/instance: {{ .Release.Name }}
  annotations:
    helm.sh/hook: post-install,post-upgrade
    helm.sh/hook-weight: "10"
    helm.sh/hook-delete-policy: before-hook-creation
spec:
  backoffLimit: 3
  activeDeadlineSeconds: 600

  template:
    metadata:
      labels:
        app.kubernetes.io/name: polaris-bootstrap
        app.kubernetes.io/instance: {{ .Release.Name }}

    spec:
      restartPolicy: Never

      containers:
        - name: bootstrap
          image: alpine:3.21
          imagePullPolicy: IfNotPresent

          resources:
            requests:
              cpu: 50m
              memory: 32Mi
            limits:
              cpu: 200m
              memory: 56Mi

          envFrom:
            - configMapRef:
                name: polaris-config

          env:
            - name: POLARIS_ROOT_SECRET
              valueFrom:
                secretKeyRef:
                  name: polaris-credential
                  key: POLARIS_ROOT_SECRET

            - name: AWS_REGION
              value: us-east-1

            - name: KUBERNETES_NAMESPACE
              valueFrom:
                fieldRef:
                  fieldPath: metadata.namespace

            - name: LAB_BUCKET_NAME
              value: "$(KUBERNETES_NAMESPACE)"

            - name: POLARIS_BASE_LOCATION
              value: "s3://$(LAB_BUCKET_NAME)/warehouse/"

            - name: S3_ENDPOINT
              value: "https://s3.seaweedfs.adm.adaltas.cloud"

          command:
            - /bin/sh
            - -ec
            - |
              apk add --no-cache curl jq

              POLARIS_URI="${POLARIS_URI%/}"
              API="${POLARIS_URI}/api/management/v1"
              TOKEN_URI="${POLARIS_URI}/api/catalog/v1/oauth/tokens"

              echo "Waiting for Polaris..."

              until curl -fsS \
                "${POLARIS_MGMT_URI}/q/health/ready" >/dev/null; do
                echo "Polaris is not ready; retrying in 5 seconds..."
                sleep 5
              done

              echo "Polaris is ready."
              echo "Realm: ${POLARIS_REALM}"
              echo "Catalog: ${POLARIS_CATALOG}"

              # --------------------------------------------------
              # Authentication
              # --------------------------------------------------

              echo "Obtaining administrative access token..."

              TOKEN_RESPONSE=$(curl -fsS \
                -u "${POLARIS_CLIENT_ID}:${POLARIS_ROOT_SECRET}" \
                -H "Polaris-Realm: ${POLARIS_REALM}" \
                -H "Content-Type: application/x-www-form-urlencoded" \
                -d "grant_type=client_credentials" \
                -d "scope=PRINCIPAL_ROLE:ALL" \
                "${TOKEN_URI}")

              TOKEN=$(echo "${TOKEN_RESPONSE}" |
                jq -er '.access_token')

              # --------------------------------------------------
              # API helpers
              # --------------------------------------------------

              api_get() {
                curl -fsS \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Accept: application/json" \
                  "$1"
              }

              api_post() {
                curl -fsS \
                  -X POST \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Content-Type: application/json" \
                  -H "Accept: application/json" \
                  -d "$2" \
                  "$1"
              }

              api_put() {
                curl -fsS \
                  -X PUT \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  -H "Content-Type: application/json" \
                  -H "Accept: application/json" \
                  -d "$2" \
                  "$1"
              }

              # Return HTTP status without hiding unexpected API errors.
              http_status() {
                curl -sS \
                  -o /dev/null \
                  -w '%{http_code}' \
                  -H "Authorization: Bearer ${TOKEN}" \
                  -H "Polaris-Realm: ${POLARIS_REALM}" \
                  "$1"
              }

              # --------------------------------------------------
              # Create principals
              # --------------------------------------------------

              ensure_principal() {
                name="$1"
                endpoint="${API}/principals/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Principal '${name}' already exists."
                    ;;
                  404)
                    echo "Creating principal '${name}'..."

                    response=$(api_post "${API}/principals" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          principal: {
                            name: $name,
                            properties: {}
                          },
                          credentialRotationRequired: false
                        }')")

                    echo "Principal '${name}' created."

                    # Credentials are returned only when the principal
                    # is created. Store them securely for later use.
                    echo "${response}" | jq -c '{
                      principal: .principal.name,
                      credentials: .credentials
                    }'
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking principal '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_principal dev
              ensure_principal analyst

              # --------------------------------------------------
              # Create principal roles
              # --------------------------------------------------

              ensure_principal_role() {
                name="$1"
                endpoint="${API}/principal-roles/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Principal role '${name}' already exists."
                    ;;
                  404)
                    echo "Creating principal role '${name}'..."

                    api_post "${API}/principal-roles" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          principalRole: {
                            name: $name,
                            properties: {}
                          }
                        }')" >/dev/null

                    echo "Principal role '${name}' created."
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking principal role '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_principal_role data_engineer
              ensure_principal_role data_analyst

              # --------------------------------------------------
              # Create catalog
              # --------------------------------------------------

              endpoint="${API}/catalogs/${POLARIS_CATALOG}"
              status=$(http_status "${endpoint}")

              case "${status}" in
                200)
                  echo "Catalog '${POLARIS_CATALOG}' already exists."
                  ;;
                404)
                  echo "Creating catalog '${POLARIS_CATALOG}'..."

                  CATALOG_PAYLOAD=$(jq -nc \
                    --arg name "${POLARIS_CATALOG}" \
                    --arg location "${POLARIS_BASE_LOCATION}" \
                    --arg endpoint "${S3_ENDPOINT}" \
                    --arg region "${AWS_REGION}" \
                    '{
                      catalog: {
                        name: $name,
                        type: "INTERNAL",
                        properties: {
                          "default-base-location": $location
                        },
                        storageConfigInfo: {
                          storageType: "S3",
                          allowedLocations: [$location],
                          endpoint: $endpoint,
                          endpointInternal: $endpoint,
                          pathStyleAccess: true,
                          region: $region,
                          stsUnavailable: true
                        }
                      }
                    }')

                  api_post "${API}/catalogs" \
                    "${CATALOG_PAYLOAD}" >/dev/null

                  echo "Catalog '${POLARIS_CATALOG}' created."
                  ;;
                *)
                  echo "Unexpected HTTP ${status} checking catalog." >&2
                  exit 1
                  ;;
              esac

              # --------------------------------------------------
              # Create catalog roles
              # --------------------------------------------------

              ensure_catalog_role() {
                name="$1"
                endpoint="${API}/catalogs/${POLARIS_CATALOG}/catalog-roles/${name}"
                status=$(http_status "${endpoint}")

                case "${status}" in
                  200)
                    echo "Catalog role '${name}' already exists."
                    ;;
                  404)
                    echo "Creating catalog role '${name}'..."

                    api_post \
                      "${API}/catalogs/${POLARIS_CATALOG}/catalog-roles" \
                      "$(jq -nc --arg name "${name}" \
                        '{
                          catalogRole: {
                            name: $name,
                            properties: {}
                          }
                        }')" >/dev/null

                    echo "Catalog role '${name}' created."
                    ;;
                  *)
                    echo "Unexpected HTTP ${status} checking catalog role '${name}'." >&2
                    exit 1
                    ;;
                esac
              }

              ensure_catalog_role catalog_contributor
              ensure_catalog_role catalog_reader

              # --------------------------------------------------
              # Grant privileges to catalog roles
              # --------------------------------------------------

              grant_privilege() {
                role="$1"
                privilege="$2"

                endpoint="${API}/catalogs/${POLARIS_CATALOG}/catalog-roles/${role}/grants"

                # Avoid submitting duplicate grants.
                grants=$(api_get "${endpoint}")

                if echo "${grants}" | jq -e \
                  --arg privilege "${privilege}" \
                  '.grants[]? |
                    select(
                      .type == "catalog" and
                      .privilege == $privilege
                    )' >/dev/null; then
                  echo "${privilege} is already granted to '${role}'."
                  return
                fi

                echo "Granting ${privilege} to '${role}'..."

                payload=$(jq -nc --arg privilege "${privilege}" \
                  '{
                    grant: {
                      type: "catalog",
                      privilege: $privilege
                    }
                  }')

                api_put "${endpoint}" "${payload}" >/dev/null

                echo "Granted ${privilege} to '${role}'."
              }

              # Contributor privileges.
              grant_privilege catalog_contributor CATALOG_MANAGE_ACCESS
              grant_privilege catalog_contributor CATALOG_MANAGE_METADATA
              grant_privilege catalog_contributor CATALOG_MANAGE_CONTENT

              # Reader privileges.
              grant_privilege catalog_reader TABLE_READ_DATA
              grant_privilege catalog_reader TABLE_READ_PROPERTIES
              grant_privilege catalog_reader CATALOG_READ_PROPERTIES

              # --------------------------------------------------
              # Assign principal roles to principals
              # --------------------------------------------------

              assign_principal_role() {
                principal="$1"
                role="$2"

                endpoint="${API}/principals/${principal}/principal-roles"

                roles=$(api_get "${endpoint}")

                if echo "${roles}" | jq -e \
                  --arg role "${role}" \
                  '.roles[]? | select(.name == $role)' \
                  >/dev/null; then
                  echo "Principal role '${role}' is already assigned to '${principal}'."
                  return
                fi

                echo "Assigning '${role}' to '${principal}'..."

                api_put "${endpoint}" \
                  "$(jq -nc --arg role "${role}" \
                    '{
                      principalRole: {
                        name: $role
                      }
                    }')" >/dev/null

                echo "Assigned '${role}' to '${principal}'."
              }

              assign_principal_role dev data_engineer
              assign_principal_role analyst data_analyst

              # --------------------------------------------------
              # Assign catalog roles to principal roles
              # --------------------------------------------------

              assign_catalog_role() {
                principal_role="$1"
                catalog_role="$2"

                endpoint="${API}/principal-roles/${principal_role}/catalog-roles/${POLARIS_CATALOG}"

                roles=$(api_get "${endpoint}")

                if echo "${roles}" | jq -e \
                  --arg role "${catalog_role}" \
                  '.roles[]? | select(.name == $role)' \
                  >/dev/null; then
                  echo "Catalog role '${catalog_role}' is already assigned to '${principal_role}'."
                  return
                fi

                echo "Assigning '${catalog_role}' to '${principal_role}'..."

                api_put "${endpoint}" \
                  "$(jq -nc --arg role "${catalog_role}" \
                    '{
                      catalogRole: {
                        name: $role
                      }
                    }')" >/dev/null

                echo "Assigned '${catalog_role}' to '${principal_role}'."
              }

              assign_catalog_role data_engineer catalog_contributor
              assign_catalog_role data_analyst catalog_reader

              echo "========================================"
              echo "Polaris RBAC bootstrap completed."
              echo "Catalog: ${POLARIS_CATALOG}"
              echo "dev      -> data_engineer -> catalog_contributor"
              echo "analyst  -> data_analyst -> catalog_reader"
              echo "========================================"
YAML
# polaris-db-secret.yaml
cat <<'YAML' >lab-09/deployment/templates/polaris-db-secret.yaml
apiVersion: v1
kind: Secret
metadata:
  name: polaris-db
  annotations:
    secret-generator.v1.mittwald.de/autogenerate: password
type: Opaque
stringData:
  username: polaris
  jdbcUrl: jdbc:postgresql://{{ .Release.Name }}-postgresql:5432/polaris
YAML

echo "=== Variables"
GIT_REPO_NAME=bigdata
export LAB_BUCKET_NAME="$KUBERNETES_NAMESPACE"
export POLARIS_URI="http://polaris:8181"
export POLARIS_MGMT_URI="http://polaris-mgmt:8182"
export POLARIS_CATALOG="$LAB_BUCKET_NAME"
export API="$POLARIS_URI/api/management/v1"
echo "$LAB_BUCKET_NAME"
aws s3 --profile 'default' ls "s3://$LAB_BUCKET_NAME/bronze/"

echo "=== ConfigMap et Secrets"
kubectl create configmap polaris-config \
  --from-literal=POLARIS_CATALOG="$POLARIS_CATALOG" \
  --from-literal=POLARIS_URI="$POLARIS_URI" \
  --from-literal=POLARIS_MGMT_URI="$POLARIS_MGMT_URI" \
  --from-literal=POLARIS_REALM="POLARIS" \
  --from-literal=POLARIS_CLIENT_ID="root" \
  --from-literal=POLARIS_BASE_LOCATION="s3://$LAB_BUCKET_NAME/warehouse/" \
  --from-literal=S3_ENDPOINT="https://s3.seaweedfs.adm.adaltas.cloud/" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-credential \
  --from-literal=POLARIS_ROOT_SECRET="s3cr3t" \
  --dry-run=client -o yaml | kubectl apply -f -
kubectl create secret generic polaris-s3 \
  --from-literal=AWS_ACCESS_KEY_ID="$AWS_ACCESS_KEY_ID" \
  --from-literal=AWS_SECRET_ACCESS_KEY="$AWS_SECRET_ACCESS_KEY" \
  --from-literal=AWS_SESSION_TOKEN="$AWS_SESSION_TOKEN" \
  --dry-run=client -o yaml | kubectl apply -f -

echo "=== Deploiement (2 a 3 minutes)"
helm repo add polaris https://downloads.apache.org/polaris/helm-chart
helm repo update
helm dependency update lab-09/deployment
helm upgrade --install polaris lab-09/deployment --wait

echo "=== Sante"
curl -s http://polaris-mgmt:8182/q/health

echo
echo "=== Identifiants dev et analyst"
kubectl logs job/polaris-bootstrap \
  | grep '"credentials"' \
  | jq -r 'select(.principal == "dev" or .principal == "analyst")
      | "POLARIS_\(.principal | ascii_upcase)_CREDENTIAL=\"\(.credentials.clientId):\(.credentials.clientSecret)\""' \
  > /home/onyxia/.polaris-credentials.env
set -a; . /home/onyxia/.polaris-credentials.env; set +a
echo "${POLARIS_DEV_CREDENTIAL%%:*}:..."
echo "${POLARIS_ANALYST_CREDENTIAL%%:*}:..."

echo "=== Entites creees par le bootstrap"
TOKEN=$(curl -s "$POLARIS_URI/api/catalog/v1/oauth/tokens" \
  -d grant_type=client_credentials \
  -d client_id=root \
  -d client_secret=s3cr3t \
  -d scope=PRINCIPAL_ROLE:ALL \
| jq -r .access_token)
curl -s "$API/catalogs" \
  -H "Authorization: Bearer $TOKEN" \
| jq
curl -s "$API/principals" \
  -H "Authorization: Bearer $TOKEN" \
| jq
curl -s "$API/principal-roles" \
  -H "Authorization: Bearer $TOKEN" \
| jq
curl -s "$API/catalogs/$POLARIS_CATALOG/catalog-roles" \
  -H "Authorization: Bearer $TOKEN" \
| jq

kubectl get pods
echo "=== Termine"
