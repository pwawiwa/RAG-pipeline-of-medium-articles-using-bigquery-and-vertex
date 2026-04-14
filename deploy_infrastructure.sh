#!/bin/bash
# Path: deploy_infrastructure.sh
# Purpose: Single-click script to provision all GCP infrastructure for the Medium RAG Pipeline

set -e

PROJECT_ID="my-playground-492309"
REGION="asia-southeast1"
BRONZE_BUCKET="medium-bronze-${PROJECT_ID}"
BQ_DATASET="medium_pipeline"
BQ_TABLE="silver_medium_articles"
REPO_NAME="medium-repo"
INGEST_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/medium-ingest-node:latest"
API_IMAGE="${REGION}-docker.pkg.dev/${PROJECT_ID}/${REPO_NAME}/medium-rag-api:latest"
COMPOSER_ENV_NAME="medium-airflow-orchestrator"
CORPUS_NAME="medium-rag-corpus"

echo "=========================================================="
echo "🚀 INITIATING GCP MEDIUM RAG PIPELINE DEPLOYMENT"
echo "Project: $PROJECT_ID | Region: $REGION"
echo "=========================================================="

# 1. Enable Required Services
echo "[1/8] Enabling GCP APIs (this can take a moment)..."
gcloud services enable \
    compute.googleapis.com \
    storage.googleapis.com \
    bigquery.googleapis.com \
    run.googleapis.com \
    secretmanager.googleapis.com \
    artifactregistry.googleapis.com \
    aiplatform.googleapis.com \
    composer.googleapis.com \
    cloudbuild.googleapis.com \
    --project $PROJECT_ID

# 2. Provision GCS Bronze Bucket
echo "[2/8] Provisioning Bronze Storage: gs://$BRONZE_BUCKET..."
if ! gcloud storage buckets describe gs://$BRONZE_BUCKET --project $PROJECT_ID &>/dev/null; then
    gcloud storage buckets create gs://$BRONZE_BUCKET --location=$REGION --project $PROJECT_ID
else
    echo "Bucket already exists."
fi

# 3. Setup BigQuery Silver Layer
echo "[3/8] Provisioning BigQuery Dataset & Table..."
bq mk --force=true --location=$REGION ${PROJECT_ID}:${BQ_DATASET} || true

# Extract just the column definitions for the BQ CLI
# Using python to cleanly extract the schema array ignoring comments
python3 -c "import json; f = open('schema_context/catalog.json'); content = '\n'.join([line for line in f if not line.strip().startswith('//')]); print(json.dumps(json.loads(content)['bigquery_schema']['columns']));" > bq_schema.json

if ! bq show ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} &>/dev/null; then
    bq mk --table \
        --description "Silver Medium Articles" \
        --time_partitioning_field published_date --time_partitioning_type DAY \
        --clustering_fields topic,published_date \
        ${PROJECT_ID}:${BQ_DATASET}.${BQ_TABLE} \
        bq_schema.json
else
    echo "Table already exists."
fi
rm bq_schema.json

# 4. Provision Secret Manager
echo "[4/8] Provisioning Secret Manager for API Key..."
if ! gcloud secrets describe medium-api-key --project $PROJECT_ID &>/dev/null; then
    gcloud secrets create medium-api-key --replication-policy="user-managed" --locations="$REGION" --project $PROJECT_ID
    printf "prod-secure-token-12345" | gcloud secrets versions add medium-api-key --data-file=- --project $PROJECT_ID
else
    echo "Secret already exists."
fi

# 5. Provision Artifact Registry & Build Containers
echo "[5/8] Creating Artifact Registry & Building Images using Cloud Build..."
if ! gcloud artifacts repositories describe $REPO_NAME --location=$REGION --project $PROJECT_ID &>/dev/null; then
    gcloud artifacts repositories create $REPO_NAME --repository-format=docker --location=$REGION --description="Medium RAG containers" --project $PROJECT_ID
fi

# Build Node.js Scraper using Cloud Build (bypasses need for local Docker)
gcloud builds submit ./apt/tools/ingest --tag $INGEST_IMAGE --project $PROJECT_ID --region $REGION
# Build FastAPI Container
gcloud builds submit ./api --tag $API_IMAGE --project $PROJECT_ID --region $REGION

# 6. Deploy Cloud Run API
echo "[6/8] Deploying Cloud Run API..."
# Get default compute service account to grant secret access
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
COMPUTE_SA="${PROJECT_NUMBER}-compute@developer.gserviceaccount.com"

gcloud secrets add-iam-policy-binding medium-api-key \
    --member="serviceAccount:${COMPUTE_SA}" \
    --role="roles/secretmanager.secretAccessor" \
    --project $PROJECT_ID > /dev/null

gcloud run deploy medium-rag-api \
    --image $API_IMAGE \
    --region $REGION \
    --project $PROJECT_ID \
    --allow-unauthenticated \
    --memory 1Gi \
    --concurrency 80 \
    --set-env-vars GCP_PROJECT_ID=$PROJECT_ID,GCP_REGION=$REGION,VERTEX_RAG_CORPUS_NAME=$CORPUS_NAME,VERTEX_LLM_MODEL="gemini-flash-latest",API_KEY_SECRET_NAME="medium-api-key"

# 7. Provision Vertex AI RAG Corpus (Via Python Snippet)
echo "[7/8] Provisioning Vertex AI Managed Corpus..."
python3 -m venv .rag_venv
source .rag_venv/bin/activate
pip install google-cloud-aiplatform > /dev/null 2>&1
python3 -c "
import os
import vertexai
from vertexai.preview import rag

vertexai.init(project='$PROJECT_ID', location='$REGION')
try:
    rag.create_corpus(display_name='$CORPUS_NAME')
    print('Corpus created successfully.')
except Exception as e:
    print('Corpus may already exist:', e)
"
deactivate
rm -rf .rag_venv

# 8. Provision Cloud Composer (Airflow)
echo "[8/8] Provisioning Cloud Composer (Expected time: 25-35 minutes)..."
echo "WARNING: If you wish to skip this step to save time/credits, hit Ctrl+C now. The rest of the API is deployed!"
read -n 1 -s -r -p "Press any key to initiate Airflow creation..."

if ! gcloud composer environments describe $COMPOSER_ENV_NAME --location=$REGION --project $PROJECT_ID &>/dev/null; then
    echo "Configuring permissions for Composer Service Account..."
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:${COMPUTE_SA}" \
        --role="roles/composer.worker" \
        --quiet

    gcloud iam service-accounts add-iam-policy-binding ${COMPUTE_SA} \
        --member="serviceAccount:${COMPUTE_SA}" \
        --role="roles/iam.serviceAccountUser" \
        --project $PROJECT_ID \
        --quiet

    echo "Creating Cloud Composer environment (this takes 25-35 mins)..."
    gcloud composer environments create $COMPOSER_ENV_NAME \
        --location $REGION \
        --project $PROJECT_ID \
        --service-account $COMPUTE_SA
    
    # Upload DAGs
    DAG_BUCKET=$(gcloud composer environments describe $COMPOSER_ENV_NAME --location=$REGION --format="value(config.dagGcsPrefix)")
    gcloud storage cp dags/medium_rag_pipeline.py ${DAG_BUCKET}/
    gcloud storage rsync apt ${DAG_BUCKET}/../data/apt --recursive
    
    echo "[SUCCESS] DAGs uploaded to Composer."
else
    echo "Composer environment already exists."
fi

echo "=========================================================="
echo "🎉 DEPLOYMENT COMPLETE 🎉"
API_URL=$(gcloud run services describe medium-rag-api --region $REGION --format="value(status.url)")
echo "API Endpoint: $API_URL"
echo "Test API command:"
echo 'curl -X POST '"$API_URL"'/ask -H "X-API-Key: prod-secure-token-12345" -H "Content-Type: application/json" -d '\''{"query":"data engineering"}'\'''
echo "=========================================================="
