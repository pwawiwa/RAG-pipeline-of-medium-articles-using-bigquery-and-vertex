# Path: dags/medium_rag_pipeline.py
# Purpose: Orchestrates the ingest, transform, and sync layers using Airflow 3 TaskFlow
# Idempotent: true
# Dependencies: KubernetesPodOperator, google-cloud-storage

import os
import json
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from airflow.operators.bash import BashOperator
from kubernetes.client import models as k8s
from google.cloud import storage

# Environment and Schema bindings (Matches catalog.json)
TOPIC = "data-engineering"
PROJECT_ID = "my-playground-492309"
BRONZE_BUCKET = "medium-bronze-my-playground-492309"
GCP_REGION = "asia-southeast1"
IMAGE_INGEST_NODE = "asia-southeast1-docker.pkg.dev/my-playground-492309/medium-repo/medium-ingest-node:latest"
GCP_PROJECT_ID = "my-playground-492309"
BQ_DATASET = "medium_pipeline"
BQ_TABLE = "silver_medium_articles"
# Vertex RAG Corpus - Managed by GCP
CORPUS_NAME = "projects/my-playground-492309/locations/asia-southeast1/ragCorpora/7679774659341189120"

# Shared environment variables for all tasks (K8s and Bash)
COMMON_ENV = {
    "GCP_PROJECT_ID": GCP_PROJECT_ID,
    "GCP_REGION": GCP_REGION,
    "BRONZE_BUCKET": BRONZE_BUCKET,
    "BQ_DATASET": BQ_DATASET,
    "BQ_TABLE": BQ_TABLE,
    "VERTEX_RAG_CORPUS_NAME": CORPUS_NAME
}

# Senior Recommendation: Explicit Resource Management (Increased for Bulk Mode)
SCRAPER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"}
)

def fetch_failed_callback(context):
    """Task-level failure callback that writes failure metadata to GCS."""
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date").strftime("%Y-%m-%d")
    exception = str(context.get("exception"))
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BRONZE_BUCKET)
    blob = bucket.blob(f"errors/airflow/{TOPIC}/{execution_date}/{task_id}_failure.json")
    
    payload = {
        "task_id": task_id,
        "timestamp": datetime.utcnow().isoformat(),
        "error": exception
    }
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    print(f"Logged failure to GCS: {blob.name}")

default_args = {
    "owner": "data-engineering-team",
    "retries": 1, # Reduced retry as scraper now handles failure internally per-article
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": fetch_failed_callback,
}

@dag(
    dag_id="medium_rag_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medium", "rag", "bulk-manifest", "1000x"],
)
def medium_rag_pipeline():
    
    # Task 1: Fetch RSS URLs (Node.js in K8s)
    fetch_rss = KubernetesPodOperator(
        task_id="fetch_topic_urls",
        name="fetch-topic-urls-pod",
        namespace="composer-user-workloads",
        image=IMAGE_INGEST_NODE,
        cmds=["npm", "run", "fetch-urls", "---", "--topic", TOPIC],
        env_vars=COMMON_ENV,
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task 2: Read URLs emitted by Task 1 from GCS via native Python TaskFlow
    @task
    def extract_urls_from_gcs(ds: str = None):
        storage_client = storage.Client()
        bucket = storage_client.bucket(BRONZE_BUCKET)
        blob = bucket.blob(f"raw/topic-urls/{TOPIC}/{ds}/urls.json")
        
        if not blob.exists():
            raise FileNotFoundError(f"URLs file not found: {blob.name}")
            
        data = json.loads(blob.download_as_text())
        urls = data.get("urls", [])
        print(f"Found {len(urls)} URLs to scrape.")
        return urls

    article_urls = extract_urls_from_gcs()

    # Helper task to Chunk URLs into Manifests (1000 urls per batch)
    @task
    def prepare_bulk_manifests(urls: list, ds: str = None):
        if not urls:
            return []
            
        chunk_size = 1000
        chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
        
        storage_client = storage.Client()
        bucket = storage_client.bucket(BRONZE_BUCKET)
        
        commands = []
        for i, chunk in enumerate(chunks):
            manifest_path = f"manifests/{TOPIC}/{ds}/chunk_{i}.json"
            blob = bucket.blob(manifest_path)
            blob.upload_from_string(json.dumps(chunk), content_type="application/json")
            
            # Form the command for KPO
            commands.append(["npm", "run", "fetch-articles", "---", "--topic", TOPIC, "--manifest", f"gs://{BRONZE_BUCKET}/{manifest_path}"])
            print(f"Created manifest: gs://{BRONZE_BUCKET}/{manifest_path} with {len(chunk)} URLs")
            
        return commands

    scraper_commands = prepare_bulk_manifests(article_urls)

    # Task 3: Map the scraper over the manifests (usually just 1 pod for < 1000 articles)
    scrape_articles = KubernetesPodOperator.partial(
        task_id="fetch_medium_article",
        name="fetch-medium-article-pod",
        namespace="composer-user-workloads",
        image=IMAGE_INGEST_NODE,
        env_vars=COMMON_ENV,
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        cmds=scraper_commands
    )

    # Task 4: Load to BigQuery (Silver)
    transform_to_silver = BashOperator(
        task_id="load_to_bigquery_silver",
        bash_command=f"python /home/airflow/gcs/data/apt/tools/transform/load_to_bigquery.py --topic {TOPIC} --date {{{{ ds }}}}",
        env=COMMON_ENV
    )

    # Task 5: Sync to Vertex AI RAG (Gold)
    sync_to_gold = BashOperator(
        task_id="update_vertex_rag",
        bash_command=f"python /home/airflow/gcs/data/apt/tools/serve/update_vertex_rag.py --date {{{{ ds }}}}",
        env=COMMON_ENV
    )

    # Architectural Dependency Graph
    fetch_rss >> article_urls >> scrape_articles >> transform_to_silver >> sync_to_gold

medium_rag_pipeline()
