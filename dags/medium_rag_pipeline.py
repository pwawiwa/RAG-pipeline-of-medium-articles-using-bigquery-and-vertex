# dags/medium_rag_pipeline.py
# Orchestrates the ingest, transform, and native BQ Vector sync layers
# Refactored for OOP Scalability

import os
import sys
from datetime import datetime, timedelta

# Bootstrap: Ensure the DAG directory is in the path for module resolution
dag_path = os.path.dirname(os.path.abspath(__file__))
if dag_path not in sys.path:
    sys.path.append(dag_path)

from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from google.cloud import storage

# New OOP Imports
from medium_rag_utils import cfg, MediumCollector, BigQueryManager

# Resource Management
SCRAPER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"}
)

default_args = {
    "owner": "data-engineering-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

@dag(
    dag_id="medium_rag_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medium", "bq-vector", "oop-refactor"],
)
def medium_rag_pipeline():
    
    @task
    def setup_bq_infrastructure():
        """Initialize connection, model, and tables via the Warehouse Manager."""
        warehouse = BigQueryManager()
        warehouse.setup_infrastructure()

    # Task 1: Fetch RSS URLs
    fetch_rss = KubernetesPodOperator(
        task_id="fetch_topic_urls",
        name="fetch-topic-urls-pod",
        namespace="composer-user-workloads",
        image=cfg.image_ingest_node,
        cmds=["node", "dist/fetch-topic-urls.js", "---", "--topic", cfg.topic, "--date", "{{ ds }}"],
        env_vars={
            "GCP_PROJECT_ID": cfg.project_id,
            "GCP_REGION": cfg.region,
            "BRONZE_BUCKET": cfg.bronze_bucket
        },
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task 2: Read URLs from GCS
    @task
    def extract_urls_from_gcs(ds: str = None):
        storage_client = storage.Client(project=cfg.project_id)
        bucket = storage_client.bucket(cfg.bronze_bucket)
        blob = bucket.blob(f"raw/topic-urls/{cfg.topic}/{ds}/urls.json")
        
        if not blob.exists():
            raise FileNotFoundError(f"URLs file not found: {blob.name}")
            
        import json
        data = json.loads(blob.download_as_text())
        urls = data.get("urls", [])
        return urls

    # Helper task to Chunk URLs into Manifests
    @task
    def prepare_bulk_manifests(urls: list, ds: str = None):
        if not urls:
            return []
            
        chunk_size = 2500
        chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
        
        storage_client = storage.Client(project=cfg.project_id)
        bucket = storage_client.bucket(cfg.bronze_bucket)
        
        import json
        commands = []
        for i, chunk in enumerate(chunks):
            manifest_path = f"manifests/{cfg.topic}/{ds}/chunk_{i}.json"
            blob = bucket.blob(manifest_path)
            blob.upload_from_string(json.dumps(chunk), content_type="application/json")
            
            commands.append(["node", "dist/fetch-medium-article.js", "---", "--topic", cfg.topic, "--manifest", f"gs://{cfg.bronze_bucket}/{manifest_path}", "--date", ds])
            
        return commands

    # Task 3: Map the scraper
    scrape_articles = KubernetesPodOperator.partial(
        task_id="fetch_medium_article",
        name="fetch-medium-article-pod",
        namespace="composer-user-workloads",
        image=cfg.image_ingest_node,
        env_vars={
            "GCP_PROJECT_ID": cfg.project_id,
            "GCP_REGION": cfg.region,
            "BRONZE_BUCKET": cfg.bronze_bucket
        },
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        cmds=prepare_bulk_manifests(urls=extract_urls_from_gcs(ds="{{ ds }}"), ds="{{ ds }}")
    )

    # Task 4: Load to BQ, Validate Contracts, Chunk & Embed
    @task
    def transform_and_vectorize(ds: str = None):
        """Invoke the MediumCollector to handle load and indexing."""
        collector = MediumCollector()
        collector.process(ds)

    # Architectural Dependency Graph
    # 1. Setup Infra (only if needed)
    # 2. Pipeline
    setup_bq_infrastructure() >> fetch_rss >> scrape_articles >> transform_and_vectorize(ds="{{ ds }}")

medium_rag_pipeline()
