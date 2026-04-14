# dags/medium_rag_pipeline.py
# Orchestrates the ingest, transform, and native BQ Vector sync layers

import os
from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from google.cloud import storage

# Centralized configuration
import medium_rag_utils.config as config

# Shared environment variables
COMMON_ENV = {
    "GCP_PROJECT_ID": config.GCP_PROJECT_ID,
    "GCP_REGION": config.GCP_REGION,
    "BRONZE_BUCKET": config.BRONZE_BUCKET,
    "BQ_DATASET": config.BQ_DATASET,
    "BQ_TABLE": config.BQ_TABLE
}

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
    tags=["medium", "bq-vector", "data-contracts"],
)
def medium_rag_pipeline():
    
    # Task 0: Optional Infrastructure Setup (One-off)
    @task
    def setup_bq_infrastructure():
        from medium_rag_utils.setup_bq import setup_infrastructure
        setup_infrastructure(config.GCP_PROJECT_ID, config.GCP_REGION, config.BQ_DATASET)

    # Task 1: Fetch RSS URLs
    fetch_rss = KubernetesPodOperator(
        task_id="fetch_topic_urls",
        name="fetch-topic-urls-pod",
        namespace="composer-user-workloads",
        image=config.IMAGE_INGEST_NODE,
        cmds=["node", "dist/fetch-topic-urls.js", "---", "--topic", config.TOPIC, "--date", "{{ ds }}"],
        env_vars=COMMON_ENV,
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    # Task 2: Read URLs from GCS
    @task
    def extract_urls_from_gcs(ds: str = None):
        storage_client = storage.Client(project=config.GCP_PROJECT_ID)
        bucket = storage_client.bucket(config.BRONZE_BUCKET)
        blob = bucket.blob(f"raw/topic-urls/{config.TOPIC}/{ds}/urls.json")
        
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
            
        chunk_size = 1000
        chunks = [urls[i : i + chunk_size] for i in range(0, len(urls), chunk_size)]
        
        storage_client = storage.Client(project=config.GCP_PROJECT_ID)
        bucket = storage_client.bucket(config.BRONZE_BUCKET)
        
        import json
        commands = []
        for i, chunk in enumerate(chunks):
            manifest_path = f"manifests/{config.TOPIC}/{ds}/chunk_{i}.json"
            blob = bucket.blob(manifest_path)
            blob.upload_from_string(json.dumps(chunk), content_type="application/json")
            
            commands.append(["node", "dist/fetch-medium-article.js", "---", "--topic", config.TOPIC, "--manifest", f"gs://{config.BRONZE_BUCKET}/{manifest_path}", "--date", ds])
            
        return commands

    # Task 3: Map the scraper
    scrape_articles = KubernetesPodOperator.partial(
        task_id="fetch_medium_article",
        name="fetch-medium-article-pod",
        namespace="composer-user-workloads",
        image=config.IMAGE_INGEST_NODE,
        env_vars=COMMON_ENV,
        container_resources=SCRAPER_RESOURCES,
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        cmds=prepare_bulk_manifests(urls=extract_urls_from_gcs(ds="{{ ds }}"), ds="{{ ds }}")
    )

    # Task 4: Load to BQ, Validate Contracts, Chunk & Embed
    @task
    def transform_and_vectorize(ds: str = None):
        from medium_rag_utils.bq_loader import run_load
        run_load(
            topic=config.TOPIC,
            date=ds,
            project_id=config.GCP_PROJECT_ID,
            bucket_name=config.BRONZE_BUCKET,
            dataset=config.BQ_DATASET,
            table=config.BQ_TABLE
        )

    # Architectural Dependency Graph
    # 1. Setup Infra (only if needed)
    # 2. Pipeline
    setup_bq_infrastructure() >> fetch_rss >> scrape_articles >> transform_and_vectorize(ds="{{ ds }}")

medium_rag_pipeline()
