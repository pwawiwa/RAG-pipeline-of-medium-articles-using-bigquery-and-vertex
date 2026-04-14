# dags/medium_rag_pipeline.py
# Orchestrates the ingest, transform, and native BQ Vector sync layers
# Refactored for OOP Scalability

import os
import sys
import logging
from datetime import datetime, timedelta

# Bootstrap: Ensure the DAG directory is in the path for module resolution
dag_path = os.path.dirname(os.path.abspath(__file__))
if dag_path not in sys.path:
    sys.path.append(dag_path)

from airflow.decorators import dag, task  # pylint: disable=no-name-in-module
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import models as k8s
from google.cloud import storage

logger = logging.getLogger(__name__)

# New OOP Imports
from medium_rag_utils import cfg, MediumCollector, BigQueryManager

# Resource Management
SCRAPER_RESOURCES = k8s.V1ResourceRequirements(
    requests={"cpu": "500m", "memory": "512Mi"},
    limits={"cpu": "1000m", "memory": "1Gi"}
)

default_args = {
    "owner": "data-engineering-team",
    "retries": 3,
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
        """Fetches the list of URLs found by the RSS scraper. Returns [] if none found."""
        storage_client = storage.Client(project=cfg.project_id)
        bucket = storage_client.bucket(cfg.bronze_bucket)
        blob = bucket.blob(f"raw/topic-urls/{cfg.topic}/{ds}/urls.json")
        
        if not blob.exists():
            logger.warning(f"No URLs found for {cfg.topic} on {ds}. Skipping downstream scraping.")
            return []
            
        import json
        data = json.loads(blob.download_as_text())
        urls = data.get("urls", [])
        logger.info(f"Extracted {len(urls)} URLs to scrape.")
        return urls

    # Helper task to Chunk URLs into Manifests
    @task
    def prepare_bulk_manifests(urls: list, ds: str = None):
        """Chunks URLs and uploads manifests to GCS for the parallel scrapers."""
        if not urls:
            logger.info("Empty URL list. No manifests to prepare.")
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
            
        logger.info(f"Prepared {len(commands)} scraper manifest commands.")
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
    def transform_and_vectorize(cmds: list, ds: str = None):
        """Invoke the MediumCollector. Skips if no scrape commands were generated."""
        if not cmds:
            logger.info("No scraping was performed. Skipping BigQuery load and indexing.")
            return
            
        collector = MediumCollector()
        collector.process(ds)

    # Architectural Dependency Graph
    infra_task = setup_bq_infrastructure()
    urls_list = extract_urls_from_gcs(ds="{{ ds }}")
    manifest_cmds = prepare_bulk_manifests(urls=urls_list, ds="{{ ds }}")
    
    scrape_tasks = scrape_articles.expand(cmds=manifest_cmds)
    
    infra_task >> fetch_rss >> urls_list >> manifest_cmds >> scrape_tasks >> transform_and_vectorize(cmds=manifest_cmds, ds="{{ ds }}")

medium_rag_pipeline()
