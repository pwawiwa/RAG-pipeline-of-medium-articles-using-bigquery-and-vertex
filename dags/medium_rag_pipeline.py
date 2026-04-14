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
from google.cloud import storage

# Environment and Schema bindings (Matches catalog.json)
BRONZE_BUCKET = os.environ.get("BRONZE_BUCKET", "medium-bronze-asia-southeast1-my-playground-492309")
TOPIC = "data-engineering" # Default target topic
IMAGE_INGEST_NODE = "asia-southeast1-docker.pkg.dev/my-playground-492309/medium-repo/medium-ingest-node:latest"

def fetch_failed_callback(context):
    """Task-level failure callback that writes failure metadata to GCS."""
    task_id = context.get("task_instance").task_id
    execution_date = context.get("execution_date").strftime("%Y-%m-%d")
    exception = str(context.get("exception"))
    
    storage_client = storage.Client()
    bucket = storage_client.bucket(BRONZE_BUCKET)
    blob = bucket.file(f"errors/airflow/{TOPIC}/{execution_date}/{task_id}_failure.json")
    
    payload = {
        "task_id": task_id,
        "timestamp": datetime.utcnow().isoformat(),
        "error": exception
    }
    blob.upload_from_string(json.dumps(payload), content_type="application/json")
    print(f"Logged failure to GCS: {blob.name}")

default_args = {
    "owner": "data-engineering-team",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": fetch_failed_callback,
}

@dag(
    dag_id="medium_rag_pipeline",
    default_args=default_args,
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["medium", "rag", "bronze-silver-gold"],
)
def medium_rag_pipeline():
    
    # Task 1: Fetch RSS URLs (Node.js in K8s)
    fetch_rss = KubernetesPodOperator(
        task_id="fetch_topic_urls",
        name="fetch-topic-urls-pod",
        namespace="default",
        image=IMAGE_INGEST_NODE,
        cmds=["npm", "run", "fetch-urls", "---", "--topic", TOPIC],
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

    # Helper task to format command strings for the dynamic pods
    @task
    def prepare_scraper_commands(urls: list):
        return [["npm", "run", "fetch-articles", "---", "--topic", TOPIC, "--url", url] for url in urls]

    scraper_commands = prepare_scraper_commands(article_urls)

    # Task 3: Dynamically map the article scraper over every URL individually
    # Required by specification: "One execution per URL"
    scrape_articles = KubernetesPodOperator.partial(
        task_id="fetch_medium_article",
        name="fetch-medium-article-pod",
        namespace="default",
        image=IMAGE_INGEST_NODE,
        get_logs=True,
        is_delete_operator_pod=True,
    ).expand(
        cmds=scraper_commands
    )

    # Task 4: Load to BigQuery (Silver) using BashOperator to invoke deployed Python script
    transform_to_silver = BashOperator(
        task_id="load_to_bigquery_silver",
        # Assuming apt/tools are deployed alongside DAGs in Composer
        bash_command=f"python /home/airflow/gcs/data/apt/tools/transform/load_to_bigquery.py --topic {TOPIC} --date {{{{ ds }}}}"
    )

    # Task 5: Sync to Vertex AI RAG (Gold) using BashOperator
    sync_to_gold = BashOperator(
        task_id="update_vertex_rag",
        bash_command=f"python /home/airflow/gcs/data/apt/tools/serve/update_vertex_rag.py --date {{{{ ds }}}}"
    )

    # Architectural Dependency Graph
    fetch_rss >> article_urls >> scrape_articles >> transform_to_silver >> sync_to_gold

medium_rag_pipeline()
