# dags/libs/config.py
import os
from airflow.models import Variable

def get_var(name, default):
    """Fetches variable from Airflow Variables, then Environment, then Default."""
    try:
        return Variable.get(name)
    except Exception:
        return os.getenv(name, default)

# Project Config
GCP_PROJECT_ID = get_var("GCP_PROJECT_ID", "my-playground-492309")
GCP_REGION = get_var("GCP_REGION", "asia-southeast1")

# Storage Config
BRONZE_BUCKET = get_var("BRONZE_BUCKET", "medium-bronze-my-playground-492309")

# BigQuery Config
BQ_DATASET = get_var("BQ_DATASET", "medium_pipeline")
BQ_TABLE = get_var("BQ_TABLE", "silver_medium_articles")

# Vertex AI Config
VERTEX_RAG_CORPUS_NAME = get_var("VERTEX_RAG_CORPUS_NAME", "projects/my-playground-492309/locations/asia-southeast1/ragCorpora/7679774659341189120")

# Scraper Config
IMAGE_INGEST_NODE = get_var("IMAGE_INGEST_NODE", "asia-southeast1-docker.pkg.dev/my-playground-492309/medium-repo/medium-ingest-node:latest")
TOPIC = get_var("TOPIC", "data-engineering")
