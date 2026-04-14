# dags/libs/vertex_sync.py
import os
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
import vertexai
from vertexai.preview import rag
from . import config

def run_sync(topic: str, date: str, project_id: str, region: str, dataset: str, table: str, bucket_name: str, corpus_name: str):
    print(f"[V2-SYNC] Starting Defensive Vertex AI sync for topic: {topic}, date: {date}")

    # 1. Initialize & Validate (Fail Fast)
    vertexai.init(project=project_id, location=region)
    
    print(f"[INFO] Validating RAG Corpus: {corpus_name}")
    try:
        # Pre-flight check: Verify corpus exists and we have permissions
        rag.get_corpus(name=corpus_name)
        print("[SUCCESS] Corpus validated.")
    except google_exceptions.NotFound:
        msg = f"[CRITICAL ERROR] RAG Corpus NOT FOUND: {corpus_name}. Please verify the ID in Airflow Variables/config.py."
        print(msg)
        raise RuntimeError(msg)
    except google_exceptions.PermissionDenied:
        msg = (
            f"[CRITICAL ERROR] IAM PERMISSION DENIED on {corpus_name}. \n"
            "The Service Account needs 'roles/aiplatform.user' or the specific 'aiplatform.ragFiles.import' permission."
        )
        print(msg)
        raise RuntimeError(msg)
    except Exception as e:
        print(f"[ERROR] Unexpected error during validation: {e}")
        raise e

    # 2. Native BigQuery Export
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    
    clean_bucket = bucket_name.strip().replace("gs://", "").strip("/")
    staging_prefix = f"staging/vertex-rag/{date}/"
    gcs_staging_uri = f"gs://{clean_bucket}/{staging_prefix}"

    export_query = f"""
    EXPORT DATA OPTIONS(
      uri='{gcs_staging_uri}articles_*.json',
      format='JSON',
      overwrite=true
    ) AS
    SELECT 
      CONCAT(
        'Title: ', IFNULL(title, 'Untitled'), 
        '\\nAuthor: ', IFNULL(author_name, 'Unknown'), 
        '\\nDate: ', IFNULL(published_date, '{date}'), 
        '\\nURL: ', IFNULL(article_url, ''), 
        '\\n\\n', IFNULL(page_content, '')
      ) as text_content
    FROM `{table_id}`
    WHERE content_gated = FALSE 
      AND published_date = '{date}'
    """
    
    print(f"[INFO] Exporting articles from BQ to {gcs_staging_uri}")
    try:
        export_job = bq_client.query(export_query)
        export_job.result()
    except Exception as e:
        print(f"[WARN] Export failed (likely no matching rows): {e}")
        return

    # 3. Import from GCS into Managed Corpus
    print(f"[INFO] Importing JSONL files into Vertex RAG Corpus...")
    try:
        response = rag.import_files(
            corpus_name=corpus_name,
            paths=[gcs_staging_uri],
            chunk_size=1024,
            chunk_overlap=128
        )
        print(f"[SUCCESS] Import triggered. Total files processed: {response.imported_files_count}")
    except Exception as e:
        # Wrap the error with a senior data engineer's helpful context
        if "PermissionDenied" in str(e):
            print("[CRITICAL] Import failed due to late-stage PermissionDenied. Check GCS bucket access for the Vertex AI Service Agent.")
        raise RuntimeError(f"Failed in importing the RagFiles due to: {e}") from e
