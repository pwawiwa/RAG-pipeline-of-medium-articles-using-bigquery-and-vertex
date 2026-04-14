# Path: apt/tools/serve/update_vertex_rag.py
# Purpose: Exports clean BigQuery records and upserts them into Vertex AI RAG Managed Corpus
# Idempotent: true

import os
import argparse
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from google.api_core import exceptions as google_exceptions
import vertexai
from vertexai.preview import rag
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
CORPUS_NAME = os.getenv("VERTEX_RAG_CORPUS_NAME")

def main():
    parser = argparse.ArgumentParser(description="Sync BQ Silver data to Vertex AI Gold RAG Corpus (Optimized & Defensive).")
    parser.add_argument("--date", required=False, help="Date partition to sync (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.utcnow().strftime("%Y-%m-%d")
    
    if not all([GCP_PROJECT_ID, GCP_REGION, BQ_DATASET, BQ_TABLE, BRONZE_BUCKET, CORPUS_NAME]):
        print("[ERROR] Missing required environment variables. Verify .env or catalog.json")
        exit(1)

    print(f"[DEFENSIVE-SYNC] Starting Vertex AI sync for date: {target_date}")

    # 1. Initialize & Validate (Fail Fast)
    vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)
    
    print(f"[INFO] Validating RAG Corpus: {CORPUS_NAME}")
    try:
        rag.get_corpus(name=CORPUS_NAME)
        print("[SUCCESS] Corpus validated.")
    except google_exceptions.NotFound:
        print(f"[CRITICAL ERROR] RAG Corpus NOT FOUND: {CORPUS_NAME}. Check VERTEX_RAG_CORPUS_NAME env var.")
        exit(1)
    except google_exceptions.PermissionDenied:
        print(f"[CRITICAL ERROR] IAM PERMISSION DENIED on {CORPUS_NAME}. Service account needs aiplatform.user role.")
        exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected validation error: {e}")
        exit(1)

    # 2. Native BigQuery Export
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    clean_bucket = BRONZE_BUCKET.strip().replace("gs://", "").strip("/")
    staging_prefix = f"staging/vertex-rag/{target_date}/"
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
        '\\nDate: ', IFNULL(published_date, '{target_date}'), 
        '\\nURL: ', IFNULL(article_url, ''), 
        '\\n\\n', IFNULL(page_content, '')
      ) as text_content
    FROM `{table_id}`
    WHERE content_gated = FALSE 
      AND published_date = '{target_date}'
    """
    
    print(f"[INFO] Exporting articles from BQ to {gcs_staging_uri}...")
    try:
        export_job = bq_client.query(export_query)
        export_job.result()
    except Exception as e:
        print(f"[WARN] Export failed (likely no rows for this date): {e}")
        return

    # 3. Bulk Import
    print(f"[INFO] Importing files into Vertex RAG Corpus...")
    try:
        response = rag.import_files(
            corpus_name=CORPUS_NAME,
            paths=[gcs_staging_uri],
            chunk_size=1024,
            chunk_overlap=128
        )
        print(f"[SUCCESS] Import triggered. Processed {response.imported_files_count} files.")
    except Exception as e:
        print(f"[ERROR] Failed to import into Vertex RAG: {e}")
        exit(1)

if __name__ == "__main__":
    main()
