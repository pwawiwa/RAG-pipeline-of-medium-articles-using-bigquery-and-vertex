# Path: apt/tools/serve/update_vertex_rag.py
# Purpose: Exports clean BigQuery records and upserts them into Vertex AI RAG Managed Corpus
# Idempotent: true
# Dependencies: google-cloud-bigquery, vertexai, python-dotenv

import os
import argparse
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
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

if not all([GCP_PROJECT_ID, GCP_REGION, BQ_DATASET, BQ_TABLE, BRONZE_BUCKET, CORPUS_NAME]):
    print("[ERROR] Missing required environment variables from catalog.json schema")
    exit(1)

def main():
    parser = argparse.ArgumentParser(description="Sync BQ Silver data to Vertex AI Gold RAG Corpus.")
    parser.add_argument("--date", required=False, help="Date partition to sync (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[INFO] Starting Vertex AI sync for date: {target_date}")

    # 1. Fetch valid, un-gated rows from BigQuery
    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    
    query = f"""
        SELECT article_url, title, author_name, page_content, published_date
        FROM `{table_id}`
        WHERE content_gated = FALSE 
          AND published_date = '{target_date}'
    """
    
    print("[INFO] Fetching records from BigQuery...")
    query_job = bq_client.query(query)
    rows = list(query_job.result())
    
    if not rows:
        print("[INFO] No un-gated articles found for the target date. Exiting.")
        return

    # 2. Initialize Vertex AI
    vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    # Note: If the corpus doesn't exist, this job assumes it has been provisioned via Terraform or manually per standard GCP ops.
    # In Vertex RAG, files are uniquely identified by a name hash internally.
    # We will spool the text to a temporary local staging directory to utilize batch upload.
    
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(BRONZE_BUCKET)
    staging_prefix = f"staging/vertex-rag/{target_date}/"
    gcs_staging_uri = f"gs://{BRONZE_BUCKET}/{staging_prefix}"
    
    print(f"[INFO] Uploading {len(rows)} articles to GCS staging: {gcs_staging_uri}")
    for row in rows:
        slug = row['article_url'].split("/")[-1].split("?")[0]
        if not slug:
           slug = str(hash(row['article_url']))
        
        # Structure the content for optimal RAG chunking
        rag_text = f"Title: {row['title']}\n"
        rag_text += f"Author: {row['author_name']}\n"
        rag_text += f"Date: {row['published_date']}\n"
        rag_text += f"URL: {row['article_url']}\n\n"
        rag_text += row['page_content']
        
        blob = bucket.blob(f"{staging_prefix}{slug}.txt")
        blob.upload_from_string(rag_text, content_type="text/plain")

    # 3. Import path into Managed Corpus
    print(f"[INFO] Importing files into Vertex RAG Corpus: {CORPUS_NAME}")
    try:
        response = rag.import_files(
            corpus_name=CORPUS_NAME,
            paths=[gcs_staging_uri],
            chunk_size=1024,
            chunk_overlap=128
        )
        print(f"[SUCCESS] Import triggered. Uploaded {response.imported_files_count} files successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to import into Vertex RAG: {e}")
        exit(1)

if __name__ == "__main__":
    main()
