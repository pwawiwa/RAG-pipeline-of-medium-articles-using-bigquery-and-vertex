# dags/libs/vertex_sync.py
import os
from google.cloud import bigquery
from google.cloud import storage
import vertexai
from vertexai.preview import rag
from vertexai.preview.generative_models import GenerativeModel, Tool

def run_sync(topic: str, date: str, project_id: str, region: str, dataset: str, table: str, bucket_name: str, corpus_name: str):
    print(f"[V3-SYNC] Starting Native Vertex AI sync for topic: {topic}, date: {date}")

    # 1. Fetch valid, un-gated rows from BigQuery
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    
    query = f"""
        SELECT article_url, title, author_name, page_content, published_date
        FROM `{table_id}`
        WHERE content_gated = FALSE 
          AND published_date = '{date}'
    """
    
    print("[INFO] Fetching records from BigQuery...")
    query_job = bq_client.query(query)
    rows = list(query_job.result())
    
    if not rows:
        print("[INFO] No un-gated articles found for the target date. Exiting.")
        return

    # 2. Initialize Vertex AI
    vertexai.init(project=project_id, location=region)
    
    storage_client = storage.Client(project=project_id)
    clean_bucket = bucket_name.strip().replace("gs://", "").strip("/")
    bucket = storage_client.bucket(clean_bucket)
    
    staging_prefix = f"staging/vertex-rag/{date}/"
    gcs_staging_uri = f"gs://{clean_bucket}/{staging_prefix}"
    
    print(f"[V3-SYNC] Using GCS URI: {gcs_staging_uri}")
    print(f"[V3-SYNC] Uploading {len(rows)} articles to GCS staging...")
    
    for row in rows:
        slug = row['article_url'].split("/")[-1].split("?")[0]
        if not slug:
           slug = str(hash(row['article_url']))
        
        rag_text = f"Title: {row['title']}\n"
        rag_text += f"Author: {row['author_name']}\n"
        rag_text += f"Date: {row['published_date']}\n"
        rag_text += f"URL: {row['article_url']}\n\n"
        rag_text += row['page_content']
        
        blob = bucket.blob(f"{staging_prefix}{slug}.txt")
        blob.upload_from_string(rag_text, content_type="text/plain")

    # 3. Import path into Managed Corpus
    print(f"[INFO] Importing files into Vertex RAG Corpus: {corpus_name}")
    try:
        response = rag.import_files(
            corpus_name=corpus_name,
            paths=[gcs_staging_uri],
            chunk_size=1024,
            chunk_overlap=128
        )
        print(f"[SUCCESS] Import triggered. Uploaded {response.imported_files_count} files successfully.")
    except Exception as e:
        print(f"[ERROR] Failed to import into Vertex RAG: {e}")
        raise e
