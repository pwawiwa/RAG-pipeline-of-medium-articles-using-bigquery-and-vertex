# dags/libs/bq_loader.py
import os
from datetime import datetime
from google.cloud import bigquery
from . import config

def upsert_to_bigquery(topic: str, date: str, project_id: str, bucket_name: str, dataset: str, table: str):
    """Loads raw JSON article blobs from GCS to BQ via native LOAD and MERGE."""
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    
    # Use a unique temp table name tied to the task run
    temp_table_id = f"{project_id}.{dataset}.{table}_temp_{date.replace('-', '_')}"
    
    # 1. Native Load from GCS
    # Scraper stores individual JSON objects. BQ can load these if treated as Newline Delimited.
    source_uri = f"gs://{bucket_name}/raw/articles/{topic}/{date}/*.json"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    
    print(f"[INFO] Triggering BQ LOAD from {source_uri} to {temp_table_id}")
    try:
        load_job = bq_client.load_table_from_uri(source_uri, temp_table_id, job_config=job_config)
        load_job.result() # Wait for completion
        print(f"[SUCCESS] Loaded {load_job.output_rows} raw records into temp table.")
    except Exception as e:
        print(f"[WARN] No records found or load failed: {e}")
        return

    # 2. SQL MERGE (Handles transformation and upsert)
    # We clean the published_date to YYYY-MM-DD during the merge
    merge_query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT 
        article_url, 
        topic, 
        title, 
        author_name, 
        page_content, 
        first_line, 
        SAFE_CAST(SPLIT(published_date, 'T')[OFFSET(0)] AS STRING) as published_date,
        clap_count, 
        comments_count, 
        hero_image_url, 
        author_avatar_url, 
        ingested_at, 
        content_hash, 
        content_gated
      FROM `{temp_table_id}`
    ) S
    ON T.article_url = S.article_url
    WHEN MATCHED THEN
      UPDATE SET 
        topic = S.topic,
        title = S.title,
        author_name = S.author_name,
        page_content = S.page_content,
        first_line = S.first_line,
        published_date = S.published_date,
        clap_count = S.clap_count,
        comments_count = S.comments_count,
        hero_image_url = S.hero_image_url,
        author_avatar_url = S.author_avatar_url,
        ingested_at = S.ingested_at,
        content_hash = S.content_hash,
        content_gated = S.content_gated
    WHEN NOT MATCHED THEN
      INSERT (article_url, topic, title, author_name, page_content, first_line, published_date, clap_count, comments_count, hero_image_url, author_avatar_url, ingested_at, content_hash, content_gated)
      VALUES (S.article_url, S.topic, S.title, S.author_name, S.page_content, S.first_line, S.published_date, S.clap_count, S.comments_count, S.hero_image_url, S.author_avatar_url, S.ingested_at, S.content_hash, S.content_gated)
    """
    
    print(f"[INFO] Executing MERGE into {table_id}")
    query_job = bq_client.query(merge_query)
    query_job.result()
    print(f"[SUCCESS] Upsert complete.")

    # Cleanup
    bq_client.delete_table(temp_table_id, not_found_ok=True)

def run_load(topic: str, date: str, project_id: str, bucket_name: str, dataset: str, table: str):
    print(f"[V2-LOAD] Starting Optimized Load for topic: {topic}, date: {date}")
    upsert_to_bigquery(topic, date, project_id, bucket_name, dataset, table)
