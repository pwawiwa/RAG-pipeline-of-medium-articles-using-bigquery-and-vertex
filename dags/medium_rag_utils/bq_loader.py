# dags/libs/bq_loader.py
import os
from datetime import datetime
from google.cloud import bigquery
from . import config
from . import data_contracts

def upsert_to_bigquery(topic: str, date: str, project_id: str, bucket_name: str, dataset: str, table: str):
    """Loads raw JSON article blobs from GCS to BQ via native LOAD and MERGE."""
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    temp_table_id = f"{project_id}.{dataset}.{table}_temp_{date.replace('-', '_')}"
    
    source_uri = f"gs://{bucket_name}/raw/articles/{topic}/{date}/*.json"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    
    print(f"[INFO] Triggering BQ LOAD from {source_uri}")
    try:
        load_job = bq_client.load_table_from_uri(source_uri, temp_table_id, job_config=job_config)
        load_job.result()
    except Exception as e:
        print(f"[WARN] No records found / load failed: {e}")
        return

    # --- Data Contract Validation ---
    print(f"[CONTRACT] Validating data quality in {temp_table_id}")
    validation_query = data_contracts.get_validation_query(temp_table_id)
    violations = list(bq_client.query(validation_query).result())
    
    if violations:
        print(f"[CRITICAL] Data Contract Violation! Found {len(violations)} bad records.")
        for v in violations[:5]:
            print(f" > URL: {v.article_url} | Error: {v.violation}")
        # We still merge, but we log the failures. In a strict system, we would raise an Exception here.

    # 2. SQL MERGE
    merge_query = f"""
    MERGE `{table_id}` T
    USING (
      SELECT 
        article_url, topic, title, author_name, page_content, first_line, 
        SAFE_CAST(SPLIT(published_date, 'T')[OFFSET(0)] AS STRING) as published_date,
        clap_count, comments_count, hero_image_url, author_avatar_url, ingested_at, content_hash, content_gated
      FROM `{temp_table_id}`
    ) S
    ON T.article_url = S.article_url
    WHEN MATCHED THEN
      UPDATE SET 
        topic = S.topic, title = S.title, page_content = S.page_content, published_date = S.published_date,
        clap_count = S.clap_count, ingested_at = S.ingested_at, content_hash = S.content_hash
    WHEN NOT MATCHED THEN
      INSERT (article_url, topic, title, author_name, page_content, first_line, published_date, clap_count, comments_count, hero_image_url, author_avatar_url, ingested_at, content_hash, content_gated)
      VALUES (S.article_url, S.topic, S.title, S.author_name, S.page_content, S.first_line, S.published_date, S.clap_count, S.comments_count, S.hero_image_url, S.author_avatar_url, S.ingested_at, S.content_hash, S.content_gated)
    """
    bq_client.query(merge_query).result()
    print(f"[SUCCESS] Silver Layer updated.")
    bq_client.delete_table(temp_table_id, not_found_ok=True)

def generate_vector_index(topic: str, date: str, project_id: str, dataset: str, table: str):
    """Chunks the silver articles and generates embeddings in the gold layer."""
    bq_client = bigquery.Client(project=project_id)
    silver_table = f"{project_id}.{dataset}.{table}"
    gold_chunks_table = f"{project_id}.{dataset}.gold_article_chunks"
    model_id = f"{project_id}.{dataset}.embedding_model"

    # 1. SQL-based Chunking (Splits text into 800-char blocks)
    # 2. ML.GENERATE_EMBEDDING (Calls Remote Model)
    # 3. Insert into Gold
    chunk_and_embed_query = f"""
    INSERT INTO `{gold_chunks_table}` (article_url, chunk_id, chunk_text, embedding, ingested_at)
    WITH chunks AS (
      SELECT 
        article_url,
        GENERATE_UUID() as chunk_id,
        chunk_text,
        CURRENT_TIMESTAMP() as ingested_at
      FROM `{silver_table}`,
      UNNEST(REGEXP_EXTRACT_ALL(page_content, r'.{{1,800}}(?:\\s|$)')) as chunk_text
      WHERE published_date = '{date}'
    )
    SELECT article_url, chunk_id, chunk_text, ml_generate_embedding_result, ingested_at
    FROM ML.GENERATE_EMBEDDING(
      MODEL `{model_id}`,
      (SELECT * FROM chunks),
      STRUCT(TRUE AS flatten_json_output)
    )
    """
    
    print(f"[VECTOR] Generating embeddings for articles from {date}...")
    try:
        query_job = bq_client.query(chunk_and_embed_query)
        query_job.result()
        print(f"[SUCCESS] Vector Search index populated.")
    except Exception as e:
        print(f"[ERROR] Embedding generation failed: {e}")
        raise e

def run_load(topic: str, date: str, project_id: str, bucket_name: str, dataset: str, table: str):
    print(f"[V3-LOAD] Starting Load + Contract Validation for date: {date}")
    upsert_to_bigquery(topic, date, project_id, bucket_name, dataset, table)
    generate_vector_index(topic, date, project_id, dataset, table)
