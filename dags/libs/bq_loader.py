# dags/libs/bq_loader.py
import os
import json
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage

def get_bronze_articles(topic: str, target_date: str, project_id: str, bucket_name: str) -> list:
    """Reads raw article JSONs from GCS for the specified topic and date."""
    storage_client = storage.Client(project=project_id)
    bucket = storage_client.bucket(bucket_name)
    prefix = f"raw/articles/{topic}/{target_date}/"
    
    blobs = bucket.list_blobs(prefix=prefix)
    articles = []
    
    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue
        try:
            content = blob.download_as_text()
            data = json.loads(content)
            
            if "published_date" in data and data["published_date"]:
                try:
                    data["published_date"] = data["published_date"].split("T")[0]
                except Exception:
                    data["published_date"] = None

            articles.append(data)
        except Exception as e:
            print(f"[WARN] Failed to parse {blob.name}: {e}")
            
    return articles

def upsert_to_bigquery(articles: list, project_id: str, dataset: str, table: str):
    if not articles:
        print("[INFO] No articles to process.")
        return
        
    bq_client = bigquery.Client(project=project_id)
    table_id = f"{project_id}.{dataset}.{table}"
    temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"
    
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    
    load_job = bq_client.load_table_from_json(articles, temp_table_id, job_config=job_config)
    load_job.result()
    print(f"[INFO] Loaded {load_job.output_rows} rows into temp table {temp_table_id}")

    merge_query = f"""
    MERGE `{table_id}` T
    USING `{temp_table_id}` S
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
    
    merge_job = bq_client.query(merge_query)
    merge_job.result()
    print(f"[SUCCESS] Upserted rows into {table_id}")

    bq_client.delete_table(temp_table_id, not_found_ok=True)

def run_load(topic: str, date: str, project_id: str, bucket_name: str, dataset: str, table: str):
    print(f"[INFO] Starting Native Load for topic: {topic}, date: {date}")
    articles = get_bronze_articles(topic, date, project_id, bucket_name)
    print(f"[INFO] Found {len(articles)} articles in Bronze.")
    upsert_to_bigquery(articles, project_id, dataset, table)
