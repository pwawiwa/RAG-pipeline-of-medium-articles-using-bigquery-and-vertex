# Path: apt/tools/transform/load_to_bigquery.py
# Purpose: Transforms raw JSON from GCS and upserts into BigQuery Silver layer
# Idempotent: true
# Dependencies: google-cloud-bigquery, google-cloud-storage, python-dotenv

import os
import json
import argparse
from datetime import datetime
from google.cloud import bigquery
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")
BRONZE_BUCKET = os.getenv("BRONZE_BUCKET")
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")

if not all([BQ_DATASET, BQ_TABLE, BRONZE_BUCKET, GCP_PROJECT_ID]):
    print("[ERROR] Missing required environment variables from catalog.json schema")
    exit(1)

def get_bronze_articles(topic: str, target_date: str) -> list:
    """Reads raw article JSONs from GCS for the specified topic and date."""
    storage_client = storage.Client(project=GCP_PROJECT_ID)
    bucket = storage_client.bucket(BRONZE_BUCKET)
    prefix = f"raw/articles/{topic}/{target_date}/"
    
    blobs = bucket.list_blobs(prefix=prefix)
    articles = []
    
    for blob in blobs:
        if not blob.name.endswith(".json"):
            continue
        try:
            content = blob.download_as_text()
            data = json.loads(content)
            
            # Clean/validate types per schema_context
            if "published_date" in data and data["published_date"]:
                # Cast standard ISO dates to strictly %Y-%m-%d for BQ DATE partition
                try:
                    data["published_date"] = data["published_date"].split("T")[0]
                except Exception:
                    data["published_date"] = None

            articles.append(data)
        except Exception as e:
            print(f"[WARN] Failed to parse {blob.name}: {e}")
            
    return articles

def upsert_to_bigquery(articles: list, bq_client: bigquery.Client):
    if not articles:
        print("[INFO] No articles to process.")
        return
        
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
    temp_table_id = f"{table_id}_temp_{int(datetime.now().timestamp())}"
    
    # 1. Load data into a temporary table
    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
    )
    
    load_job = bq_client.load_table_from_json(articles, temp_table_id, job_config=job_config)
    load_job.result()  # Wait for the job to complete
    print(f"[INFO] Loaded {load_job.output_rows} rows into temp table {temp_table_id}")

    # 2. Execute MERGE statement for idempotent upsert based on article_url
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
    print(f"[SUCCESS] Upserted rows from temp table into {table_id}")

    # 3. Clean up temp table
    bq_client.delete_table(temp_table_id, not_found_ok=True)
    print(f"[INFO] Dropped temp table {temp_table_id}")


def main():
    parser = argparse.ArgumentParser(description="Transform and load JSONs from GCS to BQ Silver.")
    parser.add_argument("--topic", required=True, help="Topic slug (e.g. data-engineering)")
    parser.add_argument("--date", required=False, help="Date partition (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()

    target_date = args.date if args.date else datetime.utcnow().strftime("%Y-%m-%d")
    print(f"[INFO] Starting Load for topic: {args.topic}, date: {target_date}")

    bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    
    articles = get_bronze_articles(args.topic, target_date)
    print(f"[INFO] Found {len(articles)} articles in Bronze.")
    
    upsert_to_bigquery(articles, bq_client)

if __name__ == "__main__":
    main()
