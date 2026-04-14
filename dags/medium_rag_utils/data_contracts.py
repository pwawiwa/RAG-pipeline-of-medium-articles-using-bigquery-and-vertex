import re
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class DataContractError(Exception):
    """Raised when a record violates the data contract."""
    pass

def validate_medium_article(record: dict):
    """
    Validates a single article record against the production data contract.
    """
    # 1. Non-Null Checks
    required_fields = ["article_url", "title", "page_content", "published_date"]
    for field in required_fields:
        if not record.get(field):
            raise DataContractError(f"Missing required field: {field}")

    # 2. Content Quality Checks
    content = record.get("page_content", "")
    if len(content) < 200:
        raise DataContractError(f"Article content too short ({len(content)} chars). Likely gated or failed scrape.")

    # 3. Format Checks
    url = record.get("article_url", "")
    if not url.startswith("http"):
        raise DataContractError(f"Invalid article URL: {url}")

    # 4. Date Validation
    pub_date = record.get("published_date")
    try:
        # Expected format: YYYY-MM-DD (after cleaning in BQ loader)
        datetime.strptime(pub_date, "%Y-%m-%d")
    except ValueError:
         raise DataContractError(f"Invalid date format: {pub_date}. Expected YYYY-MM-DD.")

    return True

def get_validation_query(table_id: str):
    """
    Returns a SQL query that identifies rows violating the data contract.
    Used for bulk validation in BigQuery.
    """
    return f"""
    SELECT article_url, 'Content too short' as violation
    FROM `{table_id}`
    WHERE LENGTH(page_content) < 200
    UNION ALL
    SELECT article_url, 'Missing title' as violation
    FROM `{table_id}`
    WHERE title IS NULL OR title = ''
    UNION ALL
    SELECT article_url, 'Invalid URL' as violation
    FROM `{table_id}`
    WHERE NOT article_url LIKE 'http%'
    """

def get_bq_schema():
    """Returns the explicit BigQuery schema for the silver table."""
    from google.cloud import bigquery
    return [
        bigquery.SchemaField("article_url", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("topic", "STRING"),
        bigquery.SchemaField("title", "STRING"),
        bigquery.SchemaField("author_name", "STRING"),
        bigquery.SchemaField("page_content", "STRING"),
        bigquery.SchemaField("first_line", "STRING"),
        bigquery.SchemaField("published_date", "STRING"), # Raw date from JSON
        bigquery.SchemaField("clap_count", "INTEGER"),
        bigquery.SchemaField("comments_count", "INTEGER"),
        bigquery.SchemaField("hero_image_url", "STRING"),
        bigquery.SchemaField("author_avatar_url", "STRING"),
        bigquery.SchemaField("ingested_at", "TIMESTAMP"),
        bigquery.SchemaField("content_hash", "STRING"),
        bigquery.SchemaField("content_gated", "BOOLEAN"),
    ]
