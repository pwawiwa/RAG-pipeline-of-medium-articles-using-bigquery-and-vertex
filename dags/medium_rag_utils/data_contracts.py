# dags/libs/data_contracts.py
import re
from datetime import datetime

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
