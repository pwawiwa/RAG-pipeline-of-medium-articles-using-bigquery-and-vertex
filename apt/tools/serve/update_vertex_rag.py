# Path: apt/tools/serve/update_vertex_rag.py
# Purpose: Exports clean BigQuery records and triggers Vector Search indexing
# Idempotent: true

import os
import argparse
from datetime import datetime
from google.cloud import bigquery
from dotenv import load_dotenv

# Note: Standalone tools use direct environment variables or local imports
load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
BQ_DATASET = os.getenv("BQ_DATASET")
BQ_TABLE = os.getenv("BQ_TABLE")

def main():
    parser = argparse.ArgumentParser(description="Populate BigQuery Vector Search index for Medium articles.")
    parser.add_argument("--date", required=False, help="Date partition to sync (YYYY-MM-DD), defaults to today")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.utcnow().strftime("%Y-%m-%d")
    
    if not all([GCP_PROJECT_ID, GCP_REGION, BQ_DATASET, BQ_TABLE]):
        print("[ERROR] Missing required environment variables. Verify .env")
        exit(1)

    print(f"[REFACTOR-SYNC] Starting BigQuery Vector Sync for date: {target_date}")

    # Use the same optimized logic as the DAG
    from dags.medium_rag_utils.bq_loader import generate_vector_index
    
    try:
        generate_vector_index(
            topic="data-engineering", # Default topic
            date=target_date,
            project_id=GCP_PROJECT_ID,
            dataset=BQ_DATASET,
            table=BQ_TABLE
        )
        print(f"[SUCCESS] Vector Search index updated for {target_date}.")
    except Exception as e:
        print(f"[ERROR] Sync failed: {e}")
        exit(1)

if __name__ == "__main__":
    main()
