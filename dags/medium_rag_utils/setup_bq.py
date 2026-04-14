# dags/libs/setup_bq.py
import os
from google.cloud import bigquery
from . import config

def setup_infrastructure(project_id: str, location: str, dataset: str):
    """
    Sets up the BigQuery Cloud Connection and the Embedding Model.
    """
    client = bigquery.Client(project=project_id)
    connection_id = "vertex-ai-connection"
    connection_path = f"{project_id}.{location}.{connection_id}"
    model_id = f"{project_id}.{dataset}.embedding_model"

    print(f"[SETUP] Creating Cloud Connection: {connection_id}...")
    # SQL DDL for connection (Requires specific permissions)
    # Note: If this fails, the user may need to create it manually in the Console.
    try:
        connection_query = f"""
        CREATE CONNECTION `{connection_path}`
        REMOTE_TYPE = "CLOUD_RESOURCE"
        """
        # Connection creation via DDL is sometimes restricted; 
        # normally done via 'bq mk' or Console.
        # client.query(connection_query).result()
        print("[INFO] Connection creation DDL generated. Handled via infrastructure-as-code.")
    except Exception as e:
        print(f"[WARN] Connection might already exist or needs manual creation: {e}")

    print(f"[SETUP] Creating Remote Embedding Model: {model_id}...")
    model_query = f"""
    CREATE OR REPLACE MODEL `{model_id}`
    REMOTE WITH CONNECTION `{connection_path}`
    OPTIONS(ENDPOINT = 'text-embedding-004');
    """
    try:
        client.query(model_query).result()
        print(f"[SUCCESS] Model {model_id} created.")
    except Exception as e:
        print(f"[ERROR] Failed to create model: {e}")
        print("[HINT] Ensure the BigQuery Connection has 'Vertex AI User' role.")

    print(f"[SETUP] Creating Gold Article Chunks Table...")
    chunks_table_id = f"{project_id}.{dataset}.gold_article_chunks"
    create_chunks_query = f"""
    CREATE TABLE IF NOT EXISTS `{chunks_table_id}` (
      article_url STRING,
      chunk_id STRING,
      chunk_text STRING,
      embedding ARRAY<FLOAT64>,
      ingested_at TIMESTAMP
    )
    PARTITION BY TIMESTAMP_TRUNC(ingested_at, DAY);
    """
    client.query(create_chunks_query).result()
    print(f"[SUCCESS] Table {chunks_table_id} ready.")

if __name__ == "__main__":
    from . import config
    setup_infrastructure(config.cfg.project_id, config.cfg.region, config.cfg.bq_dataset)
