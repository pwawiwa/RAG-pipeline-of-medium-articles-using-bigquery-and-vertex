import os
import logging
from google.cloud import bigquery
from medium_rag_utils import config

logger = logging.getLogger(__name__)

def setup_infrastructure(project_id: str, location: str, dataset: str):
    """
    Sets up the BigQuery Cloud Connection and the Embedding Model.
    """
    client = bigquery.Client(project=project_id)
    connection_id = "vertex-ai-connection"
    connection_path = f"{project_id}.{location}.{connection_id}"
    model_id = f"{project_id}.{dataset}.embedding_model"

    logger.info(f"Creating Cloud Connection: {connection_id}...")
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
        logger.info("Connection creation DDL generated. Handled via infrastructure-as-code.")
    except Exception as e:
        logger.warning(f"Connection might already exist or needs manual creation: {e}")

    logger.info(f"Creating Remote Embedding Model: {model_id}...")
    model_query = f"""
    CREATE OR REPLACE MODEL `{model_id}`
    REMOTE WITH CONNECTION `{connection_path}`
    OPTIONS(ENDPOINT = 'text-embedding-004');
    """
    try:
        client.query(model_query).result()
        logger.info(f"Model {model_id} created.")
    except Exception as e:
        logger.error(f"Failed to create model: {e}")
        logger.info("HINT: Ensure the BigQuery Connection has 'Vertex AI User' role.")

    logger.info(f"Creating Gold Article Chunks Table...")
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
    logger.info(f"Table {chunks_table_id} ready.")

if __name__ == "__main__":
    from medium_rag_utils import config
    setup_infrastructure(config.cfg.project_id, config.cfg.region, config.cfg.bq_dataset)
