import os
import logging
from google.cloud import bigquery
from medium_rag_utils import config

logger = logging.getLogger(__name__)

def setup_infrastructure(project_id: str, location: str, dataset: str):
    """
    Sets up the BigQuery Cloud Connection, the Embedding Model, and the Gold table.
    """
    client = bigquery.Client(project=project_id)
    connection_id = "vertex-ai-connection"
    connection_path = f"projects/{project_id}/locations/{location}/connections/{connection_id}"
    model_id = f"{project_id}.{dataset}.embedding_model"

    logger.info(f"--- [1/4] Establishing Cloud Connection: {connection_id} ---")
    # Connection creation usually requires specific roles, but we'll try it.
    try:
        # Check if exists first
        from google.cloud import bigquery_connection_v1 as bq_conn  # pylint: disable=no-name-in-module
        conn_client = bq_conn.ConnectionServiceClient()
        
        try:
            conn_client.get_connection(name=connection_path)
            logger.info("Connection already exists.")
        except Exception:
            logger.info("Creating new Cloud Resource connection...")
            parent = f"projects/{project_id}/locations/{location}"
            connection_obj = bq_conn.Connection(
                cloud_resource=bq_conn.CloudResourceProperties()
            )
            conn_client.create_connection(
                parent=parent,
                connection_id=connection_id,
                connection=connection_obj
            )
            logger.info("Connection created.")

        # Get Service Account for Diagnostic Output
        conn = conn_client.get_connection(name=connection_path)
        sa_id = conn.cloud_resource.service_account_id
        print(f"\n[DIAGNOSTIC] Connection Service Account: {sa_id}")
        print(f"[DIAGNOSTIC] ACTION REQUIRED: Grant this SA the 'Vertex AI User' role in IAM.\n")

    except Exception as e:
        logger.warning(f"Connection setup failed or restricted: {e}")

    logger.info(f"--- [2/4] Provisioning BigQuery Dataset ---")
    dataset_ref = bigquery.DatasetReference(project_id, dataset)
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset} exists.")
    except Exception:
        ds = bigquery.Dataset(dataset_ref)
        ds.location = location
        client.create_dataset(ds)
        logger.info(f"Dataset {dataset} created.")

    logger.info(f"--- [3/4] Creating Remote Embedding Model: {model_id} ---")
    # Note: We use the dotted path format for SQL
    sql_connection_path = f"{project_id}.{location}.{connection_id}"
    model_query = f"""
    CREATE OR REPLACE MODEL `{model_id}`
    REMOTE WITH CONNECTION `{sql_connection_path}`
    OPTIONS(ENDPOINT = 'text-embedding-004');
    """
    try:
        client.query(model_query).result()
        logger.info(f"Model {model_id} created/updated.")
    except Exception as e:
        logger.error(f"Failed to create model: {e}")
        return False

    logger.info(f"--- [4/4] Creating Gold Article Chunks Table ---")
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
    return True

def test_embedding_model(project_id: str, dataset: str):
    """Performs a dry run to verify the model actually works."""
    client = bigquery.Client(project=project_id)
    model_id = f"{project_id}.{dataset}.embedding_model"
    
    logger.info(f"--- Validating Model: {model_id} ---")
    test_query = f"""
    SELECT * FROM ML.GENERATE_EMBEDDING(
      MODEL `{model_id}`,
      (SELECT 'Hello World' as content)
    )
    """
    try:
        client.query(test_query).result()
        print("\n✅ SUCCESS: The BigQuery AI infrastructure is operational!")
        return True
    except Exception as e:
        print(f"\n❌ FAILURE: Model validation failed: {e}")
        print("HINT: This usually means the Connection Service Account is missing IAM permissions.")
        return False

if __name__ == "__main__":
    from medium_rag_utils import config
    c = config.ProjectConfig()
    
    success = setup_infrastructure(c.project_id, c.region, c.bq_dataset)
    if success:
        test_embedding_model(c.project_id, c.bq_dataset)
