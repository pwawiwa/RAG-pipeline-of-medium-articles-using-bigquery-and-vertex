# dags/medium_rag_utils/warehouse.py
from google.cloud import bigquery
from medium_rag_utils.config import cfg
from medium_rag_utils.data_contracts import get_validation_query

class BigQueryManager:
    """Manages all Data Warehouse operations including DDL, loading, and indexing."""
    
    def __init__(self, project_id=cfg.project_id):
        self.client = bigquery.Client(project=project_id)

    def setup_infrastructure(self):
        """Standardizes table and model creation with partitioning and clustering for scale."""
        print(f"[WAREHOUSE] Setting up optimized infrastructure for {cfg.project_id}...")
        
        # 1. Create Silver Table (Partitioned & Clustered)
        create_silver_query = f"""
        CREATE TABLE IF NOT EXISTS `{cfg.silver_table_id}` (
          article_url STRING NOT NULL,
          topic STRING,
          title STRING,
          author_name STRING,
          page_content STRING,
          first_line STRING,
          published_date DATE,
          clap_count INT64,
          comments_count INT64,
          hero_image_url STRING,
          author_avatar_url STRING,
          ingested_at TIMESTAMP,
          content_hash STRING,
          content_gated BOOLEAN
        )
        PARTITION BY published_date
        CLUSTER BY topic, article_url;
        """
        self.client.query(create_silver_query).result()

        # 2. Create Gold Chunks Table (Partitioned by ingestion)
        create_chunks_query = f"""
        CREATE TABLE IF NOT EXISTS `{cfg.gold_chunks_table_id}` (
          article_url STRING NOT NULL,
          chunk_id STRING,
          chunk_text STRING,
          embedding ARRAY<FLOAT64>,
          ingested_at TIMESTAMP
        )
        PARTITION BY TIMESTAMP_TRUNC(ingested_at, DAY)
        CLUSTER BY article_url;
        """
        self.client.query(create_chunks_query).result()

        # 3. Create Embedding Model
        model_query = f"""
        CREATE OR REPLACE MODEL `{cfg.embedding_model_id}`
        REMOTE WITH CONNECTION `{cfg.project_id}.{cfg.region}.vertex-ai-connection`
        OPTIONS(ENDPOINT = 'text-embedding-004');
        """
        try:
            self.client.query(model_query).result()
        except Exception as e:
            print(f"[WAREHOUSE][WARN] Connection setup might be required: {e}")

    def load_from_gcs(self, source_uri: str, temp_table_id: str):
        """Direct bulk load from GCS to a temporary table."""
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            autodetect=True,
            write_disposition="WRITE_TRUNCATE",
        )
        load_job = self.client.load_table_from_uri(source_uri, temp_table_id, job_config=job_config)
        load_job.result()

    def validate_data_contract(self, temp_table_id: str):
        """Runs the data contract SQL validation."""
        query = get_validation_query(temp_table_id)
        violations = list(self.client.query(query).result())
        return violations

    def merge_to_silver(self, temp_table_id: str):
        """UPSERT from temp table to production silver table (Handles Date Casting)."""
        merge_query = f"""
        MERGE `{cfg.silver_table_id}` T
        USING (
          SELECT 
            article_url, topic, title, author_name, page_content, first_line, 
            SAFE.PARSE_DATE('%Y-%m-%d', SPLIT(published_date, 'T')[OFFSET(0)]) as published_date,
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
        self.client.query(merge_query).result()
        self.client.delete_table(temp_table_id, not_found_ok=True)

    def generate_embeddings(self, date: str):
        """Chunks and generates vectors for the specified date. Logic is now idempotent."""
        
        # 1. Clean existing chunks for these articles to prevent bloat
        cleanup_query = f"""
        DELETE FROM `{cfg.gold_chunks_table_id}`
        WHERE article_url IN (
            SELECT article_url FROM `{cfg.silver_table_id}` 
            WHERE published_date = '{date}'
        )
        """
        self.client.query(cleanup_query).result()

        # 2. Insert fresh chunks
        insert_query = f"""
        INSERT INTO `{cfg.gold_chunks_table_id}` (article_url, chunk_id, chunk_text, embedding, ingested_at)
        WITH chunks AS (
          SELECT 
            article_url,
            GENERATE_UUID() as chunk_id,
            chunk_text,
            CURRENT_TIMESTAMP() as ingested_at
          FROM `{cfg.silver_table_id}`,
          UNNEST(REGEXP_EXTRACT_ALL(page_content, r'.{{1,800}}(?:\\s|$)')) as chunk_text
          WHERE published_date = '{date}'
        )
        SELECT article_url, chunk_id, chunk_text, ml_generate_embedding_result, ingested_at
        FROM ML.GENERATE_EMBEDDING(
          MODEL `{cfg.embedding_model_id}`,
          (SELECT * FROM chunks),
          STRUCT(TRUE AS flatten_json_output)
        )
        """
        self.client.query(insert_query).result()
