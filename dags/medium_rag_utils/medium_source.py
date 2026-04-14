import logging
from medium_rag_utils.base_collector import BaseCollector
from medium_rag_utils.warehouse import BigQueryManager
from medium_rag_utils.config import cfg

logger = logging.getLogger(__name__)

class MediumCollector(BaseCollector):
    """Implementation of the RAG pipeline for Medium articles."""
    
    def __init__(self, topic=cfg.topic):
        self.topic = topic
        self.warehouse = BigQueryManager()

    def extract(self, date: str):
        """Metadata for extraction (actual scraping handled by K8sPodOperator)."""
        return f"gs://{cfg.bronze_bucket}/raw/articles/{self.topic}/{date}/*.json"

    def validate(self, temp_table_id: str):
        logger.info(f"Validating records in {temp_table_id}...")
        violations = self.warehouse.validate_data_contract(temp_table_id)
        if violations:
            logger.warning(f"Found {len(violations)} contract violations.")
        return violations

    def load(self, date: str):
        source_uri = self.extract(date)
        temp_table_id = f"{cfg.silver_table_id}_temp_{date.replace('-', '_')}"
        
        logger.info(f"Loading {self.topic} for {date}...")
        try:
            self.warehouse.load_from_gcs(source_uri, temp_table_id)
            self.validate(temp_table_id)
            self.warehouse.merge_to_silver(temp_table_id)
            logger.info(f"Raw data merged for {date}.")
        except Exception as e:
            logger.error(f"Ingest failed: {e}")
            raise e

    def index(self, date: str):
        logger.info(f"Indexing vectors for {date}...")
        self.warehouse.generate_embeddings(date)
        logger.info(f"Indexing complete.")

    def process(self, date: str):
        """High-level entry point for the pipeline."""
        self.load(date)
        self.index(date)
