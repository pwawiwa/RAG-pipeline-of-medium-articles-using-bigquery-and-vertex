# dags/medium_rag_utils/medium_source.py
from medium_rag_utils.base_collector import BaseCollector
from medium_rag_utils.warehouse import BigQueryManager
from medium_rag_utils.config import cfg

class MediumCollector(BaseCollector):
    """Implementation of the RAG pipeline for Medium articles."""
    
    def __init__(self, topic=cfg.topic):
        self.topic = topic
        self.warehouse = BigQueryManager()

    def extract(self, date: str):
        """Metadata for extraction (actual scraping handled by K8sPodOperator)."""
        return f"gs://{cfg.bronze_bucket}/raw/articles/{self.topic}/{date}/*.json"

    def validate(self, temp_table_id: str):
        print(f"[MEDIUM] Validating records in {temp_table_id}...")
        violations = self.warehouse.validate_data_contract(temp_table_id)
        if violations:
            print(f"[WARN] Found {len(violations)} contract violations.")
        return violations

    def load(self, date: str):
        source_uri = self.extract(date)
        temp_table_id = f"{cfg.silver_table_id}_temp_{date.replace('-', '_')}"
        
        print(f"[MEDIUM] Loading {self.topic} for {date}...")
        try:
            self.warehouse.load_from_gcs(source_uri, temp_table_id)
            self.validate(temp_table_id)
            self.warehouse.merge_to_silver(temp_table_id)
            print(f"[MEDIUM][SUCCESS] Raw data merged for {date}.")
        except Exception as e:
            print(f"[MEDIUM][ERROR] Ingest failed: {e}")
            raise e

    def index(self, date: str):
        print(f"[MEDIUM] Indexing vectors for {date}...")
        self.warehouse.generate_embeddings(date)
        print(f"[MEDIUM][SUCCESS] Indexing complete.")

    def process(self, date: str):
        """High-level entry point for the pipeline."""
        self.load(date)
        self.index(date)
