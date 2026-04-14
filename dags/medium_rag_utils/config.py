# dags/medium_rag_utils/config.py
import os
from airflow.models import Variable

class ProjectConfig:
    """Centralized configuration manager using a Singleton-like pattern."""
    
    def __init__(self):
        # 1. Environment Detection (to allow safe defaults in tests/setup)
        import sys
        is_test_env = any(env in os.environ for env in ["PYTEST_CURRENT_TEST", "GITHUB_ACTIONS"]) or "pytest" in sys.modules
        is_standalone = any(s in sys.argv[0] for s in ["setup_bq.py", "scripts/"])
        is_test = is_test_env or is_standalone

        # 2. Project-level defaults
        self.project_id = self._get_var("GCP_PROJECT_ID", None)
        
        if not self.project_id:
            if is_test:
                self.project_id = "local-test-project"
            else:
                raise ValueError(
                    "CRITICAL: GCP_PROJECT_ID is missing. \n"
                    "Please ensure the 'GCP_PROJECT_ID' Variable is set in your Airflow Console."
                )

        self.region = self._get_var("GCP_REGION", "asia-southeast1")
        
        # Storage
        self.bronze_bucket = self._get_var("BRONZE_BUCKET", f"medium-bronze-{self.project_id}")
        
        # BigQuery
        self.bq_dataset = self._get_var("BQ_DATASET", "medium_pipeline")
        self.bq_table = self._get_var("BQ_TABLE", "silver_medium_articles")
        
        # Vertex AI (Legacy support)
        self.vertex_rag_corpus_name = self._get_var("VERTEX_RAG_CORPUS_NAME", "")
        
        # Scraper
        self.image_ingest_node = self._get_var("IMAGE_INGEST_NODE", f"{self.region}-docker.pkg.dev/{self.project_id}/medium-repo/medium-ingest-node:latest")
        self.topic = self._get_var("TOPIC", "data-engineering")

    def _get_var(self, name, default):
        """Fetches from Airflow Variables -> Environment -> Default."""
        # Standalone scripts shouldn't hit the Airflow DB
        if "AIRFLOW_HOME" not in os.environ and "AIRFLOW_CONFIG" not in os.environ:
             return os.getenv(name, default)

        try:
            return Variable.get(name)
        except Exception:
            # Handle uninitialized local DB or missing variables
            return os.getenv(name, default)

    @property
    def silver_table_id(self):
        return f"{self.project_id}.{self.bq_dataset}.{self.bq_table}"

    @property
    def gold_chunks_table_id(self):
        return f"{self.project_id}.{self.bq_dataset}.gold_article_chunks"
    
    @property
    def embedding_model_id(self):
        return f"{self.project_id}.{self.bq_dataset}.embedding_model"

# Instance for easy global access
cfg = ProjectConfig()
