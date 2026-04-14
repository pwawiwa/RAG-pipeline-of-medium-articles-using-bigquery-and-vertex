import pytest
import os
from medium_rag_utils.config import ProjectConfig

def test_config_defaults():
    # Reset env vars to test defaults
    os.environ.pop("GCP_PROJECT_ID", None)
    config = ProjectConfig()
    # Note: The new safe default in config.py is 'local-test-project'
    assert config.project_id == "local-test-project"

def test_config_env_override(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "overridden-project")
    monkeypatch.setenv("GCP_REGION", "europe-west1")
    
    config = ProjectConfig()
    assert config.project_id == "overridden-project"
    assert config.region == "europe-west1"
    assert config.bronze_bucket == "medium-bronze-overridden-project"

def test_config_property_table_ids(monkeypatch):
    monkeypatch.setenv("GCP_PROJECT_ID", "p")
    monkeypatch.setenv("BQ_DATASET", "d")
    monkeypatch.setenv("BQ_TABLE", "t")
    
    config = ProjectConfig()
    assert config.silver_table_id == "p.d.t"
    assert config.gold_chunks_table_id == "p.d.gold_article_chunks"
    assert config.embedding_model_id == "p.d.embedding_model"
