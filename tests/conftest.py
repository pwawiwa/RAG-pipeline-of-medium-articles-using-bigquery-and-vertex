import pytest
import os
import sys

# Ensure dags and api are in the path
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "dags"))
sys.path.append(os.path.join(os.path.dirname(__file__), "..", "api"))

@pytest.fixture
def mock_env_vars(monkeypatch):
    """Ensure environment variables are set for tests."""
    monkeypatch.setenv("GCP_PROJECT_ID", "test-project")
    monkeypatch.setenv("GCP_REGION", "us-central1")
    monkeypatch.setenv("BRONZE_BUCKET", "test-bucket")
    monkeypatch.setenv("BQ_DATASET", "test_dataset")

@pytest.fixture
def mock_bigquery_client(mocker):
    """Fixture to mock BigQuery Client."""
    mock_client = mocker.patch("google.cloud.bigquery.Client")
    return mock_client.return_value

@pytest.fixture
def mock_storage_client(mocker):
    """Fixture to mock Storage Client."""
    mock_client = mocker.patch("google.cloud.storage.Client")
    return mock_client.return_value

@pytest.fixture(autouse=True)
def mock_airflow_variables(mocker):
    """Prevent Airflow from trying to hit a DB during tests."""
    return mocker.patch("airflow.models.Variable.get", side_effect=Exception("No DB"))

@pytest.fixture
def mock_vertex_rag(mocker):
    """Fixture to mock Vertex AI RAG services."""
    mock_rag = mocker.patch("vertexai.preview.rag")
    return mock_rag
