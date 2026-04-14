import pytest
from fastapi.testclient import TestClient
from main import app, rag_service

client = TestClient(app)

def test_healthz():
    response = client.get("/healthz")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}

def test_ask_endpoint_unauthorized():
    response = client.post("/ask", json={"query": "test"})
    # Should be 401/403 if API key is missing
    assert response.status_code == 403

def test_ask_endpoint_authorized(mocker, monkeypatch):
    # 1. Mock the API key verification
    monkeypatch.setenv("LOCAL_API_KEY", "test_key")
    
    # 2. Mock the RagService
    mock_rag = mocker.patch("main.rag_service")
    mock_rag.generate_answer.return_value = {
        "answer": "Mocked Answer",
        "sources": ["source1", "source2"]
    }
    
    response = client.post(
        "/ask", 
        json={"query": "What is data engineering?"},
        headers={"X-API-Key": "test_key"}
    )
    
    assert response.status_code == 200
    data = response.json()
    assert data["answer"] == "Mocked Answer"
    assert data["sources"] == ["source1", "source2"]
