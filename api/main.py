# Path: api/main.py
# Purpose: Cloud Run FastAPI Entrypoint with Secret Manager Header Auth
# Idempotent: true
# Dependencies: fastapi, uvicorn, google-cloud-secret-manager, rag_service.py

import os
from fastapi import FastAPI, Depends, HTTPException, Security
from fastapi.security.api_key import APIKeyHeader
from pydantic import BaseModel
from google.cloud import secretmanager
from rag_service import RagService

app = FastAPI(title="Medium RAG API", version="1.0.0")

API_KEY_NAME = "X-API-Key"
api_key_header = APIKeyHeader(name=API_KEY_NAME, auto_error=False)

# Lazy loading logic to avoid cold boot crashes if secrets fail initially
cached_secret = None

def get_secret_api_key() -> str:
    global cached_secret
    if cached_secret:
        return cached_secret

    project_id = os.environ.get("GCP_PROJECT_ID")
    secret_name = os.environ.get("API_KEY_SECRET_NAME")
    
    # Fallback to local development .env check if secret config is missing
    if not secret_name:
        return os.environ.get("LOCAL_API_KEY", "test_key_123")

    try:
        client = secretmanager.SecretManagerServiceClient()
        name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
        response = client.access_secret_version(request={"name": name})
        cached_secret = response.payload.data.decode("UTF-8").strip()
        return cached_secret
    except Exception as e:
        print(f"[ERROR] Failed to fetch secret from GCP: {e}")
        return "UNKNOWN_SECRET"

async def verify_api_key(api_key: str = Security(api_key_header)):
    if not api_key:
        raise HTTPException(status_code=403, detail="X-API-Key header missing")
        
    expected_key = get_secret_api_key()
    
    if api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API Key")
        
    return api_key

class AskRequest(BaseModel):
    query: str

class AskResponse(BaseModel):
    answer: str
    sources: list[str]

# Global instance initialization
rag_service = None

@app.on_event("startup")
def startup_event():
    global rag_service
    try:
        rag_service = RagService()
        print("[INFO] Vertex AI RAG Service initialized successfully.")
    except Exception as e:
        print(f"[ERROR] Service init failed. Endpoints may 500. {e}")

@app.post("/ask", response_model=AskResponse)
async def ask_endpoint(payload: AskRequest, api_key: str = Depends(verify_api_key)):
    if not rag_service:
        raise HTTPException(status_code=500, detail="RAG engine not initialized")
        
    try:
        result = rag_service.generate_answer(payload.query)
        return AskResponse(
            answer=result["answer"],
            sources=result["sources"]
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/healthz")
def health_check():
    return {"status": "ok"}
