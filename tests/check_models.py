# tests/check_models.py
from google.cloud import bigquery
import os

PROJECT_ID = 'my-playground-492309'
REGION = 'asia-southeast1'
DATASET = 'medium_pipeline'
CONN_ID = f'{PROJECT_ID}.{REGION}.vertex-ai-connection'

def check_model(endpoint):
    client = bigquery.Client(project=PROJECT_ID)
    model_id = f'{PROJECT_ID}.{DATASET}.check_{endpoint.replace(".", "_").replace("-", "_")}'
    sql = f"""
    CREATE OR REPLACE MODEL `{model_id}`
    REMOTE WITH CONNECTION `{CONN_ID}`
    OPTIONS(ENDPOINT = '{endpoint}');
    """
    print(f"Testing endpoint: {endpoint}...", end=" ", flush=True)
    try:
        client.query(sql).result()
        print("✅ SUCCESS")
        client.delete_model(model_id)
        return True
    except Exception as e:
        import traceback
        print(f"❌ FAILED: {str(e)}")
        return False

if __name__ == "__main__":
    endpoints = [
        "gemini-1.5-flash-002",
        "gemini-1.5-flash-001",
        "gemini-1.5-flash",
        "gemini-pro"
    ]
    
    results = []
    for ep in endpoints:
        if check_model(ep):
            results.append(ep)
    
    print("\n--- Summary of Available Models in Singapore ---")
    if results:
        for r in results:
            print(f"  - {r}")
    else:
        print("  No models found. Check permissions or region support.")
