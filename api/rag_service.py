# api/rag_service.py
import os
import vertexai
from google.cloud import bigquery
from vertexai.generative_models import GenerativeModel
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
BQ_DATASET = os.getenv("BQ_DATASET")
CHUNKS_TABLE = "gold_article_chunks"
EMBEDDING_MODEL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.embedding_model"

# Shared client instance for connection pooling/reuse
_bq_client = None

def get_bq_client():
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=GCP_PROJECT_ID)
    return _bq_client

class RagService:
    """Production-grade RAG service with optimized client lifecycle."""
    
    def __init__(self):
        if not all([GCP_PROJECT_ID, BQ_DATASET]):
            raise ValueError("[ERROR] Missing required environment: GCP_PROJECT_ID, BQ_DATASET")
            
        self.bq_client = get_bq_client()
        self.gen_model = GenerativeModel("gemini-1.5-flash")
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    def retrieve_context(self, query: str, limit: int = 5) -> list:
        """
        Performs a native BigQuery Vector Search.
        Optimized to use the shared client and paramaterized SQL.
        """
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{CHUNKS_TABLE}"
        
        search_query = f"""
        SELECT article_url, chunk_text
        FROM VECTOR_SEARCH(
            TABLE `{table_id}`,
            'embedding',
            (
                SELECT ml_generate_embedding_result
                FROM ML.GENERATE_EMBEDDING(
                    MODEL `{EMBEDDING_MODEL}`,
                    (SELECT @query_text AS content),
                    STRUCT(TRUE AS flatten_json_output)
                )
            ),
            top_k => {limit}
        )
        """
        
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("query_text", "STRING", query)
            ]
        )
        
        results = self.bq_client.query(search_query, job_config=job_config).result()
        
        return [{"url": row.article_url, "text": row.chunk_text} for row in results]

    def generate_answer(self, query: str) -> dict:
        """End-to-end RAG with source-grounded synthesis."""
        context = self.retrieve_context(query)
        if not context:
            return {"answer": "No relevant info found.", "sources": []}

        context_str = "\n\n".join([f"Source: {c['url']}\n{c['text']}" for c in context])
        prompt = f"Using this context:\n{context_str}\n\nQuestion: {query}"

        try:
            response = self.gen_model.generate_content(prompt)
            return {
                "answer": response.text,
                "sources": list(set([c['url'] for c in context]))
            }
        except Exception as e:
            print(f"[ERROR] Synthesis failed: {e}")
            raise
