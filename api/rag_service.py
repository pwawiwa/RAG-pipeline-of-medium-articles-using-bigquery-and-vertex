# api/rag_service.py
import os
from google import genai
from google.genai import types
from google.cloud import bigquery

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("BQ_DATASET")

class RagService:
    def __init__(self):
        if not all([GCP_PROJECT_ID, BQ_DATASET]):
            raise ValueError("[ERROR] Missing required environment: GCP_PROJECT_ID, BQ_DATASET")
        
        # Using GEMINI_API_KEY as a fallback for POC compatibility
        api_key = os.environ.get("GOOGLE_CLOUD_API_KEY") or os.environ.get("GEMINI_API_KEY")
        self.client = genai.Client(
            vertexai=False,
            api_key=api_key
        )
        self.bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        self.model_name = "gemini-3.1-flash-lite-preview"

    def retrieve_context(self, query: str, limit: int = 3) -> list:
        """Retrieves article chunks from Singapore BigQuery."""
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.gold_article_chunks"
        model_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.embedding_model"
        
        search_query = f"""
        SELECT base.article_url, base.chunk_text
        FROM VECTOR_SEARCH(
            TABLE `{table_id}`,
            'embedding',
            (
                SELECT ml_generate_embedding_result
                FROM ML.GENERATE_EMBEDDING(
                    MODEL `{model_id}`,
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
        """Generates answer using Gemini 3.1 via the Google AI API."""
        context = self.retrieve_context(query)
        context_str = "\n\n".join([f"Source: {c['url']}\n{c['text']}" for c in context]) if context else "No context found."
        
        prompt = f"Using this context (from Medium articles):\n{context_str}\n\nQuestion: {query}"
        
        # Updated config for gemini-3.1-flash-lite-preview
        generate_content_config = types.GenerateContentConfig(
            temperature=1,
            top_p=0.95,
            max_output_tokens=4096,
            safety_settings=[
                types.SafetySetting(category="HARM_CATEGORY_HATE_SPEECH", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_DANGEROUS_CONTENT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_SEXUALLY_EXPLICIT", threshold="OFF"),
                types.SafetySetting(category="HARM_CATEGORY_HARASSMENT", threshold="OFF")
            ],
            tools=[types.Tool(google_search=types.GoogleSearch())],
            thinking_config=types.ThinkingConfig(thinking_level="LOW")
        )

        try:
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=prompt,
                config=generate_content_config
            )
            
            return {
                "answer": response.text,
                "sources": list(set([c['url'] for c in context])) if context else ["web_search"]
            }
        except Exception as e:
            print(f"[ERROR] Synthesis failed: {e}")
            raise