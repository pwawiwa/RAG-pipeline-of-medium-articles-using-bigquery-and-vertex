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
# Full model path for BQ Remote Model
EMBEDDING_MODEL = f"{GCP_PROJECT_ID}.{BQ_DATASET}.embedding_model"

class RagService:
    def __init__(self):
        if not all([GCP_PROJECT_ID, BQ_DATASET]):
            raise ValueError("[ERROR] Missing required environment: GCP_PROJECT_ID, BQ_DATASET")
            
        self.bq_client = bigquery.Client(project=GCP_PROJECT_ID)
        self.gen_model = GenerativeModel("gemini-1.5-flash")
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)

    def retrieve_context(self, query: str, limit: int = 5) -> list:
        """
        Performs a native BigQuery Vector Search.
        Embeds the query and searches the index in one SQL call.
        """
        table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{CHUNKS_TABLE}"
        
        # VECTOR_SEARCH with inline query embedding
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
        
        print(f"[RETRIVAL] Searching BigQuery for: {query}")
        results = self.bq_client.query(search_query, job_config=job_config).result()
        
        context_chunks = []
        for row in results:
            context_chunks.append({
                "url": row.article_url,
                "text": row.chunk_text
            })
        return context_chunks

    def generate_answer(self, query: str) -> dict:
        """
        End-to-end RAG: Retrieval from BQ -> Synthesis with Gemini.
        """
        # 1. Retrieve
        context = self.retrieve_context(query)
        if not context:
            return {
                "answer": "I couldn't find any relevant Medium articles on that topic.",
                "sources": []
            }

        # 2. Prepare Context String
        context_str = "\n\n".join([f"--- Source: {c['url']} ---\n{c['text']}" for c in context])
        
        # 3. Grounded Synthesis
        prompt = f"""
        You are an expert Data Engineering assistant. 
        Use the following retrieved Medium article snippets to answer the user question.
        
        CONTEXT:
        {context_str}
        
        USER QUESTION: {query}
        
        INSTRUCTIONS:
        - Provide a concise, technical answer based ONLY on the context.
        - List the source URLs at the end.
        - If the context doesn't have the answer, say "I don't have enough information from the scraped articles."
        """

        try:
            response = self.gen_model.generate_content(prompt)
            
            sources = list(set([c['url'] for c in context]))

            return {
                "answer": response.text,
                "sources": sources
            }
        except Exception as e:
            print(f"[ERROR] Synthesis failed: {e}")
            raise Exception("RAG Service experienced a synthesis error.")
