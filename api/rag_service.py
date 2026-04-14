# Path: api/rag_service.py
# Purpose: Core retrieval and synthesis logic using Vertex AI Native RAG Tooling
# Idempotent: true
# Dependencies: vertexai, python-dotenv

import os
import vertexai
from vertexai.preview import rag
from vertexai.preview.generative_models import GenerativeModel, Tool
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_REGION = os.getenv("GCP_REGION")
CORPUS_NAME = os.getenv("VERTEX_RAG_CORPUS_NAME")
VERTEX_LLM_MODEL = os.getenv("VERTEX_LLM_MODEL", "gemini-flash-latest")

class RagService:
    def __init__(self):
        if not all([GCP_PROJECT_ID, GCP_REGION, CORPUS_NAME]):
            raise ValueError("[ERROR] Missing required environment variables: GCP_PROJECT_ID, GCP_REGION, VERTEX_RAG_CORPUS_NAME")
            
        vertexai.init(project=GCP_PROJECT_ID, location=GCP_REGION)
        
        # Initialize native Vertex RAG tool mapped to our corpus
        # Vertex SDK requires the fully qualified name (projects/*/locations/*/ragCorpora/*), not the display_name.
        # We must look up the corpus by display name first.
        corpora_list = rag.list_corpora()
        actual_corpus_name = None
        for c in corpora_list:
             # Depending on SDK version, object properties are exposed usually as .display_name or ['display_name']
             c_display = getattr(c, "display_name", None)
             if c_display == CORPUS_NAME:
                 actual_corpus_name = getattr(c, "name", None)
                 break
                 
        if not actual_corpus_name:
             raise ValueError(f"[ERROR] Could not find any Vertex RAG Corpus with display name: {CORPUS_NAME}")

        self.rag_retrieval_tool = Tool.from_retrieval(
            retrieval=rag.Retrieval(
                source=rag.VertexRagStore(
                    rag_resources=[
                        rag.RagResource(rag_corpus=actual_corpus_name)
                    ]
                )
            )
        )
        
        self.model = GenerativeModel(VERTEX_LLM_MODEL)

    def generate_answer(self, query: str) -> dict:
        """
        Takes a raw user query, triggers Vertex retrieval, and synthesizes a grounded response.
        """
        # Formulate strict system prompt integrated with user query
        prompt = f"""
        You are an expert Data Engineering assistant. 
        You MUST answer the question using ONLY the provided context retrieved from the datastore. 
        If the context does not contain the answer, politely respond: "I cannot answer this based on the ingested Medium articles."
        
        Question: {query}
        """

        try:
            # Native grounding integration
            response = self.model.generate_content(
                prompt,
                tools=[self.rag_retrieval_tool],
            )
            
            answer_text = response.text if response.text else "No response generated."
            
            # Extract citations from Vertex RAG metadata if available
            sources = []
            if response.candidates and response.candidates[0].grounding_metadata:
                metadata = response.candidates[0].grounding_metadata
                
                # Check for web/retrieval chunks
                if hasattr(metadata, "grounding_chunks"):
                    for chunk in metadata.grounding_chunks:
                        if hasattr(chunk, "retrieved_context") and chunk.retrieved_context.uri:
                            sources.append(chunk.retrieved_context.uri)
                            
            # Ensure unique source URLs
            unique_sources = list(set(sources))

            return {
                "answer": answer_text,
                "sources": unique_sources
            }

        except Exception as e:
            print(f"[ERROR] Gemini Generation Failed: {e}")
            raise Exception("RAG Synthesis failed internally. Please try again later.")
