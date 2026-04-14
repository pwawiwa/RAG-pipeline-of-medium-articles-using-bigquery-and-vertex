# Medium Articles RAG Pipeline

A production-grade Retrieval-Augmented Generation (RAG) pipeline designed to scrape Medium articles, generate high-quality text embeddings, store them in a BigQuery data warehouse, and answer questions using Google's newest Gemini models.

## 🚀 Architecture

This system utilizes a **Hybrid-Region** design for optimal latency and access to experimental AI features:

1.  **Data Ingestion & Vectorization**: Articles are scraped and embedded using the `text-embedding-004` model.
2.  **Vector Storage (Singapore)**: Embeddings are stored in Google BigQuery (`asia-southeast1`). We utilize BigQuery's native `VECTOR_SEARCH` capabilities to perform ultra-fast similarity searches directly within the data warehouse.
3.  **Inference (Global)**: Context retrieval happens in Singapore, but Generation leverages the latest **Gemini 3.1 Flash-Lite Preview** model via the `google-genai` SDK.

## ✨ Features

-   **Gemini 3.1 Integration**: Uses the newest `google-genai` SDK.
-   **Thinking Mode**: Generative responses are powered by "LOW" level thinking for enhanced synthesis and reasoning.
-   **Google Search Grounding**: The generation model can fall back to the live web to double-check facts or augment missing context.
-   **Data Contracts**: Enforces strict typing (e.g., handling metric strings like "1.2K claps") to prevent pipeline crashes during ingestion.

## 🛠️ Setup & Requirements

### Infrastructure

Ensure you have the following enabled in your Google Cloud Project:
-   `aiplatform.googleapis.com` (Vertex AI API)
-   `generativelanguage.googleapis.com` (Google AI API)
-   A BigQuery Dataset (e.g., `medium_pipeline` in `asia-southeast1`)

### Environment Variables

To run the RAG inference service, you need to set up your environment:

```bash
# Set your API Key from Google AI Studio
export GEMINI_API_KEY="your_api_key_here"

# (Optional) Override defaults
export GCP_PROJECT_ID="your_project_id"
export BQ_DATASET="medium_pipeline"
export GCP_REGION="asia-southeast1"
```

## 🧪 Running the Pipeline

### 1. Provision Infrastructure
Run the setup script to create the necessary BigQuery tables and remote connections:
```bash
PYTHONPATH=dags python3 dags/medium_rag_utils/setup_bq.py
```

### 2. Test RAG Inference
You can test the end-to-end context retrieval and Gemini synthesis using the manual test script:
```bash
PYTHONPATH=dags python3 tests/manual_check_rag.py
```
*Note: Ensure your `GEMINI_API_KEY` has available quota.*

## 📂 Project Structure

-   **`api/`**: Contains the FastAPI or inference logic. `rag_service.py` is the core orchestrator for BigQuery retrieval and Gemini 3.1 synthesis.
-   **`dags/`**: Contains the Airflow DAGs and utilities (`medium_rag_utils/`) for data ingestion, transformation, and BigQuery vector updates.
-   **`data/`**: (Ignored by default) Local storage for intermediate files.
-   **`poc/`**: Proof-of-concept scripts for initial local vector store testing (ChromaDB).
-   **`tests/`**: Verification scripts like `manual_check_rag.py` to test the inference workflow.
