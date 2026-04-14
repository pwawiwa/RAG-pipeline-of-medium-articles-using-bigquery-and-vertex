# Path: apt/playbooks/ingest-medium-to-gcp.md
# Purpose: Playbook for executing the Medium RAG pipeline ingestion & serve logic
# Idempotent: true
# Dependencies: catalog.json

## Metadata
* **Stage:** Phase 1 (Ingestion to Production API)
* **Tools required:** Node.js 20, Python 3.11, Airflow 3.x, gcloud CLI
* **Idempotent:** true

## Objective
Build a production-grade Kubernetes-native data pipeline that scrapes Medium articles using robust Cloudflare-bypass routines (`got-scraping`), stores them in BigQuery with deduplication, syncs embeddings into Vertex AI Managed Corpi, and exposes an identical RAG-chat interface over Cloud Run API.

## Inputs Required
- **Google Cloud Platform Account:** Configured via Application Default Credentials
- **Secrets:** API key provisioned in Secret Manager
- **Variables:** Set per `schema_context/catalog.json`

## Schema Context
Refer to `schema_context/catalog.json` for canonical environment variables and structural formatting.

BigQuery Canonical Table (`silver_medium_articles`):
- `article_url` (STRING, REQUIRED)  [Merge Key]
- `topic` (STRING, REQUIRED)
- `title` (STRING, NULLABLE)
- `author_name` (STRING, NULLABLE)
- `page_content` (STRING, NULLABLE)
- `first_line` (STRING, NULLABLE)
- `published_date` (DATE, NULLABLE) [Partition Column]
- `clap_count` (STRING, NULLABLE)
- `comments_count` (STRING, NULLABLE)
- `hero_image_url` (STRING, NULLABLE)
- `author_avatar_url` (STRING, NULLABLE)
- `ingested_at` (TIMESTAMP, REQUIRED)
- `content_hash` (STRING, REQUIRED)
- `content_gated` (BOOLEAN, REQUIRED)

## Tools to Use
- `apt/tools/ingest/fetch-topic-urls.ts`: Pulls external URLs directly from the Medium feed tag.
- `apt/tools/ingest/fetch-medium-article.ts`: High-resiliency scraper using `got-scraping`.
- `apt/tools/transform/load_to_bigquery.py`: PySpark/Pandas script to transform raw GCS JSONs and merge into BigQuery.
- `apt/tools/serve/update_vertex_rag.py`: Pushes updated BigQuery rows directly into Google Vertex AI Managed Corpus.
- `api/main.py`: Standalone Cloud Run deployment offering RAG inference endpoints.
- `dags/medium_rag_pipeline.py`: Apache Airflow 3.x Directed Acyclic Graph tracking dependencies across Bronze, Silver, Gold.

## API Contract Summary
**Endpoint:** `POST /ask`
**Headers:**
- `X-API-Key`: Evaluated against Google Cloud Secret Manager
**Body:**
```json
{
  "query": "databricks CI/CD strategies?"
}
```
**Response:**
```json
{
  "answer": "Generated LLM context string targeting Databricks DABs...",
  "sources": [
    "https://medium.com/@rohit.../beyond-dataset..."
  ]
}
```

## Edge Cases & Lessons Learned
1. **Empty RSS feed after retries:** Soft exit code 1 to explicitly halt the downstream DAG while preserving raw dumps.
2. **getArticleInfo timeout / 429 rate limit:** Delay handled by exponential backoff; logs placed locally in `/errors` bucket directory before continuing the loop without throwing exceptions.
3. **Duplicate article URL across two topics:** Deduplicated organically using the `article_url` Primary Merge Key acting as an upsert trigger in BigQuery directly.
4. **clap_count "1.2k" string — do not cast:** Keep strings exact for later custom extraction; do not implicitly cast to integer, as "k" denotes thousands.
5. **Medium layout change breaking cheerio selectors:** Cascaded selector priority array to gracefully fallback onto generic HTML body checks if primary nodes vanish.
6. **Vertex AI RAG corpus upsert collision on re-run:** Configured script payload to execute incremental syncs (Diffing hashes) inside Vertex, circumventing chunk duplications.
7. **Paywalled article returns pageContent < 200 chars:** Scraper flags `content_gated = TRUE`; pipeline ignores it during Vertex AI sync to prevent junk contextual data from muddying the RAG embeddings.
