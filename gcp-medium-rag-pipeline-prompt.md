SYSTEM DIRECTIVE: GCP MEDIUM RAG PIPELINE
BEHAVIORAL CONSTRAINTS

* Do not write any code until explicitly instructed per the step sequence below.
* Do not make assumptions about schema, product choice, or operator type — resolve ambiguity by applying the constraints in this prompt.
* Every output file must include its target path as a comment on line 1.
* Never hardcode credentials. Use GCP Secret Manager references or .env via python-dotenv / dotenv npm package.
* Generate schema_context/catalog.json BEFORE any tool code is written. All subsequent files must reference this catalog as the canonical schema source.

PROJECT OBJECTIVE
Build a production-grade, idempotent data pipeline that:

1. Discovers Medium article URLs by topic (e.g. "data-engineering") via Medium's public RSS feed.
2. Scrapes full article content for each URL using custom `got-scraping` + `cheerio` (Bronze layer: Raw JSON in GCS).
3. Transforms and loads the cleaned data into BigQuery (Silver layer: Parsed structured JSON).
4. Syncs article content + embeddings into Vertex AI RAG Engine (Gold layer: Managed Corpus).
5. Exposes a Cloud Run REST API for RAG-based question answering.

The pipeline (steps 1–4) is orchestrated by Apache Airflow 3.x on Cloud Composer 2. The API (step 5) is a standalone Cloud Run service.
The frontend at pwawiwa.my.id is an EXTERNAL consumer of the Cloud Run API only.

ARCHITECTURE DECISIONS (FINAL — DO NOT DEVIATE)
Concern                    Decision
Topic discovery            Medium RSS: https://medium.com/feed/tag/{topic-slug}
RSS parser                 fast-xml-parser (npm)
Topic slug format          kebab-case (data-engineering, machine-learning, etc.)
Article scraping method    got-scraping + cheerio 
Ingestion runtime          Node.js 20 TypeScript CLI scripts
(Airflow) operator           KubernetesPodOperator
BQ merge key               article_url (STRING, NOT NULL)
BQ partitioning            published_date (DAY)
BQ clustering              topic, published_date
Vertex AI product          RAG Engine (managed corpus)
Embedding model            text-embedding-004
LLM for synthesis          gemini-1.5-pro
API runtime                Python 3.11 FastAPI on Cloud Run
Cloud Run region           asia-southeast2
Cloud Run memory           1Gi minimum
Cloud Run max concurrency  80
API auth                   X-API-Key validated via Secret Manager

Approval token             Respond with [APPROVED: PLAYBOOK] to proceed

ENVIRONMENT VARIABLES CONTRACT
GCP_PROJECT_ID, GCP_REGION, BRONZE_BUCKET, BQ_DATASET, BQ_TABLE, VERTEX_RAG_CORPUS_NAME, VERTEX_EMBEDDING_MODEL, VERTEX_LLM_MODEL, API_KEY_SECRET_NAME, REQUEST_DELAY_MS

SYSTEM FLOW (Updated & Clear Layering)

KEYWORD INPUT ("data-engineering")
        │
        ▼
[Bronze 1: fetch-topic-urls.ts]
  → GET https://medium.com/feed/tag/{topic}
  → Parse RSS with fast-xml-parser
  → Extract article URLs
  → Save to GCS: raw/topic-urls/{topic}/{YYYY-MM-DD}/urls.json
        │
        ▼
[Bronze 2: fetch-medium-article.ts]   ← One execution per URL
  → Custom got-scraping + cheerio parsing
  → Throttle with REQUEST_DELAY_MS
  → Save raw scraped data to GCS: raw/articles/{topic}/{YYYY-MM-DD}/{url_slug}.json
  → On failure: log to errors/ and continue
        │
        ▼
[Silver: load_to_bigquery.py]
  → Read raw JSONs from Bronze articles folder
  → Parse dates, compute content_hash, detect paywall (content_gated)
  → Clean and structure data
  → MERGE into BigQuery silver_medium_articles on article_url
        │
        ▼
[Gold: update_vertex_rag.py]
  → Read changed records from BigQuery (exclude content_gated = TRUE)
  → Upsert into Vertex AI RAG Engine corpus
        │
        ▼
[Cloud Run: FastAPI]
  → POST /ask → RAG query → Gemini synthesis → Return answer + sources
        │
        ▼
  pwawiwa.my.id (frontend)

BIGQUERY SCHEMA (SILVER LAYER)
Table: silver_medium_articles
- article_url        STRING    REQUIRED
- topic              STRING    REQUIRED
- title              STRING    NULLABLE
- author_name        STRING    NULLABLE
- page_content       STRING    NULLABLE
- first_line         STRING    NULLABLE
- published_date     DATE      NULLABLE     (partition column)
- clap_count         STRING    NULLABLE
- comments_count     STRING    NULLABLE
- hero_image_url     STRING    NULLABLE
- author_avatar_url  STRING    NULLABLE
- ingested_at        TIMESTAMP REQUIRED
- content_hash       STRING    REQUIRED
- content_gated      BOOLEAN   REQUIRED

ARTICLE SCRAPER SPECIFICATION: got-scraping + cheerio
Use only got-scraping and cheerio.
(Full scraper specification including headers and selectors remains as previously defined — use priority order for selectors, paywall detection, etc.)

MEDIUM RSS SPECIFICATION
Feed URL pattern: https://medium.com/feed/tag/{topic-slug}
Topic slug is kebab-case: data-engineering, machine-learning, etc.
Parse with fast-xml-parser. Extract URLs from <item><link> nodes.
Expected items per feed: 10–30 articles.
Retry policy: 3 attempts, exponential backoff (1s, 2s, 4s).
If feed returns empty after retries: exit code 1, write to GCS errors path, halt DAG.
If RSS is unreachable AND a pre-supplied URL list exists at
gs://{BRONZE_BUCKET}/manual-urls/{topic}/urls.json, fall back to that list.

FILE STRUCTURE TO GENERATE
schema_context/
  catalog.json                    ← canonical schema — generated FIRST in Step 1

docs/
  scraper-specification.md        ← custom axios+cheerio scraper spec — READ before writing any ingestion tool

apt/
  playbooks/
    ingest-medium-to-gcp.md       ← generated in Step 1
  tools/
    ingest/
      fetch-topic-urls.ts         ← RSS feed → URL list → GCS
      fetch-medium-article.ts     ← single URL → getArticleInfo → GCS
      package.json                ← dependencies for both ingest scripts
      tsconfig.json               ← TypeScript config
      Dockerfile                  ← container for KubernetesPodOperator
    transform/
      load_to_bigquery.py         ← GCS JSON → BigQuery silver upsert
      requirements.txt
    serve/
      update_vertex_rag.py        ← BigQuery silver → Vertex AI RAG corpus upsert
      requirements.txt

api/
  main.py                         ← FastAPI Cloud Run entrypoint
  rag_service.py                  ← RAG query + Gemini synthesis logic
  Dockerfile
  requirements.txt

dags/
  medium_rag_pipeline.py          ← Airflow 3.x TaskFlow DAG

STEP SEQUENCE
STEP 1 — Schema Catalog + Playbook (execute now)
Generate the following two files in order:
1a. schema_context/catalog.json
Define the canonical schema contract: BQ table name, all column definitions
(name, type, mode, description), GCS path patterns, and env var references.
This file is the single source of truth — all subsequent tools must reference
it rather than re-defining schema inline.
1b. apt/playbooks/ingest-medium-to-gcp.md with exactly these sections:
## Metadata             ← Stage, Tools required, Idempotent: true
## Objective
## Inputs Required
## Schema Context       ← reference catalog.json; embed the BQ table above
## Tools to Use         ← every filename with one-line purpose
## API Contract Summary ← summarize POST /ask contract
## Edge Cases & Lessons Learned  ← minimum 6 concrete cases:
                                    1. Empty RSS feed after retries
                                    2. getArticleInfo timeout / 429 rate limit
                                    3. Duplicate article URL across two topics
                                    4. clap_count "1.2k" string — do not cast
                                    5. Medium layout change breaking cheerio selectors
                                    6. Vertex AI RAG corpus upsert collision on re-run
                                    7. Paywalled article returns pageContent < 200 chars — set content_gated = TRUE, exclude from RAG sync
Output each file in its own fenced code block.
Then stop. Wait for [APPROVED: PLAYBOOK] before proceeding.

STEP 2 — Ingestion Tools (only after [APPROVED: PLAYBOOK])
Before writing any code in this step, re-read the ARTICLE SCRAPER SPECIFICATION
section above in full. Use only the selectors, headers, and field names defined there.
Generate in order, one file per response:

apt/tools/ingest/fetch-topic-urls.ts
apt/tools/ingest/fetch-medium-article.ts
apt/tools/ingest/package.json
apt/tools/ingest/tsconfig.json
apt/tools/ingest/Dockerfile

Wait for [APPROVED: STEP 2] before proceeding.

STEP 3 — Transform & Serve Tools (only after [APPROVED: STEP 2])
Generate in order:

apt/tools/transform/load_to_bigquery.py
apt/tools/transform/requirements.txt
apt/tools/serve/update_vertex_rag.py
apt/tools/serve/requirements.txt

Wait for [APPROVED: STEP 3] before proceeding.

STEP 4 — Cloud Run API (only after [APPROVED: STEP 3])
Generate in order:

api/rag_service.py
api/main.py
api/Dockerfile
api/requirements.txt

Wait for [APPROVED: STEP 4] before proceeding.

STEP 5 — Orchestration (only after [APPROVED: STEP 4])
Generate:

dags/medium_rag_pipeline.py

Requirements:

Airflow 3.x TaskFlow API (@task decorators)
KubernetesPodOperator for Node.js ingestion containers
Retry logic: 3 retries, 5-minute delay between retries
Task-level failure callbacks that write failure metadata to GCS
XCom passing of GCS paths between tasks
DAG-level tags: ["medium", "rag", "bronze-silver-gold"]


FILE HEADER CONTRACT
Every generated file must begin with this comment block:
# Path: <relative file path>
# Purpose: <one sentence>
# Idempotent: true | false
# Dependencies: <comma-separated list>
(Use // for TypeScript/JSON files, # for Python/Dockerfile/YAML.)