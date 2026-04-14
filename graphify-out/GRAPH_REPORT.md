# Graph Report - .  (2026-04-15)

## Corpus Check
- 24 files · ~26,491 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 147 nodes · 158 edges · 26 communities detected
- Extraction: 94% EXTRACTED · 6% INFERRED · 0% AMBIGUOUS · INFERRED: 10 edges (avg confidence: 0.5)
- Token cost: 0 input · 0 output

## Community Hubs (Navigation)
- [[_COMMUNITY_Community 0|Community 0]]
- [[_COMMUNITY_Community 1|Community 1]]
- [[_COMMUNITY_Community 2|Community 2]]
- [[_COMMUNITY_Community 3|Community 3]]
- [[_COMMUNITY_Community 4|Community 4]]
- [[_COMMUNITY_Community 5|Community 5]]
- [[_COMMUNITY_Community 6|Community 6]]
- [[_COMMUNITY_Community 7|Community 7]]
- [[_COMMUNITY_Community 8|Community 8]]
- [[_COMMUNITY_Community 9|Community 9]]
- [[_COMMUNITY_Community 10|Community 10]]
- [[_COMMUNITY_Community 11|Community 11]]
- [[_COMMUNITY_Community 12|Community 12]]
- [[_COMMUNITY_Community 13|Community 13]]
- [[_COMMUNITY_Community 14|Community 14]]
- [[_COMMUNITY_Community 15|Community 15]]
- [[_COMMUNITY_Community 16|Community 16]]
- [[_COMMUNITY_Community 17|Community 17]]
- [[_COMMUNITY_Community 18|Community 18]]
- [[_COMMUNITY_Community 19|Community 19]]
- [[_COMMUNITY_Community 20|Community 20]]
- [[_COMMUNITY_Community 21|Community 21]]
- [[_COMMUNITY_Community 22|Community 22]]
- [[_COMMUNITY_Community 23|Community 23]]
- [[_COMMUNITY_Community 24|Community 24]]
- [[_COMMUNITY_Community 25|Community 25]]

## God Nodes (most connected - your core abstractions)
1. `BigQueryManager` - 12 edges
2. `MediumCollector` - 11 edges
3. `BaseCollector` - 7 edges
4. `RagService` - 7 edges
5. `fetchFeed()` - 5 edges
6. `GeminiEmbeddingFunction` - 4 edges
7. `main()` - 4 edges
8. `ProjectConfig` - 4 edges
9. `DataContractError` - 4 edges
10. `scrapeAndUpload()` - 4 edges

## Surprising Connections (you probably didn't know these)
- `fetchFeed()` --calls--> `sleep()`  [EXTRACTED]
  apt/tools/ingest/fetch-topic-urls.ts → poc/ingest/fetch-topic-urls.ts
- `main()` --calls--> `fetchFeed()`  [EXTRACTED]
  poc/ingest/fetch-topic-urls.ts → apt/tools/ingest/fetch-topic-urls.ts
- `MediumCollector` --uses--> `BigQueryManager`  [INFERRED]
  dags/medium_rag_utils/medium_source.py → dags/medium_rag_utils/warehouse.py
- `Implementation of the RAG pipeline for Medium articles.` --uses--> `BigQueryManager`  [INFERRED]
  dags/medium_rag_utils/medium_source.py → dags/medium_rag_utils/warehouse.py
- `Metadata for extraction (actual scraping handled by K8sPodOperator).` --uses--> `BigQueryManager`  [INFERRED]
  dags/medium_rag_utils/medium_source.py → dags/medium_rag_utils/warehouse.py

## Communities

### Community 0 - "Community 0"
Cohesion: 0.14
Nodes (8): ABC, BaseCollector, Abstract base class define the interface for any RAG data source., BaseCollector, MediumCollector, Metadata for extraction (actual scraping handled by K8sPodOperator)., High-level entry point for the pipeline., Implementation of the RAG pipeline for Medium articles.

### Community 1 - "Community 1"
Cohesion: 0.14
Nodes (7): BigQueryManager, Chunks and generates vectors for the specified date. Logic is now idempotent., Standardizes table and model creation with partitioning and clustering for scale, Direct bulk load from GCS to a temporary table with schema enforcement., Runs the data contract SQL validation., UPSERT from temp table to production silver table (Handles Date Casting)., Manages all Data Warehouse operations including DDL, loading, and indexing.

### Community 2 - "Community 2"
Cohesion: 0.18
Nodes (10): mock_airflow_variables(), mock_bigquery_client(), mock_env_vars(), mock_storage_client(), mock_vertex_rag(), Ensure environment variables are set for tests., Fixture to mock BigQuery Client., Fixture to mock Storage Client. (+2 more)

### Community 3 - "Community 3"
Cohesion: 0.22
Nodes (9): DataContractError, get_bq_schema(), get_validation_query(), Validates a single article record against the production data contract., Returns a SQL query that identifies rows violating the data contract.     Used f, Returns the explicit BigQuery schema for the silver table., Raised when a record violates the data contract., validate_medium_article() (+1 more)

### Community 4 - "Community 4"
Cohesion: 0.25
Nodes (3): ProjectConfig, Fetches from Airflow Variables -> Environment -> Default., Centralized configuration manager using a Singleton-like pattern.

### Community 5 - "Community 5"
Cohesion: 0.28
Nodes (5): get_bq_client(), RagService, Production-grade RAG service with optimized client lifecycle., Performs a native BigQuery Vector Search.         Optimized to use the shared cl, End-to-end RAG with source-grounded synthesis.

### Community 6 - "Community 6"
Cohesion: 0.31
Nodes (6): BaseModel, ask_endpoint(), AskRequest, AskResponse, get_secret_api_key(), verify_api_key()

### Community 7 - "Community 7"
Cohesion: 0.43
Nodes (5): EmbeddingFunction, chat_loop(), GeminiEmbeddingFunction, index_documents(), main()

### Community 8 - "Community 8"
Cohesion: 0.48
Nodes (5): fetchFeed(), main(), run(), sleep(), uploadToGcs()

### Community 9 - "Community 9"
Cohesion: 0.47
Nodes (5): generate_vector_index(), Chunks the silver articles and generates embeddings in the gold layer., Loads raw JSON article blobs from GCS to BQ via native LOAD and MERGE., run_load(), upsert_to_bigquery()

### Community 10 - "Community 10"
Cohesion: 0.33
Nodes (0): 

### Community 11 - "Community 11"
Cohesion: 0.4
Nodes (4): Sets up the BigQuery Cloud Connection, the Embedding Model, and the Gold table., Performs a dry run to verify the model actually works., setup_infrastructure(), test_embedding_model()

### Community 12 - "Community 12"
Cohesion: 0.7
Nodes (4): generateSlug(), run(), scrapeAndUpload(), uploadToGcs()

### Community 13 - "Community 13"
Cohesion: 0.6
Nodes (4): get_bronze_articles(), main(), Reads raw article JSONs from GCS for the specified topic and date., upsert_to_bigquery()

### Community 14 - "Community 14"
Cohesion: 0.83
Nodes (3): main(), scrapeArticle(), sleep()

### Community 15 - "Community 15"
Cohesion: 0.5
Nodes (0): 

### Community 16 - "Community 16"
Cohesion: 0.5
Nodes (0): 

### Community 17 - "Community 17"
Cohesion: 1.0
Nodes (0): 

### Community 18 - "Community 18"
Cohesion: 1.0
Nodes (0): 

### Community 19 - "Community 19"
Cohesion: 1.0
Nodes (0): 

### Community 20 - "Community 20"
Cohesion: 1.0
Nodes (0): 

### Community 21 - "Community 21"
Cohesion: 1.0
Nodes (0): 

### Community 22 - "Community 22"
Cohesion: 1.0
Nodes (1): Logic to fetch raw data from source.

### Community 23 - "Community 23"
Cohesion: 1.0
Nodes (1): Logic to check data against contracts.

### Community 24 - "Community 24"
Cohesion: 1.0
Nodes (1): Logic to load data to the warehouse.

### Community 25 - "Community 25"
Cohesion: 1.0
Nodes (1): Logic to generate vectors/indices in the warehouse.

## Knowledge Gaps
- **30 isolated node(s):** `Manages all Data Warehouse operations including DDL, loading, and indexing.`, `Standardizes table and model creation with partitioning and clustering for scale`, `Direct bulk load from GCS to a temporary table with schema enforcement.`, `Runs the data contract SQL validation.`, `UPSERT from temp table to production silver table (Handles Date Casting).` (+25 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 17`** (2 nodes): `medium_rag_pipeline.py`, `medium_rag_pipeline()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 18`** (2 nodes): `vertex_sync.py`, `run_sync()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 19`** (2 nodes): `update_vertex_rag.py`, `main()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 20`** (1 nodes): `test_embed.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 21`** (1 nodes): `__init__.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 22`** (1 nodes): `Logic to fetch raw data from source.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 23`** (1 nodes): `Logic to check data against contracts.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 24`** (1 nodes): `Logic to load data to the warehouse.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 25`** (1 nodes): `Logic to generate vectors/indices in the warehouse.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `BigQueryManager` connect `Community 1` to `Community 0`?**
  _High betweenness centrality (0.032) - this node is a cross-community bridge._
- **Why does `MediumCollector` connect `Community 0` to `Community 1`?**
  _High betweenness centrality (0.021) - this node is a cross-community bridge._
- **Are the 4 inferred relationships involving `BigQueryManager` (e.g. with `MediumCollector` and `Implementation of the RAG pipeline for Medium articles.`) actually correct?**
  _`BigQueryManager` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `MediumCollector` (e.g. with `BaseCollector` and `BigQueryManager`) actually correct?**
  _`MediumCollector` has 2 INFERRED edges - model-reasoned connections that need verification._
- **Are the 4 inferred relationships involving `BaseCollector` (e.g. with `MediumCollector` and `Implementation of the RAG pipeline for Medium articles.`) actually correct?**
  _`BaseCollector` has 4 INFERRED edges - model-reasoned connections that need verification._
- **Are the 2 inferred relationships involving `RagService` (e.g. with `AskRequest` and `AskResponse`) actually correct?**
  _`RagService` has 2 INFERRED edges - model-reasoned connections that need verification._
- **What connects `Manages all Data Warehouse operations including DDL, loading, and indexing.`, `Standardizes table and model creation with partitioning and clustering for scale`, `Direct bulk load from GCS to a temporary table with schema enforcement.` to the rest of the system?**
  _30 weakly-connected nodes found - possible documentation gaps or missing edges._