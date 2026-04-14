# Path: apt/tools/ingest/Dockerfile
# Purpose: Containerize the Node.js ingestion utilities for Airflow KubernetesPodOperator
# Idempotent: true
# Dependencies: package.json, tsconfig.json, fetch-topic-urls.ts, fetch-medium-article.ts

FROM node:20-slim

WORKDIR /app

# Install dependencies required by got-scraping to construct TLS connections natively
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

COPY package*.json ./
RUN npm install

COPY tsconfig.json ./
COPY fetch-topic-urls.ts ./
COPY fetch-medium-article.ts ./

# Run TypeScript compilation during the build phase
RUN npm run build

# Default entrypoint (Airflow will override the command)
ENTRYPOINT ["npm", "run"]
