#!/bin/bash
# Path: poc/run_pipeline.sh
# Purpose: Orchestrate the local POC pipeline.

set -e

TOPIC=${1:-"data-engineering"}

echo "================================================="
echo " Medium RAG Pipeline Local POC"
echo " Topic: $TOPIC"
echo "================================================="

# Create data directories
mkdir -p poc/data/articles
mkdir -p poc/data/errors

echo "--- 1. Node.js Ingest Dependencies ---"
cd poc/ingest
npm install

echo "--- 2. Fetch Medium RSS URLs ---"
npm run fetch-urls -- --topic "$TOPIC"

echo "--- 3. Scrape Articles ---"
npm run fetch-articles

cd ../serve

echo "--- 4. Python Serve Dependencies ---"
# use venv to avoid messing with global env
if [ ! -d "venv" ]; then
    python3 -m venv venv
fi
source venv/bin/activate
pip install -r requirements.txt

echo "--- 5. Start Local RAG Chat ---"
echo "Ensure GEMINI_API_KEY is set in your environment or a .env file in poc/serve/"
python local_rag.py
