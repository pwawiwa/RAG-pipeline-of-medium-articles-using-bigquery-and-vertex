# tests/manual_check_rag.py
import sys
import os
from pprint import pprint

# Ensure we can import from api/
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'api')))

# Fill in defaults for the manual test BEFORE importing the module
os.environ.setdefault("GCP_PROJECT_ID", "my-playground-492309")
os.environ.setdefault("BQ_DATASET", "medium_pipeline")
os.environ.setdefault("GCP_REGION", "asia-southeast1")

from rag_service import RagService

def run_test():
    print("🚀 Initializing Gemini 3.1 RAG Service (Thinking Mode)...")
    
    # Check for API Key (Both names supported)
    if not os.environ.get("GOOGLE_CLOUD_API_KEY") and not os.environ.get("GEMINI_API_KEY"):
        print("\n❌ MISSING API KEY!")
        print("Please run: export GEMINI_API_KEY='your_key_from_ai_studio'")
        return

    try:
        service = RagService()
        
        query = "What are the latest spatial analytics trends mentioned in our Medium articles?"
        print(f"\n❓ QUERY: {query}")
        
        print("\n🔍 Step 1: Retrieving Context from BigQuery (Singapore)...")
        context = service.retrieve_context(query, limit=3)
        if not context:
            print("⚠️ No specific Medium articles found on this topic. Falling back to Google Search grounding...")
        else:
            print(f"✅ Found {len(context)} relevant article chunks.")

        print("\n🧠 Step 2: Generating Answer with Gemini 3.1 (Thinking + Search Grounding)...")
        result = service.generate_answer(query)
        
        print("\n🤖 GEMINI 3.1 RESPONSE:")
        print("-" * 60)
        print(result['answer'])
        print("-" * 60)
        
        print("\n🔗 SOURCES:")
        pprint(result['sources'])
        
        print("\n✅ End-to-end test complete.")
        
    except Exception as e:
        print(f"💥 TEST FAILED: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_test()
