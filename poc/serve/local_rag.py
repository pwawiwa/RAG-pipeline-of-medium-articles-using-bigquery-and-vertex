# Path: poc/serve/local_rag.py
# Purpose: Read scraped local JSONs, index into ChromaDB, and query Gemini 1.5 Pro to answer questions based on the RAG context.
# Idempotent: true

import os
import json
import glob
from pathlib import Path
from dotenv import load_dotenv

import chromadb
from chromadb import Documents, EmbeddingFunction, Embeddings
from google import genai
from google.genai import types

load_dotenv()

# Verify API key
if not os.environ.get("GEMINI_API_KEY"):
    print("[ERROR] GEMINI_API_KEY is not set in environment.")
    exit(1)

# Initialize Gemini Client
api_key = os.environ.get("GEMINI_API_KEY")
client = genai.Client(api_key=api_key)

class GeminiEmbeddingFunction(EmbeddingFunction):
    def __call__(self, input: Documents) -> Embeddings:
        # text-embedding-004 is the recommended model for text embeddings
        response = client.models.embed_content(
            model="gemini-embedding-001",
            contents=input,
        )
        # response.embeddings is a list of embeddings
        return [e.values for e in response.embeddings]

def index_documents(chroma_collection):
    print("\n[INFO] Loading scraped articles...")
    data_dir = os.path.join(os.path.dirname(__file__), "..", "data", "articles", "*.json")
    files = glob.glob(data_dir)
    
    if not files:
        print("[WARN] No JSON articles found in poc/data/articles/")
        return

    docs = []
    metadata = []
    ids = []

    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as f:
            data = json.load(f)
        
        # Skip gated content as requested
        if data.get("content_gated"):
            print(f"  [SKIP] {data['article_url']} (paywalled/gated)")
            continue
            
        doc_id = data.get("content_hash") or Path(fpath).stem
        
        # Chunking: for POC, we will just take the first 4000 chars if it's very long 
        # to avoid embedding limits, though text-embedding-004 supports up to 2048 tokens.
        content = data.get("page_content") or ""
        
        # Simplistic chunking for POC
        chunk_size = 3000
        chunks = [content[i:i+chunk_size] for i in range(0, len(content), chunk_size)]
        
        for i, chunk in enumerate(chunks):
            chunk_id = f"{doc_id}_chunk_{i}"
            docs.append(chunk)
            metadata.append({
                "source": data.get("article_url"),
                "title": data.get("title") or "Unknown",
                "author": data.get("author_name") or "Unknown"
            })
            ids.append(chunk_id)
            
    if docs:
        print(f"[INFO] Indexing {len(docs)} chunks into Chroma...")
        chroma_collection.upsert(
            documents=docs,
            metadatas=metadata,
            ids=ids
        )
        print("[SUCCESS] Indexing complete.")

def chat_loop(chroma_collection):
    print("\n" + "="*50)
    print("🧠 Local Medium RAG Pipeline Active")
    print("="*50)
    print("Type your question below (or 'quit' to exit)\n")
    
    while True:
        query = input("\nUser> ")
        if query.lower() in ("quit", "exit", "q"):
            break
            
        if not query.strip():
            continue
            
        print("\n[INFO] Retrieving context...")
        results = chroma_collection.query(
            query_texts=[query],
            n_results=3,
        )
        
        if not results['documents'] or not results['documents'][0]:
            print("System> No relevant context found in the local database. I cannot answer based on the retrieved sources.")
            continue
            
        contexts = results['documents'][0]
        sources = results['metadatas'][0]
        
        # Construct the prompt with retrieved contexts
        context_block = ""
        for i, (ctx, meta) in enumerate(zip(contexts, sources)):
            context_block += f"\n--- Source {i+1}: {meta.get('title')} ({meta.get('source')}) ---\n{ctx}\n"
            
        prompt = f"""You are a helpful assistant answering technical questions based ONLY on the provided context.
        If the context does not contain the answer, say "I don't know based on the provided articles."
        
        Context:
        {context_block}
        
        Question:
        {query}
        """

        print("[INFO] Generating answer with Gemini Flash...\n")
        response = client.models.generate_content(
            model='gemini-flash-latest',
            contents=prompt,
        )
        
        print("Gemini>", response.text.strip())
        print("\nSources Used:")
        # Deduplicate sources
        unique_sources = list({m['source'] for m in sources})
        for s in unique_sources:
            print(f"- {s}")


def main():
    db_path = os.path.join(os.path.dirname(__file__), "..", "data", "chroma_db")
    client = chromadb.PersistentClient(path=db_path)
    
    # Use our custom Gemini embedding function
    emb_fn = GeminiEmbeddingFunction()
    
    collection = client.get_or_create_collection(
        name="medium_articles",
        embedding_function=emb_fn
    )
    
    index_documents(collection)
    chat_loop(collection)

if __name__ == "__main__":
    main()
