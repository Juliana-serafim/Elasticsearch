import logging
import time
from datetime import datetime
from typing import Optional

import uvicorn
from elasticsearch import Elasticsearch, exceptions
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize the FastAPI app with a title and description.
app = FastAPI(
    title="Elasticsearch API",
    description="API to interact with Elasticsearch. Swagger docs available at /docs",
    version="0.1.0",
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Define a Pydantic model to validate incoming document data.
class Document(BaseModel):
    author: str
    text: str
    timestamp: Optional[str] = None  # ISO formatted date string
    views: Optional[int] = 0


# Connect to Elasticsearch with retry logic
def connect_elasticsearch():
    """Establish connection to Elasticsearch with retry logic"""
    max_retries = 5
    retry_interval = 5  # seconds

    for attempt in range(max_retries):
        try:
            es = Elasticsearch("http://elasticsearch:9200", verify_certs=False)
            # Test connection
            if es.ping():
                logger.info("Successfully connected to Elasticsearch")
                return es
            else:
                logger.warning("Elasticsearch ping failed")
        except Exception as e:
            logger.error(f"Connection attempt {attempt+1} failed: {str(e)}")

        if attempt < max_retries - 1:
            logger.info(f"Retrying in {retry_interval} seconds...")
            time.sleep(retry_interval)

    logger.error("Failed to connect to Elasticsearch after multiple attempts")
    raise ConnectionError("Could not connect to Elasticsearch")


# Initialize Elasticsearch client
es = connect_elasticsearch()


# Create 'documents' index if it doesn't exist and add sample data
def initialize_index():
    """Create 'documents' index if it doesn't exist and populate with sample data"""
    try:
        # Check if the index exists
        if not es.indices.exists(index="documents"):
            # Create the index with mapping
            mapping = {
                "mappings": {
                    "properties": {
                        "author": {"type": "keyword"},
                        "text": {"type": "text", "analyzer": "standard"},
                        "timestamp": {
                            "type": "date",
                            "format": "strict_date_optional_time||epoch_millis",
                        },
                        "views": {"type": "integer"},
                    }
                }
            }
            es.indices.create(index="documents", body=mapping)
            logger.info("Created 'documents' index")

            # Add sample documents
            sample_docs = [
                Document(
                    author="John Doe",
                    text="Elasticsearch is a powerful search engine based on Lucene.",
                    timestamp=datetime.now().isoformat(),
                    views=42,
                ),
                Document(
                    author="Jane Smith",
                    text="FastAPI makes it easy to build high-performance APIs quickly.",
                    timestamp=datetime.now().isoformat(),
                    views=17,
                ),
                Document(
                    author="Bob Johnson",
                    text="Docker simplifies deployment by containerizing applications.",
                    timestamp=datetime.now().isoformat(),
                    views=29,
                ),
            ]

            # Index sample documents
            for doc in sample_docs:
                es.index(index="documents", document=doc.dict())

            logger.info(
                f"Added {len(sample_docs)} sample documents to the 'documents' index"
            )
        else:
            logger.info("'documents' index already exists")
    except Exception as e:
        logger.error(f"Error initializing index: {str(e)}")
        raise


# Initialize index on startup
@app.on_event("startup")
async def startup_event():
    """Run initialization tasks on application startup"""
    logger.info("Starting application...")
    try:
        initialize_index()
        logger.info("Initialization complete")
    except Exception as e:
        logger.error(f"Startup initialization failed: {str(e)}")


# Root endpoint for a friendly welcome message.
@app.get("/", summary="Welcome", tags=["Root"])
async def read_root():
    return {
        "message": "Welcome to the Elasticsearch API. See /docs for API documentation."
    }


# Endpoint to list all documents using a match_all query.
@app.get("/documents", summary="List all documents", tags=["Documents"])
async def list_documents():
    """
    List all documents from the 'documents' index.
    """
    try:
        response = es.search(index="documents", body={"query": {"match_all": {}}})
        hits = response.get("hits", {}).get("hits", [])
        return {"results": [hit["_source"] for hit in hits]}
    except exceptions.NotFoundError:
        # Handle case where index doesn't exist
        return {"results": []}
    except Exception as e:
        logger.error(f"Error listing documents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to index a new document.
@app.post("/documents", summary="Index a new document", tags=["Documents"])
async def create_document(document: Document):
    """
    Index a new document in the 'documents' index.
    Returns the generated document ID.
    """
    try:
        # Set timestamp if not provided
        if not document.timestamp:
            document.timestamp = datetime.now().isoformat()

        response = es.index(index="documents", document=document.dict())
        return {"result": "Document indexed", "id": response["_id"]}
    except Exception as e:
        logger.error(f"Error creating document: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to retrieve a document by its ID.
@app.get("/documents/{doc_id}", summary="Retrieve a document", tags=["Documents"])
async def get_document(doc_id: str):
    """
    Retrieve a document from the 'documents' index by its ID.
    """
    try:
        response = es.get(index="documents", id=doc_id)
        return response["_source"]
    except exceptions.NotFoundError:
        raise HTTPException(status_code=404, detail="Document not found")
    except Exception as e:
        logger.error(f"Error retrieving document {doc_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to perform a search on documents.
@app.get("/search", summary="Search documents", tags=["Search"])
async def search_documents(q: str):
    """
    Search for documents in the 'documents' index that match the query parameter 'q' in the 'text' field.
    """
    try:
        response = es.search(
            index="documents",
            body={
                "query": {"match": {"text": q}},
                "highlight": {"fields": {"text": {}}},
            },
        )
        hits = response.get("hits", {}).get("hits", [])

        results = []
        for hit in hits:
            doc = hit["_source"]
            # Add highlight if available
            if "highlight" in hit:
                doc["highlight"] = hit["highlight"]
            # Add score
            doc["score"] = hit["_score"]
            results.append(doc)

        return {"results": results}
    except exceptions.NotFoundError:
        return {"results": []}
    except Exception as e:
        logger.error(f"Error searching documents: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


# When running locally, this block will start the uvicorn server.
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
