from typing import Optional

import uvicorn
from elasticsearch import Elasticsearch, exceptions
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

# Initialize the FastAPI app with a title and description.
app = FastAPI(
    title="Elasticsearch API",
    description="API to interact with Elasticsearch. Swagger docs available at /docs",
    version="0.1.0",
)

# Connect to Elasticsearch. In a Docker environment, the hostname 'elasticsearch'
# refers to the service defined in docker-compose.yml.
es = Elasticsearch("http://elasticsearch:9200", verify_certs=False)


# Define a Pydantic model to validate incoming document data.
class Document(BaseModel):
    author: str
    text: str
    timestamp: Optional[str] = None  # ISO formatted date string
    views: Optional[int] = 0


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
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to index a new document.
@app.post("/documents", summary="Index a new document", tags=["Documents"])
async def create_document(document: Document):
    """
    Index a new document in the 'documents' index.
    Returns the generated document ID.
    """
    try:
        response = es.index(index="documents", document=document.dict())
        return {"result": "Document indexed", "id": response["_id"]}
    except Exception as e:
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
        raise HTTPException(status_code=500, detail=str(e))


# Endpoint to perform a search on documents.
@app.get("/search", summary="Search documents", tags=["Search"])
async def search_documents(q: str):
    """
    Search for documents in the 'documents' index that match the query parameter 'q' in the 'text' field.
    """
    try:
        response = es.search(index="documents", body={"query": {"match": {"text": q}}})
        hits = response.get("hits", {}).get("hits", [])
        return {"results": [hit["_source"] for hit in hits]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# When running locally, this block will start the uvicorn server.
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
