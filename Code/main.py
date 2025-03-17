import time
from datetime import datetime
from elasticsearch import Elasticsearch, helpers

# Connect to Elasticsearch running on the "elasticsearch" host defined in docker-compose
es = None
for i in range(10):
    try:
        es = Elasticsearch("http://elasticsearch:9200", verify_certs=False)
        if es.ping():
            print("Connected to Elasticsearch")
            break
    except Exception as e:
        print("Waiting for Elasticsearch...", e)
        time.sleep(2)
else:
    print("Elasticsearch is not available")
    exit(1)

# Define the index name
index_name = "blog-index"

# Create the index with custom mapping if it doesn't exist
if not es.indices.exists(index=index_name):
    mapping = {
        "mappings": {
            "properties": {
                "author": {"type": "text"},
                "text": {"type": "text"},
                "timestamp": {"type": "date"},
                "views": {"type": "integer"}
            }
        }
    }
    es.indices.create(index=index_name, body=mapping)
    print(f"Index '{index_name}' created.")

# Index individual documents
doc1 = {
    "author": "Alice",
    "text": "Elasticsearch is a powerful search engine for distributed data.",
    "timestamp": datetime.now(),
    "views": 10
}
response1 = es.index(index=index_name, document=doc1)
print("Document 1 indexed:", response1)

doc2 = {
    "author": "Bob",
    "text": "Learning Elasticsearch is fun and educational.",
    "timestamp": datetime.now(),
    "views": 20
}
response2 = es.index(index=index_name, document=doc2)
print("Document 2 indexed:", response2)

# Refresh the index to make sure documents are searchable immediately
es.indices.refresh(index=index_name)

# Retrieve a document by its ID
doc_id = response1['_id']
retrieved = es.get(index=index_name, id=doc_id)
print("Retrieved document:", retrieved['_source'])

# Update a document (e.g., updating the 'views' field)
update_body = {"doc": {"views": 15}}
update_response = es.update(index=index_name, id=doc_id, body=update_body)
print("Updated document:", update_response)

# Delete a document (e.g., delete the document indexed as doc2)
delete_response = es.delete(index=index_name, id=response2['_id'])
print("Deleted document:", delete_response)

# Perform a search query to find documents containing "search engine"
search_query = {
    "query": {
        "match": {
            "text": "search engine"
        }
    }
}
search_results = es.search(index=index_name, body=search_query)
print("Search results:", search_results)

# Aggregation example: Count documents by author
# Note: For aggregations on text fields, a keyword sub-field is used.
aggregation_query = {
    "size": 0,
    "aggs": {
        "authors": {
            "terms": {
                "field": "author.keyword"  # use .keyword for aggregations on text fields
            }
        }
    }
}
agg_results = es.search(index=index_name, body=aggregation_query)
print("Aggregation results (count by author):", agg_results['aggregations']['authors']['buckets'])

# Bulk indexing: Index multiple documents at once
bulk_docs = [
    {
        "_index": index_name,
        "_source": {
            "author": "Charlie",
            "text": "Bulk indexing test document.",
            "timestamp": datetime.now(),
            "views": 5
        }
    },
    {
        "_index": index_name,
        "_source": {
            "author": "Diana",
            "text": "Another bulk document for testing Elasticsearch.",
            "timestamp": datetime.now(),
            "views": 7
        }
    }
]
bulk_response = helpers.bulk(es, bulk_docs)
print("Bulk indexing response:", bulk_response)

# Final refresh to include bulk operations
es.indices.refresh(index=index_name)
