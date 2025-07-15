from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import ConnectionFailure
from typing import Dict, Any, List
import config
from local.utils.logger import logger

def create_mongodb_client(uri: str) -> MongoClient:
    """
    Create a MongoDB client with connection pooling.
    
    Args:
        uri: MongoDB connection URI
        
    Returns:
        MongoClient: MongoDB client
        
    Raises:
        ConnectionFailure: If connection fails
    """
    try:
        client = MongoClient(
            uri,
            w="majority",
            readConcernLevel="majority",
            connectTimeoutMS=config.MONGODB_CONNECTION_TIMEOUT,
            serverSelectionTimeoutMS=config.MONGODB_CONNECTION_TIMEOUT,
            maxPoolSize=10
        )
        
        # Test connection
        client.admin.command('ismaster')
        logger.info("Successfully connected to MongoDB")
        return client
    except ConnectionFailure as e:
        logger.error(f"Failed to connect to MongoDB: {str(e)}")
        raise

def get_collection(
    uri: str, 
    database_name: str, 
    collection_name: str
) -> Collection:
    """
    Get a MongoDB collection.
    
    Args:
        uri: MongoDB connection URI
        database_name: Database name
        collection_name: Collection name
        
    Returns:
        Collection: MongoDB collection
    """
    client = create_mongodb_client(uri)
    database = client[database_name]
    return database[collection_name]

def ensure_vector_index(
    collection: Collection,
    index_name: str,
    embedding_field: str,
    dimension: int = config.VECTOR_DIMENSION
) -> None:
    """
    Ensure vector search index exists on the collection.
    
    Args:
        collection: MongoDB collection
        index_name: Name of the vector search index
        embedding_field: Field name for embeddings
        dimension: Vector dimension
        
    Note:
        This assumes the vector search index is created via Atlas UI or API
        This function just checks if the index exists and logs a warning if not
    """
    try:
        # Check existing indexes
        indexes = list(collection.list_search_indexes())
        index_names = [idx.get('name') for idx in indexes]
        
        if index_name not in index_names:
            logger.warning(
                f"Vector search index '{index_name}' not found in collection. "
                "Please create it via MongoDB Atlas UI or API."
            )
            logger.warning(
                f"The index should be created on field '{embedding_field}' "
                f"with dimension {dimension}."
            )
    except Exception as e:
        logger.error(f"Error checking vector index: {str(e)}")

def insert_documents(
    collection: Collection, 
    documents: List[Dict[str, Any]],
    embedding_field: str
) -> int:
    """
    Insert documents with embeddings into MongoDB.
    
    Args:
        collection: MongoDB collection
        documents: List of documents with embeddings
        embedding_field: Field name for embeddings
        
    Returns:
        int: Number of documents inserted
    """
    # Validate documents
    for doc in documents:
        if embedding_field not in doc:
            raise ValueError(f"Document missing required field: {embedding_field}")
            
    # Insert documents
    if documents:
        result = collection.insert_many(documents)
        return len(result.inserted_ids)
    return 0

def vector_search(
    collection: Collection,
    query_vector: List[float],
    embedding_field: str,
    vector_index: str,
    limit: int = 10
) -> List[Dict[str, Any]]:
    """
    Perform vector similarity search.
    
    Args:
        collection: MongoDB collection
        query_vector: Query embedding vector
        embedding_field: Field name for embeddings
        limit: Maximum number of results
        
    Returns:
        List[Dict]: Search results
    """
    pipeline = [
        {
            "$vectorSearch": {
                "index": vector_index,
                "path": embedding_field,
                "queryVector": query_vector,
                "numCandidates": limit * 10,
                "limit": limit
            }
        },
        {
            "$project": {
                "score": {"$meta": "vectorSearchScore"},
                "text": 1,
                "metadata": 1
            }
        }
    ]
    
    results = list(collection.aggregate(pipeline))
    return results