from typing import List, Dict
from local.services.document_service import DocumentService
from local.database.mongodb import get_collection, ensure_vector_index, insert_documents
from local.utils.logger import logger

class EmbeddingService:
    def __init__(self):
        """Initialize the embedding service."""
        self.document_service = DocumentService()
    
    async def process_and_store_files(
        self,
        file_paths: List[str],
        user_id: str,
        mongodb_config: Dict[str, str]
    ) -> int:
        """
        Process files and store documents in MongoDB.
        
        Args:
            file_paths: List of file paths
            user_id: User ID
            mongodb_config: MongoDB configuration
            
        Returns:
            int: Number of documents stored
        """
        # Process files and get documents with embeddings
        documents = await self.document_service.process_files(file_paths, user_id)
        
        if not documents:
            return 0
        
        # Get MongoDB collection
        collection = get_collection(
            mongodb_config["uri"],
            mongodb_config["database"],
            mongodb_config["collection"]
        )
        
        # Ensure vector index exists
        ensure_vector_index(
            collection, 
            mongodb_config["index_name"],
            mongodb_config["embedding_field"]
        )
        
        # Insert documents
        count = insert_documents(
            collection, 
            documents, 
            mongodb_config["embedding_field"]
        )
        
        logger.info(f"Stored {count} documents from files in MongoDB")
        return count
    
    async def process_and_store_urls(
        self,
        urls: List[str],
        user_id: str,
        mongodb_config: Dict[str, str]
    ) -> int:
        """
        Process URLs and store documents in MongoDB.
        
        Args:
            urls: List of URLs
            user_id: User ID
            mongodb_config: MongoDB configuration
            
        Returns:
            int: Number of documents stored
        """
        # Process URLs and get documents with embeddings
        documents = await self.document_service.process_web_urls(urls, user_id)
        
        if not documents:
            return 0
        
        # Get MongoDB collection
        collection = get_collection(
            mongodb_config["uri"],
            mongodb_config["database"],
            mongodb_config["collection"]
        )
        
        # Ensure vector index exists
        ensure_vector_index(
            collection, 
            mongodb_config["index_name"],
            mongodb_config["embedding_field"]
        )
        
        # Insert documents
        count = insert_documents(
            collection, 
            documents, 
            mongodb_config["embedding_field"]
        )
        
        logger.info(f"Stored {count} documents from URLs in MongoDB")
        return count