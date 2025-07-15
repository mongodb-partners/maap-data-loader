import asyncio
import httpx
from typing import List, Dict, Any
from datetime import datetime, timezone
from langchain_unstructured import UnstructuredLoader
from unstructured.cleaners.core import clean_extra_whitespace
import config
from local.utils.logger import logger
from local.services.bedrock_service import get_bedrock_service

class DocumentService:
    def __init__(self):
        self.bedrock_service = get_bedrock_service()
    
    async def _process_document(
        self,
        text: str, 
        metadata: Dict[str, Any], 
        user_id: str
    ) -> Dict[str, Any]:
        """
        Process document text and metadata.
        
        Args:
            text: Document text
            metadata: Document metadata
            user_id: User ID
            
        Returns:
            Dict: Processed document with embedding
        """
        # Add standard metadata
        metadata["user_id"] = user_id
        metadata["timestamp"] = datetime.now(timezone.utc)
        metadata["char_count"] = len(text)
        
        # Generate embedding
        embeddings = self.bedrock_service.generate_embeddings([text])
        embedding = embeddings[0] if embeddings else []
        
        document = {
            "text": text,
            "embedding": embedding,
            "metadata": metadata
        }
        
        logger.info(f"Processed document with {metadata['char_count']} characters")
        return document

    async def process_file(
        self,
        file_path: str,
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Process a file and extract documents with embeddings.
        
        Args:
            file_path: Path to the file
            user_id: User ID
            
        Returns:
            List[Dict]: List of documents with embeddings
        """
        try:
            loader = UnstructuredLoader(
                file_path=file_path,
                post_processors=[clean_extra_whitespace],
                chunking_strategy="basic",
                max_characters=config.MAX_DOCUMENT_CHARACTERS,
                include_orig_elements=False,
                strategy="hi_res",
            )
            
            raw_docs = loader.load()
            logger.info(f"Loaded {len(raw_docs)} documents from file: {file_path}")
            
            processed_docs = []
            for doc in raw_docs:
                # Update metadata with file info
                metadata = doc.metadata or {}
                metadata["source"] = "file"
                metadata["filename"] = file_path
                
                # Process document
                processed_doc = await self._process_document(
                    doc.page_content, 
                    metadata, 
                    user_id
                )
                processed_docs.append(processed_doc)
                
            return processed_docs
            
        except Exception as e:
            logger.error(f"Error processing file {file_path}: {str(e)}")
            raise
    
    async def process_web_url(
        self,
        url: str,
        user_id: str,
        retry_count: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Process a web URL and extract documents with embeddings.
        
        Args:
            url: URL to process
            user_id: User ID
            retry_count: Current retry attempt
            
        Returns:
            List[Dict]: List of documents with embeddings
        """
        try:
            loader = UnstructuredLoader(
                web_url=str(url),  # Convert from HttpUrl to str
                post_processors=[clean_extra_whitespace],
                chunking_strategy="basic",
                max_characters=config.MAX_DOCUMENT_CHARACTERS,
                include_orig_elements=False,
                strategy="hi_res",
            )
            
            raw_docs = loader.load()
            logger.info(f"Loaded {len(raw_docs)} documents from URL: {url}")
            
            processed_docs = []
            for doc in raw_docs:
                # Update metadata with URL info
                metadata = doc.metadata or {}
                metadata["source"] = "web"
                metadata["url"] = str(url)
                
                # Process document
                processed_doc = await self._process_document(
                    doc.page_content, 
                    metadata, 
                    user_id
                )
                processed_docs.append(processed_doc)
                
            return processed_docs
            
        except httpx.ReadTimeout:
            if retry_count < config.MAX_RETRIES:
                retry_count += 1
                logger.warning(f"Timeout loading URL: {url}. Retry {retry_count}/{config.MAX_RETRIES}")
                await asyncio.sleep(2 ** retry_count)  # Exponential backoff
                return await self.process_web_url(url, user_id, retry_count)
            else:
                logger.error(f"Failed to load URL after {config.MAX_RETRIES} retries: {url}")
                raise
                
        except Exception as e:
            logger.error(f"Error processing URL {url}: {str(e)}")
            raise
    
    async def process_files(
        self,
        file_paths: List[str],
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Process multiple files concurrently.
        
        Args:
            file_paths: List of file paths
            user_id: User ID
            
        Returns:
            List[Dict]: List of documents with embeddings
        """
        if not file_paths:
            return []
        
        tasks = [self.process_file(path, user_id) for path in file_paths]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_docs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process file {file_paths[i]}: {str(result)}")
            else:
                all_docs.extend(result)
        
        return all_docs
    
    async def process_web_urls(
        self,
        urls: List[str],
        user_id: str
    ) -> List[Dict[str, Any]]:
        """
        Process multiple web URLs concurrently.
        
        Args:
            urls: List of URLs
            user_id: User ID
            
        Returns:
            List[Dict]: List of documents with embeddings
        """
        if not urls:
            return []
        
        tasks = [self.process_web_url(url, user_id) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        all_docs = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                logger.error(f"Failed to process URL {urls[i]}: {str(result)}")
            else:
                all_docs.extend(result)
        
        return all_docs