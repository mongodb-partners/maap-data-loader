import boto3
from typing import List
from functools import lru_cache
import json
import config
from local.utils.logger import logger

class BedrockService:
    def __init__(self, region_name: str = config.AWS_REGION):
        """
        Initialize the Bedrock service.
        
        Args:
            region_name: AWS region name
        """
        self.client = boto3.client("bedrock-runtime", region_name=region_name)
        self.embedding_model_id = config.EMBEDDING_MODEL_ID
    
    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """
        Generate embeddings for a list of texts.
        
        Args:
            texts: List of texts to generate embeddings for
            
        Returns:
            List[List[float]]: List of embedding vectors
        """
        if not texts:
            return []
            
        embeddings = []
        
        for text in texts:
            try:
                response = self.client.invoke_model(
                    modelId=self.embedding_model_id,
                    contentType="application/json",
                    accept="application/json",
                    body=json.dumps({
                        "inputText": text
                    })
                )
                
                response_body = json.loads(response["body"].read())
                embedding = response_body.get("embedding")
                
                if not embedding:
                    logger.error(f"Failed to generate embedding: {response_body}")
                    # Use a zero vector as fallback
                    embedding = [0.0] * config.VECTOR_DIMENSION
                
                embeddings.append(embedding)
                
            except Exception as e:
                logger.error(f"Error generating embeddings: {str(e)}")
                # Use a zero vector as fallback
                embeddings.append([0.0] * config.VECTOR_DIMENSION)
        
        return embeddings

@lru_cache()
def get_bedrock_service() -> BedrockService:
    """
    Get a cached Bedrock service instance.
    
    Returns:
        BedrockService: Bedrock service instance
    """
    return BedrockService()