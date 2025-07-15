from pydantic import BaseModel, Field, HttpUrl
from typing import List, Optional, Dict, Any


class MongoDBConfig(BaseModel):
    """MongoDB connection configuration."""
    uri: str = Field(..., description="MongoDB connection URI")
    database: str = Field(..., description="Database name")
    collection: str = Field(..., description="Collection name")
    index_name: str = Field(..., description="Vector index name")
    text_field: str = Field("text", description="Field name for document text")
    embedding_field: str = Field("embedding", description="Field name for embeddings")

class UploadRequest(BaseModel):
    """Request model for document upload."""
    user_id: str = Field(..., description="User ID for the documents")
    mongodb_config: MongoDBConfig
    web_pages: Optional[List[HttpUrl]] = Field(default=[], description="URLs to process")

class Document(BaseModel):
    """Document model with text and metadata."""
    text: str = Field(..., description="Document content")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Document metadata")

class UploadResponse(BaseModel):
    """Response model for document upload."""
    success: bool = Field(..., description="Whether the operation was successful")
    message: str = Field(..., description="Operation result message")
    details: Dict[str, Any] = Field(default_factory=dict, description="Additional details")
    
class ErrorResponse(BaseModel):
    """Error response model."""
    success: bool = Field(False, description="Operation failed")
    error: str = Field(..., description="Error message")
    traceback: Optional[str] = Field(None, description="Error traceback (debug only)")