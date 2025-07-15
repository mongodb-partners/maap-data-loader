from contextlib import asynccontextmanager
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from pydantic_settings import BaseSettings


class SourceConfig(BaseModel):
    source_type: str
    credentials: Optional[Dict[str, str]] = None
    params: Optional[Dict[str, str]] = None

class DestinationConfig(BaseModel):
    mongodb_uri: str
    database: str
    collection: str
    index_name: str = Field(default="default", description="Name of the index to be created or used")
    embedding_path: Optional[str] = Field(default="embeddings", description="Path to the embedding field in the document")
    embedding_dimensions: int = Field(default=None, description="Number of dimensions in the embedding")
    id_fields: Optional[List[str]] = Field(default=["text"], description="Fields to be used to create the document ID")
    create_md5: Optional[bool] = Field(default=False, description="Whether to create an MD5 hash of the document")
    batch_size: Optional[int] = Field(default=100, description="Number of documents to upload in each batch")



class Config(BaseModel):
    sync_interval_seconds: int
    source: SourceConfig
    destination: DestinationConfig


class AppConfig(BaseSettings):
    config_path: str = "config.yaml"  # Default path to config file