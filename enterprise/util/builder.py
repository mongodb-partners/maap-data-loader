import os

# Import connection configuration classes for each data source
from unstructured_ingest.v2.interfaces import ProcessorConfig
from unstructured_ingest.v2.pipeline.pipeline import Pipeline
from unstructured_ingest.v2.processes.chunker import ChunkerConfig
from unstructured_ingest.v2.processes.connectors.mongodb import (
    mongodb_destination_entry,
    MongoDBUploadStagerConfig,
    MongoDBUploadStager
)

from enterprise.util.unstructured_mongodb import (
    MongoDBAccessConfig,
    MongoDBConnectionConfig,
    MongoDBUploaderConfig,
    MAAPUploader,
)

from unstructured_ingest.v2.processes.embedder import EmbedderConfig
from unstructured_ingest.v2.processes.partitioner import PartitionerConfig

from enterprise.mongodb_ingest import CustomMongoDBUploader

from enterprise.util.base_configs import Config, DestinationConfig, SourceConfig
from enterprise.util.configs.source import SourceConnectionFactory
from enterprise.util.configs.indexer import IndexerFactory
from enterprise.util.configs.downloader import DownloaderFactory
from pymongo import MongoClient

class PipelineBuilder:
    def __init__(self):
        # TODO: Add default values
        self.pipeline = None
        self.source_connection_config = None
        self.indexer_config = None
        self.downloader_config = None
        self.destination_connection_config = None
        self.uploader_config = MongoDBUploaderConfig(batch_size=100)
        self.stager_config = MongoDBUploadStagerConfig()
        self.chunker_config = None
        self.embedder_config = None



    def configure_source_connection(self, source: SourceConfig) -> 'PipelineBuilder':
        source_type = source.source_type
        credentials = source.credentials or {}
        self.source_connection_config = SourceConnectionFactory.get_source_connection(source_type, credentials)
        return self

    def configure_indexer(self, source: SourceConfig) -> 'PipelineBuilder':
        source_type = source.source_type
        self.indexer_config = IndexerFactory.get_indexer_connection(source_type, source.params)
        return self

    def configure_downloader(self, source: SourceConfig) -> 'PipelineBuilder':
        source_type = source.source_type
        self.downloader_config = DownloaderFactory.get_downloader_connection(source_type, source.params)
        return self

    def configure_destination(self, config: DestinationConfig ) -> 'PipelineBuilder':
        mongodb_destination_entry.connection_config = MongoDBConnectionConfig
        self.destination_connection_config = MongoDBConnectionConfig(
            access_config=MongoDBAccessConfig(uri=config.mongodb_uri),
            collection=config.collection,
            index_name=config.index_name,
            database=config.database,
            embedding_dimensions=config.embedding_dimensions,
            embedding_path=config.embedding_path,
            id_fields=config.id_fields,
            create_md5=config.create_md5 if config.create_md5 else True, # by default, create MD5 hash
        )
        return self
    
    def configure_uploader(self, config: DestinationConfig) -> 'PipelineBuilder':
        mongodb_destination_entry.uploader_config = MongoDBUploaderConfig
        self.uploader_config = MongoDBUploaderConfig(batch_size=config.batch_size)
        return self
    
    def configure_stager(self) -> 'PipelineBuilder':
        mongodb_destination_entry.stager_config = MongoDBUploadStagerConfig
        self.stager_config = MongoDBUploadStagerConfig()
        return self

    def processor_config(self) -> ProcessorConfig:
        return ProcessorConfig(
                reprocess=True,
                verbose=True,
                tqdm=True,
                num_processes=5,
                work_dir="./content/temp",
                re_download=True,
            )
    
    def partition_config(self) -> PartitionerConfig:
        if os.getenv("RUN_ENV", "local") == "local":
            return PartitionerConfig(
                    partition_by_api=False,
                    strategy="hi_res",
                    fields_include=["element_id", "text", "type", "metadata"],
                    flatten_metadata=True,
                    metadata_exclude=["filename"],
                    additional_partition_args={
                        "include_page_breaks": True,
                        "ocr_languages": ["eng"]
                    },
            )
        else:
            return PartitionerConfig(
                    partition_by_api=True,
                    api_key=os.getenv("UNSTRUCTURED_API_KEY"),
                    partition_endpoint=os.getenv("UNSTRUCTURED_URL"),
                )
    
    def configure_chunker_config(self, config: SourceConfig) -> 'PipelineBuilder':
        if config.params:
            self.chunker_config = ChunkerConfig(
                    chunking_strategy=config.params.get("chunking_strategy", "by_title"),
                    chunk_max_characters=config.params.get("chunk_max_characters", 1500),
                    chunk_overlap=config.params.get("chunk_overlap", 100),
                )
        else:
            raise ValueError("Source configuration params not provided")
        return self
    
    def configure_embedder_config(self) -> 'PipelineBuilder':
        self.embedder_config = EmbedderConfig(
                embedding_provider="huggingface",
                embedding_model_name="all-MiniLM-L6-v2",
            )
        return self
    
    #Build the pipeline
    def build(self) -> Pipeline:
        mongodb_destination_entry.uploader = MAAPUploader
        print(self.stager_config)
        self.pipeline = Pipeline.from_configs(
            context= self.processor_config(),
            indexer_config=self.indexer_config,
            downloader_config=self.downloader_config,
            partitioner_config=self.partition_config(),
            chunker_config=self.chunker_config,
            embedder_config=self.embedder_config,
            source_connection_config=self.source_connection_config,
            destination_connection_config=self.destination_connection_config,
            stager_config=self.stager_config,
            uploader_config=self.uploader_config,
        )
        return self
    
def start_pipeline(config: Config):
    source_config = config.source
    destination_config = config.destination
    builder = PipelineBuilder()
    builder = builder.configure_source_connection(source_config)\
        .configure_indexer(source_config)\
        .configure_downloader(source_config)\
        .configure_destination(destination_config)\
        .configure_uploader(destination_config)\
        .configure_stager()\
        .configure_chunker_config(source_config)\
        .configure_embedder_config()
    pipeline = builder.build().pipeline
    pipeline.run()

if __name__=="__main__":
    # Example test case for PipelineBuilder
    source_config = SourceConfig(
        source_type="s3",
        credentials={"aws_secret_access_key":"XrMCse3bwu0sabibpwxEJzmI7/VqUM2tkRbSs+zc","aws_session_token":"IQoJb3JpZ2luX2VjEAoaCXVzLWVhc3QtMSJHMEUCIBr5gCn0DAnnE1WWgL2FZ35WTkuWi9eVc8hCVqSnqeWKAiEA307FLdhaDfZcl/sgARvAG7eoNTKF0KynabA1Bhd7O1IqwwMIExAEGgw5Nzk1NTkwNTYzMDciDNKYesf7weH44m/2PCqgA4Q+sGawv5Ry/EjTLw/vWl/vrI7+Xouy88CQadIZPbFDkP/Z2hkZEvGkX3KsqNqW2QijaZcgMXSNhSQlVdj0/DqkuCIdqhiWjbxS2tIrfKIpgy1P+O6R90ivaXbHnsgHwwpbIWbPCPfymm9mPnqLrDU3M/LQ9WGdWzuF04HvbRYKb/yfw+hukyBrRBKNI1ViM/zYwCf3hnimP0b67rejx/Bddxlg8q4MQSzwUzPXMGNqkyVMbjrLcVfLsVmBh0pngTkSnQ8e/o9a2NAO1Ejd6IQqyjpCrLnhaKaxlvFxLEuF+WRF/mnGdRe4bjdTx+t6fYwBnrhzDspYwzj8bJqDnrz00kO+la864CELI4jrc04hm77V0mLbfiXlja1PJO5G/BoeGwKxbK5l6IElailYRIKNmGUUcNC31IyDtFv08tKH6THLsIfyc815mSFhVNkpgfaXLMRJdF0dHPXRLorKad56kj4T2jsjcosB0QUiv6zuI7x0ay5c8iiTt90NOy8Hoa5sCNnLyouBJczNiDV7ahM+nlXaCrp7Rtb9Nn6iovZLMOO5zbwGOqYB+gJWWCSaZDNH4qaShq6vi7Lg4o9vv7CxUV/gR9ZVfLuoYwkt+nsFgG4HRDB5GvoX3UK+iIZLbqZeSHJNmE5b8YJZLSKwMudMjSNUKw0R56IOL4rPHnzKBvWI4CY67pngOzdQ2Jc7bMda3m4wHTR4x2GKSi828B/QleKV2Of50azKpNMO7T/15C53PgClzaD/YJfyvwcqKOrc2/elhIzViIKswCthsw=="},
        params={"remote_url": "s3://ashwin-partner-bucket/testunstruct", "chunking_strategy": "by_title", "chunk_max_characters": 1500, "chunk_overlap": 100}
    )
    destination_config = DestinationConfig(
        mongodb_uri=os.getenv("MONGODB_URI"),
        collection="test_collection",
        database="test_database",
        embedding_dimensions=384,
        embedding_path="embeddings",
        id_fields=["metadata.data_source.url"],
        create_md5=True,
        batch_size=100
    )

    builder = PipelineBuilder()
    builder.configure_source_connection(source_config)
    builder.configure_indexer(source_config)
    builder.configure_downloader(source_config)
    builder.configure_destination(destination_config)
    builder.configure_uploader(destination_config)
    builder.configure_chunker_config(source_config)
    builder.configure_embedder_config()
    builder.build()
    builder.pipeline.run()  # Uncomment to run the pipeline
