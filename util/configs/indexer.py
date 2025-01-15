from unstructured_ingest.v2.processes.connectors.local import (
    LocalIndexerConfig
)

from unstructured_ingest.v2.processes.connectors.fsspec.s3 import (
    S3IndexerConfig
)

from unstructured_ingest.v2.processes.connectors.google_drive import (
    GoogleDriveIndexerConfig,
)

from traceback import print_exc

class IndexerFactory:
    @staticmethod
    def get_indexer_connection(source_type, params=None):
        # if params is None:
            # raise ValueError("Params cannot be None")        
        try:
            if source_type == "local":
                return LocalIndexerConfig()
            elif source_type == "s3":
                return S3IndexerConfig(remote_url=params.get("remote_url"))
            elif source_type == "google_drive":
                return GoogleDriveIndexerConfig()
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
        except Exception as e:
            print_exc()
            raise ValueError(f"Error in getting indexer connection: {e}") from e