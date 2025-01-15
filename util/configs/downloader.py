from traceback import print_exc

from unstructured_ingest.v2.processes.connectors.local import (
    LocalDownloaderConfig
)

from unstructured_ingest.v2.processes.connectors.fsspec.s3 import (
    S3DownloaderConfig
)

from unstructured_ingest.v2.processes.connectors.google_drive import (
    GoogleDriveDownloaderConfig
)

import os

class DownloaderFactory:
    @staticmethod
    def get_downloader_connection(source_type, params=None):
        # if params is None:
        #     raise ValueError("Params cannot be None")
        try:
            if source_type == "local":
                return LocalDownloaderConfig()
            elif source_type == "s3":
                return S3DownloaderConfig()
            elif source_type == "google_drive":
                return GoogleDriveDownloaderConfig()
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
        except Exception as e:
            print_exc()
            raise ValueError(f"Error in getting downloader connection: {e}") from e