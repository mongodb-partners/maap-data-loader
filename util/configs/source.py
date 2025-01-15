from unstructured_ingest.v2.processes.connectors.local import (
    LocalConnectionConfig
)

from unstructured_ingest.v2.processes.connectors.fsspec.s3 import (
    S3AccessConfig,
    S3ConnectionConfig
)

from unstructured_ingest.v2.processes.connectors.google_drive import (
    GoogleDriveConnectionConfig,
    GoogleDriveAccessConfig
)

class SourceConnectionFactory:
    @staticmethod
    def get_source_connection(source_type, credentials=None):
        if source_type == "local":
            return LocalConnectionConfig()
        elif source_type == "s3":
            if credentials:
                access_config = S3AccessConfig(
                    key=credentials.get("aws_access_key_id"),
                    secret=credentials.get("aws_secret_access_key"),
                    token=credentials.get("aws_session_token")
                )
            else:
                access_config = S3AccessConfig()
            return S3ConnectionConfig(
                access_config=access_config
            )
        elif source_type == "google_drive":
            return GoogleDriveConnectionConfig(
                access_config=GoogleDriveAccessConfig(service_account_key=credentials.get("gcp_service_account_key_string")),
                drive_id=credentials.get("google_drive_folder_id")
            )

        else:
            raise ValueError(f"Unsupported source type: {source_type}")