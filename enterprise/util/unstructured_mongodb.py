import json
import sys
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from time import time, sleep
from typing import TYPE_CHECKING, Any, Generator, Optional, List
import hashlib
import copy

from pydantic import Field, Secret


from unstructured_ingest.__version__ import __version__ as unstructured_version
from unstructured_ingest.error import DestinationConnectionError, SourceConnectionError
from unstructured_ingest.utils.data_prep import batch_generator, flatten_dict
from unstructured_ingest.utils.dep_check import requires_dependencies
from unstructured_ingest.v2.interfaces import (
    AccessConfig,
    ConnectionConfig,
    Downloader,
    DownloaderConfig,
    FileData,
    FileDataSourceMetadata,
    Indexer,
    IndexerConfig,
    SourceIdentifiers,
    Uploader,
    UploaderConfig,
    UploadStager,
    UploadStagerConfig,
    download_responses,
)
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.v2.processes.connector_registry import (
    DestinationRegistryEntry,
    SourceRegistryEntry,
)
from unstructured_ingest.v2.processes.connectors.mongodb import mongodb_destination_entry

from pymongo import MongoClient
from pymongo.operations import SearchIndexModel, DeleteMany, InsertOne
from pymongo import ASCENDING

from tqdm import tqdm

CONNECTOR_TYPE = "mongodb"
SERVER_API_VERSION = "1"


class MongoDBAccessConfig(AccessConfig):
    uri: Optional[str] = Field(
        default=None, description="URI to user when connecting")



class MongoDBConnectionConfig(ConnectionConfig):
    access_config: Secret[MongoDBAccessConfig] = Field(
        default=MongoDBAccessConfig(), validate_default=True
    )
    host: Optional[str] = Field(
        default=None,
        description="hostname or IP address or Unix domain socket path of a single mongod or "
        "mongos instance to connect to, or a list of hostnames",
    )
    database: Optional[str] = Field(
        default=None, description="database name to connect to")
    collection: Optional[str] = Field(
        default=None, description="collection name to connect to")
    port: int = Field(default=27017)
    connector_type: str = Field(default=CONNECTOR_TYPE, init=False)
    index_name: Optional[str] = Field(
        default="default", description="Name of the index to be created or used")
    embedding_path: Optional[str] = Field(
        default=None, description="Path to the embedding field in the document")
    embedding_dimensions: Optional[int] = Field(
        default=None, description="Number of dimensions in the embedding")
    id_fields: Optional[List[str]] = Field(
        default=None, description="Fields to be used to create the document ID")
    create_md5: Optional[bool] = Field(
        default=False, description="Whether to create an MD5 hash of the document")



class MongoDBUploadStagerConfig(UploadStagerConfig):
    pass


class MongoDBIndexerConfig(IndexerConfig):
    batch_size: int = Field(
        default=100, description="Number of records per batch")



class MongoDBDownloaderConfig(DownloaderConfig):
    pass


@dataclass
class MongoDBIndexer(Indexer):
    connection_config: MongoDBConnectionConfig
    index_config: MongoDBIndexerConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        """Validates the connection to the MongoDB server."""
        try:
            client = self.create_client()
            client.admin.command("ping")
        except Exception as e:
            logger.error(f"Failed to validate connection: {e}", exc_info=True)
            raise SourceConnectionError(f"Failed to validate connection: {e}")

    @requires_dependencies(["pymongo"], extras="mongodb")
    def create_client(self) -> "MongoClient":
        from pymongo import MongoClient
        from pymongo.driver_info import DriverInfo
        from pymongo.server_api import ServerApi
        import certifi

        access_config = self.connection_config.access_config.get_secret_value()

        if access_config.uri:
            return MongoClient(
                access_config.uri,
                server_api=ServerApi(version=SERVER_API_VERSION),
                driver=DriverInfo(name="unstructured",
                                  version=unstructured_version),

                tlsCAFile=certifi.where(),
            )
        else:
            return MongoClient(
                host=self.connection_config.host,
                port=self.connection_config.port,
                server_api=ServerApi(version=SERVER_API_VERSION),
                tlsCAFile=certifi.where(),
            )

    def run(self, **kwargs: Any) -> Generator[FileData, None, None]:
        """Generates FileData objects for each document in the MongoDB collection."""
        client = self.create_client()
        database = client[self.connection_config.database]
        collection = database[self.connection_config.collection]

        # Get list of document IDs
        ids = collection.distinct("_id")
        batch_size = self.index_config.batch_size if self.index_config else 100

        for id_batch in batch_generator(ids, batch_size=batch_size):
            # Make sure the hash is always a positive number to create identifier
            batch_id = str(hash(frozenset(id_batch)) + sys.maxsize + 1)

            metadata = FileDataSourceMetadata(
                date_processed=str(time()),
                record_locator={
                    "database": self.connection_config.database,
                    "collection": self.connection_config.collection,
                },
            )

            file_data = FileData(
                identifier=batch_id,
                doc_type="batch",
                connector_type=self.connector_type,
                metadata=metadata,
                additional_metadata={
                    "ids": [str(doc_id) for doc_id in id_batch],
                },
            )
            yield file_data


@dataclass
class MongoDBDownloader(Downloader):
    download_config: MongoDBDownloaderConfig
    connection_config: MongoDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    @requires_dependencies(["pymongo"], extras="mongodb")
    def create_client(self) -> "MongoClient":
        from pymongo import MongoClient
        from pymongo.driver_info import DriverInfo
        from pymongo.server_api import ServerApi
        import certifi

        access_config = self.connection_config.access_config.get_secret_value()

        if access_config.uri:
            return MongoClient(
                access_config.uri,
                server_api=ServerApi(version=SERVER_API_VERSION),
                driver=DriverInfo(name="unstructured",
                                  version=unstructured_version),

                tlsCAFile=certifi.where(),
            )
        else:
            return MongoClient(
                host=self.connection_config.host,
                port=self.connection_config.port,
                server_api=ServerApi(version=SERVER_API_VERSION),
                tlsCAFile=certifi.where(),
            )

    @SourceConnectionError.wrap
    @requires_dependencies(["bson"], extras="mongodb")
    def run(self, file_data: FileData, **kwargs: Any) -> download_responses:
        """Fetches the document from MongoDB and writes it to a file."""
        from bson.errors import InvalidId
        from bson.objectid import ObjectId

        client = self.create_client()
        database = client[self.connection_config.database]
        collection = database[self.connection_config.collection]

        ids = file_data.additional_metadata.get("ids", [])
        if not ids:
            raise ValueError("No document IDs provided in additional_metadata")

        object_ids = []
        for doc_id in ids:
            try:
                object_ids.append(ObjectId(doc_id))
            except InvalidId as e:
                error_message = f"Invalid ObjectId for doc_id '{doc_id}': {str(e)}"
                logger.error(error_message)
                raise ValueError(error_message) from e

        try:
            docs = list(collection.find({"_id": {"$in": object_ids}}))
        except Exception as e:
            logger.error(f"Failed to fetch documents: {e}", exc_info=True)
            raise e

        download_responses = []
        for doc in docs:
            doc_id = doc["_id"]
            doc.pop("_id", None)

            # Extract date_created from the document or ObjectId
            date_created = None
            if "date_created" in doc:
                # If the document has a 'date_created' field, use it
                date_created = doc["date_created"]
                if isinstance(date_created, datetime):
                    date_created = date_created.isoformat()
                else:
                    # Convert to ISO format if it's a string
                    date_created = str(date_created)
            elif isinstance(doc_id, ObjectId):
                # Use the ObjectId's generation time
                date_created = doc_id.generation_time.isoformat()

            flattened_dict = flatten_dict(dictionary=doc)
            concatenated_values = "\n".join(
                str(value) for value in flattened_dict.values())


            # Create a FileData object for each document with source_identifiers
            individual_file_data = FileData(
                identifier=str(doc_id),
                connector_type=self.connector_type,
                source_identifiers=SourceIdentifiers(
                    filename=str(doc_id),
                    fullpath=str(doc_id),
                    rel_path=str(doc_id),
                ),
            )

            # Determine the download path
            download_path = self.get_download_path(individual_file_data)
            if download_path is None:
                raise ValueError("Download path could not be determined")

            download_path.parent.mkdir(parents=True, exist_ok=True)
            download_path = download_path.with_suffix(".txt")

            # Write the concatenated values to the file
            with open(download_path, "w", encoding="utf8") as f:
                f.write(concatenated_values)

            individual_file_data.local_download_path = str(download_path)

            # Update metadata
            individual_file_data.metadata = FileDataSourceMetadata(
                date_created=date_created,  # Include date_created here
                date_processed=str(time()),
                record_locator={
                    "database": self.connection_config.database,
                    "collection": self.connection_config.collection,
                    "document_id": str(doc_id),
                },
            )

            download_response = self.generate_download_response(
                file_data=individual_file_data, download_path=download_path
            )
            download_responses.append(download_response)

        return download_responses


@dataclass
class MongoDBUploadStager(UploadStager):
    upload_stager_config: MongoDBUploadStagerConfig = field(
        default_factory=lambda: MongoDBUploadStagerConfig()
    )

    def run(
        self,
        elements_filepath: Path,
        file_data: FileData,
        output_dir: Path,
        output_filename: str,
        **kwargs: Any,
    ) -> Path:
        with open(elements_filepath) as elements_file:
            elements_contents = json.load(elements_file)

        output_path = Path(output_dir) / Path(f"{output_filename}.json")
        with open(output_path, "w") as output_file:
            json.dump(elements_contents, output_file)
        return output_path


class MongoDBUploaderConfig(UploaderConfig):
    batch_size: int = Field(
        default=100, description="Number of records per batch")



@dataclass
class MongoDBUploader(Uploader):
    upload_config: MongoDBUploaderConfig
    connection_config: MongoDBConnectionConfig
    connector_type: str = CONNECTOR_TYPE

    def precheck(self) -> None:
        try:
            client = self.create_client()
            client.admin.command("ping")
        except Exception as e:
            logger.error(f"failed to validate connection: {e}", exc_info=True)
            raise DestinationConnectionError(
                f"failed to validate connection: {e}")


    @requires_dependencies(["pymongo"], extras="mongodb")
    def create_client(self) -> "MongoClient":
        from pymongo import MongoClient
        from pymongo.driver_info import DriverInfo
        from pymongo.server_api import ServerApi
        import certifi

        access_config = self.connection_config.access_config.get_secret_value()

        if access_config.uri:
            return MongoClient(
                access_config.uri,
                server_api=ServerApi(version=SERVER_API_VERSION),
                driver=DriverInfo(name="unstructured",
                                  version=unstructured_version),

                tlsCAFile=certifi.where(),
            )
        else:
            return MongoClient(
                host=self.connection_config.host,
                port=self.connection_config.port,
                server_api=ServerApi(version=SERVER_API_VERSION),
                tlsCAFile=certifi.where(),
            )

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            "writing %d objects to destination db, %s, collection %s at %s",
            len(elements_dict),
            self.connection_config.database,
            self.connection_config.collection,
            self.connection_config.host,
        )
        client = self.create_client()
        db = client[self.connection_config.database]
        collection = db[self.connection_config.collection]
        for chunk in batch_generator(elements_dict, self.upload_config.batch_size):

            collection.insert_many(chunk)


@dataclass
class MAAPUploader(MongoDBUploader):

    def _get_search_index_model(self):
        return SearchIndexModel(
            name=self.connection_config.index_name,
            type="vectorSearch",
            definition={
                "fields": [
                    {
                        "path": self.connection_config.embedding_path,
                        "type": "vector",
                        "numDimensions": self.connection_config.embedding_dimensions,
                        "similarity": "cosine"
                    }
                ]
            }
        )

    def _get_index_config(self, collection, index_name):
        idxs = list(collection.list_search_indexes())
        for ele in idxs:
            if ele["name"] == index_name:
                return ele

    def _check_n_create_index(self):
        client = self.create_client()
        db = client[self.connection_config.database]
        collection = db[self.connection_config.collection]
        index_name = self.connection_config.index_name
        print(">>>>>>>>Index name: ", index_name, "<<<<<<<<<<")
        idx = self._get_index_config(collection, index_name)
        print(idx)
        if not idx:
            print("I am here!!!!")
            logger.info("Creating search index ...")
            search_index_model = self._get_search_index_model()
            collection.create_search_index(search_index_model)
            while True:
                idx = self._get_index_config(collection, index_name)
                if idx and idx["queryable"]:
                    print("Search index created successfully.")
                    break
                else:
                    print("Waiting for search index to be created ...")
                    sleep(5)
        else:
            logger.info("Search index already exists.")

    def _create_id_from_doc(self, doc: dict, fields: list, create_md5: bool = False) -> str:
        all_field_vals = [
            str(self._get_nested_value(doc, field)) for field in fields if self._get_nested_value(doc, field)
        ]
        doc_id = "_".join(all_field_vals)
        if create_md5:
            doc_id = hashlib.md5(doc_id.encode()).hexdigest()
        return doc_id

    def _get_nested_value(self, doc: dict, field_path: str) -> Any:
        for subfield in field_path.split("."):
            if isinstance(doc, dict) and subfield in doc:
                doc = doc[subfield]
            else:
                return None
        return doc


    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            "writing %d objects to destination db, %s, collection %s at %s",
            len(elements_dict),
            self.connection_config.database,
            self.connection_config.collection,
            self.connection_config.host,
        )
        client = self.create_client()
        db = client[self.connection_config.database]
        collection = db[self.connection_config.collection]
        elements = elements_dict.copy()
        for doc in elements:
            doc["doc_id"] = self._create_id_from_doc(
                doc, self.connection_config.id_fields, self.connection_config.create_md5)
        ids = set(map(lambda x: x["doc_id"], elements_dict))
        ids = filter(lambda x: collection.find_one({"doc_id": x}), ids)
        print(ids)
        collection.delete_many({"doc_id": {"$in": list(ids)}})
        for chunk in tqdm(batch_generator(elements, self.upload_config.batch_size)):
            operations = []
            # delete the pre-exisiting doc_id and add new
            operations += [InsertOne(doc) for doc in chunk]
            collection.bulk_write(operations)
        # create search index if not exists
        self._check_n_create_index()

