from pathlib import Path

import json
from pymongo import UpdateOne
from unstructured_ingest.utils.data_prep import batch_generator
from unstructured_ingest.v2.logger import logger
from unstructured_ingest.error import DestinationConnectionError
from typing import Any

# For pipeline using a MongoDB destination
from unstructured_ingest.v2.processes.connectors.mongodb import MongoDBUploader

from unstructured_ingest.v2.interfaces import FileData


class CustomMongoDBUploader(MongoDBUploader):
    """
    This module provides functionality to ingest data into a MongoDB database.
    Classes:
        CustomMongoDBUploader: A custom uploader class that extends MongoDBUploader to handle
                               the ingestion of data from JSON files into a MongoDB collection.
    Functions:
        CustomMongoDBUploader.run(path: Path, file_data: FileData, **kwargs: Any) -> None:
            Reads data from a JSON file, logs the operation, and writes the data to a MongoDB
            collection in batches using bulk write operations.
            Parameters:
                path (Path): The path to the JSON file containing the data to be ingested.
                file_data (FileData): Metadata about the file being processed.
                **kwargs (Any): Additional keyword arguments.
            Raises:
                DestinationConnectionError: If there is an error during the bulk write operation.
    """

    def run(self, path: Path, file_data: FileData, **kwargs: Any) -> None:
        with path.open("r") as file:
            elements_dict = json.load(file)
        logger.info(
            f"Writing {len(elements_dict)} objects to destination "
            f"db: {self.connection_config.database}, "
            f"collection: {self.connection_config.collection} "
            f"at {self.connection_config.host}"
        )

        # Create MongoDB client
        client = self.create_client()
        db = client[self.connection_config.database]
        collection = db[self.connection_config.collection]

        # Prepare batch update operations
        for chunk in batch_generator(elements_dict, self.upload_config.batch_size):
            operations = [
                UpdateOne({"text": record["text"]}, {"$set": record}, upsert=True)
                for record in chunk
            ]
            if operations:
                try:
                    collection.bulk_write(operations, ordered=False)
                    logger.info(
                        f"Batch of {len(operations)} records processed successfully."
                    )
                except Exception as e:
                    logger.error(f"Error during bulk write: {e}", exc_info=True)
                    raise DestinationConnectionError(f"Bulk write failed: {e}")
