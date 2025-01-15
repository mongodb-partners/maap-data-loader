from datetime import datetime
from typing import Dict, Any
from pymongo import MongoClient
from pymongo.errors import PyMongoError
import certifi
import os
from dotenv import load_dotenv
from functools import lru_cache
from util.builder import start_pipeline
from util.base_configs import Config

load_dotenv()

class MongoDBConnection:
    @staticmethod
    @lru_cache(1)
    def get_collection():
        """Get MongoDB collection with cached connection."""
        try:
            client = MongoClient(
                os.getenv("MONGODB_URI"),
                tlsCAFile=certifi.where()
            )
            return client[os.getenv("MONGODB_DATABASE")][os.getenv("MONGODB_COLLECTION")]
        except PyMongoError as e:
            raise ConnectionError(f"Error connecting to MongoDB: {e}") from e

class PipelineExecutor:
    DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
    
    def __init__(self):
        self.collection = MongoDBConnection.get_collection()
    
    def _update_entry_status(self, entry_id: str, status: str, last_run: datetime = None) -> None:
        """Update the status and optionally the last_run time of an entry."""
        update_data = {"status": status}
        if last_run:
            update_data["last_run"] = last_run.strftime(self.DATE_FORMAT)
        self.collection.update_one({"_id": entry_id}, {"$set": update_data})
    
    def _should_run_pipeline(self, entry: Dict[str, Any]) -> bool:
        """Check if pipeline should run based on entry conditions."""
        if entry["status"] != "completed":
            return False
        
        last_run = datetime.strptime(entry["last_run"], self.DATE_FORMAT)
        time_elapsed = (datetime.now() - last_run).total_seconds()
        return time_elapsed > entry["sync_interval_seconds"]
    
    def run_scheduled_jobs(self) -> None:
        """Execute scheduled pipeline jobs."""
        for entry in self.collection.find({}):
            if not self._should_run_pipeline(entry):
                continue
                
            self._execute_pipeline(entry)
    
    def _execute_pipeline(self, entry: Dict[str, Any]) -> None:
        """Execute a single pipeline."""
        try:
            self._update_entry_status(entry["_id"], "running")
            config = Config(**entry)
            start_pipeline(config)
            self._update_entry_status(entry["_id"], "completed", datetime.now())
        except Exception as e:
            self._update_entry_status(entry["_id"], "failed")
            raise RuntimeError(f"Pipeline execution failed: {e}") from e
    
    def execute_first_time(self, data: Dict[str, Any]) -> None:
        """Execute pipeline for the first time and store in database."""
        config = Config(**data)
        if not config.sync_interval_seconds:
            config.sync_interval_seconds = 10_000_000
            
        start_pipeline(config)
        data.update({
            "status": "completed",
            "last_run": datetime.now().strftime(self.DATE_FORMAT)
        })
        self.collection.insert_one(data)
    
    def delete_job(self, data) -> None:
        """Delete a scheduled job by ID."""
        self.collection.delete_one(data)


# For backwards compatibility
def job():
    """Legacy wrapper for scheduled jobs."""
    executor = PipelineExecutor()
    executor.run_scheduled_jobs()

def execute_pipeline_first_time(data: Dict[str, Any]):
    """Legacy wrapper for first-time execution."""
    executor = PipelineExecutor()
    executor.execute_first_time(data)

def delete_job(data: Dict[str, Any]):
    """Legacy wrapper for deleting a job."""
    executor = PipelineExecutor()
    executor.delete_job(data)