import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Application settings
APP_NAME = "MAAP-Loader"
APP_VERSION = "1.0"
APP_DESCRIPTION = "MongoDB AI Applications Program"
DEBUG = os.getenv("DEBUG", "False").lower() == "true"

# Service configuration
SERVICE_HOST = os.getenv("SERVICE_HOST", "0.0.0.0")
SERVICE_PORT = int(os.getenv("SERVICE_PORT", "8184"))
LOGGER_SERVICE_URL = os.getenv("LOGGER_SERVICE_URL", "http://event-logger:8181")

# AWS configuration
AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
EMBEDDING_MODEL_ID = os.getenv("EMBEDDING_MODEL_ID", "amazon.titan-embed-text-v1")

# File handling
UPLOAD_DIR = os.getenv("UPLOAD_DIR", os.path.join(os.getcwd(), "uploaded_files"))
MAX_DOCUMENT_CHARACTERS = int(os.getenv("MAX_DOCUMENT_CHARACTERS", "10000"))

# Create upload directory if it doesn't exist
os.makedirs(UPLOAD_DIR, exist_ok=True)

# MongoDB configuration
MONGODB_CONNECTION_TIMEOUT = int(os.getenv("MONGODB_CONNECTION_TIMEOUT", "5000"))  # ms

# Vector search configuration
VECTOR_DIMENSION = int(os.getenv("VECTOR_DIMENSION", "1536"))  # Titan embeddings dimension