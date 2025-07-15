import json
# import humanize
from typing import List, Dict, Any
from fastapi import FastAPI, File, Form, UploadFile, HTTPException, Depends, Request, status
from fastapi.middleware.cors import CORSMiddleware
from typing_extensions import Annotated
from pydantic import ValidationError
from contextlib import asynccontextmanager
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv
import uvicorn

# Import from local modules
import config as config
from local.utils.logger import logger
from local.utils import error_utils, file_utils
from local.models.pydantic_models import UploadRequest, UploadResponse, ErrorResponse
from local.services.embedding_service import EmbeddingService
from enterprise.pipeline_executor import PipelineExecutor

# Load environment variables
load_dotenv()

# Initialize FastAPI app
app = FastAPI(
    title=config.APP_NAME,
    version=config.APP_VERSION,
    description=config.APP_DESCRIPTION,
    docs_url="/docs",
    redoc_url="/redoc",
    openapi_tags=[
        {
            "name": "Local",
            "description": "Local file processing operations"
        },
        {
            "name": "Enterprise",
            "description": "Enterprise source management"
        },
        {
            "name": "System",
            "description": "System health and monitoring"
        }
    ]
)

# CORS configuration
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialize services
executor = PipelineExecutor()

# Dependencies
def get_embedding_service() -> EmbeddingService:
    return EmbeddingService()

# Lifespan for background scheduler
@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(executor.run_scheduled_jobs, "interval", minutes=1)
    scheduler.start()
    yield
    scheduler.shutdown()

app.router.lifespan_context = lifespan

# Health check endpoint
@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}

# Upload endpoint
@app.post(
    "/local/upload",
    response_model=UploadResponse,
    responses={
        200: {"model": UploadResponse},
        400: {"model": ErrorResponse},
        500: {"model": ErrorResponse}
    }
    )
async def upload(
    files: Annotated[List[UploadFile], File(description="Multiple files to upload")] = [],
    json_input_params: str = Form(description="Input parameters as a JSON string"),
    embedding_service: EmbeddingService = Depends(get_embedding_service)
):
    """
    Upload files and web URLs to be processed and stored in the vector database.
    """
    try:
        # Parse JSON input
        try:
            upload_data = json.loads(json_input_params)
            request = UploadRequest(**upload_data)
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Invalid JSON input"
            )
        except ValidationError as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid input parameters: {str(e)}"
            )
        
        # Convert MongoDB config to dictionary for service
        mongodb_config = {
            "uri": request.mongodb_config.uri,
            "database": request.mongodb_config.database,
            "collection": request.mongodb_config.collection,
            "index_name": request.mongodb_config.index_name,
            "text_field": request.mongodb_config.text_field,
            "embedding_field": request.mongodb_config.embedding_field
        }
        
        # Process files if any
        file_docs_count = 0
        if files:
            logger.info(f"Processing {len(files)} uploaded files")
            saved_files = file_utils.save_uploaded_files(files)
            file_docs_count = await embedding_service.process_and_store_files(
                saved_files, 
                request.user_id, 
                mongodb_config
            )
        
        # Process web URLs if any
        web_docs_count = 0
        if request.web_pages:
            web_urls = [str(url) for url in request.web_pages]
            logger.info(f"Processing {len(web_urls)} web URLs")
            web_docs_count = await embedding_service.process_and_store_urls(
                web_urls, 
                request.user_id, 
                mongodb_config
            )
        
        # Build response
        file_details = [
            f"{file.filename} " #({humanize.naturalsize(file.size)})" 
            for file in files
        ]
        
        web_details = [str(url) for url in request.web_pages]
        
        response = UploadResponse(
            success=True,
            message="Upload processed successfully",
            details={
                "files_processed": len(files),
                "urls_processed": len(request.web_pages),
                "documents_stored": file_docs_count + web_docs_count,
                "file_list": file_details,
                "url_list": web_details
            }
        )
        
        return response
        
    except HTTPException:
        # Pass through HTTP exceptions
        raise
    except Exception as e:
        error_response = error_utils.handle_exception(e)
        return ErrorResponse(**error_response)

# Source registration endpoint
@app.post("enterprise/register",
          response_model=dict,
          responses={
              200: {"description": "Source registered successfully"},
              400: {"model": ErrorResponse, "description": "Failed to validate source through config"},
              500: {"model": ErrorResponse, "description": "Internal server error"}
          })
async def register_source(
    request: Request
    ):
    """
    Register a new source for the enterprise pipeline.
    This endpoint validates the source configuration and registers it for processing.
    """
    data = await request.json()
    try:
        executor.execute_first_time(data)
        return {"status": "Source registered successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to validate source through config: {str(e)}"
        )

# Source deletion endpoint
@app.post("enterprise/delete",
          response_model=dict,
          responses={
              200: {"description": "Source deleted successfully"},
              400: {"model": ErrorResponse, "description": "Failed to delete source"},
              500: {"model": ErrorResponse, "description": "Internal server error"}
          })
async def delete_source(request: Request):
    """
    Delete an existing source from the enterprise pipeline.
    This endpoint removes the source configuration and stops any associated jobs.
    """
    data = await request.json()
    try:
        executor.delete_job(data)
        return {"status": "Source deleted successfully"}
    except Exception as e:
        raise HTTPException(
            status_code=400,
            detail=f"Failed to delete source: {str(e)}"
        )

if __name__ == "__main__":
    uvicorn.run(
        app, 
        host=config.SERVICE_HOST, 
        port=config.SERVICE_PORT,
        reload=config.DEBUG
    )
