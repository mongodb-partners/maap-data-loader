from contextlib import asynccontextmanager
from typing import Dict, Any
import uvicorn
from fastapi import FastAPI, Request, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from dotenv import load_dotenv

from pipeline_executor import PipelineExecutor

# Load environment variables
load_dotenv()

executor = PipelineExecutor()

class SourceManager:
    @staticmethod
    async def register(data: Dict[Any, Any]):
        try:
            executor.execute_first_time(data)
            return {"status": "Source registerd successfully"}
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to validate source through config: {str(e)}"
            )

    @staticmethod
    async def delete(data: Dict[Any, Any]):
        try:
            executor.delete_job(data)
            return {"status": "Source deleted successfully"}
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail=f"Failed to delete source: {str(e)}"
            )

@asynccontextmanager
async def lifespan(app: FastAPI):
    scheduler = BackgroundScheduler()
    scheduler.add_job(executor.run_scheduled_jobs, "interval", minutes=1)
    scheduler.start()
    yield

def create_app() -> FastAPI:
    app = FastAPI(lifespan=lifespan)
    
    # CORS configuration
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
    return app

app = create_app()

@app.post("/register/source")
async def register_source(request: Request):
    data = await request.json()
    return await SourceManager.register(data)

@app.post("/delete/source")
async def delete_source(request: Request):
    data = await request.json()
    return await SourceManager.delete(data)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8182)
