import os
import shutil
import datetime
from typing import List
from fastapi import UploadFile, HTTPException
import config
from local.utils.logger import logger

def validate_file(file: UploadFile) -> None:
    """
    Validate a file upload.
    
    Args:
        file: The uploaded file to validate
        
    Raises:
        HTTPException: If file validation fails
    """
    if not file.filename:
        raise HTTPException(status_code=400, detail="Empty filename")
    
    # Add additional validation as needed (file size, type, etc.)

def save_uploaded_file(file: UploadFile) -> str:
    """
    Save an uploaded file to disk and return the path.
    
    Args:
        file: The uploaded file
        
    Returns:
        str: Path to the saved file
        
    Raises:
        HTTPException: If file saving fails
    """
    validate_file(file)
    
    timestamp = datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S")
    base_name = os.path.basename(file.filename).replace(" ", "-")
    name_parts = base_name.split(".")
    
    if len(name_parts) < 2:
        raise HTTPException(status_code=400, detail="Invalid filename format")
    
    file_name = f"{name_parts[0]}_{timestamp}.{name_parts[1]}"
    file_path = os.path.join(config.UPLOAD_DIR, file_name)
    
    try:
        with open(file_path, "wb") as f:
            shutil.copyfileobj(file.file, f)
        return file_path
    except Exception as e:
        logger.error(f"File save error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"File save error: {str(e)}")
    finally:
        file.file.close()

def save_uploaded_files(files: List[UploadFile]) -> List[str]:
    """
    Save multiple uploaded files and return their paths.
    
    Args:
        files: List of uploaded files
        
    Returns:
        List[str]: Paths to saved files
    """
    saved_files = []
    
    for file in files:
        saved_files.append(save_uploaded_file(file))
    
    return saved_files