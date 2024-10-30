from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
from . import models
from .database import engine, get_db
from io import StringIO
import asyncio
import logging
from concurrent.futures import ThreadPoolExecutor
from .schemas import BaseResponse, UsersResponse, UserResponse, User

app = FastAPI()
logging.basicConfig(level=logging.INFO)

models.Base.metadata.create_all(bind=engine)

# Define a ThreadPoolExecutor for blocking tasks
executor = ThreadPoolExecutor()

def process_csv_blocking(file_content: bytes, filename: str):
    """
    Blocking function to process CSV and save data to the database.
    Runs in a separate thread to avoid blocking the main event loop.
    
    Args:
        file_content (bytes): The content of the uploaded CSV file.
        filename (str): The name of the uploaded file.
    """
    chunk_size = 1000
    file_stream = StringIO(file_content.decode('utf-8'))
    db = next(get_db())

    try:
        for chunk in pd.read_csv(file_stream, chunksize=chunk_size):
            users = [
                models.User(
                    firstName=row['FirstName'],
                    lastName=row['LastName'],
                    age=row['Age'],
                    email=row['Email']
                )
                for _, row in chunk.iterrows()
            ]
            db.bulk_save_objects(users)
            db.commit()
        logging.info(f"CSV file '{filename}' processed successfully.")
    except Exception as e:
        db.rollback()
        logging.error(f"An error occurred while processing the CSV '{filename}': {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while processing the CSV: {str(e)}")
    finally:
        db.close()

async def process_csv_async(file_content: bytes, filename: str):
    """
    Asynchronous wrapper to run CSV processing in a thread to avoid blocking.
    Uses run_in_executor to offload the blocking task.
    
    Args:
        file_content (bytes): The content of the uploaded CSV file.
        filename (str): The name of the uploaded file.
    """
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(executor, process_csv_blocking, file_content, filename)

@app.get("/")
def read_root() -> BaseResponse:
    """
    Root endpoint that returns a welcome message.
    
    Returns:
        BaseResponse: A response indicating success with a welcome message.
    """
    return BaseResponse(success=True, message="Welcome to the User Management API")

@app.post("/upload-csv/", response_model=BaseResponse)
async def upload_csv(file: UploadFile = File(...)) -> BaseResponse:
    """
    Uploads a CSV file and processes it asynchronously in a separate thread.
    
    Args:
        file (UploadFile): The uploaded CSV file.

    Raises:
        HTTPException: If the file type is invalid.

    Returns:
        BaseResponse: A response indicating that the CSV file is being processed.
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    file_content = await file.read()
    asyncio.create_task(process_csv_async(file_content, file.filename))  # Process CSV in the background
    
    return BaseResponse(success=True, message="CSV file is being processed in the background")

@app.get("/users/", response_model=UsersResponse)
def get_users(limit: int = 10, page: int = 1, db: Session = Depends(get_db)) -> UsersResponse:
    """
    Retrieves a paginated list of users from the database.
    
    Args:
        limit (int): The number of users to return per page.
        page (int): The page number to retrieve.

    Returns:
        UsersResponse: A response containing the list of users, total pages, and next page.
    """
    total_users = db.query(models.User).count()
    total_pages = (total_users + limit - 1) // limit  # Calculate total pages
    offset = (page - 1) * limit
    users = db.query(models.User).offset(offset).limit(limit).all()
    user_responses = [User.model_validate(user) for user in users]
    
    next_page = page < total_pages
    
    return UsersResponse(success=True, data=user_responses, total_pages=total_pages, next_page=next_page)

# Endpoint to get a specific user by ID
@app.get("/users/{user_id}", response_model=UserResponse)
def get_user(user_id: int, db: Session = Depends(get_db)) -> UserResponse:
    """
    Retrieves a user by their ID.
    
    Args:
        user_id (int): The ID of the user to retrieve.

    Raises:
        HTTPException: If the user is not found.

    Returns:
        UserResponse: A response containing the user's details.
    """
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(success=True, data=User.model_validate(user))