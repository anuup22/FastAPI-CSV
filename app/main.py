from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import pandas as pd
from . import models
from .database import engine, get_db
from io import StringIO
import logging
from .schemas import BaseResponse, UsersResponse, UserResponse, User

app = FastAPI()
logging.basicConfig(level=logging.INFO)

models.Base.metadata.create_all(bind=engine)

# Function to process CSV files
def process_csv(file_content: bytes, filename: str) -> None:
    """
    Processes a CSV file, reading user data and saving it to the database.
    
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

# Root endpoint
@app.get("/")
def read_root() -> BaseResponse:
    """
    Root endpoint that returns a welcome message.
    
    Returns:
        BaseResponse: A response indicating success with a welcome message.
    """
    return BaseResponse(success=True, message="Welcome to the User Management API")

# Endpoint to upload CSV files
@app.post("/upload-csv/", response_model=BaseResponse)
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    """
    Uploads a CSV file and processes it in the background.
    
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
    background_tasks.add_task(process_csv, file_content, file.filename)
    
    return BaseResponse(success=True, message="CSV file is being processed in the background")

# Endpoint to get a paginated list of users
@app.get("/users/", response_model=UsersResponse)
def get_users(limit: int = 10, page: int = 1, db: Session = Depends(get_db)) -> UsersResponse:
    """
    Retrieves a paginated list of users from the database.
    
    Args:
        limit (int): The number of users to return per page.
        page (int): The page number to retrieve.

    Returns:
        UsersResponse: A response containing the list of users.
    """
    offset = (page - 1) * limit
    users = db.query(models.User).offset(offset).limit(limit).all()
    return UsersResponse(success=True, data=[User.model_validate(user) for user in users])

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
