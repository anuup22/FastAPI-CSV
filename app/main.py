from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import pandas as pd
from . import models, database
from io import StringIO
import logging
from .schemas import BaseResponse, UsersResponse, UserResponse, User

app = FastAPI()
logging.basicConfig(level=logging.INFO)

models.Base.metadata.create_all(bind=database.engine)

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

def process_csv(file_content: bytes, filename: str):
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

@app.get("/")
def read_root() -> BaseResponse:
    return BaseResponse(success=True, message="Welcome to the User Management API")

# Endpoint to upload CSV files
@app.post("/upload-csv/", response_model=BaseResponse)
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    file_content = await file.read()
    background_tasks.add_task(process_csv, file_content, file.filename)
    
    return BaseResponse(success=True, message="CSV file is being processed in the background")

# Endpoint to get a paginated list of users
@app.get("/users/", response_model=UsersResponse)
def get_users(limit: int = 10, page: int = 1, db: Session = Depends(get_db)):
    offset = (page - 1) * limit
    users = db.query(models.User).offset(offset).limit(limit).all()
    return UsersResponse(success=True, data=[User.model_validate(user) for user in users])

# Endpoint to get a specific user by ID
@app.get("/users/{user_id}", response_model=UserResponse)
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return UserResponse(success=True, data=User.model_validate(user))
