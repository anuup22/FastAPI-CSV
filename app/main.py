from fastapi import FastAPI, UploadFile, File, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
import pandas as pd
from . import models, database
from io import StringIO
import logging

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
        logging.info(f"CSV file {filename} processed successfully.")
    except Exception as e:
        db.rollback()
        logging.error(f"An error occurred while processing the CSV {filename}: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred while processing the CSV: {str(e)}")
    finally:
        db.close()

@app.post("/upload-csv/")
async def upload_csv(background_tasks: BackgroundTasks, file: UploadFile = File(...)):
    # Ensure the file is a valid CSV
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    # Read the uploaded file content
    file_content = await file.read()
    
    # Add the CSV processing task to the background tasks
    background_tasks.add_task(process_csv, file_content, file.filename)
    
    return {"message": "CSV file is being processed in the background"}

@app.get("/users/")
def get_users(limit: int = 10, page: int = 1, db: Session = Depends(get_db)):
    offset = (page - 1) * limit
    users = db.query(models.User).offset(offset).limit(limit).all()
    return users

@app.get("/users/{user_id}")
def get_user(user_id: int, db: Session = Depends(get_db)):
    user = db.query(models.User).filter(models.User.id == user_id).first()
    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    return user