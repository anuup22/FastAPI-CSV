from fastapi import FastAPI, UploadFile, File, Depends, HTTPException
from sqlalchemy.orm import Session
import pandas as pd
from . import models, database
from io import StringIO

app = FastAPI()

models.Base.metadata.create_all(bind=database.engine)

def get_db():
    db = database.SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/upload-csv/")
async def upload_csv(file: UploadFile = File(...), db: Session = Depends(get_db)):
    # Read the file in chunks
    chunk_size = 1000
    
    # Read the uploaded file content
    file_content = await file.read()
    
    # Ensure the file is a valid CSV
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")

    # Convert to StringIO to read with pandas
    file_stream = StringIO(file_content.decode('utf-8'))
    
    try:
        # Process the CSV in chunks
        for chunk in pd.read_csv(file_stream, chunksize=chunk_size):
            users = [
                models.User(
                    firstName=row['FirstName'],
                    lastName=row['LastName'],
                    age=row['Age'],
                    email=row['Email'],
                    fileName=file.filename
                )
                for _, row in chunk.iterrows()
            ]
            db.bulk_save_objects(users)
            db.commit()
        
    except Exception as e:
        # Handle exceptions and roll back if needed
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred while processing the CSV: {str(e)}")

    return {"message": "CSV data inserted successfully"}

@app.get("/users/")
def get_users(db: Session = Depends(get_db)):
    users = db.query(models.User).all()
    return users
