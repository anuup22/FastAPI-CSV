from fastapi import FastAPI, UploadFile, File, Depends
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
    file_content = await file.read()
    file_stream = StringIO(file_content.decode('utf-8'))
    
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
    
    return {"message": "CSV data inserted successfully"}

@app.get("/users/")
def get_users(db: Session = Depends(get_db)):
    users = db.query(models.User).all()
    return users