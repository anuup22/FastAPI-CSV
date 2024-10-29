from fastapi import FastAPI, UploadFile, File, Depends
from sqlalchemy.orm import Session
import pandas as pd
from . import models, database

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
    df = pd.read_csv(file.file)
    for _, row in df.iterrows():
        user = models.User(
            firstName=row['FirstName'],
            lastName=row['LastName'],
            age=row['Age'],
            email=row['Email']
        )
        db.add(user)
    db.commit()
    return {"message": "CSV data inserted successfully"}