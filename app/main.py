import asyncio
import logging
import pandas as pd
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends, Query
from sqlalchemy.sql import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from contextlib import asynccontextmanager
from io import StringIO
from typing import Dict
from . import models
from .database import engine, get_db
from .schemas import User, BaseResponse, UsersResponse, UserResponse

# Configuration
NUM_WORKERS = 5  # Number of concurrent workers
CHUNK_SIZE = 1000  # Rows per chunk
MAX_QUEUE_SIZE = 10  # Maximum chunks in queue

# Create tables if they don't exist
async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(models.Base.metadata.create_all)

# Queue for asynchronous batch processing
queue = asyncio.Queue(maxsize=MAX_QUEUE_SIZE)

# Track processing status
processing_status: Dict[str, Dict] = {}

# --------------------------------- Worker Pool ---------------------------------
async def db_worker(worker_id: int):
    """Worker process that handles database insertions"""
    async with get_db() as db:
    
        while True:
            try:
                batch, file_id = await queue.get()
                
                try:
                    db.add_all(batch)
                    await db.commit()
                    
                    # Update processing status
                    if file_id in processing_status:
                        processing_status[file_id]["processed_chunks"] += 1
                        processed = processing_status[file_id]["processed_chunks"]
                        total = processing_status[file_id]["total_chunks"]
                        processing_status[file_id]["progress"] = (processed / total) * 100
                        
                    logging.info(f"Worker {worker_id}: {processing_status[file_id]['processed_chunks']} Batch inserted successfully")
                    
                except Exception as e:
                    await db.rollback()
                    logging.error(f"Worker {worker_id}: Error inserting batch: {str(e)}")
                    # Requeue failed batch for retry
                    await asyncio.sleep(1)
                    await queue.put((batch, file_id))
                
                finally:
                    # Notify queue that task is complete
                    queue.task_done()
                    
            except Exception as e:
                logging.error(f"Worker {worker_id}: Critical error: {str(e)}")
                await asyncio.sleep(1) # Wait before retrying

# --------------------------------- Worker Pool Management ---------------------------------
async def start_workers():
    """Ensure the worker pool is running"""
    workers = []
    for i in range(NUM_WORKERS):
        worker = asyncio.create_task(db_worker(i))
        workers.append(worker)
    return workers

# Initialize worker pool
workers = []

# --------------------------------- Project Setup ----------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    global workers
    workers = await start_workers()
    await init_db()

    yield

    for worker in workers:
        worker.cancel()
    await engine.dispose()

app = FastAPI(lifespan=lifespan)
logging.basicConfig(level=logging.INFO)
file_dependency = File(...)
db_dependency = Depends(get_db)

# --------------------------------- CSV Processing ---------------------------------
async def process_csv_async(file_content: bytes, filename: str):
    """Process CSV file with concurrent chunk processing"""
    file_id = filename + "_" + str(id(file_content))
    file_stream = StringIO(file_content.decode('utf-8'))
    
    try:
        # Calculate total chunks for progress tracking
        df = pd.read_csv(file_stream)
        total_chunks = (len(df) + CHUNK_SIZE - 1) // CHUNK_SIZE
        file_stream.seek(0)
        
        # Initialize processing status
        processing_status[file_id] = {
            "filename": filename,
            "total_chunks": total_chunks,
            "processed_chunks": 0,
            "progress": 0,
            "status": "processing"
        }
        
        # Process the CSV file in chunks
        for chunk in pd.read_csv(file_stream, chunksize=CHUNK_SIZE):
            users = [
                models.User(
                    firstName=row['FirstName'],
                    lastName=row['LastName'],
                    age=row['Age'],
                    email=row['Email']
                )
                for _, row in chunk.iterrows()
            ]
            
            # Add chunk to queue
            await queue.put((users, file_id))
        
        # Wait for all chunks to be processed
        await queue.join()
        
        processing_status[file_id]["status"] = "completed"
        logging.info(f"CSV file '{filename}' processed successfully")
        
    except Exception as e:
        processing_status[file_id]["status"] = "failed"
        processing_status[file_id]["error"] = str(e)
        logging.error(f"Error processing CSV '{filename}': {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

# --------------------------------- API Endpoints ---------------------------------
@app.post("/upload-csv/", response_model=BaseResponse)
async def upload_csv(file: UploadFile = Depends(file_dependency)) -> BaseResponse:
    """Upload and process a CSV file"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    file_content = await file.read()
    file_id = file.filename + "_" + str(id(file_content))
    
    # Start processing in background
    asyncio.create_task(process_csv_async(file_content, file.filename))
    
    return BaseResponse(
        success=True,
        message="CSV processing started",
        data={"file_id": file_id}
    )

@app.get("/process-status/", response_model=BaseResponse)
def get_process_status() -> BaseResponse:
    """Get the status of all CSV processing jobs"""
    return BaseResponse(
        success=True,
        message="Process status retrieved",
        data=processing_status
    )

@app.get("/users/", response_model=UsersResponse)
async def get_users(
    db: AsyncSession = db_dependency,
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
) -> UsersResponse:
    """Get all users from the database with pagination"""
    offset = (page - 1) * limit
    query = models.User.__table__.select().limit(limit).offset(offset)
    result = await db.execute(query)
    users = [User.model_validate(user) for user in result.scalars()]
    
    # Calculate total pages
    total_users = await db.scalar(select(func.count()).select_from(models.User.__table__))
    total_pages = (total_users + limit - 1) // limit
    
    return UsersResponse(
        success=True,
        next_page=(page < total_pages),
        total_pages=total_pages,
        data=users
    )

@app.get("/user/{user_id}/", response_model=UserResponse)
async def get_user(user_id: int, db: AsyncSession = db_dependency) -> UserResponse:
    """Get a single user by ID"""
    result = await db.execute(select(models.User).where(models.User.id == user_id))
    user = result.scalars().first()

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(success=True, data=User.model_validate(user))