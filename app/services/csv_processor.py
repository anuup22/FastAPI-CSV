import pandas as pd
from io import StringIO
from fastapi import HTTPException
import logging
from ..core.config import settings
from ..core.worker_pool import worker_pool
from ..models.user import User

async def process_csv_async(file_content: bytes, filename: str):
    file_id = filename + "_" + str(id(file_content))
    file_stream = StringIO(file_content.decode('utf-8'))
    
    try:
        df = pd.read_csv(file_stream)
        total_chunks = (len(df) + settings.CHUNK_SIZE - 1) // settings.CHUNK_SIZE
        file_stream.seek(0)
        
        worker_pool.processing_status[file_id] = {
            "filename": filename,
            "total_chunks": total_chunks,
            "processed_chunks": 0,
            "progress": 0,
            "status": "processing"
        }
        
        for chunk in pd.read_csv(file_stream, chunksize=settings.CHUNK_SIZE):
            users = [
                User(
                    firstName=row['FirstName'],
                    lastName=row['LastName'],
                    age=row['Age'],
                    email=row['Email']
                )
                for _, row in chunk.iterrows()
            ]
            
            await worker_pool.queue.put((users, file_id))
        
        # Wait for all chunks to be processed
        # await worker_pool.queue.join()
        
        worker_pool.processing_status[file_id]["status"] = "completed"
        logging.info(f"CSV file '{filename}' processed successfully")
        
    except Exception as e:
        worker_pool.processing_status[file_id]["status"] = "failed"
        worker_pool.processing_status[file_id]["error"] = str(e)
        logging.error(f"Error processing CSV '{filename}': {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))