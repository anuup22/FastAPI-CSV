import asyncio
import logging
from typing import Dict
from ..core.config import settings
from ..db.session import get_db

class WorkerPool:
    def __init__(self):
        self.queue = asyncio.Queue(maxsize=settings.MAX_QUEUE_SIZE)
        self.processing_status: Dict[str, Dict] = {}
        self.workers = []

    async def db_worker(self, worker_id: int):
        db_session = await anext(get_db())
        
        while True:
            try:
                batch, file_id = await self.queue.get()
                
                try:
                    db_session.add_all(batch)
                    await db_session.commit()
                    
                    if file_id in self.processing_status:
                        self.processing_status[file_id]["processed_chunks"] += 1
                        processed = self.processing_status[file_id]["processed_chunks"]
                        total = self.processing_status[file_id]["total_chunks"]
                        self.processing_status[file_id]["progress"] = (processed / total) * 100
                        
                    logging.info(f"Worker {worker_id}: {self.processing_status[file_id]['processed_chunks']} Batch inserted successfully")
                    
                except Exception as e:
                    await db_session.rollback()
                    logging.error(f"Worker {worker_id}: Error inserting batch: {str(e)}")
                    await asyncio.sleep(1)
                    await self.queue.put((batch, file_id))
                
                finally:
                    self.queue.task_done()
                    
            except Exception as e:
                logging.error(f"Worker {worker_id}: Critical error: {str(e)}")
                await asyncio.sleep(1)

    async def start_workers(self):
        self.workers = []
        for i in range(settings.NUM_WORKERS):
            worker = asyncio.create_task(self.db_worker(i))
            self.workers.append(worker)
        return self.workers

    async def stop_workers(self):
        for worker in self.workers:
            worker.cancel()

worker_pool = WorkerPool()