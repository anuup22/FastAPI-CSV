import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI
from .core.config import settings
from .core.worker_pool import worker_pool
from .db.session import engine, SessionLocal
from .db.base import Base
from .api.v1.router import api_router

# Configuration
logging.basicConfig(level=logging.INFO)

async def init_db():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Initialize database
    await init_db()
    
    # Start worker pool
    await worker_pool.start_workers()

    yield

    # Cleanup
    await worker_pool.stop_workers()
    await engine.dispose()

# Create FastAPI application
app = FastAPI(
    title=settings.PROJECT_NAME,
    version=settings.VERSION,
    lifespan=lifespan
)

# Include routers
app.include_router(api_router, prefix="/api/v1")