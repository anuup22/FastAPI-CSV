from fastapi import APIRouter, Depends, File, UploadFile, Query, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import select, func
import asyncio
from ....db.session import get_db
from ....models.user import User as UserModel
from ....schemas.user import User, UserResponse, UsersResponse
from ....schemas.base import BaseResponse
from ....services.csv_processor import process_csv_async
from ....core.worker_pool import worker_pool

router = APIRouter()

@router.post("/upload-csv/", response_model=BaseResponse)
async def upload_csv(
    file: UploadFile = File(...)
) -> BaseResponse:
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

@router.get("/process-status/", response_model=BaseResponse)
def get_process_status() -> BaseResponse:
    return BaseResponse(
        success=True,
        message="Process status retrieved",
        data=worker_pool.processing_status
    )

@router.get("/users/", response_model=UsersResponse)
async def get_users(
    db: AsyncSession = Depends(get_db),
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100)
) -> UsersResponse:
    offset = (page - 1) * limit
    query = select(UserModel).limit(limit).offset(offset)
    result = await db.execute(query)
    users = [User.model_validate(user) for user in result.scalars()]
    
    total_users = await db.scalar(select(func.count()).select_from(UserModel.__table__))
    total_pages = (total_users + limit - 1) // limit
    
    return UsersResponse(
        success=True,
        next_page=(page < total_pages),
        total_pages=total_pages,
        data=users
    )

@router.get("/user/{user_id}/", response_model=UserResponse)
async def get_user(
    user_id: int,
    db: AsyncSession = Depends(get_db)
) -> UserResponse:
    result = await db.execute(select(UserModel).where(UserModel.id == user_id))
    user = result.scalars().first()

    if user is None:
        raise HTTPException(status_code=404, detail="User not found")
    
    return UserResponse(success=True, data=User.model_validate(user))