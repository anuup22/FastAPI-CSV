from pydantic import BaseModel
from typing import List

class UserCreate(BaseModel):
    firstName: str
    lastName: str
    age: int
    email: str

class User(BaseModel):
    id: int
    firstName: str
    lastName: str
    age: int
    email: str

    class Config:
        from_attributes = True

class UserResponse(BaseModel):
    success: bool
    data: User

class UsersResponse(BaseModel):
    success: bool
    next_page: bool
    total_pages: int
    data: List[User]

class BaseResponse(BaseModel):
    success: bool
    message: str