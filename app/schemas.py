from pydantic import BaseModel, ConfigDict
from typing import Any, List, Optional

class BaseResponse(BaseModel):
    success: bool
    message: Optional[str] = None

    class Config:
        from_attributes = True

# User model for responses
class User(BaseModel):
    id: int
    firstName: str
    lastName: str
    age: int
    email: str

class UserResponse(BaseResponse):
    data: User

class UsersResponse(BaseResponse):
    data: List[User]