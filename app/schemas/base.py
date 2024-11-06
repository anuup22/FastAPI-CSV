from pydantic import BaseModel

class BaseResponse(BaseModel):
    success: bool
    message: str
    data: dict | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Operation completed successfully",
                "data": {"key": "value"}
            }
        }