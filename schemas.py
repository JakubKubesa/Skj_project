from pydantic import BaseModel

class FileResponse(BaseModel):
    id: str 
    filename: str
    size: int

    class Config:
        from_attributes = True