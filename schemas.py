from pydantic import BaseModel

class FileResponse(BaseModel):
    id: str 
    filename: str
    size: int

    class Config:
        from_attributes = True


class BucketBilling(BaseModel):
    current_storage_bytes: int
    ingress_bytes: int
    egress_bytes: int
    internal_transfer_bytes: int

    class Config:
        from_attributes = True


class BucketCreate(BaseModel):
    name: str | None = None


class BucketResponse(BaseModel):
    id: str
    name: str | None = None
    files: list[FileResponse] = []

    class Config:
        from_attributes = True
