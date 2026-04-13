import os
import shutil
from fastapi import FastAPI, UploadFile, Depends, HTTPException, Request
from fastapi.responses import FileResponse as FastAPIFileResponse
from sqlalchemy.orm import Session
from database import engine, get_db, Base
import models
import schemas

# Vytvoření tabulek v DB
Base.metadata.create_all(bind=engine)

app = FastAPI()

STORAGE_DIR = "storage"

@app.post("/files/upload", response_model=schemas.FileResponse)
async def upload_file(
    request: Request,
    user_id: str,
    bucket_id: str,
    file: UploadFile,
    db: Session = Depends(get_db),
) -> models.FileModel:
    # 1. Cesta pro uložení
    user_dir = os.path.join(STORAGE_DIR, user_id)
    os.makedirs(user_dir, exist_ok=True)
    
    file_path = os.path.join(user_dir, file.filename)
    
    # 2. Uložení fyzického souboru
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)
    
    # 3. Ensure bucket exists (required for FK) and then save metadata
    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        bucket = models.Bucket(id=bucket_id)
        db.add(bucket)
        db.commit()
        db.refresh(bucket)

    db_file = models.FileModel(
        user_id=user_id,
        filename=file.filename,
        path=file_path,
        size=os.path.getsize(file_path),
        bucket_id=bucket_id,
    )
    db.add(db_file)
    db.commit()
    db.refresh(db_file)

    # Billing: update bucket counters
    size = db_file.size or 0
    bucket.current_storage_bytes = bucket.current_storage_bytes + size
    if is_internal:
        bucket.internal_transfer_bytes = bucket.internal_transfer_bytes + size
    else:
        bucket.ingress_bytes = bucket.ingress_bytes + size

    db.add(bucket)
    db.commit()
    
    return db_file

@app.get("/files/{file_id}")
async def get_file(file_id: str, db: Session = Depends(get_db)): 
    db_file = db.query(models.FileModel).filter(models.FileModel.id == file_id).first()
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    return FastAPIFileResponse(path=db_file.path, filename=db_file.filename)

@app.delete("/files/{file_id}")
async def delete_file(file_id: str, db: Session = Depends(get_db)): # Změněno na str
    db_file = db.query(models.FileModel).filter(models.FileModel.id == file_id).first()
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")
    
    # Smazání z disku
    if os.path.exists(db_file.path):
        os.remove(db_file.path)
    
    # Smazání z DB
    # Update bucket storage counter if assigned
    if db_file.bucket_id:
        bucket = db.get(models.Bucket, db_file.bucket_id)
        if bucket:
            try:
                dec = int(db_file.size or 0)
            except Exception:
                dec = 0
            bucket.current_storage_bytes = max(0, bucket.current_storage_bytes - dec)
            db.add(bucket)
            db.commit()

    db.delete(db_file)
    db.commit()
    
    return {"message": "File deleted successfully"}


@app.get("/files/download/{file_id}")
async def download_file(file_id: str, request: Request, db: Session = Depends(get_db)):
    db_file = db.query(models.FileModel).filter(models.FileModel.id == file_id).first()
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")

    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"
    if db_file.bucket_id:
        bucket = db.get(models.Bucket, db_file.bucket_id)
        if bucket:
            size = db_file.size or 0
            if is_internal:
                bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + size
            else:
                bucket.egress_bytes = (bucket.egress_bytes or 0) + size
            db.add(bucket)
            db.commit()

    return FastAPIFileResponse(path=db_file.path, filename=db_file.filename)


@app.get("/buckets/{bucket_id}/billing/", response_model=schemas.BucketBilling)
async def get_bucket_billing(bucket_id: str, db: Session = Depends(get_db)) -> schemas.BucketBilling:
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    return schemas.BucketBilling(
        current_storage_bytes=bucket.current_storage_bytes or 0,
        ingress_bytes=bucket.ingress_bytes or 0,
        egress_bytes=bucket.egress_bytes or 0,
        internal_transfer_bytes=bucket.internal_transfer_bytes or 0,
    )
