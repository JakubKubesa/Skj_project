import os
import shutil
import asyncio
from typing import List, Dict, Set, Optional

from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse as FastAPIFileResponse
from sqlalchemy.orm import Session

import models
import schemas
from database import Base, engine, get_db
from broker_utils import manager

Base.metadata.create_all(bind=engine)

app = FastAPI()

STORAGE_DIR = "storage"


def increment_bucket_request_counter(bucket: models.Bucket, counter_name: str) -> None:
    current_value = getattr(bucket, counter_name, 0) or 0
    setattr(bucket, counter_name, current_value + 1)


def get_active_file_or_404(db: Session, file_id: str) -> models.FileModel:
    db_file = (
        db.query(models.FileModel)
        .filter(
            models.FileModel.id == file_id,
            models.FileModel.is_deleted.is_(False),
        )
        .first()
    )
    if not db_file:
        raise HTTPException(status_code=404, detail="File not found")
    return db_file


def ensure_file_exists_or_404(db_file: models.FileModel) -> None:
    if not os.path.exists(db_file.path):
        raise HTTPException(status_code=404, detail="File not found")


@app.post("/files/upload", response_model=schemas.FileResponse)
async def upload_file(
    request: Request,
    user_id: str,
    bucket_id: str,
    file: UploadFile,
    db: Session = Depends(get_db),
) -> models.FileModel:
    user_dir = os.path.join(STORAGE_DIR, user_id)
    os.makedirs(user_dir, exist_ok=True)

    file_path = os.path.join(user_dir, file.filename)
    with open(file_path, "wb") as buffer:
        shutil.copyfileobj(file.file, buffer)

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

    size = db_file.size or 0
    bucket.current_storage_bytes = (bucket.current_storage_bytes or 0) + size
    if is_internal:
        bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + size
    else:
        bucket.ingress_bytes = (bucket.ingress_bytes or 0) + size
    increment_bucket_request_counter(bucket, "count_write_requests")

    db.add(bucket)
    db.commit()
    db.refresh(db_file)

    return db_file


@app.get("/files/{file_id}")
async def get_file(file_id: str, db: Session = Depends(get_db)):
    db_file = get_active_file_or_404(db, file_id)
    ensure_file_exists_or_404(db_file)
    if db_file.bucket_id:
        bucket = db.get(models.Bucket, db_file.bucket_id)
        if bucket:
            increment_bucket_request_counter(bucket, "count_read_requests")
            db.add(bucket)
            db.commit()

    return FastAPIFileResponse(path=db_file.path, filename=db_file.filename)


@app.post("/buckets/", response_model=schemas.BucketResponse)
async def create_bucket(bucket_in: schemas.BucketCreate, db: Session = Depends(get_db)) -> schemas.BucketResponse:
    bucket = models.Bucket(name=bucket_in.name)
    increment_bucket_request_counter(bucket, "count_write_requests")
    db.add(bucket)
    db.commit()
    db.refresh(bucket)
    return schemas.BucketResponse(id=bucket.id, name=bucket.name, files=[])


@app.get("/buckets/{bucket_id}/objects/", response_model=List[schemas.FileResponse])
async def list_bucket_objects(bucket_id: str, db: Session = Depends(get_db)) -> List[schemas.FileResponse]:
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    files = (
        db.query(models.FileModel)
        .filter(
            models.FileModel.bucket_id == bucket_id,
            models.FileModel.is_deleted.is_(False),
        )
        .all()
    )
    increment_bucket_request_counter(bucket, "count_write_requests")
    db.add(bucket)
    db.commit()
    return files


@app.delete("/files/{file_id}")
async def delete_file(file_id: str, db: Session = Depends(get_db)):
    db_file = get_active_file_or_404(db, file_id)
    db_file.is_deleted = True
    db.add(db_file)

    if db_file.bucket_id:
        bucket = db.get(models.Bucket, db_file.bucket_id)
        if bucket:
            increment_bucket_request_counter(bucket, "count_write_requests")
            db.add(bucket)

    db.commit()
    return {"message": "File deleted successfully"}


@app.get("/files/download/{file_id}")
async def download_file(file_id: str, request: Request, db: Session = Depends(get_db)):
    db_file = get_active_file_or_404(db, file_id)
    ensure_file_exists_or_404(db_file)

    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"
    if db_file.bucket_id:
        bucket = db.get(models.Bucket, db_file.bucket_id)
        if bucket:
            size = db_file.size or 0
            if is_internal:
                bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + size
            else:
                bucket.egress_bytes = (bucket.egress_bytes or 0) + size
            increment_bucket_request_counter(bucket, "count_read_requests")
            db.add(bucket)
            db.commit()

    return FastAPIFileResponse(path=db_file.path, filename=db_file.filename)


@app.get("/buckets/{bucket_id}/billing/", response_model=schemas.BucketBilling)
async def get_bucket_billing(bucket_id: str, db: Session = Depends(get_db)) -> schemas.BucketBilling:
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    increment_bucket_request_counter(bucket, "count_read_requests")
    db.add(bucket)
    db.commit()

    return schemas.BucketBilling(
        current_storage_bytes=bucket.current_storage_bytes or 0,
        ingress_bytes=bucket.ingress_bytes or 0,
        egress_bytes=bucket.egress_bytes or 0,
        internal_transfer_bytes=bucket.internal_transfer_bytes or 0,
        count_write_requests=bucket.count_write_requests or 0,
        count_read_requests=bucket.count_read_requests or 0,
    )


@app.websocket("/ws/broker/{topic}")
async def broker_ws(websocket: WebSocket, topic: str):
    """WebSocket endpoint for the simple in-process broker.

    - Registers client under `topic` on connect
    - Receives text or binary messages and broadcasts them to other clients in the same topic
    - Ensures cleanup on disconnect or error
    """
    await manager.connect(websocket, topic)
    try:
        while True:
            data = await websocket.receive()
            # websocket.receive() returns a dict with either 'text' or 'bytes'
            if "text" in data and data["text"] is not None:
                await manager.broadcast(data["text"], topic, sender=websocket)
            elif "bytes" in data and data["bytes"] is not None:
                await manager.broadcast(data["bytes"], topic, sender=websocket)
            else:
                # ignore other message types
                continue
    except WebSocketDisconnect:
        # normal client disconnect
        pass
    except Exception:
        # swallow unexpected errors to ensure cleanup runs
        pass
    finally:
        await manager.disconnect(websocket, topic)
