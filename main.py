import os
import shutil
import asyncio
import json
from typing import Any, List, Dict, Set, Optional

import msgpack
from fastapi import Depends, FastAPI, HTTPException, Request, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse as FastAPIFileResponse
from sqlalchemy.orm import Session

import models
import schemas
from database import Base, engine, get_db, SessionLocal
from broker_utils import manager

Base.metadata.create_all(bind=engine)

app = FastAPI()

STORAGE_DIR = "storage"


# ensure storage dir exists
os.makedirs(STORAGE_DIR, exist_ok=True)


@app.get("/buckets/{bucket_id}/objects/{object_id}")
async def get_bucket_object(bucket_id: str, object_id: str):
    """Return a file stored under storage/{bucket_id}/{object_id}.

    This acts as a simple S3 gateway GET endpoint used by workers.
    """
    bucket_dir = os.path.join(STORAGE_DIR, bucket_id)
    file_path = os.path.join(bucket_dir, object_id)
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="Object not found")
    return FastAPIFileResponse(path=file_path, filename=object_id)


@app.post("/buckets/{bucket_id}/objects/{object_id}/upload")
async def upload_bucket_object(bucket_id: str, object_id: str, file: UploadFile):
    """Accept a file upload and store it under storage/{bucket_id}/{object_id}.

    This acts as a simple S3 gateway POST endpoint used by workers.
    """
    bucket_dir = os.path.join(STORAGE_DIR, bucket_id)
    os.makedirs(bucket_dir, exist_ok=True)
    dst = os.path.join(bucket_dir, object_id)
    with open(dst, "wb") as f:
        shutil.copyfileobj(file.file, f)
    return {"status": "ok", "bucket_id": bucket_id, "object_id": object_id}


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


def serialize_payload(payload: Any, payload_format: str) -> bytes:
    if payload_format == "msgpack":
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload).encode("utf-8")


def deserialize_payload(payload: bytes, payload_format: str) -> Any:
    if payload_format == "msgpack":
        return msgpack.unpackb(payload, raw=False)
    return json.loads(payload.decode("utf-8"))


def queued_message_to_deliver(message: models.QueuedMessage) -> Dict[str, Any]:
    return {
        "action": "deliver",
        "topic": message.topic,
        "message_id": message.id,
        "payload": deserialize_payload(message.payload, message.payload_format),
    }


def store_queued_message(topic: str, payload: Any, payload_format: str) -> Dict[str, Any]:
    db = SessionLocal()
    try:
        db_message = models.QueuedMessage(
            topic=topic,
            payload=serialize_payload(payload, payload_format),
            payload_format=payload_format,
            is_delivered=False,
        )
        db.add(db_message)
        db.commit()
        db.refresh(db_message)
        return queued_message_to_deliver(db_message)
    finally:
        db.close()


def load_pending_messages(topic: str) -> List[Dict[str, Any]]:
    db = SessionLocal()
    try:
        messages = (
            db.query(models.QueuedMessage)
            .filter(
                models.QueuedMessage.topic == topic,
                models.QueuedMessage.is_delivered.is_(False),
            )
            .order_by(models.QueuedMessage.id.asc())
            .all()
        )
        return [queued_message_to_deliver(message) for message in messages]
    finally:
        db.close()


def acknowledge_message(message_id: int) -> bool:
    db = SessionLocal()
    try:
        db_message = db.get(models.QueuedMessage, message_id)
        if not db_message or db_message.is_delivered:
            return False

        db_message.is_delivered = True
        db.add(db_message)
        db.commit()
        return True
    finally:
        db.close()


def decode_broker_message(data: Dict[str, Any]) -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    if data.get("text") is not None:
        return json.loads(data["text"]), "json"
    if data.get("bytes") is not None:
        return msgpack.unpackb(data["bytes"], raw=False), "msgpack"
    return None, None


def build_transient_deliver_message(topic: str, payload: Any) -> Dict[str, Any]:
    return {
        "action": "deliver",
        "topic": topic,
        "message_id": None,
        "payload": payload,
    }


@app.websocket("/ws/broker/{topic}")
async def broker_ws(websocket: WebSocket, topic: str):
    """WebSocket endpoint for the durable in-process broker."""
    mode = websocket.query_params.get("mode", "json").lower()
    if mode not in ("json", "msgpack"):
        mode = "json"
    role = websocket.query_params.get("role", "subscriber").lower()
    is_subscriber = role != "publisher"
    durable = websocket.query_params.get("durable", "true").lower() not in ("0", "false", "no")

    await manager.connect(websocket, topic, mode, subscribe=is_subscriber)
    try:
        if is_subscriber and durable:
            pending_messages = await run_in_threadpool(load_pending_messages, topic)
            for pending_message in pending_messages:
                await manager.send_protocol_message(websocket, pending_message)

        while True:
            data = await websocket.receive()

            if data.get("type") == "websocket.disconnect":
                break

            try:
                message, payload_format = decode_broker_message(data)
            except Exception:
                await manager.send_protocol_message(
                    websocket,
                    {"action": "error", "detail": "Invalid message format"},
                )
                continue

            if not message:
                continue

            action = message.get("action")
            if action == "publish":
                publish_topic = message.get("topic")
                if publish_topic != topic:
                    await manager.send_protocol_message(
                        websocket,
                        {
                            "action": "error",
                            "detail": "Publish topic must match the WebSocket topic",
                            "topic": topic,
                        },
                    )
                    continue

                if durable:
                    deliver_message = await run_in_threadpool(
                        store_queued_message,
                        topic,
                        message.get("payload"),
                        payload_format or mode,
                    )
                else:
                    deliver_message = build_transient_deliver_message(topic, message.get("payload"))
                await manager.broadcast(deliver_message, topic)
            elif action == "ack":
                try:
                    message_id = int(message.get("message_id"))
                except (TypeError, ValueError):
                    await manager.send_protocol_message(
                        websocket,
                        {"action": "error", "detail": "Invalid ACK message_id"},
                    )
                    continue

                acked = await run_in_threadpool(acknowledge_message, message_id)
                await manager.send_protocol_message(
                    websocket,
                    {
                        "action": "ack",
                        "message_id": message_id,
                        "status": "ok" if acked else "ignored",
                    },
                )
            else:
                await manager.send_protocol_message(
                    websocket,
                    {"action": "error", "detail": f"Unsupported action: {action}"},
                )
    except WebSocketDisconnect:
        # normal client disconnect
        pass
    except Exception:
        # swallow unexpected errors to ensure cleanup runs
        pass
    finally:
        await manager.disconnect(websocket, topic)
