"""Main FastAPI application for the personal cloud and in-process message broker.

This module exposes:
- bucket/object REST endpoints with billing and soft delete support,
- a WebSocket broker with optional durable persistence,
- helper functions shared by the REST and broker flows.
"""

import asyncio
from contextlib import suppress
import json
import os
import hashlib
import mimetypes
import random
import secrets
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
import msgpack
import websockets
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect, status
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse as FastAPIFileResponse, Response as FastAPIResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from sqlalchemy import or_
from sqlalchemy.orm import Session

import models
import schemas
from broker_utils import manager
from database import SessionLocal, get_db

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
WEB_STATIC_DIR = WEB_DIR / "static"
STORAGE_DIR = "storage"
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "dev-internal-token")
HAYSTACK_BASE_URL = os.getenv("HAYSTACK_BASE_URL", "http://127.0.0.1:8002")
BROKER_BASE_WS = os.getenv("BROKER_BASE_WS", "ws://127.0.0.1:8000")
STORAGE_WRITE_TOPIC = os.getenv("HAYSTACK_WRITE_TOPIC", "storage.write")
STORAGE_ACK_TOPIC = os.getenv("HAYSTACK_ACK_TOPIC", "storage.ack")
HAYSTACK_READ_TIMEOUT = float(os.getenv("HAYSTACK_READ_TIMEOUT", "30.0"))
ACK_SUBSCRIBER_RECONNECT_BASE = 1.0
ACK_SUBSCRIBER_RECONNECT_MAX = 30.0
ACK_QUEUE_POLL_INTERVAL_SECONDS = float(os.getenv("STORAGE_ACK_POLL_INTERVAL_SECONDS", "1.0"))
ACK_QUEUE_POLL_BATCH_SIZE = int(os.getenv("STORAGE_ACK_POLL_BATCH_SIZE", "50"))
os.makedirs(STORAGE_DIR, exist_ok=True)

if WEB_STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_STATIC_DIR), name="static")



def increment_bucket_request_counter(bucket: models.Bucket, counter_name: str) -> None:
    """Increment one billing counter on a bucket model instance."""
    current_value = getattr(bucket, counter_name, 0) or 0
    setattr(bucket, counter_name, current_value + 1)



def object_to_response(db_object: models.ObjectModel) -> schemas.ObjectResponse:
    """Serialize one ORM object row into the public API response model."""
    return schemas.ObjectResponse(
        record_id=db_object.id,
        object_id=db_object.storage_object_id,
        bucket_id=db_object.bucket_id or "",
        object_key=db_object.object_key,
        size=db_object.size,
        status=db_object.status,
    )


def object_to_compaction_entry(db_object: models.ObjectModel) -> schemas.StorageCompactionObject:
    """Serialize one ready Haystack-backed object for the compaction script."""
    if not db_object.storage_object_id or db_object.volume_id is None or db_object.offset is None:
        raise ValueError("Object is missing required Haystack compaction metadata")

    return schemas.StorageCompactionObject(
        record_id=db_object.id,
        object_id=db_object.storage_object_id,
        bucket_id=db_object.bucket_id or "",
        object_key=db_object.object_key,
        volume_id=db_object.volume_id,
        offset=db_object.offset,
        size=db_object.size,
    )


def file_response(path: str, filename: str) -> FastAPIFileResponse:
    """Return a file without browser caching so repeated downloads hit billing."""
    return FastAPIFileResponse(
        path=path,
        filename=filename,
        headers={"Cache-Control": "no-store, max-age=0"},
    )


def binary_response(content: bytes, filename: str) -> FastAPIResponse:
    """Return in-memory bytes with a best-effort media type for preview/download."""
    media_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    return FastAPIResponse(
        content=content,
        media_type=media_type,
        headers={"Cache-Control": "no-store, max-age=0"},
    )



def get_or_create_bucket(db: Session, bucket_id: str) -> models.Bucket:
    """Load an existing bucket or create it on first object upload."""
    bucket = db.get(models.Bucket, bucket_id)
    if bucket:
        return bucket

    bucket = models.Bucket(id=bucket_id)
    db.add(bucket)
    db.commit()
    db.refresh(bucket)
    return bucket


def normalize_username(username: str) -> str:
    """Normalize a username for lookup and uniqueness."""
    return username.strip().lower()


def hash_password(password: str) -> str:
    """Hash one password with PBKDF2 and a per-user salt."""
    salt = secrets.token_hex(16)
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000).hex()
    return f"pbkdf2_sha256${salt}${digest}"


def verify_password(password: str, stored_hash: str) -> bool:
    """Verify a plaintext password against the stored hash format."""
    try:
        algorithm, salt, expected = stored_hash.split("$", 2)
    except ValueError:
        return False
    if algorithm != "pbkdf2_sha256":
        return False
    digest = hashlib.pbkdf2_hmac("sha256", password.encode("utf-8"), salt.encode("utf-8"), 120000).hex()
    return secrets.compare_digest(digest, expected)


def create_auth_session(db: Session, user: models.User) -> str:
    """Create a bearer token for the web client."""
    token = secrets.token_urlsafe(32)
    db.add(models.AuthSession(token=token, user_id=user.id))
    db.commit()
    return token


def get_user_personal_bucket(db: Session, user: models.User) -> models.Bucket:
    """Return the user's primary bucket, creating it if legacy data is missing it."""
    bucket = (
        db.query(models.Bucket)
        .filter(models.Bucket.user_id == user.id)
        .order_by(models.Bucket.id.asc())
        .first()
    )
    if bucket:
        return bucket

    bucket = models.Bucket(name=f"{user.username}-cloud", user_id=user.id)
    db.add(bucket)
    db.commit()
    db.refresh(bucket)
    return bucket


def user_to_public(db: Session, user: models.User) -> schemas.UserPublic:
    """Serialize authenticated user data with the personal bucket id."""
    bucket = get_user_personal_bucket(db, user)
    return schemas.UserPublic(id=user.id, username=user.username, bucket_id=bucket.id)


def get_current_user(request: Request, db: Session = Depends(get_db)) -> models.User:
    """Resolve the current user from a bearer token."""
    auth_header = request.headers.get("authorization", "")
    if not auth_header.lower().startswith("bearer "):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")

    token = auth_header.split(" ", 1)[1].strip()
    session = db.get(models.AuthSession, token)
    if not session:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token")

    user = db.get(models.User, session.user_id)
    if not user:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid bearer token")
    return user


def require_internal_request(request: Request) -> None:
    """Allow only trusted internal callers to use low-level bucket routes."""
    internal_source = request.headers.get("x-internal-source", "false").lower() == "true"
    supplied_token = request.headers.get("x-internal-token", "")
    if not internal_source or not secrets.compare_digest(supplied_token, INTERNAL_API_TOKEN):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Internal API only")


def get_user_bucket_object_or_404(db: Session, user: models.User, object_key: str) -> models.ObjectModel:
    """Load an active object from the authenticated user's personal bucket."""
    bucket = get_user_personal_bucket(db, user)
    return get_bucket_object_or_404(db, bucket.id, object_key)



def get_active_object_by_public_id(db: Session, bucket_id: str, object_id: str) -> Optional[models.ObjectModel]:
    """Resolve one active object by public object id or metadata record id."""
    return (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.bucket_id == bucket_id,
            models.ObjectModel.is_deleted.is_(False),
            or_(
                models.ObjectModel.storage_object_id == object_id,
                models.ObjectModel.id == object_id,
            ),
        )
        .order_by(models.ObjectModel.created_at.desc())
        .first()
    )


def get_user_object_by_public_id_or_404(db: Session, user: models.User, object_id: str) -> models.ObjectModel:
    """Load one active object owned by the authenticated user via public id."""
    bucket = get_user_personal_bucket(db, user)
    db_object = get_active_object_by_public_id(db, bucket.id, object_id)
    if not db_object:
        raise HTTPException(status_code=404, detail="Object not found")
    return db_object


def get_active_object_by_key(db: Session, bucket_id: str, object_key: str) -> Optional[models.ObjectModel]:
    """Return the most recent non-deleted object row for one bucket/key pair."""
    return (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.bucket_id == bucket_id,
            models.ObjectModel.object_key == object_key,
            models.ObjectModel.is_deleted.is_(False),
        )
        .order_by(models.ObjectModel.created_at.desc())
        .first()
    )



def get_bucket_object_or_404(db: Session, bucket_id: str, object_key: str) -> models.ObjectModel:
    """Load one active object row or raise 404 if it is missing."""
    db_object = get_active_object_by_key(db, bucket_id, object_key)
    if not db_object:
        raise HTTPException(status_code=404, detail="Object not found")
    return db_object


def ensure_object_ready(db_object: models.ObjectModel) -> None:
    """Block reads and processing while the object is still waiting for ACK."""
    if db_object.status != "ready":
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Object is still uploading")


async def read_haystack_object_bytes(db_object: models.ObjectModel) -> bytes:
    """Load one ready object from the Haystack node by volume metadata."""
    if db_object.volume_id is None or db_object.offset is None:
        raise HTTPException(status_code=404, detail="Object storage metadata is missing")

    url = f"{HAYSTACK_BASE_URL}/volume/{db_object.volume_id}/{db_object.offset}/{db_object.size}"
    try:
        async with httpx.AsyncClient(timeout=HAYSTACK_READ_TIMEOUT) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code == 404:
            raise HTTPException(status_code=404, detail="Object data not found in Haystack") from exc
        if exc.response.status_code == 416:
            raise HTTPException(status_code=502, detail="Stored Haystack offset metadata is invalid") from exc
        raise HTTPException(status_code=502, detail="Haystack storage read failed") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=503, detail="Haystack storage is unavailable") from exc


async def build_object_response(db_object: models.ObjectModel) -> FastAPIResponse:
    """Serve one ready object from Haystack or from a legacy local file path."""
    ensure_object_ready(db_object)

    if db_object.volume_id is not None and db_object.offset is not None:
        return binary_response(await read_haystack_object_bytes(db_object), db_object.object_key)

    if db_object.path and os.path.exists(db_object.path):
        return file_response(path=db_object.path, filename=db_object.object_key)

    raise HTTPException(status_code=404, detail="Object data not found")



async def enqueue_bucket_object_upload(
    db: Session,
    bucket_id: str,
    object_key: str,
    upload: UploadFile,
    *,
    user_id: str,
    is_internal: bool = False,
) -> schemas.UploadAcceptedResponse:
    """Queue one object write for Haystack and leave the row in uploading state."""
    bucket = get_or_create_bucket(db, bucket_id)
    existing = get_active_object_by_key(db, bucket_id, object_key)
    payload_bytes = await upload.read()
    storage_object_id = str(uuid.uuid4())

    previous_size = 0
    if existing:
        if existing.status == "ready":
            previous_size = existing.size or 0
        else:
            previous_size = existing.pending_previous_size or 0

        existing.user_id = user_id
        existing.path = ""
        existing.size = 0
        existing.status = "uploading"
        existing.storage_object_id = storage_object_id
        existing.volume_id = None
        existing.offset = None
        existing.pending_previous_size = previous_size
        existing.pending_is_internal = is_internal
        db_object = existing
    else:
        db_object = models.ObjectModel(
            user_id=user_id,
            object_key=object_key,
            path="",
            size=0,
            status="uploading",
            storage_object_id=storage_object_id,
            volume_id=None,
            offset=None,
            pending_previous_size=0,
            pending_is_internal=is_internal,
            bucket_id=bucket_id,
        )
        db.add(db_object)

    write_payload = schemas.StorageWritePayload(object_id=storage_object_id, data=payload_bytes).model_dump()
    db_message = create_queued_message_record(db, STORAGE_WRITE_TOPIC, write_payload, "msgpack")
    deliver_message = schemas.BrokerDeliverMessage(
        action="deliver",
        topic=STORAGE_WRITE_TOPIC,
        message_id=db_message.id,
        payload=write_payload,
    ).model_dump()

    db.add(bucket)
    db.add(db_object)
    db.commit()
    db.refresh(db_object)
    await manager.broadcast(deliver_message, STORAGE_WRITE_TOPIC)

    return schemas.UploadAcceptedResponse(
        status="accepted",
        topic=STORAGE_WRITE_TOPIC,
        bucket_id=bucket_id,
        object_key=object_key,
        record_id=db_object.id,
        object_id=storage_object_id,
    )



def register_object_download(db: Session, db_object: models.ObjectModel, *, is_internal: bool = False) -> None:
    """Account for one object download in bucket billing counters."""
    if not db_object.bucket_id:
        return

    bucket = db.get(models.Bucket, db_object.bucket_id)
    if not bucket:
        return

    size = db_object.size or 0
    if is_internal:
        bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + size
    else:
        bucket.egress_bytes = (bucket.egress_bytes or 0) + size
    increment_bucket_request_counter(bucket, "count_read_requests")
    db.add(bucket)
    db.commit()



def soft_delete_object_row(db: Session, db_object: models.ObjectModel) -> models.ObjectModel:
    """Mark one object row as soft-deleted without touching Haystack volumes."""
    db_object.is_deleted = True
    db.add(db_object)

    bucket = db.get(models.Bucket, db_object.bucket_id) if db_object.bucket_id else None
    if bucket:
        billed_size = 0
        if db_object.status == "ready":
            billed_size = db_object.size or 0
        elif db_object.pending_previous_size:
            billed_size = db_object.pending_previous_size or 0
        bucket.current_storage_bytes = max(0, (bucket.current_storage_bytes or 0) - billed_size)
        db.add(bucket)

    db.commit()
    return db_object


def soft_delete_bucket_object(db: Session, bucket_id: str, object_key: str) -> models.ObjectModel:
    """Soft delete one active object selected by bucket id and object key."""
    db_object = get_bucket_object_or_404(db, bucket_id, object_key)
    return soft_delete_object_row(db, db_object)


@app.get("/", include_in_schema=False)
def cloudik_web_client():
    """Serve the MUJ CLOUDIK web client."""
    index_path = WEB_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Web client not found")
    return FastAPIFileResponse(index_path)


@app.post("/auth/register", response_model=schemas.AuthResponse)
def register_user(register_in: schemas.RegisterRequest, db: Session = Depends(get_db)) -> schemas.AuthResponse:
    """Create a user account and its personal bucket."""
    username = normalize_username(register_in.username)
    existing = db.query(models.User).filter(models.User.username == username).first()
    if existing:
        raise HTTPException(status_code=409, detail="Username already exists")

    user = models.User(username=username, password_hash=hash_password(register_in.password))
    db.add(user)
    db.commit()
    db.refresh(user)

    get_user_personal_bucket(db, user)
    token = create_auth_session(db, user)
    return schemas.AuthResponse(token=token, user=user_to_public(db, user))


@app.post("/auth/login", response_model=schemas.AuthResponse)
def login_user(login_in: schemas.LoginRequest, db: Session = Depends(get_db)) -> schemas.AuthResponse:
    """Authenticate a user and return a bearer token."""
    username = normalize_username(login_in.username)
    user = db.query(models.User).filter(models.User.username == username).first()
    if not user or not verify_password(login_in.password, user.password_hash):
        raise HTTPException(status_code=401, detail="Invalid username or password")

    token = create_auth_session(db, user)
    return schemas.AuthResponse(token=token, user=user_to_public(db, user))


@app.get("/me", response_model=schemas.UserPublic)
def get_me(current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)) -> schemas.UserPublic:
    """Return the authenticated user profile used by the GUI."""
    return user_to_public(db, current_user)


@app.get("/me/objects", response_model=List[schemas.ObjectResponse])
def list_my_objects(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> List[schemas.ObjectResponse]:
    """Return all active objects in the authenticated user's bucket."""
    bucket = get_user_personal_bucket(db, current_user)
    db_objects = (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.bucket_id == bucket.id,
            models.ObjectModel.is_deleted.is_(False),
        )
        .order_by(models.ObjectModel.created_at.desc())
        .all()
    )
    return [object_to_response(db_object) for db_object in db_objects]


@app.put("/me/objects/{object_key}", response_model=schemas.UploadAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_my_object(
    object_key: str,
    file: UploadFile,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Accept an upload and forward it to Haystack through the durable broker."""
    bucket = get_user_personal_bucket(db, current_user)
    return await enqueue_bucket_object_upload(
        db,
        bucket.id,
        object_key,
        file,
        user_id=current_user.id,
        is_internal=False,
    )


@app.post("/upload", response_model=schemas.UploadAcceptedResponse, status_code=status.HTTP_202_ACCEPTED)
async def upload_legacy_endpoint(
    file: UploadFile,
    object_key: str | None = Form(default=None),
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> schemas.UploadAcceptedResponse:
    """Compatibility upload endpoint mirroring the original assignment shape."""
    resolved_key = (object_key or file.filename or "").strip()
    if not resolved_key:
        raise HTTPException(status_code=400, detail="object_key is required when filename is missing")

    bucket = get_user_personal_bucket(db, current_user)
    return await enqueue_bucket_object_upload(
        db,
        bucket.id,
        resolved_key,
        file,
        user_id=current_user.id,
        is_internal=False,
    )


@app.get("/download/{object_id}")
async def download_object_by_id(
    object_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Download one object through the gateway by public object identifier."""
    db_object = get_user_object_by_public_id_or_404(db, current_user, object_id)
    response = await build_object_response(db_object)
    register_object_download(db, db_object, is_internal=False)
    return response


@app.delete("/download/{object_id}")
def delete_object_by_id(
    object_id: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft delete one object through the gateway by public object identifier."""
    db_object = get_user_object_by_public_id_or_404(db, current_user, object_id)
    soft_delete_object_row(db, db_object)
    return {
        "message": "Object deleted successfully",
        "record_id": db_object.id,
        "object_id": db_object.storage_object_id,
        "object_key": db_object.object_key,
    }


@app.get("/me/objects/{object_key}")
async def download_my_object(
    object_key: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Download one object from the authenticated user's bucket."""
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)
    response = await build_object_response(db_object)
    register_object_download(db, db_object, is_internal=False)
    return response


@app.get("/me/objects/{object_key}/preview", include_in_schema=False)
async def preview_my_object(
    object_key: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return an object preview without counting it as a user download."""
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)
    return await build_object_response(db_object)


@app.delete("/me/objects/{object_key}")
def delete_my_object(
    object_key: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Soft delete an object from the authenticated user's bucket."""
    bucket = get_user_personal_bucket(db, current_user)
    soft_delete_bucket_object(db, bucket.id, object_key)
    return {"message": "Object deleted successfully", "bucket_id": bucket.id, "object_key": object_key}


@app.get("/me/billing", response_model=schemas.BucketBilling)
def get_my_billing(
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> schemas.BucketBilling:
    """Return billing counters for the authenticated user's bucket."""
    bucket = get_user_personal_bucket(db, current_user)
    return schemas.BucketBilling(
        current_storage_bytes=bucket.current_storage_bytes or 0,
        ingress_bytes=bucket.ingress_bytes or 0,
        egress_bytes=bucket.egress_bytes or 0,
        internal_transfer_bytes=bucket.internal_transfer_bytes or 0,
        count_write_requests=bucket.count_write_requests or 0,
        count_read_requests=bucket.count_read_requests or 0,
    )


@app.post("/me/objects/{object_key}/process", response_model=schemas.ProcessResponse)
async def process_my_object(
    object_key: str,
    process_in: schemas.ProcessRequest,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
) -> schemas.ProcessResponse:
    """Enqueue image processing for an object owned by the current user."""
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)
    ensure_object_ready(db_object)
    bucket_id = db_object.bucket_id or get_user_personal_bucket(db, current_user).id

    job_payload = schemas.WorkerJobPayload(
        operation=process_in.operation,
        object_key=object_key,
        bucket_id=bucket_id,
        user_id=current_user.id,
        params=process_in.params,
    ).model_dump()
    await publish_to_topic("image.jobs", job_payload, payload_format="json", durable=True)

    return schemas.ProcessResponse(
        status="processing_started",
        topic="image.jobs",
        bucket_id=bucket_id,
        object_key=object_key,
        operation=process_in.operation,
    )


@app.get("/buckets/{bucket_id}/objects/{object_key}", include_in_schema=False)
async def download_bucket_object(bucket_id: str, object_key: str, request: Request, db: Session = Depends(get_db)):
    """Download one object and account for egress or internal transfer."""
    require_internal_request(request)
    db_object = get_bucket_object_or_404(db, bucket_id, object_key)
    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"
    response = await build_object_response(db_object)
    register_object_download(db, db_object, is_internal=is_internal)
    return response


@app.put(
    "/buckets/{bucket_id}/objects/{object_key}",
    response_model=schemas.UploadAcceptedResponse,
    status_code=status.HTTP_202_ACCEPTED,
    include_in_schema=False,
)
async def upload_bucket_object(
    bucket_id: str,
    object_key: str,
    request: Request,
    file: UploadFile,
    user_id: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    """Accept one internal upload and enqueue the real write to Haystack.

    Args:
        bucket_id: Target bucket identifier.
        object_key: Logical object name within the bucket.
        request: Incoming request used to detect internal worker uploads.
        file: Multipart file payload.
        user_id: Required owner identifier for every upload, including worker
            rewrites of already existing objects.
    """
    require_internal_request(request)
    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"
    return await enqueue_bucket_object_upload(
        db,
        bucket_id,
        object_key,
        file,
        user_id=user_id,
        is_internal=is_internal,
    )


@app.post("/buckets/{bucket_id}/objects/{object_key}/process", response_model=schemas.ProcessResponse, include_in_schema=False)
async def process_bucket_object(
    bucket_id: str,
    object_key: str,
    request: Request,
    process_in: schemas.ProcessRequest,
    db: Session = Depends(get_db),
) -> schemas.ProcessResponse:
    """Enqueue an asynchronous image processing job for one object."""
    require_internal_request(request)
    db_object = get_bucket_object_or_404(db, bucket_id, object_key)
    ensure_object_ready(db_object)

    job_payload = schemas.WorkerJobPayload(
        operation=process_in.operation,
        object_key=object_key,
        bucket_id=bucket_id,
        user_id=db_object.user_id,
        params=process_in.params,
    ).model_dump()
    await publish_to_topic("image.jobs", job_payload, payload_format="json", durable=True)

    return schemas.ProcessResponse(
        status="processing_started",
        topic="image.jobs",
        bucket_id=bucket_id,
        object_key=object_key,
        operation=process_in.operation,
    )


@app.post("/buckets/", response_model=schemas.BucketResponse, include_in_schema=False)
def create_bucket(
    bucket_in: schemas.BucketCreate,
    request: Request,
    db: Session = Depends(get_db),
) -> schemas.BucketResponse:
    """Create a new bucket row and initialize billing counters."""
    require_internal_request(request)
    bucket = models.Bucket(name=bucket_in.name)
    increment_bucket_request_counter(bucket, "count_write_requests")
    db.add(bucket)
    db.commit()
    db.refresh(bucket)
    return schemas.BucketResponse(id=bucket.id, name=bucket.name, objects=[])


@app.get("/buckets/{bucket_id}/objects/", response_model=List[schemas.ObjectResponse], include_in_schema=False)
def list_bucket_objects(bucket_id: str, request: Request, db: Session = Depends(get_db)) -> List[schemas.ObjectResponse]:
    """Return all non-deleted objects stored in one bucket."""
    require_internal_request(request)
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    db_objects = (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.bucket_id == bucket_id,
            models.ObjectModel.is_deleted.is_(False),
        )
        .order_by(models.ObjectModel.created_at.desc())
        .all()
    )
    return [object_to_response(db_object) for db_object in db_objects]


@app.delete("/buckets/{bucket_id}/objects/{object_key}", include_in_schema=False)
def delete_bucket_object(bucket_id: str, object_key: str, request: Request, db: Session = Depends(get_db)):
    """Soft delete one object row from the personal cloud API."""
    require_internal_request(request)
    soft_delete_bucket_object(db, bucket_id, object_key)
    return {"message": "Object deleted successfully", "bucket_id": bucket_id, "object_key": object_key}


@app.get("/buckets/{bucket_id}/billing/", response_model=schemas.BucketBilling, include_in_schema=False)
def get_bucket_billing(bucket_id: str, request: Request, db: Session = Depends(get_db)) -> schemas.BucketBilling:
    """Return current billing counters for a bucket."""
    require_internal_request(request)
    bucket = db.get(models.Bucket, bucket_id)
    if not bucket:
        raise HTTPException(status_code=404, detail="Bucket not found")

    return schemas.BucketBilling(
        current_storage_bytes=bucket.current_storage_bytes or 0,
        ingress_bytes=bucket.ingress_bytes or 0,
        egress_bytes=bucket.egress_bytes or 0,
        internal_transfer_bytes=bucket.internal_transfer_bytes or 0,
        count_write_requests=bucket.count_write_requests or 0,
        count_read_requests=bucket.count_read_requests or 0,
    )



@app.get(
    "/internal/storage/volumes/{volume_id}/objects",
    response_model=schemas.StorageCompactionListResponse,
    include_in_schema=False,
)
def list_volume_objects_for_compaction(
    volume_id: int,
    request: Request,
    db: Session = Depends(get_db),
) -> schemas.StorageCompactionListResponse:
    """Return all ready non-deleted objects that still reference one volume."""
    require_internal_request(request)
    db_objects = (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.volume_id == volume_id,
            models.ObjectModel.status == "ready",
            models.ObjectModel.is_deleted.is_(False),
            models.ObjectModel.storage_object_id.is_not(None),
            models.ObjectModel.offset.is_not(None),
        )
        .order_by(models.ObjectModel.offset.asc(), models.ObjectModel.created_at.asc())
        .all()
    )
    return schemas.StorageCompactionListResponse(
        volume_id=volume_id,
        objects=[object_to_compaction_entry(db_object) for db_object in db_objects],
    )


@app.post(
    "/internal/storage/objects/{object_id}/relocate",
    response_model=schemas.StorageCompactionUpdateResponse,
    include_in_schema=False,
)
def relocate_compacted_object(
    object_id: str,
    update_in: schemas.StorageCompactionUpdateRequest,
    request: Request,
    db: Session = Depends(get_db),
) -> schemas.StorageCompactionUpdateResponse:
    """Update one object row after compaction copied it into a new volume."""
    require_internal_request(request)
    db_object = (
        db.query(models.ObjectModel)
        .filter(models.ObjectModel.storage_object_id == object_id)
        .order_by(models.ObjectModel.created_at.desc())
        .first()
    )
    if not db_object:
        return schemas.StorageCompactionUpdateResponse(status="ignored", object_id=object_id)

    if (
        db_object.is_deleted
        or db_object.status != "ready"
        or db_object.volume_id != update_in.source_volume_id
        or db_object.size != update_in.size
    ):
        return schemas.StorageCompactionUpdateResponse(
            status="ignored",
            object_id=object_id,
            record_id=db_object.id,
            volume_id=db_object.volume_id,
            offset=db_object.offset,
        )

    db_object.volume_id = update_in.target_volume_id
    db_object.offset = update_in.target_offset
    db.add(db_object)
    db.commit()
    db.refresh(db_object)
    return schemas.StorageCompactionUpdateResponse(
        status="updated",
        object_id=object_id,
        record_id=db_object.id,
        volume_id=db_object.volume_id,
        offset=db_object.offset,
    )


def serialize_payload(payload: Any, payload_format: str) -> bytes:
    """Serialize broker payload bytes using JSON or MessagePack."""
    if payload_format == "msgpack":
        return msgpack.packb(payload, use_bin_type=True)
    return json.dumps(payload).encode("utf-8")



def deserialize_payload(payload: bytes, payload_format: str) -> Any:
    """Deserialize broker payload bytes into Python objects."""
    if payload_format == "msgpack":
        return msgpack.unpackb(payload, raw=False)
    return json.loads(payload.decode("utf-8"))


def create_queued_message_record(
    db: Session,
    topic: str,
    payload: Any,
    payload_format: str,
) -> models.QueuedMessage:
    """Attach one durable broker message to an existing SQLAlchemy session."""
    db_message = models.QueuedMessage(
        topic=topic,
        payload=serialize_payload(payload, payload_format),
        payload_format=payload_format,
        is_delivered=False,
    )
    db.add(db_message)
    db.flush()
    return db_message



def queued_message_to_deliver(message: models.QueuedMessage) -> Dict[str, Any]:
    """Convert one queued DB message into the broker delivery envelope."""
    return schemas.BrokerDeliverMessage(
        action="deliver",
        topic=message.topic,
        message_id=message.id,
        payload=deserialize_payload(message.payload, message.payload_format),
    ).model_dump()



def store_queued_message(topic: str, payload: Any, payload_format: str) -> Dict[str, Any]:
    """Persist one durable broker message and return the delivery envelope."""
    db = SessionLocal()
    try:
        db_message = create_queued_message_record(db, topic, payload, payload_format)
        db.commit()
        db.refresh(db_message)
        return queued_message_to_deliver(db_message)
    finally:
        db.close()



def load_pending_messages(topic: str) -> List[Dict[str, Any]]:
    """Load all non-acknowledged durable messages for one topic."""
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
    """Mark one durable broker message as delivered."""
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
    """Decode one incoming WebSocket frame into a Python dict and wire format."""
    if data.get("text") is not None:
        return json.loads(data["text"]), "json"
    if data.get("bytes") is not None:
        return msgpack.unpackb(data["bytes"], raw=False), "msgpack"
    return None, None



def build_transient_deliver_message(topic: str, payload: Any) -> Dict[str, Any]:
    """Build a non-persistent delivery envelope used in benchmark mode."""
    return schemas.BrokerDeliverMessage(
        action="deliver",
        topic=topic,
        message_id=None,
        payload=payload,
    ).model_dump()


async def publish_to_topic(topic: str, payload: Any, payload_format: str = "json", durable: bool = True) -> Dict[str, Any]:
    """Publish a message to a topic, optionally persisting it before broadcast."""
    if durable:
        deliver_message = await run_in_threadpool(store_queued_message, topic, payload, payload_format)
    else:
        deliver_message = build_transient_deliver_message(topic, payload)

    await manager.broadcast(deliver_message, topic)
    return deliver_message


def build_broker_uri(topic: str, *, role: str = "subscriber", durable: bool = True, mode: str = "json") -> str:
    """Construct the broker WebSocket URI used by the ACK subscriber."""
    durable_value = "true" if durable else "false"
    return f"{BROKER_BASE_WS}/ws/broker/{topic}?mode={mode}&role={role}&durable={durable_value}"


def decode_wire_message(raw: Any) -> Dict[str, Any]:
    """Decode one raw WebSocket frame into a Python dict."""
    if isinstance(raw, (bytes, bytearray)):
        return msgpack.unpackb(raw, raw=False)
    return json.loads(raw)


def finalize_object_upload_from_ack(ack_payload: schemas.StorageAckPayload) -> str:
    """Apply one storage ACK to object metadata and deferred billing counters."""
    db = SessionLocal()
    try:
        db_object = (
            db.query(models.ObjectModel)
            .filter(models.ObjectModel.storage_object_id == ack_payload.object_id)
            .order_by(models.ObjectModel.created_at.desc())
            .first()
        )
        if not db_object:
            print(f"[GATEWAY] storage.ack ignored, object_id={ack_payload.object_id} is no longer current")
            return "ignored"

        was_ready = db_object.status == "ready"
        previous_size = db_object.pending_previous_size or 0
        pending_is_internal = bool(db_object.pending_is_internal)

        db_object.path = ""
        db_object.size = ack_payload.size
        db_object.status = "ready"
        db_object.volume_id = ack_payload.volume_id
        db_object.offset = ack_payload.offset
        db_object.pending_previous_size = 0
        db_object.pending_is_internal = False
        db.add(db_object)

        if not was_ready and not db_object.is_deleted and db_object.bucket_id:
            bucket = db.get(models.Bucket, db_object.bucket_id)
            if bucket:
                size_delta = ack_payload.size - previous_size
                bucket.current_storage_bytes = max(0, (bucket.current_storage_bytes or 0) + size_delta)
                if pending_is_internal:
                    bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + ack_payload.size
                else:
                    bucket.ingress_bytes = (bucket.ingress_bytes or 0) + ack_payload.size
                increment_bucket_request_counter(bucket, "count_write_requests")
                db.add(bucket)

        db.commit()
        return "updated" if not was_ready else "duplicate"
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def load_pending_storage_ack_messages(limit: int = ACK_QUEUE_POLL_BATCH_SIZE) -> List[Dict[str, Any]]:
    """Load pending durable storage.ack messages directly from SQLite."""
    db = SessionLocal()
    try:
        messages = (
            db.query(models.QueuedMessage)
            .filter(
                models.QueuedMessage.topic == STORAGE_ACK_TOPIC,
                models.QueuedMessage.is_delivered.is_(False),
            )
            .order_by(models.QueuedMessage.id.asc())
            .limit(max(1, int(limit)))
            .all()
        )
        deliveries: List[Dict[str, Any]] = []
        for message in messages:
            try:
                payload = deserialize_payload(message.payload, message.payload_format)
            except Exception as exc:
                payload = {"_decode_error": str(exc)}
            deliveries.append(
                {
                    "message_id": message.id,
                    "payload": payload,
                }
            )
        return deliveries
    finally:
        db.close()


async def handle_storage_ack_delivery(raw: Any) -> None:
    """Validate and apply one deliver frame received from storage.ack."""
    try:
        data = decode_wire_message(raw)
    except Exception as exc:
        print(f"[GATEWAY] Invalid storage.ack frame: {exc}")
        return

    if not isinstance(data, dict):
        print(f"[GATEWAY] Unsupported storage.ack frame payload: {type(data).__name__}")
        return

    action = data.get("action")
    if action in {"ack", "error"}:
        return
    if action != "deliver":
        print(f"[GATEWAY] Unsupported storage.ack action: {action}")
        return

    message_id = data.get("message_id")
    try:
        deliver = schemas.BrokerDeliverMessage.model_validate(data)
        ack_payload = schemas.StorageAckPayload.model_validate(deliver.payload)
    except ValidationError as exc:
        if isinstance(message_id, int):
            await run_in_threadpool(acknowledge_message, message_id)
        print(f"[GATEWAY] Invalid storage.ack payload: {exc.errors(include_url=False)}")
        return

    try:
        result = await run_in_threadpool(finalize_object_upload_from_ack, ack_payload)
        print(f"[GATEWAY] storage.ack {result} for object_id={ack_payload.object_id}")
        if deliver.message_id is not None:
            await run_in_threadpool(acknowledge_message, deliver.message_id)
    except Exception as exc:
        print(f"[GATEWAY] Failed to apply storage.ack for object_id={ack_payload.object_id}: {exc}")


async def poll_storage_ack_queue(stop_event: asyncio.Event) -> None:
    """Process durable storage.ack messages directly from the broker table."""
    while not stop_event.is_set():
        deliveries = await run_in_threadpool(load_pending_storage_ack_messages, ACK_QUEUE_POLL_BATCH_SIZE)
        if not deliveries:
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=ACK_QUEUE_POLL_INTERVAL_SECONDS)
                break
            except asyncio.TimeoutError:
                continue

        for delivery in deliveries:
            if stop_event.is_set():
                break

            message_id = int(delivery["message_id"])
            payload = delivery.get("payload")
            try:
                ack_payload = schemas.StorageAckPayload.model_validate(payload)
            except ValidationError as exc:
                print(f"[GATEWAY] Invalid durable storage.ack payload for message_id={message_id}: {exc.errors(include_url=False)}")
                await run_in_threadpool(acknowledge_message, message_id)
                continue

            try:
                result = await run_in_threadpool(finalize_object_upload_from_ack, ack_payload)
                print(f"[GATEWAY] storage.ack {result} from durable queue for object_id={ack_payload.object_id}")
                await run_in_threadpool(acknowledge_message, message_id)
            except Exception as exc:
                print(f"[GATEWAY] Failed durable storage.ack handling for message_id={message_id}: {exc}")

    print("[GATEWAY] storage.ack durable poller stopped.")


async def run_storage_ack_subscriber(stop_event: asyncio.Event) -> None:
    """Keep one durable subscription to storage.ack alive in the background."""
    backoff = ACK_SUBSCRIBER_RECONNECT_BASE

    while not stop_event.is_set():
        try:
            broker_uri = build_broker_uri(STORAGE_ACK_TOPIC, role="subscriber", durable=True, mode="json")
            print(f"[GATEWAY] Connecting ACK subscriber: {broker_uri}")
            async with websockets.connect(broker_uri) as ws:
                print(f"[GATEWAY] ACK subscriber connected: {broker_uri}")
                backoff = ACK_SUBSCRIBER_RECONNECT_BASE

                while not stop_event.is_set():
                    recv_task = asyncio.create_task(ws.recv())
                    stop_task = asyncio.create_task(stop_event.wait())
                    done, pending = await asyncio.wait(
                        {recv_task, stop_task},
                        return_when=asyncio.FIRST_COMPLETED,
                    )
                    for task in pending:
                        task.cancel()
                    if pending:
                        await asyncio.gather(*pending, return_exceptions=True)

                    if stop_task in done:
                        recv_task.cancel()
                        await asyncio.gather(recv_task, return_exceptions=True)
                        break

                    await handle_storage_ack_delivery(recv_task.result())
        except asyncio.CancelledError:
            raise
        except websockets.exceptions.ConnectionClosed:
            if stop_event.is_set():
                break
            print("[GATEWAY] storage.ack subscriber connection closed.")
        except Exception as exc:
            print(f"[GATEWAY] storage.ack subscriber error: {exc}")

        if stop_event.is_set():
            break

        wait_seconds = backoff + random.random()
        print(f"[GATEWAY] Reconnecting storage.ack subscriber in {wait_seconds:.1f}s")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            backoff = min(ACK_SUBSCRIBER_RECONNECT_MAX, backoff * 2)

    print("[GATEWAY] storage.ack subscriber stopped.")


@app.on_event("startup")
async def start_storage_ack_listener() -> None:
    """Launch ACK consumers in the background."""
    stop_event = asyncio.Event()
    subscriber_task = asyncio.create_task(run_storage_ack_subscriber(stop_event))
    poller_task = asyncio.create_task(poll_storage_ack_queue(stop_event))
    app.state.storage_ack_stop_event = stop_event
    app.state.storage_ack_task = subscriber_task
    app.state.storage_ack_poller_task = poller_task


@app.on_event("shutdown")
async def stop_storage_ack_listener() -> None:
    """Stop the background ACK consumers gracefully."""
    stop_event = getattr(app.state, "storage_ack_stop_event", None)
    subscriber_task = getattr(app.state, "storage_ack_task", None)
    poller_task = getattr(app.state, "storage_ack_poller_task", None)
    if stop_event is not None:
        stop_event.set()
    if subscriber_task is not None:
        subscriber_task.cancel()
        with suppress(asyncio.CancelledError):
            await subscriber_task
    if poller_task is not None:
        poller_task.cancel()
        with suppress(asyncio.CancelledError):
            await poller_task


@app.websocket("/ws/broker/{topic}")
async def broker_ws(websocket: WebSocket, topic: str):
    """Handle one broker WebSocket connection for publishers and subscribers."""
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
                try:
                    publish_message = schemas.BrokerPublishMessage.model_validate(message)
                except ValidationError as exc:
                    await manager.send_protocol_message(
                        websocket,
                        {"action": "error", "detail": exc.errors(include_url=False)},
                    )
                    continue

                if publish_message.topic != topic:
                    await manager.send_protocol_message(
                        websocket,
                        {
                            "action": "error",
                            "detail": "Publish topic must match the WebSocket topic",
                            "topic": topic,
                        },
                    )
                    continue

                await publish_to_topic(
                    topic,
                    publish_message.payload,
                    payload_format=payload_format or mode,
                    durable=durable,
                )
            elif action == "ack":
                try:
                    ack_message = schemas.BrokerAckMessage.model_validate(message)
                except ValidationError as exc:
                    await manager.send_protocol_message(
                        websocket,
                        {"action": "error", "detail": exc.errors(include_url=False)},
                    )
                    continue

                acked = await run_in_threadpool(acknowledge_message, ack_message.message_id)
                await manager.send_protocol_message(
                    websocket,
                    {
                        "action": "ack",
                        "message_id": ack_message.message_id,
                        "status": "ok" if acked else "ignored",
                    },
                )
            else:
                await manager.send_protocol_message(
                    websocket,
                    {"action": "error", "detail": f"Unsupported action: {action}"},
                )
    except WebSocketDisconnect:
        pass
    except Exception as exc:
        print(f"[BROKER] Unexpected error on topic '{topic}': {exc}")
    finally:
        await manager.disconnect(websocket, topic)
