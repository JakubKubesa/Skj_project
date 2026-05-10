"""Main FastAPI application for the personal cloud and in-process message broker.

This module exposes:
- bucket/object REST endpoints with billing and soft delete support,
- a WebSocket broker with optional durable persistence,
- helper functions shared by the REST and broker flows.
"""

import json
import os
import hashlib
import secrets
import shutil
from pathlib import Path
from typing import Any, Dict, List, Optional

import msgpack
try:
    import msgpack as _msgpack_check
    print("[DEBUG] Msgpack je úspěšně nainstalován.")
except ImportError:
    print("[ERROR] Msgpack NENÍ nainstalován v prostředí Gateway!")
import httpx
from fastapi.responses import StreamingResponse
import time
import uuid
import asyncio
from contextlib import suppress
import websockets
from fastapi import Depends, FastAPI, HTTPException, Query, Request, UploadFile, WebSocket, WebSocketDisconnect, status, Response
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse as FastAPIFileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import ValidationError
from sqlalchemy.orm import Session

import models
import schemas
from broker_utils import manager
from database import SessionLocal, get_db, engine, Base
from fastapi.responses import JSONResponse
from fastapi.requests import Request as FastAPIRequest
import traceback

app = FastAPI()

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"
WEB_STATIC_DIR = WEB_DIR / "static"
STORAGE_DIR = "storage"
INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "dev-internal-token")
os.makedirs(STORAGE_DIR, exist_ok=True)

# Broker config used by gateway background subscriber/publish
BROKER_BASE_WS = os.getenv("BROKER_BASE_WS", "ws://127.0.0.1:8000")
HAYSTACK_BASE_URL = os.getenv("HAYSTACK_BASE_URL", "http://127.0.0.1:8002")
HAYSTACK_READ_MEDIA_TYPE = os.getenv("HAYSTACK_READ_MEDIA_TYPE", "application/octet-stream")


def build_haystack_url_or_400(volume_id: Optional[int], offset: Optional[int], size: Optional[int]) -> str:
    """Validate metadata and build haystack URL or raise 400/500.

    Ensures volume_id/offset/size are integers and returns the URL used to
    request the bytes from the Haystack node.
    """
    try:
        vid = int(volume_id)  # may raise TypeError/ValueError
        off = int(offset)
        sz = int(size)
    except Exception:
        raise HTTPException(status_code=500, detail="Invalid storage metadata")
    return f"{HAYSTACK_BASE_URL}/volume/{vid}/{off}/{sz}"
WRITE_TOPIC = os.getenv("WRITE_TOPIC", "storage.write")
ACK_TOPIC = os.getenv("ACK_TOPIC", "storage.ack")

if WEB_STATIC_DIR.exists():
    app.mount("/static", StaticFiles(directory=WEB_STATIC_DIR), name="static")


def bucket_object_path(bucket_id: str, object_key: str) -> str:
    """Return the on-disk path used for one object inside a bucket."""
    return os.path.join(STORAGE_DIR, bucket_id, object_key)



def increment_bucket_request_counter(bucket: models.Bucket, counter_name: str) -> None:
    """Increment one billing counter on a bucket model instance."""
    current_value = getattr(bucket, counter_name, 0) or 0
    setattr(bucket, counter_name, current_value + 1)



def object_to_response(db_object: models.ObjectModel) -> schemas.ObjectResponse:
    """Serialize one ORM object row into the public API response model."""
    return schemas.ObjectResponse(
        record_id=db_object.id,
        bucket_id=db_object.bucket_id or "",
        object_key=db_object.object_key,
        size=db_object.size,
    )


def file_response(path: str, filename: str) -> FastAPIFileResponse:
    """Return a file without browser caching so repeated downloads hit billing."""
    return FastAPIFileResponse(
        path=path,
        filename=filename,
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
    db_object = (
        db.query(models.ObjectModel)
        .filter(
            models.ObjectModel.bucket_id == bucket.id,
            models.ObjectModel.object_key == object_key,
            models.ObjectModel.is_deleted.is_(False),
        )
        .order_by(models.ObjectModel.created_at.desc())
        .first()
    )
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
    """Load one active object or raise 404 if it is missing or inaccessible."""
    db_object = get_active_object_by_key(db, bucket_id, object_key)
    if not db_object:
        raise HTTPException(status_code=404, detail="Object not found")
    # If the object points to a Haystack volume (logical pointer like
    # "volume_<id>.dat:<offset>"), don't require a local file to exist.
    if db_object.path and not db_object.path.startswith("volume_"):
        if not os.path.exists(db_object.path):
            raise HTTPException(status_code=404, detail="Object not found")
    return db_object



def save_bucket_object(
    db: Session,
    bucket_id: str,
    object_key: str,
    upload: UploadFile,
    *,
    user_id: str,
    is_internal: bool = False,
) -> models.ObjectModel:
    """Persist one uploaded object and update billing counters.

    Args:
        db: Active SQLAlchemy session.
        bucket_id: Target bucket identifier.
        object_key: Logical object name within the bucket.
        upload: FastAPI upload wrapper containing file contents.
        user_id: Required owner identifier stored with the object metadata.
        is_internal: Whether the transfer comes from the worker/internal API.
    """
    bucket = get_or_create_bucket(db, bucket_id)
    existing = get_active_object_by_key(db, bucket_id, object_key)

    destination = bucket_object_path(bucket_id, object_key)
    os.makedirs(os.path.dirname(destination), exist_ok=True)
    with open(destination, "wb") as handle:
        shutil.copyfileobj(upload.file, handle)

    new_size = os.path.getsize(destination)
    previous_size = existing.size if existing else 0
    size_delta = new_size - previous_size

    if existing:
        existing.path = destination
        existing.size = new_size
        existing.user_id = user_id
        db_object = existing
    else:
        db_object = models.ObjectModel(
            user_id=user_id,
            object_key=object_key,
            path=destination,
            size=new_size,
            bucket_id=bucket_id,
        )
        db.add(db_object)

    bucket.current_storage_bytes = max(0, (bucket.current_storage_bytes or 0) + size_delta)
    if is_internal:
        bucket.internal_transfer_bytes = (bucket.internal_transfer_bytes or 0) + new_size
    else:
        bucket.ingress_bytes = (bucket.ingress_bytes or 0) + new_size
    increment_bucket_request_counter(bucket, "count_write_requests")

    db.add(bucket)
    db.add(db_object)
    db.commit()
    db.refresh(db_object)
    return db_object



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



def soft_delete_bucket_object(db: Session, bucket_id: str, object_key: str) -> models.ObjectModel:
    """Mark one object row as deleted without removing the underlying file.

    This is a logical delete: API reads stop returning the object, but the file
    remains on disk. That behavior is useful for auditing, but it is not a full
    versioned archive because a later upload with the same object key reuses the
    same file path. Deletes update storage usage only; they do not count as new
    write requests.
    """
    db_object = get_bucket_object_or_404(db, bucket_id, object_key)
    db_object.is_deleted = True
    db.add(db_object)

    bucket = db.get(models.Bucket, bucket_id)
    if bucket:
        bucket.current_storage_bytes = max(0, (bucket.current_storage_bytes or 0) - (db_object.size or 0))
        db.add(bucket)

    db.commit()
    return db_object


@app.get("/", include_in_schema=False)
def cloudik_web_client():
    """Serve the MUJ CLOUDIK web client."""
    index_path = WEB_DIR / "index.html"
    if not index_path.exists():
        raise HTTPException(status_code=404, detail="Web client not found")
    return FastAPIFileResponse(index_path)


@app.post("/auth/register", response_model=schemas.AuthResponse)
def register_user(register_in: schemas.RegisterRequest, db: Session = Depends(get_db)) -> schemas.AuthResponse:
    """Create a user account and its personal bucket.

    Errors during DB operations are caught and logged — the endpoint
    returns a clean JSON error instead of a raw 500 HTML page.
    """
    try:
        username = normalize_username(register_in.username)
        existing = db.query(models.User).filter(models.User.username == username).first()
        if existing:
            raise HTTPException(status_code=409, detail="Username already exists")

        user = models.User(username=username, password_hash=hash_password(register_in.password))
        db.add(user)
        db.commit()
        db.refresh(user)

        # Ensure the user's personal bucket exists; helper will create it if missing
        get_user_personal_bucket(db, user)
        token = create_auth_session(db, user)
        return schemas.AuthResponse(token=token, user=user_to_public(db, user))
    except HTTPException:
        # Re-raise HTTPExceptions (like 409) untouched
        raise
    except Exception as exc:
        print("[GATEWAY] Error during registration:")
        traceback.print_exc()
        # Return 500 with JSON detail so frontend can parse it
        raise HTTPException(status_code=500, detail="Database error during registration")


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


@app.put("/me/objects/{object_key}")
async def upload_my_object(
    object_key: str,
    file: UploadFile,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Asynchronous upload: publish binary to Haystack via broker.

    The gateway generates a UUID `object_id`, stores a DB record with
    `status='uploading'` and publishes the payload to `storage.write` using
    MessagePack. The HTTP response is 202 Accepted — completion is signalled
    later via a storage.ack message processed by a background task.
    """
    # Ensure user's bucket exists
    bucket = get_user_personal_bucket(db, current_user)

    # Read all bytes from the uploaded file
    data = await file.read()

    # Generate stable object id and create DB row with uploading status
    object_id = str(uuid.uuid4())
    db_object = models.ObjectModel(
        id=object_id,
        user_id=current_user.id,
        object_key=object_key,
        path=f"pending/{object_id}",
        size=0,
        bucket_id=bucket.id,
        status="uploading",
    )
    db.add(db_object)
    db.commit()

    # Build storage.write payload and publish as MessagePack (durable)
    payload = schemas.StorageWritePayload(object_id=object_id, data=data).model_dump()
    # fire-and-forget publish so HTTP request returns quickly
    await publish_to_topic(WRITE_TOPIC, payload, payload_format="msgpack", durable=True)

    # Return accepted — actual metadata will be updated asynchronously
    return Response(status_code=202)


@app.get("/me/objects/{object_key}")
async def download_my_object(
    object_key: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Download one object from the authenticated user's bucket.

    If the object has been offloaded to Haystack, proxy the byte range
    request to the Haystack node and stream the response back to the user.
    """
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)

    if db_object.status != "ready" or not db_object.volume_id:
        # not yet available in Haystack or still local
        # fall back to the old behavior if a local path exists
        if db_object.path and os.path.exists(db_object.path):
            register_object_download(db, db_object, is_internal=False)
            return file_response(path=db_object.path, filename=db_object.object_key)
        # Signal that the object is pending; client should retry later
        return JSONResponse(status_code=202, content={"detail": "Object is being uploaded/processed"})

    # Register billing before streaming
    register_object_download(db, db_object, is_internal=False)

    # Build and validate Haystack URL
    url = build_haystack_url_or_400(db_object.volume_id, db_object.offset, db_object.size)
    print(f"[DEBUG] Calling Haystack: {url}")
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            async with client.stream("GET", url) as r:
                if r.status_code == 404:
                    raise HTTPException(status_code=404, detail="Object not found on storage node")
                if r.status_code != 200:
                    print(f"[ERROR] Haystack responded {r.status_code} for {url}")
                    raise HTTPException(status_code=502, detail="Error reading from storage node")
                return StreamingResponse(r.aiter_bytes(), media_type=HAYSTACK_READ_MEDIA_TYPE)
    except httpx.ConnectError as exc:
        print(f"[ERROR] Haystack Node is unreachable at {url}: {exc}")
        raise HTTPException(status_code=502, detail="Haystack unavailable")
    except httpx.RequestError as exc:
        print(f"[ERROR] Haystack request failed for {url}: {exc}")
        raise HTTPException(status_code=502, detail="Haystack request failed")


@app.get("/me/objects/{object_key}/preview", include_in_schema=False)
async def preview_my_object(
    object_key: str,
    current_user: models.User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Return an object preview without counting it as a user download.

    Previews are proxied from Haystack when the object is ready.
    """
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)

    if db_object.status != "ready" or not db_object.volume_id:
        if db_object.path and os.path.exists(db_object.path):
            return file_response(path=db_object.path, filename=db_object.object_key)
        return JSONResponse(status_code=202, content={"detail": "Object is being uploaded/processed"})

    url = build_haystack_url_or_400(db_object.volume_id, db_object.offset, db_object.size)
    print(f"[DEBUG] Calling Haystack: {url}")
    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            r = await client.get(url)
            if r.status_code == 200:
                return Response(content=r.content, media_type=HAYSTACK_READ_MEDIA_TYPE)
            if r.status_code == 404:
                raise HTTPException(status_code=404, detail="Object not found on storage node")
            print(f"[ERROR] Haystack responded {r.status_code} for {url}")
            raise HTTPException(status_code=502, detail="Error reading from storage node")
    except httpx.ConnectError as exc:
        print(f"[ERROR] Haystack Node is unreachable at {url}: {exc}")
        raise HTTPException(status_code=502, detail="Haystack unavailable")
    except httpx.RequestError as exc:
        print(f"[ERROR] Haystack request failed for {url}: {exc}")
        raise HTTPException(status_code=502, detail="Haystack request failed")


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
    """Process one object in-place using Haystack storage semantics.

    Steps:
    - Ensure object exists and is ready in Haystack
    - Download original bytes from Haystack
    - Run the NumPy-based transform using image_processor.process_image
      (via temporary files to avoid loading large images into memory on the
      main thread)
    - Publish the transformed bytes back to the broker on `storage.write`
      using the same `object_id` so the Haystack node will append a new
      version and the ACK subscriber will update DB metadata.
    """
    db_object = get_user_bucket_object_or_404(db, current_user, object_key)
    if db_object.is_deleted:
        raise HTTPException(status_code=404, detail="Object not found")

    if db_object.status != "ready" or not db_object.volume_id:
        raise HTTPException(status_code=409, detail="Object not available for processing")

    # Download original bytes from Haystack
    url = f"{HAYSTACK_BASE_URL}/volume/{db_object.volume_id}/{db_object.offset}/{db_object.size}"
    try:
        async with httpx.AsyncClient(timeout=60.0) as client:
            r = await client.get(url)
            if r.status_code != 200:
                if r.status_code == 404:
                    raise HTTPException(status_code=404, detail="Object not found on storage node")
                raise HTTPException(status_code=502, detail="Error reading from storage node")
            original = r.content
    except httpx.RequestError:
        raise HTTPException(status_code=502, detail="Haystack unavailable")

    # Try to process locally; if processing dependencies are missing or
    # processing fails, enqueue a worker job that includes object_id so the
    # worker can publish the transformed bytes back to storage.write.
    try:
        # Prepare temp files and run CPU-bound processing off the event loop.
        # Ensure filenames contain a proper extension derived from object_key so
        # Pillow can infer output format.
        os.makedirs("temp_in", exist_ok=True)
        os.makedirs("temp_out", exist_ok=True)
        base_name = os.path.basename(object_key) or db_object.id
        ext = os.path.splitext(base_name)[1] or ".png"
        safe_base = f"{db_object.id}_{base_name}"
        in_path = os.path.join("temp_in", safe_base + ext)
        out_path = os.path.join("temp_out", safe_base + ext)

        await asyncio.to_thread(lambda: open(in_path, "wb").write(original))

        # Call blocking process_image in a thread
        from image_processor import process_image

        await asyncio.to_thread(process_image, in_path, out_path, process_in.operation, process_in.params)
        transformed = await asyncio.to_thread(lambda: open(out_path, "rb").read())

        # Best-effort cleanup of temp files
        try:
            if os.path.exists(in_path):
                os.remove(in_path)
        except Exception:
            pass
        try:
            if os.path.exists(out_path):
                os.remove(out_path)
        except Exception:
            pass

        # Publish transformed bytes back to Haystack via broker using same object_id
        print(f"[GATEWAY] Sending transformed data to Haystack for object_id: {db_object.id}")
        write_payload = schemas.StorageWritePayload(object_id=db_object.id, data=transformed).model_dump()
        await publish_to_topic(WRITE_TOPIC, write_payload, payload_format="msgpack", durable=True)

        # Inform client so it can bust caches (frontend should append ?t=cache_bust)
        cache_bust = int(time.time() * 1000)

        bucket_id = db_object.bucket_id or get_user_personal_bucket(db, current_user).id
        return schemas.ProcessResponse(
            status="processing_started",
            topic="storage.write",
            bucket_id=bucket_id,
            object_key=object_key,
            operation=process_in.operation,
            cache_bust=cache_bust,
        )
    except Exception as exc:
        # Fallback: enqueue a worker job that includes the object_id so the
        # worker can perform processing and publish the new bytes to storage.write.
        print(f"[GATEWAY] Local processing failed, falling back to worker: {exc}")
        # Build worker job payload including object_id
        job_payload = schemas.WorkerJobPayload(
            operation=process_in.operation,
            object_key=object_key,
            bucket_id=db_object.bucket_id or get_user_personal_bucket(db, current_user).id,
            user_id=current_user.id,
            params=process_in.params,
        ).model_dump()
        # Attach object_id so worker can reuse it
        job_payload["object_id"] = db_object.id
        await publish_to_topic("image.jobs", job_payload, payload_format="json", durable=True)
        return schemas.ProcessResponse(
            status="processing_started",
            topic="image.jobs",
            bucket_id=db_object.bucket_id,
            object_key=object_key,
            operation=process_in.operation,
            cache_bust=int(time.time() * 1000),
        )


@app.get("/buckets/{bucket_id}/objects/{object_key}", include_in_schema=False)
def download_bucket_object(bucket_id: str, object_key: str, request: Request, db: Session = Depends(get_db)):
    """Download one object and account for egress or internal transfer."""
    require_internal_request(request)
    db_object = get_active_object_by_key(db, bucket_id, object_key)
    if not db_object:
        raise HTTPException(status_code=404, detail="Object not found")

    is_internal = request.headers.get("x-internal-source", "false").lower() == "true"

    # If object is stored in Haystack, proxy the request
    if db_object.status == "ready" and db_object.volume_id is not None:
        register_object_download(db, db_object, is_internal=is_internal)
        url = f"{HAYSTACK_BASE_URL}/volume/{db_object.volume_id}/{db_object.offset}/{db_object.size}"
        try:
            async def _stream():
                async with httpx.AsyncClient(timeout=30.0) as client:
                    async with client.stream("GET", url) as r:
                        if r.status_code == 404:
                            raise HTTPException(status_code=404, detail="Object not found on storage node")
                        if r.status_code != 200:
                            raise HTTPException(status_code=502, detail="Error reading from storage node")
                        async for chunk in r.aiter_bytes():
                            yield chunk

            return StreamingResponse(_stream(), media_type=HAYSTACK_READ_MEDIA_TYPE)
        except httpx.RequestError:
            raise HTTPException(status_code=502, detail="Haystack unavailable")

    # fallback to local path if present
    if db_object.path and os.path.exists(db_object.path):
        register_object_download(db, db_object, is_internal=is_internal)
        return file_response(path=db_object.path, filename=db_object.object_key)

    raise HTTPException(status_code=404, detail="Object not found")


@app.put("/buckets/{bucket_id}/objects/{object_key}", include_in_schema=False)
def upload_bucket_object(
    bucket_id: str,
    object_key: str,
    request: Request,
    file: UploadFile,
    user_id: str = Query(..., min_length=1),
    db: Session = Depends(get_db),
):
    """Upload or replace one object in a bucket and persist metadata/billing.

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
    db_object = save_bucket_object(
        db,
        bucket_id,
        object_key,
        file,
        user_id=user_id,
        is_internal=is_internal,
    )
    return {
        "status": "ok",
        "bucket_id": bucket_id,
        "object_key": object_key,
        "record_id": db_object.id,
        "size": db_object.size,
    }


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


@app.delete("/download/{object_id}")
def soft_delete_by_id(object_id: str, current_user: models.User = Depends(get_current_user), db: Session = Depends(get_db)):
    """Soft delete an object by id (user-facing). Does not delete physical data.

    This marks the DB row as deleted so it no longer appears in listings or
    can be downloaded by the owning user.
    """
    obj = db.get(models.ObjectModel, object_id)
    if not obj or obj.is_deleted:
        raise HTTPException(status_code=404, detail="Object not found")
    # Ensure ownership
    if obj.user_id != current_user.id:
        raise HTTPException(status_code=403, detail="Forbidden")

    obj.is_deleted = True
    db.add(obj)
    db.commit()
    return {"message": "Object soft-deleted", "object_id": object_id}


@app.get("/admin/volumes/{volume_id}/active-objects", include_in_schema=False)
def admin_list_active_objects(volume_id: int, request: Request, db: Session = Depends(get_db)):
    """Return active (non-deleted) objects residing in one volume.

    This endpoint is intended for internal administration and compaction.
    It requires the internal API headers.
    """
    require_internal_request(request)
    objs = (
        db.query(models.ObjectModel)
        .filter(models.ObjectModel.volume_id == volume_id, models.ObjectModel.is_deleted.is_(False))
        .order_by(models.ObjectModel.offset.asc())
        .all()
    )
    return [
        {
            "object_id": o.id,
            "object_key": o.object_key,
            "offset": o.offset,
            "size": o.size,
        }
        for o in objs
    ]


@app.patch("/admin/objects/{object_id}/relocate", include_in_schema=False)
def admin_relocate_object(object_id: str, payload: dict, request: Request, db: Session = Depends(get_db)):
    """Update one object's volume/offset metadata during compaction.

    Expects JSON body: {"volume_id": "1", "offset": 12345, "size": 987}
    """
    require_internal_request(request)
    db_object = db.get(models.ObjectModel, object_id)
    if not db_object:
        raise HTTPException(status_code=404, detail="Object not found")

    try:
        new_volume = int(payload.get("volume_id"))
        new_offset = int(payload.get("offset"))
        new_size = int(payload.get("size", db_object.size or 0))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid payload")

    db_object.volume_id = new_volume
    db_object.offset = new_offset
    db_object.size = new_size
    db_object.path = f"volume_{new_volume}.dat:{new_offset}"
    db.add(db_object)
    db.commit()
    return {"status": "ok", "object_id": object_id, "volume_id": new_volume, "offset": new_offset, "size": new_size}


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



def serialize_payload(payload: Any, payload_format: str) -> bytes:
    """Serialize broker payload bytes using JSON or MessagePack."""
    if payload_format == "msgpack":
        return msgpack.packb(payload, use_bin_type=True)
    # Fallback: ensure JSON serializable; convert bytes -> base64 strings
    def _convert(obj):
        import base64
        if isinstance(obj, (bytes, bytearray)):
            return {"__type": "bytes", "data": base64.b64encode(bytes(obj)).decode("ascii")}
        if isinstance(obj, dict):
            return {k: _convert(v) for k, v in obj.items()}
        if isinstance(obj, list):
            return [_convert(v) for v in obj]
        return obj

    safe = _convert(payload)
    return json.dumps(safe).encode("utf-8")



def deserialize_payload(payload: bytes, payload_format: str) -> Any:
    """Deserialize broker payload bytes into Python objects."""
    if payload_format == "msgpack":
        return msgpack.unpackb(payload, raw=False)
    return json.loads(payload.decode("utf-8"))



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
    # If payload contains raw bytes and caller requested JSON, switch to msgpack
    def _contains_bytes(obj) -> bool:
        if isinstance(obj, (bytes, bytearray)):
            return True
        if isinstance(obj, dict):
            return any(_contains_bytes(v) for v in obj.values())
        if isinstance(obj, (list, tuple)):
            return any(_contains_bytes(v) for v in obj)
        return False

    if payload_format == "json" and _contains_bytes(payload):
        payload_format = "msgpack"

    if durable:
        deliver_message = await run_in_threadpool(store_queued_message, topic, payload, payload_format)
    else:
        deliver_message = build_transient_deliver_message(topic, payload)

    await manager.broadcast(deliver_message, topic)
    return deliver_message


async def send_broker_ack(topic: str, message_id: int) -> bool:
    """Send an ACK action to the broker for a delivered durable message.

    This uses a temporary WebSocket publisher connection so the broker can
    mark the queued message as acknowledged and avoid redelivery.
    """
    uri = f"{BROKER_BASE_WS}/ws/broker/{topic}?mode=msgpack&role=publisher&durable=false"
    envelope = schemas.BrokerAckMessage(action="ack", message_id=message_id).model_dump()
    try:
        async with websockets.connect(uri) as ws:
            await ws.send(msgpack.packb(envelope, use_bin_type=True))
            raw = await ws.recv()
            if isinstance(raw, (bytes, bytearray)):
                response = msgpack.unpackb(raw, raw=False)
            else:
                response = json.loads(raw)
            return response.get("action") == "ack" and response.get("message_id") == message_id
    except Exception as exc:
        print(f"[GATEWAY] Failed to ACK broker message {message_id}: {exc}")
        return False


async def run_storage_ack_subscriber(stop_event: asyncio.Event) -> None:
    """Keep one durable subscription to storage.ack and update DB records.

    The subscriber updates object rows previously created with status
    'uploading' and marks them as 'ready' when the Haystack node confirms
    the append with volume/offset/size metadata.
    """
    backoff = 1.0
    while not stop_event.is_set():
        try:
            uri = f"{BROKER_BASE_WS}/ws/broker/{ACK_TOPIC}?mode=msgpack&role=subscriber&durable=false"
            print(f"[GATEWAY] Connecting to broker ACK topic: {uri}")
            async with websockets.connect(uri, ping_interval=20, ping_timeout=10) as ws:
                print(f"[GATEWAY] Connected to broker ACK topic")
                backoff = 1.0

                while not stop_event.is_set():
                    raw = await ws.recv()
                    if isinstance(raw, (bytes, bytearray)):
                        try:
                            data = msgpack.unpackb(raw, raw=False)
                        except Exception:
                            print("[GATEWAY] Invalid ACK msgpack frame from broker")
                            continue
                    else:
                        try:
                            data = json.loads(raw)
                        except Exception:
                            print("[GATEWAY] Invalid ACK frame from broker")
                            continue

                    action = data.get("action")
                    if action != "deliver":
                        # ignore ack/error messages or others
                        continue

                    try:
                        deliver = schemas.BrokerDeliverMessage.model_validate(data)
                        ack_payload = schemas.StorageAckPayload.model_validate(deliver.payload)
                    except ValidationError as exc:
                        print(f"[GATEWAY] Invalid storage.ack payload: {exc.errors(include_url=False)}")
                        # still ack the broker delivery so it won't redeliver
                        if isinstance(deliver.message_id, int):
                            await send_broker_ack(ACK_TOPIC, deliver.message_id)
                        continue

                    # Update DB record for the object
                    db = SessionLocal()
                    try:
                        obj = db.get(models.ObjectModel, ack_payload.object_id)
                        if not obj:
                            print(f"[GATEWAY] ACK for unknown object_id={ack_payload.object_id}")
                        else:
                            obj.volume_id = ack_payload.volume_id
                            obj.offset = ack_payload.offset
                            obj.size = ack_payload.size
                            obj.path = f"volume_{ack_payload.volume_id}.dat:{ack_payload.offset}"
                            obj.status = "ready"
                            db.add(obj)
                            db.commit()
                            print(f"[DEBUG] Objekt {ack_payload.object_id} je nyní READY")
                            print(f"[GATEWAY] PŘIJATO ACK pro objekt {ack_payload.object_id}")
                    except Exception as exc:
                        print(f"[GATEWAY] Failed to update object metadata: {exc}")
                    finally:
                        db.close()

                    # ACK the broker message so it is removed from queue
                    if isinstance(deliver.message_id, int):
                        await send_broker_ack(ACK_TOPIC, deliver.message_id)
        except asyncio.CancelledError:
            raise
        except websockets.exceptions.ConnectionClosed:
            if stop_event.is_set():
                break
            print("[GATEWAY] Broker ACK connection closed.")
        except Exception as exc:
            print(f"[GATEWAY] Broker ACK subscriber error: {exc}")

        if stop_event.is_set():
            break

        wait_seconds = backoff + (0.5 * (0.5 + (0.5 * backoff)))
        print(f"[GATEWAY] Reconnecting to ACK topic in {wait_seconds:.1f}s")
        try:
            await asyncio.wait_for(stop_event.wait(), timeout=wait_seconds)
            break
        except asyncio.TimeoutError:
            backoff = min(30.0, backoff * 2)


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
        # Re-introduce minimal publish/ack handling (no durable pending delivery)
        while True:
            try:
                data = await websocket.receive()
            except WebSocketDisconnect:
                break

            try:
                message, payload_format = decode_broker_message(data)
            except Exception:
                await manager.send_protocol_message(websocket, {"action": "error", "detail": "Invalid message format"})
                continue

            if not message:
                continue

            action = message.get("action")
            if action == "publish":
                try:
                    publish_message = schemas.BrokerPublishMessage.model_validate(message)
                except ValidationError as exc:
                    await manager.send_protocol_message(websocket, {"action": "error", "detail": exc.errors(include_url=False)})
                    continue

                if publish_message.topic != topic:
                    await manager.send_protocol_message(websocket, {"action": "error", "detail": "Publish topic must match the WebSocket topic", "topic": topic})
                    continue

                # Log publish receipt
                if publish_message.topic == "storage.write":
                    print(f"[BROKER] Publish received for storage.write (format={payload_format})")

                await publish_to_topic(topic, publish_message.payload, payload_format=payload_format or mode, durable=False)

            elif action == "ack":
                try:
                    ack_message = schemas.BrokerAckMessage.model_validate(message)
                except ValidationError as exc:
                    await manager.send_protocol_message(websocket, {"action": "error", "detail": exc.errors(include_url=False)})
                    continue

                acked = await run_in_threadpool(acknowledge_message, ack_message.message_id)
                await manager.send_protocol_message(websocket, {"action": "ack", "message_id": ack_message.message_id, "status": "ok" if acked else "ignored"})
            else:
                await manager.send_protocol_message(websocket, {"action": "error", "detail": f"Unsupported action: {action}"})
    except Exception as e:
        print(f"[BROKER CRASH] Chyba při obsluze klienta: {e}")
        import traceback
        traceback.print_exc()
    finally:
        print(f"[DEBUG] Connection to {topic} closed by client or server.")
        await manager.disconnect(websocket, topic)


@app.on_event("startup")
async def _start_ack_subscriber():
    # Ensure all DB tables exist before handling requests
    try:
        print("[GATEWAY] Ensuring database tables exist...")
        Base.metadata.create_all(bind=engine)
    except Exception as exc:
        print("[GATEWAY] Failed to create database tables:")
        traceback.print_exc()
        raise

    # start background task to listen for storage.ack messages
    stop_event = asyncio.Event()
    app.state._ack_stop = stop_event
    app.state._ack_task = asyncio.create_task(run_storage_ack_subscriber(stop_event))


@app.on_event("shutdown")
async def _stop_ack_subscriber():
    stop_event = getattr(app.state, "_ack_stop", None)
    task = getattr(app.state, "_ack_task", None)
    if stop_event is not None:
        stop_event.set()
    if task is not None:
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task



@app.exception_handler(Exception)
async def _handle_all_exceptions(request: FastAPIRequest, exc: Exception):
    """Global exception handler: log full traceback and return JSON error.

    This ensures the frontend always receives valid JSON for errors and
    the server console retains the full traceback for debugging.
    """
    print("[GATEWAY] Unhandled exception:")
    traceback.print_exc()
    return JSONResponse(status_code=500, content={"detail": "Internal Server Error"})
