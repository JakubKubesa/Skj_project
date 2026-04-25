"""Integration tests for the bucket/object REST API."""

import io
import os
import shutil
import uuid

from fastapi.testclient import TestClient
from PIL import Image

import models
from database import SessionLocal
from main import app

client = TestClient(app)



def make_image_bytes() -> bytes:
    """Create a tiny in-memory PNG used by upload tests."""
    image = Image.new("RGB", (6, 6), color=(10, 20, 30))
    buffer = io.BytesIO()
    image.save(buffer, format="PNG")
    return buffer.getvalue()



def clear_bucket(bucket_id: str) -> None:
    """Remove test data from the DB and filesystem for one bucket."""
    db = SessionLocal()
    try:
        db.query(models.ObjectModel).filter(models.ObjectModel.bucket_id == bucket_id).delete()
        bucket = db.get(models.Bucket, bucket_id)
        if bucket:
            db.delete(bucket)
        db.commit()
    finally:
        db.close()

    bucket_dir = os.path.join("storage", bucket_id)
    if os.path.isdir(bucket_dir):
        shutil.rmtree(bucket_dir)



def test_bucket_object_upload_list_and_delete_flow():
    """Uploading, listing, and deleting an object should work end to end."""
    bucket_id = f"bucket-{uuid.uuid4().hex[:8]}"
    object_key = f"obj-{uuid.uuid4().hex[:8]}.png"
    user_id = f"user-{uuid.uuid4().hex[:8]}"

    try:
        response = client.put(
            f"/buckets/{bucket_id}/objects/{object_key}",
            params={"user_id": user_id},
            files={"file": (object_key, make_image_bytes(), "image/png")},
        )
        assert response.status_code == 200
        body = response.json()
        assert body["bucket_id"] == bucket_id
        assert body["object_key"] == object_key
        assert "record_id" in body

        list_response = client.get(f"/buckets/{bucket_id}/objects/")
        assert list_response.status_code == 200
        items = list_response.json()
        assert len(items) == 1
        assert items[0]["bucket_id"] == bucket_id
        assert items[0]["object_key"] == object_key
        assert items[0]["record_id"] == body["record_id"]

        delete_response = client.delete(f"/buckets/{bucket_id}/objects/{object_key}")
        assert delete_response.status_code == 200

        list_response = client.get(f"/buckets/{bucket_id}/objects/")
        assert list_response.status_code == 200
        assert list_response.json() == []
    finally:
        clear_bucket(bucket_id)



def test_soft_delete_marks_row_and_hides_object_from_api():
    """Soft delete should hide the object from list/download but keep the DB row."""
    bucket_id = f"bucket-{uuid.uuid4().hex[:8]}"
    object_key = f"obj-{uuid.uuid4().hex[:8]}.png"
    user_id = f"user-{uuid.uuid4().hex[:8]}"

    try:
        upload_response = client.put(
            f"/buckets/{bucket_id}/objects/{object_key}",
            params={"user_id": user_id},
            files={"file": (object_key, make_image_bytes(), "image/png")},
        )
        assert upload_response.status_code == 200

        delete_response = client.delete(f"/buckets/{bucket_id}/objects/{object_key}")
        assert delete_response.status_code == 200

        download_response = client.get(f"/buckets/{bucket_id}/objects/{object_key}")
        assert download_response.status_code == 404

        list_response = client.get(f"/buckets/{bucket_id}/objects/")
        assert list_response.status_code == 200
        assert list_response.json() == []

        db = SessionLocal()
        try:
            db_object = (
                db.query(models.ObjectModel)
                .filter(
                    models.ObjectModel.bucket_id == bucket_id,
                    models.ObjectModel.object_key == object_key,
                )
                .one()
            )
            assert db_object.is_deleted is True
            assert db_object.user_id == user_id
        finally:
            db.close()
    finally:
        clear_bucket(bucket_id)



def test_invalid_process_request_is_rejected_by_pydantic_validation():
    """Unsupported image operations should fail before a broker job is published."""
    bucket_id = f"bucket-{uuid.uuid4().hex[:8]}"
    object_key = f"obj-{uuid.uuid4().hex[:8]}.png"
    user_id = f"user-{uuid.uuid4().hex[:8]}"

    try:
        upload_response = client.put(
            f"/buckets/{bucket_id}/objects/{object_key}",
            params={"user_id": user_id},
            files={"file": (object_key, make_image_bytes(), "image/png")},
        )
        assert upload_response.status_code == 200

        process_response = client.post(
            f"/buckets/{bucket_id}/objects/{object_key}/process",
            json={"operation": "explode", "params": {}},
        )
        assert process_response.status_code == 422
    finally:
        clear_bucket(bucket_id)



def test_new_object_upload_requires_explicit_user_id():
    """New uploads must provide user_id explicitly instead of inheriting bucket_id."""
    bucket_id = f"bucket-{uuid.uuid4().hex[:8]}"
    object_key = f"obj-{uuid.uuid4().hex[:8]}.png"

    try:
        response = client.put(
            f"/buckets/{bucket_id}/objects/{object_key}",
            files={"file": (object_key, make_image_bytes(), "image/png")},
        )
        assert response.status_code == 422
        assert "user_id" in str(response.json())
    finally:
        clear_bucket(bucket_id)
