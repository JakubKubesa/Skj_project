"""Pydantic schemas used by the REST API and broker protocol.

This module centralizes request/response validation so that the FastAPI routes,
worker process, and WebSocket broker all speak the same typed protocol.
"""

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

_ALLOWED_OPERATIONS = {"invert", "flip", "crop", "brightness", "grayscale"}


def _ensure_int_param(params: dict[str, Any], key: str) -> None:
    """Require an integer parameter in a dict-based payload."""
    value = params.get(key)
    if not isinstance(value, int):
        raise ValueError(f"Parameter '{key}' must be an integer")


def _validate_operation_params(operation: str, params: dict[str, Any]) -> dict[str, Any]:
    """Validate operation-specific parameters for image processing jobs."""
    if operation not in _ALLOWED_OPERATIONS:
        raise ValueError(f"Unsupported operation: {operation}")
    if not isinstance(params, dict):
        raise ValueError("params must be an object")

    if operation in {"invert", "flip", "grayscale"}:
        if params:
            raise ValueError(f"Operation '{operation}' does not accept params")
        return params

    if operation == "brightness":
        unexpected = set(params) - {"value"}
        if unexpected:
            raise ValueError(f"Unexpected brightness params: {sorted(unexpected)}")
        value = params.get("value", 0)
        if not isinstance(value, int):
            raise ValueError("Brightness param 'value' must be an integer")
        return {"value": value}

    unexpected = set(params) - {"x_start", "y_start", "width", "height"}
    if unexpected:
        raise ValueError(f"Unexpected crop params: {sorted(unexpected)}")
    missing = [key for key in ("x_start", "y_start", "width", "height") if key not in params]
    if missing:
        raise ValueError(f"Missing crop params: {missing}")
    for key in ("x_start", "y_start", "width", "height"):
        _ensure_int_param(params, key)
    return params


class StrictSchema(BaseModel):
    """Base schema with strict field handling for the whole project."""

    model_config = ConfigDict(extra="forbid", from_attributes=True, str_strip_whitespace=True)


class ObjectResponse(StrictSchema):
    """Object metadata returned by bucket/object endpoints."""

    record_id: str
    object_id: str | None = None
    bucket_id: str
    object_key: str
    size: int
    status: Literal["uploading", "ready"]


class UploadAcceptedResponse(StrictSchema):
    """Immediate 202 response returned after enqueuing a storage write."""

    status: Literal["accepted"]
    topic: str
    bucket_id: str
    object_key: str
    record_id: str
    object_id: str


class BucketBilling(StrictSchema):
    """Aggregated usage counters tracked for one bucket."""

    current_storage_bytes: int
    ingress_bytes: int
    egress_bytes: int
    internal_transfer_bytes: int
    count_write_requests: int
    count_read_requests: int


class BucketCreate(StrictSchema):
    """Payload used to create a bucket."""

    name: str | None = Field(default=None, max_length=255)


class BucketResponse(StrictSchema):
    """Serialized bucket response including contained objects."""

    id: str
    name: str | None = None
    objects: list[ObjectResponse] = Field(default_factory=list)


class UserPublic(StrictSchema):
    """Public user data returned to the authenticated web client."""

    id: str
    username: str
    bucket_id: str


class RegisterRequest(StrictSchema):
    """Registration payload for the MUJ CLOUDIK web client."""

    username: str = Field(min_length=3, max_length=64, pattern=r"^[A-Za-z0-9_.-]+$")
    password: str = Field(min_length=6, max_length=128)


class LoginRequest(StrictSchema):
    """Login payload for the MUJ CLOUDIK web client."""

    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=1, max_length=128)


class AuthResponse(StrictSchema):
    """Bearer token response after login or registration."""

    token: str
    user: UserPublic


class ProcessRequest(StrictSchema):
    """Request body for asynchronous image processing jobs."""

    operation: Literal["invert", "flip", "crop", "brightness", "grayscale"]
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_params(self) -> "ProcessRequest":
        self.params = _validate_operation_params(self.operation, self.params)
        return self


class ProcessResponse(StrictSchema):
    """Immediate acknowledgement returned after enqueuing a processing job."""

    status: str
    topic: str
    bucket_id: str
    object_key: str
    operation: str


class BrokerPublishMessage(StrictSchema):
    """Protocol envelope used when a client publishes to a broker topic."""

    action: Literal["publish"]
    topic: str = Field(min_length=1)
    payload: Any


class BrokerAckMessage(StrictSchema):
    """Protocol envelope used by subscribers to acknowledge delivery."""

    action: Literal["ack"]
    message_id: int


class BrokerDeliverMessage(StrictSchema):
    """Protocol envelope emitted by the broker to subscribers."""

    action: Literal["deliver"]
    topic: str = Field(min_length=1)
    message_id: int | None = None
    payload: Any


class WorkerJobPayload(StrictSchema):
    """Validated payload consumed by the image worker from image.jobs."""

    operation: Literal["invert", "flip", "crop", "brightness", "grayscale"]
    object_key: str = Field(min_length=1)
    bucket_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def validate_params(self) -> "WorkerJobPayload":
        self.params = _validate_operation_params(self.operation, self.params)
        return self


class WorkerStatusPayload(StrictSchema):
    """Status message published by the worker after job handling."""

    status: Literal["completed", "failed"]
    operation: str | None = None
    bucket_id: str
    object_key: str | None = None
    error: str | None = None


class StorageWritePayload(StrictSchema):
    """Binary object payload delivered to the Haystack storage node."""

    object_id: str = Field(min_length=1)
    data: bytes


class StorageAckPayload(StrictSchema):
    """Offset metadata emitted after appending an object into a volume."""

    object_id: str = Field(min_length=1)
    volume_id: int = Field(ge=1)
    offset: int = Field(ge=0)
    size: int = Field(ge=0)


class StorageCompactionObject(StrictSchema):
    """Ready non-deleted object currently stored inside one Haystack volume."""

    record_id: str
    object_id: str
    bucket_id: str
    object_key: str
    volume_id: int = Field(ge=1)
    offset: int = Field(ge=0)
    size: int = Field(ge=0)


class StorageCompactionListResponse(StrictSchema):
    """List of active objects that still reference one source volume."""

    volume_id: int = Field(ge=1)
    objects: list[StorageCompactionObject] = Field(default_factory=list)


class StorageCompactionUpdateRequest(StrictSchema):
    """Internal relocation payload emitted by the compaction script."""

    source_volume_id: int = Field(ge=1)
    target_volume_id: int = Field(ge=1)
    target_offset: int = Field(ge=0)
    size: int = Field(ge=0)


class StorageCompactionUpdateResponse(StrictSchema):
    """Gateway reply after one object relocation update."""

    status: Literal["updated", "ignored"]
    object_id: str
    record_id: str | None = None
    volume_id: int | None = None
    offset: int | None = None
