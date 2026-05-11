"""Administrative Haystack volume compaction script.

The script compacts one existing volume by copying only ready, non-deleted
objects into a freshly allocated target volume. Object metadata stays owned by
the gateway: the script asks the gateway for the live object list and pushes
back relocated ``volume_id``/``offset`` values as it copies bytes.

This design intentionally uses a *new* volume id instead of rewriting the
source file in place. Reads remain valid during the run because already-moved
objects point to the new file while not-yet-moved objects still point to the
old one.
"""

from __future__ import annotations

import argparse
import os
import re
import sys
from pathlib import Path

import httpx

import schemas


DEFAULT_API_BASE_URL = os.getenv("API_BASE_URL", "http://127.0.0.1:8000")
DEFAULT_INTERNAL_API_TOKEN = os.getenv("INTERNAL_API_TOKEN", "dev-internal-token")
DEFAULT_VOLUME_DIR = Path(os.getenv("HAYSTACK_VOLUME_DIR", "haystack_volumes"))
DEFAULT_TIMEOUT = float(os.getenv("COMPACTION_HTTP_TIMEOUT_SECONDS", "30.0"))
VOLUME_PATTERN = re.compile(r"^volume_(\d+)\.dat$")


def parse_args() -> argparse.Namespace:
    """Parse CLI arguments for the compaction run."""
    parser = argparse.ArgumentParser(description="Compact one Haystack volume into a new volume id.")
    parser.add_argument("volume_id", type=int, help="Source volume id to compact.")
    parser.add_argument("--api-base-url", default=DEFAULT_API_BASE_URL, help="Gateway base URL.")
    parser.add_argument("--internal-token", default=DEFAULT_INTERNAL_API_TOKEN, help="Gateway internal API token.")
    parser.add_argument("--volume-dir", default=str(DEFAULT_VOLUME_DIR), help="Directory containing volume_*.dat files.")
    parser.add_argument(
        "--allow-active-volume",
        action="store_true",
        help="Allow compacting the highest existing volume id. Unsafe while Haystack is actively writing there.",
    )
    parser.add_argument(
        "--keep-source",
        action="store_true",
        help="Keep the source volume file even after all metadata moved successfully.",
    )
    return parser.parse_args()


def discover_volume_ids(base_dir: Path) -> list[int]:
    """Return all existing numeric volume ids in ascending order."""
    ids: list[int] = []
    if not base_dir.exists():
        return ids
    for candidate in base_dir.iterdir():
        match = VOLUME_PATTERN.fullmatch(candidate.name)
        if match and candidate.is_file():
            ids.append(int(match.group(1)))
    return sorted(ids)


def read_exact(handle, size: int) -> bytes:
    """Read exactly ``size`` bytes or raise an IOError."""
    data = handle.read(size)
    if len(data) != size:
        raise IOError(f"Expected {size} bytes, got {len(data)} bytes")
    return data


def read_bytes_at_offset(handle, offset: int, size: int) -> bytes:
    """Seek to one offset and read exactly the requested byte count."""
    handle.seek(offset)
    return read_exact(handle, size)


def append_bytes(handle, payload: bytes) -> tuple[int, int]:
    """Append bytes to the end of the target volume and return offset + size."""
    handle.seek(0, os.SEEK_END)
    offset = handle.tell()
    written = handle.write(payload)
    size = len(payload)
    if written != size:
        raise IOError(f"Expected to write {size} bytes, wrote {written} bytes")
    return offset, size


def fetch_live_objects(
    client: httpx.Client,
    api_base_url: str,
    headers: dict[str, str],
    volume_id: int,
) -> list[schemas.StorageCompactionObject]:
    """Fetch all ready non-deleted objects currently stored in one volume."""
    response = client.get(
        f"{api_base_url.rstrip('/')}/internal/storage/volumes/{volume_id}/objects",
        headers=headers,
    )
    response.raise_for_status()
    payload = schemas.StorageCompactionListResponse.model_validate(response.json())
    return payload.objects


def relocate_object(
    client: httpx.Client,
    api_base_url: str,
    headers: dict[str, str],
    object_entry: schemas.StorageCompactionObject,
    source_volume_id: int,
    target_volume_id: int,
    target_offset: int,
) -> schemas.StorageCompactionUpdateResponse:
    """Tell the gateway that one object was copied into the compacted volume."""
    request_payload = schemas.StorageCompactionUpdateRequest(
        source_volume_id=source_volume_id,
        target_volume_id=target_volume_id,
        target_offset=target_offset,
        size=object_entry.size,
    )
    response = client.post(
        f"{api_base_url.rstrip('/')}/internal/storage/objects/{object_entry.object_id}/relocate",
        headers=headers,
        json=request_payload.model_dump(),
    )
    response.raise_for_status()
    return schemas.StorageCompactionUpdateResponse.model_validate(response.json())


def compact_volume(
    source_volume_id: int,
    *,
    api_base_url: str,
    internal_token: str,
    volume_dir: Path,
    allow_active_volume: bool,
    keep_source: bool,
) -> int:
    """Compact one source volume into a newly allocated target volume id."""
    volume_dir = Path(volume_dir)
    source_path = volume_dir / f"volume_{source_volume_id}.dat"
    if not source_path.exists():
        print(f"[COMPACT] Source volume does not exist: {source_path}")
        return 1

    volume_ids = discover_volume_ids(volume_dir)
    if not volume_ids:
        print(f"[COMPACT] No volume files found in {volume_dir}")
        return 1

    highest_existing_volume_id = volume_ids[-1]
    if source_volume_id == highest_existing_volume_id and not allow_active_volume:
        print(
            "[COMPACT] Refusing to compact the highest existing volume id. "
            "Use --allow-active-volume only when you are sure the volume is not actively written."
        )
        return 1

    target_volume_id = highest_existing_volume_id + 1
    target_path = volume_dir / f"volume_{target_volume_id}.dat"
    if target_path.exists():
        print(f"[COMPACT] Target volume already exists, refusing to overwrite: {target_path}")
        return 1

    headers = {
        "x-internal-source": "true",
        "x-internal-token": internal_token,
        "Accept": "application/json",
    }

    with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
        live_objects = fetch_live_objects(client, api_base_url, headers, source_volume_id)
        if not live_objects:
            print(f"[COMPACT] Volume {source_volume_id} has no live objects.")
            if keep_source:
                print(f"[COMPACT] Keeping empty source file: {source_path.name}")
                return 0
            source_path.unlink()
            print(f"[COMPACT] Deleted empty source file: {source_path.name}")
            return 0

        target_path.parent.mkdir(parents=True, exist_ok=True)
        print(
            f"[COMPACT] Compacting volume {source_volume_id} -> {target_volume_id} "
            f"({len(live_objects)} live object(s))"
        )
        try:
            with open(source_path, "rb") as source_handle, open(target_path, "ab+") as target_handle:
                for object_entry in live_objects:
                    data = read_bytes_at_offset(source_handle, object_entry.offset, object_entry.size)
                    target_offset, written_size = append_bytes(target_handle, data)
                    if written_size != object_entry.size:
                        raise IOError(
                            f"Failed to fully write {object_entry.object_key}: "
                            f"expected {object_entry.size}, wrote {written_size}"
                        )
                    target_handle.flush()

                    update_result = relocate_object(
                        client,
                        api_base_url,
                        headers,
                        object_entry,
                        source_volume_id,
                        target_volume_id,
                        target_offset,
                    )
                    print(
                        f"[COMPACT] {object_entry.object_key} ({object_entry.object_id}) -> "
                        f"volume={target_volume_id} offset={target_offset} status={update_result.status}"
                    )
        except Exception as exc:
            print(f"[COMPACT] Compaction aborted: {exc}")
            print(f"[COMPACT] Leaving both source and target files in place for inspection.")
            return 1

        remaining_source_objects = fetch_live_objects(client, api_base_url, headers, source_volume_id)
        if remaining_source_objects:
            print(
                f"[COMPACT] Source volume {source_volume_id} is still referenced by "
                f"{len(remaining_source_objects)} live object(s); keeping source file."
            )
            return 1

    if keep_source:
        print(f"[COMPACT] Compaction finished, source kept as requested: {source_path.name}")
        return 0

    source_path.unlink()
    print(
        f"[COMPACT] Compaction finished. New volume: {target_path.name}. "
        f"Deleted old source: {source_path.name}"
    )
    return 0


def main() -> int:
    """CLI entrypoint."""
    args = parse_args()
    return compact_volume(
        args.volume_id,
        api_base_url=args.api_base_url,
        internal_token=args.internal_token,
        volume_dir=Path(args.volume_dir),
        allow_active_volume=args.allow_active_volume,
        keep_source=args.keep_source,
    )


if __name__ == "__main__":
    raise SystemExit(main())
