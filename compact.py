#!/usr/bin/env python3
"""Compaction script for Haystack volume files.

Usage: python compact.py --volume 1 --gateway http://127.0.0.1:8000

This script contacts the gateway admin endpoint to list active objects
in the given volume, copies their bytes into a new compacted volume file
and PATCHes the gateway to relocate each object's metadata.
"""
import argparse
import httpx
import os
import sys


def main():
    p = argparse.ArgumentParser()
    p.add_argument("volume", type=int, help="Volume id to compact")
    p.add_argument("--gateway", default=os.getenv("GATEWAY_URL", "http://127.0.0.1:8000"))
    args = p.parse_args()

    volume_id = args.volume
    gateway = args.gateway.rstrip("/")

    # Require internal token for admin operations
    token = os.getenv("INTERNAL_API_TOKEN")
    if not token:
        print("[COMPACT] INTERNAL_API_TOKEN not set. Please set INTERNAL_API_TOKEN environment variable to authenticate with the gateway admin endpoints.")
        print("Example (bash): export INTERNAL_API_TOKEN=dev-internal-token")
        sys.exit(2)

    admin_list_url = f"{gateway}/admin/volumes/{volume_id}/active-objects"
    print(f"[COMPACT] Fetching active objects from gateway: {admin_list_url}")

    try:
        r = httpx.get(admin_list_url, timeout=30.0)
        r.raise_for_status()
        objects = r.json()
    except Exception as exc:
        print(f"[COMPACT] Failed to fetch active objects: {exc}")
        sys.exit(1)

    if not objects:
        print("[COMPACT] No active objects found — nothing to do.")
        return

    # Resolve haystack volumes directory; respect HAYSTACK_VOLUME_DIR env var
    base_dir = os.getenv("HAYSTACK_VOLUME_DIR") or os.path.join(os.getcwd(), "haystack_volumes")
    base_dir = os.path.abspath(base_dir)
    old_path = os.path.join(base_dir, f"volume_{volume_id}.dat")
    new_path = os.path.join(base_dir, f"volume_{volume_id}_compacted.dat")

    print(f"[COMPACT] Looking for volume_{volume_id}.dat in: {base_dir}")
    if os.path.exists(old_path):
        print(f"[COMPACT] Found source volume: {old_path} (size={os.path.getsize(old_path)} bytes)")
    else:
        print(f"[COMPACT] Source volume not found at: {old_path}")

    if not os.path.exists(old_path):
        print(f"[COMPACT] Volume file not found: {old_path}")
        sys.exit(1)

    total_moved = 0
    try:
        with open(old_path, "rb") as oldf, open(new_path, "wb+") as newf:
            new_offset = 0
            for obj in objects:
                oid = obj.get("object_id")
                off = int(obj.get("offset"))
                sz = int(obj.get("size"))

                try:
                    oldf.seek(off)
                    data = oldf.read(sz)
                    if len(data) != sz:
                        print(f"[COMPACT] Warning: read {len(data)} bytes for {oid}, expected {sz}")
                except Exception as exc:
                    print(f"[COMPACT] Failed to read object {oid} at {off}:{sz}: {exc}")
                    continue

                # write to new file
                try:
                    newf.seek(new_offset)
                    newf.write(data)
                    newf.flush()
                except Exception as exc:
                    print(f"[COMPACT] Failed to write object {oid} at new offset {new_offset}: {exc}")
                    sys.exit(1)

                # notify gateway about new location
                relocate_url = f"{gateway}/admin/objects/{oid}/relocate"
                payload = {"volume_id": str(volume_id), "offset": new_offset, "size": sz}
                try:
                    headers = {}
                    token = os.getenv("INTERNAL_API_TOKEN")
                    if token:
                        headers = {"x-internal-source": "true", "x-internal-token": token}
                    pr = httpx.patch(relocate_url, json=payload, timeout=10.0, headers=headers)
                    pr.raise_for_status()
                except Exception as exc:
                    print(f"[COMPACT] Failed to PATCH relocate for {oid}: {exc}")
                    sys.exit(1)

                print(f"[COMPACT] Relocated {oid}: {off}->{new_offset} (size={sz})")
                total_moved += sz
                new_offset += sz

        # replace old file with compacted (handle Windows file locking gracefully)
        backup = old_path + ".bak"
        try:
            os.replace(old_path, backup)
            os.replace(new_path, old_path)
            print(f"[COMPACT] Completed. Total moved: {total_moved} bytes. Old file backed up as {backup}")
        except PermissionError as exc:
            print(f"[COMPACT] Could not replace volume file due to permission error: {exc}")
            print("[COMPACT] It is likely Haystack node has the file open. Please temporarily stop Haystack Node and re-run this script.")
            sys.exit(2)
    except Exception as exc:
        print(f"[COMPACT] Unexpected error: {exc}")
        if os.path.exists(new_path):
            os.remove(new_path)
        sys.exit(1)


if __name__ == '__main__':
    main()
