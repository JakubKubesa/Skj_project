import asyncio
import json
import re
import os
import random
import signal
import sys
from typing import Optional

import websockets
import httpx

from image_processor import process_image

# Worker configuration
BROKER_URI = "ws://localhost:8000/ws/broker/image.jobs"
RECONNECT_BASE = 1.0  # seconds
RECONNECT_MAX = 30.0  # seconds


def _fix_json_text(text: str) -> str:
    """Try to repair malformed JSON-like text where keys are unquoted (common
    when shells strip quotes). This is a best-effort fixer and may not handle
    all cases. It will:
      - replace single quotes with double quotes
      - quote unquoted object keys like: {key: -> {"key":
    """
    # Replace single quotes with double quotes
    t = text.replace("'", '"')

    # Add quotes around unquoted keys: { key:  or , key:
    # Use a regex to find occurrences of unquoted keys (starting with letter/_)
    pattern = re.compile(r'([\{,]\s*)([A-Za-z_][A-Za-z0-9_]*)\s*:\s*')
    t = pattern.sub(r'\1"\2": ', t)

    return t


async def send_status(payload_dict: dict) -> None:
    """Send a status message to the broker's image.done topic using a
    separate short-lived WebSocket connection.

    This ensures the publish topic matches the WebSocket endpoint.
    """
    uri = "ws://localhost:8000/ws/broker/image.done"
    envelope = {"action": "publish", "topic": "image.done", "payload": json.dumps(payload_dict)}
    try:
        async with websockets.connect(uri) as ws2:
            await ws2.send(json.dumps(envelope))
    except Exception as exc:
        print(f"[WORKER] Chyba při odesílání statusu: {exc}")


async def download_image(bucket_id: str, object_id: str, temp_path: str) -> None:
    """Download object from server to temp_path using async HTTP GET."""
    url = f"http://localhost:8000/buckets/{bucket_id}/objects/{object_id}"
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            # write content
            with open(temp_path, "wb") as f:
                f.write(resp.content)
    except httpx.HTTPStatusError as e:
        raise FileNotFoundError(f"Remote object not found: {e}")
    except Exception as e:
        raise


async def upload_image(bucket_id: str, object_id: str, file_path: str) -> None:
    """Upload local file to server via async HTTP POST multipart."""
    url = f"http://localhost:8000/buckets/{bucket_id}/objects/{object_id}/upload"
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            with open(file_path, "rb") as f:
                files = {"file": (object_id, f, "application/octet-stream")}
                resp = await client.post(url, files=files)
                resp.raise_for_status()
    except Exception:
        raise


async def handle_message(ws, raw) -> None:
    # 1. Dekódování a Robustní parsování (Regex oprava)
    if isinstance(raw, (bytes, bytearray)):
        raw = raw.decode("utf-8", errors="ignore")

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        fixed = _fix_json_text(raw)
        data = json.loads(fixed)

    # Získání payloadu (řeší různé formáty brokera)
    payload_raw = data.get("payload", data)
    if isinstance(payload_raw, str):
        try:
            payload = json.loads(payload_raw)
        except:
            # Tvůj slavný regex opravář
            fixed_p = re.sub(r"(\w+):", r'"\1":', payload_raw)
            fixed_p = re.sub(r":\s*(\w+[\w\.]*)", r': "\1"', fixed_p)
            payload = json.loads(fixed_p.replace('""', '"'))
    else:
        payload = payload_raw

    # Ignorování zpráv, které poslal sám worker nebo chyb z brokera
    if payload.get("status") in ["completed", "failed"] or data.get("action") == "error":
        return

    operation = payload.get("operation")
    object_id = payload.get("object_id")
    bucket_id = payload.get("bucket_id", "default") # Default bucket pokud chybí

    if not operation or not object_id:
        return

    print(f"[WORKER] Přijata úloha: {operation} pro objekt {object_id} (bucket: {bucket_id})")

    # --- TADY JE TA ZMĚNA: STŘEDOBOD ČÁSTI 3 ---
    
    # 2. Příprava cest a složek
    os.makedirs("temp_in", exist_ok=True)
    os.makedirs("temp_out", exist_ok=True)
    temp_input = os.path.join("temp_in", object_id)
    temp_output = os.path.join("temp_out", object_id)

    # 3. Stažení z S3 Gateway
    print(f"[WORKER] Stahuji {object_id} z S3 Gateway...")
    try:
        # Tady voláme tu funkci, co OpenCode přidal
        await download_image(bucket_id, object_id, temp_input)
    except Exception as e:
        print(f"[WORKER] Chyba při stahování: {e}")
        await send_status({"status": "failed", "error": "download_failed", "object_id": object_id})
        return

    # 4. Zpracování (NumPy)
    print(f"[WORKER] Startuji zpracování operace {operation}...")
    try:
        await asyncio.to_thread(process_image, temp_input, temp_output, operation, payload.get("params", {}))
    except Exception as e:
        print(f"[WORKER] Chyba NumPy: {e}")
        await send_status({"status": "failed", "error": str(e), "object_id": object_id})
        return

    # 5. Nahrání zpět na S3 Gateway
    print(f"[WORKER] Nahrávám upravený soubor zpět...")
    try:
        await upload_image(bucket_id, object_id, temp_output)
        print(f"[WORKER] Hotovo! Soubor nahrán.")
        await send_status({"status": "completed", "operation": operation, "object_id": object_id})
    except Exception as e:
        print(f"[WORKER] Chyba při nahrávání: {e}")
        
        

async def run() -> None:
    backoff = RECONNECT_BASE

    # Allow graceful shutdown via SIGINT/SIGTERM on POSIX. On Windows, KeyboardInterrupt will work.
    loop = asyncio.get_running_loop()
    stop = asyncio.Event()

    def _signal(_signum, _frame):
        stop.set()

    try:
        signal.signal(signal.SIGINT, _signal)
        signal.signal(signal.SIGTERM, _signal)
    except Exception:
        # signal may not be available on some platforms (Windows with Proactor loop etc.)
        pass

    while not stop.is_set():
        try:
            print(f"[WORKER] Pokouším se připojit k brokeru: {BROKER_URI}")
            # Set a reasonable max_size / ping interval if your broker needs tuning
            async with websockets.connect(BROKER_URI) as ws:
                print(f"[WORKER] Připojeno k brokeru: {BROKER_URI}")
                backoff = RECONNECT_BASE

                # Receive messages forever until connection closes
                async for message in ws:
                    # Handle each message concurrently so long processing doesn't block recv loop
                    asyncio.create_task(handle_message(ws, message))

        except Exception as exc:
            print(f"[WORKER] Odpojeno / chyba připojení: {exc}")

        if stop.is_set():
            break

        # Reconnect with exponential backoff + jitter
        wait = backoff + random.random()
        print(f"[WORKER] Znovu se pokusím připojit za {wait:.1f}s")
        try:
            await asyncio.wait_for(stop.wait(), timeout=wait)
            break
        except asyncio.TimeoutError:
            backoff = min(RECONNECT_MAX, backoff * 2)

    print("[WORKER] Ukončuji službu.")


if __name__ == "__main__":
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        print("[WORKER] Přerušeno uživatelem.")
        try:
            sys.exit(0)
        except SystemExit:
            pass
