"""NumPy-based image operations used by the async worker."""

import os
from typing import Any, Dict, Optional

import numpy as np
from PIL import Image


def _apply_crop(arr: np.ndarray, params: Dict[str, Any]) -> np.ndarray:
    """Crop an RGB image using explicit top-left coordinates and dimensions."""
    x_start = int(params.get("x_start", 0))
    y_start = int(params.get("y_start", 0))
    width = int(params.get("width", 0))
    height = int(params.get("height", 0))

    img_h, img_w = arr.shape[0], arr.shape[1]

    if width <= 0 or height <= 0:
        raise ValueError("Crop width and height must be positive")
    if x_start < 0 or y_start < 0:
        raise ValueError("Crop coordinates must be non-negative")
    if x_start + width > img_w or y_start + height > img_h:
        raise ValueError("Crop is outside image bounds")

    return arr[y_start:y_start + height, x_start:x_start + width, :]


def _apply_brightness(arr: np.ndarray, params: Dict[str, Any]) -> np.ndarray:
    """Increase or decrease brightness with saturation instead of uint8 overflow."""
    value = int(params.get("value", 0))
    temp = arr.astype(np.int16) + value
    temp = np.clip(temp, 0, 255)
    return temp.astype(np.uint8)


def _apply_grayscale(arr: np.ndarray) -> np.ndarray:
    """Convert an RGB image to grayscale using perceptual channel weights."""
    r = arr[:, :, 0].astype(np.float32)
    g = arr[:, :, 1].astype(np.float32)
    b = arr[:, :, 2].astype(np.float32)
    gray = 0.299 * r + 0.587 * g + 0.114 * b
    gray = np.clip(np.round(gray), 0, 255).astype(np.uint8)
    return np.stack([gray, gray, gray], axis=-1)


def process_image(input_path: str, output_path: str, operation: str, params: Optional[Dict[str, Any]] = None) -> None:
    """Load an image, apply one NumPy transformation, and save the output.

    Args:
        input_path: Local path to the source image.
        output_path: Local path where the transformed image should be saved.
        operation: One of ``invert``, ``flip``, ``crop``, ``brightness``, or
            ``grayscale``.
        params: Operation-specific numeric parameters.
    """
    params = params or {}

    img = Image.open(input_path).convert("RGB")
    arr = np.array(img)
    op = operation.lower()

    if op in ("invert", "inversion"):
        result = 255 - arr
    elif op == "flip":
        result = arr[:, ::-1, :]
    elif op == "crop":
        result = _apply_crop(arr, params)
    elif op == "brightness":
        result = _apply_brightness(arr, params)
    elif op in ("grayscale", "gray"):
        result = _apply_grayscale(arr)
    else:
        raise ValueError(f"Nepodporovana operace: {operation}")

    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    Image.fromarray(result).save(output_path)
