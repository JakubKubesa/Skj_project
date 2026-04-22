import os
from typing import Optional, Dict

import numpy as np
from PIL import Image


def process_image(input_path: str, output_path: str, operation: str, params: Optional[Dict] = None) -> None:
    """Load an image, apply a NumPy-based operation, and save the result.

    Supported operations (operation string):
    - "invert": pixel-wise inversion (255 - img)
    - "flip": horizontal flip using slicing
    - "crop": crop by params {x_start, y_start, width, height}
    - "brightness": add value by params {value}
    - "grayscale": convert to grayscale using weighted average

    This function operates fully with NumPy vectorized operations and saves
    the resulting image to `output_path` (overwriting if exists).
    """
    params = params or {}

    # Load and ensure RGB
    img = Image.open(input_path).convert("RGB")
    arr = np.array(img)  # dtype uint8, shape (H, W, 3)

    op = operation.lower()

    if op == "invert" or op == "inversion":
        result = 255 - arr

    elif op == "flip":
        # Horizontal flip (mirror along vertical axis)
        result = arr[:, ::-1, :]

    elif op == "crop":
        x_start = int(params.get("x_start", 0))
        y_start = int(params.get("y_start", 0))
        w = int(params.get("width", 0))
        h = int(params.get("height", 0))

        img_h, img_w = arr.shape[0], arr.shape[1]

        x0 = max(0, x_start)
        y0 = max(0, y_start)
        x1 = min(img_w, x0 + max(0, w))
        y1 = min(img_h, y0 + max(0, h))

        # If resulting box is empty, return original
        if x1 <= x0 or y1 <= y0:
            result = arr.copy()
        else:
            result = arr[y0:y1, x0:x1, :]

    elif op == "brightness":
        # params: value (int, can be negative)
        val = int(params.get("value", 0))
        temp = arr.astype(np.int16)
        temp = temp + val
        temp = np.clip(temp, 0, 255)
        result = temp.astype(np.uint8)

    elif op == "grayscale" or op == "gray":
        # Weighted sum -> single channel, then stack to 3 channels for compatibility
        # Use float computation for accuracy, then clip/convert
        r = arr[:, :, 0].astype(np.float32)
        g = arr[:, :, 1].astype(np.float32)
        b = arr[:, :, 2].astype(np.float32)
        gray = 0.299 * r + 0.587 * g + 0.114 * b
        gray = np.clip(np.round(gray), 0, 255).astype(np.uint8)
        result = np.stack([gray, gray, gray], axis=-1)

    else:
        raise ValueError(f"Nepodporovaná operace: {operation}")

    # Ensure output directory exists
    out_dir = os.path.dirname(output_path)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)

    out_img = Image.fromarray(result)
    out_img.save(output_path)
