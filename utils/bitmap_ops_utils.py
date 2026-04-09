"""Bitmap operations utilities for image manipulation and analysis.

This module provides low-level bitmap operations for image processing,
including pixel manipulation, channel operations, and bitmap comparisons.
"""

from __future__ import annotations

from typing import Callable
from dataclasses import dataclass


@dataclass
class BitmapRegion:
    """Region of interest within a bitmap.

    Attributes:
        x: Left X coordinate.
        y: Top Y coordinate.
        width: Width of region.
        height: Height of region.
    """
    x: int
    y: int
    width: int
    height: int


@dataclass
class BitmapMetrics:
    """Metrics for a bitmap image.

    Attributes:
        width: Image width in pixels.
        height: Image height in pixels.
        channels: Number of color channels.
        stride: Bytes per row (including padding).
        min_value: Minimum pixel value.
        max_value: Maximum pixel value.
        mean_value: Mean pixel value.
    """
    width: int
    height: int
    channels: int
    stride: int
    min_value: float
    max_value: float
    mean_value: float


def create_empty_bitmap(
    width: int,
    height: int,
    channels: int = 4,
    fill_value: int = 0
) -> list[bytearray]:
    """Create an empty bitmap with optional fill.

    Args:
        width: Bitmap width in pixels.
        height: Bitmap height in pixels.
        channels: Number of channels (3=RGB, 4=RGBA).
        fill_value: Value to fill each pixel with.

    Returns:
        List of bytearrays, one per row.
    """
    stride = width * channels
    padding = (4 - (stride % 4)) % 4
    row_stride = stride + padding

    bitmap: list[bytearray] = []

    for _ in range(height):
        row = bytearray(row_stride)
        if fill_value != 0:
            for i in range(stride):
                row[i] = fill_value
        bitmap.append(row)

    return bitmap


def get_pixel(
    bitmap: list[bytearray],
    x: int,
    y: int,
    channels: int = 4
) -> tuple[int, ...]:
    """Get pixel value at coordinates.

    Args:
        bitmap: Bitmap data.
        x: X coordinate.
        y: Y coordinate.
        channels: Number of channels.

    Returns:
        Tuple of channel values.
    """
    if y < 0 or y >= len(bitmap):
        return tuple([0] * channels)

    row = bitmap[y]
    stride = len(row) // channels if channels > 0 else len(row)

    if x < 0 or x >= stride:
        return tuple([0] * channels)

    offset = x * channels
    return tuple(row[offset:offset + channels])


def set_pixel(
    bitmap: list[bytearray],
    x: int,
    y: int,
    value: Sequence[int],
    channels: int = 4
) -> None:
    """Set pixel value at coordinates.

    Args:
        bitmap: Bitmap data.
        x: X coordinate.
        y: Y coordinate.
        value: Tuple/list of channel values.
        channels: Number of channels.
    """
    if y < 0 or y >= len(bitmap):
        return

    row = bitmap[y]
    stride = len(row) // channels if channels > 0 else len(row)

    if x < 0 or x >= stride:
        return

    offset = x * channels
    for i, v in enumerate(value):
        if i < channels:
            row[offset + i] = max(0, min(255, v))


def apply_threshold(
    bitmap: list[bytearray],
    threshold: int,
    channel: int = 0
) -> list[bytearray]:
    """Apply binary threshold to bitmap.

    Args:
        bitmap: Source bitmap data.
        threshold: Threshold value (0-255).
        channel: Which channel to threshold (0=R, 1=G, 2=B, 3=A).

    Returns:
        New bitmap with threshold applied.
    """
    height = len(bitmap)
    width = len(bitmap[0]) if height > 0 else 0
    channels = 4

    result = create_empty_bitmap(width, height, channels, fill_value=0)

    for y in range(height):
        for x in range(width):
            pixel = get_pixel(bitmap, x, y, channels)
            value = pixel[channel] if channel < len(pixel) else 0

            if value >= threshold:
                result[y][x * channels:(x + 1) * channels] = bytearray([255, 255, 255, 255])

    return result


def invert_bitmap(bitmap: list[bytearray], channels: int = 4) -> list[bytearray]:
    """Invert all pixel values in bitmap.

    Args:
        bitmap: Source bitmap data.
        channels: Number of channels.

    Returns:
        New inverted bitmap.
    """
    height = len(bitmap)
    width = len(bitmap[0]) if height > 0 else 0

    result: list[bytearray] = []

    for y in range(height):
        row = bytearray(len(bitmap[y]))
        for x in range(width):
            offset = x * channels
            for c in range(channels):
                if c < len(bitmap[y]) - offset:
                    row[offset + c] = 255 - bitmap[y][offset + c]
        result.append(row)

    return result


def crop_bitmap(
    bitmap: list[bytearray],
    region: BitmapRegion,
    channels: int = 4
) -> list[bytearray]:
    """Crop bitmap to specified region.

    Args:
        bitmap: Source bitmap data.
        region: Region to crop to.
        channels: Number of channels.

    Returns:
        New cropped bitmap.
    """
    result: list[bytearray] = []

    for y in range(region.y, region.y + region.height):
        if y < 0 or y >= len(bitmap):
            continue

        row = bytearray(region.width * channels)
        for x in range(region.x, region.x + region.width):
            if x >= 0 and x < len(bitmap[0]) // channels:
                src_offset = x * channels
                dst_offset = (x - region.x) * channels
                pixel = bitmap[y][src_offset:src_offset + channels]
                row[dst_offset:dst_offset + channels] = pixel

        result.append(row)

    return result


def blend_bitmaps(
    bg: list[bytearray],
    fg: list[bytearray],
    opacity: float = 1.0,
    channels: int = 4
) -> list[bytearray]:
    """Blend foreground bitmap over background.

    Args:
        bg: Background bitmap.
        fg: Foreground bitmap.
        opacity: Foreground opacity (0.0 to 1.0).
        channels: Number of channels.

    Returns:
        New blended bitmap.
    """
    if len(bg) == 0 or len(fg) == 0:
        return bg

    height = min(len(bg), len(fg))
    width = min(len(bg[0]) // channels, len(fg[0]) // channels)

    result: list[bytearray] = []

    for y in range(height):
        row = bytearray(len(bg[y]))
        for x in range(width):
            bg_pixel = get_pixel(bg, x, y, channels)
            fg_pixel = get_pixel(fg, x, y, channels)

            offset = x * channels
            for c in range(channels):
                if c < len(fg_pixel):
                    alpha = fg_pixel[3] / 255.0 if channels > 3 else 1.0
                    blended = int(fg_pixel[c] * alpha * opacity + bg_pixel[c] * (1 - alpha * opacity))
                    row[offset + c] = max(0, min(255, blended))

        result.append(row)

    return result


def apply_kernel(
    bitmap: list[bytearray],
    kernel: list[list[float]],
    channels: int = 4
) -> list[bytearray]:
    """Apply convolution kernel to bitmap.

    Args:
        bitmap: Source bitmap data.
        kernel: 2D convolution kernel.
        channels: Number of channels to process.

    Returns:
        New bitmap with kernel applied.
    """
    height = len(bitmap)
    width = len(bitmap[0]) // channels if height > 0 else 0
    k_height = len(kernel)
    k_width = len(kernel[0]) if k_height > 0 else 0

    k_half_h = k_height // 2
    k_half_w = k_width // 2

    result = create_empty_bitmap(width, height, channels, fill_value=0)

    for y in range(height):
        for x in range(width):
            accumulator = [0.0] * channels

            for ky in range(k_height):
                for kx in range(k_width):
                    sx = x + kx - k_half_w
                    sy = y + ky - k_half_h

                    if 0 <= sx < width and 0 <= sy < height:
                        pixel = get_pixel(bitmap, sx, sy, channels)
                        kernel_val = kernel[ky][kx]

                        for c in range(channels):
                            if c < len(pixel):
                                accumulator[c] += pixel[c] * kernel_val

            offset = y * len(result[0]) + x * channels
            for c in range(channels):
                if offset + c < len(result[y]):
                    result[y][offset + c] = max(0, min(255, int(accumulator[c])))

    return result


def compute_histogram(
    bitmap: list[bytearray],
    channel: int = 0
) -> list[int]:
    """Compute histogram for a single channel.

    Args:
        bitmap: Source bitmap data.
        channel: Channel to compute histogram for.

    Returns:
        List of 256 counts (one per possible value).
    """
    histogram = [0] * 256

    for row in bitmap:
        for i in range(0, len(row), 4):
            if channel < 4 and i + channel < len(row):
                value = row[i + channel]
                histogram[value] += 1

    return histogram


def compute_metrics(bitmap: list[bytearray], channels: int = 4) -> BitmapMetrics:
    """Compute metrics for a bitmap.

    Args:
        bitmap: Bitmap data.
        channels: Number of channels.

    Returns:
        BitmapMetrics with computed values.
    """
    height = len(bitmap)
    width = len(bitmap[0]) // channels if height > 0 else 0

    if height == 0 or width == 0:
        return BitmapMetrics(
            width=0, height=0, channels=channels,
            stride=0, min_value=0, max_value=0, mean_value=0
        )

    min_val = 255
    max_val = 0
    total = 0
    count = 0

    for row in bitmap:
        for i in range(0, len(row), channels):
            value = row[i]
            min_val = min(min_val, value)
            max_val = max(max_val, value)
            total += value
            count += 1

    return BitmapMetrics(
        width=width,
        height=height,
        channels=channels,
        stride=len(bitmap[0]),
        min_value=min_val,
        max_value=max_val,
        mean_value=total / count if count > 0 else 0
    )
