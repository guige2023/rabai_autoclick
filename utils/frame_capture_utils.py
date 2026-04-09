"""Frame capture and video processing utilities.

Provides utilities for capturing frames, video stream processing,
frame differencing, motion detection, and video encoding/decoding.
"""

from __future__ import annotations

from typing import Optional, Tuple, List, Callable, Any, Protocol
from dataclasses import dataclass, field
from enum import Enum, auto
import struct
import time


class PixelFormat(Enum):
    """Supported pixel formats for frame capture."""
    RGB24 = auto()
    RGBA32 = auto()
    BGRA32 = auto()
    GRAY8 = auto()
    GRAY16 = auto()
    YUV420 = auto()
    YUV422 = auto()


@dataclass
class Frame:
    """Represents a captured frame with metadata.

    Attributes:
        width: Frame width in pixels.
        height: Frame height in pixels.
        data: Raw pixel data as bytes.
        pixel_format: Format of pixel data.
        timestamp: Capture timestamp in seconds.
        frame_number: Sequential frame number.
        stride: Bytes per row (for planar formats).
    """
    width: int
    height: int
    data: bytes
    pixel_format: PixelFormat = PixelFormat.RGB24
    timestamp: float = 0.0
    frame_number: int = 0
    stride: int = 0

    @property
    def size_bytes(self) -> int:
        """Get frame size in bytes."""
        return len(self.data)

    @property
    def aspect_ratio(self) -> float:
        """Get frame aspect ratio (width / height)."""
        return self.width / self.height if self.height > 0 else 0.0

    @property
    def bytes_per_pixel(self) -> int:
        """Get bytes per pixel for current format."""
        format_bytes = {
            PixelFormat.RGB24: 3,
            PixelFormat.RGBA32: 4,
            PixelFormat.BGRA32: 4,
            PixelFormat.GRAY8: 1,
            PixelFormat.GRAY16: 2,
            PixelFormat.YUV420: 1,
            PixelFormat.YUV422: 1,
        }
        return format_bytes.get(self.pixel_format, 3)

    def to_rgb24(self) -> bytes:
        """Convert frame to RGB24 format."""
        if self.pixel_format == PixelFormat.RGB24:
            return self.data
        if self.pixel_format == PixelFormat.BGRA32:
            return self._bgra_to_rgb()
        if self.pixel_format == PixelFormat.GRAY8:
            return self._gray_to_rgb()
        raise NotImplementedError(
            f"Conversion from {self.pixel_format} not implemented"
        )

    def _bgra_to_rgb(self) -> bytes:
        """Convert BGRA to RGB."""
        result = bytearray(len(self.data) * 3 // 4)
        for i in range(len(self.data) // 4):
            j = i * 3
            k = i * 4
            result[j] = self.data[k + 2]  # R
            result[j + 1] = self.data[k + 1]  # G
            result[j + 2] = self.data[k]  # B
        return bytes(result)

    def _gray_to_rgb(self) -> bytes:
        """Convert grayscale to RGB."""
        return b''.join(bytes([g, g, g]) for g in self.data)

    def get_region(self, x: int, y: int, w: int, h: int) -> bytes:
        """Extract rectangular region from frame."""
        if x < 0 or y < 0 or x + w > self.width or y + h > self.height:
            raise ValueError("Region out of bounds")
        bpp = self.bytes_per_pixel
        stride = self.stride if self.stride else self.width * bpp
        row_start = y * stride + x * bpp
        return self.data[row_start:row_start + h * stride]

    def __repr__(self) -> str:
        return (
            f"Frame({self.width}x{self.height}, {self.pixel_format.name}, "
            f"t={self.timestamp:.3f}, #{self.frame_number})"
        )


@dataclass
class FrameDifference:
    """Result of frame differencing operation."""
    changed: bool
    diff_pixels: int
    diff_percentage: float
    changed_regions: List[Tuple[int, int, int, int]] = field(default_factory=list)


class FrameCapture:
    """Base frame capture interface.

    Implement this interface for specific capture backends
    (screenshot, video file, camera, etc.).
    """

    def capture(self) -> Frame:
        """Capture a single frame."""
        raise NotImplementedError

    def capture_region(
        self, x: int, y: int, width: int, height: int
    ) -> Frame:
        """Capture a region of the screen."""
        raise NotImplementedError

    def start_continuous(
        self, interval: float, callback: Callable[[Frame], None]
    ) -> None:
        """Start continuous frame capture at specified interval."""
        raise NotImplementedError

    def stop_continuous(self) -> None:
        """Stop continuous frame capture."""
        raise NotImplementedError


class FrameDiffer:
    """Computes differences between consecutive frames.

    Useful for motion detection and change tracking.
    """

    def __init__(
        self,
        threshold: int = 30,
        min_changed_pixels: int = 100,
        min_diff_percentage: float = 0.1,
    ) -> None:
        """
        Args:
            threshold: Pixel difference threshold (0-255).
            min_changed_pixels: Minimum changed pixels to flag as changed.
            min_diff_percentage: Minimum percentage changed to flag as changed.
        """
        self.threshold = threshold
        self.min_changed_pixels = min_changed_pixels
        self.min_diff_percentage = min_diff_percentage
        self._previous_frame: Optional[Frame] = None

    def compute(self, current: Frame) -> FrameDifference:
        """Compute difference between current and previous frame."""
        if self._previous_frame is None:
            self._previous_frame = current
            return FrameDifference(
                changed=False, diff_pixels=0, diff_percentage=0.0
            )

        prev_data = self._previous_frame.to_rgb24()
        curr_data = current.to_rgb24()

        if len(prev_data) != len(curr_data):
            self._previous_frame = current
            return FrameDifference(
                changed=False, diff_pixels=0, diff_percentage=0.0
            )

        diff_count = 0
        changed_pixels: List[int] = []
        total_pixels = len(curr_data) // 3

        for i in range(0, len(curr_data), 3):
            r1, g1, b1 = prev_data[i], prev_data[i + 1], prev_data[i + 2]
            r2, g2, b2 = curr_data[i], curr_data[i + 1], curr_data[i + 2]
            diff = (abs(r1 - r2) + abs(g1 - g2) + abs(b1 - b2)) // 3
            if diff > self.threshold:
                diff_count += 1
                changed_pixels.append(i // 3)

        self._previous_frame = current
        diff_percentage = (diff_count / total_pixels * 100
                           if total_pixels > 0 else 0.0)
        changed = (diff_count >= self.min_changed_pixels
                   and diff_percentage >= self.min_diff_percentage)
        regions = self._find_changed_regions(
            changed_pixels, current.width, current.height
        )

        return FrameDifference(
            changed=changed,
            diff_pixels=diff_count,
            diff_percentage=diff_percentage,
            changed_regions=regions,
        )

    def _find_changed_regions(
        self, pixels: List[int], width: int, height: int
    ) -> List[Tuple[int, int, int, int]]:
        """Find bounding boxes of changed regions."""
        if not pixels:
            return []
        threshold = width * height * 0.01
        clusters: List[List[int]] = []
        for p in pixels:
            x, y = p % width, p // width
            found = False
            for cluster in clusters:
                last = cluster[-1]
                lx, ly = last % width, last // width
                if abs(x - lx) <= 10 and abs(y - ly) <= 10:
                    cluster.append(p)
                    found = True
                    break
            if not found:
                clusters.append([p])
        regions = []
        for cluster in clusters:
            if len(cluster) < threshold:
                continue
            xs = [p % width for p in cluster]
            ys = [p // width for p in cluster]
            regions.append((min(xs), min(ys), max(xs) - min(xs) + 1,
                            max(ys) - min(ys) + 1))
        return regions

    def reset(self) -> None:
        """Reset diff state (clear previous frame)."""
        self._previous_frame = None


@dataclass
class MotionDetector:
    """Motion detection using frame differencing."""
    differ: FrameDiffer
    cooldown_seconds: float = 0.5
    _last_motion_time: float = field(default=0.0, init=False)

    def detect(self, frame: Frame) -> Tuple[bool, FrameDifference]:
        """Detect motion in frame.

        Returns:
            Tuple of (motion_detected, difference_result)
        """
        diff = self.differ.compute(frame)
        now = time.time()
        in_cooldown = (now - self._last_motion_time) < self.cooldown_seconds
        motion = diff.changed and not in_cooldown
        if motion:
            self._last_motion_time = now
        return (motion, diff)


class FrameBuffer:
    """Buffer for storing and managing captured frames."""

    def __init__(self, max_frames: int = 30) -> None:
        self.max_frames = max_frames
        self._frames: List[Frame] = []
        self._dropped: int = 0

    def push(self, frame: Frame) -> None:
        """Add frame to buffer."""
        if len(self._frames) >= self.max_frames:
            self._frames.pop(0)
            self._dropped += 1
        self._frames.append(frame)

    def get_latest(self, count: int = 1) -> List[Frame]:
        """Get latest N frames."""
        return self._frames[-count:] if self._frames else []

    def get_all(self) -> List[Frame]:
        """Get all buffered frames."""
        return self._frames[:]

    def clear(self) -> None:
        """Clear all frames."""
        self._frames.clear()
        self._dropped = 0

    @property
    def size(self) -> int:
        return len(self._frames)

    @property
    def dropped_count(self) -> int:
        return self._dropped


def compute_frame_histogram(frame: Frame) -> List[int]:
    """Compute RGB histogram for a frame.

    Returns:
        List of 768 integers (256 for R, 256 for G, 256 for B).
    """
    if frame.pixel_format != PixelFormat.RGB24:
        data = frame.to_rgb24()
    else:
        data = frame.data
    hist = [0] * 768
    for i in range(0, len(data), 3):
        hist[data[i]] += 1  # R
        hist[256 + data[i + 1]] += 1  # G
        hist[512 + data[i + 2]] += 1  # B
    return hist


def compare_histograms(
    hist1: List[int], hist2: List[int]
) -> float:
    """Compare two histograms using correlation coefficient.

    Returns:
        Value between -1 and 1 (1 = identical, 0 = uncorrelated).
    """
    if len(hist1) != len(hist2) or len(hist1) != 768:
        raise ValueError("Histograms must both be 768 elements")
    n = sum(hist1)
    m = sum(hist2)
    if n == 0 or m == 0:
        return 0.0
    mean1 = sum(i * hist1[i] for i in range(768)) / n
    mean2 = sum(i * hist2[i] for i in range(768)) / m
    cov = sum((i - mean1) * (j - mean2) * hist1[i] * hist2[j]
              for i in range(768) for j in range(768))
    var1 = sum((i - mean1) ** 2 * hist1[i] for i in range(768))
    var2 = sum((i - mean2) ** 2 * hist2[i] for i in range(768))
    std1 = var1 ** 0.5
    std2 = var2 ** 0.5
    if std1 == 0 or std2 == 0:
        return 0.0
    return cov / (std1 * std2)
