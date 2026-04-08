"""Screenshot capture and processing utilities.

Provides cross-platform screenshot capture, region-based capture,
periodic capture for monitoring, and image encoding/conversion helpers.

Example:
    >>> from utils.screenshot_utils import capture_screen, capture_region, save_screenshot
    >>> img = capture_screen()
    >>> region = capture_region((100, 100, 400, 300))
    >>> save_screenshot(img, '/tmp/screenshot.png')
"""

from __future__ import annotations

import base64
import io
import time
from pathlib import Path
from typing import Optional

__all__ = [
    "capture_screen",
    "capture_region",
    "capture_all_displays",
    "save_screenshot",
    "load_screenshot",
    "screenshot_to_bytes",
    "bytes_to_base64",
    "base64_to_bytes",
    "ScreenCapture",
]


def capture_screen(display_index: int = 0) -> Optional[bytes]:
    """Capture the entire primary display.

    Args:
        display_index: Display index to capture (0 = primary).

    Returns:
        PNG-encoded image bytes, or None on failure.
    """
    import subprocess

    try:
        result = subprocess.run(
            ["screencapture", "-x", "-D", str(display_index + 1), "-"],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout
    except Exception:
        pass
    return None


def capture_region(
    region: tuple[float, float, float, float],
    include_cursor: bool = False,
) -> Optional[bytes]:
    """Capture a specific screen region.

    Args:
        region: Screen region as (x, y, width, height).
        include_cursor: Whether to include the mouse cursor.

    Returns:
        PNG-encoded image bytes, or None on failure.
    """
    import subprocess

    x, y, w, h = map(int, region)
    flags = ["-x"] if not include_cursor else ["-C", "-x"]
    try:
        result = subprocess.run(
            ["screencapture"] + flags + ["-R", f"{x},{y},{w},{h}", "-"],
            capture_output=True,
            timeout=10,
        )
        if result.returncode == 0:
            return result.stdout
    except Exception:
        pass
    return None


def capture_all_displays() -> list[tuple[int, bytes]]:
    """Capture all active displays.

    Returns:
        List of (display_index, image_bytes) tuples.
    """
    import subprocess

    results: list[tuple[int, bytes]] = []
    try:
        result = subprocess.run(
            ["screencapture", "-x", "-D", "0", "-"],
            capture_output=True,
            timeout=10,
        )
        # -D 0 captures all displays into separate images
        if result.returncode == 0 and len(result.stdout) > 100:
            # Try to decode as a multipage TIFF
            try:
                from PIL import Image

                img = Image.open(io.BytesIO(result.stdout))
                # Some macOS versions output a TIFF with all displays
                # Each frame is a separate page
                n_frames = getattr(img, "n_frames", 1)
                for i in range(n_frames):
                    img.seek(i)
                    buf = io.BytesIO()
                    img.save(buf, format="PNG")
                    results.append((i, buf.getvalue()))
            except ImportError:
                # No PIL, return raw bytes
                results.append((0, result.stdout))
    except Exception:
        pass
    return results


def save_screenshot(image_bytes: bytes, path: str | Path) -> bool:
    """Save screenshot bytes to a file.

    Args:
        image_bytes: PNG image data.
        path: Destination file path.

    Returns:
        True if saved successfully.
    """
    try:
        Path(path).write_bytes(image_bytes)
        return True
    except Exception:
        return False


def load_screenshot(path: str | Path) -> Optional[bytes]:
    """Load a screenshot from a file.

    Args:
        path: Source file path.

    Returns:
        Image bytes, or None on failure.
    """
    try:
        return Path(path).read_bytes()
    except Exception:
        return None


def screenshot_to_bytes(
    image_bytes: bytes,
    format: str = "PNG",
    quality: Optional[int] = None,
) -> bytes:
    """Convert screenshot bytes to a different format.

    Args:
        image_bytes: Source image bytes (PNG by default).
        format: Target format ('PNG', 'JPEG', 'WEBP', etc.).
        quality: Quality for lossy formats (1-100 for JPEG).

    Returns:
        Converted image bytes.
    """
    try:
        from PIL import Image

        img = Image.open(io.BytesIO(image_bytes))
        buf = io.BytesIO()
        if format.upper() == "JPEG" and quality is not None:
            img = img.convert("RGB")
            img.save(buf, format=format, quality=quality)
        else:
            img.save(buf, format=format)
        return buf.getvalue()
    except ImportError:
        return image_bytes


def bytes_to_base64(image_bytes: bytes) -> str:
    """Encode image bytes as a base64 string.

    Args:
        image_bytes: Raw image data.

    Returns:
        Base64-encoded string.
    """
    return base64.b64encode(image_bytes).decode("ascii")


def base64_to_bytes(b64: str) -> bytes:
    """Decode a base64 string back to image bytes.

    Args:
        b64: Base64-encoded image string.

    Returns:
        Raw image bytes.
    """
    return base64.b64decode(b64)


class ScreenCapture:
    """Periodic screen capture for monitoring workflows.

    Example:
        >>> capturer = ScreenCapture('/tmp/screenshots', interval=2.0)
        >>> capturer.start()
        >>> # ... do work ...
        >>> capturer.stop()
        >>> print(f"Captured {capturer.count} screenshots")
    """

    def __init__(
        self,
        output_dir: str | Path,
        interval: float = 1.0,
        region: Optional[tuple[float, float, float, float]] = None,
        format: str = "PNG",
        include_cursor: bool = False,
    ):
        self.output_dir = Path(output_dir)
        self.interval = interval
        self.region = region
        self.format = format
        self.include_cursor = include_cursor
        self._running = False
        self._count = 0

    def start(self) -> None:
        """Start periodic capture in a background thread."""
        self._running = True
        self._count = 0
        self._thread = _CaptureThread(
            self.output_dir,
            self.interval,
            self.region,
            self.format,
            self.include_cursor,
        )
        self._thread.start()

    def stop(self) -> int:
        """Stop capture and return the number of screenshots taken."""
        self._running = False
        if hasattr(self, "_thread"):
            self._count = self._thread.join()
        return self._count

    @property
    def count(self) -> int:
        return self._count


class _CaptureThread:
    """Internal capture thread."""

    def __init__(
        self,
        output_dir: Path,
        interval: float,
        region: Optional[tuple[float, float, float, float]],
        fmt: str,
        include_cursor: bool,
    ):
        self.output_dir = output_dir
        self.interval = interval
        self.region = region
        self.format = fmt
        self.include_cursor = include_cursor
        self._count = 0

    def run(self) -> None:
        import threading

        self.output_dir.mkdir(parents=True, exist_ok=True)
        while self._running:
            try:
                if self.region is not None:
                    data = capture_region(self.region, self.include_cursor)
                else:
                    data = capture_screen()
                if data:
                    ts = time.strftime("%Y%m%d_%H%M%S_%f")
                    path = self.output_dir / f"screenshot_{ts}.{self.format.lower()}"
                    if self.format.upper() != "PNG":
                        data = screenshot_to_bytes(data, format=self.format)
                    path.write_bytes(data)
                    self._count += 1
            except Exception:
                pass
            time.sleep(self.interval)

    def start(self) -> None:
        import threading

        t = threading.Thread(target=self.run, daemon=True)
        t.start()

    def join(self) -> int:
        time.sleep(0.1)
        return self._count
