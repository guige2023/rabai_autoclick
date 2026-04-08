"""
Screen Recorder Utility

Records screen sessions as video for automation debugging and playback.
Supports region recording, frame rate control, and video encoding.

Example:
    >>> recorder = ScreenRecorder()
    >>> recorder.start(region=(0, 0, 800, 600), fps=15)
    >>> # ... run automation ...
    >>> recorder.stop(output_path="/tmp/recording.mp4")
"""

from __future__ import annotations

import os
import subprocess
import threading
import time
from dataclasses import dataclass
from typing import Optional


@dataclass
class RecordingConfig:
    """Configuration for screen recording."""
    region: tuple[int, int, int, int] = (0, 0, 1920, 1080)  # x, y, w, h
    fps: int = 15
    output_format: str = "mp4"
    codec: str = "h264"
    quality: int = 23  # CRF value (lower = higher quality)
    show_cursor: bool = True
    audio_enabled: bool = False


class ScreenRecorder:
    """
    Records screen content for later review.

    Uses macOS screencapture for frame extraction and ffmpeg for encoding.
    """

    def __init__(self) -> None:
        self._recording = False
        self._thread: Optional[threading.Thread] = None
        self._config = RecordingConfig()
        self._frames_dir: Optional[str] = None
        self._frame_count = 0
        self._start_time: Optional[float] = None
        self._lock = threading.Lock()

    def start(
        self,
        region: Optional[tuple[int, int, int, int]] = None,
        fps: int = 15,
        output_format: str = "mp4",
        show_cursor: bool = True,
    ) -> bool:
        """
        Start screen recording.

        Args:
            region: (x, y, width, height) region to record.
            fps: Frames per second.
            output_format: 'mp4', 'mov', or 'gif'.
            show_cursor: Whether to show cursor.

        Returns:
            True if recording started successfully.
        """
        if self._recording:
            return False

        if region:
            self._config.region = region
        self._config.fps = fps
        self._config.output_format = output_format
        self._config.show_cursor = show_cursor

        import tempfile
        self._frames_dir = tempfile.mkdtemp(prefix="screen_rec_")

        self._recording = True
        self._frame_count = 0
        self._start_time = time.time()
        self._thread = threading.Thread(target=self._record_loop, daemon=True)
        self._thread.start()
        return True

    def stop(self, output_path: Optional[str] = None) -> Optional[str]:
        """
        Stop recording and encode video.

        Args:
            output_path: Output file path. If None, uses timestamp.

        Returns:
            Path to output video, or None on failure.
        """
        if not self._recording:
            return None

        self._recording = False
        if self._thread:
            self._thread.join(timeout=5.0)
            self._thread = None

        if output_path is None:
            timestamp = int(time.time())
            ext = self._config.output_format
            output_path = f"/tmp/screen_recording_{timestamp}.{ext}"

        result = self._encode_video(output_path)
        self._cleanup_frames()
        return result

    def _record_loop(self) -> None:
        """Background recording loop."""
        import time as time_module

        while self._recording:
            try:
                self._capture_frame()
                self._frame_count += 1
                interval = 1.0 / self._config.fps
                time_module.sleep(interval)
            except Exception:
                pass

    def _capture_frame(self) -> None:
        """Capture a single frame using screencapture."""
        if self._frames_dir is None:
            return

        x, y, w, h = self._config.region
        frame_path = os.path.join(self._frames_dir, f"frame_{self._frame_count:06d}.png")

        # Use screencapture command
        cmd = [
            "screencapture",
            "-x",  # no sound
            "-R", f"{x},{y},{w},{h}",
            frame_path,
        ]
        try:
            subprocess.run(cmd, capture_output=True, timeout=2.0)
        except Exception:
            pass

    def _encode_video(self, output_path: str) -> Optional[str]:
        """Encode captured frames into video using ffmpeg."""
        if self._frames_dir is None:
            return None

        framerate = self._config.fps
        pattern = os.path.join(self._frames_dir, "frame_%06d.png")

        codec = "libx264" if self._config.output_format in ("mp4", "mov") else "gif"
        pipe = "-pix_fmt yuv420p" if codec != "gif" else ""

        cmd = [
            "ffmpeg",
            "-framerate", str(framerate),
            "-i", pattern,
            "-c:v", codec,
            "-crf", str(self._config.quality),
            pipe,
            "-y",
            output_path,
        ]

        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=300.0,
            )
            if result.returncode == 0:
                return output_path
        except Exception:
            pass
        return None

    def _cleanup_frames(self) -> None:
        """Remove captured frame files."""
        if self._frames_dir is None:
            return
        try:
            for f in os.listdir(self._frames_dir):
                os.remove(os.path.join(self._frames_dir, f))
            os.rmdir(self._frames_dir)
        except Exception:
            pass
        self._frames_dir = None

    def is_recording(self) -> bool:
        """Return whether recording is in progress."""
        return self._recording

    def get_frame_count(self) -> int:
        """Return number of frames captured."""
        return self._frame_count

    def get_duration(self) -> float:
        """Return recording duration in seconds."""
        if self._start_time is None:
            return 0.0
        if self._recording:
            return time.time() - self._start_time
        return 0.0
