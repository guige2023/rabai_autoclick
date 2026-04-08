"""
Animation Detector Utility

Detects UI animations by monitoring frame-by-frame visual changes.
Used to wait for animations to complete before performing automation steps.

Example:
    >>> detector = AnimationDetector()
    >>> detector.start_monitor()
    >>> await_animation_complete(detector, timeout=3.0)
    >>> print("Animation done")
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Callable, Optional
import math


@dataclass
class AnimationEvent:
    """An detected animation event."""
    start_time: float
    end_time: float
    duration: float
    displacement: float
    direction: str
    element_bounds: tuple[int, int, int, int]


class AnimationDetector:
    """
    Detects UI animations by analyzing visual changes.

    Monitors pixel differences between consecutive screenshots
    to identify motion patterns characteristic of animations.
    """

    def __init__(
        self,
        sensitivity: float = 0.05,
        min_duration: float = 0.05,
        max_duration: float = 3.0,
    ) -> None:
        self.sensitivity = sensitivity  # 0.0 to 1.0
        self.min_duration = min_duration
        self.max_duration = max_duration
        self._monitoring = False
        self._thread: Optional[threading.Thread] = None
        self._callbacks: list[Callable[[AnimationEvent], None]] = []
        self._last_screenshot: Optional[str] = None
        self._animation_start: Optional[float] = None
        self._lock = threading.Lock()
        self._screenshot_interval = 0.05  # 50ms between checks

    def add_callback(self, callback: Callable[[AnimationEvent], None]) -> None:
        """Register callback for animation events."""
        self._callbacks.append(callback)

    def remove_callback(self, callback: Callable[[AnimationEvent], None]) -> None:
        """Remove a registered callback."""
        self._callbacks.remove(callback)

    def start_monitor(self, interval: float = 0.05) -> None:
        """
        Start monitoring for animations.

        Args:
            interval: Seconds between screenshot comparisons.
        """
        if self._monitoring:
            return
        self._screenshot_interval = interval
        self._monitoring = True
        self._thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self._thread.start()

    def stop_monitor(self) -> None:
        """Stop monitoring."""
        self._monitoring = False
        if self._thread:
            self._thread.join(timeout=2.0)
            self._thread = None

    def _monitor_loop(self) -> None:
        """Background monitoring loop."""
        import time as time_module

        while self._monitoring:
            try:
                screenshot = self._capture_region()
                if screenshot and self._last_screenshot:
                    diff = self._compute_difference(self._last_screenshot, screenshot)
                    in_animation = diff > self.sensitivity

                    with self._lock:
                        if in_animation and self._animation_start is None:
                            self._animation_start = time.time()
                        elif not in_animation and self._animation_start is not None:
                            # Animation ended
                            duration = time.time() - self._animation_start
                            if self.min_duration <= duration <= self.max_duration:
                                event = AnimationEvent(
                                    start_time=self._animation_start,
                                    end_time=time.time(),
                                    duration=duration,
                                    displacement=diff * 100,
                                    direction="unknown",
                                    element_bounds=(0, 0, 0, 0),
                                )
                                self._dispatch(event)
                            self._animation_start = None

                self._last_screenshot = screenshot
            except Exception:
                pass
            time_module.sleep(self._screenshot_interval)

    def _capture_region(self) -> Optional[str]:
        """Capture a screenshot region for comparison."""
        import subprocess
        import tempfile

        try:
            with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
                path = f.name
            result = subprocess.run(
                ["screencapture", "-x", path],
                capture_output=True,
                timeout=1.0,
            )
            if result.returncode == 0:
                return path
        except Exception:
            pass
        return None

    def _compute_difference(self, path1: str, path2: str) -> float:
        """
        Compute normalized difference between two images.

        Returns:
            Float 0.0 (identical) to 1.0 (completely different).
        """
        try:
            from PIL import Image
            import numpy as np

            img1 = Image.open(path1).convert("L").resize((64, 64))
            img2 = Image.open(path2).convert("L").resize((64, 64))
            arr1 = np.array(img1).astype(float)
            arr2 = np.array(img2).astype(float)

            diff = np.abs(arr1 - arr2).mean() / 255.0
            return float(diff)
        except Exception:
            return 0.0

    def _dispatch(self, event: AnimationEvent) -> None:
        """Dispatch event to all callbacks."""
        for cb in self._callbacks:
            try:
                cb(event)
            except Exception:
                pass

    def wait_for_animation(
        self,
        timeout: float = 5.0,
    ) -> bool:
        """
        Wait for any current animation to finish.

        Args:
            timeout: Maximum seconds to wait.

        Returns:
            True when animation complete or no animation, False on timeout.
        """
        start = time.time()
        while time.time() - start < timeout:
            with self._lock:
                if self._animation_start is None:
                    return True
            time.sleep(0.05)
        return False

    def is_animating(self) -> bool:
        """Return whether an animation is currently detected."""
        with self._lock:
            return self._animation_start is not None


def wait_for_animation_complete(
    detector: AnimationDetector,
    timeout: float = 5.0,
    poll_interval: float = 0.05,
) -> bool:
    """
    Block until animations complete.

    Args:
        detector: AnimationDetector instance.
        timeout: Maximum seconds to wait.
        poll_interval: Seconds between checks.

    Returns:
        True when complete, False on timeout.
    """
    start = time.time()
    while time.time() - start < timeout:
        if not detector.is_animating():
            return True
        time.sleep(poll_interval)
    return False
